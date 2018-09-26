/**************************************************************************
   Copyright (c) 2017 sewenew

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 *************************************************************************/

#include "shards.h"
#include "errors.h"

namespace sw {

namespace redis {

const std::size_t ShardsPool::SHARDS;

ShardsPool::ShardsPool(const ConnectionPoolOptions &pool_opts,
                        const ConnectionOptions &connection_opts) :
                            _pool_opts(pool_opts),
                            _connection_opts(connection_opts) {
    if (_connection_opts.type != ConnectionType::TCP) {
        throw Error("Only support TCP connection for Redis Cluster");
    }

    _shards = _cluster_slots();
    for (const auto &shard : _shards) {
        const auto &node = shard.second;

        auto opts = _connection_opts;
        opts.host = node.host;
        opts.port = node.port;

        _pool.emplace(node, ConnectionPool(_pool_opts, opts));
    }
}

ShardsPool::ShardsPool(ShardsPool &&that) {
    std::lock_guard<std::mutex> lock(that._mutex);

    _move(std::move(that));
}

ShardsPool& ShardsPool::operator=(ShardsPool &&that) {
    if (this != &that) {
        std::lock(_mutex, that._mutex);
        std::lock_guard<std::mutex> lock_this(_mutex, std::adopt_lock);
        std::lock_guard<std::mutex> lock_that(that._mutex, std::adopt_lock);

        _move(std::move(that));
    }

    return *this;
}

Connection ShardsPool::fetch(const StringView &key) {
    auto slot = _slot(key);

    return _fetch(slot);
}

Connection ShardsPool::fetch() {
    auto slot = _slot();

    return _fetch(slot);
}

Connection ShardsPool::fetch(const Node &node) {
    std::lock_guard<std::mutex> lock(_mutex);

    auto iter = _pool.find(node);
    if (iter == _pool.end()) {
        throw Error("Unknown node: " + node.host + ":" + std::to_string(node.port));
    }

    return iter->second.fetch();
}

Connection ShardsPool::_fetch(std::size_t slot) {
    std::lock_guard<std::mutex> lock(_mutex);

    auto node_iter = _shards.lower_bound(SlotRange{slot, slot});
    if (node_iter == _shards.end() || slot < node_iter->first.min) {
        throw Error("Slot is out of range: " + std::to_string(slot));
    }

    const auto &node = node_iter->second;

    auto iter = _pool.find(node);
    if (iter == _pool.end()) {
        throw Error("Slot is NOT covered: " + std::to_string(slot));
    }

    return iter->second.fetch();
}

void ShardsPool::release(Connection connection) {
    const auto &opts = connection.options();
    Node node{opts.host, opts.port};

    std::lock_guard<std::mutex> lock(_mutex);

    auto iter = _pool.find(node);
    if (iter == _pool.end()) {
        // The corresponding pool no longer exist. Let it go.
        return;
    }

    auto &pool = iter->second;
    pool.release(std::move(connection));
}

void ShardsPool::update() {
    auto shards = _cluster_slots();

    std::unordered_map<Node, ConnectionPool, NodeHash> pool;
    for (const auto &shard : shards) {
        const auto &node = shard.second;

        auto opts = _connection_opts;
        opts.host = node.host;
        opts.port = node.port;

        pool.emplace(node, ConnectionPool(_pool_opts, opts));
    }

    std::lock_guard<std::mutex> lock(_mutex);

    // TODO: this might cause connection leak, since client might
    // release connection to this newly created pool.
    _pool = std::move(pool);
    _shards = std::move(shards);
}

void ShardsPool::_move(ShardsPool &&that) {
    _pool_opts = that._pool_opts;
    _connection_opts = that._connection_opts;
    _shards = std::move(that._shards);
    _pool = std::move(that._pool);
}

std::size_t ShardsPool::_slot(const StringView &key) const {
    // The following code is copied from: https://redis.io/topics/cluster-spec
    // And I did some minor changes.

    const auto *k = key.data();
    auto keylen = key.size();

    // start-end indexes of { and }.
    std::size_t s = 0;
    std::size_t e = 0;

    // Search the first occurrence of '{'.
    for (s = 0; s < keylen; s++)
        if (k[s] == '{') break;

    // No '{' ? Hash the whole key. This is the base case.
    if (s == keylen) return crc16(k, keylen) & SHARDS;

    // '{' found? Check if we have the corresponding '}'.
    for (e = s + 1; e < keylen; e++)
        if (k[e] == '}') break;

    // No '}' or nothing between {} ? Hash the whole key.
    if (e == keylen || e == s + 1) return crc16(k, keylen) & SHARDS;

    // If we are here there is both a { and a } on its right. Hash
    // what is in the middle between { and }.
    return crc16(k + s + 1, e - s - 1) & SHARDS;
}

std::size_t ShardsPool::_slot() const {
    static thread_local std::default_random_engine engine;

    std::uniform_int_distribution<std::size_t> uniform_dist(0, SHARDS);
    return uniform_dist(engine);
}

ReplyUPtr ShardsPool::_cluster_slots(const ConnectionOptions &opts) const {
    auto connection = Connection(opts);

    connection.send("CLUSTER SLOTS");

    return connection.recv();
}

std::map<SlotRange, Node> ShardsPool::_cluster_slots() const {
    auto reply = _cluster_slots(_connection_opts);

    assert(reply);

    if (!reply::is_array(*reply)) {
        throw ProtoError("Expect ARRAY reply");
    }

    if (reply->element == nullptr || reply->elements == 0) {
        throw Error("Empty slots");
    }

    std::map<SlotRange, Node> shards;
    for (std::size_t idx = 0; idx != reply->elements; ++idx) {
        auto *sub_reply = reply->element[idx];
        if (sub_reply == nullptr) {
            throw ProtoError("Null slot info");
        }

        shards.emplace(_parse_slot_info(*sub_reply));
    }

    return shards;
}

std::pair<SlotRange, Node> ShardsPool::_parse_slot_info(redisReply &reply) const {
    if (reply.elements < 3 || reply.element == nullptr) {
        throw ProtoError("Invalid slot info");
    }

    // Min slot id
    auto *min_slot_reply = reply.element[0];
    if (min_slot_reply == nullptr) {
        throw ProtoError("Invalid min slot");
    }
    std::size_t min_slot = reply::parse<long long>(*min_slot_reply);

    // Max slot id
    auto *max_slot_reply = reply.element[1];
    if (max_slot_reply == nullptr) {
        throw ProtoError("Invalid max slot");
    }
    std::size_t max_slot = reply::parse<long long>(*max_slot_reply);

    if (min_slot > max_slot) {
        throw ProtoError("Invalid slot range");
    }

    // Node info
    auto *node_reply = reply.element[2];
    if (node_reply == nullptr
            || !reply::is_array(*node_reply)
            || node_reply->element == nullptr
            || node_reply->elements < 2) {
        throw ProtoError("Invalid node info");
    }

    auto host = reply::parse<std::string>(*(node_reply->element[0]));
    int port = reply::parse<long long>(*(node_reply->element[1]));

    return {SlotRange{min_slot, max_slot}, Node{host, port}};
}

}

}
