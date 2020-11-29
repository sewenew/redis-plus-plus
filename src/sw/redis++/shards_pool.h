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

#ifndef SEWENEW_REDISPLUSPLUS_SHARDS_POOL_H
#define SEWENEW_REDISPLUSPLUS_SHARDS_POOL_H

#include <cassert>
#include <unordered_map>
#include <string>
#include <random>
#include <memory>
#include "reply.h"
#include "connection_pool.h"
#include "shards.h"

namespace sw {

namespace redis {

class ShardsPool {
public:
    ShardsPool() = default;

    ShardsPool(const ShardsPool &that) = delete;
    ShardsPool& operator=(const ShardsPool &that) = delete;

    ShardsPool(ShardsPool &&that);
    ShardsPool& operator=(ShardsPool &&that);

    ~ShardsPool() = default;

    ShardsPool(const ConnectionPoolOptions &pool_opts,
                const ConnectionOptions &connection_opts,
                Role role);

    // Fetch a connection by key.
    ConnectionPoolSPtr fetch(const StringView &key);

    // Randomly pick a connection.
    ConnectionPoolSPtr fetch();

    // Fetch a connection by node.
    ConnectionPoolSPtr fetch(const Node &node);

    void update();

    ConnectionOptions connection_options(const StringView &key);

    ConnectionOptions connection_options();

private:
    void _move(ShardsPool &&that);

    void _init_pool(const Shards &shards);

    Shards _cluster_slots(Connection &connection) const;

    ReplyUPtr _cluster_slots_command(Connection &connection) const;

    Shards _parse_reply(redisReply &reply) const;

    Slot _parse_slot(redisReply *reply) const;

    Node _parse_node(redisReply *reply) const;

    std::pair<SlotRange, Node> _parse_slot_info(redisReply &reply) const;

    // Get slot by key.
    std::size_t _slot(const StringView &key) const;

    // Randomly pick a slot.
    std::size_t _slot() const;

    // Get a random number between [min, max]
    std::size_t _random(std::size_t min, std::size_t max) const;

    ConnectionPoolSPtr& _get_pool(Slot slot);

    ConnectionPoolSPtr _fetch(Slot slot);

    ConnectionOptions _connection_options(Slot slot);

    using NodeMap = std::unordered_map<Node, ConnectionPoolSPtr, NodeHash>;

    NodeMap::iterator _add_node(const Node &node);

    ConnectionPoolOptions _pool_opts;

    ConnectionOptions _connection_opts;

    Shards _shards;

    NodeMap _pools;

    std::mutex _mutex;

    Role _role = Role::MASTER;

    static const std::size_t SHARDS = 16383;
};

}

}

#endif // end SEWENEW_REDISPLUSPLUS_SHARDS_POOL_H
