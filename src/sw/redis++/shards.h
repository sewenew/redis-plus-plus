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

#ifndef SEWENEW_REDISPLUSPLUS_SHARDS_H
#define SEWENEW_REDISPLUSPLUS_SHARDS_H

#include <map>
#include <unordered_map>
#include <string>
#include <random>
#include "reply.h"
#include "connection_pool.h"
#include "node.h"

namespace sw {

namespace redis {

struct SlotRange {
    std::size_t min;
    std::size_t max;
};

inline bool operator<(const SlotRange &lhs, const SlotRange &rhs) {
    return lhs.max < rhs.max;
}

class ShardsPool {
public:
    ShardsPool() = default;

    ShardsPool(const ShardsPool &that) = delete;
    ShardsPool& operator=(const ShardsPool &that) = delete;

    ShardsPool(ShardsPool &&that);
    ShardsPool& operator=(ShardsPool &&that);

    ~ShardsPool() = default;

    ShardsPool(const ConnectionPoolOptions &pool_opts,
                const ConnectionOptions &connection_opts);

    // Fetch a connection by key.
    Connection fetch(const StringView &key);

    // Randomly pick a connection.
    Connection fetch();

    // Fetch a connection by node.
    Connection fetch(const Node &node);

    void release(Connection connection);

    void update();

private:
    void _move(ShardsPool &&that);

    // Get slot by key.
    std::size_t _slot(const StringView &key) const;

    // Randomly pick a slot.
    std::size_t _slot() const;

    Connection _fetch(std::size_t slot);

    ReplyUPtr _cluster_slots(const ConnectionOptions &opts) const;

    std::map<SlotRange, Node> _cluster_slots() const;

    std::pair<SlotRange, Node> _parse_slot_info(redisReply &reply) const;

    ConnectionPoolOptions _pool_opts;

    ConnectionOptions _connection_opts;

    std::map<SlotRange, Node> _shards;

    std::unordered_map<Node, ConnectionPool, NodeHash> _pool;

    std::mutex _mutex;

    static const std::size_t SHARDS = 16383;
};

}

}

#endif // end SEWENEW_REDISPLUSPLUS_SHARDS_H
