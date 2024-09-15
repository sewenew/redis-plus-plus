/**************************************************************************
   Copyright (c) 2021 sewenew

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

#include "sw/redis++/async_redis_cluster.h"
#include <cassert>

namespace sw {

namespace redis {

AsyncRedisCluster::AsyncRedisCluster(const Uri &uri) :
    AsyncRedisCluster(uri.connection_options(), uri.connection_pool_options()) {}

AsyncRedisCluster::AsyncRedisCluster(const ConnectionOptions &opts,
        const ConnectionPoolOptions &pool_opts,
        Role role,
        const EventLoopSPtr &loop) : _loop(loop) {
    if (!_loop) {
        _loop = std::make_shared<EventLoop>();
    }

    _pool = std::make_shared<AsyncShardsPool>(_loop, pool_opts, opts, role, ClusterOptions{});
}

AsyncRedisCluster::AsyncRedisCluster(const ConnectionOptions &opts,
        const ConnectionPoolOptions &pool_opts,
        Role role,
        const ClusterOptions &cluster_opts,
        const EventLoopSPtr &loop) : _loop(loop) {
    if (!_loop) {
        _loop = std::make_shared<EventLoop>();
    }

    _pool = std::make_shared<AsyncShardsPool>(_loop, pool_opts, opts, role, cluster_opts);
}

AsyncRedis AsyncRedisCluster::redis(const StringView &hash_tag, bool new_connection) {
    assert(_pool);

    auto pool = _pool->fetch(hash_tag);
    if (new_connection) {
        // Create a new pool.
        pool = pool->clone();
    }

    return AsyncRedis(std::make_shared<GuardedAsyncConnection>(pool));
}

AsyncSubscriber AsyncRedisCluster::subscriber() {
    assert(_pool);

    auto opts = _pool->connection_options();

    auto connection = std::make_shared<AsyncConnection>(opts, _loop.get());
    connection->set_subscriber_mode();

    return AsyncSubscriber(_loop, std::move(connection));
}

AsyncSubscriber AsyncRedisCluster::subscriber(const StringView &hash_tag) {
    assert(_pool);

    auto opts = _pool->connection_options(hash_tag);

    auto connection = std::make_shared<AsyncConnection>(opts, _loop.get());
    connection->set_subscriber_mode();

    return AsyncSubscriber(_loop, std::move(connection));
}

}

}
