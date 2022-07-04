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

#include "async_redis.h"
#include "reply.h"

namespace sw {

namespace redis {

AsyncRedis::AsyncRedis(const ConnectionOptions &opts,
        const ConnectionPoolOptions &pool_opts,
        const EventLoopSPtr &loop) : _loop(loop) {
    if (!_loop) {
        _loop = std::make_shared<EventLoop>();
        _own_loop = true;
    }

    _pool = std::make_shared<AsyncConnectionPool>(_loop, pool_opts, opts);
}

AsyncRedis::AsyncRedis(const std::shared_ptr<AsyncSentinel> &sentinel,
        const std::string &master_name,
        Role role,
        const ConnectionOptions &opts,
        const ConnectionPoolOptions &pool_opts,
        const EventLoopSPtr &loop) : _loop(loop) {
    if (!_loop) {
        _loop = std::make_shared<EventLoop>();
        _own_loop = true;
    }

    _pool = std::make_shared<AsyncConnectionPool>(SimpleAsyncSentinel(sentinel, master_name, role),
                                                    _loop,
                                                    pool_opts,
                                                    opts);
}

AsyncRedis::~AsyncRedis() {
    if (_own_loop && _loop) {
        _loop->stop();
    }
}

AsyncSubscriber AsyncRedis::subscriber() {
    // TODO: maybe we don't need to check this,
    // since there's no Transaction or Pipeline for AsyncRedis
    if (!_pool) {
        throw Error("cannot create subscriber in single connection mode");
    }

    auto connection = _pool->create();
    connection->set_subscriber_mode();

    return AsyncSubscriber(_loop, std::move(connection));
}

}

}
