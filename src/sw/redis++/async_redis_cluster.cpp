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

#include "async_redis_cluster.h"
#include <cassert>

namespace {

struct SetResultParser {
    bool operator()(redisReply &reply) const {
        sw::redis::reply::rewrite_set_reply(reply);
        return sw::redis::reply::parse<bool>(reply);
    }
};

}

namespace sw {

namespace redis {

AsyncRedisCluster::AsyncRedisCluster(const ConnectionOptions &opts,
        const ConnectionPoolOptions &pool_opts,
        Role role,
        const EventLoopSPtr &loop) : _loop(loop) {
    if (!_loop) {
        _loop = std::make_shared<EventLoop>();
    }

    _pool = std::make_shared<AsyncShardsPool>(_loop, pool_opts, opts, role);
}

Future<OptionalString> AsyncRedisCluster::get(const StringView &key) {
    return _command<OptionalString>(key, "GET %b", key.data(), key.size());
}

/*
Future<bool> AsyncRedisCluster::set(const StringView &key,
        const StringView &val,
        const std::chrono::milliseconds &ttl,
        UpdateType type) {
    CmdArgs args;
    args << "SET" << key << val;

    if (ttl > std::chrono::milliseconds(0)) {
        args << "PX" << ttl.count();
    }

    cmd::detail::set_update_type(args, type);

    return _command_with_parser<bool, SetResultParser>(key, args);
}
*/

}

}
