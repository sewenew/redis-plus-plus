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

#ifndef SEWENEW_REDISPLUSPLUS_ASYNC_REDIS_H
#define SEWENEW_REDISPLUSPLUS_ASYNC_REDIS_H

#include "async_connection.h"
#include "event_loop.h"
#include "utils.h"
#include "command_args.h"

namespace sw {

namespace redis {

class AsyncRedis {
public:
    AsyncRedis(const ConnectionOptions &opts, const EventLoopSPtr &loop = nullptr);

    AsyncRedis(const AsyncRedis &) = delete;
    AsyncRedis& operator=(const AsyncRedis &) = delete;

    AsyncRedis(AsyncRedis &&) = default;
    AsyncRedis& operator=(AsyncRedis &&) = default;

    ~AsyncRedis();

    template <typename Result, typename ...Args>
    Future<Result> command(const StringView &cmd_name, Args &&...args) {
        CmdArgs cmd_args;
        cmd_args.append(cmd_name, std::forward<Args>(args)...);

        return _connection->send<Result>(cmd_args);
    }

private:
    EventLoopSPtr _loop;

    AsyncConnectionSPtr _connection;
};

}

}

#endif // end SEWENEW_REDISPLUSPLUS_ASYNC_REDIS_H
