/**************************************************************************
   Copyright (c) 2022 sewenew

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

#ifndef SEWENEW_REDISPLUSPLUS_CO_REDIS_AWAITER_H
#define SEWENEW_REDISPLUSPLUS_CO_REDIS_AWAITER_H

#include <experimental/coroutine>
#include "async_utils.h"
#include "async_connection.h"

namespace sw {

namespace redis {

template <typename Result, typename ResultParser = DefaultResultParser<Result>>
class Awaiter {
public:
    bool await_ready() noexcept {
        return false;
    }

    void await_suspend(std::experimental::coroutine_handle<> handle) {
        _async_redis->co_command_with_parser<Result, ResultParser>(std::move(_cmd),
                [this, handle](Future<Result> &&fut) mutable {
                    _result = std::move(fut);

                    handle.resume();
                });
    }

    Result await_resume() {
        return _result.get();
    }

private:
    friend class CoRedis;

    Awaiter(AsyncRedis *r, FormattedCommand cmd) : _async_redis(r), _cmd(std::move(cmd)) {}

    AsyncRedis *_async_redis = nullptr;

    FormattedCommand _cmd;

    Future<Result> _result;
};

template <>
class Awaiter<void, DefaultResultParser<void>> {
public:
    bool await_ready() noexcept {
        return false;
    }

    void await_suspend(std::experimental::coroutine_handle<> handle) {
        _async_redis->co_command_with_parser<void, DefaultResultParser<void>>(std::move(_cmd),
                [this, handle](Future<void> &&fut) mutable {
                    _result = std::move(fut);

                    handle.resume();
                });
    }

    void await_resume() {
        _result.get();
    }

private:
    friend class CoRedis;

    Awaiter(AsyncRedis *r, FormattedCommand cmd) : _async_redis(r), _cmd(std::move(cmd)) {}

    AsyncRedis *_async_redis = nullptr;

    FormattedCommand _cmd;

    Future<void> _result;
};

}

}

#endif // end SEWENEW_REDISPLUSPLUS_CO_REDIS_AWAITER_H
