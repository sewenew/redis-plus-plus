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

#ifndef SEWENEW_REDISPLUSPLUS_CO_REDIS_H
#define SEWENEW_REDISPLUSPLUS_CO_REDIS_H

#if __has_include(<coroutine>)
# include <coroutine>
#elif __has_include(<experimental/coroutine>)
# include <experimental/coroutine>
# ifndef coroutine_handle
#  define coroutine_handle experimental::coroutine_handle
# endif
# ifndef suspend_never
#  define suspend_never experimental::suspend_never
# endif
#else
# error "<coroutine> not found."
#endif
#include "sw/redis++/async_redis.h"
#include "sw/redis++/cxx_utils.h"
#include "sw/redis++/cmd_formatter.h"
#include "sw/redis++/async_sentinel.h"
#include "sw/redis++/redis_uri.h"

namespace sw {

namespace redis {

using CoSentinel = AsyncSentinel;

class CoRedis {
public:
    explicit CoRedis(const ConnectionOptions &opts,
            const ConnectionPoolOptions &pool_opts = {}) : _async_redis(opts, pool_opts) {}

    explicit CoRedis(const std::string &uri) : CoRedis(Uri(uri)) {}

    CoRedis(const std::shared_ptr<CoSentinel> &sentinel,
            const std::string &master_name,
            Role role,
            const ConnectionOptions &connection_opts,
            const ConnectionPoolOptions &pool_opts = {}) :
        _async_redis(sentinel, master_name, role, connection_opts, pool_opts) {}

    CoRedis(const CoRedis &) = delete;
    CoRedis& operator=(const CoRedis &) = delete;

    CoRedis(CoRedis &&) = default;
    CoRedis& operator=(CoRedis &&) = default;

    ~CoRedis() = default;

    template <typename Result, typename ResultParser = DefaultResultParser<Result>, typename = void>
    class Awaiter {
    public:
        bool await_ready() noexcept {
            return false;
        }

        void await_suspend(std::coroutine_handle<> handle) {
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

    template <typename Result>
    class Awaiter<Result, DefaultResultParser<Result>,
          typename std::enable_if<std::is_same<Result, void>::value, void>::type> {
    public:
        bool await_ready() noexcept {
            return false;
        }

        void await_suspend(std::coroutine_handle<> handle) {
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
    template <typename Result, typename ...Args>
    Awaiter<Result> command(const StringView &cmd_name, Args &&...args) {
        auto formatter = [](const StringView &name, Args &&...params) {
            CmdArgs cmd_args;
            cmd_args.append(name, std::forward<Args>(params)...);
            return fmt::format_cmd(cmd_args);
        };

        return _command<Result>(formatter, cmd_name, std::forward<Args>(args)...);
    }

    template <typename Result, typename Input>
    auto command(Input first, Input last)
        -> typename std::enable_if<IsIter<Input>::value, Awaiter<Result>>::type {
        auto formatter = [](Input start, Input stop) {
            CmdArgs cmd_args;
            while (start != stop) {
                cmd_args.append(*start);
                ++start;
            }
            return fmt::format_cmd(cmd_args);
        };

        return _command<Result>(formatter, first, last);
    }

    // STRING commands.

    Awaiter<long long> del(const StringView &key) {
        return _command<long long>(fmt::del, key);
    }

    template <typename Input>
    Awaiter<long long> del(Input first, Input last) {
        range_check("DEL", first, last);

        return _command<long long>(fmt::del_range<Input>, first, last);
    }

    template <typename T>
    Awaiter<long long> del(std::initializer_list<T> il) {
        return del(il.begin(), il.end());
    }

    Awaiter<OptionalString> get(const StringView &key) {
        return _command<OptionalString>(fmt::get, key);
    }

    Awaiter<bool, fmt::SetResultParser> set(const StringView &key,
            const StringView &val,
            const std::chrono::milliseconds &ttl = std::chrono::milliseconds(0),
            UpdateType type = UpdateType::ALWAYS) {
        return _command_with_parser<bool, fmt::SetResultParser>(fmt::set, key, val, ttl, type);
    }

    Awaiter<bool, fmt::SetResultParser> set(const StringView &key,
            const StringView &val,
            bool keepttl,
            UpdateType type = UpdateType::ALWAYS) {
        return _command_with_parser<bool, fmt::SetResultParser>(fmt::set_keepttl, key, val, keepttl, type);
    }

    // HASH commands.

    Awaiter<long long> hdel(const StringView &key, const StringView &field) {
        return _command<long long>(fmt::hdel, key, field);
    }

    template <typename Input>
    Awaiter<long long> hdel(const StringView &key, Input first, Input last) {
        range_check("HDEL", first, last);

        return _command<long long>(fmt::hdel_range<Input>, key, first, last);
    }

    template <typename T>
    Awaiter<long long> hdel(const StringView &key, std::initializer_list<T> il) {
        return hdel(key, il.begin(), il.end());
    }

    Awaiter<OptionalString> hget(const StringView &key, const StringView &field) {
        return _command<OptionalString>(fmt::hget, key, field);
    }

    template <typename Output>
    Awaiter<Output> hgetall(const StringView &key) {
        return _command<Output>(fmt::hgetall, key);
    }

    Awaiter<long long> hset(const StringView &key, const StringView &field, const StringView &val) {
        return _command<long long>(fmt::hset, key, field, val);
    }

    Awaiter<long long> hset(const StringView &key, const std::pair<StringView, StringView> &item) {
        return hset(key, item.first, item.second);
    }

    template <typename Input>
    Awaiter<long long> hset(const StringView &key, Input first, Input last) {
        range_check("HSET", first, last);

        return _command<long long>(fmt::hset_range<Input>, key, first, last);
    }

    template <typename T>
    Awaiter<long long> hset(const StringView &key, std::initializer_list<T> il) {
        return hset(key, il.begin(), il.end());
    }

    // SET commands.

    Awaiter<long long> sadd(const StringView &key, const StringView &member) {
        return _command<long long>(fmt::sadd, key, member);
    }

    template <typename Input>
    Awaiter<long long> sadd(const StringView &key, Input first, Input last) {
        range_check("SADD", first, last);

        return _command<long long>(fmt::sadd_range<Input>, key, first, last);
    }

    template <typename T>
    Awaiter<long long> sadd(const StringView &key, std::initializer_list<T> il) {
        return sadd(key, il.begin(), il.end());
    }

    Awaiter<bool> sismember(const StringView &key, const StringView &member) {
        return _command<bool>(fmt::sismember, key, member);
    }

    template <typename Output>
    Awaiter<Output> smembers(const StringView &key) {
        return _command<Output>(fmt::smembers, key);
    }

    Awaiter<long long> srem(const StringView &key, const StringView &member) {
        return _command<long long>(fmt::srem, key, member);
    }

    template <typename Input>
    Awaiter<long long> srem(const StringView &key, Input first, Input last) {
        range_check("SREM", first, last);

        return _command<long long>(fmt::srem_range<Input>, key, first, last);
    }

    template <typename T>
    Awaiter<long long> srem(const StringView &key, std::initializer_list<T> il) {
        return srem(key, il.begin(), il.end());
    }

private:
    explicit CoRedis(const Uri &uri) :
        CoRedis(uri.connection_options(), uri.connection_pool_options()) {}

    template <typename Result, typename Formatter, typename ...Args>
    Awaiter<Result> _command(Formatter &&formatter, Args &&...args) {
        return _command_with_parser<Result, DefaultResultParser<Result>>(std::forward<Formatter>(formatter),
                std::forward<Args>(args)...);
    }

    template <typename Result, typename ResultParser, typename Formatter, typename ...Args>
    Awaiter<Result, ResultParser> _command_with_parser(Formatter &&formatter, Args &&...args) {
        return Awaiter<Result, ResultParser>(&_async_redis, formatter(std::forward<Args>(args)...));
    }

    AsyncRedis _async_redis;
};

}

}

#endif // end SEWENEW_REDISPLUSPLUS_CO_REDIS_H
