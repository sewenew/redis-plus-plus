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

#include "sw/redis++/async_connection.h"
#include "sw/redis++/async_connection_pool.h"
#include "sw/redis++/async_sentinel.h"
#include "sw/redis++/async_subscriber.h"
#include "sw/redis++/event_loop.h"
#include "sw/redis++/utils.h"
#include "sw/redis++/command.h"
#include "sw/redis++/command_args.h"
#include "sw/redis++/command_options.h"
#include "sw/redis++/cmd_formatter.h"
#include "sw/redis++/redis_uri.h"

namespace sw {

namespace redis {

class AsyncRedis {
public:
    explicit AsyncRedis(const ConnectionOptions &opts,
            const ConnectionPoolOptions &pool_opts = {},
            const EventLoopSPtr &loop = nullptr);

    explicit AsyncRedis(const std::string &uri) : AsyncRedis(Uri(uri)) {}

    AsyncRedis(const std::shared_ptr<AsyncSentinel> &sentinel,
                const std::string &master_name,
                Role role,
                const ConnectionOptions &connection_opts,
                const ConnectionPoolOptions &pool_opts = {},
                const EventLoopSPtr &loop = nullptr);

    AsyncRedis(const AsyncRedis &) = delete;
    AsyncRedis& operator=(const AsyncRedis &) = delete;

    AsyncRedis(AsyncRedis &&) = default;
    AsyncRedis& operator=(AsyncRedis &&) = default;

    ~AsyncRedis() = default;

    AsyncSubscriber subscriber();

    template <typename Result, typename ...Args>
    auto command(const StringView &cmd_name, Args &&...args)
        -> typename std::enable_if<!IsInvocable<typename LastType<Args...>::type,
                                        Future<Result> &&>::value, Future<Result>>::type {
        auto formatter = [](const StringView &name, Args &&...params) {
            CmdArgs cmd_args;
            cmd_args.append(name, std::forward<Args>(params)...);
            return fmt::format_cmd(cmd_args);
        };

        return _command<Result>(formatter, cmd_name, std::forward<Args>(args)...);
    }

    template <typename Result, typename ...Args>
    auto command(const StringView &cmd_name, Args &&...args)
        -> typename std::enable_if<IsInvocable<typename LastType<Args...>::type,
                                        Future<Result> &&>::value, void>::type {
        _callback_idx_command<Result>(LastValue(std::forward<Args>(args)...),
                cmd_name,
                MakeIndexSequence<sizeof...(Args) - 1>(),
                std::forward<Args>(args)...);
    }

    template <typename Result, typename Input>
    auto command(Input first, Input last)
        -> typename std::enable_if<IsIter<Input>::value, Future<Result>>::type {
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

    template <typename Result, typename Input, typename Callback>
    auto command(Input first, Input last, Callback &&cb)
        -> typename std::enable_if<IsIter<Input>::value, void>::type {
        auto formatter = [](Input start, Input stop) {
            CmdArgs cmd_args;
            while (start != stop) {
                cmd_args.append(*start);
                ++start;
            }
            return fmt::format_cmd(cmd_args);
        };

        _callback_fmt_command<Result>(std::forward<Callback>(cb), formatter,
                first, last);
    }

    // CONNECTION commands.

    Future<std::string> echo(const StringView &msg) {
        return _command<std::string>(fmt::echo, msg);
    }

    Future<std::string> ping() {
        return _command<std::string, FormattedCommand (*)()>(fmt::ping);
    }

    Future<std::string> ping(const StringView &msg) {
        return _command<std::string,
               FormattedCommand (*)(const StringView &)>(fmt::ping, msg);
    }

    // KEY commands.

    Future<long long> del(const StringView &key) {
        return _command<long long>(fmt::del, key);
    }

    template <typename Callback>
    auto del(const StringView &key, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<long long> &&>::value, void>::type {
        _callback_fmt_command<long long>(std::forward<Callback>(cb), fmt::del, key);
    }

    template <typename Input>
    Future<long long> del(Input first, Input last) {
        range_check("DEL", first, last);

        return _command<long long>(fmt::del_range<Input>, first, last);
    }

    template <typename Input, typename Callback>
    auto del(Input first, Input last, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<long long> &&>::value, void>::type {
        range_check("DEL", first, last);

        _callback_fmt_command<long long>(std::forward<Callback>(cb), fmt::del_range<Input>, first, last);
    }

    template <typename T>
    Future<long long> del(std::initializer_list<T> il) {
        return del(il.begin(), il.end());
    }

    template <typename T, typename Callback>
    auto del(std::initializer_list<T> il, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<long long> &&>::value, void>::type {
        del<decltype(il.begin()), Callback>(il.begin(), il.end(), std::forward<Callback>(cb));
    }

    Future<long long> exists(const StringView &key) {
        return _command<long long>(fmt::exists, key);
    }

    template <typename Callback>
    auto exists(const StringView &key, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<long long> &&>::value, void>::type {
        _callback_fmt_command<long long>(std::forward<Callback>(cb), fmt::exists, key);
    }

    template <typename Input>
    Future<long long> exists(Input first, Input last) {
        range_check("EXISTS", first, last);

        return _command<long long>(fmt::exists_range<Input>, first, last);
    }

    template <typename Input, typename Callback>
    auto exists(Input first, Input last, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<long long> &&>::value, void>::type {
        range_check("EXISTS", first, last);

        _callback_fmt_command<long long>(std::forward<Callback>(cb), fmt::exists_range<Input>, first, last);
    }

    template <typename T>
    Future<long long> exists(std::initializer_list<T> il) {
        return exists(il.begin(), il.end());
    }

    template <typename T, typename Callback>
    auto exists(std::initializer_list<T> il, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<long long> &&>::value, void>::type {
        exists<decltype(il.begin()), Callback>(il.begin(), il.end(), std::forward<Callback>(cb));
    }

    Future<bool> expire(const StringView &key, const std::chrono::seconds &timeout) {
        return _command<bool>(fmt::expire, key, timeout);
    }

    template <typename Callback>
    auto expire(const StringView &key, const std::chrono::seconds &timeout, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<bool> &&>::value, void>::type {
        _callback_fmt_command<bool>(std::forward<Callback>(cb), fmt::expire, key, timeout);
    }

    Future<bool> expireat(const StringView &key,
                    const std::chrono::time_point<std::chrono::system_clock,
                                                    std::chrono::seconds> &tp) {
        return _command<bool>(fmt::expireat, key, tp);
    }

    template <typename Callback>
    auto expireat(const StringView &key,
                  const std::chrono::time_point<std::chrono::system_clock, std::chrono::seconds> &tp,
                  Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<bool> &&>::value, void>::type {
        _callback_fmt_command<bool>(std::forward<Callback>(cb), fmt::expireat, key, tp);
    }

    Future<bool> pexpire(const StringView &key, const std::chrono::milliseconds &timeout) {
        return _command<bool>(fmt::pexpire, key, timeout);
    }

    template <typename Callback>
    auto pexpire(const StringView &key, const std::chrono::milliseconds &timeout, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<bool> &&>::value, void>::type {
        _callback_fmt_command<bool>(std::forward<Callback>(cb), fmt::pexpire, key, timeout);
    }

    Future<bool> pexpireat(const StringView &key,
                           const std::chrono::time_point<std::chrono::system_clock,
                           std::chrono::milliseconds> &tp) {
        return _command<bool>(fmt::pexpireat, key, tp);
    }

    template <typename Callback>
    auto pexpireat(const StringView &key,
                   const std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds> &tp,
                   Callback &&cb) ->
    typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<bool> &&>::value, void>::type {
        _callback_fmt_command<bool>(std::forward<Callback>(cb), fmt::pexpireat, key, tp);
    }

    Future<long long> pttl(const StringView &key) {
        return _command<long long>(fmt::pttl, key);
    }

    template <typename Callback>
    auto pttl(const StringView &key, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<long long> &&>::value, void>::type {
        _callback_fmt_command<long long>(std::forward<Callback>(cb), fmt::pttl, key);
    }

    Future<void> rename(const StringView &key, const StringView &newkey) {
        return _command<void>(fmt::rename, key, newkey);
    }

    Future<bool> renamenx(const StringView &key, const StringView &newkey) {
        return _command<bool>(fmt::renamenx, key, newkey);
    }

    template <typename Callback>
    auto renamenx(const StringView &key, const StringView &newkey, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<bool> &&>::value, void>::type {
        _callback_fmt_command<bool>(std::forward<Callback>(cb), fmt::renamenx, key, newkey);
    }

    Future<long long> ttl(const StringView &key) {
        return _command<long long>(fmt::ttl, key);
    }

    template <typename Callback>
    auto ttl(const StringView &key, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<long long> &&>::value, void>::type {
        _callback_fmt_command<long long>(std::forward<Callback>(cb), fmt::ttl, key);
    }

    Future<long long> unlink(const StringView &key) {
        return _command<long long>(fmt::unlink, key);
    }

    template <typename Callback>
    auto unlink(const StringView &key, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<long long> &&>::value, void>::type {
        _callback_fmt_command<long long>(std::forward<Callback>(cb), fmt::unlink, key);
    }

    template <typename Input>
    Future<long long> unlink(Input first, Input last) {
        range_check("UNLINK", first, last);

        return _command<long long>(fmt::unlink_range<Input>, first, last);
    }

    template <typename Input, typename Callback>
    auto unlink(Input first, Input last, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<long long> &&>::value, void>::type {
        range_check("UNLINK", first, last);

        _callback_fmt_command<long long>(std::forward<Callback>(cb), fmt::unlink_range<Input>, first, last);
    }

    template <typename T>
    Future<long long> unlink(std::initializer_list<T> il) {
        return unlink(il.begin(), il.end());
    }

    template <typename T, typename Callback>
    auto unlink(std::initializer_list<T> il, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<long long> &&>::value, void>::type {
        unlink<decltype(il.begin()), Callback>(il.begin(), il.end(), std::forward<Callback>(cb));
    }

    // STRING commands.

    Future<long long> append(const StringView &key, const StringView &str) {
        return _command<long long>(fmt::append, key, str);
    }

    template <typename Callback>
    auto append(const StringView &key, const StringView &str, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<long long> &&>::value, void>::type {
        _callback_fmt_command<long long>(std::forward<Callback>(cb), fmt::append, key, str);
    }

    Future<OptionalString> get(const StringView &key) {
        return _command<OptionalString>(fmt::get, key);
    }

    template <typename Callback>
    void get(const StringView &key, Callback &&cb) {
        _callback_fmt_command<OptionalString>(std::forward<Callback>(cb), fmt::get, key);
    }

    Future<long long> incr(const StringView &key) {
        return _command<long long>(fmt::incr, key);
    }

    template <typename Callback>
    auto incr(const StringView &key, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<long long> &&>::value, void>::type {
        _callback_fmt_command<long long>(std::forward<Callback>(cb), fmt::incr, key);
    }

    Future<long long> incrby(const StringView &key, long long increment) {
        return _command<long long>(fmt::incrby, key, increment);
    }

    template <typename Callback>
    auto incrby(const StringView &key, long long increment, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<long long> &&>::value, void>::type {
        _callback_fmt_command<long long>(std::forward<Callback>(cb), fmt::incrby, key, increment);
    }

    Future<double> incrbyfloat(const StringView &key, double increment) {
        return _command<double>(fmt::incrbyfloat, key, increment);
    }

    template <typename Callback>
    auto incrbyfloat(const StringView &key, double increment, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<double> &&>::value, void>::type {
        _callback_fmt_command<double>(std::forward<Callback>(cb), fmt::incrbyfloat, key, increment);
    }

    template <typename Output, typename Input>
    Future<Output> mget(Input first, Input last) {
        range_check("MGET", first, last);

        return _command<Output>(fmt::mget<Input>, first, last);
    }

    template <typename Output, typename Input, typename Callback>
    auto mget(Input first, Input last, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<Output> &&>::value, void>::type {
        range_check("MGET", first, last);

        _callback_fmt_command<Output>(std::forward<Callback>(cb), fmt::mget<Input>, first, last);
    }

    template <typename Output, typename T>
    Future<Output> mget(std::initializer_list<T> il) {
        return mget<Output>(il.begin(), il.end());
    }

    template <typename Output, typename T, typename Callback>
    auto mget(std::initializer_list<T> il, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<Output> &&>::value, void>::type {
        return mget<Output, decltype(il.begin()), Callback>(il.begin(), il.end(), std::forward<Callback>(cb));
    }

    template <typename Input>
    Future<void> mset(Input first, Input last) {
        range_check("MSET", first, last);

        return _command<void>(fmt::mset<Input>, first, last);
    }

    template <typename T>
    Future<void> mset(std::initializer_list<T> il) {
        return mset(il.begin(), il.end());
    }

    template <typename Input>
    Future<bool> msetnx(Input first, Input last) {
        range_check("MSETNX", first, last);

        return _command<bool>(fmt::msetnx<Input>, first, last);
    }

    template <typename Input, typename Callback>
    auto msetnx(Input first, Input last, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<bool> &&>::value, void>::type {
        range_check("MSETNX", first, last);

        _callback_fmt_command<bool>(std::forward<Callback>(cb), fmt::msetnx<Input>, first, last);
    }

    template <typename T>
    Future<bool> msetnx(std::initializer_list<T> il) {
        return msetnx(il.begin(), il.end());
    }

    template <typename T, typename Callback>
    auto msetnx(std::initializer_list<T> il, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<bool> &&>::value, void>::type {
        return msetnx(il.begin(), il.end(), std::forward<Callback>(cb));
    }

    Future<bool> set(const StringView &key,
                const StringView &val,
                const std::chrono::milliseconds &ttl = std::chrono::milliseconds(0),
                UpdateType type = UpdateType::ALWAYS) {
        return _command_with_parser<bool, fmt::SetResultParser>(fmt::set, key, val, ttl, type);
    }

    template <typename Callback>
    auto set(const StringView &key,
                const StringView &val,
                Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<bool> &&>::value, void>::type {
        _callback_command_with_parser<bool, fmt::SetResultParser>(std::forward<Callback>(cb),
                fmt::set, key, val, std::chrono::milliseconds(0), UpdateType::ALWAYS);
    }

    template <typename Callback>
    auto set(const StringView &key,
                const StringView &val,
                const std::chrono::milliseconds &ttl,
                Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<bool> &&>::value, void>::type {
        _callback_command_with_parser<bool, fmt::SetResultParser>(std::forward<Callback>(cb),
                fmt::set, key, val, ttl, UpdateType::ALWAYS);
    }

    template <typename Callback>
    auto set(const StringView &key,
                const StringView &val,
                const std::chrono::milliseconds &ttl,
                UpdateType type,
                Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<bool> &&>::value, void>::type {
        _callback_command_with_parser<bool, fmt::SetResultParser>(std::forward<Callback>(cb),
                fmt::set, key, val, ttl, type);
    }

    Future<bool> set(const StringView &key,
                const StringView &val,
                bool keepttl,
                UpdateType type = UpdateType::ALWAYS) {
        return _command_with_parser<bool, fmt::SetResultParser>(fmt::set_keepttl, key, val, keepttl, type);
    }

    template <typename Callback>
    auto set(const StringView &key,
                const StringView &val,
                bool keepttl,
                Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<bool> &&>::value, void>::type {
        _callback_command_with_parser<bool, fmt::SetResultParser>(std::forward<Callback>(cb),
                fmt::set_keepttl, key, val, keepttl, UpdateType::ALWAYS);
    }

    template <typename Callback>
    auto set(const StringView &key,
                const StringView &val,
                bool keepttl,
                UpdateType type,
                Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<bool> &&>::value, void>::type {
        _callback_command_with_parser<bool, fmt::SetResultParser>(std::forward<Callback>(cb),
                fmt::set_keepttl, key, val, keepttl, type);
    }

    Future<long long> strlen(const StringView &key) {
        return _command<long long>(fmt::strlen, key);
    }

    // LIST commands.

    Future<OptionalStringPair> blpop(const StringView &key,
                                const std::chrono::seconds &timeout = std::chrono::seconds{0}) {
        return _command<OptionalStringPair>(fmt::blpop, key, timeout);
    }

    template <typename Callback>
    auto blpop(const StringView &key,
               const std::chrono::seconds &timeout,
               Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<OptionalStringPair> &&>::value, void>::type {
        _callback_fmt_command<OptionalStringPair>(std::forward<Callback>(cb), fmt::blpop, key, timeout);
    }

    template <typename Callback>
    auto blpop(const StringView &key,
               Callback &&cb)
    -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<OptionalStringPair> &&>::value, void>::type {
        return blpop(key, std::chrono::seconds{0}, std::forward<Callback>(cb));
    }

    template <typename Input>
    Future<OptionalStringPair> blpop(Input first,
                                Input last,
                                const std::chrono::seconds &timeout = std::chrono::seconds{0}) {
        range_check("BLPOP", first, last);

        return _command<OptionalStringPair>(fmt::blpop_range<Input>, first, last, timeout);
    }

    template <typename Input, typename Callback>
    auto blpop(Input first,
               Input last,
               const std::chrono::seconds &timeout,
               Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<OptionalStringPair> &&>::value, void>::type {
        range_check("BLPOP", first, last);

        _callback_fmt_command<OptionalStringPair>(std::forward<Callback>(cb), fmt::blpop_range<Input>, first, last, timeout);
    }

    template <typename Input, typename Callback>
    auto blpop(Input first,
               Input last,
               Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<OptionalStringPair> &&>::value, void>::type {
        return blpop(first, last, std::chrono::seconds{0}, std::forward<Callback>(cb));
    }

    template <typename T>
    Future<OptionalStringPair> blpop(std::initializer_list<T> il,
                                const std::chrono::seconds &timeout = std::chrono::seconds{0}) {
        return blpop(il.begin(), il.end(), timeout);
    }

    template <typename T, typename Callback>
    auto blpop(std::initializer_list<T> il,
               const std::chrono::seconds &timeout,
               Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<OptionalStringPair> &&>::value, void>::type {
        return blpop(il.begin(), il.end(), timeout, std::forward<Callback>(cb));
    }

    template <typename T, typename Callback>
    auto blpop(std::initializer_list<T> il,
               Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<OptionalStringPair> &&>::value, void>::type {
        return blpop(il, std::chrono::seconds{0}, std::forward<Callback>(cb));
    }

    Future<OptionalStringPair> brpop(const StringView &key,
                                const std::chrono::seconds &timeout = std::chrono::seconds{0}) {
        return _command<OptionalStringPair>(fmt::brpop, key, timeout);
    }

    template <typename Callback>
    auto brpop(const StringView &key,
               const std::chrono::seconds &timeout,
               Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<OptionalStringPair> &&>::value, void>::type {
        _callback_fmt_command<OptionalStringPair>(std::forward<Callback>(cb), fmt::brpop, key, timeout);
    }

    template <typename Callback>
    auto brpop(const StringView &key,
               Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<OptionalStringPair> &&>::value, void>::type {
        return brpop(key, std::chrono::seconds{0}, std::forward<Callback>(cb));
    }

    template <typename Input>
    Future<OptionalStringPair> brpop(Input first,
                                Input last,
                                const std::chrono::seconds &timeout = std::chrono::seconds{0}) {
        range_check("BRPOP", first, last);

        return _command<OptionalStringPair>(fmt::brpop_range<Input>, first, last, timeout);
    }

    template <typename Input, typename Callback>
    auto brpop(Input first,
               Input last,
               const std::chrono::seconds &timeout,
               Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<OptionalStringPair> &&>::value, void>::type {
        range_check("BRPOP", first, last);

        _callback_fmt_command<OptionalStringPair>(std::forward<Callback>(cb), fmt::brpop_range<Input>, first, last, timeout);
    }

    template <typename Input, typename Callback>
    auto brpop(Input first,
               Input last,
               Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<OptionalStringPair> &&>::value, void>::type {
        return brpop(first, last, std::chrono::seconds{0}, std::forward<Callback>(cb));
    }

    template <typename T>
    Future<OptionalStringPair> brpop(std::initializer_list<T> il,
                                const std::chrono::seconds &timeout = std::chrono::seconds{0}) {
        return brpop(il.begin(), il.end(), timeout);
    }

    template <typename T, typename Callback>
    auto brpop(std::initializer_list<T> il,
               const std::chrono::seconds &timeout,
               Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<OptionalStringPair> &&>::value, void>::type {
        return brpop(il.begin(), il.end(), timeout, std::forward<Callback>(cb));
    }

    template <typename T, typename Callback>
    auto brpop(std::initializer_list<T> il,
               Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<OptionalStringPair> &&>::value, void>::type {
        return brpop(il, std::chrono::seconds{0}, std::forward<Callback>(cb));
    }

    Future<OptionalString> brpoplpush(const StringView &source,
                                const StringView &destination,
                                const std::chrono::seconds &timeout = std::chrono::seconds{0}) {
        return _command<OptionalString>(fmt::brpoplpush, source, destination, timeout);
    }

    template <typename Callback>
    auto brpoplpush(const StringView &source,
                    const StringView &destination,
                    const std::chrono::seconds &timeout,
                    Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<OptionalString> &&>::value, void>::type {
        _callback_fmt_command<OptionalString>(std::forward<Callback>(cb), fmt::brpoplpush, source, destination, timeout);
    }

    template <typename Callback>
    auto brpoplpush(const StringView &source,
                    const StringView &destination,
                    Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<OptionalString> &&>::value, void>::type {
        return brpoplpush(source, destination, std::chrono::seconds{0}, std::forward<Callback>(cb));
    }

    Future<long long> llen(const StringView &key) {
        return _command<long long>(fmt::llen, key);
    }

    template <typename Callback>
    auto llen(const StringView &key, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<long long> &&>::value, void>::type {
        _callback_fmt_command<long long>(std::forward<Callback>(cb), fmt::llen, key);
    }

    Future<OptionalString> lpop(const StringView &key) {
        return _command<OptionalString>(fmt::lpop, key);
    }

    template <typename Callback>
    auto lpop(const StringView &key, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<OptionalString> &&>::value, void>::type {
        _callback_fmt_command<OptionalString>(std::forward<Callback>(cb), fmt::lpop, key);
    }

    Future<long long> lpush(const StringView &key, const StringView &val) {
        return _command<long long>(fmt::lpush, key, val);
    }

    template <typename Callback>
    auto lpush(const StringView &key, const StringView &val, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<long long> &&>::value, void>::type {
        _callback_fmt_command<long long>(std::forward<Callback>(cb), fmt::lpush, key, val);
    }

    template <typename Input>
    Future<long long> lpush(const StringView &key, Input first, Input last) {
        range_check("LPUSH", first, last);

        return _command<long long>(fmt::lpush_range<Input>, key, first, last);
    }

    template <typename Input, typename Callback>
    auto lpush(const StringView &key, Input first, Input last, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<long long> &&>::value, void>::type {
        range_check("LPUSH", first, last);

        _callback_fmt_command<long long>(std::forward<Callback>(cb), fmt::lpush_range<Input>, key, first, last);
    }

    template <typename T>
    Future<long long> lpush(const StringView &key, std::initializer_list<T> il) {
        return lpush(key, il.begin(), il.end());
    }

    template <typename T, typename Callback>
    auto lpush(const StringView &key, std::initializer_list<T> il, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<long long> &&>::value, void>::type {
        return lpush(key, il.begin(), il.end(), std::forward<Callback>(cb));
    }

    template <typename Output>
    Future<Output> lrange(const StringView &key, long long start, long long stop) {
        return _command<Output>(fmt::lrange, key, start, stop);
    }

    template <typename Output, typename Callback>
    auto lrange(const StringView &key, long long start, long long stop, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<Output> &&>::value, void>::type {
        _callback_fmt_command<Output>(std::forward<Callback>(cb), fmt::lrange, key, start, stop);
    }

    Future<long long> lrem(const StringView &key, long long count, const StringView &val) {
        return _command<long long>(fmt::lrem, key, count, val);
    }

    template <typename Callback>
    auto lrem(const StringView &key, long long count, const StringView &val, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<long long> &&>::value, void>::type {
        _callback_fmt_command<long long>(std::forward<Callback>(cb), fmt::lrem, key, count, val);
    }

    Future<void> ltrim(const StringView &key, long long start, long long stop) {
        return _command<void>(fmt::ltrim, key, start, stop);
    }

    Future<OptionalString> rpop(const StringView &key) {
        return _command<OptionalString>(fmt::rpop, key);
    }

    template <typename Callback>
    auto rpop(const StringView &key, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<OptionalString> &&>::value, void>::type {
        _callback_fmt_command<OptionalString>(std::forward<Callback>(cb), fmt::rpop, key);
    }

    Future<OptionalString> rpoplpush(const StringView &source, const StringView &destination) {
        return _command<OptionalString>(fmt::rpoplpush, source, destination);
    }

    template <typename Callback>
    auto rpoplpush(const StringView &source, const StringView &destination, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<OptionalString> &&>::value, void>::type {
        _callback_fmt_command<OptionalString>(std::forward<Callback>(cb), fmt::rpoplpush, source, destination);
    }

    Future<long long> rpush(const StringView &key, const StringView &val) {
        return _command<long long>(fmt::rpush, key, val);
    }

    template <typename Callback>
    auto rpush(const StringView &key, const StringView &val, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<long long> &&>::value, void>::type {
        _callback_fmt_command<long long>(std::forward<Callback>(cb), fmt::rpush, key, val);
    }

    template <typename Input>
    Future<long long> rpush(const StringView &key, Input first, Input last) {
        range_check("RPUSH", first, last);

        return _command<long long>(fmt::rpush_range<Input>, key, first, last);
    }

    template <typename Input, typename Callback>
    auto rpush(const StringView &key, Input first, Input last, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<long long> &&>::value, void>::type {
        range_check("RPUSH", first, last);

        _callback_fmt_command<long long>(std::forward<Callback>(cb), fmt::rpush_range<Input>, key, first, last);
    }

    template <typename T>
    Future<long long> rpush(const StringView &key, std::initializer_list<T> il) {
        return rpush(key, il.begin(), il.end());
    }

    template <typename T, typename Callback>
    auto rpush(const StringView &key, std::initializer_list<T> il, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<long long> &&>::value, void>::type {
        return rpush(key, il.begin(), il.end(), std::forward<Callback>(cb));
    }

    // HASH commands.

    Future<long long> hdel(const StringView &key, const StringView &field) {
        return _command<long long>(fmt::hdel, key, field);
    }

    template <typename Callback>
    auto hdel(const StringView &key, const StringView &field, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<long long> &&>::value, void>::type {
        _callback_fmt_command<long long>(std::forward<Callback>(cb), fmt::hdel, key, field);
    }

    template <typename Input>
    Future<long long> hdel(const StringView &key, Input first, Input last) {
        range_check("HDEL", first, last);

        return _command<long long>(fmt::hdel_range<Input>, key, first, last);
    }

    template <typename Input, typename Callback>
    auto hdel(const StringView &key, Input first, Input last, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<long long> &&>::value, void>::type {
        range_check("HDEL", first, last);

        _callback_fmt_command<long long>(std::forward<Callback>(cb), fmt::hdel_range<Input>, key, first, last);
    }

    template <typename T>
    Future<long long> hdel(const StringView &key, std::initializer_list<T> il) {
        return hdel(key, il.begin(), il.end());
    }

    template <typename T, typename Callback>
    auto hdel(const StringView &key, std::initializer_list<T> il, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<long long> &&>::value, void>::type {
        hdel<decltype(il.begin()), Callback>(key, il.begin(), il.end(), std::forward<Callback>(cb));
    }

    Future<bool> hexists(const StringView &key, const StringView &field) {
        return _command<bool>(fmt::hexists, key, field);
    }

    template <typename Callback>
    auto hexists(const StringView &key, const StringView &field, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<bool> &&>::value, void>::type {
        _callback_fmt_command<bool>(std::forward<Callback>(cb), fmt::hexists, key, field);
    }

    Future<OptionalString> hget(const StringView &key, const StringView &field) {
        return _command<OptionalString>(fmt::hget, key, field);
    }

    template <typename Callback>
    void hget(const StringView &key, const StringView &field, Callback &&cb) {
        _callback_fmt_command<OptionalString>(std::forward<Callback>(cb), fmt::hget, key, field);
    }

    template <typename Output>
    Future<Output> hgetall(const StringView &key) {
        return _command<Output>(fmt::hgetall, key);
    }

    template <typename Output, typename Callback>
    void hgetall(const StringView &key, Callback &&cb) {
        _callback_fmt_command<Output>(std::forward<Callback>(cb), fmt::hgetall, key);
    }

    Future<long long> hincrby(const StringView &key, const StringView &field, long long increment) {
        return _command<long long>(fmt::hincrby, key, field, increment);
    }

    template <typename Callback>
    void hincrby(const StringView &key, const StringView &field, long long increment, Callback &&cb) {
        _callback_fmt_command<long long>(std::forward<Callback>(cb), fmt::hincrby, key, field, increment);
    }

    Future<double> hincrbyfloat(const StringView &key, const StringView &field, double increment) {
        return _command<double>(fmt::hincrbyfloat, key, field, increment);
    }

    template <typename Callback>
    void hincrbyfloat(const StringView &key, const StringView &field, double increment, Callback &&cb) {
        _callback_fmt_command<double>(std::forward<Callback>(cb), fmt::hincrbyfloat, key, field, increment);
    }

    template <typename Output>
    Future<Output> hkeys(const StringView &key) {
        return _command<Output>(fmt::hkeys, key);
    }

    template <typename Output, typename Callback>
    void hkeys(const StringView &key, Callback &&cb) {
        _callback_fmt_command<Output>(std::forward<Callback>(cb), fmt::hkeys, key);
    }

    Future<long long> hlen(const StringView &key) {
        return _command<long long>(fmt::hlen, key);
    }

    template <typename Callback>
    void hlen(const StringView &key, Callback &&cb) {
        _callback_fmt_command<long long>(std::forward<Callback>(cb), fmt::hlen, key);
    }

    template <typename Output, typename Input>
    Future<Output> hmget(const StringView &key, Input first, Input last) {
        range_check("HMGET", first, last);

        return _command<Output>(fmt::hmget<Input>, key, first, last);
    }

    template <typename Output, typename Input, typename Callback>
    auto hmget(const StringView &key, Input first, Input last, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<Output> &&>::value, void>::type {
        range_check("HMGET", first, last);

        _callback_fmt_command<Output>(std::forward<Callback>(cb), fmt::hmget<Input>, key, first, last);
    }

    template <typename Output, typename T>
    Future<Output> hmget(const StringView &key, std::initializer_list<T> il) {
        return hmget<Output>(key, il.begin(), il.end());
    }

    template <typename Output, typename T, typename Callback>
    auto hmget(const StringView &key, std::initializer_list<T> il, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<Output> &&>::value, void>::type {
        return hmget<Output>(key, il.begin(), il.end(), std::forward<Callback>(cb));
    }

    template <typename Input>
    Future<void> hmset(const StringView &key, Input first, Input last) {
        range_check("HMSET", first, last);

        return _command<void>(fmt::hmset<Input>, key, first, last);
    }

    template <typename T>
    Future<void> hmset(const StringView &key, std::initializer_list<T> il) {
        return hmset(key, il.begin(), il.end());
    }

    Future<bool> hset(const StringView &key, const StringView &field, const StringView &val) {
        return _command<bool>(fmt::hset, key, field, val);
    }

    template <typename Callback>
    auto hset(const StringView &key, const StringView &field, const StringView &val, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<long long> &&>::value, void>::type {
        _callback_fmt_command<long long>(std::forward<Callback>(cb), fmt::hset, key, field, val);
    }

    Future<bool> hset(const StringView &key, const std::pair<StringView, StringView> &item) {
        return hset(key, item.first, item.second);
    }

    template <typename Callback>
    auto hset(const StringView &key, const std::pair<StringView, StringView> &item, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<long long> &&>::value, void>::type {
        hset<Callback>(key, item.first, item.second, std::forward<Callback>(cb));
    }

    template <typename Input>
    auto hset(const StringView &key, Input first, Input last)
        -> typename std::enable_if<!std::is_convertible<Input, StringView>::value, Future<long long>>::type {
        range_check("HSET", first, last);

        return _command<long long>(fmt::hset_range<Input>, key, first, last);
    }

    template <typename Input, typename Callback>
    auto hset(const StringView &key, Input first, Input last, Callback &&cb)
        -> typename std::enable_if<!std::is_convertible<Input, StringView>::value &&
            IsInvocable<typename std::decay<Callback>::type, Future<long long> &&>::value, void>::type {
        range_check("HSET", first, last);

        _callback_fmt_command<long long>(std::forward<Callback>(cb), fmt::hset_range<Input>, key, first, last);
    }

    template <typename T>
    Future<long long> hset(const StringView &key, std::initializer_list<T> il) {
        return hset(key, il.begin(), il.end());
    }

    template <typename T, typename Callback>
    auto hset(const StringView &key, std::initializer_list<T> il, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<long long> &&>::value, void>::type {
        hset<decltype(il.begin()), Callback>(key, il.begin(), il.end(), std::forward<Callback>(cb));
    }

    template <typename Output>
    Future<Output> hvals(const StringView &key) {
        return _command<Output>(fmt::hvals, key);
    }

    template <typename Output, typename Callback>
    auto hvals(const StringView &key, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<Output> &&>::value, void>::type {
        _callback_fmt_command<Output>(std::forward<Callback>(cb), fmt::hvals, key);
    }

    // SET commands.

    Future<long long> sadd(const StringView &key, const StringView &member) {
        return _command<long long>(fmt::sadd, key, member);
    }

    template <typename Callback>
    auto sadd(const StringView &key, const StringView &member, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<long long> &&>::value, void>::type {
        _callback_fmt_command<long long>(std::forward<Callback>(cb), fmt::sadd, key, member);
    }

    template <typename Input>
    Future<long long> sadd(const StringView &key, Input first, Input last) {
        range_check("SADD", first, last);

        return _command<long long>(fmt::sadd_range<Input>, key, first, last);
    }

    template <typename Input, typename Callback>
    auto sadd(const StringView &key, Input first, Input last, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<long long> &&>::value, void>::type {
        range_check("SADD", first, last);

        _callback_fmt_command<long long>(std::forward<Callback>(cb), fmt::sadd_range<Input>, key, first, last);
    }

    template <typename T>
    Future<long long> sadd(const StringView &key, std::initializer_list<T> il) {
        return sadd(key, il.begin(), il.end());
    }

    template <typename T, typename Callback>
    auto sadd(const StringView &key, std::initializer_list<T> il, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<long long> &&>::value, void>::type {
        sadd<decltype(il.begin()), Callback>(key, il.begin(), il.end(), std::forward<Callback>(cb));
    }

    Future<long long> scard(const StringView &key) {
        return _command<long long>(fmt::scard, key);
    }

    template <typename Callback>
    auto scard(const StringView &key, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<long long> &&>::value, void>::type {
        _callback_fmt_command<long long>(std::forward<Callback>(cb), fmt::scard, key);
    }

    Future<bool> sismember(const StringView &key, const StringView &member) {
        return _command<bool>(fmt::sismember, key, member);
    }

    template <typename Callback>
    void sismember(const StringView &key, const StringView &member, Callback &&cb) {
        _callback_fmt_command<bool>(std::forward<Callback>(cb), fmt::sismember, key, member);
    }

    template <typename Output>
    Future<Output> smembers(const StringView &key) {
        return _command<Output>(fmt::smembers, key);
    }

    template <typename Output, typename Callback>
    void smembers(const StringView &key, Callback &&cb) {
        _callback_fmt_command<Output>(std::forward<Callback>(cb), fmt::smembers, key);
    }

    Future<OptionalString> spop(const StringView &key) {
        return _command<OptionalString,
               FormattedCommand (*)(const StringView &)>(fmt::spop, key);
    }

    template <typename Callback>
    void spop(const StringView &key, Callback &&cb) {
        _callback_fmt_command<OptionalString, Callback,
                FormattedCommand (*)(const StringView &)>(std::forward<Callback>(cb), fmt::spop, key);
    }

    template <typename Output>
    Future<Output> spop(const StringView &key, long long count) {
        return _command<Output,
               FormattedCommand (*)(const StringView &, long long)>(fmt::spop, key, count);
    }

    template <typename Output, typename Callback>
    void spop(const StringView &key, long long count, Callback &&cb) {
        _callback_fmt_command<Output, Callback,
                FormattedCommand (*)(const StringView &, long long count)>(std::forward<Callback>(cb), fmt::spop, key, count);
    }

    Future<long long> srem(const StringView &key, const StringView &member) {
        return _command<long long>(fmt::srem, key, member);
    }

    template <typename Callback>
    auto srem(const StringView &key, const StringView &member, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<long long> &&>::value, void>::type {
        _callback_fmt_command<long long>(std::forward<Callback>(cb), fmt::srem, key, member);
    }

    template <typename Input>
    Future<long long> srem(const StringView &key, Input first, Input last) {
        range_check("SREM", first, last);

        return _command<long long>(fmt::srem_range<Input>, key, first, last);
    }

    template <typename Input, typename Callback>
    auto srem(const StringView &key, Input first, Input last, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<long long> &&>::value, void>::type {
        range_check("SREM", first, last);

        _callback_fmt_command<long long>(std::forward<Callback>(cb), fmt::srem_range<Input>, key, first, last);
    }

    template <typename T>
    Future<long long> srem(const StringView &key, std::initializer_list<T> il) {
        return srem(key, il.begin(), il.end());
    }

    template <typename T, typename Callback>
    auto srem(const StringView &key, std::initializer_list<T> il, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<long long> &&>::value, void>::type {
        srem<decltype(il.begin()), Callback>(key, il.begin(), il.end(), std::forward<Callback>(cb));
    }

    // SORTED SET commands.

    auto bzpopmax(const StringView &key,
                    const std::chrono::seconds &timeout = std::chrono::seconds{0})
        -> Future<Optional<std::tuple<std::string, std::string, double>>> {
        return _command<Optional<std::tuple<std::string, std::string, double>>>(
                fmt::bzpopmax, key, timeout);
    }

    template <typename Input>
    auto bzpopmax(Input first,
                    Input last,
                    const std::chrono::seconds &timeout = std::chrono::seconds{0})
        -> Future<Optional<std::tuple<std::string, std::string, double>>> {
        range_check("BZPOPMAX", first, last);

        return _command<Optional<std::tuple<std::string, std::string, double>>>(
                fmt::bzpopmax_range<Input>, first, last, timeout);
    }

    template <typename T>
    auto bzpopmax(std::initializer_list<T> il,
                    const std::chrono::seconds &timeout = std::chrono::seconds{0})
        -> Future<Optional<std::tuple<std::string, std::string, double>>> {
        return bzpopmax(il.begin(), il.end(), timeout);
    }

    auto bzpopmin(const StringView &key,
                    const std::chrono::seconds &timeout = std::chrono::seconds{0})
        -> Future<Optional<std::tuple<std::string, std::string, double>>> {
        return _command<Optional<std::tuple<std::string, std::string, double>>>(
                fmt::bzpopmin, key, timeout);
    }

    template <typename Input>
    auto bzpopmin(Input first,
                    Input last,
                    const std::chrono::seconds &timeout = std::chrono::seconds{0})
        -> Future<Optional<std::tuple<std::string, std::string, double>>> {
        range_check("BZPOPMIN", first, last);

        return _command<Optional<std::tuple<std::string, std::string, double>>>(
                fmt::bzpopmin_range<Input>, first, last, timeout);
    }

    template <typename T>
    auto bzpopmin(std::initializer_list<T> il,
                    const std::chrono::seconds &timeout = std::chrono::seconds{0})
        -> Future<Optional<std::tuple<std::string, std::string, double>>> {
        return bzpopmin(il.begin(), il.end(), timeout);
    }

    Future<long long> zadd(const StringView &key,
                    const StringView &member,
                    double score,
                    UpdateType type = UpdateType::ALWAYS,
                    bool changed = false) {
        return _command<long long>(fmt::zadd, key, member, score, type, changed);
    }

    template <typename Callback>
    auto zadd(const StringView &key,
              const StringView &member,
              double score,
              UpdateType type,
              bool changed,
              Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<long long> &&>::value, void>::type {
        _callback_fmt_command<long long>(std::forward<Callback>(cb), fmt::zadd, key, member, score, type, changed);
    }

    template <typename Callback>
    auto zadd(const StringView &key,
              const StringView &member,
              double score,
              Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<long long> &&>::value, void>::type {
        return zadd(key, member, score, UpdateType::ALWAYS, false, std::forward<Callback>(cb));
    }

    template <typename Input>
    Future<long long> zadd(const StringView &key,
                    Input first,
                    Input last,
                    UpdateType type = UpdateType::ALWAYS,
                    bool changed = false) {
        range_check("ZADD", first, last);

        return _command<long long>(fmt::zadd_range<Input>,
                key,
                first,
                last,
                type,
                changed);
    }

    template <typename Input, typename Callback>
    auto zadd(const StringView &key,
              Input first,
              Input last,
              UpdateType type,
              bool changed,
              Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<long long> &&>::value, void>::type {
        range_check("ZADD", first, last);

        _callback_fmt_command<long long>(std::forward<Callback>(cb), fmt::zadd_range<Input>, key, first, last, type, changed);
    }

    template <typename Input, typename Callback>
    auto zadd(const StringView &key,
              Input first,
              Input last,
              Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<long long> &&>::value, void>::type {
        return zadd(key, first, last, UpdateType::ALWAYS, false, std::forward<Callback>(cb));
    }

    template <typename T>
    Future<long long> zadd(const StringView &key,
                    std::initializer_list<T> il,
                    UpdateType type = UpdateType::ALWAYS,
                    bool changed = false) {
        return zadd(key, il.begin(), il.end(), type, changed);
    }

    template <typename T, typename Callback>
    auto zadd(const StringView &key,
              std::initializer_list<T> il,
              UpdateType type,
              bool changed,
              Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<long long> &&>::value, void>::type {
        return zadd<decltype(il.begin()), Callback>(key, il.begin(), il.end(), type, changed, std::forward<Callback>(cb));
    }

    template <typename T, typename Callback>
    auto zadd(const StringView &key,
              std::initializer_list<T> il,
              Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<long long> &&>::value, void>::type {
        return zadd<T, Callback>(key, il, UpdateType::ALWAYS, false, std::forward<Callback>(cb));
    }

    Future<long long> zcard(const StringView &key) {
        return _command<long long>(fmt::zcard, key);
    }

    template <typename Callback>
    auto zcard(const StringView &key, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<long long> &&>::value, void>::type {
        _callback_fmt_command<long long>(std::forward<Callback>(cb), fmt::zcard, key);
    }

    template <typename Interval>
    Future<long long> zcount(const StringView &key, const Interval &interval) {
        return _command<long long>(fmt::zcount<Interval>, key, interval);
    }

    template <typename Interval, typename Callback>
    auto zcount(const StringView &key, const Interval &interval, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<long long> &&>::value, void>::type {
        _callback_fmt_command<long long>(std::forward<Callback>(cb), fmt::zcount<Interval>, key, interval);
    }

    Future<double> zincrby(const StringView &key, double increment, const StringView &member) {
        return _command<double>(fmt::zincrby, key, increment, member);
    }

    template <typename Callback>
    auto zincrby(const StringView &key, double increment, const StringView &member, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<double> &&>::value, void>::type {
        _callback_fmt_command<double>(std::forward<Callback>(cb), fmt::zincrby, key, increment, member);
    }

    template <typename Interval>
    Future<long long> zlexcount(const StringView &key, const Interval &interval) {
        return _command<long long>(fmt::zlexcount<Interval>, key, interval);
    }

    template <typename Interval, typename Callback>
    auto zlexcount(const StringView &key, const Interval &interval, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<long long> &&>::value, void>::type {
        _callback_fmt_command<long long>(std::forward<Callback>(cb), fmt::zlexcount<Interval>, key, interval);
    }

    Future<Optional<std::pair<std::string, double>>> zpopmax(const StringView &key) {
        return _command<Optional<std::pair<std::string, double>>,
                FormattedCommand (*)(const StringView &)>(fmt::zpopmax, key);
    }

    template <typename Output>
    Future<Output> zpopmax(const StringView &key, long long count) {
        return _command<Output, FormattedCommand (*)(const StringView &, long long)>(
                fmt::zpopmax, key, count);
    }

    Future<Optional<std::pair<std::string, double>>> zpopmin(const StringView &key) {
        return _command<Optional<std::pair<std::string, double>>,
                FormattedCommand (*)(const StringView &)>(fmt::zpopmin, key);
    }

    template <typename Output>
    Future<Output> zpopmin(const StringView &key, long long count) {
        return _command<Output, FormattedCommand (*)(const StringView &, long long)>(
                fmt::zpopmin, key, count);
    }

    template <typename Output>
    Future<Output> zrange(const StringView &key, long long start, long long stop) {
        return _command<Output>(fmt::zrange, key, start, stop);
    }

    template <typename Output, typename Callback>
    auto zrange(const StringView &key, long long start, long long stop, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<Output> &&>::value, void>::type {
        _callback_fmt_command<Output>(std::forward<Callback>(cb), fmt::zrange, key, start, stop);
    }

    template <typename Output, typename Interval>
    Future<Output> zrangebylex(const StringView &key,
                        const Interval &interval,
                        const LimitOptions &opts) {
        return _command<Output>(fmt::zrangebylex<Interval>,
                key, interval, opts);
    }

    template <typename Output, typename Interval, typename Callback>
    auto zrangebylex(const StringView &key, const Interval &interval, const LimitOptions &opts, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<Output> &&>::value, void>::type {
        _callback_fmt_command<Output>(std::forward<Callback>(cb), fmt::zrangebylex<Interval>, key, interval, opts);
    }

    template <typename Output, typename Interval>
    Future<Output> zrangebylex(const StringView &key, const Interval &interval) {
        return zrangebylex<Output>(key, interval, {});
    }

    template <typename Output, typename Interval, typename Callback>
    auto zrangebylex(const StringView &key, const Interval &interval, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<Output> &&>::value, void>::type {
        return zrangebylex<Output>(key, interval, {}, std::forward<Callback>(cb));
    }

    // TODO: withscores parameter
    template <typename Output, typename Interval>
    Future<Output> zrangebyscore(const StringView &key,
                        const Interval &interval,
                        const LimitOptions &opts) {
        return _command<Output>(fmt::zrangebyscore<Interval>,
                key, interval, opts);
    }

    template <typename Output, typename Interval, typename Callback>
    auto zrangebyscore(const StringView &key, const Interval &interval, const LimitOptions &opts, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<Output> &&>::value, void>::type {
        _callback_fmt_command<Output>(std::forward<Callback>(cb), fmt::zrangebyscore<Interval>, key, interval, opts);
    }

    template <typename Output, typename Interval>
    Future<Output> zrangebyscore(const StringView &key, const Interval &interval) {
        return zrangebyscore(key, interval, {});
    }

    template <typename Output, typename Interval, typename Callback>
    auto zrangebyscore(const StringView &key, const Interval &interval, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<Output> &&>::value, void>::type {
        return zrangebyscore<Output>(key, interval, {}, std::forward<Callback>(cb));
    }

    Future<OptionalLongLong> zrank(const StringView &key, const StringView &member) {
        return _command<OptionalLongLong>(fmt::zrank, key, member);
    }

    template <typename Callback>
    auto zrank(const StringView &key, const StringView &member, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<OptionalLongLong> &&>::value, void>::type {
        _callback_fmt_command<OptionalLongLong>(std::forward<Callback>(cb), fmt::zrank, key, member);
    }

    Future<long long> zrem(const StringView &key, const StringView &member) {
        return _command<long long>(fmt::zrem, key, member);
    }

    template <typename Callback>
    auto zrem(const StringView &key, const StringView &member, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<long long> &&>::value, void>::type {
        _callback_fmt_command<long long>(std::forward<Callback>(cb), fmt::zrem, key, member);
    }

    template <typename Input>
    Future<long long> zrem(const StringView &key, Input first, Input last) {
        range_check("ZREM", first, last);

        return _command<long long>(fmt::zrem_range<Input>, key, first, last);
    }

    template <typename Input, typename Callback>
    auto zrem(const StringView &key, Input first, Input last, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<long long> &&>::value, void>::type {
        range_check("ZREM", first, last);

        _callback_fmt_command<long long>(std::forward<Callback>(cb), fmt::zrem_range<Input>, key, first, last);
    }

    template <typename T>
    Future<long long> zrem(const StringView &key, std::initializer_list<T> il) {
        return zrem(key, il.begin(), il.end());
    }

    template <typename T, typename Callback>
    auto zrem(const StringView &key, std::initializer_list<T> il, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<long long> &&>::value, void>::type {
        return zrem<decltype(il.begin()), Callback>(key, il.begin(), il.end(), std::forward<Callback>(cb));
    }

    template <typename Interval>
    Future<long long> zremrangebylex(const StringView &key, const Interval &interval) {
        return _command<long long>(fmt::zremrangebylex<Interval>, key, interval);
    }

    template <typename Interval, typename Callback>
    auto zremrangebylex(const StringView &key, const Interval &interval, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<long long> &&>::value, void>::type {
        _callback_fmt_command<long long>(std::forward<Callback>(cb), fmt::zremrangebylex<Interval>, key, interval);
    }

    Future<long long> zremrangebyrank(const StringView &key, long long start, long long stop) {
        return _command<long long>(fmt::zremrangebyrank, key, start, stop);
    }

    template <typename Callback>
    auto zremrangebyrank(const StringView &key, long long start, long long stop, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<long long> &&>::value, void>::type {
        _callback_fmt_command<long long>(std::forward<Callback>(cb), fmt::zremrangebyrank, key, start, stop);
    }

    template <typename Interval>
    Future<long long> zremrangebyscore(const StringView &key, const Interval &interval) {
        return _command<long long>(fmt::zremrangebyscore<Interval>, key, interval);
    }

    template <typename Interval, typename Callback>
    auto zremrangebyscore(const StringView &key, const Interval &interval, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<long long> &&>::value, void>::type {
        _callback_fmt_command<long long>(std::forward<Callback>(cb), fmt::zremrangebyscore<Interval>, key, interval);
    }

    template <typename Output, typename Interval>
    Future<Output> zrevrangebylex(const StringView &key,
                        const Interval &interval,
                        const LimitOptions &opts) {
        return _command<Output>(fmt::zrevrangebylex<Interval>,
                key, interval, opts);
    }

    template <typename Output, typename Interval, typename Callback>
    auto zrevrangebylex(const StringView &key, const Interval &interval, const LimitOptions &opts, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<Output> &&>::value, void>::type {
        _callback_fmt_command<Output>(std::forward<Callback>(cb), fmt::zrevrangebylex<Interval>, key, interval, opts);
    }

    template <typename Output, typename Interval>
    Future<Output> zrevrangebylex(const StringView &key, const Interval &interval) {
        return zrevrangebylex<Output>(key, interval, {});
    }

    template <typename Output, typename Interval, typename Callback>
    auto zrevrangebylex(const StringView &key, const Interval &interval, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<Output> &&>::value, void>::type {
        return zrevrangebylex<Output, Interval, Callback>(key, interval, {}, std::forward<Callback>(cb));
    }

    Future<OptionalLongLong> zrevrank(const StringView &key, const StringView &member) {
        return _command<OptionalLongLong>(fmt::zrevrank, key, member);
    }

    template <typename Callback>
    auto zrevrank(const StringView &key, const StringView &member, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<OptionalLongLong> &&>::value, void>::type {
        _callback_fmt_command<OptionalLongLong>(std::forward<Callback>(cb), fmt::zrevrank, key, member);
    }

    Future<OptionalDouble> zscore(const StringView &key, const StringView &member) {
        return _command<OptionalDouble>(fmt::zscore, key, member);
    }

    template <typename Callback>
    auto zscore(const StringView &key, const StringView &member, Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<OptionalDouble> &&>::value, void>::type {
        _callback_fmt_command<OptionalDouble>(std::forward<Callback>(cb), fmt::zscore, key, member);
    }

    // SCRIPTING commands.

    template <typename Result, typename Keys, typename Args>
    Future<Result> eval(const StringView &script,
                Keys keys_first,
                Keys keys_last,
                Args args_first,
                Args args_last) {
        return _command<Result>(fmt::eval<Keys, Args>,
                script, keys_first, keys_last, args_first, args_last);
    }

    template <typename Result, typename Keys, typename Args, typename Callback>
    auto eval(const StringView &script,
              Keys keys_first,
              Keys keys_last,
              Args args_first,
              Args args_last,
              Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<Result> &&>::value, void>::type {
        _callback_fmt_command<Result>(std::forward<Callback>(cb), fmt::eval<Keys, Args>,
                                      script, keys_first, keys_last, args_first, args_last);
    }

    template <typename Result>
    Future<Result> eval(const StringView &script,
                std::initializer_list<StringView> keys,
                std::initializer_list<StringView> args) {
        return eval<Result>(script,
                keys.begin(), keys.end(),
                args.begin(), args.end());
    }

    template <typename Result, typename Callback>
    auto eval(const StringView &script,
              std::initializer_list<StringView> keys,
              std::initializer_list<StringView> args,
              Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<Result> &&>::value, void>::type {
        return eval<Result>(script, keys.begin(), keys.end(), args.begin(), args.end(), std::forward<Callback>(cb));
    }

    template <typename Result, typename Keys, typename Args>
    Future<Result> evalsha(const StringView &script,
                    Keys keys_first,
                    Keys keys_last,
                    Args args_first,
                    Args args_last) {
        return _command<Result>(fmt::evalsha<Keys, Args>,
                script, keys_first, keys_last, args_first, args_last);
    }

    template <typename Result, typename Keys, typename Args, typename Callback>
    auto evalsha(const StringView &script,
                 Keys keys_first,
                 Keys keys_last,
                 Args args_first,
                 Args args_last,
                 Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<Result> &&>::value, void>::type {
        _callback_fmt_command<Result>(std::forward<Callback>(cb), fmt::evalsha<Keys, Args>,
                                      script, keys_first, keys_last, args_first, args_last);
    }

    template <typename Result>
    Future<Result> evalsha(const StringView &script,
                    std::initializer_list<StringView> keys,
                    std::initializer_list<StringView> args) {
        return evalsha<Result>(script,
                keys.begin(), keys.end(),
                args.begin(), args.end());
    }

    template <typename Result, typename Callback>
    auto evalsha(const StringView &script,
                 std::initializer_list<StringView> keys,
                 std::initializer_list<StringView> args,
                 Callback &&cb)
        -> typename std::enable_if<IsInvocable<typename std::decay<Callback>::type, Future<Result> &&>::value, void>::type {
        return evalsha<Result>(script, keys.begin(), keys.end(), args.begin(), args.end(), std::forward<Callback>(cb));
    }

    // PUBSUB commands.

    Future<long long> publish(const StringView &channel, const StringView &message) {
        return _command<long long>(fmt::publish, channel, message);
    }

    template <typename Callback>
    void publish(const StringView &channel, const StringView &message, Callback &&cb) {
        _callback_fmt_command<long long>(std::forward<Callback>(cb), fmt::publish, channel, message);
    }

    // co_command* are used internally. DO NOT use them.

    template <typename Result, typename Callback>
    void co_command(FormattedCommand cmd, Callback &&cb) {
        return co_command_with_parser<Result, DefaultResultParser<Result>, Callback>(
                std::move(cmd), std::forward<Callback>(cb));
    }

    template <typename Result, typename ResultParser, typename Callback>
    void co_command_with_parser(FormattedCommand cmd, Callback &&cb) {
        assert(_pool);
        SafeAsyncConnection connection(*_pool);

        connection.connection().send<Result, ResultParser, Callback>(
                std::move(cmd), std::forward<Callback>(cb));
    }

private:
    friend class AsyncRedisCluster;

    explicit AsyncRedis(const GuardedAsyncConnectionSPtr &connection);

    explicit AsyncRedis(const Uri &uri);

    template <typename Result, typename Formatter, typename ...Args>
    Future<Result> _command(Formatter formatter, Args &&...args) {
        return _command_with_parser<Result, DefaultResultParser<Result>>(
                formatter, std::forward<Args>(args)...);
    }

    template <typename Result, typename ResultParser, typename Formatter, typename ...Args>
    Future<Result> _command_with_parser(Formatter formatter, Args &&...args) {
        auto formatted_cmd = formatter(std::forward<Args>(args)...);

        if (_connection) {
            // Single connection mode.
            auto &connection = _connection->connection();
            if (connection.broken()) {
                throw Error("connection is broken");
            }

            return connection.send<Result, ResultParser>(std::move(formatted_cmd));
        } else {
            assert(_pool);
            SafeAsyncConnection connection(*_pool);

            return connection.connection().send<Result, ResultParser>(std::move(formatted_cmd));
        }
    }

    template <typename Result, typename Callback, std::size_t ...Is, typename ...Args>
    void _callback_idx_command(Callback &&cb, const StringView &cmd_name,
            const IndexSequence<Is...> &, Args &&...args) {
        _callback_command<Result>(std::forward<Callback>(cb), cmd_name,
                NthValue<Is>(std::forward<Args>(args)...)...);
    }

    template <typename Result, typename Callback, typename ...Args>
    void _callback_command(Callback &&cb, const StringView &cmd_name, Args &&...args) {
        auto formatter = [](const StringView &name, Args &&...params) {
            CmdArgs cmd_args;
            cmd_args.append(name, std::forward<Args>(params)...);
            return fmt::format_cmd(cmd_args);
        };

        _callback_fmt_command<Result>(std::forward<Callback>(cb), formatter, cmd_name,
                std::forward<Args>(args)...);
    }

    template <typename Result, typename Callback, typename Formatter, typename ...Args>
    void _callback_fmt_command(Callback &&cb, Formatter formatter, Args &&...args) {
        _callback_command_with_parser<Result, DefaultResultParser<Result>, Callback>(
                std::forward<Callback>(cb), formatter, std::forward<Args>(args)...);
    }

    template <typename Result, typename ResultParser, typename Callback,
             typename Formatter, typename ...Args>
    void _callback_command_with_parser(Callback &&cb, Formatter formatter, Args &&...args) {
        auto formatted_cmd = formatter(std::forward<Args>(args)...);

        if (_connection) {
            // Single connection mode.
            auto &connection = _connection->connection();
            if (connection.broken()) {
                throw Error("connection is broken");
            }

            connection.send<Result, ResultParser, Callback>(
                    std::move(formatted_cmd), std::forward<Callback>(cb));
        } else {
            assert(_pool);
            SafeAsyncConnection connection(*_pool);

            connection.connection().send<Result, ResultParser, Callback>(
                    std::move(formatted_cmd), std::forward<Callback>(cb));
        }
    }

    EventLoopSPtr _loop;

    AsyncConnectionPoolSPtr _pool;

    GuardedAsyncConnectionSPtr _connection;
};

}

}

#endif // end SEWENEW_REDISPLUSPLUS_ASYNC_REDIS_H
