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
#include "async_connection_pool.h"
#include "async_sentinel.h"
#include "event_loop.h"
#include "utils.h"
#include "command.h"
#include "command_args.h"
#include "command_options.h"
#include "async_command.h"

namespace sw {

namespace redis {

class AsyncRedis {
public:
    AsyncRedis(const ConnectionOptions &opts,
            const ConnectionPoolOptions &pool_opts = {},
            const EventLoopSPtr &loop = nullptr);

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

    template <typename Result, typename ...Args>
    Future<Result> command(const StringView &cmd_name, Args &&...args) {
        CmdArgs cmd_args;
        cmd_args.append(cmd_name, std::forward<Args>(args)...);

        assert(_pool);
        SafeAsyncConnection connection(*_pool);

        return connection.connection().send<Result>(cmd_args);
    }

    template <typename Result, typename Input>
    auto command(Input first, Input last)
        -> typename std::enable_if<IsIter<Input>::value, Future<Result>>::type {
        CmdArgs cmd_args;
        while (first != last) {
            cmd_args.append(*first);
            ++first;
        }

        assert(_pool);
        SafeAsyncConnection connection(*_pool);

        return connection.connection().send<Result>(cmd_args);
    }

    // CONNECTION commands.

    Future<std::string> echo(const StringView &msg) {
        return _command<std::string>(async_cmd::echo, msg);
    }

    Future<std::string> ping() {
        return _command<std::string, Future<std::string> (*)(AsyncConnection &)>(async_cmd::ping);
    }

    Future<std::string> ping(const StringView &msg) {
        return _command<std::string,
                        Future<std::string> (*)(AsyncConnection &, const StringView &)>(
                                async_cmd::ping, msg);
    }

    // KEY commands.

    Future<long long> del(const StringView &key) {
        return _command<long long>(async_cmd::del, key);
    }

    template <typename Input>
    Future<long long> del(Input first, Input last) {
        range_check("DEL", first, last);

        return _command<long long>(async_cmd::del_range<Input>, first, last);
    }

    template <typename T>
    Future<long long> del(std::initializer_list<T> il) {
        return del(il.begin(), il.end());
    }

    Future<long long> exists(const StringView &key) {
        return _command<long long>(async_cmd::exists, key);
    }

    template <typename Input>
    Future<long long> exists(Input first, Input last) {
        range_check("EXISTS", first, last);

        return _command<long long>(async_cmd::exists_range<Input>, first, last);
    }

    template <typename T>
    Future<long long> exists(std::initializer_list<T> il) {
        return exists(il.begin(), il.end());
    }

    Future<bool> expire(const StringView &key, const std::chrono::seconds &timeout) {
        return _command<bool>(async_cmd::expire, key, timeout);
    }

    Future<bool> expireat(const StringView &key,
                    const std::chrono::time_point<std::chrono::system_clock,
                                                    std::chrono::seconds> &tp) {
        return _command<bool>(async_cmd::expireat, key, tp);
    }

    Future<bool> pexpire(const StringView &key, const std::chrono::milliseconds &timeout) {
        return _command<bool>(async_cmd::pexpire, key, timeout);
    }

    Future<bool> pexpireat(const StringView &key,
                    const std::chrono::time_point<std::chrono::system_clock,
                                                    std::chrono::milliseconds> &tp) {
        return _command<bool>(async_cmd::pexpireat, key, tp);
    }

    Future<long long> pttl(const StringView &key) {
        return _command<long long>(async_cmd::pttl, key);
    }

    Future<void> rename(const StringView &key, const StringView &newkey) {
        return _command<void>(async_cmd::rename, key, newkey);
    }

    Future<bool> renamenx(const StringView &key, const StringView &newkey) {
        return _command<bool>(async_cmd::renamenx, key, newkey);
    }

    Future<long long> ttl(const StringView &key) {
        return _command<long long>(async_cmd::ttl, key);
    }

    Future<long long> unlink(const StringView &key) {
        return _command<long long>(async_cmd::unlink, key);
    }

    template <typename Input>
    Future<long long> unlink(Input first, Input last) {
        range_check("UNLINK", first, last);

        return _command<long long>(async_cmd::unlink_range<Input>, first, last);
    }

    template <typename T>
    Future<long long> unlink(std::initializer_list<T> il) {
        return unlink(il.begin(), il.end());
    }

    // STRING commands.

    Future<OptionalString> get(const StringView &key) {
        return _command<OptionalString>(async_cmd::get, key);
    }

    Future<long long> incr(const StringView &key) {
        return _command<long long>(async_cmd::incr, key);
    }

    Future<long long> incrby(const StringView &key, long long increment) {
        return _command<long long>(async_cmd::incrby, key, increment);
    }

    Future<double> incrbyfloat(const StringView &key, double increment) {
        return _command<double>(async_cmd::incrbyfloat, key, increment);
    }

    template <typename Output, typename Input>
    Future<Output> mget(Input first, Input last) {
        range_check("MGET", first, last);

        return _command<Output>(async_cmd::mget<Output, Input>, first, last);
    }

    template <typename Output, typename T>
    Future<Output> mget(std::initializer_list<T> il) {
        return mget<Output>(il.begin(), il.end());
    }

    template <typename Input>
    Future<void> mset(Input first, Input last) {
        range_check("MSET", first, last);

        return _command<void>(async_cmd::mset<Input>, first, last);
    }

    template <typename T>
    Future<void> mset(std::initializer_list<T> il) {
        return mset(il.begin(), il.end());
    }

    template <typename Input>
    Future<bool> msetnx(Input first, Input last) {
        range_check("MSETNX", first, last);

        return _command<bool>(async_cmd::msetnx<Input>, first, last);
    }

    template <typename T>
    Future<bool> msetnx(std::initializer_list<T> il) {
        return msetnx(il.begin(), il.end());
    }

    Future<bool> set(const StringView &key,
                const StringView &val,
                const std::chrono::milliseconds &ttl = std::chrono::milliseconds(0),
                UpdateType type = UpdateType::ALWAYS) {
        return _command<bool>(async_cmd::set, key, val, ttl, type);
    }

    Future<long long> strlen(const StringView &key) {
        return _command<long long>(async_cmd::strlen, key);
    }

    // LIST commands.

    Future<OptionalStringPair> blpop(const StringView &key,
                                const std::chrono::seconds &timeout = std::chrono::seconds{0}) {
        return _command<OptionalStringPair>(async_cmd::blpop, key, timeout);
    }

    template <typename Input>
    Future<OptionalStringPair> blpop(Input first,
                                Input last,
                                const std::chrono::seconds &timeout = std::chrono::seconds{0}) {
        range_check("BLPOP", first, last);

        return _command<OptionalStringPair>(async_cmd::blpop_range<Input>, first, last, timeout);
    }

    template <typename T>
    Future<OptionalStringPair> blpop(std::initializer_list<T> il,
                                const std::chrono::seconds &timeout = std::chrono::seconds{0}) {
        return blpop(il.begin(), il.end(), timeout);
    }

    Future<OptionalStringPair> brpop(const StringView &key,
                                const std::chrono::seconds &timeout = std::chrono::seconds{0}) {
        return _command<OptionalStringPair>(async_cmd::brpop, key, timeout);
    }

    template <typename Input>
    Future<OptionalStringPair> brpop(Input first,
                                Input last,
                                const std::chrono::seconds &timeout = std::chrono::seconds{0}) {
        range_check("BRPOP", first, last);

        return _command<OptionalStringPair>(async_cmd::brpop_range<Input>, first, last, timeout);
    }

    template <typename T>
    Future<OptionalStringPair> brpop(std::initializer_list<T> il,
                                const std::chrono::seconds &timeout = std::chrono::seconds{0}) {
        return brpop(il.begin(), il.end(), timeout);
    }

    Future<OptionalString> brpoplpush(const StringView &source,
                                const StringView &destination,
                                const std::chrono::seconds &timeout = std::chrono::seconds{0}) {
        return _command<OptionalString>(async_cmd::brpoplpush, source, destination, timeout);
    }

    Future<long long> llen(const StringView &key) {
        return _command<long long>(async_cmd::llen, key);
    }

    Future<OptionalString> lpop(const StringView &key) {
        return _command<OptionalString>(async_cmd::lpop, key);
    }

    Future<long long> lpush(const StringView &key, const StringView &val) {
        return _command<long long>(async_cmd::lpush, key, val);
    }

    template <typename Input>
    Future<long long> lpush(const StringView &key, Input first, Input last) {
        range_check("LPUSH", first, last);

        return _command<long long>(async_cmd::lpush_range<Input>, key, first, last);
    }

    template <typename T>
    Future<long long> lpush(const StringView &key, std::initializer_list<T> il) {
        return lpush(key, il.begin(), il.end());
    }

    template <typename Output>
    Future<Output> lrange(const StringView &key, long long start, long long stop) {
        return _command<Output>(async_cmd::lrange<Output>, key, start, stop);
    }

    Future<long long> lrem(const StringView &key, long long count, const StringView &val) {
        return _command<long long>(async_cmd::lrem, key, count, val);
    }

    Future<void> ltrim(const StringView &key, long long start, long long stop) {
        return _command<void>(async_cmd::ltrim, key, start, stop);
    }

    Future<OptionalString> rpop(const StringView &key) {
        return _command<OptionalString>(async_cmd::rpop, key);
    }

    Future<OptionalString> rpoplpush(const StringView &source, const StringView &destination) {
        return _command<OptionalString>(async_cmd::rpoplpush, source, destination);
    }

    Future<long long> rpush(const StringView &key, const StringView &val) {
        return _command<long long>(async_cmd::rpush, key, val);
    }

    template <typename Input>
    Future<long long> rpush(const StringView &key, Input first, Input last) {
        range_check("RPUSH", first, last);

        return _command<long long>(async_cmd::rpush_range<Input>, key, first, last);
    }

    template <typename T>
    Future<long long> rpush(const StringView &key, std::initializer_list<T> il) {
        return rpush(key, il.begin(), il.end());
    }

    // HASH commands.

    Future<long long> hdel(const StringView &key, const StringView &field) {
        return _command<long long>(async_cmd::hdel, key, field);
    }

    template <typename Input>
    Future<long long> hdel(const StringView &key, Input first, Input last) {
        range_check("HDEL", first, last);

        return _command<long long>(async_cmd::hdel_range<Input>, key, first, last);
    }

    template <typename T>
    Future<long long> hdel(const StringView &key, std::initializer_list<T> il) {
        return hdel(key, il.begin(), il.end());
    }

    Future<bool> hexists(const StringView &key, const StringView &field) {
        return _command<bool>(async_cmd::hexists, key, field);
    }

    Future<OptionalString> hget(const StringView &key, const StringView &field) {
        return _command<OptionalString>(async_cmd::hget, key, field);
    }

    template <typename Output>
    Future<Output> hgetall(const StringView &key) {
        return _command<Output>(async_cmd::hgetall<Output>, key);
    }

    Future<long long> hincrby(const StringView &key, const StringView &field, long long increment) {
        return _command<long long>(async_cmd::hincrby, key, field, increment);
    }

    Future<double> hincrbyfloat(const StringView &key, const StringView &field, double increment) {
        return _command<double>(async_cmd::hincrbyfloat, key, field, increment);
    }

    template <typename Output>
    Future<Output> hkeys(const StringView &key) {
        return _command<Output>(async_cmd::hkeys<Output>, key);
    }

    Future<long long> hlen(const StringView &key) {
        return _command<long long>(async_cmd::hlen, key);
    }

    template <typename Output, typename Input>
    Future<Output> hmget(const StringView &key, Input first, Input last) {
        range_check("HMGET", first, last);

        return _command<Output>(async_cmd::hmget<Output, Input>, key, first, last);
    }

    template <typename Output, typename T>
    Future<Output> hmget(const StringView &key, std::initializer_list<T> il) {
        return hmget<Output>(key, il.begin(), il.end());
    }

    template <typename Input>
    Future<void> hmset(const StringView &key, Input first, Input last) {
        range_check("HMSET", first, last);

        return _command<void>(async_cmd::hmset<Input>, key, first, last);
    }

    template <typename T>
    Future<void> hmset(const StringView &key, std::initializer_list<T> il) {
        return hmset(key, il.begin(), il.end());
    }

    Future<bool> hset(const StringView &key, const StringView &field, const StringView &val) {
        return _command<bool>(async_cmd::hset, key, field, val);
    }

    Future<bool> hset(const StringView &key, const std::pair<StringView, StringView> &item) {
        return hset(key, item.first, item.second);
    }

    template <typename Input>
    auto hset(const StringView &key, Input first, Input last)
        -> typename std::enable_if<!std::is_convertible<Input, StringView>::value, Future<long long>>::type {
        range_check("HSET", first, last);

        return _command<long long>(async_cmd::hset_range<Input>, key, first, last);
    }

    template <typename T>
    Future<long long> hset(const StringView &key, std::initializer_list<T> il) {
        return hset(key, il.begin(), il.end());
    }

    template <typename Output>
    Future<Output> hvals(const StringView &key) {
        return _command<Output>(async_cmd::hvals<Output>, key);
    }

    // SET commands.

    Future<long long> sadd(const StringView &key, const StringView &member) {
        return _command<long long>(async_cmd::sadd, key, member);
    }

    template <typename Input>
    Future<long long> sadd(const StringView &key, Input first, Input last) {
        range_check("SADD", first, last);

        return _command<long long>(async_cmd::sadd_range<Input>, key, first, last);
    }

    template <typename T>
    Future<long long> sadd(const StringView &key, std::initializer_list<T> il) {
        return sadd(key, il.begin(), il.end());
    }

    Future<long long> scard(const StringView &key) {
        return _command<long long>(async_cmd::scard, key);
    }

    Future<bool> sismember(const StringView &key, const StringView &member) {
        return _command<bool>(async_cmd::sismember, key, member);
    }

    template <typename Output>
    Future<Output> smembers(const StringView &key) {
        return _command<Output>(async_cmd::smembers<Output>, key);
    }

    Future<OptionalString> spop(const StringView &key) {
        return _command<OptionalString>(async_cmd::spop, key);
    }

    template <typename Output>
    Future<Output> spop(const StringView &key, long long count) {
        return _command<Output>(async_cmd::spop_count<Output>, key, count);
    }

    Future<long long> srem(const StringView &key, const StringView &member) {
        return _command<long long>(async_cmd::srem, key, member);
    }

    template <typename Input>
    Future<long long> srem(const StringView &key, Input first, Input last) {
        range_check("SREM", first, last);

        return _command<long long>(async_cmd::srem_range<Input>, key, first, last);
    }

    template <typename T>
    Future<long long> srem(const StringView &key, std::initializer_list<T> il) {
        return srem(key, il.begin(), il.end());
    }

    // SORTED SET commands.

    auto bzpopmax(const StringView &key,
                    const std::chrono::seconds &timeout = std::chrono::seconds{0})
        -> Future<Optional<std::tuple<std::string, std::string, double>>> {
        return _command<Optional<std::tuple<std::string, std::string, double>>>(
                async_cmd::bzpopmax, key, timeout);
    }

    template <typename Input>
    auto bzpopmax(Input first,
                    Input last,
                    const std::chrono::seconds &timeout = std::chrono::seconds{0})
        -> Future<Optional<std::tuple<std::string, std::string, double>>> {
        range_check("BZPOPMAX", first, last);

        return _command<Optional<std::tuple<std::string, std::string, double>>>(
                async_cmd::bzpopmax_range<Input>, first, last, timeout);
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
                async_cmd::bzpopmin, key, timeout);
    }

    template <typename Input>
    auto bzpopmin(Input first,
                    Input last,
                    const std::chrono::seconds &timeout = std::chrono::seconds{0})
        -> Future<Optional<std::tuple<std::string, std::string, double>>> {
        range_check("BZPOPMIN", first, last);

        return _command<Optional<std::tuple<std::string, std::string, double>>>(
                async_cmd::bzpopmin_range<Input>, first, last, timeout);
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
        return _command<long long>(async_cmd::zadd, key, member, score, type, changed);
    }

    template <typename Input>
    Future<long long> zadd(const StringView &key,
                    Input first,
                    Input last,
                    UpdateType type = UpdateType::ALWAYS,
                    bool changed = false) {
        range_check("ZADD", first, last);

        return _command<long long>(async_cmd::zadd_range<Input>,
                key,
                first,
                last,
                type,
                changed);
    }

    template <typename T>
    Future<long long> zadd(const StringView &key,
                    std::initializer_list<T> il,
                    UpdateType type = UpdateType::ALWAYS,
                    bool changed = false) {
        return zadd(key, il.begin(), il.end(), type, changed);
    }

    Future<long long> zcard(const StringView &key) {
        return _command<long long>(async_cmd::zcard, key);
    }

    template <typename Interval>
    Future<long long> zcount(const StringView &key, const Interval &interval) {
        return _command<long long>(async_cmd::zcount<Interval>, key, interval);
    }

    Future<double> zincrby(const StringView &key, double increment, const StringView &member) {
        return _command<double>(async_cmd::zincrby, key, increment, member);
    }

    template <typename Interval>
    Future<long long> zlexcount(const StringView &key, const Interval &interval) {
        return _command<long long>(async_cmd::zlexcount<Interval>, key, interval);
    }

    Future<Optional<std::pair<std::string, double>>> zpopmax(const StringView &key) {
        return _command<Optional<std::pair<std::string, double>>>(async_cmd::zpopmax, key);
    }

    template <typename Output>
    Future<Output> zpopmax(const StringView &key, long long count) {
        return _command<Output>(async_cmd::zpopmax_count<Output>, key, count);
    }

    Future<Optional<std::pair<std::string, double>>> zpopmin(const StringView &key) {
        return _command<Optional<std::pair<std::string, double>>>(async_cmd::zpopmin, key);
    }

    template <typename Output>
    Future<Output> zpopmin(const StringView &key, long long count) {
        return _command<Output>(async_cmd::zpopmin_count<Output>, key, count);
    }

    template <typename Output>
    Future<Output> zrange(const StringView &key, long long start, long long stop) {
        return _command<Output>(async_cmd::zrange<Output>, key, start, stop);
    }

    template <typename Output, typename Interval>
    Future<Output> zrangebylex(const StringView &key,
                        const Interval &interval,
                        const LimitOptions &opts) {
        return _command<Output>(async_cmd::zrangebylex<Output, Interval>,
                key, interval, opts);
    }

    template <typename Output, typename Interval>
    Future<Output> zrangebylex(const StringView &key, const Interval &interval) {
        return zrangebylex<Output>(key, interval, {});
    }

    // TODO: withscores parameter
    template <typename Output, typename Interval>
    Future<Output> zrangebyscore(const StringView &key,
                        const Interval &interval,
                        const LimitOptions &opts) {
        return _command<Output>(async_cmd::zrangebyscore<Output, Interval>,
                key, interval, opts);
    }

    template <typename Output, typename Interval>
    Future<Output> zrangebyscore(const StringView &key, const Interval &interval) {
        return zrangebyscore(key, interval, {});
    }

    Future<OptionalLongLong> rank(const StringView &key, const StringView &member) {
        return _command<OptionalLongLong>(async_cmd::rank, key, member);
    }

    Future<long long> zrem(const StringView &key, const StringView &member) {
        return _command<long long>(async_cmd::zrem, key, member);
    }

    template <typename Input>
    Future<long long> zrem(const StringView &key, Input first, Input last) {
        range_check("ZREM", first, last);

        return _command<long long>(async_cmd::zrem_range<Input>, key, first, last);
    }

    template <typename T>
    Future<long long> zrem(const StringView &key, std::initializer_list<T> il) {
        return zrem(key, il.begin(), il.end());
    }

    template <typename Interval>
    Future<long long> zremrangebylex(const StringView &key, const Interval &interval) {
        return _command<long long>(async_cmd::zremrangebylex<Interval>, key, interval);
    }

    Future<long long> zremrangebyrank(const StringView &key, long long start, long long stop) {
        return _command<long long>(async_cmd::zremrangebyrank, key, start, stop);
    }

    template <typename Interval>
    Future<long long> zremrangebyscore(const StringView &key, const Interval &interval) {
        return _command<long long>(async_cmd::zremrangebyscore<Interval>, key, interval);
    }

    template <typename Output, typename Interval>
    Future<Output> zrevrangebylex(const StringView &key,
                        const Interval &interval,
                        const LimitOptions &opts) {
        return _command<Output>(async_cmd::zrevrangebylex<Output, Interval>,
                key, interval, opts);
    }

    template <typename Output, typename Interval>
    Future<Output> zrevrangebylex(const StringView &key, const Interval &interval) {
        return zrevrangebylex<Output>(key, interval, {});
    }

    Future<OptionalLongLong> zrevrank(const StringView &key, const StringView &member) {
        return _command<OptionalLongLong>(async_cmd::zrevrank, key, member);
    }

    Future<OptionalDouble> zscore(const StringView &key, const StringView &member) {
        return _command<OptionalDouble>(async_cmd::zscore, key, member);
    }

    // SCRIPTING commands.

    template <typename Result, typename Keys, typename Args>
    Future<Result> eval(const StringView &script,
                Keys keys_first,
                Keys keys_last,
                Args args_first,
                Args args_last) {
        return _command<Result>(async_cmd::eval<Result, Keys, Args>,
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

    template <typename Result, typename Keys, typename Args>
    Future<Result> evalsha(const StringView &script,
                    Keys keys_first,
                    Keys keys_last,
                    Args args_first,
                    Args args_last) {
        return _command<Result>(async_cmd::evalsha<Result, Keys, Args>,
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

private:
    template <typename Result, typename Cmd, typename ...Args>
    auto _command(Cmd cmd, Args &&...args)
        -> typename std::enable_if<!std::is_convertible<Cmd, StringView>::value,
                                    Future<Result>>::type {
        assert(_pool);
        SafeAsyncConnection connection(*_pool);

        return cmd(connection.connection(), std::forward<Args>(args)...);
    }

    template <typename Result, typename ResultParser, typename ...Args>
    Future<Result> _command_with_parser(Args &&...args) {
        assert(_pool);
        SafeAsyncConnection connection(*_pool);

        return connection.connection().send<Result, ResultParser>(std::forward<Args>(args)...);
    }

    EventLoopSPtr _loop;

    AsyncConnectionPoolSPtr _pool;
};

}

}

#endif // end SEWENEW_REDISPLUSPLUS_ASYNC_REDIS_H
