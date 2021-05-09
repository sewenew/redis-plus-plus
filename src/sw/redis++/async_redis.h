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
#include "event_loop.h"
#include "utils.h"
#include "command.h"
#include "command_args.h"
#include "command_options.h"

namespace sw {

namespace redis {

class AsyncRedis {
public:
    AsyncRedis(const ConnectionOptions &opts,
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

        return _command<Result>(cmd_args);
    }

    template <typename Result, typename Input>
    auto command(Input first, Input last)
        -> typename std::enable_if<IsIter<Input>::value, Future<Result>>::type {
        CmdArgs cmd_args;
        while (first != last) {
            cmd_args.append(*first);
            ++first;
        }

        return _command<Result>(cmd_args);
    }

    // CONNECTION commands.

    Future<std::string> echo(const StringView &msg);

    Future<std::string> ping();

    Future<std::string> ping(const StringView &msg);

    Future<long long> del(const StringView &key);

    template <typename Input>
    Future<long long> del(Input first, Input last) {
        range_check("DEL", first, last);

        CmdArgs args;
        args << "DEL" << std::make_pair(first, last);

        return _command<long long>(args);
    }

    template <typename T>
    Future<long long> del(std::initializer_list<T> il) {
        return del(il.begin(), il.end());
    }

    Future<long long> exists(const StringView &key);

    template <typename Input>
    Future<long long> exists(Input first, Input last) {
        range_check("EXISTS", first, last);

        CmdArgs args;
        args << "EXISTS" << std::make_pair(first, last);

        return _command<long long>(args);
    }

    template <typename T>
    Future<long long> exists(std::initializer_list<T> il) {
        return exists(il.begin(), il.end());
    }

    Future<bool> expire(const StringView &key, const std::chrono::seconds &timeout);

    Future<bool> expireat(const StringView &key,
                    const std::chrono::time_point<std::chrono::system_clock,
                                                    std::chrono::seconds> &tp);

    Future<bool> pexpire(const StringView &key, const std::chrono::milliseconds &timeout);

    Future<bool> pexpireat(const StringView &key,
                    const std::chrono::time_point<std::chrono::system_clock,
                                                    std::chrono::milliseconds> &tp);

    Future<long long> pttl(const StringView &key);

    Future<void> rename(const StringView &key, const StringView &newkey);

    Future<bool> renamenx(const StringView &key, const StringView &newkey);

    Future<long long> ttl(const StringView &key);

    Future<long long> unlink(const StringView &key);

    template <typename Input>
    Future<long long> unlink(Input first, Input last) {
        range_check("UNLINK", first, last);

        CmdArgs args;
        args << "UNLINK" << std::make_pair(first, last);

        return _command<long long>(args);
    }

    template <typename T>
    Future<long long> unlink(std::initializer_list<T> il) {
        return unlink(il.begin(), il.end());
    }

    // STRING commands.

    Future<OptionalString> get(const StringView &key);

    Future<long long> incr(const StringView &key);

    Future<long long> incrby(const StringView &key, long long increment);

    Future<double> incrbyfloat(const StringView &key, double increment);

    template <typename Input, typename Output>
    Future<Output> mget(Input first, Input last) {
        range_check("MGET", first, last);

        CmdArgs args;
        args << "MGET" << std::make_pair(first, last);

        return _command<Output>(args);
    }

    template <typename T, typename Output>
    Future<Output> mget(std::initializer_list<T> il) {
        return mget(il.begin(), il.end());
    }

    template <typename Input>
    Future<void> mset(Input first, Input last) {
        range_check("MSET", first, last);

        CmdArgs args;
        args << "MSET" << std::make_pair(first, last);

        return _command<void>(args);
    }

    template <typename T>
    Future<void> mset(std::initializer_list<T> il) {
        return mset(il.begin(), il.end());
    }

    template <typename Input>
    Future<bool> msetnx(Input first, Input last) {
        range_check("MSETNX", first, last);

        CmdArgs args;
        args << "MSETNX" << std::make_pair(first, last);

        return _command<bool>(args);
    }

    template <typename T>
    Future<bool> msetnx(std::initializer_list<T> il) {
        return msetnx(il.begin(), il.end());
    }

    Future<bool> set(const StringView &key,
                const StringView &val,
                const std::chrono::milliseconds &ttl = std::chrono::milliseconds(0),
                UpdateType type = UpdateType::ALWAYS);

    Future<long long> strlen(const StringView &key);

    // LIST commands.

    Future<OptionalStringPair> blpop(const StringView &key,
                                const std::chrono::seconds &timeout = std::chrono::seconds{0});

    template <typename Input>
    Future<OptionalStringPair> blpop(Input first,
                                Input last,
                                const std::chrono::seconds &timeout = std::chrono::seconds{0}) {
        range_check("BLPOP", first, last);

        CmdArgs args;
        args << "BLPOP" << std::make_pair(first, last) << timeout.count();

        return _command<OptionalStringPair>(args);
    }

    template <typename T>
    Future<OptionalStringPair> blpop(std::initializer_list<T> il,
                                const std::chrono::seconds &timeout = std::chrono::seconds{0}) {
        return blpop(il.begin(), il.end(), timeout);
    }

    Future<OptionalStringPair> brpop(const StringView &key,
                                const std::chrono::seconds &timeout = std::chrono::seconds{0});

    template <typename Input>
    Future<OptionalStringPair> brpop(Input first,
                                Input last,
                                const std::chrono::seconds &timeout = std::chrono::seconds{0}) {
        range_check("BRPOP", first, last);

        CmdArgs args;
        args << "BRPOP" << std::make_pair(first, last) << timeout.count();

        return _command<OptionalStringPair>(args);
    }

    template <typename T>
    Future<OptionalStringPair> brpop(std::initializer_list<T> il,
                                const std::chrono::seconds &timeout = std::chrono::seconds{0}) {
        return brpop(il.begin(), il.end(), timeout);
    }

    Future<OptionalString> brpoplpush(const StringView &source,
                                const StringView &destination,
                                const std::chrono::seconds &timeout = std::chrono::seconds{0});

    Future<long long> llen(const StringView &key);

    Future<OptionalString> lpop(const StringView &key);

    Future<long long> lpush(const StringView &key, const StringView &val);

    template <typename Input>
    Future<long long> lpush(const StringView &key, Input first, Input last) {
        range_check("LPUSH", first, last);

        CmdArgs args;
        args << "LPUSH" << key << std::make_pair(first, last);

        return _command<long long>(args);
    }

    template <typename T>
    Future<long long> lpush(const StringView &key, std::initializer_list<T> il) {
        return lpush(key, il.begin(), il.end());
    }

    template <typename Output>
    Future<Output> lrange(const StringView &key, long long start, long long stop) {
        return _command<Output>("LRANGE %b %lld %lld",
                key.data(), key.size(),
                start, stop);
    }

    Future<long long> lrem(const StringView &key, long long count, const StringView &val);

    Future<void> ltrim(const StringView &key, long long start, long long stop);

    Future<OptionalString> rpop(const StringView &key);

    Future<OptionalString> rpoplpush(const StringView &source, const StringView &destination);

    Future<long long> rpush(const StringView &key, const StringView &val);

    template <typename Input>
    Future<long long> rpush(const StringView &key, Input first, Input last) {
        range_check("RPUSH", first, last);

        CmdArgs args;
        args << "RPUSH" << key << std::make_pair(first, last);

        return _command<long long>(args);
    }

    template <typename T>
    Future<long long> rpush(const StringView &key, std::initializer_list<T> il) {
        return rpush(key, il.begin(), il.end());
    }

    // HASH commands.

    Future<long long> hdel(const StringView &key, const StringView &field);

    template <typename Input>
    Future<long long> hdel(const StringView &key, Input first, Input last) {
        range_check("HDEL", first, last);

        CmdArgs args;
        args << "HDEL" << key << std::make_pair(first, last);

        return _command<long long>(args);
    }

    template <typename T>
    Future<long long> hdel(const StringView &key, std::initializer_list<T> il) {
        return hdel(key, il.begin(), il.end());
    }

    Future<bool> hexists(const StringView &key, const StringView &field);

    Future<OptionalString> hget(const StringView &key, const StringView &field);

    template <typename Output>
    Future<Output> hgetall(const StringView &key) {
        return _command<Output>("HGETALL %b", key.data(), key.size());
    }

    Future<long long> hincrby(const StringView &key, const StringView &field, long long increment);

    Future<double> hincrbyfloat(const StringView &key, const StringView &field, double increment);

    template <typename Output>
    Future<Output> hkeys(const StringView &key) {
        return _command<Output>("HKEYS %b", key.data(), key.size());
    }

    Future<long long> hlen(const StringView &key);

    template <typename Input, typename Output>
    Future<Output> hmget(const StringView &key, Input first, Input last) {
        range_check("HMGET", first, last);

        CmdArgs args;
        args << "HMGET" << key << std::make_pair(first, last);

        return _command<Output>(args);
    }

    template <typename T, typename Output>
    Future<Output> hmget(const StringView &key, std::initializer_list<T> il) {
        return hmget(key, il.begin(), il.end());
    }

    template <typename Input>
    Future<void> hmset(const StringView &key, Input first, Input last) {
        range_check("HMSET", first, last);

        CmdArgs args;
        args << "HMSET" << key << std::make_pair(first, last);

        return _command<void>(args);
    }

    template <typename T>
    Future<void> hmset(const StringView &key, std::initializer_list<T> il) {
        return hmset(key, il.begin(), il.end());
    }

    Future<bool> hset(const StringView &key, const StringView &field, const StringView &val);

    Future<bool> hset(const StringView &key, const std::pair<StringView, StringView> &item) {
        return hset(key, item.first, item.second);
    }

    template <typename Input>
    auto hset(const StringView &key, Input first, Input last)
        -> typename std::enable_if<!std::is_convertible<Input, StringView>::value, Future<long long>>::type {
        range_check("HSET", first, last);

        CmdArgs args;
        args << "HSET" << key << std::make_pair(first, last);

        return _command<long long>(args);
    }

    template <typename T>
    Future<long long> hset(const StringView &key, std::initializer_list<T> il) {
        return hset(key, il.begin(), il.end());
    }

    template <typename Output>
    Future<Output> hvals(const StringView &key) {
        return _command<Output>("HVALS %b", key.data(), key.size());
    }

    // SET commands.

    Future<long long> sadd(const StringView &key, const StringView &member);

    template <typename Input>
    Future<long long> sadd(const StringView &key, Input first, Input last) {
        range_check("SADD", first, last);

        CmdArgs args;
        args << "SADD" << key << std::make_pair(first, last);

        return _command<long long>(args);
    }

    template <typename T>
    Future<long long> sadd(const StringView &key, std::initializer_list<T> il) {
        return sadd(key, il.begin(), il.end());
    }

    Future<long long> scard(const StringView &key);

    Future<bool> sismember(const StringView &key, const StringView &member);

    template <typename Output>
    Future<Output> smembers(const StringView &key) {
        return _command<Output>("SMEMBERS %b", key.data(), key.size());
    }

    Future<OptionalString> spop(const StringView &key);

    template <typename Output>
    Future<Output> spop(const StringView &key, long long count) {
        return _command<Output>("SPOP %b %lld",
                key.data(), key.size(),
                count);
    }

    Future<long long> srem(const StringView &key, const StringView &member);

    template <typename Input>
    Future<long long> srem(const StringView &key, Input first, Input last) {
        range_check("SREM", first, last);

        CmdArgs args;
        args << "SREM" << key << std::make_pair(first, last);

        return _command<long long>(args);
    }

    template <typename T>
    Future<long long> srem(const StringView &key, std::initializer_list<T> il) {
        return srem(key, il.begin(), il.end());
    }

    // SORTED SET commands.

    auto bzpopmax(const StringView &key,
                    const std::chrono::seconds &timeout = std::chrono::seconds{0})
        -> Future<Optional<std::tuple<std::string, std::string, double>>>;

    template <typename Input>
    auto bzpopmax(Input first,
                    Input last,
                    const std::chrono::seconds &timeout = std::chrono::seconds{0})
        -> Future<Optional<std::tuple<std::string, std::string, double>>> {
        range_check("BZPOPMAX", first, last);

        CmdArgs args;
        args << "BZPOPMAX" << std::make_pair(first, last) << timeout.count();

        return _command<Optional<std::tuple<std::string, std::string, double>>>(args);
    }

    template <typename T>
    auto bzpopmax(std::initializer_list<T> il,
                    const std::chrono::seconds &timeout = std::chrono::seconds{0})
        -> Future<Optional<std::tuple<std::string, std::string, double>>> {
        return bzpopmax(il.begin(), il.end(), timeout);
    }

    auto bzpopmin(const StringView &key,
                    const std::chrono::seconds &timeout = std::chrono::seconds{0})
        -> Future<Optional<std::tuple<std::string, std::string, double>>>;

    template <typename Input>
    auto bzpopmin(Input first,
                    Input last,
                    const std::chrono::seconds &timeout = std::chrono::seconds{0})
        -> Future<Optional<std::tuple<std::string, std::string, double>>> {
        range_check("BZPOPMIN", first, last);

        CmdArgs args;
        args << "BZPOPMIN" << std::make_pair(first, last) << timeout.count();

        return _command<Optional<std::tuple<std::string, std::string, double>>>(args);
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
                    bool changed = false);

    template <typename Input>
    Future<long long> zadd(const StringView &key,
                    Input first,
                    Input last,
                    UpdateType type = UpdateType::ALWAYS,
                    bool changed = false) {
        range_check("ZADD", first, last);

        CmdArgs args;
        args << "ZADD" << key;

        cmd::detail::set_update_type(args, type);

        if (changed) {
            args << "CH";
        }

        while (first != last) {
            // Swap the <member, score> pair to <score, member> pair.
            args << first->second << first->first;
            ++first;
        }

        return _command<long long>(args);
    }

    template <typename T>
    Future<long long> zadd(const StringView &key,
                    std::initializer_list<T> il,
                    UpdateType type = UpdateType::ALWAYS,
                    bool changed = false) {
        return zadd(key, il.begin(), il.end(), type, changed);
    }

    Future<long long> zcard(const StringView &key);

    template <typename Interval>
    Future<long long> zcount(const StringView &key, const Interval &interval) {
        return _command<long long>("ZCOUNT %b %s %s",
                key.data(), key.size(),
                interval.min().c_str(),
                interval.max().c_str());
    }

    Future<double> zincrby(const StringView &key, double increment, const StringView &member);

    template <typename Interval>
    Future<long long> zlexcount(const StringView &key, const Interval &interval) {
        const auto &min = interval.min();
        const auto &max = interval.max();

        return _command<long long>("ZLEXCOUNT %b %b %b",
                        key.data(), key.size(),
                        min.data(), min.size(),
                        max.data(), max.size());
    }

    Future<Optional<std::pair<std::string, double>>> zpopmax(const StringView &key);

    template <typename Output>
    Future<Output> zpopmax(const StringView &key, long long count) {
        return _command<Output>("ZPOPMAX %b %lld",
                key.data(), key.size(),
                count);
    }

    Future<Optional<std::pair<std::string, double>>> zpopmin(const StringView &key);

    template <typename Output>
    Future<Output> zpopmin(const StringView &key, long long count) {
        return _command<Output>("ZPOPMIN %b %lld",
                key.data(), key.size(),
                count);
    }

    template <typename Output>
    Future<Output> zrange(const StringView &key, long long start, long long stop) {
        return _command<Output>("ZRANGE %b %lld %lld",
                key.data(), key.size(),
                start, stop);
    }

    template <typename Interval, typename Output>
    Future<Output> zrangebylex(const StringView &key,
                        const Interval &interval,
                        const LimitOptions &opts) {
        const auto &min = interval.min();
        const auto &max = interval.max();

        return _command<Output>("ZRANGEBYLEX %b %b %b LIMIT %lld %lld",
                        key.data(), key.size(),
                        min.data(), min.size(),
                        max.data(), max.size(),
                        opts.offset,
                        opts.count);
    }

    template <typename Interval, typename Output>
    Future<Output> zrangebylex(const StringView &key, const Interval &interval) {
        return zrangebylex(key, interval, {});
    }

    template <typename Interval, typename Output>
    Future<Output> zrangebyscore(const StringView &key, const Interval &interval) {
        return zrangebyscore(key, interval, {});
    }

    Future<OptionalLongLong> rank(const StringView &key, const StringView &member);

    Future<long long> zrem(const StringView &key, const StringView &member);

    template <typename Input>
    Future<long long> zrem(const StringView &key, Input first, Input last) {
        range_check("ZREM", first, last);

        CmdArgs args;
        args << "ZREM" << key << std::make_pair(first, last);

        return _command<long long>(args);
    }

    template <typename T>
    Future<long long> zrem(const StringView &key, std::initializer_list<T> il) {
        return zrem(key, il.begin(), il.end());
    }

    template <typename Interval>
    Future<long long> zremrangebylex(const StringView &key, const Interval &interval) {
        const auto &min = interval.min();
        const auto &max = interval.max();

        return _command<long long>("ZREMRANGEBYLEX %b %b %b",
                        key.data(), key.size(),
                        min.data(), min.size(),
                        max.data(), max.size());
    }

    Future<long long> zremrangebyrank(const StringView &key, long long start, long long stop);

    template <typename Interval>
    Future<long long> zremrangebyscore(const StringView &key, const Interval &interval) {
        const auto &min = interval.min();
        const auto &max = interval.max();

        return _command<long long>("ZREMRANGEBYSCORE %b %b %b",
                        key.data(), key.size(),
                        min.data(), min.size(),
                        max.data(), max.size());
    }

    template <typename Interval, typename Output>
    Future<Output> zrevrangebylex(const StringView &key,
                        const Interval &interval,
                        const LimitOptions &opts) {
        const auto &min = interval.min();
        const auto &max = interval.max();

        return _command<Output>("ZREVRANGEBYLEX %b %b %b LIMIT %lld %lld",
                        key.data(), key.size(),
                        max.data(), max.size(),
                        min.data(), min.size(),
                        opts.offset,
                        opts.count);
    }

    template <typename Interval, typename Output>
    Future<Output> zrevrangebylex(const StringView &key, const Interval &interval) {
        return zrevrangebylex(key, interval, {});
    }

    Future<OptionalLongLong> zrevrank(const StringView &key, const StringView &member);

    Future<OptionalDouble> zscore(const StringView &key, const StringView &member);

private:
    template <typename Result, typename ...Args>
    Future<Result> _command(Args &&...args) {
        assert(_pool);
        SafeAsyncConnection connection(*_pool);

        return connection.connection().send<Result>(std::forward<Args>(args)...);
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
