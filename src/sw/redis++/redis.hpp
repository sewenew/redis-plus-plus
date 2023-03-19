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

#ifndef SEWENEW_REDISPLUSPLUS_REDIS_HPP
#define SEWENEW_REDISPLUSPLUS_REDIS_HPP

#include "sw/redis++/command.h"
#include "sw/redis++/reply.h"
#include "sw/redis++/utils.h"
#include "sw/redis++/errors.h"

namespace sw {

namespace redis {

template <typename Cmd, typename ...Args>
auto Redis::command(Cmd cmd, Args &&...args)
    -> typename std::enable_if<!std::is_convertible<Cmd, StringView>::value, ReplyUPtr>::type {
    if (_connection) {
        // Single Connection Mode.
        // TODO: In this case, should we reconnect?
        auto &connection = _connection->connection();
        if (connection.broken()) {
            throw Error("Connection is broken");
        }

        return _command(connection, cmd, std::forward<Args>(args)...);
    } else {
        assert(_pool);

        // Pool Mode, i.e. get connection from pool.
        SafeConnection connection(*_pool);

        return _command(connection.connection(), cmd, std::forward<Args>(args)...);
    }
}

template <typename ...Args>
auto Redis::command(const StringView &cmd_name, Args &&...args)
    -> typename std::enable_if<!IsIter<typename LastType<Args...>::type>::value, ReplyUPtr>::type {
    auto cmd = [](Connection &connection, const StringView &name, Args &&...params) {
                    CmdArgs cmd_args;
                    cmd_args.append(name, std::forward<Args>(params)...);
                    connection.send(cmd_args);
    };

    return command(cmd, cmd_name, std::forward<Args>(args)...);
}

template <typename Input>
auto Redis::command(Input first, Input last)
    -> typename std::enable_if<IsIter<Input>::value, ReplyUPtr>::type {
    range_check("command", first, last);

    auto cmd = [](Connection &connection, Input start, Input stop) {
                    CmdArgs cmd_args;
                    while (start != stop) {
                        cmd_args.append(*start);
                        ++start;
                    }
                    connection.send(cmd_args);
    };

    return command(cmd, first, last);
}

template <typename Result, typename ...Args>
Result Redis::command(const StringView &cmd_name, Args &&...args) {
    auto r = command(cmd_name, std::forward<Args>(args)...);

    assert(r);

    return reply::parse<Result>(*r);
}

template <typename ...Args>
auto Redis::command(const StringView &cmd_name, Args &&...args)
    -> typename std::enable_if<IsIter<typename LastType<Args...>::type>::value, void>::type {
    auto r = _command(cmd_name,
                        MakeIndexSequence<sizeof...(Args) - 1>(),
                        std::forward<Args>(args)...);

    assert(r);

    reply::to_array(*r, LastValue(std::forward<Args>(args)...));
}

template <typename Result, typename Input>
auto Redis::command(Input first, Input last)
    -> typename std::enable_if<IsIter<Input>::value, Result>::type {
    auto r = command(first, last);

    assert(r);

    return reply::parse<Result>(*r);
}

template <typename Input, typename Output>
auto Redis::command(Input first, Input last, Output output)
    -> typename std::enable_if<IsIter<Input>::value, void>::type {
    auto r = command(first, last);

    assert(r);

    reply::to_array(*r, output);
}

// KEY commands.

template <typename Input>
long long Redis::del(Input first, Input last) {
    range_check("DEL", first, last);

    auto reply = command(cmd::del_range<Input>, first, last);

    return reply::parse<long long>(*reply);
}

template <typename Input>
long long Redis::exists(Input first, Input last) {
    range_check("EXISTS", first, last);

    auto reply = command(cmd::exists_range<Input>, first, last);

    return reply::parse<long long>(*reply);
}

inline bool Redis::expire(const StringView &key, const std::chrono::seconds &timeout) {
    return expire(key, timeout.count());
}

inline bool Redis::expireat(const StringView &key,
                                    const std::chrono::time_point<std::chrono::system_clock,
                                                                    std::chrono::seconds> &tp) {
    return expireat(key, tp.time_since_epoch().count());
}

template <typename Output>
void Redis::keys(const StringView &pattern, Output output) {
    auto reply = command(cmd::keys, pattern);

    reply::to_array(*reply, output);
}

inline bool Redis::pexpire(const StringView &key, const std::chrono::milliseconds &timeout) {
    return pexpire(key, timeout.count());
}

inline bool Redis::pexpireat(const StringView &key,
                                const std::chrono::time_point<std::chrono::system_clock,
                                                                std::chrono::milliseconds> &tp) {
    return pexpireat(key, tp.time_since_epoch().count());
}

inline void Redis::restore(const StringView &key,
                            const StringView &val,
                            const std::chrono::milliseconds &ttl,
                            bool replace) {
    return restore(key, val, ttl.count(), replace);
}

template <typename Output>
long long Redis::scan(long long cursor,
                    const StringView &pattern,
                    long long count,
                    Output output) {
    auto reply = command(cmd::scan, cursor, pattern, count);

    return reply::parse_scan_reply(*reply, output);
}

template <typename Output>
inline long long Redis::scan(long long cursor,
                                const StringView &pattern,
                                Output output) {
    return scan(cursor, pattern, 10, output);
}

template <typename Output>
inline long long Redis::scan(long long cursor,
                                long long count,
                                Output output) {
    return scan(cursor, "*", count, output);
}

template <typename Output>
inline long long Redis::scan(long long cursor,
                                Output output) {
    return scan(cursor, "*", 10, output);
}

template <typename Input>
long long Redis::touch(Input first, Input last) {
    range_check("TOUCH", first, last);

    auto reply = command(cmd::touch_range<Input>, first, last);

    return reply::parse<long long>(*reply);
}

template <typename Input>
long long Redis::unlink(Input first, Input last) {
    range_check("UNLINK", first, last);

    auto reply = command(cmd::unlink_range<Input>, first, last);

    return reply::parse<long long>(*reply);
}

inline long long Redis::wait(long long numslaves, const std::chrono::milliseconds &timeout) {
    return wait(numslaves, timeout.count());
}

// STRING commands.

template <typename Input>
long long Redis::bitop(BitOp op, const StringView &destination, Input first, Input last) {
    range_check("BITOP", first, last);

    auto reply = command(cmd::bitop_range<Input>, op, destination, first, last);

    return reply::parse<long long>(*reply);
}

template <typename Input, typename Output>
void Redis::mget(Input first, Input last, Output output) {
    range_check("MGET", first, last);

    auto reply = command(cmd::mget<Input>, first, last);

    reply::to_array(*reply, output);
}

template <typename Input>
void Redis::mset(Input first, Input last) {
    range_check("MSET", first, last);

    auto reply = command(cmd::mset<Input>, first, last);

    reply::parse<void>(*reply);
}

template <typename Input>
bool Redis::msetnx(Input first, Input last) {
    range_check("MSETNX", first, last);

    auto reply = command(cmd::msetnx<Input>, first, last);

    return reply::parse<bool>(*reply);
}

inline void Redis::psetex(const StringView &key,
                            const std::chrono::milliseconds &ttl,
                            const StringView &val) {
    return psetex(key, ttl.count(), val);
}

inline void Redis::setex(const StringView &key,
                            const std::chrono::seconds &ttl,
                            const StringView &val) {
    setex(key, ttl.count(), val);
}

// LIST commands.

template <typename Input>
OptionalStringPair Redis::blpop(Input first, Input last, long long timeout) {
    range_check("BLPOP", first, last);

    auto reply = command(cmd::blpop_range<Input>, first, last, timeout);

    return reply::parse<OptionalStringPair>(*reply);
}

template <typename Input>
OptionalStringPair Redis::blpop(Input first,
                                Input last,
                                const std::chrono::seconds &timeout) {
    return blpop(first, last, timeout.count());
}

template <typename Input>
OptionalStringPair Redis::brpop(Input first, Input last, long long timeout) {
    range_check("BRPOP", first, last);

    auto reply = command(cmd::brpop_range<Input>, first, last, timeout);

    return reply::parse<OptionalStringPair>(*reply);
}

template <typename Input>
OptionalStringPair Redis::brpop(Input first,
                                Input last,
                                const std::chrono::seconds &timeout) {
    return brpop(first, last, timeout.count());
}

inline OptionalString Redis::brpoplpush(const StringView &source,
                                        const StringView &destination,
                                        const std::chrono::seconds &timeout) {
    return brpoplpush(source, destination, timeout.count());
}

template <typename Input>
inline long long Redis::lpush(const StringView &key, Input first, Input last) {
    range_check("LPUSH", first, last);

    auto reply = command(cmd::lpush_range<Input>, key, first, last);

    return reply::parse<long long>(*reply);
}

template <typename Output>
inline void Redis::lrange(const StringView &key, long long start, long long stop, Output output) {
    auto reply = command(cmd::lrange, key, start, stop);

    reply::to_array(*reply, output);
}

template <typename Input>
inline long long Redis::rpush(const StringView &key, Input first, Input last) {
    range_check("RPUSH", first, last);

    auto reply = command(cmd::rpush_range<Input>, key, first, last);

    return reply::parse<long long>(*reply);
}

// HASH commands.

template <typename Input>
inline long long Redis::hdel(const StringView &key, Input first, Input last) {
    range_check("HDEL", first, last);

    auto reply = command(cmd::hdel_range<Input>, key, first, last);

    return reply::parse<long long>(*reply);
}

template <typename Output>
inline void Redis::hgetall(const StringView &key, Output output) {
    auto reply = command(cmd::hgetall, key);

    reply::to_array(*reply, output);
}

template <typename Output>
inline void Redis::hkeys(const StringView &key, Output output) {
    auto reply = command(cmd::hkeys, key);

    reply::to_array(*reply, output);
}

template <typename Input, typename Output>
inline void Redis::hmget(const StringView &key, Input first, Input last, Output output) {
    range_check("HMGET", first, last);

    auto reply = command(cmd::hmget<Input>, key, first, last);

    reply::to_array(*reply, output);
}

template <typename Input>
inline void Redis::hmset(const StringView &key, Input first, Input last) {
    range_check("HMSET", first, last);

    auto reply = command(cmd::hmset<Input>, key, first, last);

    reply::parse<void>(*reply);
}

template <typename Output>
long long Redis::hscan(const StringView &key,
                        long long cursor,
                        const StringView &pattern,
                        long long count,
                        Output output) {
    auto reply = command(cmd::hscan, key, cursor, pattern, count);

    return reply::parse_scan_reply(*reply, output);
}

template <typename Output>
inline long long Redis::hscan(const StringView &key,
                                long long cursor,
                                const StringView &pattern,
                                Output output) {
    return hscan(key, cursor, pattern, 10, output);
}

template <typename Output>
inline long long Redis::hscan(const StringView &key,
                                long long cursor,
                                long long count,
                                Output output) {
    return hscan(key, cursor, "*", count, output);
}

template <typename Output>
inline long long Redis::hscan(const StringView &key,
                                long long cursor,
                                Output output) {
    return hscan(key, cursor, "*", 10, output);
}

template <typename Input>
auto Redis::hset(const StringView &key, Input first, Input last)
        -> typename std::enable_if<!std::is_convertible<Input, StringView>::value,
                                    long long>::type {
    range_check("HSET", first, last);

    auto reply = command(cmd::hset_range<Input>, key, first, last);

    return reply::parse<long long>(*reply);
}

template <typename Output>
inline void Redis::hvals(const StringView &key, Output output) {
    auto reply = command(cmd::hvals, key);

    reply::to_array(*reply, output);
}

// SET commands.

template <typename Input>
long long Redis::sadd(const StringView &key, Input first, Input last) {
    range_check("SADD", first, last);

    auto reply = command(cmd::sadd_range<Input>, key, first, last);

    return reply::parse<long long>(*reply);
}

template <typename Input, typename Output>
void Redis::sdiff(Input first, Input last, Output output) {
    range_check("SDIFF", first, last);

    auto reply = command(cmd::sdiff<Input>, first, last);

    reply::to_array(*reply, output);
}

template <typename Input>
long long Redis::sdiffstore(const StringView &destination,
                            Input first,
                            Input last) {
    range_check("SDIFFSTORE", first, last);

    auto reply = command(cmd::sdiffstore_range<Input>, destination, first, last);

    return reply::parse<long long>(*reply);
}

template <typename Input, typename Output>
void Redis::sinter(Input first, Input last, Output output) {
    range_check("SINTER", first, last);

    auto reply = command(cmd::sinter<Input>, first, last);

    reply::to_array(*reply, output);
}

template <typename Input>
long long Redis::sinterstore(const StringView &destination,
                            Input first,
                            Input last) {
    range_check("SINTERSTORE", first, last);

    auto reply = command(cmd::sinterstore_range<Input>, destination, first, last);

    return reply::parse<long long>(*reply);
}

template <typename Output>
void Redis::smembers(const StringView &key, Output output) {
    auto reply = command(cmd::smembers, key);

    reply::to_array(*reply, output);
}

template <typename Output>
void Redis::spop(const StringView &key, long long count, Output output) {
    auto reply = command(cmd::spop_range, key, count);

    reply::to_array(*reply, output);
}

template <typename Output>
void Redis::srandmember(const StringView &key, long long count, Output output) {
    auto reply = command(cmd::srandmember_range, key, count);

    reply::to_array(*reply, output);
}

template <typename Input>
long long Redis::srem(const StringView &key, Input first, Input last) {
    range_check("SREM", first, last);

    auto reply = command(cmd::srem_range<Input>, key, first, last);

    return reply::parse<long long>(*reply);
}

template <typename Output>
long long Redis::sscan(const StringView &key,
                        long long cursor,
                        const StringView &pattern,
                        long long count,
                        Output output) {
    auto reply = command(cmd::sscan, key, cursor, pattern, count);

    return reply::parse_scan_reply(*reply, output);
}

template <typename Output>
inline long long Redis::sscan(const StringView &key,
                                long long cursor,
                                const StringView &pattern,
                                Output output) {
    return sscan(key, cursor, pattern, 10, output);
}

template <typename Output>
inline long long Redis::sscan(const StringView &key,
                                long long cursor,
                                long long count,
                                Output output) {
    return sscan(key, cursor, "*", count, output);
}

template <typename Output>
inline long long Redis::sscan(const StringView &key,
                                long long cursor,
                                Output output) {
    return sscan(key, cursor, "*", 10, output);
}

template <typename Input, typename Output>
void Redis::sunion(Input first, Input last, Output output) {
    range_check("SUNION", first, last);

    auto reply = command(cmd::sunion<Input>, first, last);

    reply::to_array(*reply, output);
}

template <typename Input>
long long Redis::sunionstore(const StringView &destination, Input first, Input last) {
    range_check("SUNIONSTORE", first, last);

    auto reply = command(cmd::sunionstore_range<Input>, destination, first, last);

    return reply::parse<long long>(*reply);
}

// SORTED SET commands.

inline auto Redis::bzpopmax(const StringView &key, const std::chrono::seconds &timeout)
    -> Optional<std::tuple<std::string, std::string, double>> {
    return bzpopmax(key, timeout.count());
}

template <typename Input>
auto Redis::bzpopmax(Input first, Input last, long long timeout)
    -> Optional<std::tuple<std::string, std::string, double>> {
    auto reply = command(cmd::bzpopmax_range<Input>, first, last, timeout);

    return reply::parse<Optional<std::tuple<std::string, std::string, double>>>(*reply);
}

template <typename Input>
inline auto Redis::bzpopmax(Input first,
                            Input last,
                            const std::chrono::seconds &timeout)
    -> Optional<std::tuple<std::string, std::string, double>> {
    return bzpopmax(first, last, timeout.count());
}

inline auto Redis::bzpopmin(const StringView &key, const std::chrono::seconds &timeout)
    -> Optional<std::tuple<std::string, std::string, double>> {
    return bzpopmin(key, timeout.count());
}

template <typename Input>
auto Redis::bzpopmin(Input first, Input last, long long timeout)
    -> Optional<std::tuple<std::string, std::string, double>> {
    auto reply = command(cmd::bzpopmin_range<Input>, first, last, timeout);

    return reply::parse<Optional<std::tuple<std::string, std::string, double>>>(*reply);
}

template <typename Input>
inline auto Redis::bzpopmin(Input first,
                            Input last,
                            const std::chrono::seconds &timeout)
    -> Optional<std::tuple<std::string, std::string, double>> {
    return bzpopmin(first, last, timeout.count());
}

template <typename Input>
long long Redis::zadd(const StringView &key,
                        Input first,
                        Input last,
                        UpdateType type,
                        bool changed) {
    range_check("ZADD", first, last);

    auto reply = command(cmd::zadd_range<Input>, key, first, last, type, changed);

    return reply::parse<long long>(*reply);
}

template <typename Interval>
long long Redis::zcount(const StringView &key, const Interval &interval) {
    auto reply = command(cmd::zcount<Interval>, key, interval);

    return reply::parse<long long>(*reply);
}

template <typename Input>
long long Redis::zinterstore(const StringView &destination,
                                Input first,
                                Input last,
                                Aggregation type) {
    range_check("ZINTERSTORE", first, last);

    auto reply = command(cmd::zinterstore_range<Input>,
                            destination,
                            first,
                            last,
                            type);

    return reply::parse<long long>(*reply);
}

template <typename Interval>
long long Redis::zlexcount(const StringView &key, const Interval &interval) {
    auto reply = command(cmd::zlexcount<Interval>, key, interval);

    return reply::parse<long long>(*reply);
}

template <typename Output>
void Redis::zpopmax(const StringView &key, long long count, Output output) {
    auto reply = command(cmd::zpopmax, key, count);

    reply::to_array(*reply, output);
}

template <typename Output>
void Redis::zpopmin(const StringView &key, long long count, Output output) {
    auto reply = command(cmd::zpopmin, key, count);

    reply::to_array(*reply, output);
}

template <typename Output>
void Redis::zrange(const StringView &key, long long start, long long stop, Output output) {
    auto reply = _score_command<Output>(cmd::zrange, key, start, stop);

    reply::to_array(*reply, output);
}

template <typename Interval, typename Output>
void Redis::zrangebylex(const StringView &key, const Interval &interval, Output output) {
    zrangebylex(key, interval, {}, output);
}

template <typename Interval, typename Output>
void Redis::zrangebylex(const StringView &key,
                        const Interval &interval,
                        const LimitOptions &opts,
                        Output output) {
    auto reply = command(cmd::zrangebylex<Interval>, key, interval, opts);

    reply::to_array(*reply, output);
}

template <typename Interval, typename Output>
void Redis::zrangebyscore(const StringView &key,
                            const Interval &interval,
                            Output output) {
    zrangebyscore(key, interval, {}, output);
}

template <typename Interval, typename Output>
void Redis::zrangebyscore(const StringView &key,
                            const Interval &interval,
                            const LimitOptions &opts,
                            Output output) {
    auto reply = _score_command<Output>(cmd::zrangebyscore<Interval>,
                                        key,
                                        interval,
                                        opts);

    reply::to_array(*reply, output);
}

template <typename Input>
long long Redis::zrem(const StringView &key, Input first, Input last) {
    range_check("ZREM", first, last);

    auto reply = command(cmd::zrem_range<Input>, key, first, last);

    return reply::parse<long long>(*reply);
}

template <typename Interval>
long long Redis::zremrangebylex(const StringView &key, const Interval &interval) {
    auto reply = command(cmd::zremrangebylex<Interval>, key, interval);

    return reply::parse<long long>(*reply);
}

template <typename Interval>
long long Redis::zremrangebyscore(const StringView &key, const Interval &interval) {
    auto reply = command(cmd::zremrangebyscore<Interval>, key, interval);

    return reply::parse<long long>(*reply);
}

template <typename Output>
void Redis::zrevrange(const StringView &key, long long start, long long stop, Output output) {
    auto reply = _score_command<Output>(cmd::zrevrange, key, start, stop);

    reply::to_array(*reply, output);
}

template <typename Interval, typename Output>
inline void Redis::zrevrangebylex(const StringView &key,
                                    const Interval &interval,
                                    Output output) {
    zrevrangebylex(key, interval, {}, output);
}

template <typename Interval, typename Output>
void Redis::zrevrangebylex(const StringView &key,
                            const Interval &interval,
                            const LimitOptions &opts,
                            Output output) {
    auto reply = command(cmd::zrevrangebylex<Interval>, key, interval, opts);

    reply::to_array(*reply, output);
}

template <typename Interval, typename Output>
void Redis::zrevrangebyscore(const StringView &key, const Interval &interval, Output output) {
    zrevrangebyscore(key, interval, {}, output);
}

template <typename Interval, typename Output>
void Redis::zrevrangebyscore(const StringView &key,
                                const Interval &interval,
                                const LimitOptions &opts,
                                Output output) {
    auto reply = _score_command<Output>(cmd::zrevrangebyscore<Interval>, key, interval, opts);

    reply::to_array(*reply, output);
}

template <typename Output>
long long Redis::zscan(const StringView &key,
                        long long cursor,
                        const StringView &pattern,
                        long long count,
                        Output output) {
    auto reply = command(cmd::zscan, key, cursor, pattern, count);

    return reply::parse_scan_reply(*reply, output);
}

template <typename Output>
inline long long Redis::zscan(const StringView &key,
                                long long cursor,
                                const StringView &pattern,
                                Output output) {
    return zscan(key, cursor, pattern, 10, output);
}

template <typename Output>
inline long long Redis::zscan(const StringView &key,
                                long long cursor,
                                long long count,
                                Output output) {
    return zscan(key, cursor, "*", count, output);
}

template <typename Output>
inline long long Redis::zscan(const StringView &key,
                                long long cursor,
                                Output output) {
    return zscan(key, cursor, "*", 10, output);
}

template <typename Input>
long long Redis::zunionstore(const StringView &destination,
                                    Input first,
                                    Input last,
                                    Aggregation type) {
    range_check("ZUNIONSTORE", first, last);

    auto reply = command(cmd::zunionstore_range<Input>,
                            destination,
                            first,
                            last,
                            type);

    return reply::parse<long long>(*reply);
}

// HYPERLOGLOG commands.

template <typename Input>
bool Redis::pfadd(const StringView &key, Input first, Input last) {
    range_check("PFADD", first, last);

    auto reply = command(cmd::pfadd_range<Input>, key, first, last);

    return reply::parse<bool>(*reply);
}

template <typename Input>
long long Redis::pfcount(Input first, Input last) {
    range_check("PFCOUNT", first, last);

    auto reply = command(cmd::pfcount_range<Input>, first, last);

    return reply::parse<long long>(*reply);
}

template <typename Input>
void Redis::pfmerge(const StringView &destination,
                    Input first,
                    Input last) {
    range_check("PFMERGE", first, last);

    auto reply = command(cmd::pfmerge_range<Input>, destination, first, last);

    reply::parse<void>(*reply);
}

// GEO commands.

template <typename Input>
inline long long Redis::geoadd(const StringView &key,
                                Input first,
                                Input last) {
    range_check("GEOADD", first, last);

    auto reply = command(cmd::geoadd_range<Input>, key, first, last);

    return reply::parse<long long>(*reply);
}

template <typename Input, typename Output>
void Redis::geohash(const StringView &key, Input first, Input last, Output output) {
    range_check("GEOHASH", first, last);

    auto reply = command(cmd::geohash_range<Input>, key, first, last);

    reply::to_array(*reply, output);
}

template <typename Input, typename Output>
void Redis::geopos(const StringView &key, Input first, Input last, Output output) {
    range_check("GEOPOS", first, last);

    auto reply = command(cmd::geopos_range<Input>, key, first, last);

    reply::to_array(*reply, output);
}

template <typename Output>
void Redis::georadius(const StringView &key,
                        const std::pair<double, double> &loc,
                        double radius,
                        GeoUnit unit,
                        long long count,
                        bool asc,
                        Output output) {
    auto reply = command(cmd::georadius,
                            key,
                            loc,
                            radius,
                            unit,
                            count,
                            asc,
                            WithCoord<typename IterType<Output>::type>::value,
                            WithDist<typename IterType<Output>::type>::value,
                            WithHash<typename IterType<Output>::type>::value);

    reply::to_array(*reply, output);
}

template <typename Output>
void Redis::georadiusbymember(const StringView &key,
                                const StringView &member,
                                double radius,
                                GeoUnit unit,
                                long long count,
                                bool asc,
                                Output output) {
    auto reply = command(cmd::georadiusbymember,
                            key,
                            member,
                            radius,
                            unit,
                            count,
                            asc,
                            WithCoord<typename IterType<Output>::type>::value,
                            WithDist<typename IterType<Output>::type>::value,
                            WithHash<typename IterType<Output>::type>::value);

    reply::to_array(*reply, output);
}

// SCRIPTING commands.

template <typename Result, typename Keys, typename Args>
Result Redis::eval(const StringView &script,
                   Keys keys_first,
                   Keys keys_last,
                   Args args_first,
                   Args args_last) {
    auto reply = command(cmd::eval<Keys, Args>, script, keys_first, keys_last, args_first, args_last);

    return reply::parse<Result>(*reply);
}

template <typename Result>
Result Redis::eval(const StringView &script,
                    std::initializer_list<StringView> keys,
                    std::initializer_list<StringView> args) {
    return eval<Result>(script, keys.begin(), keys.end(), args.begin(), args.end());
}

template <typename Keys, typename Args, typename Output>
void Redis::eval(const StringView &script,
                   Keys keys_first,
                   Keys keys_last,
                   Args args_first,
                   Args args_last,
                   Output output) {
    auto reply = command(cmd::eval<Keys, Args>,
                            script,
                            keys_first, keys_last,
                            args_first, args_last);

    reply::to_array(*reply, output);
}

template <typename Output>
void Redis::eval(const StringView &script,
                    std::initializer_list<StringView> keys,
                    std::initializer_list<StringView> args,
                    Output output) {
    eval(script, keys.begin(), keys.end(), args.begin(), args.end(), output);
}

template <typename Result, typename Keys, typename Args>
Result Redis::evalsha(const StringView &script,
                       Keys keys_first,
                       Keys keys_last,
                       Args args_first,
                       Args args_last) {
    auto reply = command(cmd::evalsha<Keys, Args>, script,
            keys_first, keys_last, args_first, args_last);

    return reply::parse<Result>(*reply);
}

template <typename Result>
Result Redis::evalsha(const StringView &script,
                        std::initializer_list<StringView> keys,
                        std::initializer_list<StringView> args) {
    return evalsha<Result>(script, keys.begin(), keys.end(), args.begin(), args.end());
}

template <typename Keys, typename Args, typename Output>
void Redis::evalsha(const StringView &script,
                       Keys keys_first,
                       Keys keys_last,
                       Args args_first,
                       Args args_last,
                       Output output) {
    auto reply = command(cmd::evalsha<Keys, Args>,
                            script,
                            keys_first, keys_last,
                            args_first, args_last);

    reply::to_array(*reply, output);
}

template <typename Output>
void Redis::evalsha(const StringView &script,
                        std::initializer_list<StringView> keys,
                        std::initializer_list<StringView> args,
                        Output output) {
    evalsha(script, keys.begin(), keys.end(), args.begin(), args.end(), output);
}

template <typename Input, typename Output>
void Redis::script_exists(Input first, Input last, Output output) {
    range_check("SCRIPT EXISTS", first, last);

    auto reply = command(cmd::script_exists_range<Input>, first, last);

    reply::to_array(*reply, output);
}

// Transaction commands.

template <typename Input>
void Redis::watch(Input first, Input last) {
    auto reply = command(cmd::watch_range<Input>, first, last);

    reply::parse<void>(*reply);
}

// Stream commands.

template <typename Input>
long long Redis::xack(const StringView &key, const StringView &group, Input first, Input last) {
    auto reply = command(cmd::xack_range<Input>, key, group, first, last);

    return reply::parse<long long>(*reply);
}

template <typename Input>
std::string Redis::xadd(const StringView &key, const StringView &id, Input first, Input last) {
    auto reply = command(cmd::xadd_range<Input>, key, id, first, last);

    return reply::parse<std::string>(*reply);
}

template <typename Input>
std::string Redis::xadd(const StringView &key,
                        const StringView &id,
                        Input first,
                        Input last,
                        long long count,
                        bool approx) {
    auto reply = command(cmd::xadd_maxlen_range<Input>, key, id, first, last, count, approx);

    return reply::parse<std::string>(*reply);
}

template <typename Output>
void Redis::xclaim(const StringView &key,
                    const StringView &group,
                    const StringView &consumer,
                    const std::chrono::milliseconds &min_idle_time,
                    const StringView &id,
                    Output output) {
    auto reply = command(cmd::xclaim, key, group, consumer, min_idle_time.count(), id);

    reply::to_array(*reply, output);
}

template <typename Input, typename Output>
void Redis::xclaim(const StringView &key,
                    const StringView &group,
                    const StringView &consumer,
                    const std::chrono::milliseconds &min_idle_time,
                    Input first,
                    Input last,
                    Output output) {
    auto reply = command(cmd::xclaim_range<Input>,
                            key,
                            group,
                            consumer,
                            min_idle_time.count(),
                            first,
                            last);

    reply::to_array(*reply, output);
}

template <typename Input>
long long Redis::xdel(const StringView &key, Input first, Input last) {
    auto reply = command(cmd::xdel_range<Input>, key, first, last);

    return reply::parse<long long>(*reply);
}

template <typename Output>
auto Redis::xpending(const StringView &key, const StringView &group, Output output)
    -> std::tuple<long long, OptionalString, OptionalString> {
    auto reply = command(cmd::xpending, key, group);

    return reply::parse_xpending_reply(*reply, output);
}

template <typename Output>
void Redis::xpending(const StringView &key,
                        const StringView &group,
                        const StringView &start,
                        const StringView &end,
                        long long count,
                        Output output) {
    auto reply = command(cmd::xpending_detail, key, group, start, end, count);

    reply::to_array(*reply, output);
}

template <typename Output>
void Redis::xpending(const StringView &key,
                        const StringView &group,
                        const StringView &start,
                        const StringView &end,
                        long long count,
                        const StringView &consumer,
                        Output output) {
    auto reply = command(cmd::xpending_per_consumer, key, group, start, end, count, consumer);

    reply::to_array(*reply, output);
}

template <typename Output>
void Redis::xrange(const StringView &key,
                    const StringView &start,
                    const StringView &end,
                    Output output) {
    auto reply = command(cmd::xrange, key, start, end);

    reply::to_array(*reply, output);
}

template <typename Output>
void Redis::xrange(const StringView &key,
                    const StringView &start,
                    const StringView &end,
                    long long count,
                    Output output) {
    auto reply = command(cmd::xrange_count, key, start, end, count);

    reply::to_array(*reply, output);
}

template <typename Output>
void Redis::xread(const StringView &key,
                    const StringView &id,
                    long long count,
                    Output output) {
    auto reply = command(cmd::xread, key, id, count);

    if (!reply::is_nil(*reply)) {
        reply::to_array(*reply, output);
    }
}

template <typename Input, typename Output>
auto Redis::xread(Input first, Input last, long long count, Output output)
    -> typename std::enable_if<!std::is_convertible<Input, StringView>::value>::type {
    range_check("XREAD", first, last);

    auto reply = command(cmd::xread_range<Input>, first, last, count);

    if (!reply::is_nil(*reply)) {
        reply::to_array(*reply, output);
    }
}

template <typename Output>
void Redis::xread(const StringView &key,
                    const StringView &id,
                    const std::chrono::milliseconds &timeout,
                    long long count,
                    Output output) {
    auto reply = command(cmd::xread_block, key, id, timeout.count(), count);

    if (!reply::is_nil(*reply)) {
        reply::to_array(*reply, output);
    }
}

template <typename Input, typename Output>
auto Redis::xread(Input first,
                    Input last,
                    const std::chrono::milliseconds &timeout,
                    long long count,
                    Output output)
    -> typename std::enable_if<!std::is_convertible<Input, StringView>::value>::type {
    range_check("XREAD", first, last);

    auto reply = command(cmd::xread_block_range<Input>, first, last, timeout.count(), count);

    if (!reply::is_nil(*reply)) {
        reply::to_array(*reply, output);
    }
}

template <typename Output>
void Redis::xreadgroup(const StringView &group,
                        const StringView &consumer,
                        const StringView &key,
                        const StringView &id,
                        long long count,
                        bool noack,
                        Output output) {
    auto reply = command(cmd::xreadgroup, group, consumer, key, id, count, noack);

    if (!reply::is_nil(*reply)) {
        reply::to_array(*reply, output);
    }
}

template <typename Input, typename Output>
auto Redis::xreadgroup(const StringView &group,
                        const StringView &consumer,
                        Input first,
                        Input last,
                        long long count,
                        bool noack,
                        Output output)
    -> typename std::enable_if<!std::is_convertible<Input, StringView>::value>::type {
    range_check("XREADGROUP", first, last);

    auto reply = command(cmd::xreadgroup_range<Input>, group, consumer, first, last, count, noack);

    if (!reply::is_nil(*reply)) {
        reply::to_array(*reply, output);
    }
}

template <typename Output>
void Redis::xreadgroup(const StringView &group,
                        const StringView &consumer,
                        const StringView &key,
                        const StringView &id,
                        const std::chrono::milliseconds &timeout,
                        long long count,
                        bool noack,
                        Output output) {
    auto reply = command(cmd::xreadgroup_block,
                            group,
                            consumer,
                            key,
                            id,
                            timeout.count(),
                            count,
                            noack);

    if (!reply::is_nil(*reply)) {
        reply::to_array(*reply, output);
    }
}

template <typename Input, typename Output>
auto Redis::xreadgroup(const StringView &group,
                        const StringView &consumer,
                        Input first,
                        Input last,
                        const std::chrono::milliseconds &timeout,
                        long long count,
                        bool noack,
                        Output output)
    -> typename std::enable_if<!std::is_convertible<Input, StringView>::value>::type {
    range_check("XREADGROUP", first, last);

    auto reply = command(cmd::xreadgroup_block_range<Input>,
                            group,
                            consumer,
                            first,
                            last,
                            timeout.count(),
                            count,
                            noack);

    if (!reply::is_nil(*reply)) {
        reply::to_array(*reply, output);
    }
}

template <typename Output>
void Redis::xrevrange(const StringView &key,
                        const StringView &end,
                        const StringView &start,
                        Output output) {
    auto reply = command(cmd::xrevrange, key, end, start);

    reply::to_array(*reply, output);
}

template <typename Output>
void Redis::xrevrange(const StringView &key,
                        const StringView &end,
                        const StringView &start,
                        long long count,
                        Output output) {
    auto reply = command(cmd::xrevrange_count, key, end, start, count);

    reply::to_array(*reply, output);
}

template <typename Cmd, typename ...Args>
ReplyUPtr Redis::_command(Connection &connection, Cmd cmd, Args &&...args) {
    assert(!connection.broken());

    cmd(connection, std::forward<Args>(args)...);

    auto reply = connection.recv();

    return reply;
}

template <typename Cmd, typename ...Args>
inline ReplyUPtr Redis::_score_command(std::true_type, Cmd cmd, Args &&... args) {
    return command(cmd, std::forward<Args>(args)..., true);
}

template <typename Cmd, typename ...Args>
inline ReplyUPtr Redis::_score_command(std::false_type, Cmd cmd, Args &&... args) {
    return command(cmd, std::forward<Args>(args)..., false);
}

template <typename Output, typename Cmd, typename ...Args>
inline ReplyUPtr Redis::_score_command(Cmd cmd, Args &&... args) {
    return _score_command(typename IsKvPairIter<Output>::type(),
                            cmd,
                            std::forward<Args>(args)...);
}

}

}

#endif // end SEWENEW_REDISPLUSPLUS_REDIS_HPP
