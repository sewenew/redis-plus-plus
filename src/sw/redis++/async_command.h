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

#ifndef SEWENEW_REDISPLUSPLUS_ASYNC_COMMAND_H
#define SEWENEW_REDISPLUSPLUS_ASYNC_COMMAND_H

#include "async_connection.h"

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

namespace async_cmd {

// CONNECTION commands.

inline Future<std::string> echo(AsyncConnection &connection, const StringView &msg) {
    return connection.send<std::string>("ECHO %b", msg.data(), msg.size());
}

inline Future<std::string> ping(AsyncConnection &connection) {
    return connection.send<std::string>("PING");
}

inline Future<std::string> ping(AsyncConnection &connection, const StringView &msg) {
    return connection.send<std::string>("PING %b", msg.data(), msg.size());
}

inline Future<long long> del(AsyncConnection &connection, const StringView &key) {
    return connection.send<long long>("DEL %b", key.data(), key.size());
}

template <typename Input>
Future<long long> del_range(AsyncConnection &connection, Input first, Input last) {
    assert(first != last);

    CmdArgs args;
    args << "DEL" << std::make_pair(first, last);

    return connection.send<long long>(args);
}

inline Future<long long> exists(AsyncConnection &connection, const StringView &key) {
    return connection.send<long long>("EXISTS %b", key.data(), key.size());
}

template <typename Input>
Future<long long> exists_range(AsyncConnection &connection, Input first, Input last) {
    assert(first != last);

    CmdArgs args;
    args << "EXISTS" << std::make_pair(first, last);

    return connection.send<long long>(args);
}

inline Future<bool> expire(AsyncConnection &connection,
        const StringView &key,
        const std::chrono::seconds &timeout) {
    return connection.send<bool>("EXPIRE %b %lld", key.data(), key.size(), timeout.count());
}

inline Future<bool> expireat(AsyncConnection &connection,
        const StringView &key,
        const std::chrono::time_point<std::chrono::system_clock,
                                        std::chrono::seconds> &tp) {
    return connection.send<bool>("EXPIREAT %b %lld",
            key.data(), key.size(),
            tp.time_since_epoch().count());
}

inline Future<bool> pexpire(AsyncConnection &connection,
        const StringView &key,
        const std::chrono::milliseconds &timeout) {
    return connection.send<bool>("PEXPIRE %b %lld",
            key.data(), key.size(),
            timeout.count());
}

inline Future<bool> pexpireat(AsyncConnection &connection,
        const StringView &key,
        const std::chrono::time_point<std::chrono::system_clock,
                                            std::chrono::milliseconds> &tp) {
    return connection.send<bool>("PEXPIREAT %b %lld",
            key.data(), key.size(),
            tp.time_since_epoch().count());
}

inline Future<long long> pttl(AsyncConnection &connection, const StringView &key) {
    return connection.send<long long>("PTTL %b", key.data(), key.size());
}

inline Future<void> rename(AsyncConnection &connection,
        const StringView &key,
        const StringView &newkey) {
    return connection.send<void>("RENAME %b %b",
            key.data(), key.size(),
            newkey.data(), newkey.size());
}

inline Future<bool> renamenx(AsyncConnection &connection,
        const StringView &key,
        const StringView &newkey) {
    return connection.send<bool>("RENAMENX %b %b",
            key.data(), key.size(),
            newkey.data(), newkey.size());
}

inline Future<long long> ttl(AsyncConnection &connection, const StringView &key) {
    return connection.send<long long>("TTL %b", key.data(), key.size());
}

inline Future<long long> unlink(AsyncConnection &connection, const StringView &key) {
    return connection.send<long long>("UNLINK %b", key.data(), key.size());
}

template <typename Input>
Future<long long> unlink_range(AsyncConnection &connection, Input first, Input last) {
    CmdArgs args;
    args << "UNLINK" << std::make_pair(first, last);

    return connection.send<long long>(args);
}

// STRING commands.

inline Future<OptionalString> get(AsyncConnection &connection, const StringView &key) {
    return connection.send<OptionalString>("GET %b", key.data(), key.size());
}

inline Future<long long> incr(AsyncConnection &connection, const StringView &key) {
    return connection.send<long long>("INCR %b", key.data(), key.size());
}

inline Future<long long> incrby(AsyncConnection &connection,
        const StringView &key,
        long long increment) {
    return connection.send<long long>("INCRBY %b %lld",
            key.data(), key.size(),
            increment);
}

inline Future<double> incrbyfloat(AsyncConnection &connection,
        const StringView &key,
        double increment) {
    return connection.send<double>("INCRBYFLOAT %b %f",
            key.data(), key.size(),
            increment);
}

template <typename Output, typename Input>
Future<Output> mget(AsyncConnection &connection, Input first, Input last) {
    CmdArgs args;
    args << "MGET" << std::make_pair(first, last);

    return connection.send<Output>(args);
}

template <typename Input>
Future<void> mset(AsyncConnection &connection, Input first, Input last) {
    CmdArgs args;
    args << "MSET" << std::make_pair(first, last);

    return connection.send<void>(args);
}

template <typename Input>
Future<bool> msetnx(AsyncConnection &connection, Input first, Input last) {
    CmdArgs args;
    args << "MSETNX" << std::make_pair(first, last);

    return connection.send<bool>(args);
}

inline Future<bool> set(AsyncConnection &connection,
        const StringView &key,
        const StringView &val,
        const std::chrono::milliseconds &ttl,
        UpdateType type) {
    CmdArgs args;
    args << "SET" << key << val;

    if (ttl > std::chrono::milliseconds(0)) {
        args << "PX" << ttl.count();
    }

    cmd::detail::set_update_type(args, type);

    return connection.send<bool, SetResultParser>(args);
}

inline Future<long long> strlen(AsyncConnection &connection, const StringView &key) {
    return connection.send<long long>("STRLEN %b", key.data(), key.size());
}

inline Future<OptionalStringPair> blpop(AsyncConnection &connection,
        const StringView &key,
        const std::chrono::seconds &timeout) {
    return connection.send<OptionalStringPair>("BLPOP %b %lld",
            key.data(), key.size(),
            timeout.count());
}

template <typename Input>
Future<OptionalStringPair> blpop_range(AsyncConnection &connection,
        Input first,
        Input last,
        const std::chrono::seconds &timeout) {
    assert(first != last);

    CmdArgs args;
    args << "BLPOP" << std::make_pair(first, last) << timeout.count();

    return connection.send<OptionalStringPair>(args);
}

inline Future<OptionalStringPair> brpop(AsyncConnection &connection,
        const StringView &key,
        const std::chrono::seconds &timeout) {
    return connection.send<OptionalStringPair>("BRPOP %b %lld",
            key.data(), key.size(), timeout.count());
}


template <typename Input>
Future<OptionalStringPair> brpop_range(AsyncConnection &connection,
        Input first,
        Input last,
        const std::chrono::seconds &timeout) {
    assert(first != last);

    CmdArgs args;
    args << "BRPOP" << std::make_pair(first, last) << timeout.count();

    return connection.send<OptionalStringPair>(args);
}

inline Future<OptionalString> brpoplpush(AsyncConnection &connection,
        const StringView &source,
        const StringView &destination,
        const std::chrono::seconds &timeout) {
    return connection.send<OptionalString>("BRPOPLPUSH %b %b %lld",
            source.data(), source.size(),
            destination.data(), destination.size(),
            timeout.count());
}

inline Future<long long> llen(AsyncConnection &connection, const StringView &key) {
    return connection.send<long long>("LLEN %b", key.data(), key.size());
}

inline Future<OptionalString> lpop(AsyncConnection &connection, const StringView &key) {
    return connection.send<OptionalString>("LPOP %b", key.data(), key.size());
}

inline Future<long long> lpush(AsyncConnection &connection,
        const StringView &key,
        const StringView &val) {
    return connection.send<long long>("LPUSH %b %b",
            key.data(), key.size(),
            val.data(), val.size());
}

template <typename Input>
Future<long long> lpush_range(AsyncConnection &connection,
        const StringView &key,
        Input first,
        Input last) {
    assert(first != last);

    CmdArgs args;
    args << "LPUSH" << key << std::make_pair(first, last);

    return connection.send<long long>(args);
}

template <typename Output>
Future<Output> lrange(AsyncConnection &connection,
        const StringView &key,
        long long start,
        long long stop) {
    return connection.send<Output>("LRANGE %b %lld %lld",
            key.data(), key.size(),
            start, stop);
}

inline Future<long long> lrem(AsyncConnection &connection,
        const StringView &key,
        long long count,
        const StringView &val) {
    return connection.send<long long>("LREM %b %lld %b",
            key.data(), key.size(),
            count,
            val.data(), val.size());
}

inline Future<void> ltrim(AsyncConnection &connection,
        const StringView &key,
        long long start,
        long long stop) {
    return connection.send<void>("LTRIM %b %lld %lld",
            key.data(), key.size(),
            start, stop);
}

inline Future<OptionalString> rpop(AsyncConnection &connection, const StringView &key) {
    return connection.send<OptionalString>("RPOP %b", key.data(), key.size());
}

inline Future<OptionalString> rpoplpush(AsyncConnection &connection,
        const StringView &source,
        const StringView &destination) {
    return connection.send<OptionalString>("RPOPLPUSH %b %b",
            source.data(), source.size(),
            destination.data(), destination.size());
}

inline Future<long long> rpush(AsyncConnection &connection,
        const StringView &key,
        const StringView &val) {
    return connection.send<long long>("RPUSH %b %b",
            key.data(), key.size(),
            val.data(), val.size());
}

template <typename Input>
Future<long long> rpush_range(AsyncConnection &connection,
        const StringView &key,
        Input first,
        Input last) {
    assert(first != last);

    CmdArgs args;
    args << "RPUSH" << key << std::make_pair(first, last);

    return connection.send<long long>(args);
}

// HASH commands.

inline Future<long long> hdel(AsyncConnection &connection,
        const StringView &key,
        const StringView &field) {
    return connection.send<long long>("HDEL %b %b",
            key.data(), key.size(),
            field.data(), field.size());
}

template <typename Input>
Future<long long> hdel_range(AsyncConnection &connection,
        const StringView &key,
        Input first,
        Input last) {
    assert(first != last);

    CmdArgs args;
    args << "HDEL" << key << std::make_pair(first, last);

    return connection.send<long long>(args);
}

inline Future<bool> hexists(AsyncConnection &connection,
        const StringView &key,
        const StringView &field) {
    return connection.send<bool>("HEXISTS %b %b",
            key.data(), key.size(),
            field.data(), field.size());
}

inline Future<OptionalString> hget(AsyncConnection &connection,
        const StringView &key,
        const StringView &field) {
    return connection.send<OptionalString>("HGET %b %b",
            key.data(), key.size(),
            field.data(), field.size());
}

template <typename Output>
Future<Output> hgetall(AsyncConnection &connection, const StringView &key) {
    return connection.send<Output>("HGETALL %b", key.data(), key.size());
}

inline Future<long long> hincrby(AsyncConnection &connection,
        const StringView &key,
        const StringView &field,
        long long increment) {
    return connection.send<long long>("HINCRBY %b %b %lld",
            key.data(), key.size(),
            field.data(), field.size(),
            increment);
}

inline Future<double> hincrbyfloat(AsyncConnection &connection,
        const StringView &key,
        const StringView &field,
        double increment) {
    return connection.send<double>("HINCRBYFLOAT %b %b %f",
            key.data(), key.size(),
            field.data(), field.size(),
            increment);
}

template <typename Output>
Future<Output> hkeys(AsyncConnection &connection, const StringView &key) {
    return connection.send<Output>("HKEYS %b", key.data(), key.size());
}

inline Future<long long> hlen(AsyncConnection &connection, const StringView &key) {
    return connection.send<long long>("HLEN %b", key.data(), key.size());
}

template <typename Output, typename Input>
Future<Output> hmget(AsyncConnection &connection,
        const StringView &key,
        Input first,
        Input last) {
    assert(first != last);

    CmdArgs args;
    args << "HMGET" << key << std::make_pair(first, last);

    return connection.send<Output>(args);
}

template <typename Input>
Future<void> hmset(AsyncConnection &connection,
        const StringView &key,
        Input first,
        Input last) {
    assert(first != last);

    CmdArgs args;
    args << "HMSET" << key << std::make_pair(first, last);

    return connection.send<void>(args);
}

inline Future<bool> hset(AsyncConnection &connection,
        const StringView &key,
        const StringView &field,
        const StringView &val) {
    return connection.send<bool>("HSET %b %b %b",
            key.data(), key.size(),
            field.data(), field.size(),
            val.data(), val.size());
}

template <typename Input>
auto hset_range(AsyncConnection &connection,
        const StringView &key,
        Input first,
        Input last)
    -> typename std::enable_if<!std::is_convertible<Input, StringView>::value, Future<long long>>::type {
    assert(first != last);

    CmdArgs args;
    args << "HSET" << key << std::make_pair(first, last);

    return connection.send<long long>(args);
}

template <typename Output>
Future<Output> hvals(AsyncConnection &connection, const StringView &key) {
    return connection.send<Output>("HVALS %b", key.data(), key.size());
}

// SET commands.

inline Future<long long> sadd(AsyncConnection &connection,
        const StringView &key,
        const StringView &member) {
    return connection.send<long long>("SADD %b %b",
            key.data(), key.size(),
            member.data(), member.size());
}

template <typename Input>
Future<long long> sadd_range(AsyncConnection &connection,
        const StringView &key,
        Input first,
        Input last) {
    assert(first != last);

    CmdArgs args;
    args << "SADD" << key << std::make_pair(first, last);

    return connection.send<long long>(args);
}

inline Future<long long> scard(AsyncConnection &connection, const StringView &key) {
    return connection.send<long long>("SCARD %b", key.data(), key.size());
}

inline Future<bool> sismember(AsyncConnection &connection,
        const StringView &key,
        const StringView &member) {
    return connection.send<bool>("SISMEMBER %b %b",
            key.data(), key.size(),
            member.data(), member.size());
}

template <typename Output>
Future<Output> smembers(AsyncConnection &connection, const StringView &key) {
    return connection.send<Output>("SMEMBERS %b", key.data(), key.size());
}

inline Future<OptionalString> spop(AsyncConnection &connection, const StringView &key) {
    return connection.send<OptionalString>("SPOP %b", key.data(), key.size());
}

template <typename Output>
Future<Output> spop_count(AsyncConnection &connection,
        const StringView &key,
        long long count) {
    return connection.send<Output>("SPOP %b %lld",
            key.data(), key.size(),
            count);
}

inline Future<long long> srem(AsyncConnection &connection,
        const StringView &key,
        const StringView &member) {
    return connection.send<long long>("SREM %b %b",
            key.data(), key.size(),
            member.data(), member.size());
}

template <typename Input>
Future<long long> srem_range(AsyncConnection &connection,
        const StringView &key,
        Input first,
        Input last) {
    assert(first != last);

    CmdArgs args;
    args << "SREM" << key << std::make_pair(first, last);

    return connection.send<long long>(args);
}

// SORTED SET commands.

inline auto bzpopmax(AsyncConnection &connection,
        const StringView &key,
        const std::chrono::seconds &timeout)
    -> Future<Optional<std::tuple<std::string, std::string, double>>> {
    return connection.send<Optional<std::tuple<std::string, std::string, double>>>(
            "BZPOPMAX %b %lld",
            key.data(), key.size(),
            timeout.count());
}

template <typename Input>
auto bzpopmax_range(AsyncConnection &connection,
        Input first,
        Input last,
        const std::chrono::seconds &timeout)
    -> Future<Optional<std::tuple<std::string, std::string, double>>> {
    assert(first != last);

    CmdArgs args;
    args << "BZPOPMAX" << std::make_pair(first, last) << timeout.count();

    return connection.send<Optional<std::tuple<std::string, std::string, double>>>(args);
}

inline auto bzpopmin(AsyncConnection &connection,
        const StringView &key,
        const std::chrono::seconds &timeout)
    -> Future<Optional<std::tuple<std::string, std::string, double>>> {
    return connection.send<Optional<std::tuple<std::string, std::string, double>>>(
            "BZPOPMIN %b %lld",
            key.data(), key.size(),
            timeout.count());
}

template <typename Input>
auto bzpopmin_range(AsyncConnection &connection,
        Input first,
        Input last,
        const std::chrono::seconds &timeout)
    -> Future<Optional<std::tuple<std::string, std::string, double>>> {
    assert(first != last);

    CmdArgs args;
    args << "BZPOPMIN" << std::make_pair(first, last) << timeout.count();

    return connection.send<Optional<std::tuple<std::string, std::string, double>>>(args);
}

inline Future<long long> zadd(AsyncConnection &connection,
        const StringView &key,
        const StringView &member,
        double score,
        UpdateType type,
        bool changed) {
    CmdArgs args;
    args << "ZADD" << key;

    cmd::detail::set_update_type(args, type);

    if (changed) {
        args << "CH";
    }

    args << score << member;

    return connection.send<long long>(args);
}

template <typename Input>
Future<long long> zadd_range(AsyncConnection &connection,
        const StringView &key,
        Input first,
        Input last,
        UpdateType type,
        bool changed) {
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

    return connection.send<long long>(args);
}

inline Future<long long> zcard(AsyncConnection &connection, const StringView &key) {
    return connection.send<long long>("ZCARD %b", key.data(), key.size());
}

template <typename Interval>
Future<long long> zcount(AsyncConnection &connection,
        const StringView &key,
        const Interval &interval) {
    return connection.send<long long>("ZCOUNT %b %s %s",
            key.data(), key.size(),
            interval.min().c_str(),
            interval.max().c_str());
}

inline Future<double> zincrby(AsyncConnection &connection,
        const StringView &key,
        double increment,
        const StringView &member) {
    return connection.send<double>("ZINCRBY %b %f %b",
            key.data(), key.size(),
            increment,
            member.data(), member.size());
}

template <typename Interval>
Future<long long> zlexcount(AsyncConnection &connection,
        const StringView &key,
        const Interval &interval) {
    const auto &min = interval.min();
    const auto &max = interval.max();

    return connection.send<long long>("ZLEXCOUNT %b %b %b",
                    key.data(), key.size(),
                    min.data(), min.size(),
                    max.data(), max.size());
}

inline Future<Optional<std::pair<std::string, double>>> zpopmax(AsyncConnection &connection,
        const StringView &key) {
    return connection.send<Optional<std::pair<std::string, double>>>("ZPOPMAX %b",
            key.data(), key.size());
}

template <typename Output>
Future<Output> zpopmax_count(AsyncConnection &connection,
        const StringView &key,
        long long count) {
    return connection.send<Output>("ZPOPMAX %b %lld",
            key.data(), key.size(),
            count);
}

inline Future<Optional<std::pair<std::string, double>>> zpopmin(AsyncConnection &connection,
        const StringView &key) {
    return connection.send<Optional<std::pair<std::string, double>>>("ZPOPMIN %b",
            key.data(), key.size());
}

template <typename Output>
Future<Output> zpopmin_count(AsyncConnection &connection,
        const StringView &key,
        long long count) {
    return connection.send<Output>("ZPOPMIN %b %lld",
            key.data(), key.size(),
            count);
}

template <typename Output>
Future<Output> zrange(AsyncConnection &connection,
        const StringView &key,
        long long start,
        long long stop) {
    return connection.send<Output>("ZRANGE %b %lld %lld",
            key.data(), key.size(),
            start, stop);
}

template <typename Output, typename Interval>
Future<Output> zrangebylex(AsyncConnection &connection,
        const StringView &key,
        const Interval &interval,
        const LimitOptions &opts) {
    const auto &min = interval.min();
    const auto &max = interval.max();

    return connection.send<Output>("ZRANGEBYLEX %b %b %b LIMIT %lld %lld",
                    key.data(), key.size(),
                    min.data(), min.size(),
                    max.data(), max.size(),
                    opts.offset,
                    opts.count);
}

template <typename Output, typename Interval>
Future<Output> zrangebyscore(AsyncConnection &connection,
        const StringView &key,
        const Interval &interval,
        const LimitOptions &opts) {
    const auto &min = interval.min();
    const auto &max = interval.max();

    return connection.send<Output>("ZRANGEBYSCORE %b %b %b LIMIT %lld %lld",
                    key.data(), key.size(),
                    min.data(), min.size(),
                    max.data(), max.size(),
                    opts.offset,
                    opts.count);
}

inline Future<OptionalLongLong> rank(AsyncConnection &connection,
        const StringView &key,
        const StringView &member) {
    return connection.send<OptionalLongLong>("RANK %b %b",
            key.data(), key.size(),
            member.data(), member.size());
}

inline Future<long long> zrem(AsyncConnection &connection,
        const StringView &key,
        const StringView &member) {
    return connection.send<long long>("ZREM %b %b",
            key.data(), key.size(),
            member.data(), member.size());
}

template <typename Input>
Future<long long> zrem_range(AsyncConnection &connection,
        const StringView &key,
        Input first,
        Input last) {
    assert(first != last);

    CmdArgs args;
    args << "ZREM" << key << std::make_pair(first, last);

    return connection.send<long long>(args);
}

template <typename Interval>
Future<long long> zremrangebylex(AsyncConnection &connection,
        const StringView &key,
        const Interval &interval) {
    const auto &min = interval.min();
    const auto &max = interval.max();

    return connection.send<long long>("ZREMRANGEBYLEX %b %b %b",
                    key.data(), key.size(),
                    min.data(), min.size(),
                    max.data(), max.size());
}

inline Future<long long> zremrangebyrank(AsyncConnection &connection,
        const StringView &key,
        long long start,
        long long stop) {
    return connection.send<long long>("ZREMRANGEBYRANK %b %lld %lld",
            key.data(), key.size(),
            start, stop);
}

template <typename Interval>
Future<long long> zremrangebyscore(AsyncConnection &connection,
        const StringView &key,
        const Interval &interval) {
    const auto &min = interval.min();
    const auto &max = interval.max();

    return connection.send<long long>("ZREMRANGEBYSCORE %b %b %b",
                    key.data(), key.size(),
                    min.data(), min.size(),
                    max.data(), max.size());
}

template <typename Output, typename Interval>
Future<Output> zrevrangebylex(AsyncConnection &connection,
        const StringView &key,
        const Interval &interval,
        const LimitOptions &opts) {
    const auto &min = interval.min();
    const auto &max = interval.max();

    return connection.send<Output>("ZREVRANGEBYLEX %b %b %b LIMIT %lld %lld",
                    key.data(), key.size(),
                    max.data(), max.size(),
                    min.data(), min.size(),
                    opts.offset,
                    opts.count);
}

inline Future<OptionalLongLong> zrevrank(AsyncConnection &connection,
        const StringView &key,
        const StringView &member) {
    return connection.send<OptionalLongLong>("ZREVRANK %b %b",
            key.data(), key.size(),
            member.data(), member.size());
}

inline Future<OptionalDouble> zscore(AsyncConnection &connection,
        const StringView &key,
        const StringView &member) {
    return connection.send<OptionalDouble>("ZSCORE %b %b",
            key.data(), key.size(),
            member.data(), member.size());
}

// SCRIPTING commands.
template <typename Result, typename Keys, typename Args>
Future<Result> eval(AsyncConnection &connection,
        const StringView &script,
        Keys keys_first,
        Keys keys_last,
        Args args_first,
        Args args_last) {
    CmdArgs args;
    auto keys_num = std::distance(keys_first, keys_last);

    args << "EVAL" << script << keys_num
            << std::make_pair(keys_first, keys_last)
            << std::make_pair(args_first, args_last);

    return connection.send<Result>(args);
}

template <typename Result, typename Keys, typename Args>
Future<Result> evalsha(AsyncConnection &connection,
        const StringView &script,
        Keys keys_first,
        Keys keys_last,
        Args args_first,
        Args args_last) {
    CmdArgs args;
    auto keys_num = std::distance(keys_first, keys_last);

    args << "EVALSHA" << script << keys_num
            << std::make_pair(keys_first, keys_last)
            << std::make_pair(args_first, args_last);

    return connection.send<Result>(args);
}

}

}

}

#endif // end SEWENEW_REDISPLUSPLUS_ASYNC_COMMAND_H
