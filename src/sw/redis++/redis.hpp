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

#include "command.h"
#include "reply.h"
#include "utils.h"

namespace sw {

namespace redis {

template <typename Cmd, typename ...Args>
ReplyUPtr Redis::command(Cmd cmd, Args &&...args) {
    auto connection = _pool.fetch();

    ConnectionPoolGuard guard(_pool, connection);

    cmd(connection, std::forward<Args>(args)...);

    auto reply = connection.recv();

    return reply;
}

// KEY commands.

template <typename Input>
long long Redis::del(Input first, Input last) {
    auto reply = command(cmd::del_range<Input>, first, last);

    return reply::to_integer(*reply);
}

template <typename Input>
long long Redis::exists(Input first, Input last) {
    auto reply = command(cmd::exists_range<Input>, first, last);

    return reply::to_integer(*reply);
}

// STRING commands.

template <typename Input>
long long Redis::bitop(BitOp op, const StringView &destination, Input first, Input last) {
    auto reply = command(cmd::bitop<Input>, op, destination, first, last);

    return reply::to_integer(*reply);
}

template <typename Input, typename Output>
void Redis::mget(Input first, Input last, Output output) {
    auto reply = command(cmd::mget<Input>, first, last);

    reply::to_optional_string_array(*reply, output);
}

template <typename Input>
void Redis::mset(Input first, Input last) {
    auto reply = command(cmd::mset<Input>, first, last);

    if (!reply::status_ok(*reply)) {
        throw RException("Invalid status reply: " + reply::to_status(*reply));
    }
}

template <typename Input>
bool Redis::msetnx(Input first, Input last) {
    auto reply = command(cmd::msetnx<Input>, first, last);

    return reply::to_bool(*reply);
}

// LIST commands.

template <typename Input>
OptionalStringPair Redis::blpop(Input first,
                                Input last,
                                const std::chrono::seconds &timeout) {
    auto reply = command(cmd::blpop<Input>, first, last, timeout);

    return reply::to_optional_string_pair(*reply);
}

template <typename Input>
OptionalStringPair Redis::brpop(Input first,
                                Input last,
                                const std::chrono::seconds &timeout) {
    auto reply = command(cmd::brpop<Input>, first, last, timeout);

    return reply::to_optional_string_pair(*reply);
}

template <typename Iter>
inline long long Redis::lpush(const StringView &key, Iter first, Iter last) {
    auto reply = command(cmd::lpush_range<Iter>, key, first, last);

    return reply::to_integer(*reply);
}

template <typename Iter>
inline void Redis::lrange(const StringView &key, long long start, long long stop, Iter output) {
    auto reply = command(cmd::lrange, key, start, stop);

    reply::to_array(*reply, output);
}

template <typename Iter>
inline long long Redis::rpush(const StringView &key, Iter first, Iter last) {
    auto reply = command(cmd::rpush_range<Iter>, key, first, last);

    return reply::to_integer(*reply);
}

// HASH commands.

template <typename Iter>
inline long long Redis::hdel(const StringView &key, Iter first, Iter last) {
    auto reply = command(cmd::hdel_range<Iter>, key, first, last);

    return reply::to_integer(*reply);
}

template <typename Iter>
inline void Redis::hgetall(const StringView &key, Iter output) {
    auto reply = command(cmd::hgetall, key);

    reply::to_array(*reply, output);
}

template <typename Iter>
inline void Redis::hkeys(const StringView &key, Iter output) {
    auto reply = command(cmd::hkeys, key);

    reply::to_array(*reply, output);
}

template <typename Input, typename Output>
inline void Redis::hmget(const StringView &key, Input first, Input last, Output output) {
    auto reply = command(cmd::hmget<Input>, key, first, last);

    reply::to_optional_string_array(*reply, output);
}

template <typename Iter>
inline void Redis::hmset(const StringView &key, Iter first, Iter last) {
    auto reply = command(cmd::hmset<Iter>, key, first, last);

    if (!reply::status_ok(*reply)) {
        throw RException("Invalid status reply: " + reply::to_status(*reply));
    }
}

template <typename Iter>
inline void Redis::hvals(const StringView &key, Iter output) {
    auto reply = command(cmd::hvals, key);

    reply::to_array(*reply, output);
}

// SET commands.

template <typename Iter>
long long Redis::sadd(const StringView &key, Iter first, Iter last) {
    auto reply = command(cmd::sadd_range<Iter>, key, first, last);

    return reply::to_integer(*reply);
}

template <typename Input, typename Output>
void Redis::sdiff(Input first, Input last, Output output) {
    auto reply = command(cmd::sdiff<Input>, first, last);

    reply::to_array(*reply, output);
}

template <typename Input>
long long Redis::sdiffstore(const StringView &destination,
                            Input first,
                            Input last) {
    auto reply = command(cmd::sdiffstore<Input>, destination, first, last);

    return reply::to_integer(*reply);
}

template <typename Input, typename Output>
void Redis::sinter(Input first, Input last, Output output) {
    auto reply = command(cmd::sinter<Input>, first, last);

    reply::to_array(*reply, output);
}

template <typename Input>
long long Redis::sinterstore(const StringView &destination,
                            Input first,
                            Input last) {
    auto reply = command(cmd::sinterstore<Input>, destination, first, last);

    return reply::to_integer(*reply);
}

template <typename Iter>
void Redis::smembers(const StringView &key, Iter output) {
    auto reply = command(cmd::smembers, key);

    reply::to_array(*reply, output);
}

template <typename Iter>
void Redis::spop(const StringView &key, long long count, Iter output) {
    auto reply = command(cmd::spop_range, key, count);

    reply::to_array(*reply, output);
}

template <typename Iter>
void Redis::srandmember(const StringView &key, long long count, Iter output) {
    auto reply = command(cmd::srandmember_range, key, count);

    reply::to_array(*reply, output);
}

template <typename Iter>
long long Redis::srem(const StringView &key, Iter first, Iter last) {
    auto reply = command(cmd::srem_range<Iter>, key, first, last);

    return reply::to_integer(*reply);
}

template <typename Input, typename Output>
void Redis::sunion(Input first, Input last, Output output) {
    auto reply = command(cmd::sunion<Input>, first, last);

    reply::to_array(*reply, output);
}

template <typename Input>
long long Redis::sunionstore(const StringView &destination, Input first, Input last) {
    auto reply = command(cmd::sunionstore<Input>, destination, first, last);

    return reply::to_integer(*reply);
}

// SORTED SET commands.

template <typename Input>
long long Redis::zadd(const StringView &key,
                        Input first,
                        Input last,
                        bool changed,
                        UpdateType type) {
    auto reply = command(cmd::zadd_range<Input>, key, first, last, changed, type);

    return reply::to_integer(*reply);
}

template <typename Interval>
long long Redis::zcount(const StringView &key, const Interval &interval) {
    auto reply = command(cmd::zcount<Interval>, key, interval);

    return reply::to_integer(*reply);
}

template <typename Input>
long long Redis::zinterstore(const StringView &destination,
                                Input first,
                                Input last,
                                Aggregation type) {
    auto reply = command(cmd::zinterstore<Input>,
                            destination,
                            first,
                            last,
                            type);

    return reply::to_integer(*reply);
}

template <typename Interval>
long long Redis::zlexcount(const StringView &key, const Interval &interval) {
    auto reply = command(cmd::zlexcount<Interval>, key, interval);

    return reply::to_integer(*reply);
}

template <typename Output>
void Redis::zrange(const StringView &key, long long start, long long stop, Output output) {
    auto reply = command(cmd::zrange<Output>, key, start, stop);

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
    auto reply = command(cmd::zrangebyscore<Interval, Output>,
                            key,
                            interval,
                            opts);

    reply::to_array(*reply, output);
}

template <typename Input>
long long Redis::zrem(const StringView &key, Input first, Input last) {
    auto reply = command(cmd::zrem_range<Input>, key, first, last);

    return reply::to_integer(*reply);
}

template <typename Interval>
long long Redis::zremrangebylex(const StringView &key, const Interval &interval) {
    auto reply = command(cmd::zremrangebylex<Interval>, key, interval);

    return reply::to_integer(*reply);
}

template <typename Interval>
long long Redis::zremrangebyscore(const StringView &key, const Interval &interval) {
    auto reply = command(cmd::zremrangebyscore, key, interval);

    return reply::to_integer(*reply);
}

template <typename Output>
void Redis::zrevrange(const StringView &key, long long start, long long stop, Output output) {
    auto reply = command(cmd::zrevrange<Output>, key, start, stop);

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
    auto reply = command(cmd::zrevrangebyscore<Interval, Output>, key, interval, opts);

    reply::to_array(*reply, output);
}

template <typename Input>
long long Redis::zunionstore(const StringView &destination,
                                    Input first,
                                    Input last,
                                    Aggregation type) {
    auto reply = command(cmd::zunionstore<Input>,
                            destination,
                            first,
                            last,
                            type);

    return reply::to_integer(*reply);
}

// HYPERLOGLOG commands.

template <typename Input>
bool Redis::pfadd(const StringView &key, Input first, Input last) {
    auto reply = command(cmd::pfadd_range<Input>, key, first, last);

    return reply::to_bool(*reply);
}

template <typename Input>
long long Redis::pfcount(Input first, Input last) {
    auto reply = command(cmd::pfcount_range<Input>, first, last);

    return reply::to_integer(*reply);
}

template <typename Input>
void Redis::pfmerge(const StringView &destination,
                    Input first,
                    Input last) {
    auto reply = command(cmd::pfmerge, destination, first, last);

    if (!reply::status_ok(*reply)) {
        throw RException("Invalid status reply: " + reply::to_status(*reply));
    }
}

// GEO commands.

template <typename Input>
inline long long Redis::geoadd(const StringView &key,
                                Input first,
                                Input last) {
    auto reply = command(cmd::geoadd_range<Input>, key, first, last);

    return reply::to_integer(*reply);
}

template <typename Input, typename Output>
void Redis::geohash(const StringView &key, Input first, Input last, Output output) {
    auto reply = command(cmd::geohash_range<Input>, key, first, last);

    reply::to_optional_string_array(*reply, output);
}

}

}

#endif // end SEWENEW_REDISPLUSPLUS_REDIS_HPP
