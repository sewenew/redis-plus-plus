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

#ifndef SEWENEW_REDISPLUSPLUS_R_SORTED_SET_H
#define SEWENEW_REDISPLUSPLUS_R_SORTED_SET_H

#include <string>
#include "reply.h"
#include "command.h"
#include "redis.h"
#include "utils.h"

namespace sw {

namespace redis {

// Redis' SORTED SET type.
class RSortedSet {
public:
    // We don't support the INCR option, since you can always use ZINCRBY instead.
    long long zadd(double score,
                    const StringView &member,
                    bool changed = false,
                    cmd::UpdateType type = cmd::UpdateType::ALWAYS);

    template <typename Input>
    long long zadd(Input first,
                    Input last,
                    bool changed = false,
                    cmd::UpdateType type = cmd::UpdateType::ALWAYS);

    long long zcard();

    template <typename Interval>
    long long zcount(const Interval &interval);

    double zincrby(double increment, const StringView &member);

    template <typename Interval>
    long long zlexcount(const Interval &interval);

    template <typename Output>
    void zrange(long long start, long long stop, Output output);

    template <typename Interval, typename Output>
    void zrangebylex(const Interval &interval, Output output);

    template <typename Interval, typename Output>
    void zrangebylex(const Interval &interval, const LimitOptions &opts, Output output);

    template <typename Interval, typename Output>
    void zrangebyscore(const Interval &interval, Output output);

    template <typename Interval, typename Output>
    void zrangebyscore(const Interval &interval, const LimitOptions &opts, Output output);

    OptionalLongLong zrank(const StringView &member);

    long long zrem(const StringView &member);

    template <typename Input>
    long long zrem(Input first, Input last);

    template <typename Interval>
    long long zremrangebylex(const Interval &interval);

    long long zremrangebyrank(long long start, long long stop);

    template <typename Interval>
    long long zremrangebyscore(const Interval &interval);

    template <typename Output>
    void zrevrange(long long start, long long stop, Output output);

private:
    friend class Redis;

    RSortedSet(const std::string &key, Redis &redis) : _key(key), _redis(redis) {}

    template <typename Output, typename ...Args>
    void _score_command(Output output, Args &&...args);

    template <typename Output, typename ...Args>
    void _score_command(std::true_type, Output output, Args &&...args);

    template <typename Output, typename ...Args>
    void _score_command(std::false_type, Output output, Args &&...args);

    template <typename Output, typename ...Args>
    void _score_command_impl(std::true_type, Output output, Args &&...args);

    template <typename Output, typename ...Args>
    void _score_command_impl(std::false_type, Output output, Args &&...args);

    std::string _key;

    Redis &_redis;
};

template <typename Input>
long long RSortedSet::zadd(Input first, Input last, bool changed, cmd::UpdateType type) {
    auto reply = _redis.command(cmd::zadd_range<Input>, _key, first, last, changed, type);

    return reply::to_integer(*reply);
}

template <typename Interval>
long long RSortedSet::zcount(const Interval &interval) {
    auto reply = _redis.command(cmd::zcount<Interval>, _key, interval);

    return reply::to_integer(*reply);
}

template <typename Interval>
long long RSortedSet::zlexcount(const Interval &interval) {
    auto reply = _redis.command(cmd::zlexcount<Interval>, _key, interval);

    return reply::to_integer(*reply);
}

template <typename Output>
void RSortedSet::zrange(long long start, long long stop, Output output) {
    _score_command(output, cmd::zrange, _key, start, stop);
}

template <typename Interval, typename Output>
void RSortedSet::zrangebylex(const Interval &interval, Output output) {
    zrangebylex(interval, {}, output);
}

template <typename Interval, typename Output>
void RSortedSet::zrangebylex(const Interval &interval, const LimitOptions &opts, Output output) {
    auto reply = _redis.command(cmd::zrangebylex<Interval>, _key, interval, opts);

    reply::to_array(*reply, output);
}

template <typename Interval, typename Output>
void RSortedSet::zrangebyscore(const Interval &interval,
                                Output output) {
    zrangebyscore(interval, {}, output);
}

template <typename Interval, typename Output>
void RSortedSet::zrangebyscore(const Interval &interval,
                                const LimitOptions &opts,
                                Output output) {
    _score_command(output, cmd::zrangebyscore<Interval>, _key, interval, opts);
}

template <typename Input>
long long RSortedSet::zrem(Input first, Input last) {
    auto reply = _redis.command(cmd::zrem_range<Input>, _key, first, last);

    return reply::to_integer(*reply);
}

template <typename Interval>
long long RSortedSet::zremrangebylex(const Interval &interval) {
    auto reply = _redis.command(cmd::zremrangebylex<Interval>, _key, interval);

    return reply::to_integer(*reply);
}

template <typename Interval>
long long RSortedSet::zremrangebyscore(const Interval &interval) {
    auto reply = _redis.command(cmd::zremrangebyscore, _key, interval);

    return reply::to_integer(*reply);
}

template <typename Output>
void RSortedSet::zrevrange(long long start, long long stop, Output output) {
    _score_command(output, cmd::zrevrange, _key, start, stop);
}

template <typename Output, typename ...Args>
void RSortedSet::_score_command(Output output, Args &&...args) {
    _score_command(typename IsInserter<Output>::type(),
                    output,
                    std::forward<Args>(args)...);
}

template <typename Output, typename ...Args>
void RSortedSet::_score_command(std::true_type, Output output, Args &&...args) {
    // std::inserter or std::back_inserter
    _score_command_impl(IsKvPair<typename Output::container_type::value_type>(),
                        output,
                        std::forward<Args>(args)...);
}

template <typename Output, typename ...Args>
void RSortedSet::_score_command(std::false_type, Output output, Args &&...args) {
    // Normal iterator
    _score_command_impl(IsKvPair<typename std::decay<decltype(*output)>::type>(),
                        output,
                        std::forward<Args>(args)...);
}

template <typename Output, typename ...Args>
void RSortedSet::_score_command_impl(std::true_type, Output output, Args &&...args) {
    auto reply = _redis.command(std::forward<Args>(args)..., true);

    reply::to_array(*reply, output);
}

template <typename Output, typename ...Args>
void RSortedSet::_score_command_impl(std::false_type, Output output, Args &&...args) {
    auto reply = _redis.command(std::forward<Args>(args)..., false);

    reply::to_array(*reply, output);
}

}

}

#endif // end SEWENEW_REDISPLUSPLUS_R_SORTED_SET_H
