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

private:
    friend class Redis;

    RSortedSet(const std::string &key, Redis &redis) : _key(key), _redis(redis) {}

    template <typename Output>
    void _zrange_impl(std::true_type, long long start, long long stop, Output output);

    template <typename Output>
    void _zrange_impl(std::false_type, long long start, long long stop, Output output);

    template <typename Output>
    void _zrange(std::true_type, long long start, long long stop, Output output) {
        // std::inserter or std::back_inserter
        _zrange_impl(IsKvPair<typename Output::container_type::value_type>(),
                        start,
                        stop,
                        output);
    }

    template <typename Output>
    void _zrange(std::false_type, long long start, long long stop, Output output) {
        // Normal iterator
        _zrange_impl(IsKvPair<typename std::decay<decltype(*output)>::type>(),
                        start,
                        stop,
                        output);
    }

    template <typename Interval, typename Output>
    void _zrangebyscore_impl(std::true_type,
                                const Interval &interval,
                                const LimitOptions &opts,
                                Output output);

    template <typename Interval, typename Output>
    void _zrangebyscore_impl(std::false_type,
                                const Interval &interval,
                                const LimitOptions &opts,
                                Output output);

    template <typename Interval, typename Output>
    void _zrangebyscore(std::true_type,
                        const Interval &interval,
                        const LimitOptions &opts,
                        Output output) {
        // std::inserter or std::back_inserter
        _zrangebyscore_impl(IsKvPair<typename Output::container_type::value_type>(),
                            interval,
                            opts,
                            output);
    }

    template <typename Interval, typename Output>
    void _zrangebyscore(std::false_type,
                        const Interval &interval,
                        const LimitOptions &opts,
                        Output output) {
        // Normal iterator
        _zrangebyscore_impl(IsKvPair<typename std::decay<decltype(*output)>::type>(),
                            interval,
                            opts,
                            output);
    }

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
    _zrange(typename IsInserter<Output>::type(), start, stop, output);
}

template <typename Output>
void RSortedSet::_zrange_impl(std::true_type, long long start, long long stop, Output output) {
    // With scores
    auto reply = _redis.command(cmd::zrange, _key, start, stop, true);

    std::vector<std::string> tmp;
    reply::to_string_array(*reply, std::back_inserter(tmp));

    if (tmp.size() % 2 != 0) {
        throw RException("Score member pairs DO NOT match.");
    }

    for (std::size_t idx = 0; idx != tmp.size(); idx += 2) {
        *output = std::make_pair(std::move(tmp[idx]), std::stod(tmp[idx+1]));
        ++output;
    }
}

template <typename Output>
void RSortedSet::_zrange_impl(std::false_type, long long start, long long stop, Output output) {
    // Without scores
    auto reply = _redis.command(cmd::zrange, _key, start, stop, false);

    reply::to_string_array(*reply, output);
}

template <typename Interval, typename Output>
void RSortedSet::zrangebylex(const Interval &interval, Output output) {
    zrangebylex(interval, {}, output);
}

template <typename Interval, typename Output>
void RSortedSet::zrangebylex(const Interval &interval, const LimitOptions &opts, Output output) {
    auto reply = _redis.command(cmd::zrangebylex<Interval>, _key, interval, opts);

    reply::to_string_array(*reply, output);
}

template <typename Interval, typename Output>
void RSortedSet::zrangebyscore(const Interval &interval,
                                Output output) {
    _zrangebyscore(typename IsInserter<Output>::type(), interval, {}, output);
}

template <typename Interval, typename Output>
void RSortedSet::zrangebyscore(const Interval &interval,
                                const LimitOptions &opts,
                                Output output) {
    _zrangebyscore(typename IsInserter<Output>::type(), interval, opts, output);
}

template <typename Interval, typename Output>
void RSortedSet::_zrangebyscore_impl(std::true_type,
                                        const Interval &interval,
                                        const LimitOptions &opts,
                                        Output output) {
    // With scores
    auto reply = _redis.command(cmd::zrangebyscore<Interval>, _key, interval, true, opts);

    std::vector<std::string> tmp;
    reply::to_string_array(*reply, std::back_inserter(tmp));

    if (tmp.size() % 2 != 0) {
        throw RException("Score member pairs DO NOT match.");
    }

    for (std::size_t idx = 0; idx != tmp.size(); idx += 2) {
        *output = std::make_pair(std::move(tmp[idx]), std::stod(tmp[idx+1]));
        ++output;
    }
}

template <typename Interval, typename Output>
void RSortedSet::_zrangebyscore_impl(std::false_type,
                                        const Interval &interval,
                                        const LimitOptions &opts,
                                        Output output) {
    // Without scores
    auto reply = _redis.command(cmd::zrangebyscore<Interval>, _key, interval, false, opts);

    reply::to_string_array(*reply, output);
}
}

}

#endif // end SEWENEW_REDISPLUSPLUS_R_SORTED_SET_H
