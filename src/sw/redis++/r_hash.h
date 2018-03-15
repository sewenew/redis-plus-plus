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

#ifndef SEWENEW_REDISPLUSPLUS_R_HASH_H
#define SEWENEW_REDISPLUSPLUS_R_HASH_H

#include <string>
#include "reply.h"
#include "command.h"
#include "redis.h"
#include "utils.h"

namespace sw {

namespace redis {

// Redis' HASH type.
class RHash {
public:
    const std::string& key() const {
        return _key;
    }

    long long hdel(const StringView &field);

    template <typename Iter>
    long long hdel(Iter first, Iter last);

    bool hexists(const StringView &field);

    OptionalString hget(const StringView &field);

    template <typename Iter>
    void hgetall(Iter output);

    long long hincrby(const StringView &field, long long increment);

    double hincrbyfloat(const StringView &field, double increment);

    template <typename Iter>
    void hkeys(Iter output);

    long long hlen();

    template <typename Input, typename Output>
    void hmget(Input first, Input last, Output output);

    template <typename Iter>
    void hmset(Iter first, Iter last);

    bool hset(const StringView &field, const StringView &val);

    bool hsetnx(const StringView &field, const StringView &val);

    long long hstrlen(const StringView &field);

    template <typename Iter>
    void hvals(Iter output);

private:
    friend class Redis;

    RHash(const std::string &key, Redis &redis) : _key(key), _redis(redis) {}

    std::string _key;

    Redis &_redis;
};

// Inline implementations.

template <typename Iter>
inline long long RHash::hdel(Iter first, Iter last) {
    auto reply = _redis.command(cmd::hdel_range<Iter>, _key, first, last);

    return reply::to_integer(*reply);
}

template <typename Iter>
inline void RHash::hgetall(Iter output) {
    auto reply = _redis.command(cmd::hgetall, _key);

    reply::to_array(*reply, output);
}

template <typename Iter>
inline void RHash::hkeys(Iter output) {
    auto reply = _redis.command(cmd::hkeys, _key);

    reply::to_array(*reply, output);
}

template <typename Input, typename Output>
inline void RHash::hmget(Input first, Input last, Output output) {
    auto reply = _redis.command(cmd::hmget<Input>, _key, first, last);

    reply::to_optional_string_array(*reply, output);
}

template <typename Iter>
inline void RHash::hmset(Iter first, Iter last) {
    auto reply = _redis.command(cmd::hmset<Iter>, _key, first, last);

    if (!reply::status_ok(*reply)) {
        throw RException("Invalid status reply: " + reply::to_status(*reply));
    }
}

template <typename Iter>
inline void RHash::hvals(Iter output) {
    auto reply = _redis.command(cmd::hvals, _key);

    reply::to_array(*reply, output);
}

}

}

#endif // end SEWENEW_REDISPLUSPLUS_R_HASH_H
