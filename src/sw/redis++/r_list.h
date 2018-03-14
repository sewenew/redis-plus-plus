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

#ifndef SEWENEW_REDISPLUSPLUS_R_LIST_H
#define SEWENEW_REDISPLUSPLUS_R_LIST_H

#include <string>
#include "reply.h"
#include "command.h"
#include "redis.h"
#include "utils.h"

namespace sw {

namespace redis {

class StringView;

// Redis' LIST type.
class RList {
public:
    const std::string& key() const {
        return _key;
    }

    OptionalString lpop();

    OptionalString lindex(long long index);

    long long linsert(const StringView &val,
                        cmd::InsertPosition position,
                        const StringView &pivot);

    long long llen();

    long long lpush(const StringView &val);

    template <typename Iter>
    long long lpush(Iter first, Iter last);

    long long lpushx(const StringView &val);

    template <typename Iter>
    void lrange(long long start, long long stop, Iter output);

    long long lrem(const StringView &val, long long count = 0);

    void lset(long long index, const StringView &val);

    void ltrim(long long start, long long stop);

    OptionalString rpop();

    long long rpush(const StringView &val);

    template <typename Iter>
    long long rpush(Iter first, Iter last);

    long long rpushx(const StringView &val);

private:
    friend class Redis;

    RList(const std::string &key, Redis &redis) : _key(key), _redis(redis) {}

    std::string _key;

    Redis &_redis;
};

template <typename Iter>
inline long long RList::lpush(Iter first, Iter last) {
    auto reply = _redis.command(cmd::lpush_range<Iter>, _key, first, last);

    return reply::to_integer(*reply);
}

template <typename Iter>
inline void RList::lrange(long long start, long long stop, Iter output) {
    auto reply = _redis.command(cmd::lrange, _key, start, stop);

    reply::to_array(*reply, output);
}

template <typename Iter>
inline long long RList::rpush(Iter first, Iter last) {
    auto reply = _redis.command(cmd::rpush_range<Iter>, _key, first, last);

    return reply::to_integer(*reply);
}

}

}

#endif // end SEWENEW_REDISPLUSPLUS_R_LIST_H
