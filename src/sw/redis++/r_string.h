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

#ifndef SEWENEW_REDISPLUSPLUS_R_STRING_H
#define SEWENEW_REDISPLUSPLUS_R_STRING_H

#include <string>
#include "reply.h"
#include "redis.h"
#include "command.h"
#include "utils.h"

namespace sw {

namespace redis {

class StringView;

// Redis' STRING type.
class RString {
public:
    const std::string& key() const {
        return _key;
    }

    long long append(const StringView &str);

    long long bitcount(long long start = 0, long long end = -1);

    long long decr();

    long long decrby(long long decrement);

    OptionalString get();

    long long getbit(long long offset);

    std::string getrange(long long start, long long end);

    OptionalString getset(const StringView &val);

    long long incr();

    long long incrby(long long increment);

    double incrbyfloat(double increment);

    template <typename Input, typename Output>
    void mget(Input first, Input last, Output output);

    template <typename Input>
    void mset(Input first, Input last);

    template <typename Input>
    bool msetnx(Input first, Input last);

    void psetex(const StringView &val,
                const std::chrono::milliseconds &ttl);

    bool set(const StringView &val,
                const std::chrono::milliseconds &ttl = std::chrono::milliseconds(0),
                cmd::UpdateType type = cmd::UpdateType::ALWAYS);

    long long setbit(long long offset, long long value);

    bool setnx(const StringView &val);

    void setex(const StringView &val, const std::chrono::seconds &ttl);

    long long setrange(long long offset, const StringView &val);

    long long strlen();

private:
    friend class Redis;

    RString(const std::string &key, Redis &redis) : _key(key), _redis(redis) {}

    std::string _key;

    Redis &_redis;
};

template <typename Input, typename Output>
void RString::mget(Input first, Input last, Output output) {
    auto reply = _redis.command(cmd::mget<Input>, first, last);

    reply::to_optional_string_array(*reply, output);
}

template <typename Input>
void RString::mset(Input first, Input last) {
    auto reply = _redis.command(cmd::mset<Input>, first, last);

    if (!reply::status_ok(*reply)) {
        throw RException("Invalid status reply: " + reply::to_status(*reply));
    }
}

template <typename Input>
bool RString::msetnx(Input first, Input last) {
    auto reply = _redis.command(cmd::msetnx<Input>, first, last);

    return reply::to_bool(*reply);
}

}

}

#endif // end SEWENEW_REDISPLUSPLUS_R_STRING_H
