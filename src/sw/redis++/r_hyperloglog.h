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

#ifndef SEWENEW_REDISPLUSPLUS_R_HYPERLOGLOG_H
#define SEWENEW_REDISPLUSPLUS_R_HYPERLOGLOG_H

#include <string>
#include "reply.h"
#include "command.h"
#include "redis.h"
#include "utils.h"

namespace sw {

namespace redis {

class StringView;

// Redis' HyperLogLog type.
class RHyperLogLog {
public:
    const std::string& key() const {
        return _key;
    }

    bool pfadd(const StringView &element);

    template <typename Input>
    bool pfadd(Input first, Input last);

    long long pfcount(const StringView &key);

    template <typename Input>
    long long pfcount(Input first, Input last);

    template <typename Input>
    void pfmerge(const StringView &destination, Input first, Input last);

private:
    friend class Redis;

    RHyperLogLog(const std::string &key, Redis &redis) : _key(key), _redis(redis) {}

    std::string _key;

    Redis &_redis;
};

template <typename Input>
bool RHyperLogLog::pfadd(Input first, Input last) {
    auto reply = _redis.command(cmd::pfadd_range<Input>, _key, first, last);

    return reply::to_bool(*reply);
}

template <typename Input>
long long RHyperLogLog::pfcount(Input first, Input last) {
    auto reply = _redis.command(cmd::pfcount_range<Input>, first, last);

    return reply::to_integer(*reply);
}

template <typename Input>
void RHyperLogLog::pfmerge(const StringView &destination,
                            Input first,
                            Input last) {
    auto reply = _redis.command(cmd::pfmerge, destination, first, last);

    if (!reply::status_ok(*reply)) {
        throw RException("Invalid status reply: " + reply::to_status(*reply));
    }
}

}

}

#endif // end SEWENEW_REDISPLUSPLUS_R_HYPERLOGLOG_H
