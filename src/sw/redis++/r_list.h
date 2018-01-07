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

namespace sw {

namespace redis {

class StringView;

// Redis' LIST type.
class RList {
public:
    std::string lpop();

    long long lpush(const StringView &val);

    template <typename Iter>
    long long lpush(Iter first, Iter last);

private:
    friend class Redis;

    RList(const std::string &key, Redis &redis) : _key(key), _redis(redis) {}

    std::string _key;

    Redis &_redis;
};

template <typename Iter>
long long RList::lpush(Iter first, Iter last) {
    auto reply = _redis.command(cmd::lpush_range, _key, first, last);

    return reply::to_integer(*reply);
}

}

}

#endif // end SEWENEW_REDISPLUSPLUS_R_LIST_H
