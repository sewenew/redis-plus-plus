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
    /*
    // We don't support the INCR option, since you can always use ZINCRBY instead.
    long long zadd(const StringView &member,
                    double score,
                    bool changed = false,
                    cmd::UpdateType type = cmd::UpdateType::ALWAYS);

    template <typename Iter>
    long long zadd(Iter first,
                    Iter last,
                    bool changed = false,
                    cmd::UpdateType type = cmd::UpdateType::ALWAYS);
                    */

private:
    friend class Redis;

    RSortedSet(const std::string &key, Redis &redis) : _key(key), _redis(redis) {}

    std::string _key;

    Redis &_redis;
};

}

}

#endif // end SEWENEW_REDISPLUSPLUS_R_SORTED_SET_H
