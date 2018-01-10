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
#include "redis.h"
#include "command.h"
#include "utils.h"

namespace sw {

namespace redis {

class StringView;

// Redis' STRING type.
class RString {
public:
    long long append(const StringView &str);

    OptionalString get();

    std::string getrange(long long start, long long end);

    OptionalString getset(const StringView &val);

    void psetex(const StringView &val,
                const std::chrono::milliseconds &ttl);

    bool set(const StringView &val,
                const std::chrono::milliseconds &ttl = std::chrono::milliseconds(0),
                cmd::UpdateType type = cmd::UpdateType::ALWAYS);

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

}

}

#endif // end SEWENEW_REDISPLUSPLUS_R_STRING_H
