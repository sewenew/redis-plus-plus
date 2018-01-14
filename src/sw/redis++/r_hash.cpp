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

#include "r_hash.h"

namespace sw {

namespace redis {

long long RHash::hdel(const StringView &field) {
    auto reply = _redis.command(cmd::hdel, _key, field);

    return reply::to_integer(*reply);
}

bool RHash::hexists(const StringView &field) {
    auto reply = _redis.command(cmd::hexists, _key, field);

    auto ret = reply::to_integer(*reply);

    if (ret == 1) {
        return true;
    } else if (ret == 0) {
        return false;
    } else {
        throw RException("Invalid integer reply: " + std::to_string(ret));
    }
}

OptionalString RHash::hget(const StringView &field) {
    auto reply = _redis.command(cmd::hget, _key, field);

    return reply::to_optional_string(*reply);
}

long long RHash::hlen() {
    auto reply = _redis.command(cmd::hlen, _key);

    return reply::to_integer(*reply);
}

bool RHash::hset(const StringView &field, const StringView &val) {
    auto reply = _redis.command(cmd::hset, _key, field, val);

    auto ret = reply::to_integer(*reply);
    if (ret == 1) {
        return true;
    } else if (ret == 0) {
        return false;
    } else {
        throw RException("Invalid integer reply: " + std::to_string(ret));
    }
}

bool RHash::hsetnx(const StringView &field, const StringView &val) {
    auto reply = _redis.command(cmd::hsetnx, _key, field, val);

    auto ret = reply::to_integer(*reply);
    if (ret == 1) {
        return true;
    } else if (ret == 0) {
        return false;
    } else {
        throw RException("Invalid integer reply: " + std::to_string(ret));
    }
}

long long RHash::hstrlen(const StringView &field) {
    auto reply = _redis.command(cmd::hstrlen, _key, field);

    return reply::to_integer(*reply);
}

}

}
