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

#include "r_set.h"

namespace sw {

namespace redis {

long long RSet::sadd(const StringView &member) {
    auto reply = _redis.command(cmd::sadd, _key, member);

    return reply::to_integer(*reply);
}

long long RSet::scard() {
    auto reply = _redis.command(cmd::scard, _key);

    return reply::to_integer(*reply);
}

bool RSet::sismember(const StringView &member) {
    auto reply = _redis.command(cmd::sismember, _key, member);

    return reply::to_bool(*reply);
}

bool RSet::smove(const StringView &destination, const StringView &member) {
    auto reply = _redis.command(cmd::smove, _key, destination, member);

    return reply::to_bool(*reply);
}

OptionalString RSet::spop() {
    auto reply = _redis.command(cmd::spop, _key);

    return reply::to_optional_string(*reply);
}

OptionalString RSet::srandmember() {
    auto reply = _redis.command(cmd::srandmember, _key);

    return reply::to_optional_string(*reply);
}

long long RSet::srem(const StringView &member) {
    auto reply = _redis.command(cmd::srem, _key, member);

    return reply::to_integer(*reply);
}

}

}
