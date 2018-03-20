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

#include "r_list.h"
#include "command.h"
#include "exceptions.h"

namespace sw {

namespace redis {

OptionalString RList::lindex(long long index) {
    auto reply = _redis.command(cmd::lindex, _key, index);

    return reply::to_optional_string(*reply);
}

long long RList::linsert(const StringView &val,
                            InsertPosition position,
                            const StringView &pivot) {
    auto reply = _redis.command(cmd::linsert, _key, val, position, pivot);

    return reply::to_integer(*reply);
}

long long RList::llen() {
    auto reply = _redis.command(cmd::llen, _key);

    return reply::to_integer(*reply);
}

OptionalString RList::brpoplpush(const StringView &source,
                                    const StringView &destination,
                                    const std::chrono::seconds &timeout) {
    auto reply = _redis.command(cmd::brpoplpush, source, destination, timeout);

    return reply::to_optional_string(*reply);
}

OptionalString RList::lpop() {
    auto reply = _redis.command(cmd::lpop, _key);

    return reply::to_optional_string(*reply);
}

long long RList::lpush(const StringView &val) {
    auto reply = _redis.command(cmd::lpush, _key, val);

    return reply::to_integer(*reply);
}

long long RList::lpushx(const StringView &val) {
    auto reply = _redis.command(cmd::lpushx, _key, val);

    return reply::to_integer(*reply);
}

long long RList::lrem(const StringView &val, long long count) {
    auto reply = _redis.command(cmd::lrem, _key, val, count);

    return reply::to_integer(*reply);
}

void RList::lset(long long index, const StringView &val) {
    auto reply = _redis.command(cmd::lset, _key, index, val);

    if (!reply::status_ok(*reply)) {
        throw RException("Invalid status reply: " + reply::to_status(*reply));
    }
}

void RList::ltrim(long long start, long long stop) {
    auto reply = _redis.command(cmd::ltrim, _key, start, stop);

    if (!reply::status_ok(*reply)) {
        throw RException("Invalid status reply: " + reply::to_status(*reply));
    }
}

OptionalString RList::rpop() {
    auto reply = _redis.command(cmd::rpop, _key);

    return reply::to_optional_string(*reply);
}

long long RList::rpush(const StringView &val) {
    auto reply = _redis.command(cmd::rpush, _key, val);

    return reply::to_integer(*reply);
}

long long RList::rpushx(const StringView &val) {
    auto reply = _redis.command(cmd::rpushx, _key, val);

    return reply::to_integer(*reply);
}

}

}
