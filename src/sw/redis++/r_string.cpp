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

#include "r_string.h"
#include "command.h"
#include "exceptions.h"

namespace sw {

namespace redis {

long long RString::append(const StringView &str) {
    auto reply = _redis.command(cmd::append, _key, str);

    return reply::to_integer(*reply);
}

OptionalString RString::get() {
    auto reply = _redis.command(cmd::get, _key);

    return reply::to_optional_string(*reply);
}

std::string RString::getrange(long long start, long long end) {
    auto reply = _redis.command(cmd::getrange, _key, start, end);

    return reply::to_string(*reply);
}

OptionalString RString::getset(const StringView &val) {
    auto reply = _redis.command(cmd::getset, _key, val);

    return reply::to_optional_string(*reply);
}

void RString::psetex(const StringView &val,
                        const std::chrono::milliseconds &ttl) {
    if (ttl <= std::chrono::milliseconds(0)) {
        throw RException("TTL must be positive.");
    }

    auto reply = _redis.command(cmd::psetex, _key, val, ttl);

    if (!reply::status_ok(*reply)) {
        throw RException("Invalid status reply: " + reply::to_status(*reply));
    }
}

bool RString::set(const StringView &val,
                    const std::chrono::milliseconds &ttl,
                    cmd::UpdateType type) {
    auto reply = _redis.command(cmd::set, _key, val, ttl, type);

    if (reply::is_nil(*reply)) {
        // Failed to set.
        return false;
    }

    assert(reply::is_status(*reply));

    if (!reply::status_ok(*reply)) {
        throw RException("Invalid status reply: " + reply::to_status(*reply));
    }

    return true;
}

bool RString::setnx(const StringView &val) {
    auto reply = _redis.command(cmd::setnx, _key, val);

    auto ret = reply::to_integer(*reply);

    if (ret == 1) {
        return true;
    } else if (ret == 0) {
        return false;
    } else {
        throw RException("Invalid integer reply: " + std::to_string(ret));
    }
}

void RString::setex(const StringView &val, const std::chrono::seconds &ttl) {
    if (ttl <= std::chrono::seconds(0)) {
        throw RException("TTL must be positive.");
    }

    auto reply = _redis.command(cmd::setex, _key, val, ttl);

    if (!reply::status_ok(*reply)) {
        throw RException("Invalid status reply: " + reply::to_status(*reply));
    }
}

long long RString::setrange(long long offset, const StringView &val) {
    auto reply = _redis.command(cmd::setrange, _key, offset, val);

    return reply::to_integer(*reply);
}

long long RString::strlen() {
    auto reply = _redis.command(cmd::strlen, _key);

    return reply::to_integer(*reply);
}

}

}
