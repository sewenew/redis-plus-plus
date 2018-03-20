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

#include "reply.h"

namespace sw {

namespace redis {

namespace reply {

std::string to_error(redisReply &reply) {
    if (!reply::is_error(reply)) {
        throw RException("Expect ERROR reply.");
    }

    if (reply.str == nullptr) {
        throw RException("A null error reply");
    }

    return {reply.str, reply.len};
}

std::string to_status(redisReply &reply) {
    if (!reply::is_status(reply)) {
        throw RException("Expect STATUS reply.");
    }

    if (reply.str == nullptr) {
        throw RException("A null status reply");
    }

    return {reply.str, reply.len};
}

std::string to_string(redisReply &reply) {
    if (!reply::is_string(reply)) {
        throw RException("Expect STRING reply.");
    }

    if (reply.str == nullptr) {
        throw RException("A null string reply");
    }

    return {reply.str, reply.len};
}

OptionalString to_optional_string(redisReply &reply) {
    if (reply::is_nil(reply)) {
        return {};
    }

    return OptionalString(reply::to_string(reply));
}

OptionalStringPair to_optional_string_pair(redisReply &reply) {
    if (reply::is_nil(reply)) {
        return {};
    }

    if (reply.elements != 2) {
        throw RException("Not a string pair");
    }

    auto *key = reply.element[0];
    auto *val = reply.element[1];
    if (key == nullptr || val == nullptr) {
        throw RException("Null string pair");
    }

    return OptionalStringPair(reply::to_string(*key), reply::to_string(*val));
}

long long to_integer(redisReply &reply) {
    if (!reply::is_integer(reply)) {
        throw RException("Expect INTEGER reply.");
    }

    return reply.integer;
}

OptionalLongLong to_optional_integer(redisReply &reply) {
    if (reply::is_nil(reply)) {
        return {};
    }

    return OptionalLongLong(reply::to_integer(reply));
}

OptionalDouble to_optional_double(redisReply &reply) {
    if (reply::is_nil(reply)) {
        return {};
    }

    return OptionalDouble(reply::to_double(reply));
}

bool to_bool(redisReply &reply) {
    auto ret = reply::to_integer(reply);

    if (ret == 1) {
        return true;
    } else if (ret == 0) {
        return false;
    } else {
        throw RException("Invalid bool reply: " + std::to_string(ret));
    }
}

bool status_ok(redisReply &reply) {
    if (!reply::is_status(reply)) {
        throw RException("Expect STATUS reply.");
    }

    if (reply.str == nullptr) {
        throw RException("A null status reply");
    }

    static const std::string OK = "OK";

    return OK.compare(0, OK.size(), reply.str, reply.len) == 0;
}

}

}

}
