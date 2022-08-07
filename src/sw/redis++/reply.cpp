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
#include <cstdlib>
#include <stdexcept>

namespace sw {

namespace redis {

std::string ParseError::_err_info(const std::string &expect_type,
        const redisReply &reply) const {
    return "expect " + expect_type + " reply, but got " +
        reply::type_to_string(reply.type) + " reply";
}

namespace reply {

std::string type_to_string(int type) {
    switch (type) {
    case REDIS_REPLY_ERROR:
        return "ERROR";

    case REDIS_REPLY_NIL:
        return "NULL";

    case REDIS_REPLY_STRING:
        return "STRING";

    case REDIS_REPLY_STATUS:
        return "STATUS";

    case REDIS_REPLY_INTEGER:
        return "INTEGER";

    case REDIS_REPLY_ARRAY:
        return "ARRAY";

    default:
        return "UNKNOWN";
    }
}

std::string to_status(redisReply &reply) {
    if (!reply::is_status(reply)) {
        throw ParseError("STATUS", reply);
    }

    if (reply.str == nullptr) {
        throw ProtoError("A null status reply");
    }

    // Old version hiredis' *redisReply::len* is of type int.
    // So we CANNOT have something like: *return {reply.str, reply.len}*.
    return std::string(reply.str, reply.len);
}

std::string parse(ParseTag<std::string>, redisReply &reply) {
    if (!reply::is_string(reply) && !reply::is_status(reply)) {
        throw ParseError("STRING or STATUS", reply);
    }

    if (reply.str == nullptr) {
        throw ProtoError("A null string reply");
    }

    // Old version hiredis' *redisReply::len* is of type int.
    // So we CANNOT have something like: *return {reply.str, reply.len}*.
    return std::string(reply.str, reply.len);
}

long long parse(ParseTag<long long>, redisReply &reply) {
    if (!reply::is_integer(reply)) {
        throw ParseError("INTEGER", reply);
    }

    return reply.integer;
}

double parse(ParseTag<double>, redisReply &reply) {
    try {
        return std::stod(parse<std::string>(reply));
    } catch (const std::invalid_argument &) {
        throw ProtoError("not a double reply");
    } catch (const std::out_of_range &) {
        throw ProtoError("double reply out of range");
    }
}

bool parse(ParseTag<bool>, redisReply &reply) {
    auto ret = parse<long long>(reply);

    if (ret == 1) {
        return true;
    } else if (ret == 0) {
        return false;
    } else {
        throw ProtoError("Invalid bool reply: " + std::to_string(ret));
    }
}

void parse(ParseTag<void>, redisReply &reply) {
    if (!reply::is_status(reply)) {
        throw ParseError("STATUS", reply);
    }

    if (reply.str == nullptr) {
        throw ProtoError("A null status reply");
    }

    static const std::string OK = "OK";

    // Old version hiredis' *redisReply::len* is of type int.
    // So we have to cast it to an unsigned int.
    if (static_cast<std::size_t>(reply.len) != OK.size()
            || OK.compare(0, OK.size(), reply.str, reply.len) != 0) {
        throw ProtoError("NOT ok status reply: " + reply::to_status(reply));
    }
}

bool parse_set_reply(redisReply &reply) {
    if (is_nil(reply)) {
        // Failed to set, and make it a FALSE reply.
        return false;
    }

    // Check if it's a "OK" status reply.
    reply::parse<void>(reply);

    // Make it a TRUE reply.
    return true;
}

void rewrite_empty_array_reply(redisReply &reply) {
    if (is_array(reply) && reply.elements == 0) {
        // Make it a nil reply.
        reply.type = REDIS_REPLY_NIL;
    }
}

namespace detail {

bool is_flat_array(redisReply &reply) {
    assert(reply::is_array(reply));

    // Empty array reply.
    if (reply.element == nullptr || reply.elements == 0) {
        return false;
    }

    auto *sub_reply = reply.element[0];

    // Null element.
    if (sub_reply == nullptr) {
        return false;
    }

    return !reply::is_array(*sub_reply);
}

}

}

}

}
