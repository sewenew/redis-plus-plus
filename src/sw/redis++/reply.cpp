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
#include "exceptions.h"

namespace sw {

namespace redis {

namespace reply {

std::string to_error(redisReply &reply) {
    if (reply.type != REDIS_REPLY_ERROR) {
        throw RException("Expect ERROR reply.");
    }

    if (reply.str == nullptr) {
        throw RException("A null error reply");
    }

    return {reply.str, reply.len};
}

std::string to_status(redisReply &reply) {
    if (reply.type != REDIS_REPLY_STATUS) {
        throw RException("Expect STATUS reply.");
    }

    if (reply.str == nullptr) {
        throw RException("A null status reply");
    }

    return {reply.str, reply.len};
}

std::string to_string(redisReply &reply) {
    if (reply.type != REDIS_REPLY_STRING) {
        throw RException("Expect STRING reply.");
    }

    if (reply.str == nullptr) {
        throw RException("A null string reply");
    }

    return {reply.str, reply.len};
}

long long to_integer(redisReply &reply) {
    if (reply.type != REDIS_REPLY_INTEGER) {
        throw RException("Expect INTEGER reply.");
    }

    return reply.integer;
}

bool status_ok(redisReply &reply) {
    if (reply.type != REDIS_REPLY_STATUS) {
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
