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

#include "errors.h"
#include <cassert>
#include <cerrno>
#include <unordered_map>

namespace {

sw::redis::ReplyErrorType error_type(const std::string &msg);

std::unordered_map<std::string, sw::redis::ReplyErrorType> error_map = {
    {"MOVED", sw::redis::ReplyErrorType::MOVED}
};

}

namespace sw {

namespace redis {

void throw_error(redisContext &context, const std::string &err_info) {
    auto err_code = context.err;
    const auto *err_str = context.errstr;
    if (err_str == nullptr) {
        throw Error(err_info + ": null error message: " + std::to_string(err_code));
    }

    auto err_msg = err_info + ": " + err_str;

    switch (err_code) {
    case REDIS_ERR_IO:
        if (errno == EAGAIN) {
            context.err = 0;
            throw TimeoutError(err_msg);
        } else {
            throw IoError(err_msg);
        }
        break;

    case REDIS_ERR_EOF:
        throw ClosedError(err_msg);
        break;

    case REDIS_ERR_PROTOCOL:
        throw ProtoError(err_msg);
        break;

    case REDIS_ERR_OOM:
        throw OomError(err_msg);
        break;

    case REDIS_ERR_OTHER:
        throw Error(err_msg);
        break;

    default:
        throw Error(err_info + ": Unknown error code");
    }
}

void throw_error(const redisReply &reply) {
    assert(reply.type == REDIS_REPLY_ERROR);

    if (reply.str == nullptr) {
        throw Error("Null error reply");
    }

    auto err_str = std::string(reply.str, reply.len);

    auto err_type = error_type(err_str);
    switch (err_type) {
    case ReplyErrorType::MOVED:
        throw MovedError(err_str);
        break;

    default:
        throw ReplyError(err_str);
        break;
    }
}

}

}

namespace {

sw::redis::ReplyErrorType error_type(const std::string &msg) {
    // The error message contains an Error Prefix, and an optional description.
    auto idx = msg.find_first_of(" \n");

    auto err_prefix = msg.substr(0, idx);

    auto iter = error_map.find(err_prefix);
    if (iter == error_map.end()) {
        // Generic error.
        return sw::redis::ReplyErrorType::ERR;
    } else {
        // Specific error.
        return iter->second;
    }
}

}
