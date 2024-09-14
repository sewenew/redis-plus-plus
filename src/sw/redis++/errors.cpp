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

#include "sw/redis++/errors.h"
#include <cassert>
#include <cerrno>
#include <unordered_map>
#include <tuple>
#include "sw/redis++/shards.h"

namespace {

using namespace sw::redis;

std::pair<ReplyErrorType, std::string> parse_error(const std::string &msg);

std::unordered_map<std::string, ReplyErrorType> error_map = {
    {"MOVED", ReplyErrorType::MOVED},
    {"ASK", ReplyErrorType::ASK}
};

}

namespace sw {

namespace redis {

[[noreturn]] void throw_error(const redisContext &context, const std::string &err_info) {
    auto err_code = context.err;
    const auto *err_str = context.errstr;
    if (err_str == nullptr) {
        throw Error(err_info + ": null error message: " + std::to_string(err_code));
    }

    auto err_msg = err_info + ": " + err_str
        + ", err: " + std::to_string(err_code) + ", errno: " + std::to_string(errno);

    switch (err_code) {
    case REDIS_ERR_IO:
        if (errno == EAGAIN || errno == EWOULDBLOCK || errno == ETIMEDOUT) {
            throw TimeoutError(err_msg);
        } else {
            throw IoError(err_msg);
        }

    case REDIS_ERR_EOF:
        throw ClosedError(err_msg);

    case REDIS_ERR_PROTOCOL:
        throw ProtoError(err_msg);

    case REDIS_ERR_OOM:
        throw OomError(err_msg);

    case REDIS_ERR_OTHER:
        throw Error(err_msg);

#ifdef REDIS_ERR_TIMEOUT
    case REDIS_ERR_TIMEOUT:
        throw TimeoutError(err_msg);
#endif

    default:
        throw Error("unknown error code: " + err_msg);
    }
}

[[noreturn]] void throw_error(const redisReply &reply) {
    assert(reply.type == REDIS_REPLY_ERROR);

    if (reply.str == nullptr) {
        throw Error("Null error reply");
    }

    auto err_str = std::string(reply.str, reply.len);

    auto err_type = ReplyErrorType::ERR;
    std::string err_msg;
    std::tie(err_type, err_msg) = parse_error(err_str);

    switch (err_type) {
    case ReplyErrorType::MOVED:
        throw MovedError(err_msg);

    case ReplyErrorType::ASK:
        throw AskError(err_msg);

    default:
        throw ReplyError(err_str);
    }
}

}

}

namespace {

using namespace sw::redis;

std::pair<ReplyErrorType, std::string> parse_error(const std::string &err) {
    // The error contains an Error Prefix, and an optional error message.
    auto idx = err.find_first_of(" \n");

    if (idx == std::string::npos) {
        throw ProtoError("No Error Prefix: " + err);
    }

    auto err_prefix = err.substr(0, idx);
    auto err_type = ReplyErrorType::ERR;

    auto iter = error_map.find(err_prefix);
    if (iter != error_map.end()) {
        // Specific error.
        err_type = iter->second;
    } // else Generic error.

    return {err_type, err.substr(idx + 1)};
}

}
