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

#ifndef SEWENEW_REDISPLUSPLUS_REPLY_H
#define SEWENEW_REDISPLUSPLUS_REPLY_H

#include <cassert>
#include <string>
#include <functional>
#include <hiredis/hiredis.h>

namespace sw {

namespace redis {

struct ReplyDeleter {
    void operator()(redisReply *reply) const {
        if (reply != nullptr) {
            freeReplyObject(reply);
        }
    }
};

using ReplyUPtr = std::unique_ptr<redisReply, ReplyDeleter>;

class DummyReplyFunctor {
public:
    void operator()(redisReply & /*reply*/) {}
};

class StatusReplyFunctor {
public:
    template <typename StringReplyCallback>
    explicit StatusReplyFunctor(StringReplyCallback callback);

    void operator()(redisReply &reply);

private:
    std::function<void (const std::string &)> _callback;
};

class StringReplyFunctor {
public:
    template <typename StringReplyCallback>
    explicit StringReplyFunctor(StringReplyCallback callback);

    void operator()(redisReply &reply);

private:
    std::function<void (const std::string &)> _callback;
};

class IntegerReplyFunctor {
public:
    template <typename IntegerReplyCallback>
    explicit IntegerReplyFunctor(IntegerReplyCallback callback);

    void operator()(redisReply &reply);

private:
    std::function<void (long long)> _callback;
};

namespace reply {

bool has_error(redisReply &reply);

bool is_nil(redisReply &reply);

std::string to_error(redisReply &reply);

std::string to_status(redisReply &reply);

std::string to_string(redisReply &reply);

long long to_integer(redisReply &reply);

bool status_ok(redisReply &reply);

// Inline implementations

inline bool has_error(redisReply &reply) {
    return reply.type == REDIS_REPLY_ERROR;
}

bool status_ok(redisReply &reply);

}

// Inline implementations.

template <typename StringReplyCallback>
inline StatusReplyFunctor::StatusReplyFunctor(StringReplyCallback callback) :
    _callback(callback) {}

inline void StatusReplyFunctor::operator()(redisReply &reply) {
    _callback(reply::to_status(reply));
}

template <typename StringReplyCallback>
inline StringReplyFunctor::StringReplyFunctor(StringReplyCallback callback) :
    _callback(callback) {}

inline void StringReplyFunctor::operator()(redisReply &reply) {
    _callback(reply::to_string(reply));
}

template <typename IntegerReplyCallback>
inline IntegerReplyFunctor::IntegerReplyFunctor(IntegerReplyCallback callback) :
    _callback(callback) {}

inline void IntegerReplyFunctor::operator()(redisReply &reply) {
    _callback(reply::to_integer(reply));
}

namespace reply {

inline bool is_nil(redisReply &reply) {
    return reply.type == REDIS_REPLY_NIL;
}

}

}

}

#endif // end SEWENEW_REDISPLUSPLUS_REPLY_H
