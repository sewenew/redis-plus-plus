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
#include <memory>
#include <functional>
#include <hiredis/hiredis.h>
#include "exceptions.h"
#include "utils.h"

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
    explicit StatusReplyFunctor(StringReplyCallback callback) : _callback(callback) {}

    void operator()(redisReply &reply);

private:
    std::function<void (const std::string &)> _callback;
};

class StringReplyFunctor {
public:
    template <typename StringReplyCallback>
    explicit StringReplyFunctor(StringReplyCallback callback) : _callback(callback) {}

    void operator()(redisReply &reply);

private:
    std::function<void (const std::string &)> _callback;
};

class IntegerReplyFunctor {
public:
    template <typename IntegerReplyCallback>
    explicit IntegerReplyFunctor(IntegerReplyCallback callback) : _callback(callback) {}

    void operator()(redisReply &reply);

private:
    std::function<void (long long)> _callback;
};

namespace reply {

template <typename T>
struct ParseTag {};

template <typename T>
inline T parse(redisReply &reply) {
    return parse(ParseTag<T>(), reply);
}

std::string parse(ParseTag<std::string>, redisReply &reply);

long long parse(ParseTag<long long>, redisReply &reply);

double parse(ParseTag<double>, redisReply &reply);

bool parse(ParseTag<bool>, redisReply &reply);

template <typename T>
Optional<T> parse(ParseTag<Optional<T>>, redisReply &reply);

template <typename T, typename U>
std::pair<T, U> parse(ParseTag<std::pair<T, U>>, redisReply &reply);

template <typename ...Args>
std::tuple<Args...> parse(ParseTag<std::tuple<Args...>>, redisReply &reply);

inline bool is_error(redisReply &reply) {
    return reply.type == REDIS_REPLY_ERROR;
}

inline bool is_nil(redisReply &reply) {
    return reply.type == REDIS_REPLY_NIL;
}

inline bool is_string(redisReply &reply) {
    return reply.type == REDIS_REPLY_STRING;
}

inline bool is_status(redisReply &reply) {
    return reply.type == REDIS_REPLY_STATUS;
}

inline bool is_integer(redisReply &reply) {
    return reply.type == REDIS_REPLY_INTEGER;
}

inline bool is_array(redisReply &reply) {
    return reply.type == REDIS_REPLY_ARRAY;
}

std::string to_error(redisReply &reply);

std::string to_status(redisReply &reply);

inline std::string to_string(redisReply &reply) {
    return parse<std::string>(reply);
}

inline OptionalString to_optional_string(redisReply &reply) {
    return parse<OptionalString>(reply);
}

inline OptionalStringPair to_optional_string_pair(redisReply &reply) {
    return parse<OptionalStringPair>(reply);
}

inline long long to_integer(redisReply &reply) {
    return parse<long long>(reply);
}

inline OptionalLongLong to_optional_integer(redisReply &reply) {
    return parse<OptionalLongLong>(reply);
}

inline double to_double(redisReply &reply) {
    return parse<double>(reply);
}

inline OptionalDouble to_optional_double(redisReply &reply) {
    return parse<OptionalDouble>(reply);
}

template<typename Iter>
void to_score_array(redisReply &reply, Iter output);

template <typename Output>
void to_array(redisReply &reply, Output output);

template <typename Iter>
void to_optional_string_array(redisReply &reply, Iter output);

inline bool to_bool(redisReply &reply) {
    return parse<bool>(reply);
}

void expect_ok_status(redisReply &reply);

}

// Inline implementations.

inline void StatusReplyFunctor::operator()(redisReply &reply) {
    _callback(reply::to_status(reply));
}

inline void StringReplyFunctor::operator()(redisReply &reply) {
    _callback(reply::to_string(reply));
}

inline void IntegerReplyFunctor::operator()(redisReply &reply) {
    _callback(reply::to_integer(reply));
}

namespace reply {

namespace detail {

template <typename Iter>
void to_score_array(std::true_type, redisReply &reply, Iter output) {
    if (reply.elements % 2 != 0) {
        throw RException("Not string pair array reply");
    }

    if (reply.element == nullptr) {
        throw RException("Null reply.");
    }

    for (std::size_t idx = 0; idx != reply.elements; idx += 2) {
        auto *key_reply = reply.element[idx];
        auto *val_reply = reply.element[idx + 1];
        if (key_reply == nullptr || val_reply == nullptr) {
            throw RException("Null string array reply.");
        }

        using Pair = typename IterType<Iter>::type;
        *output = std::make_pair(parse<typename Pair::first_type>(*key_reply),
                                    parse<typename Pair::second_type>(*val_reply));

        ++output;
    }
}

template <typename Iter>
void to_score_array(std::false_type, redisReply &reply, Iter output) {
    to_array(reply, output);
}

template <typename T>
std::tuple<T> parse_tuple(redisReply **reply, std::size_t idx) {
    assert(reply != nullptr);

    auto *sub_reply = reply[idx];
    if (sub_reply == nullptr) {
        throw RException("Null reply.");
    }

    return make_tuple(parse<T>(*sub_reply));
}

template <typename T, typename ...Args>
auto parse_tuple(redisReply **reply, std::size_t idx) ->
    typename std::enable_if<sizeof...(Args) != 0, std::tuple<T, Args...>>::type {
    assert(reply != nullptr);

    return std::tuple_cat(parse_tuple<T>(reply, idx),
                            parse_tuple<Args...>(reply, idx + 1));
}

}

template<typename Iter>
void to_score_array(redisReply &reply, Iter output) {
    if (!reply::is_array(reply)) {
        throw RException("Expect ARRAY reply.");
    }

    detail::to_score_array(typename IsKvPairIter<Iter>::type(), reply, output);
}

template <typename Iter>
void to_optional_string_array(redisReply &reply, Iter output) {
    to_array(reply, output);
}

template <typename T>
Optional<T> parse(ParseTag<Optional<T>>, redisReply &reply) {
    if (reply::is_nil(reply)) {
        return {};
    }

    return Optional<T>(parse<T>(reply));
}

template <typename T, typename U>
std::pair<T, U> parse(ParseTag<std::pair<T, U>>, redisReply &reply) {
    if (reply.elements != 2) {
        throw RException("Expect PAIR reply.");
    }

    if (reply.element == nullptr) {
        throw RException("Null reply.");
    }

    auto *first = reply.element[0];
    auto *second = reply.element[1];
    if (first == nullptr || second == nullptr) {
        throw RException("Null pair reply.");
    }

    return std::make_pair(parse<T>(*first), parse<U>(*second));
}

template <typename ...Args>
std::tuple<Args...> parse(ParseTag<std::tuple<Args...>>, redisReply &reply) {
    constexpr auto size = sizeof...(Args);
    if (reply.elements != size) {
        throw RException("Expect tuple reply with " + std::to_string(size) + "elements");
    }

    if (reply.element == nullptr) {
        throw RException("Null reply.");
    }

    return detail::parse_tuple<Args...>(reply.element, 0);
}

template <typename Output>
void to_array(redisReply &reply, Output output) {
    if (!reply::is_array(reply)) {
        throw RException("Expect ARRAY reply.");
    }

    if (reply.element == nullptr) {
        throw RException("Null ARRAY reply.");
    }

    for (std::size_t idx = 0; idx != reply.elements; ++idx) {
        auto *sub_reply = reply.element[idx];
        if (sub_reply == nullptr) {
            throw RException("Null string array reply.");
        }

        *output = parse<typename IterType<Output>::type>(*sub_reply);

        ++output;
    }
}

}

}

}

#endif // end SEWENEW_REDISPLUSPLUS_REPLY_H
