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
#include <iterator>
#include <memory>
#include <functional>
#include <tuple>
#include <hiredis/hiredis.h>
#include "errors.h"
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

class ParseError : public ProtoError {
public:
    ParseError(const std::string &expect_type,
            const redisReply &reply) : ProtoError(_err_info(expect_type, reply)) {}

    ParseError(const ParseError &) = default;
    ParseError& operator=(const ParseError &) = default;

    ParseError(ParseError &&) = default;
    ParseError& operator=(ParseError &&) = default;

    virtual ~ParseError() override = default;

private:
    std::string _err_info(const std::string &type, const redisReply &reply) const;
};

namespace reply {

template <typename T>
struct ParseTag {};

template <typename T>
inline T parse(redisReply &reply) {
    return parse(ParseTag<T>(), reply);
}

template <typename T>
T parse_leniently(redisReply &reply);

void parse(ParseTag<void>, redisReply &reply);

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

#ifdef REDIS_PLUS_PLUS_HAS_VARIANT

inline Monostate parse(ParseTag<Monostate>, redisReply &) {
    // Just ignore the reply
    return {};
}

template <typename ...Args>
Variant<Args...> parse(ParseTag<Variant<Args...>>, redisReply &reply);

#endif

template <typename T, typename std::enable_if<IsSequenceContainer<T>::value, int>::type = 0>
T parse(ParseTag<T>, redisReply &reply);

template <typename T, typename std::enable_if<IsAssociativeContainer<T>::value, int>::type = 0>
T parse(ParseTag<T>, redisReply &reply);

template <typename Output>
long long parse_scan_reply(redisReply &reply, Output output);

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

std::string type_to_string(int type);

std::string to_status(redisReply &reply);

template <typename Output>
void to_array(redisReply &reply, Output output);

// Parse set reply to bool type
bool parse_set_reply(redisReply &reply);

// Some command might return an empty array reply as a nil reply,
// e.g. georadius, zpopmin, zpopmax. In this case, we rewrite the
// reply to a nil reply.
void rewrite_empty_array_reply(redisReply &reply);

template <typename Output>
auto parse_xpending_reply(redisReply &reply, Output output)
    -> std::tuple<long long, OptionalString, OptionalString>;

}

// Inline implementations.

namespace reply {

namespace detail {

template <typename Output>
void to_array(redisReply &reply, Output output) {
    if (!is_array(reply)) {
        throw ParseError("ARRAY", reply);
    }

    if (reply.element == nullptr) {
        // Empty array.
        return;
    }

    for (std::size_t idx = 0; idx != reply.elements; ++idx) {
        auto *sub_reply = reply.element[idx];
        if (sub_reply == nullptr) {
            throw ProtoError("Null array element reply");
        }

        *output = parse<typename IterType<Output>::type>(*sub_reply);

        ++output;
    }
}

bool is_flat_array(redisReply &reply);

template <typename Output>
void to_flat_array(redisReply &reply, Output output) {
    if (reply.element == nullptr) {
        // Empty array.
        return;
    }

    if (reply.elements % 2 != 0) {
        throw ProtoError("Not string pair array reply");
    }

    for (std::size_t idx = 0; idx != reply.elements; idx += 2) {
        auto *key_reply = reply.element[idx];
        auto *val_reply = reply.element[idx + 1];
        if (key_reply == nullptr || val_reply == nullptr) {
            throw ProtoError("Null string array reply");
        }

        using Pair = typename IterType<Output>::type;
        using FirstType = typename std::decay<typename Pair::first_type>::type;
        using SecondType = typename std::decay<typename Pair::second_type>::type;
        *output = std::make_pair(parse<FirstType>(*key_reply),
                                    parse<SecondType>(*val_reply));

        ++output;
    }
}

template <typename Output>
void to_array(std::true_type, redisReply &reply, Output output) {
    if (is_flat_array(reply)) {
        to_flat_array(reply, output);
    } else {
        to_array(reply, output);
    }
}

template <typename Output>
void to_array(std::false_type, redisReply &reply, Output output) {
    to_array(reply, output);
}

template <typename T>
std::tuple<T> parse_tuple(redisReply **reply, std::size_t idx) {
    assert(reply != nullptr);

    auto *sub_reply = reply[idx];
    if (sub_reply == nullptr) {
        throw ProtoError("Null reply");
    }

    return std::make_tuple(parse<T>(*sub_reply));
}

template <typename T, typename ...Args>
auto parse_tuple(redisReply **reply, std::size_t idx) ->
    typename std::enable_if<sizeof...(Args) != 0, std::tuple<T, Args...>>::type {
    assert(reply != nullptr);

    return std::tuple_cat(parse_tuple<T>(reply, idx),
                            parse_tuple<Args...>(reply, idx + 1));
}

#ifdef REDIS_PLUS_PLUS_HAS_VARIANT

template <typename T>
Variant<T> parse_variant(redisReply &reply) {
    return parse<T>(reply);
}

template <typename T, typename ...Args>
auto parse_variant(redisReply &reply) ->
    typename std::enable_if<sizeof...(Args) != 0, Variant<T, Args...>>::type {
    auto return_var = [](auto &&arg) {
        return Variant<T, Args...>(std::move(arg));
    };

    try {
        return std::visit(return_var, parse_variant<T>(reply));
    } catch (const ProtoError &) {
        return std::visit(return_var, parse_variant<Args...>(reply));
    }
}

#endif

}

template <typename T>
T parse_leniently(redisReply &reply) {
    if (is_array(reply) && reply.elements == 1) {
        if (reply.element == nullptr) {
            throw ProtoError("null array reply");
        }

        auto *ele = reply.element[0];
        if (ele != nullptr) {
            return parse<T>(*ele);
        } // else fall through
    }

    return parse<T>(reply);
}

template <typename T>
Optional<T> parse(ParseTag<Optional<T>>, redisReply &reply) {
    if (reply::is_nil(reply)) {
        // Because of a GCC bug, we cannot return {} for -std=c++17
        // Refer to https://gcc.gnu.org/bugzilla/show_bug.cgi?id=86465
#if defined REDIS_PLUS_PLUS_HAS_OPTIONAL
        return std::nullopt;
#else
        return {};
#endif
    }

    return Optional<T>(parse<T>(reply));
}

template <typename T, typename U>
std::pair<T, U> parse(ParseTag<std::pair<T, U>>, redisReply &reply) {
    if (!is_array(reply)) {
        throw ParseError("ARRAY", reply);
    }

    if (reply.elements != 2) {
        throw ProtoError("NOT key-value PAIR reply");
    }

    if (reply.element == nullptr) {
        throw ProtoError("Null PAIR reply");
    }

    auto *first = reply.element[0];
    auto *second = reply.element[1];
    if (first == nullptr || second == nullptr) {
        throw ProtoError("Null pair reply");
    }

    return std::make_pair(parse<typename std::decay<T>::type>(*first),
                            parse<typename std::decay<U>::type>(*second));
}

template <typename ...Args>
std::tuple<Args...> parse(ParseTag<std::tuple<Args...>>, redisReply &reply) {
    constexpr auto size = sizeof...(Args);

    static_assert(size > 0, "DO NOT support parsing tuple with 0 element");

    if (!is_array(reply)) {
        throw ParseError("ARRAY", reply);
    }

    if (reply.elements != size) {
        throw ProtoError("Expect tuple reply with " + std::to_string(size) + " elements" +
                ", but got " + std::to_string(reply.elements) + " elements");
    }

    if (reply.element == nullptr) {
        throw ProtoError("Null TUPLE reply");
    }

    return detail::parse_tuple<Args...>(reply.element, 0);
}

#ifdef REDIS_PLUS_PLUS_HAS_VARIANT

template <typename ...Args>
Variant<Args...> parse(ParseTag<Variant<Args...>>, redisReply &reply) {
    return detail::parse_variant<Args...>(reply);
}

#endif

template <typename T, typename std::enable_if<IsSequenceContainer<T>::value, int>::type>
T parse(ParseTag<T>, redisReply &reply) {
    if (!is_array(reply)) {
        throw ParseError("ARRAY", reply);
    }

    T container;

    to_array(reply, std::back_inserter(container));

    return container;
}

template <typename T, typename std::enable_if<IsAssociativeContainer<T>::value, int>::type>
T parse(ParseTag<T>, redisReply &reply) {
    if (!is_array(reply)) {
        throw ParseError("ARRAY", reply);
    }

    T container;

    to_array(reply, std::inserter(container, container.end()));

    return container;
}

template <typename Output>
long long parse_scan_reply(redisReply &reply, Output output) {
    if (reply.elements != 2 || reply.element == nullptr) {
        throw ProtoError("Invalid scan reply");
    }

    auto *cursor_reply = reply.element[0];
    auto *data_reply = reply.element[1];
    if (cursor_reply == nullptr || data_reply == nullptr) {
        throw ProtoError("Invalid cursor reply or data reply");
    }

    auto cursor_str = reply::parse<std::string>(*cursor_reply);
    long long new_cursor = 0;
    try {
        new_cursor = std::stoll(cursor_str);
    } catch (const std::exception &e) {
        throw ProtoError("Invalid cursor reply: " + cursor_str);
    }

    reply::to_array(*data_reply, output);

    return new_cursor;
}

template <typename Output>
void to_array(redisReply &reply, Output output) {
    if (!is_array(reply)) {
        throw ParseError("ARRAY", reply);
    }

    detail::to_array(typename IsKvPairIter<Output>::type(), reply, output);
}

template <typename Output>
auto parse_xpending_reply(redisReply &reply, Output output)
    -> std::tuple<long long, OptionalString, OptionalString> {
    if (!is_array(reply) || reply.elements != 4) {
        throw ProtoError("expect array reply with 4 elements");
    }

    for (std::size_t idx = 0; idx != reply.elements; ++idx) {
        if (reply.element[idx] == nullptr) {
            throw ProtoError("null array reply");
        }
    }

    auto num = parse<long long>(*(reply.element[0]));
    auto start = parse<OptionalString>(*(reply.element[1]));
    auto end = parse<OptionalString>(*(reply.element[2]));

    auto &entry_reply = *(reply.element[3]);
    if (!is_nil(entry_reply)) {
        to_array(entry_reply, output);
    }

    return std::make_tuple(num, std::move(start), std::move(end));
}

}

}

}

#endif // end SEWENEW_REDISPLUSPLUS_REPLY_H
