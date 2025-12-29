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
#include <cstdlib>
#include <string>
#include <iterator>
#include <memory>
#include <functional>
#include <tuple>
#include <hiredis/hiredis.h>
#include "sw/redis++/errors.h"
#include "sw/redis++/utils.h"

#ifdef REDIS_REPLY_MAP

#define REDIS_PLUS_PLUS_RESP_VERSION_3

#endif

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
Cursor parse_scan_reply(redisReply &reply, Output output);

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

#ifdef REDIS_PLUS_PLUS_RESP_VERSION_3

inline bool is_double(redisReply &reply) {
    return reply.type == REDIS_REPLY_DOUBLE;
}

inline bool is_bool(redisReply &reply) {
    return reply.type == REDIS_REPLY_BOOL;
}

inline bool is_map(redisReply &reply) {
    return reply.type == REDIS_REPLY_MAP;
}

inline bool is_set(redisReply &reply) {
    return reply.type == REDIS_REPLY_SET;
}

inline bool is_attr(redisReply &reply) {
    return reply.type == REDIS_REPLY_ATTR;
}

inline bool is_push(redisReply &reply) {
    return reply.type == REDIS_REPLY_PUSH;
}

inline bool is_bignum(redisReply &reply) {
    return reply.type == REDIS_REPLY_BIGNUM;
}

inline bool is_verb(redisReply &reply) {
    return reply.type == REDIS_REPLY_VERB;
}

#endif

std::string type_to_string(int type);

std::string to_status(redisReply &reply);

template <typename Output>
void to_array(redisReply &reply, Output output);

template <typename Output>
void to_optional_array(redisReply &reply, Output output);

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
#ifdef REDIS_PLUS_PLUS_RESP_VERSION_3
    if (!is_array(reply) && !is_map(reply) && !is_set(reply)) {
        throw ParseError("ARRAY or MAP or SET", reply);
    }
#else
    if (!is_array(reply)) {
        throw ParseError("ARRAY", reply);
    }
#endif

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

template <typename T>
bool is_parsable(redisReply &reply);

bool is_parsable(ParseTag<std::string>, redisReply &reply);

bool is_parsable(ParseTag<long long>, redisReply &reply);

bool is_parsable(ParseTag<double>, redisReply &reply);

bool is_parsable(ParseTag<bool>, redisReply &reply);

bool is_parsable(ParseTag<void>, redisReply &reply);

template <typename T>
bool is_parsable(ParseTag<Optional<T>>, redisReply &reply);

template <typename T, typename U>
bool is_parsable(ParseTag<std::pair<T, U>>, redisReply &reply);

template <typename ...Args>
bool is_parsable(ParseTag<std::tuple<Args...>>, redisReply &reply);

template <typename T, typename std::enable_if<IsSequenceContainer<T>::value, int>::type = 0>
bool is_parsable(ParseTag<T>, redisReply &reply);

template <typename T, typename std::enable_if<IsAssociativeContainer<T>::value, int>::type = 0>
bool is_parsable(ParseTag<T>, redisReply &reply);

#ifdef REDIS_PLUS_PLUS_HAS_VARIANT

bool is_parsable(ParseTag<Monostate>, redisReply &reply);

template <typename T>
bool is_parsable(ParseTag<Variant<T>>, redisReply &reply);

template <typename T, typename ...Args>
auto is_parsable(ParseTag<Variant<T, Args...>>, redisReply &reply)
    -> typename std::enable_if<sizeof...(Args) != 0, bool>::type;

#endif

template <typename T>
inline bool is_parsable(redisReply &reply) {
    return is_parsable(ParseTag<T>{}, reply);
}

inline bool is_parsable(ParseTag<std::string>, redisReply &reply) {
#ifdef REDIS_PLUS_PLUS_RESP_VERSION_3
    return is_string(reply) || is_status(reply)
            || is_verb(reply) || is_bignum(reply);
#else
    return is_string(reply) || is_status(reply);
#endif
}

inline bool is_parsable(ParseTag<long long>, redisReply &reply) {
    return is_integer(reply);
}

inline bool is_parsable(ParseTag<double>, redisReply &reply) {
#ifdef REDIS_PLUS_PLUS_RESP_VERSION_3
    if (is_double(reply)) {
        return true;
    }
#endif

    if (!is_string(reply) || reply.str == nullptr) {
        return false;
    }

    char *end = nullptr;
    std::strtod(reply.str, &end);
    return end != reply.str;
}

inline bool is_parsable(ParseTag<bool>, redisReply &reply) {
#ifdef REDIS_PLUS_PLUS_RESP_VERSION_3
    if (!is_bool(reply) && !is_integer(reply)) {
        return false;
    }
#else
    if (!is_integer(reply)) {
        return false;
    }
#endif

    return reply.integer == 0 || reply.integer == 1;
}

inline bool is_parsable(ParseTag<void>, redisReply &reply) {
    return is_status(reply);
}

template <typename T>
inline bool is_parsable(ParseTag<Optional<T>>, redisReply &reply) {
    if (is_nil(reply)) {
        return true;
    }

    return is_parsable<T>(reply);
}

template <typename T, typename U>
inline bool is_parsable(ParseTag<std::pair<T, U>>, redisReply &reply) {
    if (!is_array(reply)) {
        return false;
    }

    if (reply.element == nullptr) {
        return false;
    }

    if (reply.elements == 1) {
        // Nested array reply. Check the first element of the nested array.
        auto *nested_element = reply.element[0];
        if (nested_element == nullptr) {
            return false;
        }

        return is_parsable(ParseTag<std::pair<T, U>>{}, *nested_element);
    }

    if (reply.elements != 2) {
        return false;
    }

    auto *first = reply.element[0];
    auto *second = reply.element[1];
    if (first == nullptr || second == nullptr) {
        return false;
    }

    return is_parsable(ParseTag<typename std::decay<T>::type>{}, *first) &&
        is_parsable(ParseTag<typename std::decay<U>::type>{}, *second);
}

template <typename T>
bool is_tuple_parsable(redisReply **reply, std::size_t idx) {
    assert(reply != nullptr);

    auto *sub_reply = reply[idx];
    if (sub_reply == nullptr) {
        return false;
    }

    return is_parsable<T>(*sub_reply);
}

template <typename T, typename ...Args>
auto is_tuple_parsable(redisReply **reply, std::size_t idx) ->
    typename std::enable_if<sizeof...(Args) != 0, bool>::type {
    assert(reply != nullptr);

    return is_tuple_parsable<T>(reply, idx) &&
        is_tuple_parsable<Args...>(reply, idx + 1);
}

template <typename ...Args>
bool is_parsable(ParseTag<std::tuple<Args...>>, redisReply &reply) {
    constexpr auto size = sizeof...(Args);

    static_assert(size > 0, "DO NOT support parsing tuple with 0 element");

    if (!is_array(reply)) {
        return false;
    }

    if (reply.elements != size) {
        return false;
    }

    if (reply.element == nullptr) {
        return false;
    }

    return is_tuple_parsable<Args...>(reply.element, 0);
}

template <typename T>
bool is_flat_kv_parsable(redisReply &reply) {
    if (reply.element == nullptr || reply.elements == 0) {
        // Empty array.
        return true;
    }

    if (reply.elements % 2 != 0) {
        return false;
    }

    auto *key_reply = reply.element[0];
    auto *val_reply = reply.element[1];
    if (key_reply == nullptr || val_reply == nullptr) {
        return false;
    }

    using Pair = typename T::value_type;
    using FirstType = typename std::decay<typename Pair::first_type>::type;
    using SecondType = typename std::decay<typename Pair::second_type>::type;
    return is_parsable<FirstType>(*key_reply) &&
        is_parsable<SecondType>(*val_reply);
}

template <typename T>
bool is_container_parsable(std::false_type, redisReply &reply) {
    if (reply.element == nullptr || reply.elements == 0) {
        // Empty array.
        return true;
    }

    auto *sub_reply = reply.element[0];
    if (sub_reply == nullptr) {
        return false;
    }

    return is_parsable<typename T::value_type>(*sub_reply);
}

template <typename T>
bool is_container_parsable(std::true_type, redisReply &reply) {
    if (is_flat_array(reply)) {
        return is_flat_kv_parsable<T>(reply);
    } else {
        return is_container_parsable<T>(std::false_type{}, reply);
    }
}

template <typename T, typename std::enable_if<IsSequenceContainer<T>::value, int>::type>
bool is_parsable(ParseTag<T>, redisReply &reply) {
#ifdef REDIS_PLUS_PLUS_RESP_VERSION_3
    if (!is_array(reply) && !is_set(reply)) {
        return false;
#else
    if (!is_array(reply)) {
        return false;
#endif
    }

    return is_container_parsable<T>(typename IsKvPair<typename T::value_type>::type(), reply);
}

template <typename T, typename std::enable_if<IsAssociativeContainer<T>::value, int>::type>
bool is_parsable(ParseTag<T>, redisReply &reply) {
#ifdef REDIS_PLUS_PLUS_RESP_VERSION_3
    if (!is_array(reply) && !is_map(reply) && !is_set(reply)) {
#else
    if (!is_array(reply)) {
#endif
        return false;
    }

    return is_container_parsable<T>(std::true_type{}, reply);
}

#ifdef REDIS_PLUS_PLUS_HAS_VARIANT

template <typename Result, typename T>
Result parse_variant(redisReply &reply) {
    if (!is_parsable<T>(reply)) {
        throw Error("no variant type matches");
    }

    auto return_var = [](auto &&arg) {
        return Result(std::forward<decltype(arg)>(arg));
    };

    return std::visit(return_var, Variant<T>(parse<T>(reply)));
}

template <typename Result, typename T, typename ...Args>
auto parse_variant(redisReply &reply) ->
    typename std::enable_if<sizeof...(Args) != 0, Result>::type {
    if (is_parsable<T>(reply)) {
        auto return_var = [](auto &&arg) {
            return Result(std::forward<decltype(arg)>(arg));
        };

        return std::visit(return_var, Variant<T>(parse<T>(reply)));
    }

    return parse_variant<Result, Args...>(reply);
}

inline bool is_parsable(ParseTag<Monostate>, redisReply &) {
    return true;
}

template <typename T>
bool is_parsable(ParseTag<Variant<T>>, redisReply &reply) {
    return is_parsable(ParseTag<T>{}, reply);
}

template <typename T, typename ...Args>
auto is_parsable(ParseTag<Variant<T, Args...>>, redisReply &reply) ->
    typename std::enable_if<sizeof...(Args) != 0, bool>::type {
    if (is_parsable(ParseTag<T>{}, reply)) {
        return true;
    }

    return is_parsable(ParseTag<Variant<Args...>>{}, reply);
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

    if (reply.element == nullptr) {
        throw ProtoError("Null PAIR reply");
    }

    if (reply.elements == 1) {
        // Nested array reply. Check the first element of the nested array.
        auto *nested_element = reply.element[0];
        if (nested_element == nullptr) {
            throw ProtoError("null nested PAIR reply");
        }

        return parse(ParseTag<std::pair<T, U>>{}, *nested_element);
    }

    if (reply.elements != 2) {
        throw ProtoError("NOT key-value PAIR reply");
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
    return detail::parse_variant<Variant<Args...>, Args...>(reply);
}

#endif

template <typename T, typename std::enable_if<IsSequenceContainer<T>::value, int>::type>
T parse(ParseTag<T>, redisReply &reply) {
#ifdef REDIS_PLUS_PLUS_RESP_VERSION_3
    if (!is_array(reply) && !is_set(reply)) {
        throw ParseError("ARRAY or SET", reply);
#else
    if (!is_array(reply)) {
        throw ParseError("ARRAY", reply);
#endif
    }

    T container;

    to_array(reply, std::back_inserter(container));

    return container;
}

template <typename T, typename std::enable_if<IsAssociativeContainer<T>::value, int>::type>
T parse(ParseTag<T>, redisReply &reply) {
#ifdef REDIS_PLUS_PLUS_RESP_VERSION_3
    if (!is_array(reply) && !is_map(reply) && !is_set(reply)) {
#else
    if (!is_array(reply)) {
#endif
        throw ParseError("ARRAY", reply);
    }

    T container;

    to_array(reply, std::inserter(container, container.end()));

    return container;
}

template <typename Output>
Cursor parse_scan_reply(redisReply &reply, Output output) {
    if (reply.elements != 2 || reply.element == nullptr) {
        throw ProtoError("Invalid scan reply");
    }

    auto *cursor_reply = reply.element[0];
    auto *data_reply = reply.element[1];
    if (cursor_reply == nullptr || data_reply == nullptr) {
        throw ProtoError("Invalid cursor reply or data reply");
    }

    auto cursor_str = reply::parse<std::string>(*cursor_reply);
    Cursor new_cursor = 0;
    try {
        new_cursor = std::stoull(cursor_str);
    } catch (const std::exception &) {
        throw ProtoError("Invalid cursor reply: " + cursor_str);
    }

    reply::to_array(*data_reply, output);

    return new_cursor;
}

template <typename Output>
void to_array(redisReply &reply, Output output) {
#ifdef REDIS_PLUS_PLUS_RESP_VERSION_3
    if (!is_array(reply) && !is_map(reply) && !is_set(reply)) {
        throw ParseError("ARRAY or MAP or SET", reply);
    }
#else
    if (!is_array(reply)) {
        throw ParseError("ARRAY", reply);
    }
#endif

    detail::to_array(typename IsKvPairIter<Output>::type(), reply, output);
}

template <typename Output>
void to_optional_array(redisReply &reply, Output output) {
    if (is_nil(reply)) {
        return;
    }

    to_array(reply, output);
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
