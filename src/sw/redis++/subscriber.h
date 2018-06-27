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

#ifndef SEWENEW_REDISPLUSPLUS_SUBSCRIBER_H
#define SEWENEW_REDISPLUSPLUS_SUBSCRIBER_H

#include <unordered_map>
#include <string>
#include <functional>
#include "connection.h"
#include "reply.h"
#include "command.h"
#include "utils.h"

namespace sw {

namespace redis {

// @NOTE: Subscriber is NOT thread-safe.
// Subscriber uses callbacks to handle messages. There are 6 kinds of messages:
// 1) MESSAGE: message sent to a channel.
// 2) PMESSAGE: message sent to channels of a given pattern.
// 3) SUBSCRIBE: meta message sent when we successfully subscribe to a channel.
// 4) UNSUBSCRIBE: meta message sent when we successfully unsubscribe to a channel.
// 5) PSUBSCRIBE: meta message sent when we successfully subscribe to a channel pattern.
// 6) PUNSUBSCRIBE: meta message sent when we successfully unsubscribe to a channel pattern.
//
// The callback interface of message of *MESSAGE* type is:
// void (std::string channel, std::string msg)
//
// The callback interface of message of *PMESSAGE* type is:
// void (std::string pattern, std::string channel, std::string msg)
//
// Messages of other types are called *META MESSAGE*, they have the same callback interface:
// void (Subscriber::MsgType type, OptionalString channel, long long num)
//
// *META MESSAGE* callback is optional. If you don't specify one in the constructor,
// these *META MESSAGE*s are ignored.
// All these callback interfaces pass std::string by value, and you can take their ownership
// (i.e. std::move) safely.
class Subscriber {
public:
    Subscriber(const Subscriber &) = delete;
    Subscriber& operator=(const Subscriber &) = delete;

    Subscriber(Subscriber &&) = default;
    Subscriber& operator=(Subscriber &&) = default;

    ~Subscriber() = default;

    enum class MsgType {
        SUBSCRIBE,
        UNSUBSCRIBE,
        PSUBSCRIBE,
        PUNSUBSCRIBE,
        MESSAGE,
        PMESSAGE
    };

    template <typename MsgCb>
    void subscribe(const StringView &channel, MsgCb msg_callback);

    template <typename Input, typename MsgCb>
    void subscribe(Input first, Input last, MsgCb msg_callback);

    template <typename T, typename MsgCb>
    void subscribe(std::initializer_list<T> channels, MsgCb msg_callback) {
        subscribe(channels.begin(), channels.end(), msg_callback);
    }

    void unsubscribe();

    void unsubscribe(const StringView &channel);

    template <typename Input>
    void unsubscribe(Input first, Input last);

    template <typename T>
    void unsubscribe(std::initializer_list<T> channels) {
        unsubscribe(channels.begin(), channels.end());
    }

    template <typename MsgCb>
    void psubscribe(const StringView &pattern, MsgCb msg_callback);

    template <typename Input, typename MsgCb>
    void psubscribe(Input first, Input last, MsgCb msg_callback);

    template <typename T, typename MsgCb>
    void psubscribe(std::initializer_list<T> channels, MsgCb msg_callback) {
        psubscribe(channels.begin(), channels.end(), msg_callback);
    }

    void punsubscribe();

    void punsubscribe(const StringView &channel);

    template <typename Input>
    void punsubscribe(Input first, Input last);

    template <typename T>
    void punsubscribe(std::initializer_list<T> channels) {
        punsubscribe(channels.begin(), channels.end());
    }

    void consume();

private:
    friend class Redis;

    explicit Subscriber(Connection connection);

    template <typename MetaCb>
    Subscriber(Connection connection, MetaCb meta_callback);

    MsgType _msg_type(redisReply *reply) const;

    void _check_connection();

    std::pair<OptionalString, long long> _parse_meta_reply(redisReply &reply) const;

    void _handle_message(redisReply &reply);

    void _handle_pmessage(redisReply &reply);

    void _handle_meta(MsgType type, redisReply &reply);

    using MsgCallback = std::function<void (std::string channel, std::string msg)>;

    using PatternMsgCallback = std::function<void (std::string pattern,
                                                    std::string channel,
                                                    std::string msg)>;

    using MetaCallback = std::function<void (MsgType type,
                                                OptionalString channel,
                                                long long num)>;

    using TypeIndex = std::unordered_map<std::string, MsgType>;
    static const TypeIndex _msg_type_index;

    Connection _connection;

    std::unordered_map<std::string, MsgCallback> _channel_callbacks;

    std::unordered_map<std::string, PatternMsgCallback> _pattern_callbacks;

    MetaCallback _meta_callback = nullptr;
};

template <typename MetaCb>
Subscriber::Subscriber(Connection connection,
                        MetaCb meta_callback) : _connection(std::move(connection)),
                                                _meta_callback(meta_callback) {}

template <typename MsgCb>
void Subscriber::subscribe(const StringView &channel, MsgCb msg_callback) {
    _check_connection();

    if (!_channel_callbacks.emplace(channel, msg_callback).second) {
        throw Error("Channel has already been subscribed");
    }

    // TODO: cmd::subscribe DOES NOT send the subscribe message to Redis.
    // In fact, it puts the command to network buffer.
    // So we need a queue to record these sub or unsub commands, and
    // ensure that before stopping the subscriber, all these commands
    // have really been sent to Redis.
    cmd::subscribe(_connection, channel);
}

template <typename Input, typename MsgCb>
void Subscriber::subscribe(Input first, Input last, MsgCb msg_callback) {
    if (first == last) {
        return;
    }

    _check_connection();

    for (auto iter = first; iter != last; ++iter) {
        if (_channel_callbacks.find(*iter) != _channel_callbacks.end()) {
            throw Error("Channel has already been subscribed");
        }
    }

    for (auto iter = first; iter != last; ++iter) {
        _channel_callbacks.emplace(*first, msg_callback);
    }

    cmd::subscribe_range(_connection, first, last);
}

template <typename Input>
void Subscriber::unsubscribe(Input first, Input last) {
    _check_connection();

    // We don't care if the given channel has been subscribed.

    cmd::unsubscribe_range(_connection, first, last);
}

template <typename MsgCb>
void Subscriber::psubscribe(const StringView &pattern, MsgCb msg_callback) {
    _check_connection();

    if (!_pattern_callbacks.emplace(pattern, msg_callback).second) {
        throw Error("Channel has already been subscribed");
    }

    cmd::psubscribe(_connection, pattern);
}

template <typename Input, typename MsgCb>
void Subscriber::psubscribe(Input first, Input last, MsgCb msg_callback) {
    if (first == last) {
        return;
    }

    _check_connection();

    for (auto iter = first; iter != last; ++iter) {
        if (_pattern_callbacks.find(*iter) != _pattern_callbacks.end()) {
            throw Error("Pattern has already been subscribed");
        }
    }

    for (auto iter = first; iter != last; ++iter) {
        _pattern_callbacks.emplace(*first, msg_callback);
    }

    cmd::psubscribe_range(_connection, first, last);
}

template <typename Input>
void Subscriber::punsubscribe(Input first, Input last) {
    _check_connection();

    // We don't care if the given channel has been subscribed.

    cmd::punsubscribe_range(_connection, first, last);
}

}

}

#endif // end SEWENEW_REDISPLUSPLUS_SUBSCRIBER_H
