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
#include <chrono>
#include <string>
#include <functional>
#include <atomic>
#include <mutex>
#include <future>
#include "connection_pool.h"
#include "reply.h"
#include "command.h"
#include "utils.h"

namespace sw {

namespace redis {

class Subscriber {
public:
    Subscriber(const Subscriber &) = delete;
    Subscriber& operator=(const Subscriber &) = delete;

    Subscriber(Subscriber &&) = default;
    Subscriber& operator=(Subscriber &&) = default;

    ~Subscriber();

    struct IgnoreMeta {
        bool operator()(const StringView &, long long) const {
            return true;
        }
    };

    template <typename MsgCb,
                typename SubCb = IgnoreMeta,
                typename UnsubCb = IgnoreMeta>
    void subscribe(const StringView &channel,
                    MsgCb msg_callback,
                    SubCb sub_callback = IgnoreMeta{},
                    UnsubCb unsub_callback = IgnoreMeta{});

    template <typename Input,
                typename MsgCb,
                typename SubCb = IgnoreMeta,
                typename UnsubCb = IgnoreMeta>
    void subscribe(Input first,
                    Input last,
                    MsgCb msg_callback,
                    SubCb sub_callback = IgnoreMeta{},
                    UnsubCb unsub_callback = IgnoreMeta{});

    void unsubscribe();

    void unsubscribe(const StringView &channel);

    template <typename Input>
    void unsubscribe(Input first, Input last);

    template <typename MsgCb,
                typename SubCb = IgnoreMeta,
                typename UnsubCb = IgnoreMeta>
    void psubscribe(const StringView &pattern,
                    MsgCb msg_callback,
                    SubCb sub_callback = IgnoreMeta{},
                    UnsubCb unsub_callback = IgnoreMeta{});

    template <typename Input,
                typename MsgCb,
                typename SubCb = IgnoreMeta,
                typename UnsubCb = IgnoreMeta>
    void psubscribe(Input first,
                    Input last,
                    MsgCb msg_callback,
                    SubCb sub_callback = IgnoreMeta{},
                    UnsubCb unsub_callback = IgnoreMeta{});

    void punsubscribe();

    void punsubscribe(const StringView &channel);

    template <typename Input>
    void punsubscribe(Input first, Input last);

    // Stop the subscribing thread.
    void stop();

    // Wait the subscribing thread exits, i.e. something bad happens,
    // and throws exception.
    void wait();

    // Wait the subscribing thread exits or timeout.
    // Return false if the timeout expired, otherwise, true.
    bool wait_for(const std::chrono::steady_clock::duration &timeout);

private:
    friend class Redis;

    explicit Subscriber(ConnectionPool &pool);

    enum class MsgType {
        SUBSCRIBE,
        UNSUBSCRIBE,
        PSUBSCRIBE,
        PUNSUBSCRIBE,
        MESSAGE,
        PMESSAGE
    };

    MsgType _msg_type(redisReply *reply) const;

    bool _wait_for(const std::chrono::steady_clock::duration &timeout);

    void _lazy_start_subscribe();

    std::pair<std::string, long long> _parse_meta_reply(redisReply &reply) const;

    bool _handle_message(redisReply &reply);

    bool _handle_pmessage(redisReply &reply);

    bool _handle_subscribe(redisReply &reply);

    bool _handle_unsubscribe(redisReply &reply);

    bool _handle_psubscribe(redisReply &reply);

    bool _handle_punsubscribe(redisReply &reply);

    void _consume();

    void _stop_subscribe();

    ConnectionPool &_pool;

    Connection _connection;

    using MsgCallback = std::function<bool (const StringView &channel, const StringView &msg)>;

    using PatternMsgCallback = std::function<bool (const StringView &pattern,
                                                    const StringView &channel,
                                                    const StringView &msg)>;

    using MetaCallback = std::function<bool (const StringView &channel, long long num)>;

    struct ChannelCallbacks {
        MsgCallback msg_callback;
        MetaCallback sub_callback;
        MetaCallback unsub_callback;
    };

    struct PatternCallbacks {
        PatternMsgCallback msg_callback;
        MetaCallback sub_callback;
        MetaCallback unsub_callback;
    };

    using TypeIndex = std::unordered_map<std::string, MsgType>;
    static const TypeIndex _msg_type_index;

    std::unordered_map<std::string, ChannelCallbacks> _channel_callbacks;

    std::unordered_map<std::string, PatternCallbacks> _pattern_callbacks;

    std::unique_ptr<std::atomic<bool>> _stop;

    std::unique_ptr<std::mutex> _mutex;

    std::future<void> _future;
};

template <typename MsgCb, typename SubCb, typename UnsubCb>
void Subscriber::subscribe(const StringView &channel,
                            MsgCb msg_callback,
                            SubCb sub_callback,
                            UnsubCb unsub_callback) {
    std::lock_guard<std::mutex> lock(*_mutex);

    // TODO: check if _connection is broken.

    _lazy_start_subscribe();

    ChannelCallbacks callbacks = {msg_callback, sub_callback, unsub_callback};
    if (!_channel_callbacks.emplace(channel, callbacks).second) {
        throw Error("Channel has already been subscribed");
    }

    // TODO: cmd::subscribe DOES NOT send the subscribe message to Redis.
    // In fact, it puts the command to network buffer.
    // So we need a queue to record these sub or unsub commands, and
    // ensure that before stopping the subscriber, all these commands
    // have really been sent to Redis.
    cmd::subscribe(_connection, channel);
}

template <typename Input, typename MsgCb, typename SubCb, typename UnsubCb>
void Subscriber::subscribe(Input first,
                            Input last,
                            MsgCb msg_callback,
                            SubCb sub_callback,
                            UnsubCb unsub_callback) {
    if (first == last) {
        return;
    }

    std::lock_guard<std::mutex> lock(*_mutex);

    _lazy_start_subscribe();

    for (auto iter = first; iter != last; ++iter) {
        if (_channel_callbacks.find(*iter) != _channel_callbacks.end()) {
            throw Error("Channel has already been subscribed");
        }
    }

    ChannelCallbacks callbacks = {msg_callback, sub_callback, unsub_callback};
    for (auto iter = first; iter != last; ++iter) {
        _channel_callbacks.emplace(*first, callbacks);
    }

    cmd::subscribe_range(_connection, first, last);
}

template <typename Input>
void Subscriber::unsubscribe(Input first, Input last) {
    std::lock_guard<std::mutex> lock(*_mutex);

    for (auto iter = first; iter != last; ++iter) {
        if (_channel_callbacks.find(*iter) == _channel_callbacks.end()) {
            throw Error("Try to unsubscribe unsubscribed channel");
        }
    }

    cmd::unsubscribe_range(_connection, first, last);
}

template <typename MsgCb, typename SubCb, typename UnsubCb>
void Subscriber::psubscribe(const StringView &pattern,
                            MsgCb msg_callback,
                            SubCb sub_callback,
                            UnsubCb unsub_callback) {
    std::lock_guard<std::mutex> lock(*_mutex);

    _lazy_start_subscribe();

    PatternCallbacks callbacks = {msg_callback, sub_callback, unsub_callback};
    if (!_pattern_callbacks.emplace(pattern, callbacks).second) {
        throw Error("Channel has already been subscribed");
    }

    cmd::psubscribe(_connection, pattern);
}

template <typename Input, typename MsgCb, typename SubCb, typename UnsubCb>
void Subscriber::psubscribe(Input first,
                            Input last,
                            MsgCb msg_callback,
                            SubCb sub_callback,
                            UnsubCb unsub_callback) {
    if (first == last) {
        return;
    }

    std::lock_guard<std::mutex> lock(*_mutex);

    _lazy_start_subscribe();

    for (auto iter = first; iter != last; ++iter) {
        if (_pattern_callbacks.find(*iter) != _pattern_callbacks.end()) {
            throw Error("Pattern has already been subscribed");
        }
    }

    PatternCallbacks callbacks = {msg_callback, sub_callback, unsub_callback};
    for (auto iter = first; iter != last; ++iter) {
        _pattern_callbacks.emplace(*first, callbacks);
    }

    cmd::psubscribe_range(_connection, first, last);
}

template <typename Input>
void Subscriber::punsubscribe(Input first, Input last) {
    std::lock_guard<std::mutex> lock(*_mutex);

    for (auto iter = first; iter != last; ++iter) {
        if (_pattern_callbacks.find(*iter) == _pattern_callbacks.end()) {
            throw Error("Try to unsubscribe unsubscribed pattern");
        }
    }

    cmd::punsubscribe_range(_connection, first, last);
}

}

}

#endif // end SEWENEW_REDISPLUSPLUS_SUBSCRIBER_H
