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

#include "subscriber.h"
#include <cassert>

namespace sw {

namespace redis {

const Subscriber::TypeIndex Subscriber::_msg_type_index = {
    {"message", MsgType::MESSAGE},
    {"pmessage", MsgType::PMESSAGE},
    {"subscribe", MsgType::SUBSCRIBE},
    {"unsubscribe", MsgType::UNSUBSCRIBE},
    {"psubscribe", MsgType::PSUBSCRIBE},
    {"punsubscribe", MsgType::PUNSUBSCRIBE}
};

Subscriber::Subscriber(ConnectionPool &pool) :
        _pool(pool),
        _connection(_pool.fetch()),
        _stop(std::unique_ptr<std::atomic<bool>>(new std::atomic<bool>(false))),
        _mutex(std::unique_ptr<std::mutex>(new std::mutex)) {}

Subscriber::~Subscriber() {
    try {
        stop();

        _pool.release(std::move(_connection));
    } catch (...) {
        // In case that stop() throws exception,
        // or failed to release connection.
        // Avoid throwing exception from destructor.
        // TODO: if _pool.release() throws, we'll get connection leak problem.
    }
}

void Subscriber::unsubscribe() {
    std::lock_guard<std::mutex> lock(*_mutex);

    _check_connection();

    if (_channel_callbacks.empty()) {
        throw Error("No channel has been subscribed");
    }

    cmd::unsubscribe(_connection);
}

void Subscriber::unsubscribe(const StringView &channel) {
    std::lock_guard<std::mutex> lock(*_mutex);

    _check_connection();

    if (_channel_callbacks.find(std::string(channel.data(), channel.size()))
            == _channel_callbacks.end()) {
        throw Error("Channel has NOT been subscribed");
    }

    cmd::unsubscribe(_connection, channel);
}

void Subscriber::punsubscribe() {
    std::lock_guard<std::mutex> lock(*_mutex);

    _check_connection();

    if (_pattern_callbacks.empty()) {
        throw Error("No pattern has been subscribed");
    }

    cmd::punsubscribe(_connection);
}

void Subscriber::punsubscribe(const StringView &pattern) {
    std::lock_guard<std::mutex> lock(*_mutex);

    _check_connection();

    if (_pattern_callbacks.find(std::string(pattern.data(), pattern.size()))
            == _pattern_callbacks.end()) {
        throw Error("Pattern has NOT been subscribed");
    }

    cmd::punsubscribe(_connection, pattern);
}

void Subscriber::stop() {
    *_stop = true;

    wait();
}

void Subscriber::wait() {
    if (!_future.valid()) {
        // Subscribing thread is NOT running.
        return;
    }

    // Wait until subscribing thread exits, and get possible exceptions.
    _future.get();
}

bool Subscriber::wait_for(const std::chrono::steady_clock::duration &timeout) {
    if (!_future.valid()) {
        // Subscribing thread is NOT running.
        return true;
    }

    // Wait until subscribing thread exits or timeout.
    auto ready = _wait_for(timeout);
    if (ready) {
        // Get possible exceptions.
        _future.get();
    }

    return ready;
}

Subscriber::MsgType Subscriber::_msg_type(redisReply *reply) const {
    if (reply == nullptr) {
        throw ProtoError("Null type reply.");
    }

    auto type = reply::parse<std::string>(*reply);

    auto iter = _msg_type_index.find(type);
    if (iter == _msg_type_index.end()) {
        throw ProtoError("Invalid message type.");
    }

    return iter->second;
}

void Subscriber::_check_connection() {
    if (_connection.broken()) {
        throw Error("Connection is broken");
    }
}

bool Subscriber::_wait_for(const std::chrono::steady_clock::duration &timeout) {
    assert(_future.valid());

    auto status = _future.wait_for(timeout);
    if (status == std::future_status::timeout) {
        return false;
    } else if (status == std::future_status::ready) {
        return true;
    } else {
        // The subscribing thread should NOT be deferred.
        throw Error("Subscribing thread has been deferred");
    }
}

void Subscriber::_lazy_start_subscribe() {
    if (!_future.valid()) {
        *_stop = false;
        _future = std::async(&Subscriber::_consume, this);
    }
}

std::pair<std::string, long long> Subscriber::_parse_meta_reply(redisReply &reply) const {
    if (reply.elements != 3) {
        throw ProtoError("Expect 3 sub replies");
    }

    assert(reply.element != nullptr);

    auto *name_reply = reply.element[1];
    if (name_reply == nullptr) {
        throw ProtoError("Null name reply");
    }

    auto *num_reply = reply.element[2];
    if (num_reply == nullptr) {
        throw ProtoError("Null num reply");
    }

    return {
            reply::parse<std::string>(*name_reply),
            reply::parse<long long>(*num_reply)
    };
}

bool Subscriber::_handle_message(redisReply &reply) {
    if (reply.elements != 3) {
        throw ProtoError("Expect 3 sub replies");
    }

    assert(reply.element != nullptr);

    auto *channel_reply = reply.element[1];
    if (channel_reply == nullptr) {
        throw ProtoError("Null channel reply");
    }
    auto channel = reply::parse<std::string>(*channel_reply);

    auto *msg_reply = reply.element[2];
    if (msg_reply == nullptr) {
        throw ProtoError("Null message reply");
    }
    auto msg = reply::parse<std::string>(*msg_reply);

    auto iter = _channel_callbacks.find(channel);
    if (iter == _channel_callbacks.end()) {
        throw Error("Unknown channel");
    }

    return iter->second.msg_callback(channel, msg);
}

bool Subscriber::_handle_pmessage(redisReply &reply) {
    if (reply.elements != 4) {
        throw ProtoError("Expect 4 sub replies");
    }

    assert(reply.element != nullptr);

    auto *pattern_reply = reply.element[1];
    if (pattern_reply == nullptr) {
        throw ProtoError("Null pattern reply");
    }
    auto pattern = reply::parse<std::string>(*pattern_reply);

    auto *channel_reply = reply.element[2];
    if (channel_reply == nullptr) {
        throw ProtoError("Null channel reply");
    }
    auto channel = reply::parse<std::string>(*channel_reply);

    auto *msg_reply = reply.element[3];
    if (msg_reply == nullptr) {
        throw ProtoError("Null message reply");
    }
    auto msg = reply::parse<std::string>(*msg_reply);

    auto iter = _pattern_callbacks.find(pattern);
    if (iter == _pattern_callbacks.end()) {
        throw Error("Unknown pattern");
    }

    return iter->second.msg_callback(pattern, channel, msg);
}

bool Subscriber::_handle_subscribe(redisReply &reply) {
    auto meta = _parse_meta_reply(reply);

    auto iter = _channel_callbacks.find(meta.first);
    if (iter == _channel_callbacks.end()) {
        throw Error("Unknown channel");
    }

    return iter->second.sub_callback(meta.first, meta.second);
}

bool Subscriber::_handle_unsubscribe(redisReply &reply) {
    auto meta = _parse_meta_reply(reply);

    auto iter = _channel_callbacks.find(meta.first);
    if (iter == _channel_callbacks.end()) {
        throw Error("Unknown channel");
    }

    auto ret = iter->second.unsub_callback(meta.first, meta.second);

    _channel_callbacks.erase(iter);

    return ret;
}

bool Subscriber::_handle_psubscribe(redisReply &reply) {
    auto meta = _parse_meta_reply(reply);

    auto iter = _pattern_callbacks.find(meta.first);
    if (iter == _pattern_callbacks.end()) {
        throw Error("Unknown pattern");
    }

    return iter->second.sub_callback(meta.first, meta.second);
}

bool Subscriber::_handle_punsubscribe(redisReply &reply) {
    auto meta = _parse_meta_reply(reply);

    auto iter = _pattern_callbacks.find(meta.first);
    if (iter == _pattern_callbacks.end()) {
        throw Error("Unknown pattern");
    }

    auto ret = iter->second.unsub_callback(meta.first, meta.second);

    _pattern_callbacks.erase(iter);

    return ret;
}

void Subscriber::_consume() {
    while (true) {
        // TODO: Narrow down the scope of the lock.
        std::lock_guard<std::mutex> lock(*_mutex);

        _check_connection();

        if (*_stop) {
            _stop_subscribe();
            break;
        }

        try {
            auto reply = _connection.recv();

            assert(reply);

            if (reply->elements < 1 || reply->element == nullptr) {
                throw ProtoError("Invalid subscribe message");
            }

            auto type = _msg_type(reply->element[0]);
            auto ret = true;
            switch (type) {
            case MsgType::MESSAGE:
                ret = _handle_message(*reply);
                break;

            case MsgType::PMESSAGE:
                ret = _handle_pmessage(*reply);
                break;

            case MsgType::SUBSCRIBE:
                ret = _handle_subscribe(*reply);
                break;

            case MsgType::UNSUBSCRIBE:
                ret = _handle_unsubscribe(*reply);
                break;

            case MsgType::PSUBSCRIBE:
                ret = _handle_psubscribe(*reply);
                break;

            case MsgType::PUNSUBSCRIBE:
                ret = _handle_punsubscribe(*reply);
                break;

            default:
                assert(false);
            }

            if (!ret) {
                // Stop consuming.
                _stop_subscribe();
                break;
            }
        } catch (const TimeoutError &e) {
            // Try latter.
            continue;
        } catch (...) {
            // TODO: should we allow an error_callback to handle the exception?
            _stop_subscribe();
            throw;
        }
    }
}

void Subscriber::_stop_subscribe() {
    *_stop = true;

    _channel_callbacks.clear();
    _pattern_callbacks.clear();

    // Reset connection.
    // TODO: Maybe unsubscribe all channels should be better.
    try {
        _pool.reconnect(_connection);
    } catch (const Error &e) {
        // At least we broke the connection.
        return;
    }
}

}

}
