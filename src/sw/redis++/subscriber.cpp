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

Subscriber::Subscriber(Connection connection) : _connection(std::move(connection)) {}

void Subscriber::unsubscribe() {
    _check_connection();

    cmd::unsubscribe(_connection);
}

void Subscriber::unsubscribe(const StringView &channel) {
    _check_connection();

    cmd::unsubscribe(_connection, channel);
}

void Subscriber::punsubscribe() {
    _check_connection();

    cmd::punsubscribe(_connection);
}

void Subscriber::punsubscribe(const StringView &pattern) {
    _check_connection();

    cmd::punsubscribe(_connection, pattern);
}

void Subscriber::consume() {
    _check_connection();

    auto reply = _connection.recv();

    assert(reply);

    if (!reply::is_array(*reply) || reply->elements < 1 || reply->element == nullptr) {
        throw ProtoError("Invalid subscribe message");
    }

    auto type = _msg_type(reply->element[0]);
    switch (type) {
    case MsgType::MESSAGE:
        _handle_message(*reply);
        break;

    case MsgType::PMESSAGE:
        _handle_pmessage(*reply);
        break;

    case MsgType::SUBSCRIBE:
    case MsgType::UNSUBSCRIBE:
    case MsgType::PSUBSCRIBE:
    case MsgType::PUNSUBSCRIBE:
        _handle_meta(type, *reply);
        break;

    default:
        assert(false);
    }
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

std::pair<OptionalString, long long> Subscriber::_parse_meta_reply(redisReply &reply) const {
    if (reply.elements != 3) {
        throw ProtoError("Expect 3 sub replies");
    }

    assert(reply.element != nullptr);

    std::string name;
    auto *name_reply = reply.element[1];
    if (name_reply == nullptr) {
        throw ProtoError("Null channel reply");
    }

    auto *num_reply = reply.element[2];
    if (num_reply == nullptr) {
        throw ProtoError("Null num reply");
    }

    return {
        reply::parse<OptionalString>(*name_reply),
        reply::parse<long long>(*num_reply)
    };
    //auto num = reply::parse<long long>(*num_reply);

    //return {std::move(name), num};
}

void Subscriber::_handle_message(redisReply &reply) {
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

    iter->second(std::move(channel), std::move(msg));
}

void Subscriber::_handle_pmessage(redisReply &reply) {
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

    iter->second(std::move(pattern), std::move(channel), std::move(msg));
}

void Subscriber::_handle_meta(MsgType type, redisReply &reply) {
    if (_meta_callback != nullptr) {
        auto meta = _parse_meta_reply(reply);
        _meta_callback(type, std::move(meta.first), meta.second);
    }
}

}

}
