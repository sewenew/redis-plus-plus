/**************************************************************************
   Copyright (c) 2022 sewenew

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

#include "async_subscriber_impl.h"

namespace sw {

namespace redis {

void AsyncSubscriberImpl::consume(redisReply *reply) {
    try {
        if (reply == nullptr) {
            // Connection has been closed.
            _run_err_callback(std::make_exception_ptr(Error("connection has been closed")));
        } else if (reply::is_error(*reply)) {
            try {
                throw_error(*reply);
            } catch (const TimeoutError &) {
                // TODO: need to reset connection and run err callback
            } catch (const Error &) {
                _run_err_callback(std::current_exception());
            }
        } else {
            _run_callback(*reply);
        }
    } catch (...) {
        _run_err_callback(std::current_exception());
    }
}

void AsyncSubscriberImpl::_run_err_callback(std::exception_ptr err) {
    if (_err_callback) {
        _err_callback(err);
    }
}

void AsyncSubscriberImpl::_run_callback(redisReply &reply) {
    if (!reply::is_array(reply) || reply.elements < 1 || reply.element == nullptr) {
        throw ProtoError("Invalid subscribe message");
    }

    auto type = _msg_type(reply.element[0]);
    switch (type) {
    case Subscriber::MsgType::MESSAGE:
        _handle_message(reply);
        break;

    case Subscriber::MsgType::PMESSAGE:
        _handle_pmessage(reply);
        break;

    case Subscriber::MsgType::SUBSCRIBE:
    case Subscriber::MsgType::UNSUBSCRIBE:
    case Subscriber::MsgType::PSUBSCRIBE:
    case Subscriber::MsgType::PUNSUBSCRIBE:
        _handle_meta(type, reply);
        break;

    default:
        assert(false);
    }
}

Subscriber::MsgType AsyncSubscriberImpl::_msg_type(redisReply *reply) const {
    if (reply == nullptr) {
        throw ProtoError("Null type reply.");
    }

    return _msg_type(reply::parse<std::string>(*reply));
}

Subscriber::MsgType AsyncSubscriberImpl::_msg_type(const std::string &type) const {
    if ("message" == type) {
        return Subscriber::MsgType::MESSAGE;
    } else if ("pmessage" == type) {
        return Subscriber::MsgType::PMESSAGE;
    } else if ("subscribe" == type) {
        return Subscriber::MsgType::SUBSCRIBE;
    } else if ("unsubscribe" == type) {
        return Subscriber::MsgType::UNSUBSCRIBE;
    } else if ("psubscribe" == type) {
        return Subscriber::MsgType::PSUBSCRIBE;
    } else if ("punsubscribe" == type) {
        return Subscriber::MsgType::PUNSUBSCRIBE;
    }

    throw ProtoError("Invalid message type.");
    return Subscriber::MsgType::MESSAGE; // Silence "no return" warnings.
}

void AsyncSubscriberImpl::_handle_message(redisReply &reply) {
    if (_msg_callback == nullptr) {
        return;
    }

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

    _msg_callback(std::move(channel), std::move(msg));
}

void AsyncSubscriberImpl::_handle_pmessage(redisReply &reply) {
    if (_pmsg_callback == nullptr) {
        return;
    }

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

    _pmsg_callback(std::move(pattern), std::move(channel), std::move(msg));
}

void AsyncSubscriberImpl::_handle_meta(Subscriber::MsgType type, redisReply &reply) {
    if (_meta_callback == nullptr) {
        return;
    }

    if (reply.elements != 3) {
        throw ProtoError("Expect 3 sub replies");
    }

    assert(reply.element != nullptr);

    auto *channel_reply = reply.element[1];
    if (channel_reply == nullptr) {
        throw ProtoError("Null channel reply");
    }
    auto channel = reply::parse<OptionalString>(*channel_reply);

    auto *num_reply = reply.element[2];
    if (num_reply == nullptr) {
        throw ProtoError("Null num reply");
    }
    auto num = reply::parse<long long>(*num_reply);

    _meta_callback(type, std::move(channel), num);
}

}

}
