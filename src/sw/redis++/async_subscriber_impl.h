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

#ifndef SEWENEW_REDISPLUSPLUS_ASYNC_SUBSCRIBER_IMPL_H
#define SEWENEW_REDISPLUSPLUS_ASYNC_SUBSCRIBER_IMPL_H

#include <memory>
#include "sw/redis++/reply.h"
#include "sw/redis++/subscriber.h"

namespace sw {

namespace redis {

class AsyncSubscriberImpl {
public:
    // TODO: there's duplicate code between Subscriber and AsyncSubscriberImpl
    void consume(redisReply *reply);

    template <typename MsgCb>
    void on_message(MsgCb &&msg_callback) {
        _msg_callback = std::forward<MsgCb>(msg_callback);
    }

    template <typename PMsgCb>
    void on_pmessage(PMsgCb &&pmsg_callback) {
        _pmsg_callback = std::forward<PMsgCb>(pmsg_callback);
    }

    template <typename SMsgCb>
    void on_smessage(SMsgCb &&smsg_callback) {
        _smsg_callback = std::forward<SMsgCb>(smsg_callback);
    }

    template <typename MetaCb>
    void on_meta(MetaCb &&meta_callback) {
        _meta_callback = std::forward<MetaCb>(meta_callback);
    }

    template <typename ErrCb>
    void on_error(ErrCb &&err_callback) {
        _err_callback = std::forward<ErrCb>(err_callback);
    }

private:
    void _run_callback(redisReply &reply);

    void _run_err_callback(std::exception_ptr err);

    Subscriber::MsgType _msg_type(redisReply *reply) const;

    Subscriber::MsgType _msg_type(const std::string &type) const;

    void _handle_message(redisReply &reply);

    void _handle_pmessage(redisReply &reply);

    void _handle_smessage(redisReply &reply);

    void _handle_meta(Subscriber::MsgType type, redisReply &reply);

    std::function<void (std::string channel, std::string msg)> _msg_callback;

    std::function<void (std::string pattern, std::string channel,
            std::string msg)> _pmsg_callback;

    std::function<void (std::string channel, std::string msg)> _smsg_callback;

    std::function<void (Subscriber::MsgType type, OptionalString channel,
            long long num)> _meta_callback;

    std::function<void (std::exception_ptr)> _err_callback;
};

using AsyncSubscriberImplUPtr = std::unique_ptr<AsyncSubscriberImpl>;

}

}

#endif // end SEWENEW_REDISPLUSPLUS_ASYNC_SUBSCRIBER_IMPL_H
