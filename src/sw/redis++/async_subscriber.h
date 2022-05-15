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

#ifndef SEWENEW_REDISPLUSPLUS_ASYNC_SUBSCRIBER_H
#define SEWENEW_REDISPLUSPLUS_ASYNC_SUBSCRIBER_H

#include <memory>
#include "async_connection.h"
#include "async_subscriber_impl.h"

namespace sw {

namespace redis {

class SubscribeEvent : public CommandEvent<void, DefaultResultParser<void>> {
public:
    explicit SubscribeEvent(FormattedCommand cmd) :
        CommandEvent<void, DefaultResultParser<void>>(std::move(cmd)) {}

    virtual bool handle(redisAsyncContext &ctx) override;

private:
    static void _subscribe_callback(redisAsyncContext *ctx, void *r, void * /*privdata*/);
};

using SubscribeEventUPtr = std::unique_ptr<SubscribeEvent>;

class AsyncSubscriber {
public:
    AsyncSubscriber(const AsyncSubscriber &) = delete;
    AsyncSubscriber& operator=(const AsyncSubscriber &) = delete;

    AsyncSubscriber(AsyncSubscriber &&) = default;
    AsyncSubscriber& operator=(AsyncSubscriber &&) = default;

    ~AsyncSubscriber();

    template <typename MsgCb>
    void on_message(MsgCb &&msg_callback);

    template <typename PMsgCb>
    void on_pmessage(PMsgCb &&pmsg_callback);

    template <typename MetaCb>
    void on_meta(MetaCb &&meta_callback);

    template <typename ErrCb>
    void on_error(ErrCb &&err_callback);

    Future<void> subscribe(const StringView &channel);

    template <typename Input>
    Future<void> subscribe(Input first, Input last);

    template <typename T>
    Future<void> subscribe(std::initializer_list<T> channels) {
        return subscribe(channels.begin(), channels.end());
    }

    Future<void> unsubscribe();

    Future<void> unsubscribe(const StringView &channel);

    template <typename Input>
    Future<void> unsubscribe(Input first, Input last);

    template <typename T>
    Future<void> unsubscribe(std::initializer_list<T> channels) {
        return unsubscribe(channels.begin(), channels.end());
    }

    Future<void> psubscribe(const StringView &pattern);

    template <typename Input>
    Future<void> psubscribe(Input first, Input last);

    template <typename T>
    Future<void> psubscribe(std::initializer_list<T> channels) {
        return psubscribe(channels.begin(), channels.end());
    }

    Future<void> punsubscribe();

    Future<void> punsubscribe(const StringView &channel);

    template <typename Input>
    Future<void> punsubscribe(Input first, Input last);

    template <typename T>
    Future<void> punsubscribe(std::initializer_list<T> channels) {
        return punsubscribe(channels.begin(), channels.end());
    }

private:
    friend class AsyncRedis;

    friend class AsyncRedisCluster;

    AsyncSubscriber(const EventLoopSPtr &loop, AsyncConnectionSPtr connection);

    void _check_connection();

    Future<void> _send(SubscribeEventUPtr event);

    EventLoopSPtr _loop;

    AsyncConnectionSPtr _connection;
};

template <typename MsgCb>
void AsyncSubscriber::on_message(MsgCb &&msg_callback) {
    _check_connection();

    _connection->subscriber().on_message(std::forward<MsgCb>(msg_callback));
}

template <typename PMsgCb>
void AsyncSubscriber::on_pmessage(PMsgCb &&pmsg_callback) {
    _check_connection();

    _connection->subscriber().on_pmessage(std::forward<PMsgCb>(pmsg_callback));
}

template <typename MetaCb>
void AsyncSubscriber::on_meta(MetaCb &&meta_callback) {
    _check_connection();

    _connection->subscriber().on_meta(std::forward<MetaCb>(meta_callback));
}

template <typename ErrCb>
void AsyncSubscriber::on_error(ErrCb &&err_callback) {
    _check_connection();

    _connection->subscriber().on_error(std::forward<ErrCb>(err_callback));
}

template <typename Input>
Future<void> AsyncSubscriber::subscribe(Input first, Input last) {
    range_check("subscribe", first, last);

    _check_connection();

    return _send(SubscribeEventUPtr(new SubscribeEvent(fmt::subscribe_range(first, last))));
}

template <typename Input>
Future<void> AsyncSubscriber::unsubscribe(Input first, Input last) {
    range_check("unsubscribe", first, last);

    _check_connection();

    return _send(SubscribeEventUPtr(new SubscribeEvent(fmt::unsubscribe_range(first, last))));
}

template <typename Input>
Future<void> AsyncSubscriber::psubscribe(Input first, Input last) {
    range_check("psubscribe", first, last);

    _check_connection();

    return _send(SubscribeEventUPtr(new SubscribeEvent(fmt::psubscribe_range(first, last))));
}

template <typename Input>
Future<void> AsyncSubscriber::punsubscribe(Input first, Input last) {
    range_check("punsubscribe", first, last);

    _check_connection();

    return _send(SubscribeEventUPtr(new SubscribeEvent(fmt::punsubscribe_range(first, last))));
}

}

}

#endif // end SEWENEW_REDISPLUSPLUS_ASYNC_SUBSCRIBER_H
