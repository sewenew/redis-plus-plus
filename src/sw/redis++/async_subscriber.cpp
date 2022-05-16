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

#include "async_subscriber.h"
#include <cassert>

namespace sw {

namespace redis {

bool SubscribeEvent::handle(redisAsyncContext &ctx) {
    CommandEvent<void, DefaultResultParser<void>>::_handle(ctx, _subscribe_callback);

    _pro.set_value();

    return false;
}

void SubscribeEvent::_subscribe_callback(redisAsyncContext *ctx, void *r, void * /*privdata*/) {
    // NOTE: do not touch `privdata`, which might be an invalid pointer
    assert(ctx != nullptr);

    auto *context = static_cast<AsyncContext *>(ctx->data);
    assert(context != nullptr);

    auto &connection = context->connection;
    assert(connection);

    auto *reply = static_cast<redisReply *>(r);

    connection->subscriber().consume(reply);
}

AsyncSubscriber::AsyncSubscriber(const EventLoopSPtr &loop,
        AsyncConnectionSPtr connection) : _loop(loop), _connection(std::move(connection)) {}

AsyncSubscriber::~AsyncSubscriber() {
    if (_connection) {
        assert(_loop);

        _loop->unwatch(std::move(_connection));
    }
}

Future<void> AsyncSubscriber::subscribe(const StringView &channel) {
    _check_connection();

    return _send(SubscribeEventUPtr(new SubscribeEvent(fmt::subscribe(channel))));
}

Future<void> AsyncSubscriber::unsubscribe() {
    _check_connection();

    return _send(SubscribeEventUPtr(new SubscribeEvent(fmt::unsubscribe())));
}

Future<void> AsyncSubscriber::unsubscribe(const StringView &channel) {
    _check_connection();

    return _send(SubscribeEventUPtr(new SubscribeEvent(fmt::unsubscribe(channel))));
}

Future<void> AsyncSubscriber::psubscribe(const StringView &pattern) {
    _check_connection();

    return _send(SubscribeEventUPtr(new SubscribeEvent(fmt::psubscribe(pattern))));
}

Future<void> AsyncSubscriber::punsubscribe() {
    _check_connection();

    return _send(SubscribeEventUPtr(new SubscribeEvent(fmt::punsubscribe())));
}

Future<void> AsyncSubscriber::punsubscribe(const StringView &channel) {
    _check_connection();

    return _send(SubscribeEventUPtr(new SubscribeEvent(fmt::punsubscribe(channel))));
}

void AsyncSubscriber::_check_connection() {
    if (!_connection || _connection->broken()) {
        throw Error("Connection is broken");
    }
}

Future<void> AsyncSubscriber::_send(SubscribeEventUPtr event) {
    assert(event && _connection);

    auto fut = event->get_future();

    _connection->send(std::move(event));

    return fut;
}

}

}
