/**************************************************************************
   Copyright (c) 2021 sewenew

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

#include "event_loop.h"
#include <cassert>
#include <hiredis/adapters/libuv.h>
#include "async_connection.h"

namespace sw {

namespace redis {

EventLoop::EventLoop() {
    _loop = _create_event_loop();

    _event_async = _create_uv_async(_event_callback);
    _stop_async = _create_uv_async(_stop_callback);

    _loop_thread = std::thread([this]() { uv_run(this->_loop.get(), UV_RUN_DEFAULT); });
}

EventLoop::~EventLoop() {
    _stop();

    if (_loop_thread.joinable()) {
        _loop_thread.join();
    }
}

void EventLoop::unwatch(AsyncConnectionSPtr connection) {
    assert(connection);

    {
        std::lock_guard<std::mutex> lock(_mtx);

        _disconnect_events.push_back(std::move(connection));
    }

    notify();
}

void EventLoop::add(AsyncEventUPtr event) {
    assert(event);

    {
        std::lock_guard<std::mutex> lock(_mtx);

        _command_events.push_back(std::move(event));
    }

    notify();
}

void EventLoop::attach(redisAsyncContext &ctx) {
    if (redisLibuvAttach(&ctx, _loop.get()) != REDIS_OK) {
        throw Error("failed to attach to event loop");
    }

    redisAsyncSetConnectCallback(&ctx, EventLoop::_connect_callback);
    redisAsyncSetDisconnectCallback(&ctx, EventLoop::_disconnect_callback);
}

void EventLoop::_connect_callback(const redisAsyncContext *ctx, int status) {
    assert(ctx != nullptr);

    auto &connection = *(static_cast<AsyncConnectionSPtr *>(ctx->data));
    assert(connection);

    if (status == REDIS_OK) {
        connection->set_status(AsyncConnectionStatus::CONNECTED);
        connection->set_error({});
        connection->prepare();
        return;
    }

    connection->reset();

    try {
        throw_error(ctx->c, "failed to connect to server");
    } catch (const Error &e) {
        connection->set_error(std::current_exception());
        connection->set_status(AsyncConnectionStatus::CONNECT_FAILED);
    }

    // In case, there're pending commands.
    connection->notify();
}

void EventLoop::_disconnect_callback(const redisAsyncContext *ctx, int status) {
    assert(ctx != nullptr);

    auto &connection = *(static_cast<AsyncConnectionSPtr *>(ctx->data));
    assert(connection);

    if (status != REDIS_OK) {
        // TODO: should we add another status: DISCONNECT_FAILED?
        try {
            throw_error(ctx->c, "failed to disconnect from server");
        } catch (const Error &e) {
            connection->set_error(std::current_exception());
        }
    }

    connection->reset();
    connection->set_status(AsyncConnectionStatus::DISCONNECTED);

    // In case, there're pending commands.
    connection->notify();
}

void EventLoop::_event_callback(uv_async_t *handle) {
    if (handle == nullptr) {
        // This should not happen
        return;
    }

    auto *event_loop = static_cast<EventLoop*>(handle->data);
    assert(event_loop != nullptr);

    std::vector<AsyncEventUPtr> command_events;
    std::vector<AsyncConnectionSPtr> disconnect_events;
    std::tie(command_events, disconnect_events) = event_loop->_event();

    std::unordered_set<AsyncConnection *> disconnecting_connections;
    for (auto &event : disconnect_events) {
        disconnecting_connections.insert(event.get());
    }

    auto pending_events = event_loop->_send_commands(std::move(command_events),
            disconnecting_connections);

    event_loop->_disconnect(disconnect_events);

    {
        std::lock_guard<std::mutex> lock(event_loop->_mtx);

        event_loop->_command_events.insert(event_loop->_command_events.begin(),
                std::make_move_iterator(pending_events.begin()),
                std::make_move_iterator(pending_events.end()));
    }
}

void EventLoop::_stop_callback(uv_async_t *handle) {
    if (handle == nullptr) {
        // This should not happen
        return;
    }

    auto *event_loop = static_cast<EventLoop*>(handle->data);
    assert(event_loop != nullptr);

    std::vector<AsyncEventUPtr> command_events;
    std::vector<AsyncConnectionSPtr> disconnect_events;
    std::tie(command_events, disconnect_events) = event_loop->_event();

    event_loop->_clean_up(command_events, disconnect_events);

    uv_stop(event_loop->_loop.get());
}

void EventLoop::_clean_up(std::vector<AsyncEventUPtr> &command_events,
        std::vector<AsyncConnectionSPtr> &disconnect_events) {
    auto e = std::make_exception_ptr(Error("event loop is closing"));
    for (auto &event : command_events) {
        assert(event);

        event->set_exception(e);
    }

    for (auto &connection : disconnect_events) {
        assert(connection);

        auto *ctx = connection->context();
        if (ctx != nullptr) {
            auto &connection = *(static_cast<AsyncConnectionSPtr *>(ctx->data));
            assert(connection);

            connection->set_error(e);

            redisAsyncFree(ctx);
        }
    }
}

void EventLoop::LoopDeleter::operator()(uv_loop_t *loop) const {
    if (loop == nullptr) {
        return;
    }

    // How to correctly close an event loop:
    // https://stackoverflow.com/questions/25615340/closing-libuv-handles-correctly
    // TODO: do we need to call this? Since we always has 2 async_t handles.
    if (uv_loop_close(loop) == 0) {
        delete loop;

        return;
    }

    uv_walk(loop,
            [](uv_handle_t *handle, void *) {
                if (handle != nullptr) {
                    // We don't need to release handle's memory in close callback,
                    // since we'll release the memory in EventLoop's destructor.
                    uv_close(handle, nullptr);
                }
            },
            nullptr);

    // Ensure uv_walk's callback to be called.
    uv_run(loop, UV_RUN_DEFAULT);

    uv_loop_close(loop);

    delete loop;
}

void EventLoop::_disconnect(std::vector<AsyncConnectionSPtr> &connections) {
    for (auto &connection : connections) {
        assert(connection);

        // TODO: use _status to do the check
        if (!connection->broken()) {
            redisAsyncDisconnect(connection->context());
            connection->set_status(AsyncConnectionStatus::DISCONNECTING);
        }
    }
}

auto EventLoop::_send_commands(std::vector<AsyncEventUPtr> events,
        const std::unordered_set<AsyncConnection *> &disconnecting_connections)
    -> PendingEvents {
    PendingEvents pending_events;
    for (auto &event : events) {
        assert(event);

        auto *connection = event->connection();
        switch (connection->status()) {
        case AsyncConnectionStatus::UNINITIALIZED:
        case AsyncConnectionStatus::DISCONNECTED:
            connection->reconnect();
            // fall through

        case AsyncConnectionStatus::CONNECTING:
        case AsyncConnectionStatus::CONNECTED:
        case AsyncConnectionStatus::AUTH:
        case AsyncConnectionStatus::SELECT_DB:
            if (disconnecting_connections.find(connection) == disconnecting_connections.end()) {
                pending_events.push_back(std::move(event));
            } else {
                static auto e = std::make_exception_ptr(Error("connection is closing"));
                event->set_exception(e);
            }
            break;

        case AsyncConnectionStatus::DISCONNECTING:
        case AsyncConnectionStatus::CONNECT_FAILED:
            event->set_exception(connection->error());
            break;

        case AsyncConnectionStatus::READY:
            try {
                event->handle();

                // CommandEvent::_reply_callback will release the memory.
                event.release();
            } catch (...) {
                event->set_exception(std::current_exception());
            }
            break;

        default:
            assert(false);
        }
    }

    return pending_events;
}

void EventLoop::notify() {
    assert(_event_async);

    uv_async_send(_event_async.get());
}

void EventLoop::_stop() {
    assert(_stop_async);

    uv_async_send(_stop_async.get());
}

auto EventLoop::_event()
    -> std::pair<std::vector<AsyncEventUPtr>, std::vector<AsyncConnectionSPtr>> {
    std::vector<AsyncEventUPtr> command_events;
    std::vector<AsyncConnectionSPtr> disconnect_events;
    {
        std::lock_guard<std::mutex> lock(_mtx);

        command_events.swap(_command_events);
        disconnect_events.swap(_disconnect_events);
    }

    return std::make_pair(std::move(command_events), std::move(disconnect_events));
}

EventLoop::UvAsyncUPtr EventLoop::_create_uv_async(AsyncCallback callback) {
    auto uv_async = std::unique_ptr<uv_async_t>(new uv_async_t);
    auto err = uv_async_init(_loop.get(), uv_async.get(), callback);
    if (err != 0) {
        throw Error("failed to initialize async: " + _err_msg(err));
    }

    uv_async->data = this;

    return uv_async;
}

EventLoop::LoopUPtr EventLoop::_create_event_loop() const {
    auto *loop = new uv_loop_t;
    auto err = uv_loop_init(loop);
    if (err != 0) {
        delete loop;
        throw Error("failed to initialize event loop: " + _err_msg(err));
    }

    return LoopUPtr(loop);
}

}

}
