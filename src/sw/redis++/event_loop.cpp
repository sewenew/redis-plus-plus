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
#include <iostream>

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

AsyncConnectionUPtr EventLoop::watch(const ConnectionOptions &opts) {
    auto connection = AsyncConnectionUPtr(new AsyncConnection(this, opts));

    {
        std::lock_guard<std::mutex> lock(_mtx);

        _connect_events.push_back(connection.get());
    }

    _notify();

    return connection;
}

void EventLoop::unwatch(AsyncConnectionUPtr connection) {
    assert(connection);

    {
        std::lock_guard<std::mutex> lock(_mtx);

        _disconnect_events.push_back(std::move(connection));
    }

    _notify();
}

void EventLoop::add(AsyncEventUPtr event) {
    assert(event);

    {
        std::lock_guard<std::mutex> lock(_mtx);

        _command_events.push_back(std::move(event));
    }

    _notify();
}

void EventLoop::_event_callback(uv_async_t *handle) {
    if (handle == nullptr) {
        // This should not happen
        return;
    }

    auto *event_loop = static_cast<EventLoop*>(handle->data);
    assert(event_loop != nullptr);

    std::vector<AsyncConnection *> connect_events;
    std::vector<AsyncConnectionUPtr> disconnect_events;
    std::vector<AsyncEventUPtr> command_events;
    {
        std::lock_guard<std::mutex> lock(event_loop->_mtx);

        connect_events.swap(event_loop->_connect_events);
        disconnect_events.swap(event_loop->_disconnect_events);
        command_events.swap(event_loop->_command_events);
    }

    event_loop->_connect(connect_events);

    auto to_be_reconnect = event_loop->_send_commands(std::move(command_events));

    event_loop->_disconnect(disconnect_events, to_be_reconnect);

    if (!to_be_reconnect.empty()) {
        std::lock_guard<std::mutex> lock(event_loop->_mtx);

        // TODO: do reconnection.
    }
}

void EventLoop::_stop_callback(uv_async_t *handle) {
    if (handle == nullptr) {
        // This should not happen
        return;
    }

    auto *event_loop = static_cast<EventLoop*>(handle->data);
    assert(event_loop != nullptr);

    uv_stop(event_loop->_loop.get());
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

void EventLoop::_connect(const std::vector<AsyncConnection *> &connections) {
    for (auto *connection : connections) {
        assert(connection != nullptr);

        auto *ctx = connection->context();

        assert(ctx != nullptr);

        _attach(ctx);
    }
}

void EventLoop::_disconnect(const std::vector<AsyncConnectionUPtr> &connections,
        std::unordered_set<AsyncConnection*> &to_be_reconnect) {
    for (auto &connection : connections) {
        assert(connection);

        auto iter = to_be_reconnect.find(connection.get());
        if (iter != to_be_reconnect.end()) {
            // No need to reconnect.
            to_be_reconnect.erase(iter);
        }

        connection->disconnect();
    }
}

std::unordered_set<AsyncConnection*> EventLoop::_send_commands(std::vector<AsyncEventUPtr> events) {
    std::unordered_set<AsyncConnection*> to_be_reconnect;
    for (auto &event : events) {
        assert(event);

        try {
            event->handle();

            // CommandEvent::_reply_callback will release the memory.
            event.release();
        } catch (...) {
            event->set_exception(std::current_exception());
            to_be_reconnect.insert(event->connection());
        }
    }

    return to_be_reconnect;
}

void EventLoop::_attach(redisAsyncContext *ctx) {
    redisLibuvAttach(ctx, _loop.get());
    // TODO: in connect callback, if connect failed, the underlying ctx will be deleted.
    // so if we get null reply, we just return without any throw error with ctx
    // also, we need a way to reconnect
    redisAsyncSetConnectCallback(ctx, [](const redisAsyncContext *, int status) {std::cout << "conn: " << status << std::endl;});
    redisAsyncSetDisconnectCallback(ctx, [](const redisAsyncContext *, int status) {std::cout << "dis: " << status << std::endl;});
}

void EventLoop::_notify() {
    assert(_event_async);

    uv_async_send(_event_async.get());
}

void EventLoop::_stop() {
    assert(_stop_async);

    uv_async_send(_stop_async.get());
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
