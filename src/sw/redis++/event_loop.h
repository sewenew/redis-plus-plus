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

#ifndef SEWENEW_REDISPLUSPLUS_EVENT_LOOP_H
#define SEWENEW_REDISPLUSPLUS_EVENT_LOOP_H

#include <unordered_set>
#include <memory>
#include <uv.h>
#include "connection.h"
#include "async_event.h"

namespace sw {

namespace redis {

class AsyncConnection;

class EventLoop {
public:
    EventLoop();

    EventLoop(const EventLoop &) = delete;
    EventLoop& operator=(const EventLoop &) = delete;

    EventLoop(EventLoop &&that);

    EventLoop& operator=(EventLoop &&that);

    ~EventLoop();

    std::unique_ptr<AsyncConnection> watch(const ConnectionOptions &opts);

    void unwatch(std::unique_ptr<AsyncConnection> connection);

    void add(AsyncEventUPtr event);

private:
    static void _event_callback(uv_async_t *handle);

    static void _stop_callback(uv_async_t *handle);

    struct LoopDeleter {
        void operator()(uv_loop_t *loop) const;
    };

    using LoopUPtr = std::unique_ptr<uv_loop_t, LoopDeleter>;

    void _attach(redisAsyncContext *ctx);

    std::string _err_msg(int err) const {
        return uv_strerror(err);
    }

    LoopUPtr _create_event_loop() const;

    using UvAsyncUPtr = std::unique_ptr<uv_async_t>;

    using AsyncCallback = void (*)(uv_async_t*);

    UvAsyncUPtr _create_uv_async(AsyncCallback callback);

    void _notify();

    void _stop();

    void _connect(const std::vector<AsyncConnection*> &connections);

    void _disconnect(const std::vector<std::unique_ptr<AsyncConnection>> &connections,
            std::unordered_set<AsyncConnection*> &to_be_reconnect);

    std::unordered_set<AsyncConnection*> _send_commands(std::vector<AsyncEventUPtr> events);

    // We must define _event_async and _stop_async before _loop,
    // because these memory can only be release after _loop's deleter
    // has been called, i.e. the deleter will close these handles.
    UvAsyncUPtr _event_async;

    UvAsyncUPtr _stop_async;

    LoopUPtr _loop;

    std::thread _loop_thread;

    std::mutex _mtx;

    std::vector<AsyncConnection *> _connect_events;

    // These connections will be released.
    // TODO: possible leak
    std::vector<std::unique_ptr<AsyncConnection>> _disconnect_events;

    std::vector<AsyncEventUPtr> _command_events;
};

using EventLoopSPtr = std::shared_ptr<EventLoop>;

}

}

#endif // end SEWENEW_REDISPLUSPLUS_EVENT_LOOP_H
