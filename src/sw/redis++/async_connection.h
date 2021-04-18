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

#ifndef SEWENEW_REDISPLUSPLUS_ASYNC_CONNECTION_H
#define SEWENEW_REDISPLUSPLUS_ASYNC_CONNECTION_H

#include <cassert>
#include <memory>
#include <hiredis/async.h>
#include "connection.h"
#include "command_args.h"
#include "async_event.h"
#include "event_loop.h"

namespace sw {

namespace redis {

class AsyncConnection {
public:
    AsyncConnection(EventLoop *loop, const ConnectionOptions &opts);

    AsyncConnection(const AsyncConnection &) = delete;
    AsyncConnection& operator=(const AsyncConnection &) = delete;

    AsyncConnection(AsyncConnection &&) = delete;
    AsyncConnection& operator=(AsyncConnection &&) = delete;

    ~AsyncConnection() {
        disconnect();
    }

    bool broken() const noexcept {
        return _ctx->err != REDIS_OK;
    }

    redisAsyncContext* context() const {
        return _ctx;
    }

    void reconnect();

    template <typename Result, typename ...Args>
    void send(const char *format, Args &&...args);

    template <typename Result>
    Future<Result> send(int argc, const char **argv, const std::size_t *argv_len);

    template <typename Result>
    Future<Result> send(CmdArgs &args) {
        char *data = nullptr;
        auto len = redisFormatCommandArgv(&data, args.size(), args.argv(), args.argv_len());
        if (len < 0) {
            throw Error("failed to format command");
        }

        return _send<Result>(FormattedCommand(data, len));
    }

    // TODO: can we call it in destructor, is there any race condition?
    void disconnect();

private:
    template <typename Result>
    Future<Result> _send(FormattedCommand cmd) {
        auto event = CommandEventUPtr<Result>(new CommandEvent<Result>(this, std::move(cmd)));

        auto fut = event->get_future();

        _loop->add(AsyncEventUPtr(std::move(event)));

        return fut;
    }

    redisAsyncContext* _connect(const ConnectionOptions &opts) const;

    ConnectionOptions _opts;

    EventLoop *_loop;

    // _ctx will be released in event loop's callback.
    redisAsyncContext *_ctx = nullptr;
};

using AsyncConnectionUPtr = std::unique_ptr<AsyncConnection>;

}

}

#endif // end SEWENEW_REDISPLUSPLUS_ASYNC_CONNECTION_H
