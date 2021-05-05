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
#include "event_loop.h"
#include "async_utils.h"

namespace sw {

namespace redis {

class FormattedCommand {
public:
    FormattedCommand(char *data, std::size_t len) : _data(data), _size(len) {}

    FormattedCommand(const FormattedCommand &) = delete;
    FormattedCommand& operator=(const FormattedCommand &) = delete;

    FormattedCommand(FormattedCommand &&that) noexcept {
        _move(std::move(that));
    }

    FormattedCommand& operator=(FormattedCommand &&that) noexcept {
        if (this != &that) {
            _move(std::move(that));
        }

        return *this;
    }

    ~FormattedCommand() noexcept;

    const char* data() const noexcept {
        return _data;
    }

    std::size_t size() const noexcept {
        return _size;
    }

private:
    void _move(FormattedCommand &&that) noexcept;

    char *_data = nullptr;
    std::size_t _size = 0;
};

class AsyncConnection : public std::enable_shared_from_this<AsyncConnection> {
public:
    AsyncConnection(const EventLoopSPtr &loop, const ConnectionOptions &opts);

    AsyncConnection(const AsyncConnection &) = delete;
    AsyncConnection& operator=(const AsyncConnection &) = delete;

    AsyncConnection(AsyncConnection &&) = delete;
    AsyncConnection& operator=(AsyncConnection &&) = delete;

    ~AsyncConnection();

    bool broken() const noexcept {
        return _ctx == nullptr || _ctx->err != REDIS_OK;
    }

    redisAsyncContext* context() {
        _last_active = std::chrono::steady_clock::now();

        return _ctx;
    }

    void reconnect();

    auto last_active() const
        -> std::chrono::time_point<std::chrono::steady_clock> {
        return _last_active;
    }

    void reset() noexcept {
        _ctx = nullptr;
    }

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

private:
    template <typename Result>
    Future<Result> _send(FormattedCommand cmd);

    static void _clean_async_context(void *data);

    struct AsyncContextDeleter {
        void operator()(redisAsyncContext *ctx) const {
            if (ctx != nullptr) {
                redisAsyncFree(ctx);
            }
        }
    };
    using AsyncContextUPtr = std::unique_ptr<redisAsyncContext, AsyncContextDeleter>;

    AsyncContextUPtr _connect(const ConnectionOptions &opts);

    EventLoopSPtr _loop;

    ConnectionOptions _opts;

    // _ctx will be release by EventLoop after attached.
    redisAsyncContext *_ctx = nullptr;

    // The time that the connection is created or the time that
    // the connection is used, i.e. *context()* is called.
    std::chrono::time_point<std::chrono::steady_clock> _last_active{};
};

using AsyncConnectionSPtr = std::shared_ptr<AsyncConnection>;

struct AsyncContext {
    explicit AsyncContext(const AsyncConnectionSPtr &conn) : connection(conn) {}

    std::exception_ptr err;

    AsyncConnectionSPtr connection;
};

class AsyncEvent {
public:
    virtual ~AsyncEvent() = default;

    virtual void handle() = 0;

    virtual void set_exception(std::exception_ptr p) = 0;
};

using AsyncEventUPtr = std::unique_ptr<AsyncEvent>;

template <typename Result>
class DefaultResultParser {
public:
    Result operator()(redisReply &reply) const {
        return reply::parse<Result>(reply);
    }
};

template <typename Result, typename ResultParser = DefaultResultParser<Result>>
class CommandEvent : public AsyncEvent {
public:
    CommandEvent(const AsyncConnectionSPtr &connection,
            FormattedCommand cmd) : _connection(connection), _cmd(std::move(cmd)) {
        assert(_connection);
    }

    Future<Result> get_future() {
        return _pro.get_future();
    }

    virtual void handle() override {
        if (_connection->broken()) {
            _connection->reconnect();
        }

        assert(!_connection->broken());

        auto *ctx = _connection->context();
        if (redisAsyncFormattedCommand(ctx,
                    _reply_callback, this, _cmd.data(), _cmd.size()) != REDIS_OK) {
            throw_error(ctx->c, "failed to send command");
        }
    }

    virtual void set_exception(std::exception_ptr p) override {
        _pro.set_exception(p);
    }

    template <typename T>
    struct ResultType {};

    void set_value(redisReply &reply) {
        _set_value(reply, ResultType<Result>{});
    }

private:
    static void _reply_callback(redisAsyncContext *ctx, void *r, void *privdata) {
        auto event = static_cast<CommandEvent<Result, ResultParser> *>(privdata);

        assert(ctx != nullptr && event != nullptr);

        try {
            redisReply *reply = static_cast<redisReply *>(r);
            if (reply == nullptr) {
                auto *async_context = static_cast<AsyncContext*>(ctx->data);
                assert(async_context != nullptr);

                event->set_exception(async_context->err);
            } else {
                event->set_value(*reply);
            }
        } catch (...) {
            event->set_exception(std::current_exception());
        }

        delete event;
    }

    template <typename T>
    void _set_value(redisReply &reply, ResultType<T>) {
        ResultParser parser;
        _pro.set_value(parser(reply));
    }

    void _set_value(redisReply &reply, ResultType<void>) {
        ResultParser parser;
        parser(reply);

        _pro.set_value();
    }

    AsyncConnectionSPtr _connection;

    FormattedCommand _cmd;

    Promise<Result> _pro;
};

template <typename Result>
using CommandEventUPtr = std::unique_ptr<CommandEvent<Result>>;

template <typename Result>
Future<Result> AsyncConnection::_send(FormattedCommand cmd) {
    auto event = CommandEventUPtr<Result>(new CommandEvent<Result>(shared_from_this(), std::move(cmd)));

    auto fut = event->get_future();

    _loop->add(AsyncEventUPtr(std::move(event)));

    return fut;
}

}

}

#endif // end SEWENEW_REDISPLUSPLUS_ASYNC_CONNECTION_H
