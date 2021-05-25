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
    FormattedCommand(char *data, int len);

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

    int size() const noexcept {
        return _size;
    }

private:
    void _move(FormattedCommand &&that) noexcept;

    char *_data = nullptr;
    int _size = 0;
};

template <typename Result>
struct DefaultResultParser {
    Result operator()(redisReply &reply) const {
        return reply::parse<Result>(reply);
    }
};

enum class AsyncConnectionStatus {
    UNINITIALIZED = 0,
    CONNECTING,
    CONNECT_FAILED,
    CONNECTED,
    AUTH,
    SELECT_DB,
    READY,
    DISCONNECTING,
    DISCONNECTED,
};

class AsyncConnection : public std::enable_shared_from_this<AsyncConnection> {
public:
    AsyncConnection(const ConnectionOptions &opts, EventLoop *loop);

    AsyncConnection(const AsyncConnection &) = delete;
    AsyncConnection& operator=(const AsyncConnection &) = delete;

    AsyncConnection(AsyncConnection &&) = delete;
    AsyncConnection& operator=(AsyncConnection &&) = delete;

    ~AsyncConnection() = default;

    AsyncConnectionStatus status() const noexcept {
        return _status;
    }

    void set_status(AsyncConnectionStatus status) noexcept {
        _status = status;
    }

    void prepare();

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

    void notify() {
        _loop->notify();
    }

    void reset() {
        _ctx = nullptr;
    }

    void disconnect(std::exception_ptr err);

    std::exception_ptr error();

    void set_error(std::exception_ptr err) {
        _err = err;
    }

    template <typename Result, typename ...Args>
    Future<Result> send(const char *format, Args &&...args);

    template <typename Result, typename ResultParser = DefaultResultParser<Result>>
    Future<Result> send(int argc, const char **argv, const std::size_t *argv_len);

    template <typename Result, typename ResultParser = DefaultResultParser<Result>>
    Future<Result> send(CmdArgs &args);

private:
    template <typename Result, typename ResultParser>
    Future<Result> _send(char *data, int len);

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

    bool _need_auth() const;

    void _auth();

    bool _need_select_db() const;

    void _select_db();

    ConnectionOptions _opts;

    EventLoop *_loop = nullptr;

    // _ctx will be release by EventLoop after attached.
    redisAsyncContext *_ctx = nullptr;

    // The time that the connection is created or the time that
    // the connection is used, i.e. *context()* is called.
    std::chrono::time_point<std::chrono::steady_clock> _last_active{};

    AsyncConnectionStatus _status = AsyncConnectionStatus::UNINITIALIZED;

    std::exception_ptr _err;
};

using AsyncConnectionSPtr = std::shared_ptr<AsyncConnection>;

class AsyncEvent {
public:
    explicit AsyncEvent(AsyncConnection *connection) : _connection(connection) {
        assert(_connection);
    }

    virtual ~AsyncEvent() = default;

    virtual void handle() = 0;

    virtual void set_exception(std::exception_ptr p) = 0;

    AsyncConnection* connection() {
        return _connection;
    }

private:
    AsyncConnection *_connection = nullptr;
};

using AsyncEventUPtr = std::unique_ptr<AsyncEvent>;

template <typename Result, typename ResultParser>
class CommandEvent : public AsyncEvent {
public:
    CommandEvent(AsyncConnection *connection, FormattedCommand cmd) :
        AsyncEvent(connection), _cmd(std::move(cmd)) {}

    Future<Result> get_future() {
        return _pro.get_future();
    }

    virtual void handle() override {
        auto *conn = connection();

        assert(!conn->broken());

        auto *ctx = conn->context();
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
                // TODO: should we set connection status?
                auto &connection = *(static_cast<AsyncConnectionSPtr *>(ctx->data));

                event->set_exception(connection->error());
            } else if (reply::is_error(*reply)) {
                try {
                    throw_error(*reply);
                } catch (const Error &e) {
                    event->set_exception(std::current_exception());
                }
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

    FormattedCommand _cmd;

    Promise<Result> _pro;
};

template <typename Result, typename ResultParser>
using CommandEventUPtr = std::unique_ptr<CommandEvent<Result, ResultParser>>;

template <typename Result, typename ...Args>
Future<Result> AsyncConnection::send(const char *format, Args &&...args) {
    char *data = nullptr;
    auto len = redisFormatCommand(&data, format, std::forward<Args>(args)...);

    return _send<Result, DefaultResultParser<Result>>(data, len);
}

template <typename Result, typename ResultParser>
Future<Result> AsyncConnection::send(int argc, const char **argv, const std::size_t *argv_len) {
    char *data = nullptr;
    auto len = redisFormatCommandArgv(&data, argc, argv, argv_len);

    return _send<Result, ResultParser>(data, len);
}

template <typename Result, typename ResultParser>
Future<Result> AsyncConnection::send(CmdArgs &args) {
    char *data = nullptr;
    auto len = redisFormatCommandArgv(&data, args.size(), args.argv(), args.argv_len());

    return _send<Result, ResultParser>(data, len);
}

template <typename Result, typename ResultParser>
Future<Result> AsyncConnection::_send(char *data, int len) {
    FormattedCommand cmd(data, len);

    auto event = CommandEventUPtr<Result, ResultParser>(
            new CommandEvent<Result, ResultParser>(this, std::move(cmd)));

    auto fut = event->get_future();

    _loop->add(AsyncEventUPtr(std::move(event)));

    return fut;
}

}

}

#endif // end SEWENEW_REDISPLUSPLUS_ASYNC_CONNECTION_H
