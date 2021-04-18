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

#ifndef SEWENEW_REDISPLUSPLUS_ASYNC_EVENT_H
#define SEWENEW_REDISPLUSPLUS_ASYNC_EVENT_H

#include <cassert>
#include <hiredis/async.h>
#include "async_utils.h"
#include "errors.h"
#include "reply.h"

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

    ~FormattedCommand() {
        if (_data != nullptr) {
            redisFreeCommand(_data);
        }
    }

    const char* data() const noexcept {
        return _data;
    }

    std::size_t size() const noexcept {
        return _size;
    }

private:
    void _move(FormattedCommand &&that) noexcept {
        _data = that._data;
        _size = that._size;
        that._data = nullptr;
        that._size = 0;
    }

    char *_data = nullptr;
    std::size_t _size = 0;
};

class AsyncConnection;

class AsyncEvent {
public:
    explicit AsyncEvent(AsyncConnection *connection) : _connection(connection) {
        assert(_connection != nullptr);
    }

    virtual ~AsyncEvent() = default;

    virtual void handle() = 0;

    virtual void set_exception(std::exception_ptr p) = 0;

    AsyncConnection* connection() const noexcept {
        return _connection;
    }

private:
    AsyncConnection *_connection = nullptr;
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
    CommandEvent(AsyncConnection *connection,
            FormattedCommand cmd) : AsyncEvent(connection), _cmd(std::move(cmd)) {}

    Future<Result> get_future() {
        return _pro.get_future();
    }

    virtual void handle() override {
        auto *conn = connection();

        assert(conn != nullptr);

        // TODO: Should we let AsyncConnection's method do the work.
        auto *ctx = conn->context();
        assert(ctx != nullptr);

        // TODO: it seems that ctx has been destroyed.
        // in this case, we should not call throw_error with ctx.
        if (conn->broken()) {
            throw_error(ctx->c, "joke failed to send command");
        }

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
    static void _reply_callback(redisAsyncContext * /*ctx*/, void *r, void *privdata) {
        assert(privdata != nullptr);

        auto event = std::unique_ptr<CommandEvent<Result, ResultParser>>(
                static_cast<CommandEvent<Result, ResultParser> *>(privdata));

        try {
            redisReply *reply = static_cast<redisReply *>(r);
            if (reply == nullptr) {
                // __redisAsyncFree has been called.
                throw Error("connection has been freed, might failed to connect");
            }

            event->set_value(*reply);
        } catch (...) {
            event->set_exception(std::current_exception());
        }
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

template <typename Result>
using CommandEventUPtr = std::unique_ptr<CommandEvent<Result>>;

}

}

#endif // end SEWENEW_REDISPLUSPLUS_ASYNC_EVENT_H
