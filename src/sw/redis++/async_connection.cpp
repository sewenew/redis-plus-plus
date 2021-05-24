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

#include "async_connection.h"
#include <hiredis/async.h>
#include "errors.h"

namespace {

using namespace sw::redis;

void prepare_callback(redisAsyncContext *ctx, void *r, void *) {
    assert(ctx != nullptr);

    auto &connection = *(static_cast<AsyncConnectionSPtr *>(ctx->data));

    redisReply *reply = static_cast<redisReply *>(r);
    if (reply == nullptr) {
        // Connection has bee closed.
        //connection->reset();
        // TODO: not sure if we should set this to be DISCONNECTING
        return;
    }

    try {
        if (reply::is_error(*reply)) {
            throw_error(*reply);
        }

        reply::parse<void>(*reply);

        connection->prepare();
    } catch (const Error &e) {
        connection->disconnect(std::make_exception_ptr(e));
    }
}

}

namespace sw {

namespace redis {

FormattedCommand::FormattedCommand(char *data, int len) : _data(data), _size(len) {
    if (data == nullptr || len < 0) {
        throw Error("failed to format command");
    }
}

FormattedCommand::~FormattedCommand() noexcept {
    if (_data != nullptr) {
        redisFreeCommand(_data);
    }
}

void FormattedCommand::_move(FormattedCommand &&that) noexcept {
    _data = that._data;
    _size = that._size;
    that._data = nullptr;
    that._size = 0;
}

AsyncConnection::AsyncConnection(const ConnectionOptions &opts,
        EventLoop *loop) : _opts(opts), _loop(loop) {}

void AsyncConnection::prepare() {
    try {
        switch (_status) {
        case AsyncConnectionStatus::CONNECTED:
            if (_need_auth()) {
                _auth();
            } else if (_need_select_db()) {
                _select_db();
            } else {
                _status = AsyncConnectionStatus::READY;
            }

            break;

        case AsyncConnectionStatus::AUTH:
            if (_need_select_db()) {
                _select_db();
            } else {
                _status = AsyncConnectionStatus::READY;
            }

            break;

        case AsyncConnectionStatus::SELECT_DB:
            _status = AsyncConnectionStatus::READY;
            break;

        default:
            assert(false);
            break;
        }

        if (_status == AsyncConnectionStatus::READY) {
            // In case, there're pending commands.
            _loop->notify();
        }
    } catch (const Error &e) {
        disconnect(std::make_exception_ptr(e));
    }
}

std::exception_ptr AsyncConnection::error() {
    switch (_status) {
    case AsyncConnectionStatus::DISCONNECTED:
        return std::make_exception_ptr(Error("connection has been closed"));

    case AsyncConnectionStatus::UNINITIALIZED:
        return std::make_exception_ptr(Error("connection is uninitialized"));

    default:
        break;
    }

    return _err;
}

void AsyncConnection::reconnect() {
    auto ctx = _connect(_opts);

    assert(ctx && ctx->err == REDIS_OK);

    _loop->attach(*ctx);

    _ctx = ctx.release();

    _status = AsyncConnectionStatus::CONNECTING;
}

void AsyncConnection::disconnect(std::exception_ptr err) {
    // TODO: what if this method throw?
    _loop->unwatch(shared_from_this());

    _err = err;
    _status = AsyncConnectionStatus::DISCONNECTING;
}

bool AsyncConnection::_need_auth() const {
    return !_opts.password.empty() || _opts.user != "default";
}

void AsyncConnection::_auth() {
    assert(!broken());

    if (_opts.user == "default") {
        if (redisAsyncCommand(_ctx, prepare_callback, nullptr, "AUTH %b",
                    _opts.password.data(), _opts.password.size()) != REDIS_OK) {
            throw Error("failed to send auth command");
        }
    } else {
        // Redis 6.0 or latter
        if (redisAsyncCommand(_ctx, prepare_callback, nullptr, "AUTH %b %b",
                    _opts.user.data(), _opts.user.size(),
                    _opts.password.data(), _opts.password.size()) != REDIS_OK) {
            throw Error("failed to send auth command");
        }
    }

    _status = AsyncConnectionStatus::AUTH;
}

bool AsyncConnection::_need_select_db() const {
    return _opts.db != 0;
}

void AsyncConnection::_select_db() {
    assert(!broken());

    if (redisAsyncCommand(_ctx, prepare_callback, nullptr, "SELECT %lld",
            _opts.db) != REDIS_OK) {
        throw Error("failed to send select command");
    }

    _status = AsyncConnectionStatus::SELECT_DB;
}

void AsyncConnection::_clean_async_context(void *data) {
    auto *connection = static_cast<AsyncConnectionSPtr *>(data);

    assert(connection != nullptr);

    delete connection;
}

AsyncConnection::AsyncContextUPtr AsyncConnection::_connect(const ConnectionOptions &opts) {
    redisAsyncContext *context = nullptr;
    switch (opts.type) {
    case ConnectionType::TCP:
        context = redisAsyncConnect(opts.host.c_str(), _opts.port);
        break;

    case ConnectionType::UNIX:
        context = redisAsyncConnectUnix(opts.path.c_str());
        break;

    default:
        // Never goes here.
        throw Error("Unknown connection type");
    }

    if (context == nullptr) {
        throw Error("Failed to allocate memory for connection.");
    }

    auto ctx = AsyncContextUPtr(context);
    if (ctx->err != REDIS_OK) {
        throw_error(ctx->c, "failed to connect to server");
    }

    ctx->data = new AsyncConnectionSPtr(shared_from_this());
    ctx->dataCleanup = _clean_async_context;

    return ctx;
}

}

}
