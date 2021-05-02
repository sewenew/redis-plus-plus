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

namespace sw {

namespace redis {

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

AsyncConnection::AsyncConnection(const EventLoopSPtr &loop,
        const ConnectionOptions &opts) : _loop(loop), _opts(opts) {}

void AsyncConnection::disconnect() {
    if (_ctx != nullptr) {
        redisAsyncDisconnect(_ctx);

        reset();
    }
}

void AsyncConnection::reconnect() {
    try {
        auto ctx = _connect(_opts);

        assert(ctx && ctx->err == REDIS_OK);

        _loop->attach(*ctx);

        _ctx = ctx.release();
    } catch (...) {
        reset();
        throw;
    }
}

void AsyncConnection::_clean_async_context(void *data) {
    auto *async_context = static_cast<AsyncContext*>(data);
    assert(async_context != nullptr);

    delete async_context;
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

    ctx->data = new AsyncContext(shared_from_this());
    ctx->dataCleanup = _clean_async_context;

    return ctx;
}

}

}
