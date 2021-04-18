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

AsyncConnection::AsyncConnection(EventLoop *loop,
        const ConnectionOptions &opts) : _opts(opts), _loop(loop), _ctx(_connect(opts)) {
    assert(_ctx != nullptr);

    if (broken()) {
        throw_error(_ctx->c, "failed to connect to server");
    }
}

void AsyncConnection::disconnect() {
    if (_ctx != nullptr) {
        redisAsyncDisconnect(_ctx);
        _ctx = nullptr;
    }
}

redisAsyncContext* AsyncConnection::_connect(const ConnectionOptions &opts) const {
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

    return context;
}

}

}
