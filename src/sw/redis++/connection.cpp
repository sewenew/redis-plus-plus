/**************************************************************************
   Copyright (c) 2017 sewenew

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

#include "sw/redis++/connection.h"
#include <cassert>
#include <tuple>
#include <algorithm>
#include "sw/redis++/reply.h"
#include "sw/redis++/command.h"
#include "sw/redis++/command_args.h"

#ifdef _MSC_VER

#include <winsock2.h>   // for `timeval` with MSVC compiler

#endif

namespace sw {

namespace redis {

class Connection::Connector {
public:
    explicit Connector(const ConnectionOptions &opts);

    ContextUPtr connect() const;

private:
    ContextUPtr _connect() const;

    redisContext* _connect_tcp() const;

    redisContext* _connect_unix() const;

    void _set_socket_timeout(redisContext &ctx) const;

    void _enable_keep_alive(redisContext &ctx) const;

    timeval _to_timeval(const std::chrono::milliseconds &dur) const;

    const ConnectionOptions &_opts;
};

Connection::Connector::Connector(const ConnectionOptions &opts) : _opts(opts) {}

Connection::ContextUPtr Connection::Connector::connect() const {
    auto ctx = _connect();

    assert(ctx);

    if (ctx->err != REDIS_OK) {
        throw_error(*ctx, "failed to connect to Redis (" + _opts._server_info() + ")");
    }

    _set_socket_timeout(*ctx);

    _enable_keep_alive(*ctx);

    return ctx;
}

Connection::ContextUPtr Connection::Connector::_connect() const {
    redisContext *context = nullptr;
    switch (_opts.type) {
    case ConnectionType::TCP:
        context = _connect_tcp();
        break;

    case ConnectionType::UNIX:
        context = _connect_unix();
        break;

    default:
        // Never goes here.
        throw Error("Unknown connection type");
    }

    if (context == nullptr) {
        throw Error("Failed to allocate memory for connection.");
    }

    return ContextUPtr(context);
}

redisContext* Connection::Connector::_connect_tcp() const {
    if (_opts.connect_timeout > std::chrono::milliseconds(0)) {
        return redisConnectWithTimeout(_opts.host.c_str(),
                    _opts.port,
                    _to_timeval(_opts.connect_timeout));
    } else {
        return redisConnect(_opts.host.c_str(), _opts.port);
    }
}

redisContext* Connection::Connector::_connect_unix() const {
    if (_opts.connect_timeout > std::chrono::milliseconds(0)) {
        return redisConnectUnixWithTimeout(
                    _opts.path.c_str(),
                    _to_timeval(_opts.connect_timeout));
    } else {
        return redisConnectUnix(_opts.path.c_str());
    }
}

void Connection::Connector::_set_socket_timeout(redisContext &ctx) const {
    if (_opts.socket_timeout <= std::chrono::milliseconds(0)) {
        return;
    }

    if (redisSetTimeout(&ctx, _to_timeval(_opts.socket_timeout)) != REDIS_OK) {
        throw_error(ctx, "Failed to set socket timeout");
    }
}

void Connection::Connector::_enable_keep_alive(redisContext &ctx) const {
#ifdef REDIS_PLUS_PLUS_HAS_redisEnableKeepAliveWithInterval
    if (_opts.keep_alive_s > std::chrono::seconds{0}) {
        if (redisEnableKeepAliveWithInterval(&ctx, static_cast<int>(_opts.keep_alive_s.count())) != REDIS_OK) {
            throw_error(ctx, "Failed to enable keep alive option");
        }

        return;
    }
#endif // end REDIS_PLUS_PLUS_HAS_redisEnableKeepAliveWithInterval

    if (!_opts.keep_alive) {
        return;
    }

    if (redisEnableKeepAlive(&ctx) != REDIS_OK) {
        throw_error(ctx, "Failed to enable keep alive option");
    }
}

timeval Connection::Connector::_to_timeval(const std::chrono::milliseconds &dur) const {
    auto sec = std::chrono::duration_cast<std::chrono::seconds>(dur);
    auto msec = std::chrono::duration_cast<std::chrono::microseconds>(dur - sec);

    timeval t;
    t.tv_sec = sec.count();
    t.tv_usec = msec.count();
    return t;
}

std::string ConnectionOptions::_server_info() const {
    std::string info;
    switch (type) {
    case ConnectionType::TCP:
        info = host + ":" + std::to_string(port);
        break;

    case ConnectionType::UNIX:
        info = path;
        break;

    default:
        // Never goes here.
        throw Error("unknown connection type");
    }

    return info;
}

void swap(Connection &lhs, Connection &rhs) noexcept {
    std::swap(lhs._ctx, rhs._ctx);
    std::swap(lhs._create_time, rhs._create_time);
    std::swap(lhs._opts, rhs._opts);
}

Connection::Connection(const ConnectionOptions &opts) :
            _ctx(Connector(opts).connect()),
            _create_time(std::chrono::steady_clock::now()),
            _last_active(std::chrono::steady_clock::now()),
            _opts(opts) {
    assert(_ctx && !broken());

    const auto &tls_opts = opts.tls;
    // If not compiled with TLS, TLS is always disabled.
    if (tls::enabled(tls_opts)) {
        _tls_ctx = tls::secure_connection(*_ctx, tls_opts);
    }

    _set_options();
}

void Connection::reconnect() {
    Connection connection(_opts);

    swap(*this, connection);
}

void Connection::send(int argc, const char **argv, const std::size_t *argv_len) {
    auto ctx = _context();

    assert(ctx != nullptr);

    if (redisAppendCommandArgv(ctx,
                                argc,
                                argv,
                                argv_len) != REDIS_OK) {
        throw_error(*ctx, "Failed to send command");
    }

    assert(!broken());
}

void Connection::send(CmdArgs &args) {
    auto ctx = _context();

    assert(ctx != nullptr);

    if (redisAppendCommandArgv(ctx,
                                static_cast<int>(args.size()),
                                args.argv(),
                                args.argv_len()) != REDIS_OK) {
        throw_error(*ctx, "Failed to send command");
    }

    assert(!broken());
}

ReplyUPtr Connection::recv(bool handle_error_reply) {
    auto *ctx = _context();

    assert(ctx != nullptr);

    void *r = nullptr;
    if (redisGetReply(ctx, &r) != REDIS_OK) {
        throw_error(*ctx, "Failed to get reply");
    }

    assert(!broken() && r != nullptr);

    auto reply = ReplyUPtr(static_cast<redisReply*>(r));

    if (handle_error_reply && reply::is_error(*reply)) {
        throw_error(*reply);
    }

    return reply;
}

#ifdef REDIS_PLUS_PLUS_RESP_VERSION_3

void Connection::set_push_callback(redisPushFn *push_func) {
    assert(!broken());

    redisSetPushCallback(_context(), push_func);
}

#endif

void Connection::_set_options() {
    _auth();

    if (_opts.resp > 2) {
        _set_resp_version();
    }

    _set_name();

    _select_db();

    if (_opts.readonly) {
        _enable_readonly();
    }
}

void Connection::_enable_readonly() {
    send("READONLY");

    auto reply = recv();

    assert(reply);

    reply::parse<void>(*reply);
}

void Connection::_set_resp_version() {
    cmd::hello(*this, _opts.resp);

    auto reply = recv();

    assert(reply);

    // TODO: parse hello reply.
}

void Connection::_auth() {
    const std::string DEFAULT_USER = "default";

    if (_opts.user == DEFAULT_USER && _opts.password.empty()) {
        return;
    }

    if (_opts.user == DEFAULT_USER) {
        cmd::auth(*this, _opts.password);
    } else {
        // Redis 6.0 or latter
        cmd::auth(*this, _opts.user, _opts.password);
    }

    auto reply = recv();

    assert(reply);

    reply::parse<void>(*reply);
}

void Connection::_set_name() {
    if (_opts.name.empty()) {
        return;
    }

    cmd::client_setname(*this, _opts.name);

    auto reply = recv();

    assert(reply);

    reply::parse<void>(*reply);
}

void Connection::_select_db() {
    if (_opts.db == 0) {
        return;
    }

    cmd::select(*this, _opts.db);

    auto reply = recv();

    assert(reply);

    reply::parse<void>(*reply);
}

}

}
