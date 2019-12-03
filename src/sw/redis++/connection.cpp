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

#include "connection.h"
#include <cassert>
#include <tuple>
#include "reply.h"
#include "command.h"
#include "command_args.h"

namespace sw {

namespace redis {

ConnectionOptions::ConnectionOptions(const std::string &uri) :
                                        ConnectionOptions(_parse_uri(uri)) {}

ConnectionOptions ConnectionOptions::_parse_uri(const std::string &uri) const {
    std::string type;
    std::string auth;
    std::string path;
    std::tie(type, auth, path) = _split_uri(uri);

    ConnectionOptions opts;

    _set_auth_opts(auth, opts);

    auto db = 0;
    std::tie(path, db) = _split_path(path);

    opts.db = db;

    if (type == "tcp") {
        _set_tcp_opts(path, opts);
    } else if (type == "unix") {
        _set_unix_opts(path, opts);
    } else {
        throw Error("invalid URI: invalid type");
    }

    return opts;
}

auto ConnectionOptions::_split_uri(const std::string &uri) const
    -> std::tuple<std::string, std::string, std::string> {
    auto pos = uri.find("://");
    if (pos == std::string::npos) {
        throw Error("invalid URI: no scheme");
    }

    auto type = uri.substr(0, pos);

    auto start = pos + 3;
    pos = uri.find("@", start);
    if (pos == std::string::npos) {
        // No auth info.
        return std::make_tuple(type, std::string{}, uri.substr(start));
    }

    auto auth = uri.substr(start, pos - start);

    return std::make_tuple(type, auth, uri.substr(pos + 1));
}

auto ConnectionOptions::_split_path(const std::string &path) const
    -> std::tuple<std::string, int> {
    auto pos = path.rfind("/");
    if (pos != std::string::npos) {
        // Might specified a db number.
        try {
            auto db = std::stoi(path.substr(pos + 1));

            return std::make_tuple(path.substr(0, pos), db);
        } catch (const std::exception &) {
            // Not a db number, and it might be a path to unix domain socket.
        }
    }

    // No db number specified, and use default one, i.e. 0.
    return std::make_tuple(path, 0);
}

void ConnectionOptions::_set_auth_opts(const std::string &auth, ConnectionOptions &opts) const {
    if (auth.empty()) {
        // No auth info.
        return;
    }

    auto pos = auth.find(":");
    if (pos == std::string::npos) {
        // No user name.
        opts.password = auth;
    } else {
        opts.user = auth.substr(0, pos);
        opts.password = auth.substr(pos + 1);
    }
}

void ConnectionOptions::_set_tcp_opts(const std::string &path, ConnectionOptions &opts) const {
    opts.type = ConnectionType::TCP;

    auto pos = path.find(":");
    if (pos != std::string::npos) {
        // Port number specified.
        try {
            opts.port = std::stoi(path.substr(pos + 1));
        } catch (const std::exception &) {
            throw Error("invalid URI: invalid port");
        }
    } // else use default port, i.e. 6379.

    opts.host = path.substr(0, pos);
}

void ConnectionOptions::_set_unix_opts(const std::string &path, ConnectionOptions &opts) const {
    opts.type = ConnectionType::UNIX;
    opts.path = path;
}

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
        throw_error(*ctx, "Failed to connect to Redis");
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
        throw Error("Unkonw connection type");
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

    return {
            static_cast<std::time_t>(sec.count()),
            static_cast<suseconds_t>(msec.count())
    };
}

void swap(Connection &lhs, Connection &rhs) noexcept {
    std::swap(lhs._ctx, rhs._ctx);
    std::swap(lhs._last_active, rhs._last_active);
    std::swap(lhs._opts, rhs._opts);
}

Connection::Connection(const ConnectionOptions &opts) :
            _ctx(Connector(opts).connect()),
            _last_active(std::chrono::steady_clock::now()),
            _opts(opts) {
    assert(_ctx && !broken());

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
                                args.size(),
                                args.argv(),
                                args.argv_len()) != REDIS_OK) {
        throw_error(*ctx, "Failed to send command");
    }

    assert(!broken());
}

ReplyUPtr Connection::recv() {
    auto *ctx = _context();

    assert(ctx != nullptr);

    void *r = nullptr;
    if (redisGetReply(ctx, &r) != REDIS_OK) {
        throw_error(*ctx, "Failed to get reply");
    }

    assert(!broken() && r != nullptr);

    auto reply = ReplyUPtr(static_cast<redisReply*>(r));

    if (reply::is_error(*reply)) {
        throw_error(*reply);
    }

    return reply;
}

void Connection::_set_options() {
    _auth();

    _select_db();
}

void Connection::_auth() {
    if (_opts.password.empty()) {
        return;
    }

    cmd::auth(*this, _opts.password);

    auto reply = recv();

    reply::parse<void>(*reply);
}

void Connection::_select_db() {
    if (_opts.db == 0) {
        return;
    }

    cmd::select(*this, _opts.db);

    auto reply = recv();

    reply::parse<void>(*reply);
}

}

}
