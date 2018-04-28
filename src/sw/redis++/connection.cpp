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
#include "reply.h"
#include "command.h"

namespace sw {

namespace redis {

Connection::Connection(redisContext *context) :
        _context(context),
        _last_active(std::chrono::steady_clock::now()) {
    if (!_context) {
        throw Error("CANNOT create connection with null context");
    }
}

void Connection::send(int argc, const char **argv, const std::size_t *argv_len) {
    assert(_context);

    if (redisAppendCommandArgv(_context.get(),
                                argc,
                                argv,
                                argv_len) != REDIS_OK) {
        throw_error(*_context, "Failed to send command");
    }

    assert(!broken());
}

auto Connection::CmdArgs::operator<<(const StringView &arg) -> CmdArgs& {
    _argv.push_back(arg.data());
    _argv_len.push_back(arg.size());

    return *this;
}

void Connection::send(CmdArgs &args) {
    assert(_context);

    if (redisAppendCommandArgv(_context.get(),
                                args.size(),
                                args.argv(),
                                args.argv_len()) != REDIS_OK) {
        throw_error(*_context, "Failed to send command");
    }

    assert(!broken());
}

std::string Connection::_server_info() const {
    assert(_context);

    auto connection_type = _context->connection_type;
    switch (connection_type) {
    case REDIS_CONN_TCP:
        return std::string(_context->tcp.host)
                    + ":" + std::to_string(_context->tcp.port);

    case REDIS_CONN_UNIX:
        return _context->unix_sock.path;

    default:
        throw Error("Unknown connection type: "
                + std::to_string(connection_type));
    }
}

Connection Connector::connect() const {
    auto connection = _connect();

    _set_socket_timeout(connection);

    _enable_keep_alive(connection);

    _auth(connection);

    _select_db(connection);

    return connection;
}

ReplyUPtr Connection::recv() {
    auto *ctx = context();

    assert(ctx != nullptr);

    void *r = nullptr;
    if (redisGetReply(ctx, &r) != REDIS_OK) {
        throw_error(*ctx, "Failed to get reply");
    }

    assert(!broken());

    auto reply = ReplyUPtr(static_cast<redisReply*>(r));

    if (reply::is_error(*reply)) {
        throw_error(*reply);
    }

    return reply;
}

Connection Connector::_connect() const {
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

    Connection connection(context);
    if (connection.broken()) {
        throw_error(*context, "Failed to connect to Redis");
    }

    return connection;
}

redisContext* Connector::_connect_tcp() const {
    if (_opts.connect_timeout > std::chrono::steady_clock::duration(0)) {
        return redisConnectWithTimeout(_opts.host.c_str(),
                    _opts.port,
                    _to_timeval(_opts.connect_timeout));
    } else {
        return redisConnect(_opts.host.c_str(), _opts.port);
        //return redisConnectNonBlock(_opts.host.c_str(), _opts.port);
    }
}

redisContext* Connector::_connect_unix() const {
    if (_opts.connect_timeout > std::chrono::steady_clock::duration(0)) {
        return redisConnectUnixWithTimeout(
                    _opts.path.c_str(),
                    _to_timeval(_opts.connect_timeout));
    } else {
        return redisConnectUnix(_opts.path.c_str());
    }
}

void Connector::_set_socket_timeout(Connection &connection) const {
    if (_opts.socket_timeout <= std::chrono::steady_clock::duration(0)) {
        return;
    }

    auto *context = connection.context();

    assert(context != nullptr);

    if (redisSetTimeout(context, _to_timeval(_opts.socket_timeout)) != REDIS_OK) {
        throw_error(*context, "Failed to set socket timeout");
    }
}

void Connector::_enable_keep_alive(Connection &connection) const {
    if (!_opts.keep_alive) {
        return;
    }

    auto *context = connection.context();

    assert(context != nullptr);

    if (redisEnableKeepAlive(context) != REDIS_OK) {
        throw_error(*context, "Failed to enable keep alive option");
    }
}

void Connector::_auth(Connection &connection) const {
    if (_opts.password.empty()) {
        return;
    }

    cmd::auth(connection, _opts.password);

    auto reply = connection.recv();

    reply::expect_ok_status(*reply);
}

void Connector::_select_db(Connection &connection) const {
    if (_opts.db == 0) {
        return;
    }

    cmd::select(connection, _opts.db);

    auto reply = connection.recv();

    reply::expect_ok_status(*reply);
}

timeval Connector::_to_timeval(const std::chrono::steady_clock::duration &dur) const {
    auto sec = std::chrono::duration_cast<std::chrono::seconds>(dur);
    auto msec = std::chrono::duration_cast<std::chrono::microseconds>(dur - sec);

    return {
            static_cast<std::time_t>(sec.count()),
            static_cast<suseconds_t>(msec.count())
    };
}

}

}
