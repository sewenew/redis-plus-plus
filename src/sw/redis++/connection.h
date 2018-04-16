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

#ifndef SEWENEW_REDISPLUSPLUS_CONNECTION_H
#define SEWENEW_REDISPLUSPLUS_CONNECTION_H

#include <cerrno>
#include <cstring>
#include <memory>
#include <vector>
#include <list>
#include <string>
#include <sstream>
#include <chrono>
#include <hiredis/hiredis.h>
#include "errors.h"
#include "reply.h"
#include "utils.h"

namespace sw {

namespace redis {

class Connection {
public:
    explicit Connection(redisContext *context);

    Connection(const Connection &) = delete;
    Connection& operator=(const Connection &) = delete;

    Connection(Connection &&) = default;
    Connection& operator=(Connection &&) = default;

    ~Connection() = default;

    // Check if the connection is broken. Client needs to do this check
    // before sending some command to the connection. If it's broken,
    // client needs to call *reconnect()*.
    bool broken() const noexcept {
        return _context->err != REDIS_OK;
    }

    void reconnect();

    redisContext* context() {
        _last_active = std::chrono::steady_clock::now();

        return _context.get();
    }

    auto last_active() const
        -> std::chrono::time_point<std::chrono::steady_clock> {
        return _last_active;
    }

    template <typename ...Args>
    void send(const char *format, Args &&...args);

    void send(int argc, const char **argv, const std::size_t *argv_len);

    class CmdArgs {
    public:
        CmdArgs& operator<<(const StringView &arg);

        template <typename T,
                     typename std::enable_if<std::is_integral<T>::value
                                                || std::is_floating_point<T>::value,
                                            int>::type = 0>
        CmdArgs& operator<<(T arg) {
            _numbers.push_back(std::to_string(arg));
            return operator<<(_numbers.back());
        }

        template <typename Iter>
        CmdArgs& operator<<(const std::pair<Iter, Iter> &range);

        template <std::size_t N, typename ...Args>
        auto operator<<(const std::tuple<Args...> &) ->
            typename std::enable_if<N == sizeof...(Args), CmdArgs&>::type {
            return *this;
        }

        template <std::size_t N = 0, typename ...Args>
        auto operator<<(const std::tuple<Args...> &arg) ->
            typename std::enable_if<N < sizeof...(Args), CmdArgs&>::type;

        const char** argv() {
            return _argv.data();
        }

        const std::size_t* argv_len() {
            return _argv_len.data();
        }

        std::size_t size() const {
            return _argv.size();
        }

    private:
        template <typename Iter>
        CmdArgs& _append(std::true_type, const std::pair<Iter, Iter> &range);

        template <typename Iter>
        CmdArgs& _append(std::false_type, const std::pair<Iter, Iter> &range);

        std::vector<const char *> _argv;
        std::vector<std::size_t> _argv_len;

        std::list<std::string> _numbers;
    };

    void send(CmdArgs &args);

    ReplyUPtr recv();

private:
    std::string _server_info() const;

    struct ContextDeleter {
        void operator()(redisContext *context) const {
            if (context != nullptr) {
                redisFree(context);
            }
        };
    };

    using ContextUPtr = std::unique_ptr<redisContext, ContextDeleter>;

    ContextUPtr _context;

    // The time that the connection is created or the time that
    // the connection is used, i.e. *context()* is called.
    std::chrono::time_point<std::chrono::steady_clock> _last_active{};
};

enum class ConnectionType {
    TCP = 0,
    UNIX
};

struct ConnectionOptions {
    ConnectionType type = ConnectionType::TCP;

    std::string host = "127.0.0.1";

    int port = 6379;

    std::string path;

    std::string password;

    // TODO: implement async interfaces.
    bool async = false;

    bool keep_alive = false;

    std::chrono::steady_clock::duration connect_timeout{0};

    std::chrono::steady_clock::duration socket_timeout{0};
};

class Connector {
public:
    explicit Connector(const ConnectionOptions &opts) : _opts(opts) {}

    Connection connect() const;

private:
    Connection _connect() const;

    redisContext* _connect_tcp() const;

    redisContext* _connect_unix() const;

    void _set_socket_timeout(Connection &connection) const;

    void _enable_keep_alive(Connection &connection) const;

    void _auth(Connection &connection) const;

    timeval _to_timeval(const std::chrono::steady_clock::duration &dur) const;

    ConnectionOptions _opts;
};

// Inline implementaions.

template <typename ...Args>
inline void Connection::send(const char *format, Args &&...args) {
    assert(_context);

    if (redisAppendCommand(_context.get(),
                format,
                std::forward<Args>(args)...) != REDIS_OK) {
        throw_error(*_context, "Failed to send command");
    }

    assert(!broken());
}

template <typename Iter>
auto Connection::CmdArgs::operator<<(const std::pair<Iter, Iter> &range) -> CmdArgs& {
    return _append(IsKvPair<typename std::decay<decltype(*std::declval<Iter>())>::type>(), range);
}

template <std::size_t N, typename ...Args>
auto Connection::CmdArgs::operator<<(const std::tuple<Args...> &arg) ->
    typename std::enable_if<N < sizeof...(Args), CmdArgs&>::type {
    operator<<(std::get<N>(arg));

    return operator<<<N + 1, Args...>(arg);
}

template <typename Iter>
auto Connection::CmdArgs::_append(std::false_type,
                                    const std::pair<Iter, Iter> &range) -> CmdArgs& {
    auto first = range.first;
    auto last = range.second;
    while (first != last) {
        *this << *first;
        ++first;
    }

    return *this;
}

template <typename Iter>
auto Connection::CmdArgs::_append(std::true_type,
                                    const std::pair<Iter, Iter> &range) -> CmdArgs& {
    auto first = range.first;
    auto last = range.second;
    while (first != last) {
        *this << first->first << first->second;
        ++first;
    }

    return *this;
}

}

}

#endif // end SEWENEW_REDISPLUSPLUS_CONNECTION_H
