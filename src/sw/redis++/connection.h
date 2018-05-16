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

enum class ConnectionType {
    TCP = 0,
    UNIX
};

struct ConnectionOptions {
    ConnectionType type = ConnectionType::TCP;

    std::string host;

    int port = 6379;

    std::string path;

    std::string password;

    int db = 0;

    bool keep_alive = false;

    std::chrono::steady_clock::duration connect_timeout{0};

    std::chrono::steady_clock::duration socket_timeout{0};
};

class Connection {
public:
    explicit Connection(const ConnectionOptions &opts);

    Connection(const Connection &) = delete;
    Connection& operator=(const Connection &) = delete;

    Connection(Connection &&) = default;
    Connection& operator=(Connection &&) = default;

    ~Connection() = default;

    // Check if the connection is broken. Client needs to do this check
    // before sending some command to the connection. If it's broken,
    // client needs to reconnect it.
    bool broken() const noexcept {
        return _ctx->err != REDIS_OK;
    }

    void reconnect();

    auto last_active() const
        -> std::chrono::time_point<std::chrono::steady_clock> {
        return _last_active;
    }

    template <typename ...Args>
    void send(const char *format, Args &&...args);

    void send(int argc, const char **argv, const std::size_t *argv_len);

    // TODO: move CmdArgs to cmd namespace
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

    friend void swap(Connection &lhs, Connection &rhs) noexcept;

private:
    class Connector;

    struct ContextDeleter {
        void operator()(redisContext *context) const {
            if (context != nullptr) {
                redisFree(context);
            }
        };
    };

    using ContextUPtr = std::unique_ptr<redisContext, ContextDeleter>;

    void _set_options();

    void _auth();

    void _select_db();

    std::string _server_info() const;

    redisContext* _context();

    ContextUPtr _ctx;

    // The time that the connection is created or the time that
    // the connection is used, i.e. *context()* is called.
    std::chrono::time_point<std::chrono::steady_clock> _last_active{};

    ConnectionOptions _opts;
};

// Inline implementaions.

template <typename ...Args>
inline void Connection::send(const char *format, Args &&...args) {
    auto ctx = _context();

    assert(ctx != nullptr);

    if (redisAppendCommand(ctx,
                format,
                std::forward<Args>(args)...) != REDIS_OK) {
        throw_error(*ctx, "Failed to send command");
    }

    assert(!broken());
}

inline redisContext* Connection::_context() {
    _last_active = std::chrono::steady_clock::now();

    return _ctx.get();
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
