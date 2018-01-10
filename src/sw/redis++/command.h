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

#ifndef SEWENEW_REDISPLUSPLUS_COMMAND_H
#define SEWENEW_REDISPLUSPLUS_COMMAND_H

#include <string>
#include "connection.h"
#include "utils.h"

namespace sw {

namespace redis {

namespace cmd {

enum class UpdateType {
    EXIST,
    NOT_EXIST,
    ALWAYS
};

inline void auth(Connection &connection, const StringView &password) {
    connection.send("AUTH %b", password.data(), password.size());
}

inline void info(Connection &connection) {
    connection.send("INFO");
}

inline void ping(Connection &connection) {
    connection.send("PING");
}

inline void ping(Connection &connection, const StringView &msg) {
    // If *msg* is empty, Redis returns am empty reply of REDIS_REPLY_STRING type.
    connection.send("PING %b", msg.data(), msg.size());
}

// STRING commands.

inline void append(Connection &connection, const StringView &key, const StringView &str) {
    connection.send("APPEND %b %b",
                    key.data(), key.size(),
                    str.data(), str.size());
}

inline void get(Connection &connection, const StringView &key) {
    connection.send("GET %b",
                    key.data(), key.size());
}

inline void getrange(Connection &connection,
                        const StringView &key,
                        long long start,
                        long long end) {
    connection.send("GETRANGE %b %lld %lld",
                    key.data(), key.size(),
                    start,
                    end);
}

inline void getset(Connection &connection,
                    const StringView &key,
                    const StringView &val) {
    connection.send("GETSET %b %b",
                    key.data(), key.size(),
                    val.data(), val.size());
}

inline void psetex(Connection &connection,
                    const StringView &key,
                    const StringView &val,
                    const std::chrono::milliseconds &ttl) {
    connection.send("PSETEX %b %lld %b",
                    key.data(), key.size(),
                    ttl.count(),
                    val.data(), val.size());
}

void set(Connection &connection,
            const StringView &key,
            const StringView &val,
            const std::chrono::milliseconds &ttl,
            UpdateType type);

inline void setnx(Connection &connection,
                    const StringView &key,
                    const StringView &val) {
    connection.send("SETNX %b %b",
                    key.data(), key.size(),
                    val.data(), val.size());
}

inline void setex(Connection &connection,
                    const StringView &key,
                    const StringView &val,
                    const std::chrono::seconds &ttl) {
    connection.send("SETEX %b %lld %b",
                    key.data(), key.size(),
                    ttl.count(),
                    val.data(), val.size());
}

inline void setrange(Connection &connection,
                        const StringView &key,
                        long long offset,
                        const StringView &val) {
    connection.send("SETRANGE %b %lld %b",
                    key.data(), key.size(),
                    offset,
                    val.data(), val.size());
}

inline void strlen(Connection &connection, const StringView &key) {
    connection.send("STRLEN %b", key.data(), key.size());
}

// LIST commands.

inline void lpop(Connection &connection, const StringView &key) {
    connection.send("LPOP %b",
                    key.data(), key.size());
}

inline void lpush(Connection &connection, const StringView &key, const StringView &val) {
    connection.send("LPUSH %b %b",
                    key.data(), key.size(),
                    val.data(), val.size());
}

template <typename Iter>
inline void lpush_range(Connection &connection,
                        const StringView &key,
                        Iter first,
                        Iter last) {
    Connection::CmdArgs args;
    args << "LPUSH" << key << std::make_pair(first, last);

    connection.send(args);
}

}

}

}

#endif // end SEWENEW_REDISPLUSPLUS_COMMAND_H
