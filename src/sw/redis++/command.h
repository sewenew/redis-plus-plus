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

enum class InsertPosition {
    BEFORE,
    AFTER
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

inline void lindex(Connection &connection, const StringView &key, long long index) {
    connection.send("LINDEX %b %lld",
                    key.data(), key.size(),
                    index);
}

void linsert(Connection &connection,
                const StringView &key,
                const StringView &val,
                InsertPosition position,
                const StringView &pivot);

inline void llen(Connection &connection,
                    const StringView &key) {
    connection.send("LLEN %b", key.data(), key.size());
}

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

inline void lpushx(Connection &connection, const StringView &key, const StringView &val) {
    connection.send("LPUSHX %b %b",
                    key.data(), key.size(),
                    val.data(), val.size());
}

inline void lrange(Connection &connection,
                    const StringView &key,
                    long long start,
                    long long stop) {
    connection.send("LRANGE %b %lld %lld",
                    key.data(), key.size(),
                    start,
                    stop);
}

inline void lrem(Connection &connection,
                    const StringView &key,
                    const StringView &val,
                    long long count) {
    connection.send("LREM %b %lld %b",
                    key.data(), key.size(),
                    count,
                    val.data(), val.size());
}

inline void lset(Connection &connection,
                    const StringView &key,
                    long long index,
                    const StringView &val) {
    connection.send("LSET %b %lld %b",
                    key.data(), key.size(),
                    index,
                    val.data(), val.size());
}

inline void ltrim(Connection &connection,
                    const StringView &key,
                    long long start,
                    long long stop) {
    connection.send("LTRIM %b %lld %lld",
                    key.data(), key.size(),
                    start,
                    stop);
}

inline void rpop(Connection &connection, const StringView &key) {
    connection.send("RPOP %b", key.data(), key.size());
}

inline void rpush(Connection &connection, const StringView &key, const StringView &val) {
    connection.send("RPUSH %b %b",
                    key.data(), key.size(),
                    val.data(), val.size());
}

template <typename Iter>
inline void rpush_range(Connection &connection,
                        const StringView &key,
                        Iter first,
                        Iter last) {
    Connection::CmdArgs args;
    args << "RPUSH" << key << std::make_pair(first, last);

    connection.send(args);
}

inline void rpushx(Connection &connection, const StringView &key, const StringView &val) {
    connection.send("RPUSHX %b %b",
                    key.data(), key.size(),
                    val.data(), val.size());
}

// HASH commands.

inline void hdel(Connection &connection, const StringView &key, const StringView &field) {
    connection.send("HDEL %b %b",
                    key.data(), key.size(),
                    field.data(), field.size());
}

template <typename Iter>
inline void hdel_range(Connection &connection,
                        const StringView &key,
                        Iter first,
                        Iter last) {
    Connection::CmdArgs args;
    args << "HDEL" << key << std::make_pair(first, last);

    connection.send(args);
}

inline void hexists(Connection &connection, const StringView &key, const StringView &field) {
    connection.send("HEXISTS %b %b",
                    key.data(), key.size(),
                    field.data(), field.size());
}

inline void hget(Connection &connection, const StringView &key, const StringView &field) {
    connection.send("HGET %b %b",
                    key.data(), key.size(),
                    field.data(), field.size());
}

inline void hgetall(Connection &connection, const StringView &key) {
    connection.send("HGETALL %b", key.data(), key.size());
}

inline void hkeys(Connection &connection, const StringView &key) {
    connection.send("HKEYS %b", key.data(), key.size());
}

inline void hlen(Connection &connection, const StringView &key) {
    connection.send("HLEN %b", key.data(), key.size());
}

template <typename Iter>
inline void hmget(Connection &connection,
                    const StringView &key,
                    Iter first,
                    Iter last) {
    Connection::CmdArgs args;
    args << "HMGET" << key << std::make_pair(first, last);

    connection.send(args);
}

template <typename Iter>
inline void hmset(Connection &connection,
                    const StringView &key,
                    Iter first,
                    Iter last) {
    Connection::CmdArgs args;
    args << "HMSET" << key << std::make_pair(first, last);

    connection.send(args);
}

inline void hset(Connection &connection,
                    const StringView &key,
                    const StringView &field,
                    const StringView &val) {
    connection.send("HSET %b %b %b",
                    key.data(), key.size(),
                    field.data(), field.size(),
                    val.data(), val.size());
}

inline void hsetnx(Connection &connection,
                    const StringView &key,
                    const StringView &field,
                    const StringView &val) {
    connection.send("HSETNX %b %b %b",
                    key.data(), key.size(),
                    field.data(), field.size(),
                    val.data(), val.size());
}

inline void hstrlen(Connection &connection,
                    const StringView &key,
                    const StringView &field) {
    connection.send("HSTRLEN %b %b",
                    key.data(), key.size(),
                    field.data(), field.size());
}

inline void hvals(Connection &connection, const StringView &key) {
    connection.send("HVALS %b", key.data(), key.size());
}

// SET commands

inline void sadd(Connection &connection,
                    const StringView &key,
                    const StringView &member) {
    connection.send("SADD %b %b",
                    key.data(), key.size(),
                    member.data(), member.size());
}

template <typename Iter>
inline void sadd_range(Connection &connection,
                        const StringView &key,
                        Iter first,
                        Iter last) {
    Connection::CmdArgs args;
    args << "SADD" << key << std::make_pair(first, last);

    connection.send(args);
}

inline void scard(Connection &connection, const StringView &key) {
    connection.send("SCARD %b", key.data(), key.size());
}

inline void sismember(Connection &connection,
                        const StringView &key,
                        const StringView &member) {
    connection.send("SISMEMBER %b %b",
                    key.data(), key.size(),
                    member.data(), member.size());
}

inline void smembers(Connection &connection, const StringView &key) {
    connection.send("SMEMBERS %b", key.data(), key.size());
}

inline void spop(Connection &connection, const StringView &key) {
    connection.send("SPOP %b", key.data(), key.size());
}

inline void spop_range(Connection &connection, const StringView &key, long long count) {
    connection.send("SPOP %b %lld",
                    key.data(), key.size(),
                    count);
}

inline void srandmember(Connection &connection, const StringView &key) {
    connection.send("SRANDMEMBER %b", key.data(), key.size());
}

inline void srandmember_range(Connection &connection,
                                const StringView &key,
                                long long count) {
    connection.send("SRANDMEMBER %b %lld",
                    key.data(), key.size(),
                    count);
}

inline void srem(Connection &connection,
                    const StringView &key,
                    const StringView &member) {
    connection.send("SREM %b %b",
                    key.data(), key.size(),
                    member.data(), member.size());
}

template <typename Iter>
inline void srem_range(Connection &connection,
                    const StringView &key,
                    Iter first,
                    Iter last) {
    Connection::CmdArgs args;
    args << "SREM" << key << std::make_pair(first, last);

    connection.send(args);
}

}

}

}

#endif // end SEWENEW_REDISPLUSPLUS_COMMAND_H
