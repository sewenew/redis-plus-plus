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

#include <ctime>
#include <string>
#include <chrono>
#include "connection.h"
#include "command_options.h"
#include "utils.h"

namespace sw {

namespace redis {

namespace cmd {

// CONNECTION command.
inline void auth(Connection &connection, const StringView &password) {
    connection.send("AUTH %b", password.data(), password.size());
}

inline void echo(Connection &connection, const StringView &msg) {
    connection.send("ECHO %b", msg.data(), msg.size());
}

inline void ping(Connection &connection) {
    connection.send("PING");
}

inline void quit(Connection &connection) {
    connection.send("QUIT");
}

inline void ping(Connection &connection, const StringView &msg) {
    // If *msg* is empty, Redis returns am empty reply of REDIS_REPLY_STRING type.
    connection.send("PING %b", msg.data(), msg.size());
}

inline void select(Connection &connection, long long idx) {
    connection.send("SELECT %lld", idx);
}

inline void swapdb(Connection &connection, long long idx1, long long idx2) {
    connection.send("SWAPDB %lld %lld", idx1, idx2);
}

// SERVER commands.

inline void bgrewriteaof(Connection &connection) {
    connection.send("BGREWRITEAOF");
}

inline void bgsave(Connection &connection) {
    connection.send("BGSAVE");
}

inline void dbsize(Connection &connection) {
    connection.send("DBSIZE");
}

inline void flushall(Connection &connection, bool async) {
    if (async) {
        connection.send("FLUSHALL ASYNC");
    } else {
        connection.send("FLUSHALL");
    }
}

inline void flushdb(Connection &connection, bool async) {
    if (async) {
        connection.send("FLUSHDB ASYNC");
    } else {
        connection.send("FLUSHDB");
    }
}

inline void info(Connection &connection) {
    connection.send("INFO");
}

inline void info(Connection &connection, const StringView &section) {
    connection.send("INFO %b", section.data(), section.size());
}

inline void lastsave(Connection &connection) {
    connection.send("LASTSAVE");
}

inline void save(Connection &connection) {
    connection.send("SAVE");
}

// KEY commands.

inline void del(Connection &connection, const StringView &key) {
    connection.send("DEL %b", key.data(), key.size());
}

template <typename Input>
inline void del_range(Connection &connection, Input first, Input last) {
    Connection::CmdArgs args;
    args << "DEL" << std::make_pair(first, last);

    connection.send(args);
}

inline void dump(Connection &connection, const StringView &key) {
    connection.send("DUMP %b", key.data(), key.size());
}

inline void exists(Connection &connection, const StringView &key) {
    connection.send("EXISTS %b", key.data(), key.size());
}

template <typename Input>
inline void exists_range(Connection &connection, Input first, Input last) {
    Connection::CmdArgs args;
    args << "EXISTS" << std::make_pair(first, last);

    connection.send(args);
}

inline void expire(Connection &connection,
                    const StringView &key,
                    long long timeout) {
    connection.send("EXPIRE %b %lld",
                    key.data(), key.size(),
                    timeout);
}

inline void expireat(Connection &connection,
                        const StringView &key,
                        long long timestamp) {
    connection.send("EXPIREAT %b %lld",
                    key.data(), key.size(),
                    timestamp);
}

inline void keys(Connection &connection, const StringView &pattern) {
    connection.send("KEYS %b", pattern.data(), pattern.size());
}

inline void move(Connection &connection, const StringView &key, long long db) {
    connection.send("MOVE %b %lld",
                    key.data(), key.size(),
                    db);
}

inline void persist(Connection &connection, const StringView &key) {
    connection.send("PERSIST %b", key.data(), key.size());
}

inline void pexpire(Connection &connection,
                    const StringView &key,
                    long long timeout) {
    connection.send("PEXPIRE %b %lld",
                    key.data(), key.size(),
                    timeout);
}

inline void pexpireat(Connection &connection,
                        const StringView &key,
                        long long timestamp) {
    connection.send("PEXPIRE %b %lld",
                    key.data(), key.size(),
                    timestamp);
}

inline void pttl(Connection &connection, const StringView &key) {
    connection.send("PTTL %b", key.data(), key.size());
}

inline void randomkey(Connection &connection) {
    connection.send("RANDOMKEY");
}

inline void rename(Connection &connection,
                    const StringView &key,
                    const StringView &newkey) {
    connection.send("RENAME %b %b",
                    key.data(), key.size(),
                    newkey.data(), newkey.size());
}

inline void renamenx(Connection &connection,
                        const StringView &key,
                        const StringView &newkey) {
    connection.send("RENAMENX %b %b",
                    key.data(), key.size(),
                    newkey.data(), newkey.size());
}

void restore(Connection &connection,
                const StringView &key,
                long long ttl,
                const StringView &val,
                bool replace);

inline void scan(Connection &connection,
                    long long cursor,
                    const StringView &pattern,
                    long long count) {
    connection.send("SCAN %lld MATCH %b COUNT %lld",
                    cursor,
                    pattern.data(), pattern.size(),
                    count);
}

inline void touch(Connection &connection, const StringView &key) {
    connection.send("TOUCH %b", key.data(), key.size());
}

template <typename Input>
inline void touch_range(Connection &connection, Input first, Input last) {
    Connection::CmdArgs args;
    args << "TOUCH" << std::make_pair(first, last);

    connection.send(args);
}

inline void ttl(Connection &connection, const StringView &key) {
    connection.send("TTL %b", key.data(), key.size());
}

inline void type(Connection &connection, const StringView &key) {
    connection.send("TYPE %b", key.data(), key.size());
}

inline void unlink(Connection &connection, const StringView &key) {
    connection.send("UNLINK %b", key.data(), key.size());
}

template <typename Input>
inline void unlink_range(Connection &connection, Input first, Input last) {
    Connection::CmdArgs args;
    args << "UNLINK" << std::make_pair(first, last);

    connection.send(args);
}

inline void wait(Connection &connection, long long numslave, long long timeout) {
    connection.send("WAIT %lld %lld", numslave, timeout);
}

// STRING commands.

inline void append(Connection &connection, const StringView &key, const StringView &str) {
    connection.send("APPEND %b %b",
                    key.data(), key.size(),
                    str.data(), str.size());
}

inline void bitcount(Connection &connection,
                        const StringView &key,
                        long long start,
                        long long end) {
    connection.send("BITCOUNT %b %lld %lld",
                    key.data(), key.size(),
                    start, end);
}

template <typename Input>
void bitop(Connection &connection,
            BitOp op,
            const StringView &destination,
            Input first,
            Input last);

inline void bitpos(Connection &connection,
                    const StringView &key,
                    long long bit,
                    long long start,
                    long long end) {
    connection.send("BITPOS %b %lld %lld %lld",
                    key.data(), key.size(),
                    bit,
                    start,
                    end);
}

inline void decr(Connection &connection, const StringView &key) {
    connection.send("DECR %b", key.data(), key.size());
}

inline void decrby(Connection &connection, const StringView &key, long long decrement) {
    connection.send("DECRBY %b",
                    key.data(), key.size(),
                    decrement);
}

inline void get(Connection &connection, const StringView &key) {
    connection.send("GET %b",
                    key.data(), key.size());
}

inline void getbit(Connection &connection, const StringView &key, long long offset) {
    connection.send("GETBIT %b %lld",
                    key.data(), key.size(),
                    offset);
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

inline void incr(Connection &connection, const StringView &key) {
    connection.send("INCR %b", key.data(), key.size());
}

inline void incrby(Connection &connection, const StringView &key, long long increment) {
    connection.send("INCRBY %b",
                    key.data(), key.size(),
                    increment);
}

inline void incrbyfloat(Connection &connection, const StringView &key, double increment) {
    connection.send("INCRBYFLOAT %b %f",
                    key.data(), key.size(),
                    increment);
}

template <typename Input>
inline void mget(Connection &connection, Input first, Input last) {
    Connection::CmdArgs args;
    args << "MGET" << std::make_pair(first, last);

    connection.send(args);
}

template <typename Input>
inline void mset(Connection &connection, Input first, Input last) {
    Connection::CmdArgs args;
    args << "MSET" << std::make_pair(first, last);

    connection.send(args);
}

template <typename Input>
inline void msetnx(Connection &connection, Input first, Input last) {
    Connection::CmdArgs args;
    args << "MSETNX" << std::make_pair(first, last);

    connection.send(args);
}

inline void psetex(Connection &connection,
                    const StringView &key,
                    long long ttl,
                    const StringView &val) {
    connection.send("PSETEX %b %lld %b",
                    key.data(), key.size(),
                    ttl,
                    val.data(), val.size());
}

void set(Connection &connection,
            const StringView &key,
            const StringView &val,
            long long ttl,
            UpdateType type);

inline void setbit(Connection &connection,
                    const StringView &key,
                    long long offset,
                    long long value) {
    connection.send("SETBIT %b %lld %lld",
                    key.data(), key.size(),
                    offset,
                    value);
}

inline void setex(Connection &connection,
                    const StringView &key,
                    long long ttl,
                    const StringView &val) {
    connection.send("SETEX %b %lld %b",
                    key.data(), key.size(),
                    ttl,
                    val.data(), val.size());
}

inline void setnx(Connection &connection,
                    const StringView &key,
                    const StringView &val) {
    connection.send("SETNX %b %b",
                    key.data(), key.size(),
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

template <typename Input>
inline void blpop(Connection &connection,
                    Input first,
                    Input last,
                    long long timeout) {
    Connection::CmdArgs args;
    args << "BLPOP" << std::make_pair(first, last) << timeout;

    connection.send(args);
}

template <typename Input>
inline void brpop(Connection &connection,
                    Input first,
                    Input last,
                    long long timeout) {
    Connection::CmdArgs args;
    args << "BRPOP" << std::make_pair(first, last) << timeout;

    connection.send(args);
}

inline void brpoplpush(Connection &connection,
                        const StringView &source,
                        const StringView &destination,
                        long long timeout) {
    connection.send("BRPOPLPUSH %b %b %lld",
                    source.data(), source.size(),
                    destination.data(), destination.size(),
                    timeout);
}

inline void lindex(Connection &connection, const StringView &key, long long index) {
    connection.send("LINDEX %b %lld",
                    key.data(), key.size(),
                    index);
}

void linsert(Connection &connection,
                const StringView &key,
                InsertPosition position,
                const StringView &pivot,
                const StringView &val);

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
                    long long count,
                    const StringView &val) {
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

inline void rpoplpush(Connection &connection,
                        const StringView &source,
                        const StringView &destination) {
    connection.send("RPOPLPUSH %b %b",
                    source.data(), source.size(),
                    destination.data(), destination.size());
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

inline void hincrby(Connection &connection,
                    const StringView &key,
                    const StringView &field,
                    long long increment) {
    connection.send("HINCRBY %b %b %lld",
                    key.data(), key.size(),
                    field.data(), field.size(),
                    increment);
}

inline void hincrbyfloat(Connection &connection,
                            const StringView &key,
                            const StringView &field,
                            double increment) {
    connection.send("HINCRBYFLOAT %b %b %f",
                    key.data(), key.size(),
                    field.data(), field.size(),
                    increment);
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

inline void hscan(Connection &connection,
                    const StringView &key,
                    long long cursor,
                    const StringView &pattern,
                    long long count) {
    connection.send("HSCAN %b %lld MATCH %b COUNT %lld",
                    key.data(), key.size(),
                    cursor,
                    pattern.data(), pattern.size(),
                    count);
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

template <typename Input>
inline void sdiff(Connection &connection, Input first, Input last) {
    Connection::CmdArgs args;
    args << "SDIFF" << std::make_pair(first, last);

    connection.send(args);
}

template <typename Input>
inline void sdiffstore(Connection &connection,
                        const StringView &destination,
                        Input first,
                        Input last) {
    Connection::CmdArgs args;
    args << "SDIFFSTORE" << destination << std::make_pair(first, last);

    connection.send(args);
}

template <typename Input>
inline void sinter(Connection &connection, Input first, Input last) {
    Connection::CmdArgs args;
    args << "SINTER" << std::make_pair(first, last);

    connection.send(args);
}

template <typename Input>
inline void sinterstore(Connection &connection,
                        const StringView &destination,
                        Input first,
                        Input last) {
    Connection::CmdArgs args;
    args << "SINTERSTORE" << destination << std::make_pair(first, last);

    connection.send(args);
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

inline void smove(Connection &connection,
                    const StringView &source,
                    const StringView &destination,
                    const StringView &member) {
    connection.send("SMOVE %b %b %b",
                    source.data(), source.size(),
                    destination.data(), destination.size(),
                    member.data(), member.size());
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

inline void sscan(Connection &connection,
                    const StringView &key,
                    long long cursor,
                    const StringView &pattern,
                    long long count) {
    connection.send("SSCAN %b %lld MATCH %b COUNT %lld",
                    key.data(), key.size(),
                    cursor,
                    pattern.data(), pattern.size(),
                    count);
}

template <typename Input>
inline void sunion(Connection &connection, Input first, Input last) {
    Connection::CmdArgs args;
    args << "SUNION" << std::make_pair(first, last);

    connection.send(args);
}

template <typename Input>
inline void sunionstore(Connection &connection,
                        const StringView &destination,
                        Input first,
                        Input last) {
    Connection::CmdArgs args;
    args << "SUNIONSTORE" << destination << std::make_pair(first, last);

    connection.send(args);
}

// Sorted Set commands.

template <typename Iter>
void zadd_range(Connection &connection,
                const StringView &key,
                Iter first,
                Iter last,
                bool changed,
                UpdateType type);

inline void zadd(Connection &connection,
                    const StringView &key,
                    double score,
                    const StringView &member,
                    bool changed,
                    UpdateType type) {
    auto tmp = {std::make_pair(score, member)};

    zadd_range(connection, key, tmp.begin(), tmp.end(), changed, type);
}

inline void zcard(Connection &connection, const StringView &key) {
    connection.send("ZCARD %b", key.data(), key.size());
}

template <typename Interval>
inline void zcount(Connection &connection,
                    const StringView &key,
                    const Interval &interval) {
    connection.send("ZCOUNT %b %s %s",
                    key.data(), key.size(),
                    interval.min().c_str(),
                    interval.max().c_str());
}

inline void zincrby(Connection &connection,
                    const StringView &key,
                    double increment,
                    const StringView &member) {
    connection.send("ZINCRBY %b %f %b",
                    key.data(), key.size(),
                    increment,
                    member.data(), member.size());
}

template <typename Input>
void zinterstore(Connection &connection,
                    const StringView &destination,
                    Input first,
                    Input last,
                    Aggregation aggr);

template <typename Interval>
inline void zlexcount(Connection &connection,
                        const StringView &key,
                        const Interval &interval) {
    const auto &min = interval.min();
    const auto &max = interval.max();

    connection.send("ZLEXCOUNT %b %b %b",
                    key.data(), key.size(),
                    min.data(), min.size(),
                    max.data(), max.size());
}

template <typename Output>
void zrange(Connection &connection,
                const StringView &key,
                long long start,
                long long stop);

template <typename Interval>
inline void zrangebylex(Connection &connection,
                        const StringView &key,
                        const Interval &interval,
                        const LimitOptions &opts) {
    const auto &min = interval.min();
    const auto &max = interval.max();

    connection.send("ZRANGEBYLEX %b %b %b LIMIT %lld %lld",
                    key.data(), key.size(),
                    min.data(), min.size(),
                    max.data(), max.size(),
                    opts.offset,
                    opts.count);
}

template <typename Interval, typename Output>
void zrangebyscore(Connection &connection,
                    const StringView &key,
                    const Interval &interval,
                    const LimitOptions &opts);

inline void zrank(Connection &connection,
                    const StringView &key,
                    const StringView &member) {
    connection.send("ZRANK %b %b",
                    key.data(), key.size(),
                    member.data(), member.size());
}

inline void zrem(Connection &connection,
                    const StringView &key,
                    const StringView &member) {
    connection.send("ZREM %b %b",
                    key.data(), key.size(),
                    member.data(), member.size());
}

template <typename Input>
inline void zrem_range(Connection &connection,
                        const StringView &key,
                        Input first,
                        Input last) {
    Connection::CmdArgs args;
    args << "ZREM" << key << std::make_pair(first, last);

    connection.send(args);
}

template <typename Interval>
inline void zremrangebylex(Connection &connection,
                            const StringView &key,
                            const Interval &interval) {
    const auto &min = interval.min();
    const auto &max = interval.max();

    connection.send("ZREMRANGEBYLEX %b %b %b",
                    key.data(), key.size(),
                    min.data(), min.size(),
                    max.data(), max.size());
}

inline void zremrangebyrank(Connection &connection,
                            const StringView &key,
                            long long start,
                            long long stop) {
    connection.send("zremrangebyrank %b %lld %lld",
                    key.data(), key.size(),
                    start,
                    stop);
}

template <typename Interval>
inline void zremrangebyscore(Connection &connection,
                                const StringView &key,
                                const Interval &interval) {
    const auto &min = interval.min();
    const auto &max = interval.max();

    connection.send("ZREMRANGEBYSCORE %b %b %b",
                    key.data(), key.size(),
                    min.data(), min.size(),
                    max.data(), max.size());
}

template <typename Outupt>
void zrevrange(Connection &connection,
                const StringView &key,
                long long start,
                long long stop);

template <typename Interval>
inline void zrevrangebylex(Connection &connection,
                            const StringView &key,
                            const Interval &interval,
                            const LimitOptions &opts) {
    const auto &min = interval.min();
    const auto &max = interval.max();

    connection.send("ZREVRANGEBYLEX %b %b %b LIMIT %lld %lld",
                    key.data(), key.size(),
                    max.data(), max.size(),
                    min.data(), min.size(),
                    opts.offset,
                    opts.count);
}

template <typename Interval, typename Output>
void zrevrangebyscore(Connection &connection,
                        const StringView &key,
                        const Interval &interval,
                        const LimitOptions &opts);

inline void zrevrank(Connection &connection,
                        const StringView &key,
                        const StringView &member) {
    connection.send("ZREVRANK %b %b",
                    key.data(), key.size(),
                    member.data(), member.size());
}

inline void zscan(Connection &connection,
                    const StringView &key,
                    long long cursor,
                    const StringView &pattern,
                    long long count) {
    connection.send("ZSCAN %b %lld MATCH %b COUNT %lld",
                    key.data(), key.size(),
                    cursor,
                    pattern.data(), pattern.size(),
                    count);
}

inline void zscore(Connection &connection,
                    const StringView &key,
                    const StringView &member) {
    connection.send("ZSCORE %b %b",
                    key.data(), key.size(),
                    member.data(), member.size());
}

template <typename Input>
void zunionstore(Connection &connection,
                    const StringView &destination,
                    Input first,
                    Input last,
                    Aggregation aggr);

// HYPERLOGLOG commands.

inline void pfadd(Connection &connection,
                    const StringView &key,
                    const StringView &element) {
    connection.send("PFADD %b %b",
                    key.data(), key.size(),
                    element.data(), element.size());
}

template <typename Input>
inline void pfadd_range(Connection &connection,
                        const StringView &key,
                        Input first,
                        Input last) {
    Connection::CmdArgs args;
    args << "PFADD" << key << std::make_pair(first, last);

    connection.send(args);
}

inline void pfcount(Connection &connection, const StringView &key) {
    connection.send("PFCOUNT %b", key.data(), key.size());
}

template <typename Input>
inline void pfcount_range(Connection &connection,
                            Input first,
                            Input last) {
    Connection::CmdArgs args;
    args << "PFCOUNT" << std::make_pair(first, last);

    connection.send(args);
}

template <typename Input>
inline void pfmerge(Connection &connection,
                    const StringView &destination,
                    Input first,
                    Input last) {
    Connection::CmdArgs args;
    args << "PFMERGE" << destination << std::make_pair(first, last);

    connection.send(args);
}

// GEO commands.

inline void geoadd(Connection &connection,
                    const StringView &key,
                    const std::tuple<double, double, std::string> &member) {
    const auto &mem = std::get<2>(member);

    connection.send("GEOADD %b %f %f %b",
                    key.data(), key.size(),
                    std::get<0>(member),
                    std::get<1>(member),
                    mem.data(), mem.size());
}

template <typename Input>
inline void geoadd_range(Connection &connection,
                            const StringView &key,
                            Input first,
                            Input last) {
    Connection::CmdArgs args;
    args << "GEOADD" << key << std::make_pair(first, last);

    connection.send(args);
}

void geodist(Connection &connection,
                const StringView &key,
                const StringView &member1,
                const StringView &member2,
                GeoUnit unit);

inline void geohash(Connection &connection,
                    const StringView &key,
                    const StringView &member) {
    connection.send("GEOHASH %b %b",
                    key.data(), key.size(),
                    member.data(), member.size());
}

template <typename Input>
inline void geohash_range(Connection &connection,
                            const StringView &key,
                            Input first,
                            Input last) {
    Connection::CmdArgs args;
    args << "GEOHASH" << key << std::make_pair(first, last);

    connection.send(args);
}

inline void geopos(Connection &connection,
                    const StringView &key,
                    const StringView &member) {
    connection.send("GEOPOS %b %b",
                    key.data(), key.size(),
                    member.data(), member.size());
}

template <typename Input>
inline void geopos_range(Connection &connection,
                            const StringView &key,
                            Input first,
                            Input last) {
    Connection::CmdArgs args;
    args << "GEOPOS" << key << std::make_pair(first, last);

    connection.send(args);
}

template <typename Output>
void georadius(Connection &connection,
                const StringView &key,
                const std::pair<double, double> &loc,
                double radius,
                GeoUnit unit,
                long long count,
                bool asc);

void georadius_store(Connection &connection,
                        const StringView &key,
                        const std::pair<double, double> &loc,
                        double radius,
                        GeoUnit unit,
                        const StringView &destination,
                        bool store_dist,
                        long long count);

template <typename Output>
void georadiusbymember(Connection &connection,
                        const StringView &key,
                        const StringView &member,
                        double radius,
                        GeoUnit unit,
                        long long count,
                        bool asc);

void georadiusbymember_store(Connection &connection,
                                const StringView &key,
                                const StringView &member,
                                double radius,
                                GeoUnit unit,
                                const StringView &destination,
                                bool store_dist,
                                long long count);

// SCRIPTING commands.

inline void eval(Connection &connection,
                    const StringView &script,
                    std::initializer_list<StringView> keys,
                    std::initializer_list<StringView> args) {
    Connection::CmdArgs cmd_args;

    cmd_args << "EVAL" << script << keys.size()
            << std::make_pair(keys.begin(), keys.end())
            << std::make_pair(args.begin(), args.end());

    connection.send(cmd_args);
}

inline void evalsha(Connection &connection,
                    const StringView &script,
                    std::initializer_list<StringView> keys,
                    std::initializer_list<StringView> args) {
    Connection::CmdArgs cmd_args;

    cmd_args << "EVALSHA" << script << keys.size()
            << std::make_pair(keys.begin(), keys.end())
            << std::make_pair(args.begin(), args.end());

    connection.send(cmd_args);
}

inline void script_exists(Connection &connection, const StringView &sha) {
    connection.send("SCRIPT EXISTS %b", sha.data(), sha.size());
}

template <typename Input>
inline void script_exists_range(Connection &connection, Input first, Input last) {
    Connection::CmdArgs args;
    args << "SCRIPT" << "EXISTS" << std::make_pair(first, last);

    connection.send(args);
}

inline void script_flush(Connection &connection) {
    connection.send("SCRIPT FLUSH");
}

inline void script_kill(Connection &connection) {
    connection.send("SCRIPT KILL");
}

inline void script_load(Connection &connection, const StringView &script) {
    connection.send("SCRIPT LOAD %b", script.data(), script.size());
}

// PUBSUB commands.

inline void psubscribe(Connection &connection, const StringView &pattern) {
    connection.send("PSUBSCRIBE %b", pattern.data(), pattern.size());
}

template <typename Input>
inline void psubscribe_range(Connection &connection, Input first, Input last) {
    Connection::CmdArgs args;
    args << "PSUBSCRIBE" << std::make_pair(first, last);

    connection.send(args);
}

inline void publish(Connection &connection,
                    const StringView &channel,
                    const StringView &message) {
    connection.send("PUBLISH %b %b",
                    channel.data(), channel.size(),
                    message.data(), message.size());
}

inline void punsubscribe(Connection &connection) {
    connection.send("PUNSUBSCRIBE");
}

inline void punsubscribe(Connection &connection, const StringView &pattern) {
    connection.send("PUNSUBSCRIBE %b", pattern.data(), pattern.size());
}

template <typename Input>
inline void punsubscribe_range(Connection &connection, Input first, Input last) {
    Connection::CmdArgs args;
    args << "PUNSUBSCRIBE" << std::make_pair(first, last);

    connection.send(args);
}

inline void subscribe(Connection &connection, const StringView &channel) {
    connection.send("SUBSCRIBE %b", channel.data(), channel.size());
}

template <typename Input>
inline void subscribe(Connection &connection, Input first, Input last) {
    Connection::CmdArgs args;
    args << "SUBSCRIBE" << std::make_pair(first, last);

    connection.send(args);
}

inline void unsubscribe(Connection &connection) {
    connection.send("UNSUBSCRIBE");
}

inline void unsubscribe(Connection &connection, const StringView &channel) {
    connection.send("UNSUBSCRIBE %b", channel.data(), channel.size());
}

template <typename Input>
inline void unsubscribe_range(Connection &connection, Input first, Input last) {
    Connection::CmdArgs args;
    args << "UNSUBSCRIBE" << std::make_pair(first, last);

    connection.send(args);
}

namespace detail {

void set_update_type(Connection::CmdArgs &args, UpdateType type);

template <typename Cmd, typename ...Args>
inline void score_command(std::true_type, Cmd cmd, Args &&... args) {
    cmd(std::forward<Args>(args)..., true);
}

template <typename Cmd, typename ...Args>
inline void score_command(std::false_type, Cmd cmd, Args &&... args) {
    cmd(std::forward<Args>(args)..., false);
}

template <typename Output, typename Cmd, typename ...Args>
inline void score_command(Cmd cmd, Args &&... args) {
    score_command(typename IsKvPairIter<Output>::type(), cmd, std::forward<Args>(args)...);
}

inline void zrange(Connection &connection,
                const StringView &key,
                long long start,
                long long stop,
                bool with_scores) {
    if (with_scores) {
        connection.send("ZRANGE %b %lld %lld WITHSCORES",
                        key.data(), key.size(),
                        start,
                        stop);
    } else {
        connection.send("ZRANGE %b %lld %lld",
                        key.data(), key.size(),
                        start,
                        stop);
    }
}

template <typename Interval>
void zrangebyscore(Connection &connection,
                    const StringView &key,
                    const Interval &interval,
                    const LimitOptions &opts,
                    bool with_scores) {
    const auto &min = interval.min();
    const auto &max = interval.max();

    if (with_scores) {
        connection.send("ZRANGEBYSCORE %b %b %b WITHSCORES LIMIT %lld %lld",
                        key.data(), key.size(),
                        min.data(), min.size(),
                        max.data(), max.size(),
                        opts.offset,
                        opts.count);
    } else {
        connection.send("ZRANGEBYSCORE %b %b %b LIMIT %lld %lld",
                        key.data(), key.size(),
                        min.data(), min.size(),
                        max.data(), max.size(),
                        opts.offset,
                        opts.count);
    }
}

inline void zrevrange(Connection &connection,
                        const StringView &key,
                        long long start,
                        long long stop,
                        bool with_scores) {
    if (with_scores) {
        connection.send("ZREVRANGE %b %lld %lld WITHSCORES",
                        key.data(), key.size(),
                        start,
                        stop);
    } else {
        connection.send("ZREVRANGE %b %lld %lld",
                        key.data(), key.size(),
                        start,
                        stop);
    }
}

template <typename Interval>
inline void zrevrangebyscore(Connection &connection,
                                const StringView &key,
                                const Interval &interval,
                                const LimitOptions &opts,
                                bool with_scores) {
    const auto &min = interval.min();
    const auto &max = interval.max();

    if (with_scores) {
        connection.send("ZREVRANGEBYSCORE %b %b %b WITHSCORES LIMIT %lld %lld",
                        key.data(), key.size(),
                        max.data(), max.size(),
                        min.data(), min.size(),
                        opts.offset,
                        opts.count);
    } else {
        connection.send("ZREVRANGEBYSCORE %b %b %b LIMIT %lld %lld",
                        key.data(), key.size(),
                        max.data(), max.size(),
                        min.data(), min.size(),
                        opts.offset,
                        opts.count);
    }
}

void set_aggregation_type(Connection::CmdArgs &args, Aggregation type);

template <typename Input>
void zinterstore(std::false_type,
                    Connection &connection,
                    const StringView &destination,
                    Input first,
                    Input last,
                    Aggregation aggr) {
    Connection::CmdArgs args;
    args << "ZINTERSTORE" << destination << std::distance(first, last)
        << std::make_pair(first, last);

    set_aggregation_type(args, aggr);

    connection.send(args);
}

template <typename Input>
void zinterstore(std::true_type,
                    Connection &connection,
                    const StringView &destination,
                    Input first,
                    Input last,
                    Aggregation aggr) {
    Connection::CmdArgs args;
    args << "ZINTERSTORE" << destination << std::distance(first, last);

    for (auto iter = first; iter != last; ++iter) {
        args << iter->first;
    }

    args << "WEIGHTS";

    for (auto iter = first; iter != last; ++iter) {
        args << iter->second;
    }

    set_aggregation_type(args, aggr);

    connection.send(args);
}

template <typename Input>
void zunionstore(std::false_type,
                    Connection &connection,
                    const StringView &destination,
                    Input first,
                    Input last,
                    Aggregation aggr) {
    Connection::CmdArgs args;
    args << "ZUNIONSTORE" << destination << std::distance(first, last)
        << std::make_pair(first, last);

    set_aggregation_type(args, aggr);

    connection.send(args);
}

template <typename Input>
void zunionstore(std::true_type,
                    Connection &connection,
                    const StringView &destination,
                    Input first,
                    Input last,
                    Aggregation aggr) {
    Connection::CmdArgs args;
    args << "ZUNIONSTORE" << destination << std::distance(first, last);

    for (auto iter = first; iter != last; ++iter) {
        args << iter->first;
    }

    args << "WEIGHTS";

    for (auto iter = first; iter != last; ++iter) {
        args << iter->second;
    }

    set_aggregation_type(args, aggr);

    connection.send(args);
}

void set_geo_unit(Connection::CmdArgs &args, GeoUnit unit);

void set_georadius_store_parameters(Connection::CmdArgs &args,
                                    double radius,
                                    GeoUnit unit,
                                    const StringView &destination,
                                    bool store_dist,
                                    long long count);

template <typename T>
struct WithCoord : TupleWithType<std::pair<double, double>, T> {};

template <typename T>
struct WithDist : TupleWithType<double, T> {};

template <typename T>
struct WithHash : TupleWithType<long long, T> {};

template <typename Output>
void set_georadius_parameters(Connection::CmdArgs &args,
                                double radius,
                                GeoUnit unit,
                                long long count,
                                bool asc) {
    args << radius;

    detail::set_geo_unit(args, unit);

    if (detail::WithCoord<typename IterType<Output>::type>::value) {
        args << "WITHCOORD";
    }

    if (detail::WithDist<typename IterType<Output>::type>::value) {
        args << "WITHDIST";
    }

    if (detail::WithHash<typename IterType<Output>::type>::value) {
        args << "WITHHASH";
    }

    args << "COUNT" << count;

    if (asc) {
        args << "ASC";
    } else {
        args << "DESC";
    }
}

}

}

}

}

namespace sw {

namespace redis {

namespace cmd {

template <typename Input>
void bitop(Connection &connection,
            BitOp op,
            const StringView &destination,
            Input first,
            Input last) {
    Connection::CmdArgs args;
    args << "BITOP";
    switch (op) {
    case BitOp::AND:
        args << "AND";
        break;

    case BitOp::OR:
        args << "OR";
        break;

    case BitOp::XOR:
        args << "XOR";
        break;

    case BitOp::NOT:
        args << "NOT";
        break;

    default:
        throw RException("Unknown bit operations");
    }

    args << destination << std::make_pair(first, last);

    connection.send(args);
}

template <typename Iter>
void zadd_range(Connection &connection,
                const StringView &key,
                Iter first,
                Iter last,
                bool changed,
                UpdateType type) {
    Connection::CmdArgs args;

    args << "ZADD" << key;

    detail::set_update_type(args, type);

    if (changed) {
        args << "CH";
    }

    args << std::make_pair(first, last);

    connection.send(args);
}

template <typename Output>
inline void zrange(Connection &connection,
                const StringView &key,
                long long start,
                long long stop) {
    detail::score_command<Output>(detail::zrange, connection, key, start, stop);
}

template <typename Interval, typename Output>
void zrangebyscore(Connection &connection,
                    const StringView &key,
                    const Interval &interval,
                    const LimitOptions &opts) {
    detail::score_command<Output>(detail::zrangebyscore<Interval>, connection, key, interval, opts);
}

template <typename Output>
void zrevrange(Connection &connection,
                const StringView &key,
                long long start,
                long long stop) {
    detail::score_command<Output>(detail::zrevrange, connection, key, start, stop);
}

template <typename Interval, typename Output>
void zrevrangebyscore(Connection &connection,
                        const StringView &key,
                        const Interval &interval,
                        const LimitOptions &opts) {
    detail::score_command<Output>(detail::zrevrangebyscore<Interval>,
                                    connection,
                                    key,
                                    interval,
                                    opts);
}

template <typename Input>
void zinterstore(Connection &connection,
                    const StringView &destination,
                    Input first,
                    Input last,
                    Aggregation aggr) {
    detail::zinterstore(typename IsKvPairIter<Input>::type(),
                        connection,
                        destination,
                        first,
                        last,
                        aggr);
}

template <typename Input>
void zunionstore(Connection &connection,
                    const StringView &destination,
                    Input first,
                    Input last,
                    Aggregation aggr) {
    detail::zunionstore(typename IsKvPairIter<Input>::type(),
                        connection,
                        destination,
                        first,
                        last,
                        aggr);
}

template <typename Output>
void georadius(Connection &connection,
                const StringView &key,
                const std::pair<double, double> &loc,
                double radius,
                GeoUnit unit,
                long long count,
                bool asc) {
    Connection::CmdArgs args;
    args << "GEORADIUS" << key << loc.first << loc.second;

    detail::set_georadius_parameters<Output>(args, radius, unit, count, asc);

    connection.send(args);
}

template <typename Output>
void georadiusbymember(Connection &connection,
                        const StringView &key,
                        const StringView &member,
                        double radius,
                        GeoUnit unit,
                        long long count,
                        bool asc) {
    Connection::CmdArgs args;
    args << "GEORADIUSBYMEMBER" << key << member;

    detail::set_georadius_parameters<Output>(args, radius, unit, count, asc);

    connection.send(args);
}

}

}

}

#endif // end SEWENEW_REDISPLUSPLUS_COMMAND_H
