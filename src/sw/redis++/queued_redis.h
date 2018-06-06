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

#ifndef SEWENEW_REDISPLUSPLUS_QUEUED_REDIS_H
#define SEWENEW_REDISPLUSPLUS_QUEUED_REDIS_H

#include <cassert>
#include <chrono>
#include <deque>
#include "connection.h"
#include "utils.h"
#include "reply.h"
#include "command.h"

namespace sw {

namespace redis {

namespace chrono = std::chrono;

class QueuedReplies;

// If any command throws, QueuedRedis resets the connection, and becomes invalid.
// In this case, the only thing we can do is to destory the QueuedRedis object.
template <typename Impl>
class QueuedRedis {
public:
    QueuedRedis(QueuedRedis &&) = default;
    QueuedRedis& operator=(QueuedRedis &&) = default;

    // When it destructs, the underlying *Connection* will be closed,
    // and any command that has NOT been executed will be ignored.
    ~QueuedRedis() = default;

    template <typename Cmd, typename ...Args>
    QueuedRedis& command(Cmd cmd, Args &&...args);

    QueuedReplies exec();

    void discard();

    // CONNECTION commands.

    QueuedRedis& auth(const StringView &password) {
        return command(cmd::auth, password);
    }

    QueuedRedis& echo(const StringView &msg) {
        return command(cmd::echo, msg);
    }

    QueuedRedis& ping() {
        return command<void (*)(Connection &)>(cmd::ping);
    }

    QueuedRedis& ping(const StringView &msg) {
        return command<void (*)(Connection &, const StringView &)>(cmd::ping, msg);
    }

    // We DO NOT support the QUIT command. See *Redis::quit* doc for details.
    //
    // QueuedRedis& quit();

    // Before returning the underlying connection to pool, i.e. destroying *QueueRedis*
    // object, we must call *select* to reset the DB index. Otherwise, the subsequent
    // command might work on the wrong DB.
    QueuedRedis& select(long long idx) {
        return command(cmd::select, idx);
    }

    QueuedRedis& swapdb(long long idx1, long long idx2) {
        return command(cmd::swapdb, idx1, idx2);
    }

    // SERVER commands.

    QueuedRedis& bgrewriteaof() {
        return command(cmd::bgrewriteaof);
    }

    QueuedRedis& bgsave() {
        return command(cmd::bgsave);
    }

    QueuedRedis& dbsize() {
        return command(cmd::dbsize);
    }

    QueuedRedis& flushall(bool async = false) {
        return command(cmd::flushall, async);
    }

    QueuedRedis& flushdb(bool async = false) {
        return command(cmd::flushdb, async);
    }

    QueuedRedis& info() {
        return command<void (*)(Connection &)>(cmd::info);
    }

    QueuedRedis& info(const StringView &section) {
        return command<void (*)(Connection &, const StringView &)>(cmd::info, section);
    }

    QueuedRedis& lastsave() {
        return command(cmd::lastsave);
    }

    QueuedRedis& save() {
        return command(cmd::save);
    }

    // KEY commands.

    QueuedRedis& del(const StringView &key) {
        return command(cmd::del, key);
    }

    template <typename Input>
    QueuedRedis& del(Input first, Input last) {
        return command(cmd::del_range<Input>, first, last);
    }

    QueuedRedis& dump(const StringView &key) {
        return command(cmd::dump, key);
    }

    QueuedRedis& exists(const StringView &key) {
        return command(cmd::exists, key);
    }

    template <typename Input>
    QueuedRedis& exists(Input first, Input last) {
        return command(cmd::exists_range<Input>, first, last);
    }

    QueuedRedis& expire(const StringView &key, long long timeout) {
        return command(cmd::expire, key, timeout);
    }

    QueuedRedis& expire(const StringView &key,
                        const chrono::seconds &timeout) {
        return expire(key, timeout.count());
    }

    QueuedRedis& expireat(const StringView &key, long long timestamp) {
        return command(cmd::expireat, key, timestamp);
    }

    QueuedRedis& expireat(const StringView &key,
                            const chrono::time_point<chrono::system_clock, chrono::seconds> &tp) {
        return expireat(key, tp.time_since_epoch().count());
    }

    QueuedRedis& keys(const StringView &pattern) {
        return command(cmd::keys, pattern);
    }

    QueuedRedis& move(const StringView &key, long long db) {
        return command(cmd::move, key, db);
    }

    QueuedRedis& persist(const StringView &key) {
        return command(cmd::persist, key);
    }

    QueuedRedis& pexpire(const StringView &key, long long timeout) {
        return command(cmd::pexpire, key, timeout);
    }

    QueuedRedis& pexpire(const StringView &key,
                            const chrono::milliseconds &timeout) {
        return pexpire(key, timeout.count());
    }

    QueuedRedis& pexpireat(const StringView &key, long long timestamp) {
        return command(cmd::pexpireat, key, timestamp);
    }

    QueuedRedis& pexpireat(const StringView &key,
                            const chrono::time_point<chrono::system_clock,
                                                        chrono::milliseconds> &tp) {
        return pexpireat(key, tp.time_since_epoch().count());
    }

    QueuedRedis& pttl(const StringView &key) {
        return command(cmd::pttl, key);
    }

    QueuedRedis& randomkey() {
        return command(cmd::randomkey);
    }

    QueuedRedis& rename(const StringView &key, const StringView &newkey) {
        return command(cmd::rename, key, newkey);
    }

    QueuedRedis& renamenx(const StringView &key, const StringView &newkey) {
        return command(cmd::renamenx, key, newkey);
    }

    QueuedRedis& restore(const StringView &key,
                                const StringView &val,
                                long long ttl,
                                bool replace = false) {
        return command(cmd::restore, key, val, ttl, replace);
    }

    QueuedRedis& restore(const StringView &key,
                            const StringView &val,
                            const chrono::milliseconds &ttl = std::chrono::milliseconds{0},
                            bool replace = false) {
        return restore(key, val, ttl.count(), replace);
    }

    // TODO: sort

    QueuedRedis& scan(long long cursor,
                        const StringView &pattern,
                        long long count) {
        return command(cmd::scan, cursor, pattern, count);
    }

    QueuedRedis& scan(long long cursor) {
        return scan(cursor, "*", 10);
    }

    QueuedRedis& scan(long long cursor,
                        const StringView &pattern) {
        return scan(cursor, pattern, 10);
    }

    QueuedRedis& scan(long long cursor,
                        long long count) {
        return scan(cursor, "*", count);
    }

    QueuedRedis& touch(const StringView &key) {
        return command(cmd::touch, key);
    }

    template <typename Input>
    QueuedRedis& touch(Input first, Input last) {
        return command(cmd::touch_range<Input>, first, last);
    }

    QueuedRedis& ttl(const StringView &key) {
        return command(cmd::ttl, key);
    }

    QueuedRedis& type(const StringView &key) {
        return command(cmd::type, key);
    }

    QueuedRedis& unlink(const StringView &key) {
        return command(cmd::unlink, key);
    }

    template <typename Input>
    QueuedRedis& unlink(Input first, Input last) {
        return command(cmd::unlink_range<Input>, first, last);
    }

    QueuedRedis& wait(long long numslaves, long long timeout) {
        return command(cmd::wait, numslaves, timeout);
    }

    QueuedRedis& wait(long long numslaves, const chrono::milliseconds &timeout) {
        return wait(numslaves, timeout.count());
    }

    // STRING commands.

    QueuedRedis& append(const StringView &key, const StringView &str) {
        return command(cmd::append, key, str);
    }

    QueuedRedis& bitcount(const StringView &key,
                            long long start = 0,
                            long long end = -1) {
        return command(cmd::bitcount, key, start, end);
    }

    template <typename Input>
    QueuedRedis& bitop(BitOp op,
                        const StringView &destination,
                        Input first,
                        Input last) {
        return command(cmd::bitop<Input>, op, destination, first, last);
    }

    QueuedRedis& bitpos(const StringView &key,
                        long long bit,
                        long long start = 0,
                        long long end = -1) {
        return command(cmd::bitpos, key, bit, start, end);
    }

    QueuedRedis& decr(const StringView &key) {
        return command(cmd::decr, key);
    }

    QueuedRedis& decrby(const StringView &key, long long decrement) {
        return command(cmd::decrby, key, decrement);
    }

    QueuedRedis& get(const StringView &key) {
        return command(cmd::get, key);
    }

    QueuedRedis& getbit(const StringView &key, long long offset) {
        return command(cmd::getbit, key, offset);
    }

    QueuedRedis& getrange(const StringView &key, long long start, long long end) {
        return command(cmd::getrange, key, start, end);
    }

    QueuedRedis& getset(const StringView &key, const StringView &val) {
        return command(cmd::getset, key, val);
    }

    QueuedRedis& incr(const StringView &key) {
        return command(cmd::incr, key);
    }

    QueuedRedis& incrby(const StringView &key, long long increment) {
        return command(cmd::incrby, key, increment);
    }

    QueuedRedis& incrbyfloat(const StringView &key, double increment) {
        return command(cmd::incrbyfloat, key, increment);
    }

    template <typename Input>
    QueuedRedis& mget(Input first, Input last) {
        return command(cmd::mget<Input>, first, last);
    }

    template <typename Input>
    QueuedRedis& mset(Input first, Input last) {
        return command(cmd::mset<Input>, first, last);
    }

    template <typename Input>
    QueuedRedis& msetnx(Input first, Input last) {
        return command(cmd::msetnx<Input>, first, last);
    }

    QueuedRedis& psetex(const StringView &key,
                        long long ttl,
                        const StringView &val) {
        return command(cmd::psetex, key, ttl, val);
    }

    QueuedRedis& psetex(const StringView &key,
                        const std::chrono::milliseconds &ttl,
                        const StringView &val) {
        return psetex(key, ttl.count(), val);
    }

    QueuedRedis& set(const StringView &key,
                        const StringView &val,
                        const chrono::milliseconds &ttl = chrono::milliseconds(0),
                        UpdateType type = UpdateType::ALWAYS) {
        return command(cmd::set, key, val, ttl.count(), type);
    }

    QueuedRedis& setbit(const StringView &key,
                        long long offset,
                        long long value) {
        return command(cmd::setbit, key, offset, value);
    }

    QueuedRedis& setex(const StringView &key,
                        long long ttl,
                        const StringView &val) {
        return command(cmd::setex, key, ttl, val);
    }

    QueuedRedis& setex(const StringView &key,
                        const std::chrono::seconds &ttl,
                        const StringView &val) {
        return command(cmd::setex, key, ttl.count(), val);
    }

    QueuedRedis& setnx(const StringView &key, const StringView &val) {
        return command(cmd::setnx, key, val);
    }

    QueuedRedis& setrange(const StringView &key,
                            long long offset,
                            const StringView &val) {
        return command(cmd::setrange, key, offset, val);
    }

    QueuedRedis& strlen(const StringView &key) {
        return command(cmd::strlen, key);
    }

    // LIST commands.

    template <typename Input>
    QueuedRedis& blpop(Input first, Input last, long long timeout = 0) {
        return command(cmd::blpop<Input>, first, last, timeout);
    }

    template <typename Input>
    QueuedRedis& blpop(Input first,
                        Input last,
                        const chrono::seconds &timeout = chrono::seconds{0}) {
        return blpop(first, last, timeout.count());
    }

    template <typename Input>
    QueuedRedis& brpop(Input first, Input last, long long timeout = 0) {
        return command(cmd::brpop<Input>, first, last, timeout);
    }

    template <typename Input>
    QueuedRedis& brpop(Input first,
                        Input last,
                        const chrono::seconds &timeout = chrono::seconds{0}) {
        return brpop(first, last, timeout.count());
    }

    QueuedRedis& brpoplpush(const StringView &source,
                            const StringView &destination,
                            long long timeout = 0) {
        return command(cmd::brpoplpush, source, destination, timeout);
    }

    QueuedRedis& brpoplpush(const StringView &source,
                            const StringView &destination,
                            const chrono::seconds &timeout = chrono::seconds{0}) {
        return brpoplpush(source, destination, timeout.count());
    }

    QueuedRedis& lindex(const StringView &key, long long index) {
        return command(cmd::lindex, key, index);
    }

    QueuedRedis& linsert(const StringView &key,
                            InsertPosition position,
                            const StringView &pivot,
                            const StringView &val) {
        return command(cmd::linsert, key, position, pivot, val);
    }

    QueuedRedis& llen(const StringView &key) {
        return command(cmd::llen, key);
    }

    QueuedRedis& lpop(const StringView &key) {
        return command(cmd::lpop, key);
    }

    QueuedRedis& lpush(const StringView &key, const StringView &val) {
        return command(cmd::lpush, key, val);
    }

    template <typename Input>
    QueuedRedis& lpush(const StringView &key, Input first, Input last) {
        return command(cmd::lpush_range<Input>, key, first, last);
    }

    QueuedRedis& lpushx(const StringView &key, const StringView &val) {
        return command(cmd::lpushx, key, val);
    }

    QueuedRedis& lrange(const StringView &key,
                        long long start,
                        long long stop) {
        return command(cmd::lrange, key, start, stop);
    }

    QueuedRedis& lrem(const StringView &key, long long count, const StringView &val) {
        return command(cmd::lrem, key, count, val);
    }

    QueuedRedis& lset(const StringView &key, long long index, const StringView &val) {
        return command(cmd::lset, key, index, val);
    }

    QueuedRedis& ltrim(const StringView &key, long long start, long long stop) {
        return command(cmd::ltrim, key, start, stop);
    }

    QueuedRedis& rpop(const StringView &key) {
        return command(cmd::rpop, key);
    }

    QueuedRedis& rpoplpush(const StringView &source, const StringView &destination) {
        return command(cmd::rpoplpush, source, destination);
    }

    QueuedRedis& rpush(const StringView &key, const StringView &val) {
        return command(cmd::rpush, key, val);
    }

    template <typename Input>
    QueuedRedis& rpush(const StringView &key, Input first, Input last) {
        return command(cmd::rpush_range<Input>, key, first, last);
    }

    QueuedRedis& rpushx(const StringView &key, const StringView &val) {
        return command(cmd::rpushx, key, val);
    }

    // HASH commands.

    QueuedRedis& hdel(const StringView &key, const StringView &field) {
        return command(cmd::hdel, key, field);
    }

    template <typename Input>
    QueuedRedis& hdel(const StringView &key, Input first, Input last) {
        return command(cmd::hdel_range<Input>, key, first, last);
    }

    QueuedRedis& hexists(const StringView &key, const StringView &field) {
        return command(cmd::hexists, key, field);
    }

    QueuedRedis& hget(const StringView &key, const StringView &field) {
        return command(cmd::hget, key, field);
    }

    QueuedRedis& hgetall(const StringView &key) {
        return command(cmd::hgetall, key);
    }

    QueuedRedis& hincrby(const StringView &key,
                            const StringView &field,
                            long long increment) {
        return command(cmd::hincrby, key, field, increment);
    }

    QueuedRedis& hincrbyfloat(const StringView &key,
                                const StringView &field,
                                double increment) {
        return command(cmd::hincrbyfloat, key, field, increment);
    }

    QueuedRedis& hkeys(const StringView &key) {
        return command(cmd::hkeys, key);
    }

    QueuedRedis& hlen(const StringView &key) {
        return command(cmd::hlen, key);
    }

    template <typename Input>
    QueuedRedis& hmget(const StringView &key, Input first, Input last) {
        return command(cmd::hmget<Input>, key, first, last);
    }

    template <typename Input>
    QueuedRedis& hmset(const StringView &key, Input first, Input last) {
        return command(cmd::hmset<Input>, key, first, last);
    }

    QueuedRedis& hscan(const StringView &key,
                        long long cursor,
                        const StringView &pattern,
                        long long count) {
        return command(cmd::hscan, key, cursor, pattern, count);
    }

    QueuedRedis& hscan(const StringView &key,
                        long long cursor,
                        const StringView &pattern) {
        return hscan(key, cursor, pattern, 10);
    }

    QueuedRedis& hscan(const StringView &key,
                        long long cursor,
                        long long count) {
        return hscan(key, cursor, "*", count);
    }

    QueuedRedis& hscan(const StringView &key,
                        long long cursor) {
        return hscan(key, cursor, "*", 10);
    }

    QueuedRedis& hset(const StringView &key, const StringView &field, const StringView &val) {
        return command(cmd::hset, key, field, val);
    }

    QueuedRedis& hsetnx(const StringView &key, const StringView &field, const StringView &val) {
        return command(cmd::hsetnx, key, field, val);
    }

    QueuedRedis& hstrlen(const StringView &key, const StringView &field) {
        return command(cmd::hstrlen, key, field);
    }

    QueuedRedis& hvals(const StringView &key) {
        return command(cmd::hvals, key);
    }

    // SET commands.

    QueuedRedis& sadd(const StringView &key, const StringView &member) {
        return command(cmd::sadd, key, member);
    }

    template <typename Input>
    QueuedRedis& sadd(const StringView &key, Input first, Input last) {
        return command(cmd::sadd_range<Input>, key, first, last);
    }

    QueuedRedis& scard(const StringView &key) {
        return command(cmd::scard, key);
    }

    template <typename Input>
    QueuedRedis& sdiff(Input first, Input last) {
        return command(cmd::sdiff<Input>, first, last);
    }

    template <typename Input>
    QueuedRedis& sdiffstore(const StringView &destination,
                            Input first,
                            Input last) {
        return command(cmd::sdiffstore<Input>, destination, first, last);
    }

    template <typename Input>
    QueuedRedis& sinter(Input first, Input last) {
        return command(cmd::sinter<Input>, first, last);
    }

    template <typename Input>
    QueuedRedis& sinterstore(const StringView &destination,
                                Input first,
                                Input last) {
        return command(cmd::sinterstore<Input>, destination, first, last);
    }

    QueuedRedis& sismember(const StringView &key, const StringView &member) {
        return command(cmd::sismember, key, member);
    }

    QueuedRedis& smembers(const StringView &key) {
        return command(cmd::smembers, key);
    }

    QueuedRedis& smove(const StringView &source,
                        const StringView &destination,
                        const StringView &member) {
        return command(cmd::smove, source, destination, member);
    }

    QueuedRedis& spop(const StringView &key) {
        return command(cmd::spop, key);
    }

    QueuedRedis& spop(const StringView &key, long long count) {
        return command(cmd::spop, key, count);
    }

    QueuedRedis& srandmember(const StringView &key) {
        return command(cmd::srandmember, key);
    }

    QueuedRedis& srandmember(const StringView &key, long long count) {
        return command(cmd::srandmember, key, count);
    }

    QueuedRedis& srem(const StringView &key, const StringView &member) {
        return command(cmd::srem, key, member);
    }

    template <typename Input>
    QueuedRedis& srem(const StringView &key, Input first, Input last) {
        return command(cmd::srem_range<Input>, key, first, last);
    }

    QueuedRedis& sscan(const StringView &key,
                        long long cursor,
                        const StringView &pattern,
                        long long count) {
        return command(cmd::sscan, key, cursor, pattern, count);
    }

    QueuedRedis& sscan(const StringView &key,
                    long long cursor,
                    const StringView &pattern) {
        return sscan(key, cursor, pattern, 10);
    }

    QueuedRedis& sscan(const StringView &key,
                        long long cursor,
                        long long count) {
        return sscan(key, cursor, "*", count);
    }

    QueuedRedis& sscan(const StringView &key,
                        long long cursor) {
        return sscan(key, cursor, "*", 10);
    }

    template <typename Input>
    QueuedRedis& sunion(Input first, Input last) {
        return command(cmd::sunion<Input>, first, last);
    }

    template <typename Input>
    QueuedRedis& sunionstore(const StringView &destination, Input first, Input last) {
        return command(cmd::sunionstore<Input>, destination, first, last);
    }

    // SORTED SET commands.

    // We don't support the INCR option, since you can always use ZINCRBY instead.
    QueuedRedis& zadd(const StringView &key,
                        double score,
                        const StringView &member,
                        bool changed = false,
                        UpdateType type = UpdateType::ALWAYS) {
        return command(cmd::zadd, key, score, member, changed, type);
    }

    template <typename Input>
    QueuedRedis& zadd(const StringView &key,
                        Input first,
                        Input last,
                        bool changed = false,
                        UpdateType type = UpdateType::ALWAYS) {
        return command(cmd::zadd_range<Input>, key, first, last, changed, type);
    }

    QueuedRedis& zcard(const StringView &key) {
        return command(cmd::zcard, key);
    }

    template <typename Interval>
    QueuedRedis& zcount(const StringView &key, const Interval &interval) {
        return command(cmd::zcount<Interval>, key, interval);
    }

    QueuedRedis& zincrby(const StringView &key, double increment, const StringView &member) {
        return command(cmd::zincrby, key, increment, member);
    }

    template <typename Input>
    QueuedRedis& zinterstore(const StringView &destination,
                                Input first,
                                Input last,
                                Aggregation type = Aggregation::SUM) {
        return command(cmd::zinterstore<Input>, destination, first, last, type);
    }

    template <typename Interval>
    QueuedRedis& zlexcount(const StringView &key, const Interval &interval) {
        return command(cmd::zlexcount<Interval>, key, interval);
    }

    QueuedRedis& zrange(const StringView &key, long long start, long long stop) {
        return command(cmd::zrange, key, start, stop);
    }

    template <typename Interval>
    QueuedRedis& zrangebylex(const StringView &key,
                                const Interval &interval,
                                const LimitOptions &opts) {
        return command(cmd::zrangebylex<Interval>, key, interval, opts);
    }

    template <typename Interval>
    QueuedRedis& zrangebylex(const StringView &key, const Interval &interval) {
        return zrangebylex(key, interval, {});
    }

    template <typename Interval>
    QueuedRedis& zrangebyscore(const StringView &key,
                        const Interval &interval,
                        const LimitOptions &opts) {
        return command(cmd::zrangebyscore<Interval>, key, interval, opts);
    }

    template <typename Interval>
    QueuedRedis& zrangebyscore(const StringView &key, const Interval &interval) {
        return zrangebyscore(key, interval, {});
    }

    QueuedRedis& zrank(const StringView &key, const StringView &member) {
        return command(cmd::zrank, key, member);
    }

    QueuedRedis& zrem(const StringView &key, const StringView &member) {
        return command(cmd::zrem, key, member);
    }

    template <typename Input>
    QueuedRedis& zrem(const StringView &key, Input first, Input last) {
        return command(cmd::zrem_range<Input>, key, first, last);
    }

    template <typename Interval>
    QueuedRedis& zremrangebylex(const StringView &key, const Interval &interval) {
        return command(cmd::zremrangebylex<Interval>, key, interval);
    }

    QueuedRedis& zremrangebyrank(const StringView &key, long long start, long long stop) {
        return command(cmd::zremrangebyrank, key, start, stop);
    }

    template <typename Interval>
    QueuedRedis& zremrangebyscore(const StringView &key, const Interval &interval) {
        return command(cmd::zremrangebyscore<Interval>, key, interval);
    }

    QueuedRedis& zrevrange(const StringView &key, long long start, long long stop) {
        return command(cmd::zrevrange, key, start, stop);
    }

    template <typename Interval>
    QueuedRedis& zrevrangebylex(const StringView &key,
                                const Interval &interval,
                                const LimitOptions &opts) {
        return command(cmd::zrevrangebylex<Interval>, key, interval, opts);
    }

    template <typename Interval>
    QueuedRedis& zrevrangebylex(const StringView &key, const Interval &interval) {
        return zrevrangebylex(key, interval, {});
    }

    template <typename Interval>
    QueuedRedis& zrevrangebyscore(const StringView &key,
                                    const Interval &interval,
                                    const LimitOptions &opts) {
        return command(cmd::zrevrangebyscore<Interval>, key, interval, opts);
    }

    template <typename Interval>
    QueuedRedis& zrevrangebyscore(const StringView &key, const Interval &interval) {
        return zrevrangebyscore(key, interval, {});
    }

    QueuedRedis& zrevrank(const StringView &key, const StringView &member) {
        return command(cmd::zrevrank, key, member);
    }

    QueuedRedis& zscan(const StringView &key,
                        long long cursor,
                        const StringView &pattern,
                        long long count) {
        return command(cmd::zscan, key, cursor, pattern, count);
    }

    QueuedRedis& zscan(const StringView &key,
                        long long cursor,
                        const StringView &pattern) {
        return zscan(key, cursor, pattern, 10);
    }

    QueuedRedis& zscan(const StringView &key,
                        long long cursor,
                        long long count) {
        return zscan(key, cursor, "*", count);
    }

    QueuedRedis& zscan(const StringView &key,
                        long long cursor) {
        return zscan(key, cursor, "*", 10);
    }

    QueuedRedis& zscore(const StringView &key, const StringView &member) {
        return command(cmd::zscore, key, member);
    }

    template <typename Input>
    QueuedRedis& zunionstore(const StringView &destination,
                                Input first,
                                Input last,
                                Aggregation type = Aggregation::SUM) {
        return command(cmd::zunionstore<Input>, destination, first, last, type);
    }

    // HYPERLOGLOG commands.

    QueuedRedis& pfadd(const StringView &key, const StringView &element) {
        return command(cmd::pfadd, key, element);
    }

    template <typename Input>
    QueuedRedis& pfadd(const StringView &key, Input first, Input last) {
        return command(cmd::pfadd_range<Input>, key, first, last);
    }

    QueuedRedis& pfcount(const StringView &key) {
        return command(cmd::pfcount, key);
    }

    template <typename Input>
    QueuedRedis& pfcount(Input first, Input last) {
        return command(cmd::pfcount_range<Input>, first, last);
    }

    template <typename Input>
    QueuedRedis& pfmerge(const StringView &destination, Input first, Input last) {
        return command(cmd::pfmerge<Input>, destination, first, last);
    }

    // GEO commands.

    QueuedRedis& geoadd(const StringView &key,
                        const std::tuple<double, double, std::string> &member) {
        return command(cmd::geoadd, key, member);
    }

    template <typename Input>
    QueuedRedis& geoadd(const StringView &key,
                        Input first,
                        Input last) {
        return command(cmd::geoadd_range<Input>, key, first, last);
    }

    QueuedRedis& geodist(const StringView &key,
                            const StringView &member1,
                            const StringView &member2,
                            GeoUnit unit = GeoUnit::M) {
        return command(cmd::geodist, key, member1, member2, unit);
    }

    template <typename Input>
    QueuedRedis& geohash(const StringView &key, Input first, Input last) {
        return command(cmd::geohash_range<Input>, key, first, last);
    }

    template <typename Input>
    QueuedRedis& geopos(const StringView &key, Input first, Input last) {
        return command(cmd::geopos_range<Input>, key, first, last);
    }

    // TODO:
    // 1. since we have different overloads for georadius and georadius-store,
    //    we might use the GEORADIUS_RO command in the future.
    // 2. there're too many parameters for this method, we might refactor it.
    QueuedRedis& georadius(const StringView &key,
                            const std::pair<double, double> &loc,
                            double radius,
                            GeoUnit unit,
                            const StringView &destination,
                            bool store_dist,
                            long long count) {
        return command(cmd::georadius, key, loc, radius, unit, destination, store_dist, count);
    }

    QueuedRedis& georadius(const StringView &key,
                            const std::pair<double, double> &loc,
                            double radius,
                            GeoUnit unit,
                            long long count,
                            bool asc) {
        return command(cmd::georadius, key, loc, radius, unit, count, asc);
    }

    QueuedRedis& georadiusbymember(const StringView &key,
                                    const StringView &member,
                                    double radius,
                                    GeoUnit unit,
                                    const StringView &destination,
                                    bool store_dist,
                                    long long count) {
        return command(cmd::georadiusbymember,
                        key,
                        member,
                        radius,
                        unit,
                        destination,
                        store_dist,
                        count);
    }

    QueuedRedis& georadiusbymember(const StringView &key,
                                    const StringView &member,
                                    double radius,
                                    GeoUnit unit,
                                    long long count,
                                    bool asc) {
        return command(cmd::georadiusbymember, key, member, radius, unit, count, asc);
    }

    // SCRIPTING commands.

    template <typename Result>
    QueuedRedis& eval(const StringView &script,
                        std::initializer_list<StringView> keys,
                        std::initializer_list<StringView> args) {
        return command(cmd::eval, script, keys, args);
    }

    template <typename Result>
    QueuedRedis& evalsha(const StringView &script,
                            std::initializer_list<StringView> keys,
                            std::initializer_list<StringView> args) {
        return command(cmd::evalsha, script, keys, args);
    }

    QueuedRedis& script_exists(const StringView &sha) {
        return command(cmd::script_exists, sha);
    }

    template <typename Input>
    QueuedRedis& script_exists(Input first, Input last) {
        return command(cmd::script_exists_range<Input>, first, last);
    }

    QueuedRedis& script_flush() {
        return command(cmd::script_flush);
    }

    QueuedRedis& script_kill() {
        return command(cmd::script_kill);
    }

    QueuedRedis& script_load(const StringView &script) {
        return command(cmd::script_load, script);
    }

    // PUBSUB commands.

    QueuedRedis& publish(const StringView &channel, const StringView &message) {
        return command(cmd::publish, channel, message);
    }

private:
    friend class Redis;

    template <typename ...Args>
    QueuedRedis(Connection connection, Args &&...args);

    void _sanity_check() const;

    void _reset();

    Connection _connection;

    Impl _impl;

    std::size_t _cmd_num = 0;

    bool _valid = true;
};

class QueuedReplies {
public:
    template <typename Result>
    Result pop();

    template <typename Output>
    void pop(Output output);

private:
    template <typename Impl>
    friend class QueuedRedis;

    explicit QueuedReplies(std::deque<ReplyUPtr> replies) : _replies(std::move(replies)) {}

    std::deque<ReplyUPtr> _replies;
};

}

}

#include "queued_redis.hpp"

#endif // end SEWENEW_REDISPLUSPLUS_QUEUED_REDIS_H
