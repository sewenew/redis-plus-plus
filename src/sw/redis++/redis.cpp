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

#include "redis.h"
#include "redis.hpp"
#include <hiredis/hiredis.h>
#include "command.h"
#include "exceptions.h"
#include "pipeline.h"

namespace sw {

namespace redis {

Redis::ConnectionPoolGuard::ConnectionPoolGuard(ConnectionPool &pool, Connection &connection) :
                                                _pool(pool),
                                                _connection(connection) {}

Redis::ConnectionPoolGuard::~ConnectionPoolGuard() {
    _pool.release(std::move(_connection));
}

Pipeline Redis::pipeline() {
    return Pipeline(_pool);
}

void Redis::auth(const StringView &password) {
    auto reply = command(cmd::auth, password);

    if (!reply::status_ok(*reply)) {
        throw RException("Invalid status reply: " + reply::to_status(*reply));
    }
}

std::string Redis::info() {
    auto reply = command(cmd::info);

    return reply::to_string(*reply);
}

std::string Redis::ping() {
    auto reply = command<void (*)(Connection &)>(cmd::ping);

    return reply::to_status(*reply);
}

std::string Redis::ping(const StringView &msg) {
    auto reply = command<void (*)(Connection &, const StringView &)>(cmd::ping, msg);

    return reply::to_string(*reply);
}

// KEY commands.

long long Redis::del(const StringView &key) {
    auto reply = command(cmd::del, key);

    return reply::to_integer(*reply);
}

OptionalString Redis::dump(const StringView &key) {
    auto reply = command(cmd::dump, key);

    return reply::to_optional_string(*reply);
}

long long Redis::exists(const StringView &key) {
    auto reply = command(cmd::exists, key);

    return reply::to_integer(*reply);
}

bool Redis::expire(const StringView &key, const std::chrono::seconds &timeout) {
    auto reply = command(cmd::expire, key, timeout);

    return reply::to_bool(*reply);
}

// STRING commands.

long long Redis::append(const StringView &key, const StringView &val) {
    auto reply = command(cmd::append, key, val);

    return reply::to_integer(*reply);
}

long long Redis::bitcount(const StringView &key, long long start, long long end) {
    auto reply = command(cmd::bitcount, key, start, end);

    return reply::to_integer(*reply);
}

long long Redis::bitpos(const StringView &key,
                            long long bit,
                            long long start,
                            long long end) {
    auto reply = command(cmd::bitpos, key, bit, start, end);

    return reply::to_integer(*reply);
}

long long Redis::decr(const StringView &key) {
    auto reply = command(cmd::decr, key);

    return reply::to_integer(*reply);
}

long long Redis::decrby(const StringView &key, long long decrement) {
    auto reply = command(cmd::decrby, key, decrement);

    return reply::to_integer(*reply);
}

OptionalString Redis::get(const StringView &key) {
    auto reply = command(cmd::get, key);

    return reply::to_optional_string(*reply);
}

long long Redis::getbit(const StringView &key, long long offset) {
    auto reply = command(cmd::getbit, key, offset);

    return reply::to_integer(*reply);
}

std::string Redis::getrange(const StringView &key, long long start, long long end) {
    auto reply = command(cmd::getrange, key, start, end);

    return reply::to_string(*reply);
}

OptionalString Redis::getset(const StringView &key, const StringView &val) {
    auto reply = command(cmd::getset, key, val);

    return reply::to_optional_string(*reply);
}

long long Redis::incr(const StringView &key) {
    auto reply = command(cmd::incr, key);

    return reply::to_integer(*reply);
}

long long Redis::incrby(const StringView &key, long long increment) {
    auto reply = command(cmd::incrby, key, increment);

    return reply::to_integer(*reply);
}

double Redis::incrbyfloat(const StringView &key, double increment) {
    auto reply = command(cmd::incrbyfloat, key, increment);

    return reply::to_double(*reply);
}

void Redis::psetex(const StringView &key,
                        const std::chrono::milliseconds &ttl,
                        const StringView &val) {
    if (ttl <= std::chrono::milliseconds(0)) {
        throw RException("TTL must be positive.");
    }

    auto reply = command(cmd::psetex, key, ttl, val);

    if (!reply::status_ok(*reply)) {
        throw RException("Invalid status reply: " + reply::to_status(*reply));
    }
}

bool Redis::set(const StringView &key,
                    const StringView &val,
                    const std::chrono::milliseconds &ttl,
                    UpdateType type) {
    auto reply = command(cmd::set, key, val, ttl, type);

    if (reply::is_nil(*reply)) {
        // Failed to set.
        return false;
    }

    assert(reply::is_status(*reply));

    if (!reply::status_ok(*reply)) {
        throw RException("Invalid status reply: " + reply::to_status(*reply));
    }

    return true;
}

long long Redis::setbit(const StringView &key, long long offset, long long value) {
    auto reply = command(cmd::setbit, key, offset, value);

    return reply::to_integer(*reply);
}

void Redis::setex(const StringView &key,
                    const std::chrono::seconds &ttl,
                    const StringView &val) {
    if (ttl <= std::chrono::seconds(0)) {
        throw RException("TTL must be positive.");
    }

    auto reply = command(cmd::setex, key, ttl, val);

    if (!reply::status_ok(*reply)) {
        throw RException("Invalid status reply: " + reply::to_status(*reply));
    }
}

bool Redis::setnx(const StringView &key, const StringView &val) {
    auto reply = command(cmd::setnx, key, val);

    return reply::to_bool(*reply);
}

long long Redis::setrange(const StringView &key, long long offset, const StringView &val) {
    auto reply = command(cmd::setrange, key, offset, val);

    return reply::to_integer(*reply);
}

long long Redis::strlen(const StringView &key) {
    auto reply = command(cmd::strlen, key);

    return reply::to_integer(*reply);
}

// LIST commands.

OptionalString Redis::brpoplpush(const StringView &source,
                                    const StringView &destination,
                                    const std::chrono::seconds &timeout) {
    auto reply = command(cmd::brpoplpush, source, destination, timeout);

    return reply::to_optional_string(*reply);
}

OptionalString Redis::lindex(const StringView &key, long long index) {
    auto reply = command(cmd::lindex, key, index);

    return reply::to_optional_string(*reply);
}

long long Redis::linsert(const StringView &key,
                            InsertPosition position,
                            const StringView &pivot,
                            const StringView &val) {
    auto reply = command(cmd::linsert, key, position, pivot, val);

    return reply::to_integer(*reply);
}

long long Redis::llen(const StringView &key) {
    auto reply = command(cmd::llen, key);

    return reply::to_integer(*reply);
}

OptionalString Redis::lpop(const StringView &key) {
    auto reply = command(cmd::lpop, key);

    return reply::to_optional_string(*reply);
}

long long Redis::lpush(const StringView &key, const StringView &val) {
    auto reply = command(cmd::lpush, key, val);

    return reply::to_integer(*reply);
}

long long Redis::lpushx(const StringView &key, const StringView &val) {
    auto reply = command(cmd::lpushx, key, val);

    return reply::to_integer(*reply);
}

long long Redis::lrem(const StringView &key, long long count, const StringView &val) {
    auto reply = command(cmd::lrem, key, count, val);

    return reply::to_integer(*reply);
}

void Redis::lset(const StringView &key, long long index, const StringView &val) {
    auto reply = command(cmd::lset, key, index, val);

    if (!reply::status_ok(*reply)) {
        throw RException("Invalid status reply: " + reply::to_status(*reply));
    }
}

void Redis::ltrim(const StringView &key, long long start, long long stop) {
    auto reply = command(cmd::ltrim, key, start, stop);

    if (!reply::status_ok(*reply)) {
        throw RException("Invalid status reply: " + reply::to_status(*reply));
    }
}

OptionalString Redis::rpop(const StringView &key) {
    auto reply = command(cmd::rpop, key);

    return reply::to_optional_string(*reply);
}

OptionalString Redis::rpoplpush(const StringView &source, const StringView &destination) {
    auto reply = command(cmd::rpoplpush, source, destination);

    return reply::to_optional_string(*reply);
}

long long Redis::rpush(const StringView &key, const StringView &val) {
    auto reply = command(cmd::rpush, key, val);

    return reply::to_integer(*reply);
}

long long Redis::rpushx(const StringView &key, const StringView &val) {
    auto reply = command(cmd::rpushx, key, val);

    return reply::to_integer(*reply);
}

long long Redis::hdel(const StringView &key, const StringView &field) {
    auto reply = command(cmd::hdel, key, field);

    return reply::to_integer(*reply);
}

bool Redis::hexists(const StringView &key, const StringView &field) {
    auto reply = command(cmd::hexists, key, field);

    return reply::to_bool(*reply);
}

OptionalString Redis::hget(const StringView &key, const StringView &field) {
    auto reply = command(cmd::hget, key, field);

    return reply::to_optional_string(*reply);
}

long long Redis::hincrby(const StringView &key, const StringView &field, long long increment) {
    auto reply = command(cmd::hincrby, key, field, increment);

    return reply::to_integer(*reply);
}

double Redis::hincrbyfloat(const StringView &key, const StringView &field, double increment) {
    auto reply = command(cmd::hincrby, key, field, increment);

    return reply::to_double(*reply);
}

long long Redis::hlen(const StringView &key) {
    auto reply = command(cmd::hlen, key);

    return reply::to_integer(*reply);
}

bool Redis::hset(const StringView &key, const StringView &field, const StringView &val) {
    auto reply = command(cmd::hset, key, field, val);

    return reply::to_bool(*reply);
}

bool Redis::hsetnx(const StringView &key, const StringView &field, const StringView &val) {
    auto reply = command(cmd::hsetnx, key, field, val);

    return reply::to_bool(*reply);
}

long long Redis::hstrlen(const StringView &key, const StringView &field) {
    auto reply = command(cmd::hstrlen, key, field);

    return reply::to_integer(*reply);
}

// SET commands.

long long Redis::sadd(const StringView &key, const StringView &member) {
    auto reply = command(cmd::sadd, key, member);

    return reply::to_integer(*reply);
}

long long Redis::scard(const StringView &key) {
    auto reply = command(cmd::scard, key);

    return reply::to_integer(*reply);
}

bool Redis::sismember(const StringView &key, const StringView &member) {
    auto reply = command(cmd::sismember, key, member);

    return reply::to_bool(*reply);
}

bool Redis::smove(const StringView &source,
                    const StringView &destination,
                    const StringView &member) {
    auto reply = command(cmd::smove, source, destination, member);

    return reply::to_bool(*reply);
}

OptionalString Redis::spop(const StringView &key) {
    auto reply = command(cmd::spop, key);

    return reply::to_optional_string(*reply);
}

OptionalString Redis::srandmember(const StringView &key) {
    auto reply = command(cmd::srandmember, key);

    return reply::to_optional_string(*reply);
}

long long Redis::srem(const StringView &key, const StringView &member) {
    auto reply = command(cmd::srem, key, member);

    return reply::to_integer(*reply);
}

// SORTED SET commands.

long long Redis::zadd(const StringView &key,
                        double score,
                        const StringView &member,
                        bool changed,
                        UpdateType type) {
    auto reply = command(cmd::zadd, key, score, member, changed, type);

    return reply::to_integer(*reply);
}

long long Redis::zcard(const StringView &key) {
    auto reply = command(cmd::zcard, key);

    return reply::to_integer(*reply);
}

double Redis::zincrby(const StringView &key, double increment, const StringView &member) {
    auto reply = command(cmd::zincrby, key, increment, member);

    return reply::to_double(*reply);
}

OptionalLongLong Redis::zrank(const StringView &key, const StringView &member) {
    auto reply = command(cmd::zrank, key, member);

    return reply::to_optional_integer(*reply);
}

long long Redis::zrem(const StringView &key, const StringView &member) {
    auto reply = command(cmd::zrem, key, member);

    return reply::to_integer(*reply);
}

long long Redis::zremrangebyrank(const StringView &key, long long start, long long stop) {
    auto reply = command(cmd::zremrangebyrank, key, start, stop);

    return reply::to_integer(*reply);
}

OptionalLongLong Redis::zrevrank(const StringView &key, const StringView &member) {
    auto reply = command(cmd::zrevrank, key, member);

    return reply::to_optional_integer(*reply);
}

OptionalDouble Redis::zscore(const StringView &key, const StringView &member) {
    auto reply = command(cmd::zscore, key, member);

    return reply::to_optional_double(*reply);
}

// HYPERLOGLOG commands.

bool Redis::pfadd(const StringView &key, const StringView &element) {
    auto reply = command(cmd::pfadd, key, element);

    return reply::to_bool(*reply);
}

long long Redis::pfcount(const StringView &key) {
    auto reply = command(cmd::pfcount, key);

    return reply::to_integer(*reply);
}

// GEO commands.

long long Redis::geoadd(const StringView &key,
                        const std::tuple<double, double, std::string> &member) {
    auto reply = command(cmd::geoadd, key, member);

    return reply::to_integer(*reply);
}

OptionalDouble Redis::geodist(const StringView &key,
                                const StringView &member1,
                                const StringView &member2,
                                GeoUnit unit) {
    auto reply = command(cmd::geodist, key, member1, member2, unit);

    return reply::to_optional_double(*reply);
}

OptionalString Redis::geohash(const StringView &key, const StringView &member) {
    auto reply = command(cmd::geohash, key, member);

    return reply::to_optional_string(*reply);
}

}

}
