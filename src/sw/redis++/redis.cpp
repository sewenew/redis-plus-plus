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
#include <hiredis/hiredis.h>
#include "command.h"
#include "exceptions.h"
#include "pipeline.h"
#include "r_list.h"
#include "r_hash.h"
#include "r_set.h"
#include "r_sorted_set.h"
#include "r_hyperloglog.h"

namespace sw {

namespace redis {

Redis::ConnectionPoolGuard::ConnectionPoolGuard(ConnectionPool &pool, Connection &connection) :
                                                _pool(pool),
                                                _connection(connection) {}

Redis::ConnectionPoolGuard::~ConnectionPoolGuard() {
    _pool.release(std::move(_connection));
}

RList Redis::list(const std::string &key) {
    return {key, *this};
}

RHash Redis::hash(const std::string &key) {
    return {key, *this};
}

RSet Redis::set(const std::string &key) {
    return {key, *this};
}

RSortedSet Redis::sorted_set(const std::string &key) {
    return {key, *this};
}

RHyperLogLog Redis::hyperloglog(const std::string &key) {
    return {key, *this};
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

}

}
