/**************************************************************************
   Copyright (c) 2021 sewenew

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

#include "async_redis.h"
#include "reply.h"

namespace {

struct SetResultParser {
    bool operator()(redisReply &reply) const {
        sw::redis::reply::rewrite_set_reply(reply);
        return sw::redis::reply::parse<bool>(reply);
    }
};

}

namespace sw {

namespace redis {

AsyncRedis::AsyncRedis(const ConnectionOptions &opts,
        const ConnectionPoolOptions &pool_opts,
        const EventLoopSPtr &loop) : _loop(loop) {
    if (!_loop) {
        _loop = std::make_shared<EventLoop>();
    }

    _pool = std::make_shared<AsyncConnectionPool>(_loop, pool_opts, opts);
}

Future<std::string> AsyncRedis::echo(const StringView &msg) {
    return _command<std::string>("ECHO %b", msg.data(), msg.size());
}

Future<std::string> AsyncRedis::ping() {
    return _command<std::string>("PING");
}

Future<std::string> AsyncRedis::ping(const StringView &msg) {
    return _command<std::string>("PING %b", msg.data(), msg.size());
}

Future<long long> AsyncRedis::del(const StringView &key) {
    return _command<long long>("DEL %b", key.data(), key.size());
}

Future<long long> AsyncRedis::exists(const StringView &key) {
    return _command<long long>("EXISTS %b", key.data(), key.size());
}

Future<bool> AsyncRedis::expire(const StringView &key, const std::chrono::seconds &timeout) {
    return _command<bool>("EXPIRE %b %lld", key.data(), key.size(), timeout.count());
}

Future<bool> AsyncRedis::expireat(const StringView &key,
                const std::chrono::time_point<std::chrono::system_clock,
                                                std::chrono::seconds> &tp) {
    return _command<bool>("EXPIREAT %b %lld",
                            key.data(), key.size(),
                            tp.time_since_epoch().count());
}

Future<bool> AsyncRedis::pexpire(const StringView &key, const std::chrono::milliseconds &timeout) {
    return _command<bool>("PEXPIRE %b %lld",
                            key.data(), key.size(),
                            timeout.count());
}

Future<bool> AsyncRedis::pexpireat(const StringView &key,
                const std::chrono::time_point<std::chrono::system_clock,
                                                std::chrono::milliseconds> &tp) {
    return _command<bool>("PEXPIREAT %b %lld",
                            key.data(), key.size(),
                            tp.time_since_epoch().count());
}

Future<long long> AsyncRedis::pttl(const StringView &key) {
    return _command<long long>("PTTL %b", key.data(), key.size());
}

Future<void> AsyncRedis::rename(const StringView &key, const StringView &newkey) {
    return _command<void>("RENAME %b %b",
                            key.data(), key.size(),
                            newkey.data(), newkey.size());
}

Future<bool> AsyncRedis::renamenx(const StringView &key, const StringView &newkey) {
    return _command<bool>("RENAMENX %b %b",
                            key.data(), key.size(),
                            newkey.data(), newkey.size());
}

Future<long long> AsyncRedis::ttl(const StringView &key) {
    return _command<long long>("TTL %b", key.data(), key.size());
}

Future<long long> AsyncRedis::unlink(const StringView &key) {
    return _command<long long>("UNLINK %b", key.data(), key.size());
}

Future<OptionalString> AsyncRedis::get(const StringView &key) {
    return _command<OptionalString>("GET %b", key.data(), key.size());
}

Future<long long> AsyncRedis::incr(const StringView &key) {
    return _command<long long>("INCR %b", key.data(), key.size());
}

Future<long long> AsyncRedis::incrby(const StringView &key, long long increment) {
    return _command<long long>("INCRBY %b %lld",
                                key.data(), key.size(),
                                increment);
}

Future<double> AsyncRedis::incrbyfloat(const StringView &key, double increment) {
    return _command<double>("INCRBYFLOAT %b %f",
                                key.data(), key.size(),
                                increment);
}

Future<bool> AsyncRedis::set(const StringView &key,
        const StringView &val,
        const std::chrono::milliseconds &ttl,
        UpdateType type) {
    CmdArgs args;
    args << "SET" << key << val;

    if (ttl > std::chrono::milliseconds(0)) {
        args << "PX" << ttl.count();
    }

    cmd::detail::set_update_type(args, type);

    return _command_with_parser<bool, SetResultParser>(args);
}

Future<long long> AsyncRedis::strlen(const StringView &key) {
    return _command<long long>("STRLEN %b", key.data(), key.size());
}

Future<OptionalStringPair> AsyncRedis::blpop(const StringView &key,
        const std::chrono::seconds &timeout) {
    return _command<OptionalStringPair>("BLPOP %b %lld",
            key.data(), key.size(),
            timeout.count());
}

Future<OptionalStringPair> AsyncRedis::brpop(const StringView &key,
                            const std::chrono::seconds &timeout) {
    return _command<OptionalStringPair>("BRPOP %b %lld",
            key.data(), key.size(), timeout.count());
}

Future<OptionalString> AsyncRedis::brpoplpush(const StringView &source,
                            const StringView &destination,
                            const std::chrono::seconds &timeout) {
    return _command<OptionalString>("BRPOPLPUSH %b %b %lld",
            source.data(), source.size(),
            destination.data(), destination.size(),
            timeout.count());
}

Future<long long> AsyncRedis::llen(const StringView &key) {
    return _command<long long>("LLEN %b", key.data(), key.size());
}

Future<OptionalString> AsyncRedis::lpop(const StringView &key) {
    return _command<OptionalString>("LPOP %b", key.data(), key.size());
}

Future<long long> AsyncRedis::lpush(const StringView &key, const StringView &val) {
    return _command<long long>("LPUSH %b %b",
            key.data(), key.size(),
            val.data(), val.size());
}

Future<long long> AsyncRedis::lrem(const StringView &key, long long count, const StringView &val) {
    return _command<long long>("LREM %b %lld %b",
            key.data(), key.size(),
            count,
            val.data(), val.size());
}

Future<void> AsyncRedis::ltrim(const StringView &key, long long start, long long stop) {
    return _command<void>("LTRIM %b %lld %lld",
            key.data(), key.size(),
            start, stop);
}

Future<OptionalString> AsyncRedis::rpop(const StringView &key) {
    return _command<OptionalString>("RPOP %b", key.data(), key.size());
}

Future<OptionalString> AsyncRedis::rpoplpush(const StringView &source, const StringView &destination) {
    return _command<OptionalString>("RPOPLPUSH %b %b",
            source.data(), source.size(),
            destination.data(), destination.size());
}

Future<long long> AsyncRedis::rpush(const StringView &key, const StringView &val) {
    return _command<long long>("RPUSH %b %b",
            key.data(), key.size(),
            val.data(), val.size());
}

Future<long long> AsyncRedis::hdel(const StringView &key, const StringView &field) {
    return _command<long long>("HDEL %b %b",
            key.data(), key.size(),
            field.data(), field.size());
}

Future<bool> AsyncRedis::hexists(const StringView &key, const StringView &field) {
    return _command<bool>("HEXISTS %b %b",
            key.data(), key.size(),
            field.data(), field.size());
}

Future<OptionalString> AsyncRedis::hget(const StringView &key, const StringView &field) {
    return _command<OptionalString>("HGET %b %b",
            key.data(), key.size(),
            field.data(), field.size());
}

Future<long long> AsyncRedis::hincrby(const StringView &key, const StringView &field, long long increment) {
    return _command<long long>("HINCRBY %b %b %lld",
            key.data(), key.size(),
            field.data(), field.size(),
            increment);
}

Future<double> AsyncRedis::hincrbyfloat(const StringView &key, const StringView &field, double increment) {
    return _command<double>("HINCRBYFLOAT %b %b %f",
            key.data(), key.size(),
            field.data(), field.size(),
            increment);
}

Future<long long> AsyncRedis::hlen(const StringView &key) {
    return _command<long long>("HLEN %b", key.data(), key.size());
}

Future<bool> AsyncRedis::hset(const StringView &key, const StringView &field, const StringView &val) {
    return _command<bool>("HSET %b %b %b",
            key.data(), key.size(),
            field.data(), field.size(),
            val.data(), val.size());
}

Future<long long> AsyncRedis::sadd(const StringView &key, const StringView &member) {
    return _command<long long>("SADD %b %b",
            key.data(), key.size(),
            member.data(), member.size());
}

Future<long long> AsyncRedis::scard(const StringView &key) {
    return _command<long long>("SCARD %b", key.data(), key.size());
}

Future<bool> AsyncRedis::sismember(const StringView &key, const StringView &member) {
    return _command<bool>("SISMEMBER %b %b",
            key.data(), key.size(),
            member.data(), member.size());
}

Future<OptionalString> AsyncRedis::spop(const StringView &key) {
    return _command<OptionalString>("SPOP %b", key.data(), key.size());
}

Future<long long> AsyncRedis::srem(const StringView &key, const StringView &member) {
    return _command<long long>("SREM %b %b",
            key.data(), key.size(),
            member.data(), member.size());
}

auto AsyncRedis::bzpopmax(const StringView &key,
                const std::chrono::seconds &timeout)
    -> Future<Optional<std::tuple<std::string, std::string, double>>> {
    return _command<Optional<std::tuple<std::string, std::string, double>>>(
            "BZPOPMAX %b %lld",
            key.data(), key.size(),
            timeout.count());
}

auto AsyncRedis::bzpopmin(const StringView &key,
                const std::chrono::seconds &timeout)
    -> Future<Optional<std::tuple<std::string, std::string, double>>> {
    return _command<Optional<std::tuple<std::string, std::string, double>>>(
            "BZPOPMIN %b %lld",
            key.data(), key.size(),
            timeout.count());
}

Future<long long> AsyncRedis::zadd(const StringView &key,
                const StringView &member,
                double score,
                UpdateType type,
                bool changed) {
    CmdArgs args;
    args << "ZADD" << key;

    cmd::detail::set_update_type(args, type);

    if (changed) {
        args << "CH";
    }

    args << score << member;

    return _command<long long>(args);
}

Future<long long> AsyncRedis::zcard(const StringView &key) {
    return _command<long long>("ZCARD %b", key.data(), key.size());
}

Future<double> AsyncRedis::zincrby(const StringView &key, double increment, const StringView &member) {
    return _command<double>("ZINCRBY %b %f %b",
            key.data(), key.size(),
            increment,
            member.data(), member.size());
}

Future<Optional<std::pair<std::string, double>>> AsyncRedis::zpopmax(const StringView &key) {
    return _command<Optional<std::pair<std::string, double>>>("ZPOPMAX %b",
            key.data(), key.size());
}

Future<Optional<std::pair<std::string, double>>> AsyncRedis::zpopmin(const StringView &key) {
    return _command<Optional<std::pair<std::string, double>>>("ZPOPMIN %b",
            key.data(), key.size());
}

Future<OptionalLongLong> AsyncRedis::rank(const StringView &key, const StringView &member) {
    return _command<OptionalLongLong>("RANK %b %b",
            key.data(), key.size(),
            member.data(), member.size());
}

Future<long long> AsyncRedis::zrem(const StringView &key, const StringView &member) {
    return _command<long long>("ZREM %b %b",
            key.data(), key.size(),
            member.data(), member.size());
}

Future<long long> AsyncRedis::zremrangebyrank(const StringView &key, long long start, long long stop) {
    return _command<long long>("ZREMRANGEBYRANK %b %lld %lld",
            key.data(), key.size(),
            start, stop);
}

Future<OptionalLongLong> AsyncRedis::zrevrank(const StringView &key, const StringView &member) {
    return _command<OptionalLongLong>("ZREVRANK %b %b",
            key.data(), key.size(),
            member.data(), member.size());
}

Future<OptionalDouble> AsyncRedis::zscore(const StringView &key, const StringView &member) {
    return _command<OptionalDouble>("ZSCORE %b %b",
            key.data(), key.size(),
            member.data(), member.size());
}

}

}
