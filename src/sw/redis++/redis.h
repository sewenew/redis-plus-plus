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

#ifndef SEWENEW_REDISPLUSPLUS_REDIS_H
#define SEWENEW_REDISPLUSPLUS_REDIS_H

#include <string>
#include "connection_pool.h"
#include "reply.h"
#include "command_options.h"

namespace sw {

namespace redis {

class StringView;
class Pipeline;

class Redis {
public:
    Redis(const ConnectionPoolOptions &pool_opts,
            const ConnectionOptions &connection_opts) : _pool(pool_opts, connection_opts) {}

    Pipeline pipeline();

    template <typename Cmd, typename ...Args>
    ReplyUPtr command(Cmd cmd, Args &&...args);

    void auth(const StringView &password);

    std::string info();

    std::string ping();

    std::string ping(const StringView &msg);

    // STRING commands.

    long long append(const StringView &key, const StringView &str);

    long long bitcount(const StringView &key, long long start = 0, long long end = -1);

    template <typename Input>
    long long bitop(BitOp op, const StringView &destination, Input first, Input last);

    long long bitpos(const StringView &key,
                        long long bit,
                        long long start = 0,
                        long long end = -1);

    long long decr(const StringView &key);

    long long decrby(const StringView &key, long long decrement);

    OptionalString get(const StringView &key);

    long long getbit(const StringView &key, long long offset);

    std::string getrange(const StringView &key, long long start, long long end);

    OptionalString getset(const StringView &key, const StringView &val);

    long long incr(const StringView &key);

    long long incrby(const StringView &key, long long increment);

    double incrbyfloat(const StringView &key, double increment);

    template <typename Input, typename Output>
    void mget(Input first, Input last, Output output);

    template <typename Input>
    void mset(Input first, Input last);

    template <typename Input>
    bool msetnx(Input first, Input last);

    void psetex(const StringView &key,
                const std::chrono::milliseconds &ttl,
                const StringView &val);

    bool set(const StringView &key,
                const StringView &val,
                const std::chrono::milliseconds &ttl = std::chrono::milliseconds(0),
                UpdateType type = UpdateType::ALWAYS);

    long long setbit(const StringView &key, long long offset, long long value);

    bool setnx(const StringView &key, const StringView &val);

    void setex(const StringView &key,
                const std::chrono::seconds &ttl,
                const StringView &val);

    long long setrange(const StringView &key, long long offset, const StringView &val);

    long long strlen(const StringView &key);

    // LIST commands.

    template <typename Input>
    OptionalStringPair blpop(Input first,
                                Input last,
                                const std::chrono::seconds &timeout = std::chrono::seconds{0});

    template <typename Input>
    OptionalStringPair brpop(Input first,
                                Input last,
                                const std::chrono::seconds &timeout = std::chrono::seconds{0});

    OptionalString brpoplpush(const StringView &source,
                                const StringView &destination,
                                const std::chrono::seconds &timeout = std::chrono::seconds{0});

    OptionalString lindex(const StringView &key, long long index);

    long long linsert(const StringView &key,
                        InsertPosition position,
                        const StringView &pivot,
                        const StringView &val);

    long long llen(const StringView &key);

    OptionalString lpop(const StringView &key);

    long long lpush(const StringView &key, const StringView &val);

    template <typename Iter>
    long long lpush(const StringView &key, Iter first, Iter last);

    long long lpushx(const StringView &key, const StringView &val);

    template <typename Iter>
    void lrange(const StringView &key, long long start, long long stop, Iter output);

    long long lrem(const StringView &key, long long count, const StringView &val);

    void lset(const StringView &key, long long index, const StringView &val);

    void ltrim(const StringView &key, long long start, long long stop);

    OptionalString rpop(const StringView &key);

    OptionalString rpoplpush(const StringView &source, const StringView &destination);

    long long rpush(const StringView &key, const StringView &val);

    template <typename Iter>
    long long rpush(const StringView &key, Iter first, Iter last);

    long long rpushx(const StringView &key, const StringView &val);

    // HASH commands.

    long long hdel(const StringView &key, const StringView &field);

    template <typename Iter>
    long long hdel(const StringView &key, Iter first, Iter last);

    bool hexists(const StringView &key, const StringView &field);

    OptionalString hget(const StringView &key, const StringView &field);

    template <typename Iter>
    void hgetall(const StringView &key, Iter output);

    long long hincrby(const StringView &key, const StringView &field, long long increment);

    double hincrbyfloat(const StringView &key, const StringView &field, double increment);

    template <typename Iter>
    void hkeys(const StringView &key, Iter output);

    long long hlen(const StringView &key);

    template <typename Input, typename Output>
    void hmget(const StringView &key, Input first, Input last, Output output);

    template <typename Iter>
    void hmset(const StringView &key, Iter first, Iter last);

    bool hset(const StringView &key, const StringView &field, const StringView &val);

    bool hsetnx(const StringView &key, const StringView &field, const StringView &val);

    long long hstrlen(const StringView &key, const StringView &field);

    template <typename Iter>
    void hvals(const StringView &key, Iter output);

    // SET commands.

    long long sadd(const StringView &key, const StringView &member);

    template <typename Iter>
    long long sadd(const StringView &key, Iter first, Iter last);

    long long scard(const StringView &key);

    template <typename Input, typename Output>
    void sdiff(Input first, Input last, Output output);

    template <typename Input>
    long long sdiffstore(const StringView &destination,
                            Input first,
                            Input last);

    template <typename Input, typename Output>
    void sinter(Input first, Input last, Output output);

    template <typename Input>
    long long sinterstore(const StringView &destination,
                            Input first,
                            Input last);

    bool sismember(const StringView &key, const StringView &member);

    template <typename Iter>
    void smembers(const StringView &key, Iter output);

    bool smove(const StringView &source,
                const StringView &destination,
                const StringView &member);

    OptionalString spop(const StringView &key);

    template <typename Iter>
    void spop(const StringView &key, long long count, Iter output);

    OptionalString srandmember(const StringView &key);

    template <typename Iter>
    void srandmember(const StringView &key, long long count, Iter output);

    long long srem(const StringView &key, const StringView &member);

    template <typename Iter>
    long long srem(const StringView &key, Iter first, Iter last);

    template <typename Input, typename Output>
    void sunion(Input first, Input last, Output output);

    template <typename Input>
    long long sunionstore(const StringView &destination, Input first, Input last);

    // SORTED SET commands.

    // We don't support the INCR option, since you can always use ZINCRBY instead.
    long long zadd(const StringView &key,
                    double score,
                    const StringView &member,
                    bool changed = false,
                    UpdateType type = UpdateType::ALWAYS);

    template <typename Input>
    long long zadd(const StringView &key,
                    Input first,
                    Input last,
                    bool changed = false,
                    UpdateType type = UpdateType::ALWAYS);

    long long zcard(const StringView &key);

    template <typename Interval>
    long long zcount(const StringView &key, const Interval &interval);

    double zincrby(const StringView &key, double increment, const StringView &member);

    template <typename Input>
    long long zinterstore(const StringView &destination,
                            Input first,
                            Input last,
                            Aggregation type = Aggregation::SUM);

    template <typename Interval>
    long long zlexcount(const StringView &key, const Interval &interval);

    template <typename Output>
    void zrange(const StringView &key, long long start, long long stop, Output output);

    template <typename Interval, typename Output>
    void zrangebylex(const StringView &key, const Interval &interval, Output output);

    template <typename Interval, typename Output>
    void zrangebylex(const StringView &key,
                        const Interval &interval,
                        const LimitOptions &opts,
                        Output output);

    template <typename Interval, typename Output>
    void zrangebyscore(const StringView &key, const Interval &interval, Output output);

    template <typename Interval, typename Output>
    void zrangebyscore(const StringView &key,
                        const Interval &interval,
                        const LimitOptions &opts,
                        Output output);

    OptionalLongLong zrank(const StringView &key, const StringView &member);

    long long zrem(const StringView &key, const StringView &member);

    template <typename Input>
    long long zrem(const StringView &key, Input first, Input last);

    template <typename Interval>
    long long zremrangebylex(const StringView &key, const Interval &interval);

    long long zremrangebyrank(const StringView &key, long long start, long long stop);

    template <typename Interval>
    long long zremrangebyscore(const StringView &key, const Interval &interval);

    template <typename Output>
    void zrevrange(const StringView &key, long long start, long long stop, Output output);

    template <typename Interval, typename Output>
    void zrevrangebylex(const StringView &key, const Interval &interval, Output output);

    template <typename Interval, typename Output>
    void zrevrangebylex(const StringView &key,
                        const Interval &interval,
                        const LimitOptions &opts,
                        Output output);

    template <typename Interval, typename Output>
    void zrevrangebyscore(const StringView &key, const Interval &interval, Output output);

    template <typename Interval, typename Output>
    void zrevrangebyscore(const StringView &key,
                            const Interval &interval,
                            const LimitOptions &opts,
                            Output output);

    OptionalLongLong zrevrank(const StringView &key, const StringView &member);

    OptionalDouble zscore(const StringView &key, const StringView &member);

    template <typename Input>
    long long zunionstore(const StringView &destination,
                            Input first,
                            Input last,
                            Aggregation type = Aggregation::SUM);

    // HYPERLOGLOG commands.

    bool pfadd(const StringView &key, const StringView &element);

    template <typename Input>
    bool pfadd(const StringView &key, Input first, Input last);

    long long pfcount(const StringView &key);

    template <typename Input>
    long long pfcount(Input first, Input last);

    template <typename Input>
    void pfmerge(const StringView &destination, Input first, Input last);

    // GEO commands.

    long long geoadd(const StringView &key,
                        const std::tuple<double, double, std::string> &member);

    template <typename Input>
    long long geoadd(const StringView &key,
                        Input first,
                        Input last);

    OptionalDouble geodist(const StringView &key,
                            const StringView &member1,
                            const StringView &member2,
                            GeoUnit unit = GeoUnit::M);

    OptionalString geohash(const StringView &key, const StringView &member);

    template <typename Input, typename Output>
    void geohash(const StringView &key, Input first, Input last, Output output);

private:
    class ConnectionPoolGuard {
    public:
        ConnectionPoolGuard(ConnectionPool &pool, Connection &connection);
        ~ConnectionPoolGuard();

    private:
        ConnectionPool &_pool;
        Connection &_connection;
    };

    ConnectionPool _pool;
};

}

}

#endif // end SEWENEW_REDISPLUSPLUS_REDIS_H
