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
class RList;
class RHash;
class RSet;
class RSortedSet;
class RHyperLogLog;
class Pipeline;

class Redis {
public:
    Redis(const ConnectionPoolOptions &pool_opts,
            const ConnectionOptions &connection_opts) : _pool(pool_opts, connection_opts) {}

    RList list(const std::string &key);

    RHash hash(const std::string &key);

    RSet set(const std::string &key);

    RSortedSet sorted_set(const std::string &key);

    RHyperLogLog hyperloglog(const std::string &key);

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
