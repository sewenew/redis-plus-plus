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
#include "utils.h"

namespace sw {

namespace redis {

class StringView;
class RString;
class RList;
class RHash;
class RSet;
class Pipeline;

class Redis {
public:
    Redis(const ConnectionPoolOptions &pool_opts,
            const ConnectionOptions &connection_opts) : _pool(pool_opts, connection_opts) {}

    RString string(const std::string &key);

    RList list(const std::string &key);

    RHash hash(const std::string &key);

    RSet set(const std::string &key);

    Pipeline pipeline();

    template <typename Cmd, typename ...Args>
    ReplyUPtr command(Cmd cmd, Args &&...args);

    void auth(const StringView &password);

    std::string info();

    std::string ping();

    std::string ping(const StringView &msg);

private:
    ConnectionPool _pool;
};

// Inline implementations.

template <typename Cmd, typename ...Args>
ReplyUPtr Redis::command(Cmd cmd, Args &&...args) {
    auto connection = _pool.fetch();

    cmd(connection, std::forward<Args>(args)...);

    auto reply = connection.recv();

    _pool.release(std::move(connection));

    return reply;
}

}

}

#endif // end SEWENEW_REDISPLUSPLUS_REDIS_H
