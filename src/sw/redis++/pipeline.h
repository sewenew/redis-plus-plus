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

#ifndef SEWENEW_REDISPLUSPLUS_PIPELINE_H
#define SEWENEW_REDISPLUSPLUS_PIPELINE_H

#include <functional>
#include <queue>
#include "connection_pool.h"
#include "reply.h"
#include "command.h"
#include "utils.h"

namespace sw {

namespace redis {

class PString;

class Pipeline {
public:
    explicit Pipeline(ConnectionPool &pool);

    Pipeline(const Pipeline &) = delete;
    Pipeline& operator=(const Pipeline &) = delete;

    Pipeline(Pipeline &&) = default;
    Pipeline& operator=(Pipeline &&) = default;

    ~Pipeline();

    void transport();

    PString string(const std::string &key);

    template <typename Cmd,
             typename ReplyFunctor,
             typename ErrorCallback,
             typename ...Args>
    Pipeline& command(Cmd cmd,
            ReplyFunctor reply_functor,
            ErrorCallback error_callback,
            Args &&...args);

    template <typename ErrorCallback>
    Pipeline& auth(const StringView &password,
            ErrorCallback error_callback);

    template <typename StringReplyCallback, typename ErrorCallback>
    Pipeline& info(StringReplyCallback reply_callback,
            ErrorCallback error_callback);

    template <typename StringReplyCallback, typename ErrorCallback>
    Pipeline& ping(StringReplyCallback reply_callback,
            ErrorCallback error_callback);

    template <typename StringReplyCallback, typename ErrorCallback>
    Pipeline& ping(const StringView &msg,
            StringReplyCallback reply_callback,
            ErrorCallback error_callback);

private:
    ConnectionPool &_pool;

    Connection _connection;

    using ReplyCallback = std::function<void (redisReply &)>;
    using ReplyErrorCallback = std::function<void (const RException &)>;

    std::queue<std::pair<ReplyCallback, ReplyErrorCallback>> _callbacks;
};

template <typename Cmd,
         typename ReplyFunctor,
         typename ErrorCallback,
         typename ...Args>
inline Pipeline& Pipeline::command(Cmd cmd,
        ReplyFunctor reply_functor,
        ErrorCallback error_callback,
        Args &&...args) {
    cmd(_connection, std::forward<Args>(args)...);

    _callbacks.emplace(reply_functor, error_callback);

    return *this;
}

template <typename ErrorCallback>
inline Pipeline& Pipeline::auth(const StringView &password,
        ErrorCallback error_callback) {
    return command(cmd::auth,
                DummyReplyFunctor{},
                error_callback,
                password);
}

template <typename StringReplyCallback, typename ErrorCallback>
inline Pipeline& Pipeline::info(StringReplyCallback reply_callback,
        ErrorCallback error_callback) {
    return command(cmd::info,
                StringReplyFunctor(reply_callback),
                error_callback);
}

template <typename StringReplyCallback, typename ErrorCallback>
inline Pipeline& Pipeline::ping(StringReplyCallback reply_callback,
        ErrorCallback error_callback) {
    return command<void (*)(Connection &)>(cmd::ping,
                StatusReplyFunctor(reply_callback),
                error_callback);
}

template <typename StringReplyCallback, typename ErrorCallback>
inline Pipeline& Pipeline::ping(const StringView &msg,
        StringReplyCallback reply_callback,
        ErrorCallback error_callback) {
    return command<void (*)(Connection &, const StringView &)>(cmd::ping,
                StringReplyFunctor(reply_callback),
                error_callback,
                msg);
}

}

}

#endif // end SEWENEW_REDISPLUSPLUS_PIPELINE_H
