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

#ifndef SEWENEW_REDISPLUSPLUS_QUEUED_REDIS_HPP
#define SEWENEW_REDISPLUSPLUS_QUEUED_REDIS_HPP

namespace sw {

namespace redis {

template <typename Impl>
template <typename ...Args>
QueuedRedis<Impl>::QueuedRedis(ConnectionPool &pool, Args &&...args) :
            _pool(pool),
            _connection(_pool.fetch()),
            _impl(std::forward<Args>(args)...) {}

template <typename Impl>
QueuedRedis<Impl>::~QueuedRedis() {
    try {
        discard();
    } catch (const Error &e) {
        // Avoid throwing from destructor.
        return;
    }
}

template <typename Impl>
template <typename Cmd, typename ...Args>
QueuedRedis<Impl>& QueuedRedis<Impl>::command(Cmd cmd, Args &&...args) {
    if (_connection.broken()) {
        throw Error("Connection is broken");
    }

    try {
        _impl.command(_connection, cmd, std::forward<Args>(args)...);
    } catch (const Error &e) {
        _reconnect();
        throw;
    }

    ++_cmd_num;

    return *this;
}

template <typename Impl>
QueuedReplies QueuedRedis<Impl>::exec() {
    auto cmd_num = _cmd_num;

    // Reset _cmd_num ASAP.
    _cmd_num = 0;

    try {
        return QueuedReplies(_impl.exec(_connection, cmd_num));
    } catch (const Error &e) {
        _reconnect();
        throw;
    }
}

template <typename Impl>
void QueuedRedis<Impl>::discard() {
    _cmd_num = 0;

    try {
        _impl.discard(_connection);
    } catch (const Error &e) {
        _reconnect();
        throw;
    }
}

template <typename Impl>
void QueuedRedis<Impl>::_reconnect() {
    _pool.reconnect(_connection);
}

template <typename Result>
inline Result QueuedReplies::pop() {
    auto reply = std::move(_replies.front());

    _replies.pop_front();

    return reply::parse<Result>(*reply);
}

template <typename Output>
inline void QueuedReplies::pop(Output output) {
    auto reply = std::move(_replies.front());

    _replies.pop_front();

    reply::to_array(*reply, output);
}

}

}

#endif // end SEWENEW_REDISPLUSPLUS_QUEUED_REDIS_HPP
