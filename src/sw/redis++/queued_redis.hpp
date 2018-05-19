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
    try {
        _sanity_check();

        _impl.command(_connection, cmd, std::forward<Args>(args)...);

        ++_cmd_num;
    } catch (const Error &e) {
        _reset();
        throw;
    }

    return *this;
}

template <typename Impl>
QueuedReplies QueuedRedis<Impl>::exec() {
    try {
        _sanity_check();

        auto replies = _impl.exec(_connection, _cmd_num);

        _cmd_num = 0;

        return QueuedReplies(std::move(replies));
    } catch (const Error &e) {
        _reset();
        throw;
    }
}

template <typename Impl>
void QueuedRedis<Impl>::discard() {
    try {
        _sanity_check();

        _cmd_num = 0;

        _impl.discard(_connection);
    } catch (const Error &e) {
        _reset();
        throw;
    }
}

template <typename Impl>
void QueuedRedis<Impl>::_sanity_check() const {
    if (!_valid) {
        throw Error("Not in valid state");
    }

    if (_connection.broken()) {
        throw Error("Connection is broken");
    }
}

template <typename Impl>
inline void QueuedRedis<Impl>::_reset() {
    _cmd_num = 0;

    _valid = false;

    _connection.reconnect();
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
