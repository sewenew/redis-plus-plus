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

    _impl.command(_connection, cmd, std::forward<Args>(args)...);

    return *this;
}

template <typename Impl>
void QueuedRedis<Impl>::exec() {
    _impl.exec(_connection);
}

template <typename Impl>
void QueuedRedis<Impl>::discard() {
    _impl.discard(_pool, _connection);
}

template <typename Impl>
template <typename Result>
Result QueuedRedis<Impl>::get() {
    auto reply = _connection.recv();

    return reply::parse<Result>(*reply);
}

}

}

#endif // end SEWENEW_REDISPLUSPLUS_QUEUED_REDIS_HPP
