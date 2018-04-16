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

#include "connection_pool.h"
#include <cassert>

namespace sw {

namespace redis {

ConnectionPool::ConnectionPool(const ConnectionPoolOptions &pool_opts,
        const ConnectionOptions &connection_opts) :
            _connector(connection_opts),
            _pool_opts(pool_opts) {
    if (_pool_opts.size == 0) {
        throw Error("CANNOT create an empty pool");
    }

    for (std::size_t idx = 0; idx != _pool_opts.size; ++idx) {
        _pool.push_back(_connector.connect());
    }
}

Connection ConnectionPool::fetch() {
    std::unique_lock<std::mutex> lock(_mutex);

    if (_pool.empty()) {
        _wait_for_connection(lock);
    }

    // _pool is NOT empty.
    auto connection = _fetch();

    lock.unlock();

    if (_need_reconnect(connection)) {
        try {
            connection.reconnect();
        } catch (const std::exception &e) {
            // Failed to reconnect, return it to the pool.
            release(std::move(connection));
            throw;
        }
    }

    return connection;
}

void ConnectionPool::release(Connection connection) {
    {
        std::lock_guard<std::mutex> lock(_mutex);

        _pool.push_back(std::move(connection));
    }

    _cv.notify_one();
}

Connection ConnectionPool::_fetch() {
    assert(!_pool.empty());

    auto connection = std::move(_pool.front());
    _pool.pop_front();

    return connection;
}

void ConnectionPool::_wait_for_connection(std::unique_lock<std::mutex> &lock) {
    auto timeout = _pool_opts.wait_timeout;
    if (timeout > std::chrono::steady_clock::duration(0)) {
        // Wait until _pool is no longer empty or timeout.
        if (!_cv.wait_for(lock,
                    timeout,
                    [this] { return !(this->_pool).empty(); })) {
            throw Error("Failed to fetch a connection in "
                    + std::to_string(timeout.count()) + " milliseconds");
        }
    } else {
        // Wait forever.
        _cv.wait(lock, [this] { return !(this->_pool).empty(); });
    }
}

bool ConnectionPool::_need_reconnect(const Connection &connection) {
    if (connection.broken()) {
        return true;
    }

    auto connection_lifetime = _pool_opts.connection_lifetime;
    if (connection_lifetime > std::chrono::steady_clock::duration(0)) {
        auto now = std::chrono::steady_clock::now();
        if (now - connection.last_active() > connection_lifetime) {
            return true;
        }
    }

    return false;
}

}

}
