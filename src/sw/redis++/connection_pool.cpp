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
#include "errors.h"

namespace sw {

namespace redis {

ConnectionPool::ConnectionPool(const ConnectionPoolOptions &pool_opts,
        const ConnectionOptions &connection_opts) :
            _opts(connection_opts),
            _pool_opts(pool_opts) {
    if (_pool_opts.size == 0) {
        throw Error("CANNOT create an empty pool");
    }

    // Lazily create connections.
}

ConnectionPool::ConnectionPool(ConnectionPool &&that) {
    std::lock_guard<std::mutex> lock(that._mutex);

    _move(std::move(that));
}

ConnectionPool& ConnectionPool::operator=(ConnectionPool &&that) {
    if (this != &that) {
        std::lock(_mutex, that._mutex);
        std::lock_guard<std::mutex> lock_this(_mutex, std::adopt_lock);
        std::lock_guard<std::mutex> lock_that(that._mutex, std::adopt_lock);

        _move(std::move(that));
    }

    return *this;
}

Connection ConnectionPool::fetch() {
    std::unique_lock<std::mutex> lock(_mutex);

    if (_pool.empty()) {
        if (_used_connections == _pool_opts.size) {
            _wait_for_connection(lock);
        } else {
            // Lazily create a new connection.
            auto connection = Connection(_opts);

            ++_used_connections;

            return connection;
        }
    }

    // _pool is NOT empty.
    auto connection = _fetch();

    lock.unlock();

    if (_need_reconnect(connection)) {
        try {
            connection.reconnect();
        } catch (const Error &e) {
            // Failed to reconnect, return it to the pool, and retry latter.
            release(std::move(connection));
            throw;
        }
    }

    return connection;
}

Connection ConnectionPool::create() {
    std::lock_guard<std::mutex> lock(_mutex);

    return Connection(_opts);
}

void ConnectionPool::release(Connection connection) {
    {
        std::lock_guard<std::mutex> lock(_mutex);

        _pool.push_back(std::move(connection));
    }

    _cv.notify_one();
}

void ConnectionPool::_move(ConnectionPool &&that) {
    _opts = std::move(that._opts);
    _pool_opts = std::move(that._pool_opts);
    _pool = std::move(that._pool);
    _used_connections = that._used_connections;
}

Connection ConnectionPool::_fetch() {
    assert(!_pool.empty());

    auto connection = std::move(_pool.front());
    _pool.pop_front();

    return connection;
}

void ConnectionPool::_wait_for_connection(std::unique_lock<std::mutex> &lock) {
    auto timeout = _pool_opts.wait_timeout;
    if (timeout > std::chrono::milliseconds(0)) {
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
    if (connection_lifetime > std::chrono::milliseconds(0)) {
        auto now = std::chrono::steady_clock::now();
        if (now - connection.last_active() > connection_lifetime) {
            return true;
        }
    }

    return false;
}

}

}
