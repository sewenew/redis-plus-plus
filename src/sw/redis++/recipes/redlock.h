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

#ifndef SEWENEW_REDISPLUSPLUS_RECIPES_REDLOCK_H
#define SEWENEW_REDISPLUSPLUS_RECIPES_REDLOCK_H

#include <random>
#include <chrono>
#include <string>
#include <vector>
#include <functional>
#include "../redis++.h"

namespace sw {

namespace redis {

class RedMutex {
public:
    // Lock with a single Redis master.
    RedMutex(Redis &master, const std::string &resource);

    // Distributed version, i.e. lock with a list of Redis masters.
    // Only successfully acquire the lock if we can lock on more than half masters.
    RedMutex(std::initializer_list<std::reference_wrapper<Redis>> masters,
                const std::string &resource);

    RedMutex(const RedMutex &) = delete;
    RedMutex& operator=(const RedMutex &) = delete;

    RedMutex(RedMutex &&) = delete;
    RedMutex& operator=(RedMutex &&) = delete;

    ~RedMutex() = default;

    std::chrono::milliseconds try_lock(const std::string &val,
            const std::chrono::milliseconds &ttl);

    bool try_lock(const std::string &val,
            const std::chrono::time_point<std::chrono::system_clock> &tp);

    bool extend_lock(const std::string &val,
            const std::chrono::time_point<std::chrono::system_clock> &tp);

    void unlock(const std::string &val);

private:
    void _unlock_master(Redis &master, const std::string &val);

    bool _try_lock(const std::string &val, const std::chrono::milliseconds &ttl);

    bool _try_lock_master(Redis &master,
            const std::string &val,
            const std::chrono::milliseconds &ttl);

    bool _extend_lock_master(Redis &master,
            const std::string &val,
            const std::chrono::time_point<std::chrono::system_clock> &tp);

    std::size_t _quorum() const {
        return _masters.size() / 2 + 1;
    }

    using SysTime = std::chrono::time_point<std::chrono::system_clock>;

    std::chrono::milliseconds _ttl(const SysTime &tp) const;

    using RedisRef = std::reference_wrapper<Redis>;

    std::vector<RedisRef> _masters;

    std::string _resource;
};

class RedLock {
public:
    RedLock(RedMutex &mut, std::defer_lock_t) : _mut(mut), _lock_val(_lock_id()) {}

    ~RedLock() {
        if (_owned) {
            unlock();
        }
    }

    // Try to acquire the lock for *ttl* milliseconds.
    // Returns how much time still left for the lock, i.e. lock validity time.
    std::chrono::milliseconds try_lock(const std::chrono::milliseconds &ttl) {
        auto time_left = _mut.try_lock(_lock_val, ttl);
        _owned = true;
        return time_left;
    }

    // Try to acquire the lock, and hold until *tp*.
    bool try_lock(const std::chrono::time_point<std::chrono::system_clock> &tp) {
        if (_mut.try_lock(_lock_val, tp)) {
            _owned = true;
        }

        return _owned;
    }

    // Try to extend the lock, and hold it until *tp*.
    bool extend_lock(const std::chrono::time_point<std::chrono::system_clock> &tp) {
        if (_mut.extend_lock(_lock_val, tp)) {
            _owned = true;
        } else {
            _owned = false;
        }

        return _owned;
    }

    void unlock() {
        try {
            _mut.unlock(_lock_val);
            _owned = false;
        } catch (const Error &err) {
            _owned = false;
            throw;
        }
    }

private:
    std::string _lock_id() const;

    RedMutex &_mut;

    bool _owned = false;

    std::string _lock_val;
};

}

}

#endif // end SEWENEW_REDISPLUSPLUS_RECIPES_REDLOCK_H
