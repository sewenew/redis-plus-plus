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

class RedLockUtils {
public:
    using SysTime = std::chrono::time_point<std::chrono::system_clock>;

    static std::chrono::milliseconds ttl(const SysTime &tp);

    static std::string lock_id();
};

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

    std::chrono::milliseconds try_lock(const std::string &val,
            const std::chrono::time_point<std::chrono::system_clock> &tp);

    std::chrono::milliseconds extend_lock(const std::string &val,
            const std::chrono::milliseconds &ttl);

    std::chrono::milliseconds extend_lock(const std::string &val,
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
            const std::chrono::milliseconds &ttl);

    std::size_t _quorum() const {
        return _masters.size() / 2 + 1;
    }

    using RedisRef = std::reference_wrapper<Redis>;

    std::vector<RedisRef> _masters;

    std::string _resource;
};

template <typename RedisInstance>
class RedLock {
public:
    RedLock(RedisInstance &mut, std::defer_lock_t) : _mut(mut), _lock_val(RedLockUtils::lock_id()) {}

    ~RedLock() {
        if (owns_lock()) {
            unlock();
        }
    }

    // Try to acquire the lock for *ttl* milliseconds.
    // Returns how much time still left for the lock, i.e. lock validity time.
    bool try_lock(const std::chrono::milliseconds &ttl) {
        auto time_left = _mut.try_lock(_lock_val, ttl);
        if (time_left <= std::chrono::milliseconds(0)) {
            return false;
        }

        _release_tp = std::chrono::steady_clock::now() + time_left;

        return true;
    }

    // Try to acquire the lock, and hold until *tp*.
    bool try_lock(const std::chrono::time_point<std::chrono::system_clock> &tp) {
        return try_lock(RedLockUtils::ttl(tp));
    }

    // Try to extend the lock, and hold it until *tp*.
    bool extend_lock(const std::chrono::milliseconds &ttl) {
        // TODO: this method is almost duplicate with `try_lock`, and I'll refactor it soon.
        auto time_left = _mut.extend_lock(_lock_val, ttl);
        if (time_left <= std::chrono::milliseconds(0)) {
            return false;
        }

        _release_tp = std::chrono::steady_clock::now() + time_left;

        return true;
    }

    bool extend_lock(const std::chrono::time_point<std::chrono::system_clock> &tp) {
        return extend_lock(RedLockUtils::ttl(tp));
    }

    void unlock() {
        try {
            _mut.unlock(_lock_val);
            _release_tp = std::chrono::time_point<std::chrono::steady_clock>{};
        } catch (const Error &err) {
            _release_tp = std::chrono::time_point<std::chrono::steady_clock>{};
            throw;
        }
    }

    bool owns_lock() const {
        if (ttl() <= std::chrono::milliseconds(0)) {
            return false;
        }

        return true;
    }

    std::chrono::milliseconds ttl() const {
        auto t = std::chrono::steady_clock::now();
        return std::chrono::duration_cast<std::chrono::milliseconds>(_release_tp - t);
    }

private:
    RedisInstance &_mut;

    std::string _lock_val;

    // The time point that we must release the lock.
    std::chrono::time_point<std::chrono::steady_clock> _release_tp{};
};

class RedLockMutexVessel
{
public:

    // This class does _not_ implement RedMutexInterface, as it gives
    // the user the ability to use an instance of it for multiple resources.
    // More than one resource can thus be locked and tracked with a single
    // instantiation of this class.

    explicit RedLockMutexVessel(Redis& instance);
    explicit RedLockMutexVessel(std::initializer_list<std::reference_wrapper<Redis>> instances);

    RedLockMutexVessel(const RedLockMutexVessel &) = delete;
    RedLockMutexVessel& operator=(const RedLockMutexVessel &) = delete;

    RedLockMutexVessel(RedLockMutexVessel &&) = delete;
    RedLockMutexVessel& operator=(RedLockMutexVessel &&) = delete;

    ~RedLockMutexVessel() = default;

    // The LockInfo struct can be used for chaining a lock to
    // one or more extend_locks and finally to an unlock.
    // All the lock's information is contained, so multiple
    // locks could be handled with a single RedLockMutexVessel instance.
    struct LockInfo {
        bool locked;
        std::chrono::time_point<std::chrono::steady_clock> startTime;
        std::chrono::milliseconds time_remaining;
        std::string resource;
        std::string random_string;
    };

    // RedLockMutexVessel::lock will (re)try to get a lock until either:
    //  - it gets a lock on (n/2)+1 of the instances.
    //  - a period of TTL elapsed.
    //  - the number of retries was reached.
    //  - an exception was thrown.
    LockInfo lock(const std::string& resource,
                  const std::string& random_string,
                  const std::chrono::milliseconds& ttl,
                  int retry_count = 3,
                  const std::chrono::milliseconds& retry_delay = std::chrono::milliseconds(200),
                  double clock_drift_factor = 0.01);

    // RedLockMutexVessel::extend_lock is exactly the same as RedLockMutexVessel::lock,
    // but needs LockInfo from a previously acquired lock.
    LockInfo extend_lock(const LockInfo& lock_info,
                         const std::chrono::milliseconds& ttl,
                         double clock_drift_factor = 0.01);

    // RedLockMutexVessel::unlock unlocks all locked instances,
    // that was locked with LockInfo,
    void unlock(const LockInfo& lock_info);

private:

    bool _lock_instance(Redis& instance,
                        const std::string& resource,
                        const std::string& random_string,
                        const std::chrono::milliseconds& ttl);

    bool _extend_lock_instance(Redis& instance,
                               const std::string& resource,
                               const std::string& random_string,
                               const std::chrono::milliseconds& ttl);

    void _unlock_instance(Redis& instance,
                          const std::string& resource,
                          const std::string& random_string);

    int _quorum() const {
        return _instances.size() / 2 + 1;
    }

    std::vector<std::reference_wrapper<Redis>> _instances;
};

class RedLockMutex
{
public:
    explicit RedLockMutex(Redis& instance, const std::string& resource) :
        _redlock_mutex(instance), _resource(resource) {}

    explicit RedLockMutex(std::initializer_list<std::reference_wrapper<Redis>> instances,
                    const std::string &resource) :
        _redlock_mutex(instances), _resource(resource) {}

    RedLockMutex(const RedLockMutex &) = delete;
    RedLockMutex& operator=(const RedLockMutex &) = delete;

    RedLockMutex(RedLockMutex &&) = delete;
    RedLockMutex& operator=(RedLockMutex &&) = delete;

    std::chrono::milliseconds try_lock(const std::string& random_string,
                                       const std::chrono::milliseconds& ttl)
    {
        const auto lock_info = _redlock_mutex.lock(_resource, random_string, ttl, 1);
        if (!lock_info.locked) {
            return std::chrono::milliseconds(-1);
        }
        return lock_info.time_remaining;
    }

    std::chrono::milliseconds try_lock(const std::string &random_string,
                  const std::chrono::time_point<std::chrono::system_clock> &tp)
    {
        return try_lock(random_string, RedLockUtils::ttl(tp));
    }

    std::chrono::milliseconds extend_lock(const std::string &random_string,
                     const std::chrono::milliseconds &ttl)
    {
        const RedLockMutexVessel::LockInfo lock_info =
            {true, std::chrono::steady_clock::now(), ttl, _resource, random_string};
        const auto result = _redlock_mutex.extend_lock(lock_info, ttl);
        if (!result.locked) {
            return std::chrono::milliseconds(-1);
        }
        else {
            return result.time_remaining;
        }
    }

    std::chrono::milliseconds extend_lock(const std::string &random_string,
                     const std::chrono::time_point<std::chrono::system_clock> &tp) {
        return extend_lock(random_string, RedLockUtils::ttl(tp));
    }

    void unlock(const std::string &random_string)
    {
        _redlock_mutex.unlock({true, std::chrono::steady_clock::now(),
                              std::chrono::milliseconds(0), _resource, random_string});
    }

private:
    RedLockMutexVessel _redlock_mutex;
    const std::string _resource;
};

}

}

#endif // end SEWENEW_REDISPLUSPLUS_RECIPES_REDLOCK_H
