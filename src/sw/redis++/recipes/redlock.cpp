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

#include "redlock.h"
#include <cassert>
#include <thread>

namespace sw {

namespace redis {

RedMutex::RedMutex(Redis &master, const std::string &resource) : _resource(resource) {
    _masters.push_back(std::ref(master));
}

RedMutex::RedMutex(std::initializer_list<std::reference_wrapper<Redis>> masters,
                    const std::string &resource)
                        : _masters(masters.begin(), masters.end()), _resource(resource) {}

std::chrono::milliseconds RedMutex::try_lock(const std::string &val,
        const std::chrono::milliseconds &ttl) {
    auto start = std::chrono::steady_clock::now();

    if (!_try_lock(val, ttl)) {
        // Failed to lock more than half masters.
        unlock(_resource);

        return std::chrono::milliseconds(-1);
    }

    auto stop = std::chrono::steady_clock::now();
    auto elapse = stop - start;

    auto time_left = std::chrono::duration_cast<std::chrono::milliseconds>(ttl - elapse);
    if (time_left <= std::chrono::milliseconds(0)) {
        // No time left for the lock.
        unlock(_resource);
    }

    return time_left;
}

std::chrono::milliseconds RedMutex::try_lock(const std::string &val,
        const std::chrono::time_point<std::chrono::system_clock> &tp) {
    return try_lock(val, RedLockUtils::ttl(tp));
}

std::chrono::milliseconds RedMutex::extend_lock(const std::string &val,
        const std::chrono::milliseconds &ttl) {
    // TODO: this method is almost duplicate with `try_lock`. I'll refactor it soon.
    auto start = std::chrono::steady_clock::now();

    auto lock_cnt = 0U;
    for (auto &master : _masters) {
        if (_extend_lock_master(master.get(), val, ttl)) {
            ++lock_cnt;
        }
    }

    auto lock_ok = lock_cnt >= _quorum();
    if (!lock_ok) {
        // Failed to lock more than half masters.
        unlock(_resource);

        return std::chrono::milliseconds(-1);
    }

    auto stop = std::chrono::steady_clock::now();
    auto elapse = stop - start;

    auto time_left = std::chrono::duration_cast<std::chrono::milliseconds>(ttl - elapse);
    if (time_left <= std::chrono::milliseconds(0)) {
        // No time left for the lock.
        unlock(_resource);
    }

    return time_left;
}

std::chrono::milliseconds RedMutex::extend_lock(const std::string &val,
        const std::chrono::time_point<std::chrono::system_clock> &tp) {
    return extend_lock(val, RedLockUtils::ttl(tp));
}

bool RedMutex::_extend_lock_master(Redis &master,
        const std::string &val,
        const std::chrono::milliseconds &ttl) {
    auto tx = master.transaction(true);
    auto r = tx.redis();
    try {
        r.watch(_resource);

        auto id = r.get(_resource);
        if (id && *id == val) {
            auto reply = tx.pexpire(_resource, ttl).exec();
            if (!reply.get<bool>(0)) {
                return false;
            }
        }
    } catch (const Error &err) {
        // key has been modified or other error happens, failed to extend the lock.
        return false;
    }

    return true;
}

void RedMutex::unlock(const std::string &val) {
    for (auto &master : _masters) {
        try {
            _unlock_master(master.get(), val);
        } catch (const Error &err) {
            // Ignore errors, and continue to unlock other maters.
        }
    }
}

void RedMutex::_unlock_master(Redis &master, const std::string &val) {
    auto tx = master.transaction(true);
    auto r = tx.redis();
    try {
        r.watch(_resource);

        auto id = r.get(_resource);
        if (id && *id == val) {
            auto reply = tx.del(_resource).exec();
            if (reply.get<long long>(0) != 1) {
                throw Error("Redis internal error: WATCH " + _resource + "failed");
            }
        }
    } catch (const WatchError &err) {
        // key has been modified. Do nothing, just let it go.
    }
}

bool RedMutex::_try_lock(const std::string &val, const std::chrono::milliseconds &ttl) {
    std::size_t lock_cnt = 0U;
    for (auto &master : _masters) {
        if (_try_lock_master(master.get(), val, ttl)) {
            ++lock_cnt;
        }
    }

    return lock_cnt >= _quorum();
}

bool RedMutex::_try_lock_master(Redis &master,
        const std::string &val,
        const std::chrono::milliseconds &ttl) {
    return master.set(_resource, val, ttl, UpdateType::NOT_EXIST);
}

std::chrono::milliseconds RedLockUtils::ttl(const SysTime &tp) {
    auto cur = std::chrono::system_clock::now();
    auto ttl = tp - cur;
    if (ttl.count() <= 0) {
        throw Error("time already pasts");
    }

    return std::chrono::duration_cast<std::chrono::milliseconds>(ttl);
}

std::string RedLockUtils::lock_id() {
    std::random_device dev;
    std::mt19937 random_gen(dev());
    int range = 10 + 26 + 26 - 1;
    std::uniform_int_distribution<> dist(0, range);
    std::string id;
    id.reserve(20);
    for (int i = 0; i != 20; ++i) {
        auto idx = dist(random_gen);
        if (idx < 10) {
            id.push_back('0' + idx);
        } else if (idx < 10 + 26) {
            id.push_back('a' + idx - 10);
        } else if (idx < 10 + 26 + 26) {
            id.push_back('A' + idx - 10 - 26);
        } else {
            assert(false && "Index out of range in RedLock::_lock_id()");
        }
    }

    return id;
}

RedLockMutexVessel::RedLockMutexVessel(Redis& instance) :
    RedLockMutexVessel({{instance}})
{
}

RedLockMutexVessel::RedLockMutexVessel(std::initializer_list<std::reference_wrapper<Redis>> instances) :
    _instances(instances.begin(), instances.end())
{
}

bool RedLockMutexVessel::_extend_lock_instance(Redis& instance,
                                         const std::string& resource,
                                         const std::string& random_string,
                                         const std::chrono::milliseconds& ttl)
{
    const static std::string script = R"(
if redis.call("GET",KEYS[1]) == ARGV[1] then
  return redis.call("pexpire",KEYS[1],ARGV[2])
else
  return 0
end
)";
    auto result = instance.eval<long long>(script, {resource}, {random_string, std::to_string(ttl.count())});
    return result != 0;
}

void RedLockMutexVessel::_unlock_instance(Redis& instance,
                                    const std::string& resource,
                                    const std::string& random_string)
{
    const std::string script = R"(
if redis.call("GET",KEYS[1]) == ARGV[1] then
  return redis.call("del",KEYS[1])
else
  return 0
end
)";
    instance.eval<long long>(script, {resource}, {random_string});
}

bool RedLockMutexVessel::_lock_instance(Redis& instance,
                                  const std::string& resource,
                                  const std::string& random_string,
                                  const std::chrono::milliseconds& ttl)
{
    const auto result = instance.set(resource, random_string, ttl, UpdateType::NOT_EXIST);
    return result;
}

RedLockMutexVessel::LockInfo RedLockMutexVessel::lock(const std::string& resource,
                                          const std::string& random_string,
                                          const std::chrono::milliseconds& ttl,
                                          int retry_count,
                                          const std::chrono::milliseconds& retry_delay,
                                          double clock_drift_factor)
{
    LockInfo lock_info = {false, std::chrono::steady_clock::now(), ttl, resource, random_string};

    for (int i=0; i<retry_count; i++) {
        int num_locked = 0;
        for (auto& instance : _instances) {
            if (_lock_instance(instance, lock_info.resource, lock_info.random_string, ttl)) {
                num_locked++;
            }
        }

        const auto drift = std::chrono::milliseconds(int(ttl.count() * clock_drift_factor) + 2);
        lock_info.time_remaining = std::chrono::duration_cast<std::chrono::milliseconds>
            (lock_info.startTime + ttl - std::chrono::steady_clock::now() - drift);

        if (lock_info.time_remaining.count() <= 0) {
            unlock(lock_info);
            break; // The should not retry after the TTL expiration.
        }
        if (num_locked >= _quorum()) {
            lock_info.locked = true;
            break; // We have the lock.
        }

        unlock(lock_info);

        // Sleep, only if it's _not_ the last attempt.
        if (i != retry_count - 1) {
            std::this_thread::sleep_for(std::chrono::milliseconds(std::rand() * retry_delay.count() / RAND_MAX));
        }
    }
    return lock_info;
}

RedLockMutexVessel::LockInfo RedLockMutexVessel::extend_lock(const RedLockMutexVessel::LockInfo& lock_info,
                                                 const std::chrono::milliseconds& ttl,
                                                 double clock_drift_factor)
{
    if (lock_info.locked) {
        LockInfo extended_lock_info = {false, std::chrono::steady_clock::now(), ttl, lock_info.resource, lock_info.random_string};
        const auto time_remaining = std::chrono::duration_cast<std::chrono::milliseconds>
            (lock_info.startTime + lock_info.time_remaining - extended_lock_info.startTime);

        if (time_remaining.count() > 0) {
            int num_locked = 0;
            for (auto& instance : _instances) {
                if (_extend_lock_instance(instance, lock_info.resource, lock_info.random_string, ttl)) {
                    num_locked++;
                }
            }
            const auto drift = std::chrono::milliseconds(int(ttl.count() * clock_drift_factor) + 2);
            extended_lock_info.time_remaining = std::chrono::duration_cast<std::chrono::milliseconds>
                (extended_lock_info.startTime + ttl - std::chrono::steady_clock::now() - drift);

            if (num_locked >= _quorum() && extended_lock_info.time_remaining.count() > 0) {
                extended_lock_info.locked = true;
            }
            else {
                unlock(lock_info);
            }
        }
        return extended_lock_info;
    }
    else {
        return lock_info;
    }
}

void RedLockMutexVessel::unlock(const LockInfo& lock_info)
{
    for (auto& instance : _instances) {
        _unlock_instance(instance, lock_info.resource, lock_info.random_string);
    }
}

}

}
