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

#include "sw/redis++/patterns/redlock.h"

namespace sw {

namespace redis {

RedMutexTx::RedMutexTx(std::shared_ptr<Redis> master, const std::string &resource) : _resource(resource) {
    _masters.push_back(std::move(master));
    _sanity_check();
}

RedMutexTx::RedMutexTx(std::initializer_list<std::shared_ptr<Redis>> masters,
                    const std::string &resource)
                        : _masters(masters.begin(), masters.end()), _resource(resource) {
    _sanity_check();
}

std::chrono::milliseconds RedMutexTx::try_lock(const std::string &val,
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

std::chrono::milliseconds RedMutexTx::try_lock(const std::string &val,
        const std::chrono::time_point<std::chrono::system_clock> &tp) {
    return try_lock(val, RedLockUtils::ttl(tp));
}

std::chrono::milliseconds RedMutexTx::extend_lock(const std::string &val,
        const std::chrono::milliseconds &ttl) {
    // TODO: this method is almost duplicate with `try_lock`. I'll refactor it soon.
    auto start = std::chrono::steady_clock::now();

    auto lock_cnt = 0U;
    for (auto &master : _masters) {
        if (_extend_lock_master(*master, val, ttl)) {
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

void RedMutexTx::_sanity_check() {
    for (auto &master : _masters) {
        if (!master) {
            throw Error("cannot construct RedMutexTx with null Redis");
        }
    }
}

std::chrono::milliseconds RedMutexTx::extend_lock(const std::string &val,
        const std::chrono::time_point<std::chrono::system_clock> &tp) {
    return extend_lock(val, RedLockUtils::ttl(tp));
}

bool RedMutexTx::_extend_lock_master(Redis &master,
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
    } catch (const Error &) {
        // key has been modified or other error happens, failed to extend the lock.
        return false;
    }

    return true;
}

void RedMutexTx::unlock(const std::string &val) {
    for (auto &master : _masters) {
        try {
            _unlock_master(*master, val);
        } catch (const Error &) {
            // Ignore errors, and continue to unlock other maters.
        }
    }
}

void RedMutexTx::_unlock_master(Redis &master, const std::string &val) {
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
    } catch (const WatchError &) {
        // key has been modified. Do nothing, just let it go.
    }
}

bool RedMutexTx::_try_lock(const std::string &val, const std::chrono::milliseconds &ttl) {
    std::size_t lock_cnt = 0U;
    for (auto &master : _masters) {
        if (_try_lock_master(*master, val, ttl)) {
            ++lock_cnt;
        }
    }

    return lock_cnt >= _quorum();
}

bool RedMutexTx::_try_lock_master(Redis &master,
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
    thread_local std::mt19937 random_gen(std::random_device{}());
    int range = 10 + 26 + 26 - 1;
    std::uniform_int_distribution<> dist(0, range);
    std::string id;
    id.reserve(20);
    for (int i = 0; i != 20; ++i) {
        auto idx = dist(random_gen);
        if (idx < 10) {
            id.push_back(static_cast<char>('0' + idx));
        } else if (idx < 10 + 26) {
            id.push_back(static_cast<char>('a' + idx - 10));
        } else if (idx < 10 + 26 + 26) {
            id.push_back(static_cast<char>('A' + idx - 10 - 26));
        } else {
            assert(false && "Index out of range in RedLock::_lock_id()");
        }
    }

    return id;
}

RedLockMutexVessel::RedLockMutexVessel(std::shared_ptr<Redis> instance) :
    RedLockMutexVessel({{instance}})
{
    _sanity_check();
}

RedLockMutexVessel::RedLockMutexVessel(std::initializer_list<std::shared_ptr<Redis>> instances) :
    _instances(instances.begin(), instances.end())
{
    _sanity_check();
}

void RedLockMutexVessel::_sanity_check() {
    for (auto &instance : _instances) {
        if (!instance) {
            throw Error("cannot construct RedLockMutexVessel with null Redis");
        }
    }
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
            if (_lock_instance(*instance, lock_info.resource, lock_info.random_string, ttl)) {
                num_locked++;
            }
        }

        const auto drift = std::chrono::duration<decltype(clock_drift_factor)>(ttl) * clock_drift_factor + std::chrono::milliseconds(2);
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
                if (_extend_lock_instance(*instance, lock_info.resource, lock_info.random_string, ttl)) {
                    num_locked++;
                }
            }

            const auto drift = std::chrono::duration<decltype(clock_drift_factor)>(ttl) * clock_drift_factor + std::chrono::milliseconds(2);
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
        _unlock_instance(*instance, lock_info.resource, lock_info.random_string);
    }
}

void RedMutexImpl::lock() {
    do {
        // Sleep a while to mitigate lock contention.
        thread_local std::mt19937 random_gen(std::random_device{}());
        std::uniform_int_distribution<> dist(0, 5);
        std::this_thread::sleep_for(std::chrono::milliseconds(dist(random_gen)));
    } while (!try_lock());
}

void RedMutexImpl::unlock() {
    std::lock_guard<std::mutex> lock(_mtx);

    if (!_locked()) {
        // If `lock` is not called yet, the behavior is undefined.
        throw Error("RedMutex is not locked");
    }

    try {
        _unlock(_lock_id);
    } catch (...) {
        // We cannot throw exception in `unlock`,
        // because normally we call unlock in the destructor of `std::lock_guard` or `std::unique_lock`.
        // If we throw exception, the application code terminates.
        // Check issue #563 for detail.
        //_reset();
        //throw;
    }

    _reset();
}

bool RedMutexImpl::try_lock() {
    std::lock_guard<std::mutex> lock(_mtx);

    auto lock_id = RedLockUtils::lock_id();
    if (_try_lock(lock_id, _ttl)) {
        _watcher->watch(shared_from_this());
        _lock_id.swap(lock_id);
        return true;
    }

    return false;
}

bool RedMutexImpl::extend_lock() {
    std::lock_guard<std::mutex> lock(_mtx);

    if (!_locked()) {
        throw Error("cannot extend an unlocked RedMutex");
    }

    try {
        if (_extend_lock(_lock_id, _ttl) <= std::chrono::milliseconds(0)) {
            throw Error("failed to extend RedMutex");
        }
    } catch (...) {
        if (_auto_extend_err_callback) {
            try {
                _auto_extend_err_callback(std::current_exception());
            } catch (...) {
                // Ignore exceptions thrown by user code.
            }
        }

        _reset();

        return false;
    }

    return true;
}

bool RedMutexImpl::locked() {
    std::lock_guard<std::mutex> lock(_mtx);

    return _locked();
}

bool LockWatcher::Task::run() {
    auto mtx = _mtx.lock();
    if (!mtx) {
        // RedMutex already destroyed.
        return false;
    }

    if (!mtx->locked()) {
        // No longer locked.
        return false;
    }

    if (!mtx->extend_lock()) {
        // Failed to extend lock.
        return false;
    }

    _update(mtx);

    return true;
}

LockWatcher::LockWatcher() {
    _watcher_thread = std::thread([this]() { this->_run(); });
}

LockWatcher::~LockWatcher() {
    // A default constructed `Task` will end `_watcher_thread`.
    _watch(Task{});

    if (_watcher_thread.joinable()) {
        _watcher_thread.join();
    }
}

void LockWatcher::watch(const std::shared_ptr<RedMutexImpl> &mtx) {
    if (!mtx) {
        throw Error("null RedMutex to watch");
    }

    _watch(Task{mtx});
}

void LockWatcher::_watch(Task task) {
    {
        std::lock_guard<std::mutex> lock(_mtx);

        _tasks.push(std::move(task));
    }

    _cv.notify_one();
}

void LockWatcher::_run() {
    while (true) {
        auto ready_tasks = _fetch_tasks();

        auto rescheduled_tasks = _run_tasks(std::move(ready_tasks));
        if (!rescheduled_tasks) {
            // Get a terminating task, quit the loop.
            return;
        }

        _reschedule_tasks(*rescheduled_tasks);
    }
}

std::chrono::milliseconds LockWatcher::_next_schedule_time() {
    if (_tasks.empty()) {
        return std::chrono::seconds(3);
    }

    auto &task = _tasks.top();
    return task.scheduled_time();
}

auto LockWatcher::_ready_tasks() -> std::vector<Task> {
    std::vector<Task> tasks;
    while (!_tasks.empty()) {
        const auto &task = _tasks.top();
        if (!task.is_ready()) {
            break;
        }

        tasks.push_back(task);
        _tasks.pop();
    }

    return tasks;
}

auto LockWatcher::_fetch_tasks() -> std::vector<Task> {
    std::unique_lock<std::mutex> lock(_mtx);

    auto timeout = _next_schedule_time();
    if (timeout > std::chrono::milliseconds(0)) {
        // It doesn't matter if we wakeup spuriously.
        _cv.wait_for(lock, timeout);
    }

    return _ready_tasks();
}

auto LockWatcher::_run_tasks(std::vector<Task> ready_tasks) -> Optional<std::vector<Task>> {
    std::vector<Task> rescheduled_tasks;
    rescheduled_tasks.reserve(ready_tasks.size());
    for (auto &task : ready_tasks) {
        if (task.is_terminate_task()) {
#if defined REDIS_PLUS_PLUS_HAS_OPTIONAL
            return std::nullopt;
#else
            return {};
#endif
        }

        try {
            if (task.run()) {
                rescheduled_tasks.push_back(std::move(task));
            }
        } catch (...) {
            // If something bad happens, no longer watch it.
        }
    }

    return Optional<std::vector<Task>>(std::move(rescheduled_tasks));
}

void LockWatcher::_reschedule_tasks(std::vector<Task> &tasks) {
    std::lock_guard<std::mutex> lock(_mtx);

    for (auto &task : tasks) {
        _tasks.push(std::move(task));
    }
}

}

}
