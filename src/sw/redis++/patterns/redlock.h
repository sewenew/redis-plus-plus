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

#include <cassert>
#include <random>
#include <chrono>
#include <condition_variable>
#include <queue>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include <functional>
#include "sw/redis++/redis++.h"

namespace sw {

namespace redis {

class RedLockUtils {
public:
    using SysTime = std::chrono::time_point<std::chrono::system_clock>;

    static std::chrono::milliseconds ttl(const SysTime &tp);

    static std::string lock_id();
};

class RedMutexTx {
public:
    // Lock with a single Redis master.
    RedMutexTx(std::shared_ptr<Redis> master, const std::string &resource);

    // Distributed version, i.e. lock with a list of Redis masters.
    // Only successfully acquire the lock if we can lock on more than half masters.
    RedMutexTx(std::initializer_list<std::shared_ptr<Redis>> masters,
                const std::string &resource);

    template <typename Input>
    RedMutexTx(Input first, Input last, const std::string &resource) : _masters(first, last), _resource(resource) {
        _sanity_check();
    }

    RedMutexTx(const RedMutexTx &) = delete;
    RedMutexTx& operator=(const RedMutexTx &) = delete;

    RedMutexTx(RedMutexTx &&) = delete;
    RedMutexTx& operator=(RedMutexTx &&) = delete;

    ~RedMutexTx() = default;

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
    void _sanity_check();

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

    std::vector<std::shared_ptr<Redis>> _masters;

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
        } catch (const Error &) {
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

    explicit RedLockMutexVessel(std::shared_ptr<Redis> instance);
    explicit RedLockMutexVessel(std::initializer_list<std::shared_ptr<Redis>> instances);

    template <typename Input>
    RedLockMutexVessel(Input first, Input last) : _instances(first, last) {
        _sanity_check();
    }

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
    void _sanity_check();

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
        return static_cast<int>(_instances.size() / 2 + 1);
    }

    std::vector<std::shared_ptr<Redis>> _instances;
};

class RedLockMutex
{
public:
    explicit RedLockMutex(std::shared_ptr<Redis> instance, const std::string& resource) :
        _redlock_mutex(instance), _resource(resource) {}

    explicit RedLockMutex(std::initializer_list<std::shared_ptr<Redis>> instances,
                    const std::string &resource) :
        _redlock_mutex(instances), _resource(resource) {}

    template <typename Input>
    RedLockMutex(Input first, Input last, const std::string &resource) :
        _redlock_mutex(first, last), _resource(resource) {}

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

struct RedMutexOptions {
    std::chrono::milliseconds ttl = std::chrono::seconds(3);

    // TODO: support clock drift
    //double clock_drift_factor = 0.01;

    std::chrono::milliseconds retry_delay = std::chrono::milliseconds{100};

    bool scripting = true;
};

class LockWatcher;

class RedMutexImpl : public std::enable_shared_from_this<RedMutexImpl> {
public:
    RedMutexImpl(const std::chrono::milliseconds &ttl,
            const std::shared_ptr<LockWatcher> &watcher,
            std::function<void (std::exception_ptr)> auto_extend_err_callback) :
        _ttl(ttl),
        _watcher(watcher),
        _auto_extend_err_callback(std::move(auto_extend_err_callback)) {
        if (!_watcher) {
            _watcher = std::make_shared<LockWatcher>();
        }
    }

    RedMutexImpl(const RedMutexImpl &) = delete;
    RedMutexImpl& operator=(const RedMutexImpl &) = delete;

    RedMutexImpl(RedMutexImpl &&) = delete;
    RedMutexImpl& operator=(RedMutexImpl &&) = delete;

    virtual ~RedMutexImpl() = default;

    void lock();

    void unlock();

    // @return true if extending lock successfully, false, otherwise.
    bool extend_lock();

    bool try_lock();

    bool locked();

    const std::chrono::milliseconds& ttl() {
        return _ttl;
    }

private:
    virtual void _lock(const std::string &lock_id, const std::chrono::milliseconds &ttl) = 0;

    virtual void _unlock(const std::string &lock_id) = 0;

    virtual std::chrono::milliseconds _extend_lock(const std::string &lock_id, const std::chrono::milliseconds &ttl) = 0;

    virtual bool _try_lock(const std::string &lock_id, const std::chrono::milliseconds &ttl) = 0;

    void _reset() {
        _lock_id.clear();
    }

    bool _locked() const {
        return !_lock_id.empty();
    }

    std::mutex _mtx;

    const std::chrono::milliseconds _ttl{};

    std::string _lock_id;

    std::shared_ptr<LockWatcher> _watcher;

    std::function<void (std::exception_ptr)> _auto_extend_err_callback;
};

template <typename Mutex>
class RedMutexImplTpl : public RedMutexImpl {
public:
    template <typename Input>
    RedMutexImplTpl(Input first, Input last,
            const std::string &resource,
            std::function<void (std::exception_ptr)> auto_extend_err_callback,
            const RedMutexOptions &opts,
            const std::shared_ptr<LockWatcher> &watcher) :
        RedMutexImpl(opts.ttl, watcher, std::move(auto_extend_err_callback)),
        _mtx(first, last, resource),
        _opts(opts) {}

private:
    virtual void _lock(const std::string &lock_id, const std::chrono::milliseconds &ttl) override {
        while (true) {
            auto time_left = _mtx.try_lock(lock_id, ttl);
            // TODO: Make it configurable.
            if (time_left > std::chrono::milliseconds(0)) {
                break;
            }

            std::this_thread::sleep_for(_opts.retry_delay);
        }
    }

    virtual void _unlock(const std::string &lock_id) override {
        _mtx.unlock(lock_id);
    }

    virtual std::chrono::milliseconds _extend_lock(const std::string &lock_id, const std::chrono::milliseconds &ttl) override {
        return _mtx.extend_lock(lock_id, ttl);
    }

    virtual bool _try_lock(const std::string &lock_id, const std::chrono::milliseconds &ttl) override {
        return _mtx.try_lock(lock_id, ttl) > std::chrono::milliseconds(0);
    }

    Mutex _mtx;

    RedMutexOptions _opts;
};

class RedMutex {
public:
    RedMutex(std::shared_ptr<Redis> master,
            const std::string &resource,
            std::function<void (std::exception_ptr)> auto_extend_err_callback = nullptr,
            const RedMutexOptions &opts = {},
            const std::shared_ptr<LockWatcher> &watcher = nullptr) :
        RedMutex(std::initializer_list<std::shared_ptr<Redis>>{master},
                resource, std::move(auto_extend_err_callback), opts, watcher) {}

    RedMutex(std::initializer_list<std::shared_ptr<Redis>> masters,
            const std::string &resource,
            std::function<void (std::exception_ptr)> auto_extend_err_callback = nullptr,
            const RedMutexOptions &opts = {},
            const std::shared_ptr<LockWatcher> &watcher = nullptr) :
        RedMutex(masters.begin(), masters.end(),
                resource, std::move(auto_extend_err_callback), opts, watcher) {}

    template <typename Input>
    RedMutex(Input first, Input last,
            const std::string &resource,
            std::function<void (std::exception_ptr)> auto_extend_err_callback = nullptr,
            const RedMutexOptions &opts = {},
            const std::shared_ptr<LockWatcher> &watcher = nullptr) {
        if (opts.scripting) {
            _mtx = std::make_shared<RedMutexImplTpl<RedLockMutex>>(first, last, resource,
                    std::move(auto_extend_err_callback), opts, watcher);
        } else {
            _mtx = std::make_shared<RedMutexImplTpl<RedMutexTx>>(first, last, resource,
                    std::move(auto_extend_err_callback), opts, watcher);
        }
    }

    RedMutex(const RedMutex &) = delete;
    RedMutex& operator=(const RedMutex &) = delete;

    RedMutex(RedMutex &&) = delete;
    RedMutex& operator=(RedMutex &&) = delete;

    void lock() {
        assert(_mtx);

        _mtx->lock();
    }

    void unlock() {
        assert(_mtx);

        _mtx->unlock();
    }

    bool try_lock() {
        assert(_mtx);

        return _mtx->try_lock();
    }

private:
    std::shared_ptr<RedMutexImpl> _mtx;
};

class LockWatcher {
public:
    LockWatcher();

    LockWatcher(const LockWatcher &) = delete;
    LockWatcher& operator=(const LockWatcher &) = delete;

    LockWatcher(LockWatcher &&) = delete;
    LockWatcher& operator=(LockWatcher &&) = delete;

    ~LockWatcher();

private:
    friend class RedMutexImpl;

    void watch(const std::shared_ptr<RedMutexImpl> &mtx);

    using SteadyTime = std::chrono::time_point<std::chrono::steady_clock>;

    class Task {
    public:
        // Default constructed Task will be on the top of task queue.
        Task() = default;

        explicit Task(const std::shared_ptr<RedMutexImpl> &mtx) : _mtx(mtx) {
            _update(mtx);
        }

        bool operator<(const Task &that) const {
            return _timestamp > that._timestamp;
        }

        // @return true, if we need to reschedule the task. false, otherwise.
        bool run();

        std::chrono::milliseconds scheduled_time() const {
            return std::chrono::duration_cast<std::chrono::milliseconds>(
                    _timestamp - std::chrono::steady_clock::now());
        }

        bool is_ready() const {
            // TODO: Make 10ms configurable.
            return scheduled_time() <= std::chrono::milliseconds(10);
        }

        bool is_terminate_task() const {
            return _timestamp == SteadyTime{};
        }

    private:
        void _update(const std::shared_ptr<RedMutexImpl> &mtx) {
            assert(mtx);

            _timestamp = std::chrono::steady_clock::now() + mtx->ttl() / 2;
        }

        std::weak_ptr<RedMutexImpl> _mtx;

        SteadyTime _timestamp;
    };

    void _run();

    void _watch(Task task);

    std::vector<Task> _fetch_tasks();

    std::chrono::milliseconds _next_schedule_time();

    std::vector<Task> _ready_tasks();

    // @return Tasks to be rescheduled.
    Optional<std::vector<Task>> _run_tasks(std::vector<Task> ready_tasks);

    void _reschedule_tasks(std::vector<Task> &tasks);

    std::priority_queue<Task> _tasks;

    std::mutex _mtx;

    std::condition_variable _cv;

    std::thread _watcher_thread;
};

}

}

#endif // end SEWENEW_REDISPLUSPLUS_RECIPES_REDLOCK_H
