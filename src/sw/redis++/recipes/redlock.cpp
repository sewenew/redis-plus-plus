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

    auto lock_ok = _try_lock(val, ttl);

    auto stop = std::chrono::steady_clock::now();
    auto elapse = stop - start;

    auto time_left = std::chrono::duration_cast<std::chrono::milliseconds>(ttl - elapse);

    if (!lock_ok || time_left < std::chrono::milliseconds(0)) {
        // Failed to lock more than half masters, or no time left for the lock.
        unlock(_resource);

        throw Error("failed to lock: " + _resource);
    }

    return time_left;
}

bool RedMutex::try_lock(const std::string &val,
        const std::chrono::time_point<std::chrono::system_clock> &tp) {
    try {
        try_lock(val, _ttl(tp));
    } catch (const Error &err) {
        return false;
    }

    return true;
}

bool RedMutex::extend_lock(const std::string &val,
        const std::chrono::time_point<std::chrono::system_clock> &tp) {
    auto lock_cnt = 0U;
    for (auto &master : _masters) {
        if (_extend_lock_master(master.get(), val, tp)) {
            ++lock_cnt;
        }
    }

    return lock_cnt >= _quorum();
}

bool RedMutex::_extend_lock_master(Redis &master,
        const std::string &val,
        const std::chrono::time_point<std::chrono::system_clock> &tp) {
    auto tx = master.transaction(true);
    auto r = tx.redis();
    try {
        auto ttl = _ttl(tp);

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
                throw Error("this should not happen");
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

std::chrono::milliseconds RedMutex::_ttl(const SysTime &tp) const {
    auto cur = std::chrono::system_clock::now();
    auto ttl = tp - cur;
    if (ttl.count() < 0) {
        throw Error("time already pasts");
    }

    return std::chrono::duration_cast<std::chrono::milliseconds>(ttl);
}

std::string RedLock::_lock_id() const {
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
            assert(false);
        }
    }

    return id;
}
}

}
