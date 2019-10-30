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
#include <string>
//#include "../redis++.h"
#include <sw/redis++/redis++.h>

namespace sw {

namespace redis {

class RedMutex {
public:
    RedMutex(Redis &redis, const std::string &resource) : _redis(redis), _resource(resource) {}

    std::chrono::milliseconds try_lock(const std::string &val, const std::chrono::milliseconds &ttl);

    bool try_lock(const std::string &val,
            const std::chrono::time_point<std::chrono::system_clock> &tp);

    bool extend_lock(const std::string &val,
            const std::chrono::time_point<std::chrono::system_clock> &tp);

    void unlock(const std::string &val);

private:
    using SysTime = std::chrono::time_point<std::chrono::system_clock>;

    std::chrono::milliseconds _ttl(const SysTime &tp) const;

    Redis &_redis;

    std::string _resource;
};

template <typename Mutex>
class RedLock {
public:
    RedLock(Mutex &mut, std::defer_lock_t) : _mut(mut), _lock_val(_lock_id()) {}

    ~RedLock() {
        if (_owned) {
            unlock();
        }
    }

    std::chrono::milliseconds try_lock(const std::chrono::milliseconds &ttl) {
        return _mut.try_lock(_lock_val, ttl);
    }

    void unlock() {
        _mut.unlock(_lock_val);
    }

private:
    std::string _lock_id() {
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

    Mutex &_mut;

    bool _owned = false;

    std::string _lock_val;
};

}

}

#endif // end SEWENEW_REDISPLUSPLUS_RECIPES_REDLOCK_H
