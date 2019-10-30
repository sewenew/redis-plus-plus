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

namespace sw {

namespace redis {

std::chrono::milliseconds RedMutex::try_lock(const std::string &val, const std::chrono::milliseconds &ttl) {
    auto start = std::chrono::steady_clock::now();

    if (!_redis.set(_resource, val, ttl, UpdateType::NOT_EXIST)) {
        throw Error("failed to lock " + _resource);
    }

    auto stop = std::chrono::steady_clock::now();
    auto elapse = stop - start;

    auto time_left = std::chrono::duration_cast<std::chrono::milliseconds>(ttl - elapse);

    if (time_left < std::chrono::milliseconds(0)) {
        // No time left for the lock.
        try {
            unlock(_resource);
        } catch (const Error &err) {
            throw Error("failed to lock " + _resource);
        }
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
    auto tx = _redis.transaction(true);
    auto r = tx.redis();
    try {
        auto ttl = _ttl(tp);

        r.watch(_resource);

        auto id = r.get(_resource);
        if (id && *id == val) {
            auto reply = tx.pexpire(_resource, ttl).exec();
            if (!reply.get<bool>(0)) {
                throw Error("this should not happen");
            }
        }
    } catch (const Error &err) {
        // key has been modified or other error happens, failed to extend the lock.
        return false;
    }

    return true;
}

void RedMutex::unlock(const std::string &val) {
    auto tx = _redis.transaction(true);
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
    } catch(const WatchError &err) {
        // key has been modified. Do nothing, just let it go.
    }
}

std::chrono::milliseconds RedMutex::_ttl(const SysTime &tp) const {
    auto cur = std::chrono::system_clock::now();
    auto ttl = tp - cur;
    if (ttl.count() < 0) {
        throw Error("time already pasts");
    }

    return std::chrono::duration_cast<std::chrono::milliseconds>(ttl);
}

}

}
