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

#include <thread>

namespace sw {

namespace redis {

template <typename RedisInstance>
RedLockMutexVessel<RedisInstance>::RedLockMutexVessel(RedisInstance& instance) :
    RedLockMutexVessel<RedisInstance>({{instance}})
{
}

template <typename RedisInstance>
RedLockMutexVessel<RedisInstance>::RedLockMutexVessel(std::initializer_list<std::reference_wrapper<RedisInstance>> instances) :
    _instances(instances.begin(), instances.end())
{
}

template <typename RedisInstance>
RedLockMutexVessel<RedisInstance>::RedLockMutexVessel(std::vector<std::reference_wrapper<RedisInstance>> instances) :
    _instances(instances.begin(), instances.end())
{
}

template <typename RedisInstance>
bool RedLockMutexVessel<RedisInstance>::_extend_lock_instance(RedisInstance& instance,
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
    auto result = instance.template eval<long long>(script, {resource}, {random_string, std::to_string(ttl.count())});
    return result != 0;
}

template <typename RedisInstance>
void RedLockMutexVessel<RedisInstance>::_unlock_instance(RedisInstance& instance,
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
    instance.template eval<long long>(script, {resource}, {random_string});
}

template <typename RedisInstance>
bool RedLockMutexVessel<RedisInstance>::_lock_instance(RedisInstance& instance,
                                  const std::string& resource,
                                  const std::string& random_string,
                                  const std::chrono::milliseconds& ttl)
{
    const auto result = instance.set(resource, random_string, ttl, UpdateType::NOT_EXIST);
    return result;
}

template <typename RedisInstance>
typename RedLockMutexVessel<RedisInstance>::LockInfo
RedLockMutexVessel<RedisInstance>::lock(const std::string& resource,
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

template <typename RedisInstance>
typename RedLockMutexVessel<RedisInstance>::LockInfo
RedLockMutexVessel<RedisInstance>::extend_lock(const RedLockMutexVessel::LockInfo& lock_info,
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

template <typename RedisInstance>
void RedLockMutexVessel<RedisInstance>::unlock(const LockInfo& lock_info)
{
    for (auto& instance : _instances) {
        _unlock_instance(instance, lock_info.resource, lock_info.random_string);
    }
}

}

}
