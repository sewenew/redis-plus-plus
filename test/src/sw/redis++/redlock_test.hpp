/**************************************************************************
   Copyright (c) 2019

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

#ifndef REDISPLUSPLUS_REDLOCK_CMDS_TEST_HPP
#define REDISPLUSPLUS_REDLOCK_CMDS_TEST_HPP

#include <sw/redis++/redis++.h>
#include <queue>
#include <iostream>
#include <thread>

namespace sw {

namespace redis {

namespace test {

class RandomBufferInterface
{
public:
    virtual ~RandomBufferInterface() {}
    virtual std::string get_updated_string() = 0;
};

#ifdef USE_OPENSSL
#include <openssl/rc4.h>

template <int N = 20>
class RandomBuffer : public RandomBufferInterface
{
public:
    RandomBuffer() :
        _in_idx(0),
        _out_idx(1)
    {
        int random_num = std::rand();
        const auto rn_size = sizeof(random_num);
        for (size_t i=0; i<N; i+=rn_size) {
            memcpy(&_data[_in_idx][i], &random_num, (N - i >= rn_size) ? rn_size : N - i);
            random_num = std::rand();
        }
        RC4_set_key(&_key, N, _data[0]);
    }

    std::string get_updated_string() {
        RC4(&_key, N, _data[_in_idx], _data[_out_idx]);
        // Swap the in and out buffers.
        if (_in_idx == 0) {
            _in_idx = 1;
            _out_idx = 0;
        }
        else {
            _in_idx = 0;
            _out_idx = 1;
        }
        return std::string((char*)(_data[_in_idx]), N);
    }

private:
    uint8_t _in_idx;
    uint8_t _out_idx;
    uint8_t _data[2][N];
    RC4_KEY _key;
};

#else // !USE_OPENSSL

template <int N = 20>
class RandomBuffer : public sw::redis::RandomBufferInterface
{
public:
    std::string get_updated_string() {
        return RedLockMutexVessel::lock_id();
    }
};

#endif // USE_OPENSSL

template <>
void RedLockTest<RedisCluster>::run() {
    // Not applicable.
}

template <>
void RedLockTest<Redis>::run() {
    std::srand(std::time(nullptr));
    RandomBuffer<> random_buffer;

    RedLockMutexVessel redlock(std::ref(_redis));

    const auto resource = test_key(RedLockUtils::lock_id());
    const auto random_string = random_buffer.get_updated_string();
    const std::chrono::milliseconds ttl(200);


    // Test if we can obtain a lock.
    {
        const auto lock_info = redlock.lock(resource, random_string, ttl);
        if (lock_info.locked) {
            redlock.unlock(lock_info);
        }
        else {
            REDIS_ASSERT(0, "unable to obtain a lock");
        }
    }

    const int n = 1000;
    auto start = std::chrono::system_clock::now();
    // Use a 2 second TTL for the multi lock tests, as
    // getting all these locks might take long.
    const std::chrono::milliseconds multi_lock_ttl(2000);
    // Test if we can obtain n locks with 1 RedLockMutexVessel instance.
    {
        std::queue<RedLockMutexVessel::LockInfo> lock_infoList;
        for (int i=0; i<n; i++) {
            const auto lock_info = redlock.lock(RedLockUtils::lock_id(), random_string, multi_lock_ttl);
            if (lock_info.locked) {
                lock_infoList.push(lock_info);
            }
            else {
                REDIS_ASSERT(0, "unable to obtain a lock");
            }
        }
        while (lock_infoList.size() != 0) {
            redlock.unlock(lock_infoList.front());
            lock_infoList.pop();
        }
    }
    auto end = std::chrono::system_clock::now();
    std::chrono::duration<double> diff = end-start;
    std::cout << "Time to lock and unlock " << n << " simultaneous locks with RedLockMutexVessel: " << diff.count() << " s" << std::endl;;

    start = std::chrono::system_clock::now();
    // Test if we can obtain n locks with a n RedLockMutex instances.
    {
        std::queue<RedLockMutex*> mutex_list;
        for (int i=0; i<n; i++) {
            mutex_list.push(new RedLockMutex(std::ref(_redis), RedLockUtils::lock_id()));
            const std::chrono::time_point<std::chrono::system_clock> tp = std::chrono::system_clock::now() + multi_lock_ttl;
            if (!(mutex_list.back()->try_lock(random_string, tp))) {
                std::cout << "Num locks = " << i << std::endl;;
                REDIS_ASSERT(0, "unable to obtain a lock");
            }
        }
        while (mutex_list.size() != 0) {
            mutex_list.front()->unlock(random_string);
            delete(mutex_list.front());
            mutex_list.pop();
        }
    }
    end = std::chrono::system_clock::now();
    diff = end-start;
    std::cout << "Time to lock and unlock " << n << " simultaneous locks with RedLockMutex: " << diff.count() << " s" << std::endl;;

    start = std::chrono::system_clock::now();
    // Test if we can obtain a n locks with a n RedMutex instances.
    {
        std::queue<RedMutex*> mutex_list;
        for (int i=0; i<n; i++) {
            const std::chrono::time_point<std::chrono::system_clock> tp = std::chrono::system_clock::now() + multi_lock_ttl;
            mutex_list.push(new RedMutex(std::ref(_redis), RedLockUtils::lock_id()));
            if (!(mutex_list.back()->try_lock(random_string, tp))) {
                REDIS_ASSERT(0, "unable to obtain a lock");
            }
        }
        while (mutex_list.size() != 0) {
            mutex_list.front()->unlock(random_string);
            delete(mutex_list.front());
            mutex_list.pop();
        }
    }
    end = std::chrono::system_clock::now();
    diff = end-start;
    std::cout << "Time to lock and unlock " << n << " simultaneous locks with RedMutex: " << diff.count() << " s" << std::endl;;

    // Test if the lock fails if we try to lock a key, after
    // a lock was already obtained.
    {
        const auto lock_info = redlock.lock(resource, random_string, ttl);
        if (lock_info.locked) {
            const auto lock_infoFail = redlock.lock(resource, random_string, ttl, 1);
            if (lock_infoFail.locked) {
                redlock.unlock(lock_infoFail);
                REDIS_ASSERT(0, "managed to get lock from redlock, while redlock was locked");
            }
            redlock.unlock(lock_info);
        }
        else {
            REDIS_ASSERT(0, "unable to obtain a lock");
        }
    }

    // Test if the lock succeeds if we're trying to obtain a lock,
    // after the TTL expired of the original lock, when the original
    // lock was not unlocked.
    {
        const auto lock_info = redlock.lock(resource, random_string, ttl);
        if (lock_info.locked) {
            // Sleep TTL duration + 50 mSec.
            std::this_thread::sleep_for(ttl + std::chrono::milliseconds(50));
            const auto lock_info_ok = redlock.lock(resource, random_string, ttl);
            if (lock_info_ok.locked) {
                redlock.unlock(lock_info_ok);
            }
            else {
                redlock.unlock(lock_info);
                REDIS_ASSERT(0, "redlock lock was not automatically released after TTL expired");
            }
        }
        else {
            REDIS_ASSERT(0, "unable to obtain a lock");
        }
    }

    // Test if the lock succeeds if we're trying to obtain a lock,
    // after 2/3 of TTL expired of the original lock. The new lock
    // should have enough time to aquire a lock, if we give it 20
    // retries.
    {
        const auto lock_info = redlock.lock(resource, random_string, ttl);
        if (lock_info.locked) {
            // Sleep 2/3 of the TTL duration.
            std::this_thread::sleep_for(ttl * 2 / 3);
            // We'll now try to get a lock with for TTL/2 mSec, retrying every mSec.
            const auto lock_info_ok = redlock.lock(resource, random_string, ttl, ttl.count() / 2, std::chrono::milliseconds(1));
            if (lock_info_ok.locked) {
                redlock.unlock(lock_info_ok);
            }
            else {
                redlock.unlock(lock_info);
                REDIS_ASSERT(0, "redlock lock retry machanism failed");
            }
        }
        else {
            REDIS_ASSERT(0, "unable to obtain a lock");
        }
    }

    // Test if the lock is extendable.
    {
        const auto lock_info = redlock.lock(resource, random_string, ttl);
        if (lock_info.locked) {
            // We'll sleep 2/3 of the ttl.
            std::this_thread::sleep_for(ttl * 2 / 3);
            // Now we have 1/3 of the ttl left, so we extend it by ttl.
            const auto lock_info_ext = redlock.extend_lock(lock_info, ttl);
            if (lock_info_ext.locked) {
                // We'll sleep 2/3 of the ttl.
                std::this_thread::sleep_for(ttl * 2 / 3);
                // Now we have 1/3 of the ttl left, if the extend_lock worked,
                // so locking should fail if we do a single lock (no retrying).
                const auto lock_infoTime = redlock.lock(resource, random_string, ttl, 0);
                if (lock_infoTime.locked) {
                    redlock.unlock(lock_infoTime);
                    REDIS_ASSERT(0, "redlock extend_lock failed to extend the timeout");
                }
                else {
                    redlock.unlock(lock_info_ext);
                }
            }
            else {
                redlock.unlock(lock_info);
                REDIS_ASSERT(0, "redlock extend_lock failed, although the lock exists");
            }
        }
        else {
            REDIS_ASSERT(0, "unable to obtain a lock");
        }
    }

    // Locking should fail, on duplicate instances.
    {
        // We now use the same instance twice, which is expected to fail on locking.
        RedLockMutexVessel redlock_2_identical_instances({std::ref(_redis), std::ref(_redis)});
        const auto lock_info = redlock_2_identical_instances.lock(resource, random_string, ttl);
        if (lock_info.locked) {
            redlock_2_identical_instances.unlock(lock_info);
            REDIS_ASSERT(0, "redlock managed to lock the same instance twice");
        }
        else {
            redlock_2_identical_instances.unlock(lock_info);
        }
    }

}

} // namespace test

} // namespace redis

} // namespace sw

#endif // REDISPLUSPLUS_REDLOCK_CMDS_TEST_HPP
