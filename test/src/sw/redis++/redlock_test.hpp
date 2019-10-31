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

namespace sw {

namespace redis {

namespace test {

#ifdef USE_OPENSSL
#include <openssl/rc4.h>

template <int N = 20>
class RandomBuffer : public sw::redis::RandomBufferInterface
{
public:
	RandomBuffer() :
		_inIdx(0),
		_outIdx(1)
	{
		int randomNum = std::rand();
		const auto RN_SIZE = sizeof(randomNum);
		for (size_t i=0; i<N; i+=RN_SIZE) {
			memcpy(&_data[_inIdx][i], &randomNum, (N - i >= RN_SIZE) ? RN_SIZE : N - i);
			randomNum = std::rand();
		}
		RC4_set_key(&_key, N, _data[0]);
	}

	std::string getUpdatedString() {
		RC4(&_key, N, _data[_inIdx], _data[_outIdx]);
		// Swap the in and out buffers.
		if (_inIdx == 0) {
			_inIdx = 1;
			_outIdx = 0;
		}
		else {
			_inIdx = 0;
			_outIdx = 1;
		}
		return std::string((char*)(_data[_inIdx]), N);
	}

private:
	uint8_t _inIdx;
	uint8_t _outIdx;
	uint8_t _data[2][N];
	RC4_KEY _key;
};

#else // !USE_OPENSSL

template <int N = 20>
class RandomBuffer : public sw::redis::RandomBufferInterface
{
public:
	// The following is dead-slow.
	// Only use this for testing, if no openssl lib is installed.
	std::string getUpdatedString() {
		int randomNum = std::rand();
		const auto RN_SIZE = sizeof(randomNum);
		for (size_t i=0; i<N; i+=RN_SIZE) {
			memcpy(&_data[i], &randomNum, (N - i >= RN_SIZE) ? RN_SIZE : N - i);
			randomNum = std::rand();
		}
		return std::string((char*)_data, N);
	}

private:
	uint8_t _data[N];
};

#endif // USE_OPENSSL

template <typename RedisInstance>
void RedlockTest<RedisInstance>::run() {
	std::srand(std::time(nullptr));
	RandomBuffer<> randomBuffer;
	Redlock<RedisInstance> redLock(_redis, randomBuffer);

	const auto lockKey = test_key("some_random_key_name.lock");
	const std::chrono::milliseconds ttl(200);

	// 1. Test if we can obtain a lock.
	if (redLock.lock(lockKey, ttl)) {
		redLock.unlock(lockKey);
	}
	else {
		REDIS_ASSERT(0, "unable to obtain a lock.");
	}

	// 2. Test if the lock fails if we try to lock a key, after
	//    a lock was already obtained.
	if (redLock.lock(lockKey, ttl)) {
		if (redLock.lock(lockKey, ttl)) {
			REDIS_ASSERT(0, "managed to get lock from redlock, while redlock was locked.");
		}
		redLock.unlock(lockKey);
	}
	else {
		REDIS_ASSERT(0, "unable to obtain a lock.");
	}

	// 3. Test if the lock succeeds if we're trying to obtain a lock,
	//    after the TTL expired of the original lock, when the original
	//    lock was not unlocked.
	if (redLock.lock(lockKey, ttl)) {
		// Sleep twice the TTL duration.
		usleep(2 * ttl.count() * 1000);
		if (redLock.lock(lockKey, ttl)) {
			redLock.unlock(lockKey);
		}
		else {
			redLock.unlock(lockKey);
			REDIS_ASSERT(0, "redlock lock was not automatically released after TTL expired.");
		}
	}
	else {
		REDIS_ASSERT(0, "unable to obtain a lock.");
	}

	// 4. Test if the lock is extendable.
	if (redLock.lock(lockKey, ttl)) {
		// We'll sleep 2/3 of the ttl.
		usleep(ttl.count() * 1000 * 2 / 3);
		// Now we have 1/3 of the ttl left, so we extend it by ttl.
		if (redLock.extend_lock(lockKey, ttl)) {
			// We'll sleep 2/3 of the ttl.
			usleep(ttl.count() * 1000 * 2 / 3);
			// Now we have 1/3 of the ttl left, if the extend_lock worked,
			// so locking should fail.
			if (redLock.lock(lockKey, ttl)) {
				redLock.unlock(lockKey);
				REDIS_ASSERT(0, "redlock extend_lock failed to extend the TTL.");
			}
			else {
				redLock.unlock(lockKey);
			}
		}
		else {
			redLock.unlock(lockKey);
			REDIS_ASSERT(0, "redlock extend_lock failed, although the lock exists.");
		}
	}
	else {
		REDIS_ASSERT(0, "unable to obtain a lock.");
	}

	// 5. Test if the an extended lock times out at the right time.
	if (redLock.lock(lockKey, ttl)) {
		// We'll sleep 2/3 of the ttl.
		usleep(ttl.count() * 1000 * 2 / 3);
		// Now we have 1/3 of the ttl left, so we extend it by ttl.
		if (redLock.extend_lock(lockKey, ttl)) {
			// We'll sleep ttl + 2 millis and see if the lock is gone.
			// 1 ms for extend_lock() and 1 ms for Redis expire latency.
			usleep((ttl.count() + 2) * 1000);
			// Locking it should not fail.
			if (redLock.lock(lockKey, ttl)) {
				redLock.unlock(lockKey);
			}
			else {
				redLock.unlock(lockKey);
				REDIS_ASSERT(0, "redlock extend_lock failed to release after TTL.");
			}
		}
		else {
			redLock.unlock(lockKey);
			REDIS_ASSERT(0, "redlock extend_lock failed, although the lock exists.");
		}
	}
	else {
		REDIS_ASSERT(0, "unable to obtain a lock.");
	}

	// 6. Test if ~Redlock() releases all the locked keys.
	{
		Redlock<RedisInstance> tmpRedLock(_redis, randomBuffer);
		if (tmpRedLock.lock(lockKey, ttl)) {
			// Don't unlock, as we're testing the destructor's unlock.
		}
		else {
			REDIS_ASSERT(0, "unable to obtain a lock.");
		}
	}
	// We destructed within TTL, so we should be able to lock,
	// if the destructor unlocked the key.
	if (redLock.lock(lockKey, ttl)) {
		redLock.unlock(lockKey);
	}
	else {
		redLock.unlock(lockKey);
		REDIS_ASSERT(0, "The Redlock destructor failed to unlock.");
	}
}

} // namespace test

} // namespace redis

} // namespace sw

#endif // REDISPLUSPLUS_REDLOCK_CMDS_TEST_HPP
