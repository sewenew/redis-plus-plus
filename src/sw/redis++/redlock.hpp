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

#ifndef REDISPLUSPLUS_REDLOCK_H
#define REDISPLUSPLUS_REDLOCK_H

#include "redis++.h"
#include <unistd.h>
#include <openssl/rc4.h>

namespace sw {

namespace redis {

class RandomBufferInterface
{
public:
	virtual ~RandomBufferInterface() {}
	virtual std::string getUpdatedString() = 0;
};

template <typename RedisInstance>
class Redlock
{
public:
	// In the constructor, we load teh scripts and initialize their
	// SHA1s so that we can check if the script exists, before calling them.
	Redlock(RedisInstance& instance, RandomBufferInterface& randomBuffer) :
		_randomBuffer(randomBuffer),
		_instance(instance),
		_unlockScript("\
if redis.call(\"GET\",KEYS[1]) == ARGV[1] then \
  return redis.call(\"del\",KEYS[1]) \
else \
  return 0 \
end \
"),
		_extendLockScript("\
if redis.call(\"GET\",KEYS[1]) == ARGV[1] then \
  return redis.call(\"pexpire\",KEYS[1],ARGV[2]) \
else \
  return 0 \
end \
"),
		_unlockSHA1(loadScript(_unlockScript)),
		_extendLockSHA1(loadScript(_extendLockScript))
	{
	}

	// Only locks if no lock for the supplied key exists.
	// The lock will stay valid until unlock() is called,
	// or until the ttl period expired.
	bool lock(const std::string& key, const std::chrono::milliseconds& ttl) {
		const auto randomString = _randomBuffer.getUpdatedString();
		bool result = _instance.set(key, randomString, ttl, UpdateType::NOT_EXIST);
		if (result) {
			_randomNumberMap[key] = randomString;
		}
		return result;
	}

	// An existing lock could be extended by ttl milliseconds.
	// This is handy for safely holding onto a lock, by calling
	// extend_lock periodically within the ttl period. It the
	// original lock expired, before calling extend_lock(),
	// the lock will not be extended.
	bool extend_lock(const std::string& key, const std::chrono::milliseconds& ttl);

	// If a lock exists and this instance is the owner of the lock,
	// the lock will be released.
	void unlock(const std::string& key);

private:
	void checkAndLoad(Redis& instance, const std::string& sha1, const std::string& scriptString);
	std::string loadScript(const std::string& script);

	RandomBufferInterface& _randomBuffer;
	RedisInstance& _instance;
	const std::string _unlockScript;
	const std::string _extendLockScript;
	const std::string _unlockSHA1;
	const std::string _extendLockSHA1;
	std::map<std::string, std::string> _randomNumberMap;
};

} // namespace sw

} // namespace redis

#endif // end REDISPLUSPLUS_REDLOCK_H
