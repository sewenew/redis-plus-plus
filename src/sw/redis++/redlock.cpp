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

#include "redlock.hpp"

namespace sw {

namespace redis {

template <typename T>
void Redlock<T>::checkAndLoad(Redis& instance, const std::string& sha1, const std::string& scriptString) {
	std::list<bool> replyList;
	instance.script_exists({sha1, std::string("")}, std::back_inserter(replyList));
	if (replyList.size() != 2) {
		throw Error("script_exists returned an invalid list size.");
	}
	else if (!*replyList.begin()) {
		instance.script_load(scriptString);
	}
}

template <>
bool Redlock<RedisCluster>::extend_lock(const std::string& key, const std::chrono::milliseconds& ttl) {
	bool result = false;
	auto item = _randomNumberMap.find(key);
	if (item != _randomNumberMap.end()) {
		Redis instance = _instance.redis(key);
		checkAndLoad(instance, _extendLockSHA1, _extendLockScript);
		result = instance.evalsha<long long>(_extendLockSHA1, {key}, {item->second, std::to_string(ttl.count())});
	}
	return result;
}

template <>
bool Redlock<Redis>::extend_lock(const std::string& key, const std::chrono::milliseconds& ttl) {
	bool result = false;
	const auto item = _randomNumberMap.find(key);
	if (item != _randomNumberMap.end()) {
		result = _instance.evalsha<long long>(_extendLockSHA1, {key}, {item->second, std::to_string(ttl.count())});
	}
	return result;
}

template <>
void Redlock<RedisCluster>::unlock(const std::string& key) {
	auto item = _randomNumberMap.find(key);
	if (item != _randomNumberMap.end()) {
		Redis instance = _instance.redis(key);
		checkAndLoad(instance, _unlockSHA1, _unlockScript);
		instance.evalsha<long long>(_unlockSHA1, {key}, {item->second});
		_randomNumberMap.erase(item);
	}
}

template <>
void Redlock<Redis>::unlock(const std::string& key) {
	const auto item = _randomNumberMap.find(key);
	if (item != _randomNumberMap.end()) {
		_instance.evalsha<long long>(_unlockSHA1, {key}, {item->second});
		_randomNumberMap.erase(item);
	}
}

template <>
std::string Redlock<RedisCluster>::loadScript(const std::string& script) {
	return _instance.redis("").script_load(script);
}

template <>
	std::string Redlock<Redis>::loadScript(const std::string& script) {
	return _instance.script_load(script);
}

} // namespace sw

} // namespace redis
