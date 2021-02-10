/**************************************************************************
   Copyright (c) 2021 Qlik

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

#ifndef SEWENEW_REDISPLUSPLUS_TEST_TIMEOUT_TEST_HPP
#define SEWENEW_REDISPLUSPLUS_TEST_TIMEOUT_TEST_HPP

#include <chrono>

#define KEY "TestKey"

namespace sw {

namespace redis {

namespace test {

template <typename RedisInstance>
TimeoutTest<RedisInstance>::TimeoutTest(RedisInstance &instance) :_redis(instance) { }

template <typename RedisInstance>
void TimeoutTest<RedisInstance>::run() {
    for(int i = 0; i < 100000; i++) {
        try{
            _redis.set(KEY, std::to_string(i));
            OptionalString stringValue = this->_redis.get(KEY);
            if (!stringValue) {
                REDIS_ASSERT(false, "Obtained null string reply");
            }
            int value = std::stoi(*stringValue);
            REDIS_ASSERT(value == i, "Obtained wrong value");
        } catch (const sw::redis::MovedError &e) {
            REDIS_ASSERT(false, e.what());
        } catch (const sw::redis::TimeoutError &) {
            //Ignore
        }  catch (const sw::redis::Error &e) {
            REDIS_ASSERT(false, e.what());
        } catch (std::invalid_argument &e) {
            REDIS_ASSERT(false, "Wrong type of value obtained");
        }
    }

    _redis.del(KEY);
}

}
}
}

#endif // end SEWENEW_REDISPLUSPLUS_TEST_TIMEOUT_TEST_HPP
