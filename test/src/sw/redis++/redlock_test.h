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

#ifndef REDISPLUSPLUS_TEST_REDLOCK_TEST_H
#define REDISPLUSPLUS_TEST_REDLOCK_TEST_H

#include <sw/redis++/redis++.h>
#include <sw/redis++/patterns/redlock.h>
#include <memory>

namespace sw {

namespace redis {

namespace test {

template <typename RedisInstance>
class RedLockTest {
public:
    explicit RedLockTest(std::shared_ptr<RedisInstance> instance) : _redis(std::move(instance)) {}

    void run();

private:
    std::shared_ptr<RedisInstance> _redis;
};

} // namespace test

} // namespace redis

} // namespace sw

#include "redlock_test.hpp"

#endif // end REDISPLUSPLUS_TEST_REDLOCK_TEST_H
