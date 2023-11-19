/**************************************************************************
   Copyright (c) 2023 sewenew

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

#ifndef SEWENEW_REDISPLUSPLUS_TEST_CLUSTER_TEST_HPP
#define SEWENEW_REDISPLUSPLUS_TEST_CLUSTER_TEST_HPP

#include "utils.h"

namespace sw {

namespace redis {

namespace test {

template <typename RedisInstance>
void ClusterTest<RedisInstance>::run() {
    _redis.for_each([](sw::redis::Redis &r) {
                REDIS_ASSERT(r.ping() == "PONG", "failed to test for_each");
            });
}

}

}

}

#endif // end SEWENEW_REDISPLUSPLUS_TEST_CLUSTER_TEST_HPP
