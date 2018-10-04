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

#ifndef SEWENEW_REDISPLUSPLUS_TEST_CLUSTER_TEST_H
#define SEWENEW_REDISPLUSPLUS_TEST_CLUSTER_TEST_H

#include <string>
#include <sw/redis++/redis++.h>

namespace sw {

namespace redis {

namespace test {

class ClusterTest {
public:
    explicit ClusterTest(const ConnectionOptions &opts);

    void run();

private:
    void _test_cluster();

    void _test_hash_tag();

    void _test_hash_tag(std::initializer_list<std::string> keys);

    RedisCluster _redis_cluster;
};

}

}

}

#endif // end SEWENEW_REDISPLUSPLUS_TEST_CLUSTER_TEST_H
