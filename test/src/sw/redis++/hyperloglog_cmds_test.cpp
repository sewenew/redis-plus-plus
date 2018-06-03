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

#include "hyperloglog_cmds_test.h"
#include "utils.h"

namespace sw {

namespace redis {

namespace test {

HyperloglogCmdTest::HyperloglogCmdTest(const ConnectionOptions &opts) : _redis(opts) {}

void HyperloglogCmdTest::run() {
    auto k1 = test_key("k1");
    auto k2 = test_key("k2");
    auto k3 = test_key("k3");

    KeyDeleter deleter(_redis, {k1, k2, k3});

    _redis.pfadd(k1, "a");
    auto members1 = {"b", "c", "d", "e", "f", "g"};
    _redis.pfadd(k1, members1);

    auto cnt = _redis.pfcount(k1);
    auto err = cnt * 1.0 / (1 + members1.size());
    REDIS_ASSERT(err < 1.02 && err > 0.98, "failed to test pfadd and pfcount");

    auto members2 = {"a", "b", "c", "h", "i", "j", "k"};
    _redis.pfadd(k2, members2);
    auto total = 1 + members1.size() + members2.size() - 3;

    cnt = _redis.pfcount({k1, k2});
    err = cnt * 1.0 / total;
    REDIS_ASSERT(err < 1.02 && err > 0.98, "failed to test pfcount");

    _redis.pfmerge(k3, {k1, k2});
    cnt = _redis.pfcount(k3);
    err = cnt * 1.0 / total;
    REDIS_ASSERT(err < 1.02 && err > 0.98, "failed to test pfcount");
}

}

}

}
