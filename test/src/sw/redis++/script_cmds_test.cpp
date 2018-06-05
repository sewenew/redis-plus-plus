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

#include "script_cmds_test.h"
#include <list>
#include <vector>
#include "utils.h"

namespace sw {

namespace redis {

namespace test {

ScriptCmdTest::ScriptCmdTest(const ConnectionOptions &opts) : _redis(opts) {}

void ScriptCmdTest::run() {
    auto script = "return 1";
    auto num = _redis.eval<long long>(script, {}, {});
    REDIS_ASSERT(num == 1, "failed to test eval");

    auto script_with_args = "return {ARGV[1] + 1, ARGV[2] + 2, ARGV[3] + 3}";
    std::vector<long long> res;
    _redis.eval(script_with_args,
                {"k"},
                {"1", "2", "3"},
                std::back_inserter(res));
    REDIS_ASSERT(res == std::vector<long long>({2, 4, 6}),
            "failed to test eval with array reply");

    auto sha1 = _redis.script_load(script);
    num = _redis.evalsha<long long>(sha1, {}, {});
    REDIS_ASSERT(num == 1, "failed to test evalsha");

    auto sha2 = _redis.script_load(script_with_args);
    res.clear();
    _redis.evalsha(sha2,
                    {"k"},
                    {"1", "2", "3"},
                    std::back_inserter(res));
    REDIS_ASSERT(res == std::vector<long long>({2, 4, 6}),
            "failed to test evalsha with array reply");

    std::list<bool> exist_res;
    _redis.script_exists({sha1, sha2, std::string("not exist")}, std::back_inserter(exist_res));
    REDIS_ASSERT(exist_res == std::list<bool>({true, true, false}),
            "failed to test script exists");

    _redis.script_flush();
    exist_res.clear();
    _redis.script_exists({sha1, sha2, std::string("not exist")}, std::back_inserter(exist_res));
    REDIS_ASSERT(exist_res == std::list<bool>({false, false, false}),
            "failed to test script flush");
}

}

}

}
