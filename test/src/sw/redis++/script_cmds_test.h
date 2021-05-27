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

#ifndef SEWENEW_REDISPLUSPLUS_TEST_SCRIPT_CMDS_TEST_H
#define SEWENEW_REDISPLUSPLUS_TEST_SCRIPT_CMDS_TEST_H

#include "test_base.h"

namespace sw {

namespace redis {

namespace test {

template <typename RedisInstance>
class ScriptCmdTest : public TestBase {
public:
    explicit ScriptCmdTest(RedisInstance &instance, const std::string& db_name, bool do_flush)
        : TestBase(db_name), _redis(instance), _do_flush(do_flush) {}

    void run();

private:
    void _run(Redis &instance);

    RedisInstance &_redis;

    const bool _do_flush;
};

}

}

}

#include "script_cmds_test.hpp"

#endif // end SEWENEW_REDISPLUSPLUS_TEST_SCRIPT_CMDS_TEST_H
