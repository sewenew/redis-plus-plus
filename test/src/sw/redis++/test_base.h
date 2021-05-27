/**************************************************************************
   Copyright (c) 2021 sewenew, wingunder

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

#ifndef SEWENEW_REDISPLUSPLUS_TEST_TEST_BASE_H
#define SEWENEW_REDISPLUSPLUS_TEST_TEST_BASE_H

#include <sw/redis++/redis++.h>

namespace sw::redis::test {

class TestBase {
public:
    TestBase(const std::string& db_name)
        : _db_name(db_name) {}

    std::string test_key(const std::string &k) const {
        // Key prefix with hash tag,
        // so that we can call multiple-key commands on RedisCluster.
        return "{" + _db_name + "}::" + k;
    }

    std::string get_db_name() const { return _db_name; }

private:
    const std::string& _db_name;
};

}

#endif
