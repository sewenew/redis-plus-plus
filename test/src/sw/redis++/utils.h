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

#ifndef SEWENEW_REDISPLUSPLUS_TEST_UTILS_H
#define SEWENEW_REDISPLUSPLUS_TEST_UTILS_H

#include <iostream>

#define REDIS_ASSERT(condition, msg) \
    sw::redis::test::redis_assert((condition), (msg), __FILE__, __LINE__)

namespace sw {

namespace redis {

namespace test {

inline void redis_assert(bool condition,
                            const std::string &msg,
                            const std::string &file,
                            int line) {
    if (!condition) {
        std::cerr << "ASSERT: " << msg << ". " << file << ":" << line << std::endl;
    }
}

inline std::string test_key(const std::string &k) {
    return "sw:redis:test:" + k;
}

}

}

}

#endif // end SEWENEW_REDISPLUSPLUS_TEST_UTILS_H
