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

#ifndef SEWENEW_REDISPLUSPLUS_TEST_STREAM_CMDS_TEST_HPP
#define SEWENEW_REDISPLUSPLUS_TEST_STREAM_CMDS_TEST_HPP

#include <vector>
#include <string>
#include <unordered_map>
#include "utils.h"

namespace sw {

namespace redis {

namespace test {

template <typename RedisInstance>
void StreamCmdsTest<RedisInstance>::run() {
    cluster_specializing_test(*this, &StreamCmdsTest<RedisInstance>::_run, _redis);
}

template <typename RedisInstance>
void StreamCmdsTest<RedisInstance>::_run(Redis &instance) {
    auto key = test_key("stream");
    auto not_exist_key = test_key("not_exist_key");

    KeyDeleter<Redis> deleter(instance, {key, not_exist_key});

    using Result = std::unordered_map<std::string,
                    std::vector<
                        std::pair<
                            std::string,
                            std::unordered_map<std::string, std::string>>>>;

    auto res = instance.command<Optional<Result>>("xread",
                                                "count",
                                                2,
                                                "STREAMS",
                                                key,
                                                not_exist_key,
                                                "0-0",
                                                "0-0");
    REDIS_ASSERT(!res, "failed to test stream commands");

    instance.command("xadd", key, "*", "f1", "v1", "f2", "v2");

    res = instance.command<Optional<Result>>("xread",
                                            "count",
                                            2,
                                            "STREAMS",
                                            key,
                                            not_exist_key,
                                            "0-0",
                                            "0-0");
    REDIS_ASSERT(res && res->size() == 1 && res->begin()->first == key
            && res->begin()->second.size() == 1,
            "failed to test stream commands");

    const auto &fields = (res->begin()->second)[0].second;
    REDIS_ASSERT((fields == std::unordered_map<std::string, std::string>{
                    std::make_pair("f1", "v1"),
                    std::make_pair("f2", "v2")
                }),
            "failed to test stream commands");
}

}

}

}

#endif // end SEWENEW_REDISPLUSPLUS_TEST_STREAM_CMDS_TEST_HPP
