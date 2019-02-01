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

#include "stream_cmds_test.h"
#include <vector>
#include <string>
#include <unordered_map>
#include "utils.h"

namespace sw {

namespace redis {

namespace test {

StreamCmdsTest::StreamCmdsTest(const ConnectionOptions &opts) : _redis(opts) {}

void StreamCmdsTest::run() {
    auto key = test_key("stream");
    auto not_exist_key = test_key("not_exist_key");

    KeyDeleter deleter(_redis, {key, not_exist_key});

    using Result = std::unordered_map<std::string,
                    std::vector<
                        std::pair<
                            std::string,
                            std::unordered_map<std::string, std::string>>>>;

    auto res = _redis.command<Optional<Result>>("xread",
                                                "count",
                                                2,
                                                "STREAMS",
                                                key,
                                                not_exist_key,
                                                "0-0",
                                                "0-0");
    REDIS_ASSERT(!res, "failed to test stream commands");

    _redis.command("xadd", key, "*", "f1", "v1", "f2", "v2");

    res = _redis.command<Optional<Result>>("xread",
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
