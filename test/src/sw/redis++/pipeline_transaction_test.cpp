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

#include "pipeline_transaction_test.h"
#include "utils.h"

namespace sw {

namespace redis {

namespace test {

PipelineTransactionTest::PipelineTransactionTest(const ConnectionOptions &opts) : _redis(opts) {}

void PipelineTransactionTest::run() {
    _test_pipeline();

    _test_transaction(true);

    _test_transaction(false);
}

void PipelineTransactionTest::_test_pipeline() {
    auto pipe = _redis.pipeline();

    auto key = test_key("pipeline");

    KeyDeleter deleter(_redis, key);

    std::string val("value");
    auto replies = pipe.set(key, val)
                        .get(key)
                        .strlen(key)
                        .exec();

    replies.pop<void>();
    auto new_val = replies.pop<OptionalString>();
    std::size_t len = replies.pop<long long>();
    REDIS_ASSERT(bool(new_val) && *new_val == val && len == val.size(),
            "failed to test pipeline with string operations");

    pipe.del(key).lpush(key, val);

    pipe.discard();
}

void PipelineTransactionTest::_test_transaction(bool piped) {
    auto tx = _redis.transaction(piped);

    auto key = test_key("transaction");

    KeyDeleter deleter(_redis, key);

    std::unordered_map<std::string, std::string> m = {
        std::make_pair("f1", "v1"),
        std::make_pair("f2", "v2"),
        std::make_pair("f3", "v3")
    };
    auto replies = tx.hmset(key, m.begin(), m.end())
                        .hgetall(key)
                        .hdel(key, "f1")
                        .exec();

    replies.pop();

    decltype(m) mm;
    replies.pop(std::inserter(mm, mm.end()));
    REDIS_ASSERT(mm == m, "failed to test transaction");

    REDIS_ASSERT(replies.pop<long long>() == 1, "failed to test transaction");
}

}

}

}
