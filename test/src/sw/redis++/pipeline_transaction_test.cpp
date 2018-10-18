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
#include <string>
#include "utils.h"

namespace sw {

namespace redis {

namespace test {

PipelineTransactionTest::PipelineTransactionTest(const ConnectionOptions &opts,
                                                    const ConnectionOptions &cluster_opts) :
                                                        _redis(opts), _cluster(cluster_opts) {}

void PipelineTransactionTest::run() {
    {
        auto key = test_key("pipeline");
        KeyDeleter deleter(_redis, key);
        auto pipe = _redis.pipeline();
        _test_pipeline(key, pipe);
    }

    {
        auto key = test_key("pipeline");
        ClusterKeyDeleter deleter(_cluster, key);
        auto pipe = _cluster.pipeline(key);
        _test_pipeline(key, pipe);
    }

    {
        auto key = test_key("transaction");
        KeyDeleter deleter(_redis, key);
        auto tx = _redis.transaction(true);
        _test_transaction(key, tx);
    }

    {
        auto key = test_key("transaction");
        KeyDeleter deleter(_redis, key);
        auto tx = _redis.transaction(false);
        _test_transaction(key, tx);
    }

    {
        auto key = test_key("transaction");
        ClusterKeyDeleter deleter(_cluster, key);
        auto tx = _cluster.transaction(key, true);
        _test_transaction(key, tx);
    }

    {
        auto key = test_key("transaction");
        ClusterKeyDeleter deleter(_cluster, key);
        auto tx = _cluster.transaction(key, false);
        _test_transaction(key, tx);
    }

    _test_watch();
}

void PipelineTransactionTest::_test_pipeline(const StringView &key, Pipeline &pipe) {
    std::string val("value");
    auto replies = pipe.set(key, val)
                        .get(key)
                        .strlen(key)
                        .exec();

    REDIS_ASSERT(replies.get<bool>(0), "failed to test pipeline with set operation");

    auto new_val = replies.get<OptionalString>(1);
    std::size_t len = replies.get<long long>(2);
    REDIS_ASSERT(bool(new_val) && *new_val == val && len == val.size(),
            "failed to test pipeline with string operations");
}

void PipelineTransactionTest::_test_transaction(const StringView &key, Transaction &tx) {
    std::unordered_map<std::string, std::string> m = {
        std::make_pair("f1", "v1"),
        std::make_pair("f2", "v2"),
        std::make_pair("f3", "v3")
    };
    auto replies = tx.hmset(key, m.begin(), m.end())
                        .hgetall(key)
                        .hdel(key, "f1")
                        .exec();

    replies.get<void>(0);

    decltype(m) mm;
    replies.get(1, std::inserter(mm, mm.end()));
    REDIS_ASSERT(mm == m, "failed to test transaction");

    REDIS_ASSERT(replies.get<long long>(2) == 1, "failed to test transaction");

    tx.set(key, "value")
        .get(key)
        .incr(key);

    tx.discard();

    replies = tx.del(key)
                .set(key, "value")
                .exec();

    REDIS_ASSERT(replies.get<long long>(0) == 1, "failed to test transaction");

    REDIS_ASSERT(replies.get<bool>(1), "failed to test transaction");
}

void PipelineTransactionTest::_test_watch() {
    auto key = test_key("watch");

    KeyDeleter deleter(_redis, key);

    {
        auto tx = _redis.transaction();

        auto redis = tx.redis();

        redis.watch(key);

        auto replies = tx.set(key, "1").get(key).exec();

        REDIS_ASSERT(replies.size() == 2 && replies.get<bool>(0) == true, "failed to test watch");

        auto val = replies.get<sw::redis::OptionalString>(1);

        REDIS_ASSERT(val && *val == "1", "failed to test watch");
    }

    try {
        auto tx = _redis.transaction();

        auto redis = tx.redis();

        redis.watch(key);

        // Key has been modified by other client.
        _redis.set(key, "val");

        // Transaction should fail, and throw WatchError
        tx.set(key, "1").exec();

        REDIS_ASSERT(false, "failed to test watch");
    } catch (const sw::redis::WatchError &err) {
        // Catch the error.
    }
}

}

}

}
