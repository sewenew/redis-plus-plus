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

#ifndef SEWENEW_REDISPLUSPLUS_TEST_PIPELINE_TRANSACTION_TEST_H
#define SEWENEW_REDISPLUSPLUS_TEST_PIPELINE_TRANSACTION_TEST_H

#include <sw/redis++/redis++.h>

namespace sw {

namespace redis {

namespace test {

template <typename RedisInstance>
class PipelineTransactionTest {
public:
    explicit PipelineTransactionTest(RedisInstance &instance) : _redis(instance) {}

    void run();

private:
    Pipeline _pipeline(const StringView &key, bool new_connection);

    Transaction _transaction(const StringView &key, bool piped, bool new_connection);

    void _test_pipeline(const StringView &key, Pipeline &pipe);

    void _test_pipeline_streams(const StringView &key, Pipeline &pipe);

    void _test_transaction(const StringView &key, Transaction &tx);

    void _test_watch();

    void _test_error_handle(bool new_connection);

    RedisInstance &_redis;
};

}

}

}

#include "pipeline_transaction_test.hpp"

#endif // end SEWENEW_REDISPLUSPLUS_TEST_PIPELINE_TRANSACTION_TEST_H
