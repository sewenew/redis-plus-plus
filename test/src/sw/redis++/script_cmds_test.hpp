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

#ifndef SEWENEW_REDISPLUSPLUS_TEST_SCRIPT_CMDS_TEST_HPP
#define SEWENEW_REDISPLUSPLUS_TEST_SCRIPT_CMDS_TEST_HPP

#include <list>
#include <vector>
#include "utils.h"

namespace sw {

namespace redis {

namespace test {

template <typename RedisInstance>
void ScriptCmdTest<RedisInstance>::run() {
    cluster_specializing_test(*this,
            &ScriptCmdTest<RedisInstance>::_run,
            _redis);

    cluster_specializing_test(*this,
            &ScriptCmdTest<RedisInstance>::_run_function_test,
            _redis);
}

template <typename RedisInstance>
void ScriptCmdTest<RedisInstance>::_run(Redis &instance) {
    auto key1 = test_key("k1");
    auto key2 = test_key("k2");

    KeyDeleter<Redis> deleter(instance, {key1, key2});

    std::string script = "redis.call('set', KEYS[1], 1);"
                    "redis.call('set', KEYS[2], 2);"
                    "local first = redis.call('get', KEYS[1]);"
                    "local second = redis.call('get', KEYS[2]);"
                    "return first + second";

    std::initializer_list<StringView> keys = {key1, key2};
    std::initializer_list<StringView> empty_list = {};

    auto num = instance.eval<long long>(script, keys, empty_list);
    REDIS_ASSERT(num == 3, "failed to test scripting for cluster");

    num = instance.eval<long long>(script, keys.begin(), keys.end(),
            empty_list.begin(), empty_list.end());
    REDIS_ASSERT(num == 3, "failed to test scripting for cluster");

    script = "return 1";
    num = instance.eval<long long>(script, empty_list, empty_list);
    REDIS_ASSERT(num == 1, "failed to test eval");

    num = instance.eval<long long>(script, empty_list.begin(), empty_list.end(),
            empty_list.begin(), empty_list.end());
    REDIS_ASSERT(num == 1, "failed to test eval");

    auto script_with_args = "return {ARGV[1] + 1, ARGV[2] + 2, ARGV[3] + 3}";
    std::initializer_list<StringView> args = {"1", "2", "3"};
    std::vector<long long> res;
    instance.eval(script_with_args,
                empty_list,
                args,
                std::back_inserter(res));
    REDIS_ASSERT(res == std::vector<long long>({2, 4, 6}),
            "failed to test eval with array reply");

    res.clear();
    instance.eval(script_with_args,
                empty_list.begin(), empty_list.end(),
                args.begin(), args.end(),
                std::back_inserter(res));
    REDIS_ASSERT(res == std::vector<long long>({2, 4, 6}),
            "failed to test eval with array reply");

    auto sha1 = instance.script_load(script);
    num = instance.evalsha<long long>(sha1, {}, {});
    REDIS_ASSERT(num == 1, "failed to test evalsha");

    num = instance.evalsha<long long>(sha1, empty_list.begin(), empty_list.end(),
            empty_list.begin(), empty_list.end());
    REDIS_ASSERT(num == 1, "failed to test evalsha");

    auto sha2 = instance.script_load(script_with_args);
    res.clear();
    instance.evalsha(sha2,
                    empty_list,
                    args,
                    std::back_inserter(res));
    REDIS_ASSERT(res == std::vector<long long>({2, 4, 6}),
            "failed to test evalsha with array reply");

    res.clear();
    instance.evalsha(sha2,
                    empty_list.begin(), empty_list.end(),
                    args.begin(), args.end(),
                    std::back_inserter(res));
    REDIS_ASSERT(res == std::vector<long long>({2, 4, 6}),
            "failed to test evalsha with array reply");

    std::list<bool> exist_res;
    instance.script_exists({sha1, sha2, std::string("not exist")}, std::back_inserter(exist_res));
    REDIS_ASSERT(exist_res == std::list<bool>({true, true, false}),
            "failed to test script exists");

    REDIS_ASSERT(instance.script_exists(sha1), "failed to test script exists");
    REDIS_ASSERT(!instance.script_exists("not exist"), "failed to test script exists");
}

template <typename RedisInstance>
void ScriptCmdTest<RedisInstance>::_run_function_test(Redis &instance) {
    auto key1 = test_key("k1");
    auto key2 = test_key("k2");

    KeyDeleter<Redis> deleter(instance, {key1, key2});

    try {
        instance.function_delete("swredistestlib");
    } catch (const Error &) {
    }

    std::string code = "#!lua name=swredistestlib\n"
                    "redis.register_function('my_func', function(keys, args) "
                    "redis.call('set', keys[1], 1);"
                    "redis.call('set', keys[2], 2);"
                    "local first = redis.call('get', keys[1]);"
                    "local second = redis.call('get', keys[2]);"
                    "return first + second\n"
                    "end)";

    auto lib_name = instance.function_load(code);
    REDIS_ASSERT(lib_name == "swredistestlib", "failed to test function_load");

    std::initializer_list<StringView> keys = {key1, key2};
    std::initializer_list<StringView> empty_list = {};

    auto num = instance.fcall<long long>("my_func", keys, empty_list);
    REDIS_ASSERT(num == 3, "failed to test fcall");

    num = instance.fcall<long long>("my_func", keys.begin(), keys.end(),
            empty_list.begin(), empty_list.end());
    REDIS_ASSERT(num == 3, "failed to test fcall");

    auto is_readonly = false;
    try {
        instance.fcall_ro<std::string>("my_func", {}, {});
    } catch (const Error &) {
        is_readonly = true;
    }
    REDIS_ASSERT(is_readonly, "failed to test fcall_ro");

    code = "#!lua name=swredistestlib\n"
        "local function readonly_func(keys, args) return 'hello' end\n"
        "redis.register_function{function_name='my_func', callback=readonly_func, flags={ 'no-writes' }}";
    lib_name = instance.function_load(code, true);
    REDIS_ASSERT(lib_name == "swredistestlib", "failed to test function_load");

    auto res = instance.fcall_ro<std::string>("my_func", {}, {});
    REDIS_ASSERT(res == "hello", "failed to test fcall_ro");

    instance.function_delete("swredistestlib");
}

}

}

}

#endif // end SEWENEW_REDISPLUSPLUS_TEST_SCRIPT_CMDS_TEST_HPP
