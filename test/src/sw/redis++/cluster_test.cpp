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

#include "cluster_test.h"
#include <vector>
#include "utils.h"

namespace sw {

namespace redis {

namespace test {

ClusterTest::ClusterTest(const ConnectionOptions &opts) : _redis_cluster(opts) {}

void ClusterTest::run() {
    _test_cluster();

    _test_hash_tag();

    _test_script();
}

void ClusterTest::_test_cluster() {
    auto str_key = test_key("{tag}str_key");
    auto list_key = test_key("{tag}list_key");
    auto hash_key = test_key("{tag}hash_key");
    auto set_key = test_key("{tag}set_key");
    auto zset_key = test_key("{tag}zset_key");

    ClusterKeyDeleter deleter(_redis_cluster,
                                {str_key, list_key, hash_key, set_key, zset_key});

    std::string value = "value";
    _redis_cluster.set(str_key, value);
    auto val = _redis_cluster.get(str_key);
    REDIS_ASSERT(val && *val == value, "failed to test cluster: get/set commands");

    _redis_cluster.lpush(list_key, value);
    val = _redis_cluster.lpop(list_key);
    REDIS_ASSERT(val && *val == value, "failed to test cluster: lpush/lpop commands");

    auto member = "member";
    _redis_cluster.hset(hash_key, member, value);
    val = _redis_cluster.hget(hash_key, member);
    REDIS_ASSERT(val && *val == value, "failed to test cluster: hset/hget commands");

    _redis_cluster.sadd(set_key, member);
    REDIS_ASSERT(_redis_cluster.sismember(set_key, member),
            "failed to test cluster: sadd/sismember commands");

    auto score = 100;
    _redis_cluster.zadd(zset_key, member, score);
    auto res = _redis_cluster.zscore(zset_key, member);
    REDIS_ASSERT(res && score == *res, "failed to test cluster: zadd/zscore commands");

    auto r = _redis_cluster.redis("hash-tag");
    r.command("client", "setname", "my-name");
    auto reply = r.command("client", "getname");
    val = reply::parse<OptionalString>(*reply);
    REDIS_ASSERT(val && *val == "my-name", "failed to test cluster");
}

void ClusterTest::_test_hash_tag() {
    _test_hash_tag({test_key("{tag}postfix1"),
                    test_key("{tag}postfix2"),
                    test_key("{tag}postfix3")});

    _test_hash_tag({test_key("prefix1{tag}postfix1"),
                    test_key("prefix2{tag}postfix2"),
                    test_key("prefix3{tag}postfix3")});

    _test_hash_tag({test_key("prefix1{tag}"),
                    test_key("prefix2{tag}"),
                    test_key("prefix3{tag}")});

    _test_hash_tag({test_key("prefix{}postfix"),
                    test_key("prefix{}postfix"),
                    test_key("prefix{}postfix")});

    _test_hash_tag({test_key("prefix1{tag}post}fix1"),
                    test_key("prefix2{tag}pos}tfix2"),
                    test_key("prefix3{tag}postfi}x3")});

    _test_hash_tag({test_key("prefix1{t{ag}postfix1"),
                    test_key("prefix2{t{ag}postfix2"),
                    test_key("prefix3{t{ag}postfix3")});

    _test_hash_tag({test_key("prefix1{t{ag}postfi}x1"),
                    test_key("prefix2{t{ag}post}fix2"),
                    test_key("prefix3{t{ag}po}stfix3")});
}

void ClusterTest::_test_hash_tag(std::initializer_list<std::string> keys) {
    ClusterKeyDeleter deleter(_redis_cluster, keys.begin(), keys.end());

    std::string value = "value";
    std::vector<std::pair<std::string, std::string>> kvs;
    for (const auto &key : keys) {
        kvs.emplace_back(key, value);
    }

    _redis_cluster.mset(kvs.begin(), kvs.end());

    std::vector<OptionalString> res;
    res.reserve(keys.size());
    _redis_cluster.mget(keys.begin(), keys.end(), std::back_inserter(res));

    REDIS_ASSERT(res.size() == keys.size(), "failed to test hash tag");

    for (const auto &ele : res) {
        REDIS_ASSERT(ele && *ele == value, "failed to test hash tag");
    }
}

void ClusterTest::_test_script() {
    auto first = test_key("first{tag}");
    auto second = test_key("second{tag}");
    ClusterKeyDeleter deleter(_redis_cluster, {first, second});

    auto script = "redis.call('set', KEYS[1], 1);"
                    "redis.call('set', KEYS[2], 2);"
                    "local first = redis.call('get', KEYS[1]);"
                    "local second = redis.call('get', KEYS[2]);"
                    "return first + second";
    auto num = _redis_cluster.eval<long long>(script, {first, second}, {});
    REDIS_ASSERT(num == 3, "failed to test scripting for cluster");
}

}

}

}
