/**************************************************************************
   Copyright (c) 2022 sewenew

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

#ifndef SEWENEW_REDISPLUSPLUS_TEST_ASYNC_TEST_H
#define SEWENEW_REDISPLUSPLUS_TEST_ASYNC_TEST_H

#ifdef REDIS_PLUS_PLUS_RUN_ASYNC_TEST

#include <atomic>
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <sw/redis++/async_redis++.h>
#include "utils.h"

namespace sw {

namespace redis {

namespace test {

template <>
inline void delete_keys<AsyncRedis>(AsyncRedis &r, const std::vector<std::string> &keys) {
    r.del(keys.begin(), keys.end()).get();
}

template <>
inline void delete_keys<AsyncRedisCluster>(AsyncRedisCluster &r, const std::vector<std::string> &keys) {
    r.del(keys.begin(), keys.end()).get();
}

template <typename RedisInstance>
class AsyncTest {
public:
    explicit AsyncTest(const sw::redis::ConnectionOptions &opts) : _redis(opts) {}

    void run();

    void set_ready(bool ready = true) {
        _ready = ready;
    }

private:
    void _test_str();

    void _test_list();

    void _test_hash();

    void _test_set();

    void _test_zset();

    void _test_generic();

    void _wait();

    std::atomic<bool> _ready{false};

    RedisInstance _redis;
};

template <typename RedisInstance>
void AsyncTest<RedisInstance>::_wait() {
    while (!_ready) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}

template <typename RedisInstance>
void AsyncTest<RedisInstance>::run() {
    _test_str();

    _test_list();

    _test_hash();

    _test_set();

    _test_zset();

    _test_generic();
}

template <typename RedisInstance>
void AsyncTest<RedisInstance>::_test_str() {
    auto key = test_key("str");

    auto key1 = test_key("key1");
    auto key2 = test_key("key2");

    KeyDeleter<RedisInstance> deleter(_redis, {key, key1, key2});

    std::string val("value");
    REDIS_ASSERT(_redis.set(key, val, std::chrono::hours(1)).get(),
            "failed to test async set");

    REDIS_ASSERT(!_redis.set(key, val, std::chrono::hours(1), sw::redis::UpdateType::NOT_EXIST).get(),
            "failed to test async set");

    REDIS_ASSERT(_redis.set(key, val).get(), "failed to test async set");

    struct SetReady {
        SetReady(AsyncTest<RedisInstance> *test, bool res = true) : _test(test), _res(res) {}

        void operator() (Future<bool> &&fut) {
            REDIS_ASSERT(fut.get() == _res, "failed to test async set");
            _test->set_ready();
        }

        AsyncTest<RedisInstance> *_test;
        bool _res;
    };

    set_ready(false);
    _redis.set(key, val, std::chrono::hours(1), SetReady{this});
    _wait();

    set_ready(false);
    _redis.set(key, val, std::chrono::hours(1), sw::redis::UpdateType::NOT_EXIST, SetReady{this, false});
    _wait();

    set_ready(false);
    _redis.set(key, val, SetReady{this});
    _wait();

    auto v = _redis.get(key).get();
    REDIS_ASSERT(v && *v == val, "failed to test async get");

    set_ready(false);
    _redis.get(key, [this, &val](Future<OptionalString> &&fut) {
                REDIS_ASSERT(*(fut.get()) == val, "failed to test async get");
                this->set_ready();
            });
    _wait();

    set_ready(false);
    _redis.del(key, [this](Future<long long> &&fut) {
                REDIS_ASSERT(fut.get() == 1, "failed to test async del");
                this->set_ready();
            });
    _wait();

    set_ready(false);
    std::unordered_map<std::string, std::string> kvs = {{key1, "val1"}, {key2, "val2"}};
    _redis.mset(kvs.begin(), kvs.end(), [this](Future<void> &&fut) {
                fut.get();
                this->set_ready();
            });
    _wait();
}

template <typename RedisInstance>
void AsyncTest<RedisInstance>::_test_list() {
    auto src = test_key("src");
    auto dest = test_key("dest");

    KeyDeleter<RedisInstance> deleter(_redis, {src, dest});

    auto num = _redis.lpush(src, {"a", "b", "c"}).get();
    REDIS_ASSERT(num == 3, "failed to test async list: lpush");

    num = _redis.lpush(dest, {"e", "f", "g"}).get();
    REDIS_ASSERT(num == 3, "failed to test async list: lpush");

    auto val = _redis.lmove(src, dest, ListWhence::LEFT, ListWhence::RIGHT).get();
    REDIS_ASSERT(val && *val == "c", "failed to test async list: lmove");

    set_ready(false);
    _redis.blmove(src, dest, ListWhence::LEFT, ListWhence::RIGHT,
            [this](Future<OptionalString> &&fut) {
                auto val = fut.get();
                REDIS_ASSERT(val && *val == "b", "failed to test async list: blmove");

                this->set_ready();
            });
    _wait();

    set_ready(false);
    _redis.blmove(src, dest, ListWhence::LEFT, ListWhence::RIGHT, std::chrono::seconds{1},
            [this](Future<OptionalString> &&fut) {
                auto val = fut.get();
                REDIS_ASSERT(val && *val == "a", "failed to test async list: blmove");

                this->set_ready();
            });
    _wait();

    auto keys = {src, dest};
    auto lmpop_res = _redis.template lmpop<std::vector<std::string>>(keys.begin(), keys.end(), ListWhence::LEFT).get();
    REDIS_ASSERT(lmpop_res && lmpop_res->first == dest && lmpop_res->second.size() == 1, "failed to test async list: lmpop");

    set_ready(false);
    _redis.template lmpop<std::vector<std::string>>(keys.begin(), keys.end(), ListWhence::LEFT, 2,
            [this, dest](Future<Optional<std::pair<std::string, std::vector<std::string>>>> && fut) {
                auto val = fut.get();
                REDIS_ASSERT(val && val->first == dest && val->second.size() == 2, "failed to test async list: lmpop");

                this->set_ready();
            });
    _wait();
}

template <typename RedisInstance>
void AsyncTest<RedisInstance>::_test_hash() {
    auto key = test_key("hash");

    KeyDeleter<RedisInstance> deleter(_redis, key);

    auto f1 = std::string("f1");
    auto v1 = std::string("v1");
    REDIS_ASSERT(_redis.hset(key, f1, v1).get() == 1, "failed to test async hset");

    set_ready(false);
    _redis.hset(key, f1, v1, [this](Future<long long> &&fut) {
                REDIS_ASSERT(fut.get() == 0, "failed to test async hset");
                this->set_ready();
            });
    _wait();

    auto v = _redis.hget(key, f1).get();
    REDIS_ASSERT(v && *v == v1, "failed to test async hget");

    set_ready(false);
    _redis.hget(key, f1, [this, &v1](Future<OptionalString> &&fut) {
                auto v = fut.get();
                REDIS_ASSERT(v && *v == v1, "failed to test async hget");
                this->set_ready();
            });
    _wait();

    REDIS_ASSERT(_redis.hdel(key, f1).get() == 1, "failed to test async hdel");

    set_ready(false);
    _redis.hdel(key, f1, [this](Future<long long> &&fut) {
                REDIS_ASSERT(fut.get() == 0, "failed to test async hdel");
                this->set_ready();
            });
    _wait();

    _redis.del(key);

    std::unordered_map<std::string, std::string> m = {{"ff1", "vv1"}, {"ff2", "vv2"}};
    REDIS_ASSERT(_redis.hset(key, m.begin(), m.end()).get() == 2, "failed to test async hset");

    set_ready(false);
    _redis.hset(key, m.begin(), m.end(),
            [this](Future<long long> &&fut) {
                REDIS_ASSERT(fut.get() == 0, "failed to test async hset");
                this->set_ready();
            });
    _wait();

    auto res = _redis.template hgetall<std::unordered_map<std::string, std::string>>(key).get();
    REDIS_ASSERT(res == m, "failed to test async hgetall");

    set_ready(false);
    _redis.template hgetall<std::unordered_map<std::string, std::string>>(key,
            [this, &m](Future<std::unordered_map<std::string, std::string>> &&fut) {
                REDIS_ASSERT(fut.get() == m, "failed to test async hgetall");
                this->set_ready();
            });
    _wait();
}

template <typename RedisInstance>
void AsyncTest<RedisInstance>::_test_set() {
    auto key = test_key("set");

    KeyDeleter<RedisInstance> deleter(_redis, key);

    std::string m = "a";
    REDIS_ASSERT(_redis.sadd(key, m).get() == 1, "failed to test async sadd");

    set_ready(false);
    _redis.sadd(key, m, [this](Future<long long> &&fut) {
                REDIS_ASSERT(fut.get() == 0, "failed to test async sadd");
                this->set_ready();
            });
    _wait();

    std::unordered_set<std::string> mem = {"1", "2", "3"};
    REDIS_ASSERT(_redis.sadd(key, mem.begin(), mem.end()).get() == 3, "failed to test async sadd");

    set_ready(false);
    _redis.sadd(key, mem.begin(), mem.end(),
            [this](Future<long long> &&fut) {
                REDIS_ASSERT(fut.get() == 0, "failed to test async sadd");
                this->set_ready();
            });
    _wait();

    auto mem_res = _redis.template smembers<std::unordered_set<std::string>>(key).get();
    mem.insert(m);
    REDIS_ASSERT(mem_res == mem, "failed to test async smembers");

    set_ready(false);
    _redis.template smembers<std::unordered_set<std::string>>(key,
            [this, &mem](Future<std::unordered_set<std::string>> &&fut) {
                REDIS_ASSERT(fut.get() == mem, "failed to test async smembers");
                this->set_ready();
            });
    _wait();

    set_ready(false);
    _redis.srem(key, "1", [this](Future<long long> &&fut) {
                REDIS_ASSERT(fut.get() == 1, "failed to test asycn srem");
                this->set_ready();
            });
    _wait();
}

template <typename RedisInstance>
void AsyncTest<RedisInstance>::_test_zset() {
    auto key = test_key("zset");

    KeyDeleter<RedisInstance> deleter(_redis, key);

    REDIS_ASSERT(_redis.zadd(key, "a", 10).get() == 1, "failed to test async zadd");

    std::unordered_map<std::string, double> mems = {std::make_pair("b", 10), std::make_pair("c", 10)};
    REDIS_ASSERT(_redis.zadd(key, mems.begin(), mems.end()).get() == 2, "failed to test async zadd");

    auto vec = _redis.template zrange<std::vector<std::string>>(key, 0, -1).get();
    REDIS_ASSERT(vec.size() == 3, "failed to test async zrange");

    auto m = _redis.template zrange<std::unordered_map<std::string, double>>(key, 0, -1).get();
    REDIS_ASSERT(m.size() == 3, "failed to test async zrange");

    set_ready(false);
    _redis.zadd(key, "a", 10, [this](Future<long long> &&fut) {
                REDIS_ASSERT(fut.get() == 0, "failed to test async zadd");
                this->set_ready();
            });
    _wait();

    set_ready(false);
    _redis.zadd(key, mems.begin(), mems.end(), [this](Future<long long> &&fut) {
                REDIS_ASSERT(fut.get() == 0, "failed to test async zadd");
                this->set_ready();
            });
    _wait();

    set_ready(false);
    _redis.template zrange<std::vector<std::string>>(key, 0, -1, [this](Future<std::vector<std::string>> &&fut) {
                auto vec = fut.get();
                REDIS_ASSERT(vec.size() == 3, "failed to test async zrange");
                this->set_ready();
            });
    _wait();

    set_ready(false);
    _redis.template zrange<std::unordered_map<std::string, double>>(key, 0, -1, [this](Future<std::unordered_map<std::string, double>> &&fut) {
                auto m = fut.get();
                REDIS_ASSERT(m.size() == 3 && m.count("a") == 1, "failed to test async zrange");
                this->set_ready();
            });
    _wait();
}

template <typename RedisInstance>
void AsyncTest<RedisInstance>::_test_generic() {
    auto key = test_key("generic");
    auto another_key = test_key("generic-another");

    KeyDeleter<RedisInstance> deleter(_redis, {key, another_key});

    std::string val = "value";
    _redis.template command<void>("set", key, val).get();
    auto v = _redis.template command<sw::redis::OptionalString>("get", key).get();
    REDIS_ASSERT(v && *v == val, "failed to test async generic command");

    val = "val";
    std::vector<std::string> args = {"set", key, val};
    _redis.template command<void>(args.begin(), args.end()).get();
    args = {"get", key};
    v = _redis.template command<sw::redis::OptionalString>(args.begin(), args.end()).get();
    REDIS_ASSERT(v && *v == val, "failed to test async generic command");

    _redis.template command<long long>("del", key);

    std::unordered_set<std::string> mems = {"a", "b", "c"};
    args = {"sadd", another_key};
    args.insert(args.end(), mems.begin(), mems.end());
    REDIS_ASSERT(_redis.template command<long long>(args.begin(), args.end()).get() == 3,
            "failed to test async generic command");
    args = {"smembers", another_key};
    auto mem_res = _redis.template command<std::unordered_set<std::string>>(args.begin(), args.end()).get();
    REDIS_ASSERT(mems == mem_res, "failed to test async generic command");

    _redis.template command<long long>("del", another_key);

    set_ready(false);
    val = "new-value";
    _redis.template command<void>("set", key, val,
            [this](Future<void> &&fut) { fut.get(); this->set_ready(); });
    _wait();

    set_ready(false);
    _redis.template command<sw::redis::OptionalString>("get", key,
            [this, &val](Future<sw::redis::OptionalString> &&fut) {
                REDIS_ASSERT(val == *(fut.get()), "failed to test async generic command");
                this->set_ready();
            });
    _wait();

    set_ready(false);
    mems = {"1", "2", "3"};
    args = {"sadd", another_key};
    args.insert(args.end(), mems.begin(), mems.end());
    _redis.template command<long long>(args.begin(), args.end(),
            [this](Future<long long> &&fut) {
                REDIS_ASSERT(fut.get() == 3, "failed to test async generic command");
                this->set_ready();
            });
    _wait();

    set_ready(false);
    std::unordered_set<std::string> smembers_res;
    args = {"smembers", another_key};
    _redis.template command<std::unordered_set<std::string>>(args.begin(), args.end(),
            [this, &mems](Future<std::unordered_set<std::string>> &&fut) {
                REDIS_ASSERT(fut.get() == mems, "failed to test async generic command");
                this->set_ready();
            });
    _wait();
}

}

}

}

#endif

#endif // end SEWENEW_REDISPLUSPLUS_TEST_ASYNC_TEST_H
