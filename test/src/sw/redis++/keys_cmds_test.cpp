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

#include "keys_cmds_test.h"
#include <vector>
#include <unordered_set>
#include "utils.h"

namespace sw {

namespace redis {

namespace test {

KeysCmdTest::KeysCmdTest(const ConnectionOptions &opts) : _redis(opts) {}

void KeysCmdTest::run() {
    _test_key();

    _test_ttl();

    _test_scan();
}

void KeysCmdTest::_test_key() {
    auto key = test_key("key");
    auto dest = test_key("dest");
    auto new_key_name = test_key("new-key");

    KeyDeleter deleter(_redis, {key, dest, new_key_name});

    REDIS_ASSERT(_redis.exists(key) == 0, "failed to test exists");

    auto val = std::string("val");
    _redis.set(key, val);

    REDIS_ASSERT(_redis.exists({key, std::string("not_exist")}) == 1, "failed to test exists");

    auto new_val = _redis.dump(key);
    REDIS_ASSERT(bool(new_val), "failed to test dump");

    _redis.restore(dest, *new_val, std::chrono::seconds(1000));

    new_val = _redis.get(dest);
    REDIS_ASSERT(bool(new_val) && *new_val == val, "failed to test dump and restore");

    auto rand_key = _redis.randomkey();
    REDIS_ASSERT(bool(rand_key), "failed to test randomkey");

    _redis.rename(dest, new_key_name);

    bool not_exist = false;
    try {
        _redis.rename("non-existent-key", "hello");
    } catch (const Error &e) {
        not_exist = true;
    }
    REDIS_ASSERT(not_exist, "failed to test rename with nonexistent key");

    REDIS_ASSERT(_redis.renamenx(new_key_name, dest), "failed to test renamenx");

    REDIS_ASSERT(_redis.touch({}) == 0, "failed to test touch");
    REDIS_ASSERT(_redis.touch({key, dest, new_key_name}) == 2, "failed to test touch");

    REDIS_ASSERT(_redis.type(key) == "string", "failed to test type");

    REDIS_ASSERT(_redis.del({new_key_name, dest}) == 1, "failed to test del");
    REDIS_ASSERT(_redis.unlink({new_key_name, key}) == 1, "failed to test unlink");
}

void KeysCmdTest::_test_ttl() {
    using namespace std::chrono;

    auto key = test_key("ttl");

    KeyDeleter deleter(_redis, key);

    _redis.set(key, "val", seconds(100));
    auto ttl = _redis.ttl(key);
    REDIS_ASSERT(ttl > 0 && ttl <= 100, "failed to test ttl");

    REDIS_ASSERT(_redis.persist(key), "failed to test persist");
    ttl = _redis.ttl(key);
    REDIS_ASSERT(ttl == -1, "failed to test ttl");

    REDIS_ASSERT(_redis.expire(key, seconds(100)),
            "failed to test expire");

    auto tp = time_point_cast<seconds>(system_clock::now() + seconds(100));
    REDIS_ASSERT(_redis.expireat(key, tp), "failed to test expireat");
    ttl = _redis.ttl(key);
    REDIS_ASSERT(ttl > 0, "failed to test expireat");

    REDIS_ASSERT(_redis.pexpire(key, milliseconds(100000)), "failed to test expire");

    auto pttl = _redis.pttl(key);
    REDIS_ASSERT(pttl > 0 && pttl <= 100000, "failed to test pttl");

    auto tp_milli = time_point_cast<milliseconds>(system_clock::now() + milliseconds(100000));
    REDIS_ASSERT(_redis.pexpireat(key, tp_milli), "failed to test pexpireat");
    pttl = _redis.pttl(key);
    REDIS_ASSERT(pttl > 0, "failed to test pexpireat");
}

void KeysCmdTest::_test_scan() {
    std::string key_pattern = "!@#$%^&()_+alseufoawhnlkszd";
    auto k1 = test_key(key_pattern + "k1");
    auto k2 = test_key(key_pattern + "k2");
    auto k3 = test_key(key_pattern + "k3");

    auto keys = {k1, k2, k3};

    KeyDeleter deleter(_redis, keys);

    _redis.set(k1, "v");
    _redis.set(k2, "v");
    _redis.set(k3, "v");

    auto cursor = 0;
    std::unordered_set<std::string> res;
    while (true) {
        cursor = _redis.scan(cursor, "*" + key_pattern + "*", 2, std::inserter(res, res.end()));
        if (cursor == 0) {
            break;
        }
    }
    REDIS_ASSERT(res == std::unordered_set<std::string>(keys),
            "failed to test scan");
}

}

}

}
