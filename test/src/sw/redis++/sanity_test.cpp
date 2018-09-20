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

#include "sanity_test.h"
#include "utils.h"

namespace sw {

namespace redis {

namespace test {

SanityTest::SanityTest(const ConnectionOptions &opts) : _opts(opts), _redis(opts) {}

void SanityTest::run() {
    _test_uri_ctor();

    _test_move_ctor();
}

void SanityTest::_test_uri_ctor() {
    std::string uri;
    switch (_opts.type) {
    case sw::redis::ConnectionType::TCP:
        uri = "tcp://" + _opts.host + ":" + std::to_string(_opts.port);
        break;

    case sw::redis::ConnectionType::UNIX:
        uri = "unix://" + _opts.path;
        break;

    default:
        REDIS_ASSERT(false, "Unknown connection type");
    }

    auto redis = sw::redis::Redis(uri);
    try {
        auto pong = redis.ping();
        REDIS_ASSERT(pong == "PONG", "Failed to test constructing Redis with uri");
    } catch (const sw::redis::ReplyError &e) {
        REDIS_ASSERT(e.what() == std::string("NOAUTH Authentication required."),
                "Failed to test constructing Redis with uri");
    }
}

void SanityTest::_test_move_ctor() {
    auto test_move_ctor = std::move(_redis);

    _redis = std::move(test_move_ctor);
}

}

}

}
