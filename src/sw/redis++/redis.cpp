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

#include "redis.h"
#include <hiredis/hiredis.h>
#include "command.h"
#include "exceptions.h"
#include "pipeline.h"
#include "r_string.h"
#include "r_list.h"

namespace sw {

namespace redis {

RString Redis::string(const std::string &key) {
    return {key, *this};
}

RList Redis::list(const std::string &key) {
    return {key, *this};
}

Pipeline Redis::pipeline() {
    return Pipeline(_pool);
}

void Redis::auth(const StringView &password) {
    auto reply = command(cmd::auth, password);

    if (!reply::status_ok(*reply)) {
        throw RException("Invalid status reply: " + reply::to_status(*reply));
    }
}

std::string Redis::info() {
    auto reply = command(cmd::info);

    return reply::to_string(*reply);
}

std::string Redis::ping() {
    auto reply = command<void (*)(Connection &)>(cmd::ping);

    return reply::to_status(*reply);
}

std::string Redis::ping(const StringView &msg) {
    auto reply = command<void (*)(Connection &, const StringView &)>(cmd::ping, msg);

    return reply::to_string(*reply);
}

}

}
