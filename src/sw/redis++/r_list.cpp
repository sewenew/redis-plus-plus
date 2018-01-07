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

#include "r_list.h"
#include "command.h"
#include "exceptions.h"

namespace sw {

namespace redis {

std::string RList::lpop() {
    auto reply = _redis.command(cmd::lpop, _key);

    if (reply::is_nil(*reply)) {
        throw RException("Empty list: " + _key);
    }

    return reply::to_string(*reply);
}

long long RList::lpush(const StringView &val) {
    auto reply = _redis.command(cmd::lpush, _key, val);

    return reply::to_integer(*reply);
}

}

}
