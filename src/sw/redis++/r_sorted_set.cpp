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

#include "r_sorted_set.h"

namespace sw {

namespace redis {

long long RSortedSet::zadd(double score,
                            const StringView &member,
                            bool changed,
                            cmd::UpdateType type) {
    auto reply = _redis.command(cmd::zadd, _key, score, member, changed, type);

    return reply::to_integer(*reply);
}

long long RSortedSet::zcard() {
    auto reply = _redis.command(cmd::zcard, _key);

    return reply::to_integer(*reply);
}

double RSortedSet::zincrby(double increment, const StringView &member) {
    auto reply = _redis.command(cmd::zincrby, _key, increment, member);

    return reply::to_double(*reply);
}

}

}
