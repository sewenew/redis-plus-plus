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

#ifndef SEWENEW_REDISPLUSPLUS_REDIS_HPP
#define SEWENEW_REDISPLUSPLUS_REDIS_HPP

namespace sw {

namespace redis {

// STRING commands.

template <typename Input>
long long Redis::bitop(BitOp op, const StringView &destination, Input first, Input last) {
    auto reply = command(cmd::bitop<Input>, op, destination, first, last);

    return reply::to_integer(*reply);
}

template <typename Input, typename Output>
void Redis::mget(Input first, Input last, Output output) {
    auto reply = command(cmd::mget<Input>, first, last);

    reply::to_optional_string_array(*reply, output);
}

template <typename Input>
void Redis::mset(Input first, Input last) {
    auto reply = command(cmd::mset<Input>, first, last);

    if (!reply::status_ok(*reply)) {
        throw RException("Invalid status reply: " + reply::to_status(*reply));
    }
}

template <typename Input>
bool Redis::msetnx(Input first, Input last) {
    auto reply = command(cmd::msetnx<Input>, first, last);

    return reply::to_bool(*reply);
}

}

}

#endif // end SEWENEW_REDISPLUSPLUS_REDIS_HPP
