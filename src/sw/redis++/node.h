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

#ifndef SEWENEW_REDISPLUSPLUS_NODE_H
#define SEWENEW_REDISPLUSPLUS_NODE_H

#include <string>

namespace sw {

namespace redis {

struct Node {
    std::string host;
    int port;
};

inline bool operator==(const Node &lhs, const Node &rhs) {
    return lhs.host == rhs.host && lhs.port == rhs.port;
}

struct NodeHash {
    std::size_t operator()(const Node &node) const noexcept {
        auto host_hash = std::hash<std::string>{}(node.host);
        auto port_hash = std::hash<int>{}(node.port);
        return host_hash ^ (port_hash << 1);
    }
};

}

}

#endif // end SEWENEW_REDISPLUSPLUS_NODE_H
