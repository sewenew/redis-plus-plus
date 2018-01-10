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

#include "command.h"

namespace sw {

namespace redis {

namespace cmd {

void set(Connection &connection,
            const StringView &key,
            const StringView &val,
            const std::chrono::milliseconds &ttl,
            UpdateType type) {
    Connection::CmdArgs args;
    args << "SET" << key << val;

    std::string ttl_options;
    if (ttl > std::chrono::milliseconds(0)) {
        ttl_options = std::to_string(ttl.count());
        args << "PX" << ttl_options;
    }

    switch (type) {
    case UpdateType::EXIST:
        args << "XX";
        break;

    case UpdateType::NOT_EXIST:
        args << "NX";
        break;

    case UpdateType::ALWAYS:
        // Do nothing.
        break;

    default:
        assert(false);
    }

    connection.send(args);
}

}

}

}
