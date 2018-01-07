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

#include "pipeline.h"
#include "p_string.h"

namespace sw {

namespace redis {

Pipeline::Pipeline(ConnectionPool &pool) :
    _pool(pool),
    _connection(_pool.fetch()) {}

Pipeline::~Pipeline() {
    try {
        transport();

        _pool.release(std::move(_connection));
    } catch (...) {
        // In case that user defined error callback throw exceptions,
        // or failed to release connection.
        // Avoid throw exception from destructor.
    }
}

void Pipeline::transport() {
    while (!_callbacks.empty()) {
        auto reply_callback = _callbacks.front().first;
        auto error_callback = _callbacks.front().second;
        _callbacks.pop();

        try {
            // TODO: what if _connection.broken() is true?
            auto reply = _connection.recv();
            reply_callback(*reply);
        } catch (const RException &e) {
            error_callback(e);
        }
    }
}

PString Pipeline::string(const std::string &key) {
    return PString(key, *this);
}

}

}
