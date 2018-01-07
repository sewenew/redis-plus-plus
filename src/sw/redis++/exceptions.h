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

#ifndef SEWENEW_REDISPLUSPLUS_EXCEPTIONS_H
#define SEWENEW_REDISPLUSPLUS_EXCEPTIONS_H

#include <exception>
#include <string>

namespace sw {

namespace redis {

class RException : public std::exception {
public:
    explicit RException(const std::string &msg) : _msg(msg) {}

    RException(const RException &) = default;
    RException& operator=(const RException &) = default;

    RException(RException &&) = default;
    RException& operator=(RException &&) = default;

    virtual ~RException() = default;

    virtual const char* what() const noexcept {
        return _msg.data();
    }

private:
    std::string _msg;
};

}

}

#endif // end SEWENEW_REDISPLUSPLUS_EXCEPTIONS_H
