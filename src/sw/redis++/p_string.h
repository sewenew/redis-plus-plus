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

#ifndef SEWENEW_REDISPLUSPLUS_P_STRING_H
#define SEWENEW_REDISPLUSPLUS_P_STRING_H

#include <string>
#include "pipeline.h"
#include "utils.h"

namespace sw {

namespace redis {

// Redis' STRING type for pipeline.
class PString {
public:
    Pipeline& pipeline();

    template <typename IntegerReplyCallback, typename ErrorCallback>
    PString& append(const StringView &str,
                    IntegerReplyCallback reply_callback,
                    ErrorCallback error_callback);

    template <typename StringReplyCallback, typename ErrorCallback>
    PString& get(StringReplyCallback reply_callback,
                    ErrorCallback error_callback);

private:
    friend class Pipeline;

    PString(const std::string &key, Pipeline &pipeline) : _key(key), _pipeline(pipeline) {}

    std::string _key;

    Pipeline &_pipeline;
};

// Inline implementations.

inline Pipeline& PString::pipeline() {
    return _pipeline;
}

template <typename IntegerReplyCallback, typename ErrorCallback>
inline PString& PString::append(const StringView &str,
                                IntegerReplyCallback reply_callback,
                                ErrorCallback error_callback) {
    _pipeline.command(cmd::append,
                        IntegerReplyFunctor(reply_callback),
                        error_callback,
                        _key,
                        str);

    return *this;
}

template <typename StringReplyCallback, typename ErrorCallback>
inline PString& PString::get(StringReplyCallback reply_callback,
                                ErrorCallback error_callback) {
    _pipeline.command(cmd::get,
                        StringReplyFunctor(reply_callback),
                        error_callback,
                        _key);

    return *this;
}

}

}

#endif // end SEWENEW_REDISPLUSPLUS_P_STRING_H
