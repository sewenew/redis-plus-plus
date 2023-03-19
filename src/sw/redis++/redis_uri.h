/**************************************************************************
   Copyright (c) 2022 sewenew

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

#ifndef SEWENEW_REDISPLUSPLUS_REDIS_URI_H
#define SEWENEW_REDISPLUSPLUS_REDIS_URI_H

#include <chrono>
#include <string>
#include <vector>
#include <unordered_map>
#include "sw/redis++/connection.h"
#include "sw/redis++/connection_pool.h"

namespace sw {

namespace redis {

class Uri {
public:
    explicit Uri(const std::string &uri);

    const ConnectionOptions& connection_options() const {
        return _opts;
    }

    const ConnectionPoolOptions& connection_pool_options() const {
        return _pool_opts;
    }

private:
    void _parse_uri(const std::string &uri);

    auto _split_uri(const std::string &uri) const
        -> std::tuple<std::string, std::string, std::string>;

    auto _split_path(const std::string &path) const
        -> std::tuple<std::string, int, std::string>;

    void _parse_parameters(const std::string &parameter_string);

    void _set_option(const std::string &key, const std::string &val);

    bool _parse_bool_option(const std::string &str) const;

    std::chrono::milliseconds _parse_timeout_option(const std::string &str) const;

    int _parse_int_option(const std::string &str) const;

    std::vector<std::string> _split(const std::string &str, const std::string &delimiter) const;

    void _set_auth_opts(const std::string &auth, ConnectionOptions &opts) const;

    void _set_tcp_opts(const std::string &path, ConnectionOptions &opts) const;

    void _set_unix_opts(const std::string &path, ConnectionOptions &opts) const;

    ConnectionOptions _opts;

    ConnectionPoolOptions _pool_opts;
};

}

}

#endif // end SEWENEW_REDISPLUSPLUS_REDIS_URI_H
