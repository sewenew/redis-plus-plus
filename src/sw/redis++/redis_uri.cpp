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

#include "sw/redis++/redis_uri.h"
#include <algorithm>
#include "sw/redis++/errors.h"

namespace sw {

namespace redis {

Uri::Uri(const std::string &uri) {
    _parse_uri(uri);
}

void Uri::_parse_uri(const std::string &uri) {
    std::string scheme;
    std::string auth;
    std::string spath;
    std::tie(scheme, auth, spath) = _split_uri(uri);

    _set_auth_opts(auth, _opts);

    auto db_num = 0;
    std::string parameter_string;
    std::tie(spath, db_num, parameter_string) = _split_path(spath);

    _opts.db = db_num;

    if (scheme == "tcp" || scheme == "redis") {
        _set_tcp_opts(spath, _opts);
    } else if (scheme == "unix") {
        _set_unix_opts(spath, _opts);
    } else {
        throw Error("invalid URI: invalid scheme");
    }

    _parse_parameters(parameter_string);
}

void Uri::_parse_parameters(const std::string &parameter_string) {
    auto parameters = _split(parameter_string, "&");
    if (parameters.empty()) {
        // No parameters
        return;
    }

    for (const auto &parameter : parameters) {
        auto kv_pair = _split(parameter, "=");
        if (kv_pair.size() != 2) {
            throw Error("invalid option: not a key-value pair: " + parameter);
        }

        const auto &key = kv_pair[0];
        const auto &val = kv_pair[1];
        _set_option(key, val);
    }
}

void Uri::_set_option(const std::string &key, const std::string &val) {
    if (key == "user") {
        _opts.user = val;
    } else if (key == "password") {
        _opts.password = val;
    } else if (key == "db") {
        _opts.db = _parse_int_option(val);
    } else if (key == "keep_alive") {
        _opts.keep_alive = _parse_bool_option(val);
    } else if (key == "connect_timeout") {
        _opts.connect_timeout = _parse_timeout_option(val);
    } else if (key == "socket_timeout") {
        _opts.socket_timeout = _parse_timeout_option(val);
    } else if (key == "resp") {
        _opts.resp = _parse_int_option(val);
    } else if (key == "pool_size") {
        _pool_opts.size = static_cast<std::size_t>(_parse_int_option(val));
    } else if (key == "pool_wait_timeout") {
        _pool_opts.wait_timeout = _parse_timeout_option(val);
    } else if (key == "pool_connection_lifetime") {
        _pool_opts.connection_lifetime = _parse_timeout_option(val);
    } else if (key == "pool_connection_idle_time") {
        _pool_opts.connection_idle_time = _parse_timeout_option(val);
    } else {
        throw Error("unknown uri parameter");
    }
}

bool Uri::_parse_bool_option(const std::string &str) const {
    bool val = false;
    if (str == "true") {
        val = true;
    } else if (str == "false") {
        val = false;
    } else {
        throw Error("invalid uri parameter of bool type: " + str);
    }

    return val;
}

std::chrono::milliseconds Uri::_parse_timeout_option(const std::string &str) const {
    std::size_t timeout = 0;
    std::string unit;
    try {
        std::size_t pos = 0;
        timeout = std::stoul(str, &pos);
        unit = str.substr(pos);
    } catch (const std::exception &e) {
        throw Error("invalid uri parameter of timeout type: " + str + ", err: " + e.what());
    }

    std::chrono::milliseconds val{0};
    if (unit == "ms") {
        val = std::chrono::milliseconds(timeout);
    } else if (unit == "s") {
        val = std::chrono::seconds(timeout);
    } else if (unit == "m") {
        val = std::chrono::minutes(timeout);
    } else {
        throw Error("unknown timeout unit: " + unit);
    }

    return val;
}

int Uri::_parse_int_option(const std::string &str) const {
    int val = 0;
    try {
        val = std::stoi(str);
    } catch (const std::exception &e) {
        throw Error("invalid uri parameter of int type: " + str + ", err: " + e.what());
    }

    return val;
}

std::vector<std::string> Uri::_split(const std::string &str,
                                                    const std::string &delimiter) const {
    if (str.empty()) {
        return {};
    }

    std::vector<std::string> fields;

    if (delimiter.empty()) {
        std::transform(str.begin(), str.end(), std::back_inserter(fields),
                [](char c) { return std::string(1, c); });
        return fields;
    }

    std::string::size_type pos = 0;
    std::string::size_type idx = 0;
    while (true) {
        pos = str.find(delimiter, idx);
        if (pos == std::string::npos) {
            fields.push_back(str.substr(idx));
            break;
        }

        fields.push_back(str.substr(idx, pos - idx));
        idx = pos + delimiter.size();
    }

    return fields;
}

auto Uri::_split_uri(const std::string &uri) const
    -> std::tuple<std::string, std::string, std::string> {
    const std::string SCHEME_DELIMITER = "://";
    auto pos = uri.find(SCHEME_DELIMITER);
    if (pos == std::string::npos) {
        throw Error("invalid URI: no scheme");
    }

    auto scheme = uri.substr(0, pos);

    auto start = pos + SCHEME_DELIMITER.size();
    pos = uri.find("@", start);
    if (pos == std::string::npos) {
        // No auth info.
        return std::make_tuple(scheme, std::string{}, uri.substr(start));
    }

    auto auth = uri.substr(start, pos - start);

    return std::make_tuple(scheme, auth, uri.substr(pos + 1));
}

auto Uri::_split_path(const std::string &spath) const
    -> std::tuple<std::string, int, std::string> {
    auto parameter_pos = spath.rfind("?");
    std::string parameter_string;
    if (parameter_pos != std::string::npos) {
        parameter_string = spath.substr(parameter_pos + 1);
    }

    auto pos = spath.rfind("/");
    if (pos != std::string::npos) {
        // Might specified a db number.
        try {
            auto db_num = std::stoi(spath.substr(pos + 1));

            return std::make_tuple(spath.substr(0, pos), db_num, parameter_string);
        } catch (const std::exception &) {
            // Not a db number, and it might be a path to unix domain socket.
        }
    }

    // No db number specified, and use default one, i.e. 0.
    return std::make_tuple(spath.substr(0, parameter_pos), 0, parameter_string);
}

void Uri::_set_auth_opts(const std::string &auth, ConnectionOptions &opts) const {
    if (auth.empty()) {
        // No auth info.
        return;
    }

    auto pos = auth.find(":");
    if (pos == std::string::npos) {
        // No user name.
        opts.password = auth;
    } else {
        opts.user = auth.substr(0, pos);
        opts.password = auth.substr(pos + 1);
    }
}

void Uri::_set_tcp_opts(const std::string &spath, ConnectionOptions &opts) const {
    opts.type = ConnectionType::TCP;

    auto pos = spath.find(":");
    if (pos != std::string::npos) {
        // Port number specified.
        try {
            opts.port = std::stoi(spath.substr(pos + 1));
        } catch (const std::exception &) {
            throw Error("invalid URI: invalid port");
        }
    } // else use default port, i.e. 6379.

    opts.host = spath.substr(0, pos);
}

void Uri::_set_unix_opts(const std::string &spath, ConnectionOptions &opts) const {
    opts.type = ConnectionType::UNIX;
    opts.path = spath;
}

}

}
