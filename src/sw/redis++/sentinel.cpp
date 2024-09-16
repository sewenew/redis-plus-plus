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

#include "sw/redis++/sentinel.h"
#include <cassert>
#include <thread>
#include <random>
#include <algorithm>
#include "sw/redis++/redis.h"
#include "sw/redis++/errors.h"

namespace {

sw::redis::Node node_info(const sw::redis::ConnectionOptions &opts) {
    return {opts.host, opts.port};
}

std::string node_info(const sw::redis::Node &node) {
    if (node.host.empty()) {
        return {};
    }

    return node.host + ":" + std::to_string(node.port);
}

std::string report_error(const sw::redis::Node &sentinel_node,
        const sw::redis::Node &node,
        const std::string &msg) {
    std::string err_msg;
    auto sentinel_info = node_info(sentinel_node);
    if (!sentinel_info.empty()) {
        err_msg += "sentinel: " + sentinel_info;
    }

    auto instance_info = node_info(node);
    if (!instance_info.empty()) {
        err_msg += "node: " + instance_info;
    }

    err_msg += "err: " + msg;

    return err_msg;
}

}

namespace sw {

namespace redis {

class Sentinel::Iterator {
public:
    Iterator(std::list<Connection> &healthy_sentinels,
                std::list<ConnectionOptions> &broken_sentinels) :
                    _healthy_sentinels(healthy_sentinels),
                    _broken_sentinels(broken_sentinels) {
        reset();
    }

    Connection& next();

    void reset();

private:
    std::list<Connection> &_healthy_sentinels;

    std::size_t _healthy_size = 0;

    std::list<ConnectionOptions> &_broken_sentinels;

    std::size_t _broken_size = 0;
};

Connection& Sentinel::Iterator::next() {
    while (_healthy_size > 0) {
        assert(_healthy_sentinels.size() >= _healthy_size);

        --_healthy_size;

        auto &connection = _healthy_sentinels.front();
        if (connection.broken()) {
            _broken_sentinels.push_front(connection.options());
            ++_broken_size;

            _healthy_sentinels.pop_front();
        } else {
            _healthy_sentinels.splice(_healthy_sentinels.end(),
                                        _healthy_sentinels,
                                        _healthy_sentinels.begin());

            return _healthy_sentinels.back();
        }
    }

    std::vector<std::string> err_msgs;
    while (_broken_size > 0) {
        assert(_broken_sentinels.size() >= _broken_size);

        --_broken_size;

        try {
            const auto &opt = _broken_sentinels.front();
            Connection connection(opt);
            _healthy_sentinels.push_back(std::move(connection));

            _broken_sentinels.pop_front();

            return _healthy_sentinels.back();
        } catch (const Error &err) {
            // Failed to connect to sentinel.
            _broken_sentinels.splice(_broken_sentinels.end(),
                                        _broken_sentinels,
                                        _broken_sentinels.begin());

            err_msgs.push_back(report_error(node_info(_broken_sentinels.back()), {"", 0}, err.what()));
        }
    }

    if (err_msgs.empty()) {
        err_msgs.push_back("already tried all sentinel nodes");
    }

    throw StopIterError(err_msgs);
}

void Sentinel::Iterator::reset() {
    _healthy_size = _healthy_sentinels.size();
    _broken_size = _broken_sentinels.size();
}

Sentinel::Sentinel(const SentinelOptions &sentinel_opts) :
                    _broken_sentinels(_parse_options(sentinel_opts)),
                    _sentinel_opts(sentinel_opts) {
    if (_sentinel_opts.connect_timeout == std::chrono::milliseconds(0)
            || _sentinel_opts.socket_timeout == std::chrono::milliseconds(0)) {
        throw Error("With sentinel, connection timeout and socket timeout cannot be 0");
    }
}

Connection Sentinel::master(const std::string &master_name, const ConnectionOptions &opts) {
    std::lock_guard<std::mutex> lock(_mutex);

    Iterator iter(_healthy_sentinels, _broken_sentinels);
    std::size_t retries = 0;
    std::vector<std::string> err_msgs;
    while (true) {
        Node sentinel_node;
        Node master;
        try {
            auto &sentinel = iter.next();
            sentinel_node = node_info(sentinel.options());

            master = _get_master_addr_by_name(sentinel, master_name);

            auto connection = _connect_redis(master, opts);
            if (_get_role(connection) != Role::MASTER) {
                // Retry the whole process at most SentinelOptions::max_retry times.
                ++retries;
                if (retries > _sentinel_opts.max_retry) {
                    throw Error("reach max retry number");
                }

                std::this_thread::sleep_for(_sentinel_opts.retry_interval);

                // Restart the iteration.
                iter.reset();
                continue;
            }

            return connection;
        } catch (const StopIterError &err) {
            err_msgs.push_back(report_error(sentinel_node, master, err.what()));
            throw StopIterError(err_msgs);
        } catch (const Error &err) {
            err_msgs.push_back(report_error(sentinel_node, master, err.what()));
            continue;
        }
    }
}

Connection Sentinel::slave(const std::string &master_name, const ConnectionOptions &opts) {
    std::lock_guard<std::mutex> lock(_mutex);

    Iterator iter(_healthy_sentinels, _broken_sentinels);
    std::size_t retries = 0;
    std::vector<std::string> err_msgs;
    Node sentinel_node;
    Node slave_node;
    while (true) {
        try {
            auto &sentinel = iter.next();
            sentinel_node = node_info(sentinel.options());

            auto slaves = _get_slave_addr_by_name(sentinel, master_name);
            if (slaves.empty()) {
                throw Error("no replica for " + master_name);
            }

            // Normally slaves list is NOT very large, so there won't be a performance problem.
            auto slave_iter = std::find(slaves.begin(),
                                        slaves.end(),
                                        Node{opts.host, opts.port});
            if (slave_iter != slaves.end() && slave_iter != slaves.begin()) {
                // The given node is still a valid slave. Try it first.
                std::swap(*(slaves.begin()), *slave_iter);
            }

            for (const auto &slave : slaves) {
                try {
                    slave_node = slave;
                    auto connection = _connect_redis(slave, opts);
                    if (_get_role(connection) != Role::SLAVE) {
                        // Retry the whole process at most SentinelOptions::max_retry times.
                        ++retries;
                        if (retries > _sentinel_opts.max_retry) {
                            throw Error("reach max retry number");
                        }

                        std::this_thread::sleep_for(_sentinel_opts.retry_interval);

                        // Restart the iteration.
                        iter.reset();
                        break;
                    }

                    return connection;
                } catch (const Error &err) {
                    err_msgs.push_back(report_error(sentinel_node, slave_node, err.what()));
                    slave_node = Node{"", 0};

                    // Try the next slave.
                    continue;
                }
            }
        } catch (const StopIterError &err) {
            err_msgs.push_back(report_error(sentinel_node, slave_node, err.what()));
            throw StopIterError(err_msgs);
        } catch (const Error &err) {
            err_msgs.push_back(report_error(sentinel_node, slave_node, err.what()));
            continue;
        }
    }
}

Node Sentinel::_get_master_addr_by_name(Connection &connection, const StringView &name) {
    connection.send("SENTINEL GET-MASTER-ADDR-BY-NAME %b", name.data(), name.size());

    auto reply = connection.recv();

    assert(reply);

    auto master = reply::parse<Optional<std::pair<std::string, std::string>>>(*reply);
    if (!master) {
        throw Error(std::string("no master named ") + name.data());
    }

    int port = 0;
    try {
        port = std::stoi(master->second);
    } catch (const std::exception &) {
        throw ProtoError("Master port is invalid: " + master->second);
    }

    return Node{master->first, port};
}

std::vector<Node> Sentinel::_get_slave_addr_by_name(Connection &connection,
                                                    const StringView &name) {
    connection.send("SENTINEL SLAVES %b", name.data(), name.size());

    auto reply = connection.recv();

    assert(reply);

    auto slaves = _parse_slave_info(*reply);

    // Make slave list random.
    thread_local std::mt19937 gen(std::random_device{}());
    std::shuffle(slaves.begin(), slaves.end(), gen);

    return slaves;
}

std::vector<Node> Sentinel::_parse_slave_info(redisReply &reply) const {
    using SlaveInfo = std::unordered_map<std::string, std::string>;

    auto slaves = reply::parse<std::vector<SlaveInfo>>(reply);

    std::vector<Node> nodes;
    for (const auto &slave : slaves) {
        auto flags_iter = slave.find("flags");
        auto ip_iter = slave.find("ip");
        auto port_iter = slave.find("port");
        if (flags_iter == slave.end() || ip_iter == slave.end() || port_iter == slave.end()) {
            throw ProtoError("Invalid slave info");
        }

        // This slave is down, e.g. 's_down,slave,disconnected'
        if (flags_iter->second != "slave") {
            continue;
        }

        int port = 0;
        try {
            port = std::stoi(port_iter->second);
        } catch (const std::exception &) {
            throw ProtoError("Slave port is invalid: " + port_iter->second);
        }

        nodes.push_back(Node{ip_iter->second, port});
    }

    return nodes;
}

Connection Sentinel::_connect_redis(const Node &node, ConnectionOptions opts) {
    opts.host = node.host;
    opts.port = node.port;

    return Connection(opts);
}

Role Sentinel::_get_role(Connection &connection) {
    connection.send("INFO REPLICATION");
    auto reply = connection.recv();

    assert(reply);
    auto info = reply::parse<std::string>(*reply);

    const std::string ROLE_PREFIX = "role:";
    auto start = info.find(ROLE_PREFIX);
    if (start == std::string::npos) {
        throw ProtoError("Invalid INFO REPLICATION reply");
    }
    start += ROLE_PREFIX.size();
    auto stop = info.find("\r\n", start);
    if (stop == std::string::npos) {
        throw ProtoError("Invalid INFO REPLICATION reply");
    }

    auto role = info.substr(start, stop - start);
    if (role == "master") {
        return Role::MASTER;
    } else if (role == "slave") {
        return Role::SLAVE;
    } else {
        throw Error("Invalid role: " + role);
    }
}

std::list<ConnectionOptions> Sentinel::_parse_options(const SentinelOptions &opts) const {
    std::list<ConnectionOptions> options;
    for (const auto &node : opts.nodes) {
        ConnectionOptions opt;
        opt.host = node.first;
        opt.port = node.second;
        opt.user = opts.user;
        opt.password = opts.password;
        opt.keep_alive = opts.keep_alive;
        opt.connect_timeout = opts.connect_timeout;
        opt.socket_timeout = opts.socket_timeout;
        opt.tls = opts.tls;
        opt.resp = opts.resp;

        options.push_back(opt);
    }

    return options;
}

SimpleSentinel::SimpleSentinel(const std::shared_ptr<Sentinel> &sentinel,
                                const std::string &master_name,
                                Role role) :
                                    _sentinel(sentinel),
                                    _master_name(master_name),
                                    _role(role) {
    if (!_sentinel) {
        throw Error("Sentinel cannot be null");
    }

    if (_role != Role::MASTER && _role != Role::SLAVE) {
        throw Error("Role must be Role::MASTER or Role::SLAVE");
    }
}

Connection SimpleSentinel::create(const ConnectionOptions &opts) {
    assert(_sentinel);

    if (_role == Role::MASTER) {
        return _sentinel->master(_master_name, opts);
    }

    assert(_role == Role::SLAVE);

    return _sentinel->slave(_master_name, opts);
}

std::string StopIterError::_to_msg(const std::vector<std::string> &errs) const {
    std::string msg;
    for (const auto &err : errs) {
        if (!msg.empty()) {
            msg += "|";
        }
        msg += err;
    }

    return msg;
}

}

}
