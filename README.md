# redis-plus-plus

## Overview

This is a C++ client for Redis. It's based on [hiredis](https://github.com/redis/hiredis), and written in C++ 11.

It supports the following features:
- Most of commands for Redis 4.0.
- Connection pool.
- Redis scripting.
- Thread safe unless otherwise stated.
- Redis publish/subscribe (unstable).
- Redis pipeline (unstable).
- Redis transaction (unstable).
- Redis Cluster (coming soon).

## Installation

### Install hiredis

Since **redis-plus-plus** is based on hiredis, you should install hiredis first.

```
git clone https://github.com/redis/hiredis.git

cd hiredis

make

make install
```

Use `make PREFIX=/non/default/path install` to install hiredis at non-default location.

### Install redis-plus-plus

*redis-plus-plus* is built with [CMAKE](https://cmake.org).

```
mkdir compile

cd compile

cmake -DCMAKE_BUILD_TYPE=Release ..

make

make install
```

If hiredis is installed at non-default location, you should specify `-DCMAKE_PREFIX_PATH=/path/to/hiredis` when running cmake. Also you can specify `-DCMAKE_INSTALL_PREFIX=/path/to/install/redis-plus-plus` to install *redis-plus-plus* at non-default location.

## Getting Started

```
#include <sw/redis++/redis++.h>

using namespace sw::redis;

try {
    Redis redis("tcp://127.0.0.1:6379");

    // STRING commands.
    redis.set("key", "val");
    auto val = redis.get("key");
    if (val) {
        std::cout << *val << std::endl;
    }

    // LIST commands.
    redis.lpush("list", {"a", "b", "c"});
    std::vector<std::string> list;
    redis.lrange("list", 0, -1, back_inserter(list));

    // HASH commands.
    // The same as redis.hset("hash", std::make_pair("field", "val"));
    redis.hset("hash", "field", "val");
    std::unorderd_map<std::string, std::string> m = {
        {"f1", "v1"},
        {"f2", "v2"}
    };
    redis.hmset("hash", m.begin(), m.end());
    m.clear();
    redis.hgetall("hash", std::inserter(m, m.begin()));
    std::vector<string> vals;
    redis.hmget("hash", {"f1", "f2"}, std::back_inserter(vals));

    // SET commands.
    redis.sadd("set", "m1");
    auto members = {"m2", "m3"};
    redis.sadd(members);
    if (redis.ismember("m1")) {
        std::cout << "m1 exists" << std::endl;
    }

    // SORTED SET commands.
    redis.zadd("sorted_set", "m1", 1.3);
    std::unorderd_map<std::string, double> scores = {
        {"m2", 2.3},
        {"m3", 4.5}
    };
    redis.zadd("sorted_set", scores.begin(), scores.end());
    std::vector<std::string> without_score;
    redis.zrangebyscore("sorted_set",
            BoundedInterval<double>(1.5, 3.4),
            std::back_inserter(without_score));
    std::unorderd_map<std::string, double> with_score;
    redis.zrangebyscore("sorted_set",
            BoundedInterval<double>(1.5, 3.4),
            std::inserter(with_score, with_score.end()));

    // SCRIPTING commands.
    auto num = redis.eval<long long>("return 1", {}, {});
    std::vector<long long> nums;
    redis.eval("return {ARGV[1], ARGV[2]}", {}, {"1", "2"}, std::back_inserter(nums));
} catch (const Error &e) {
    // Error handling.
}
```

## API Reference

### Connection

`Redis` class maintains a connection pool to Redis server. If the connection is broken, `Redis` reconnects to Redis server automatically.

You can initialize a `Redis` instance with [ConnectionOptions](https://github.com/sewenew/redis-plus-plus/blob/master/src/sw/redis%2B%2B/connection.h#L40) and [ConnectionPoolOptions](https://github.com/sewenew/redis-plus-plus/blob/master/src/sw/redis%2B%2B/connection_pool.h#L30). `ConnectionOptions` specifies options for connection to Redis server, and `ConnectionPoolOptions` specifies options for conneciton pool. `ConnectionPoolOptions` is optional. If not specified, `Redis` maintains a single connection to Redis server.

```
ConnectionOptions connection_options;
connection_options.host = "127.0.0.1";  // Required.
connection_options.port = 6666; // Optional. The default port is 6379.
connection_options.password = "auth";   // Optional. No password by default.
connection_options.db = 1;  // Optional. Use the 0th database by default.

// Connect to Redis server with a single connection.
Redis redis1(connection_options);

ConnectionPoolOptions pool_options;
pool_options.size = 3;  // Pool size, i.e. max number of connections.

// Connect to Redis server with a connection pool.
Redis redis2(connection_options, pool_options);
```

*redis-plus-plus* also supports connecting to Redis server with Unix Domain Socket.

```
ConnectionOptions options;
options.type = ConnectionType::UNIX;
options.path = "/path/to/socket";
Redis redis(options);
```

You can also connect to Redis server with a URI. However, in this case, you can only specify *host*, *port* and *socket path*. In order to specify other options, you need to use `ConnectionOptions` and `ConnectionPoolOptions`.

```
// Single connection to the given host and port.
Redis redis1("tcp://127.0.0.1:6666");

// Use default port, i.e. 6379.
Redis redis2("tcp://127.0.0.1");

// Connect to Unix Domain Socket.
Redis redis3("unix://path/to/socket");
```

### Command

You can send [Redis commands](https://redis.io/commands) through `Redis` object. `Redis` has one or more (overload) methods for each Redis command. The method has the same name as the corresponding command.

#### Parameter

Most of these methods have the same parameters as the corresponding commands. The following is a list of parameter types:

- StringView: We use [string_view](http://en.cppreference.com/w/cpp/string/basic_string_view) as the type of string parameters. However, by now, not all compilers support `string_view`. So when compiling *redis-plus-plus*, CMAKE checks if your compiler supports `string_view`. If not, it uses our own simple [implementation](https://github.com/sewenew/redis-plus-plus/blob/master/src/sw/redis%2B%2B/utils.h#L48). Since there are conversions from `std::string` and c-style string to `StringView`, you can just pass `std::string` or c-style string to methods that need a `StringView` parameter.
- long long: Parameters of integer type.
- double: Parameters of floating-point type.
- std::chrono::duration and std::chrono::time_point: Time-related parameters.
- options: See [command_options.h](https://github.com/sewenew/redis-plus-plus/blob/master/src/sw/redis%2B%2B/command_options.h) for details.
- pair of iterators: Use a pair of iterators to specify a batch of input.
- std::initializer_list<T>: Use an initializer list to specify a batch of input.

#### Reply

[Redis protocol](https://redis.io/topics/protocol) defines 5 kinds of replies:
- Status Reply: Also known as *Simple String Reply*. It's a non-binary string reply.
- Bulk String Reply: Binary safe string reply.
- Integer Reply: Signed integer reply. Large enough to hold `long long`.
- Array Reply: (Nested) Array reply.
- Error Reply: Non-binary string reply that gives error info.

Also these replies might be *NULL*. For instance, when you try to `GET` the value of a nonexistent key, Redis returns a *NULL Bulk String Reply*.

*redis-plus-plus* sends commands and receives replies synchronously. Replies are parsed into return values of these methods. The following is a list of return types:

- void: *Status Reply* that should always return a string of *OK*.
- std::string: *Status Reply* that not always returns *OK*, and *Bulk String Reply*.
- Optional<std::string>: *Status Reply* and *Bulk String Reply* that might be *NULL*.
- long long: *Integer Reply*.
- Optional<long long>: *Integer Reply* that might be *NULL*.
- bool: *Integer Reply* that always returns 0 or 1.
- double: *Bulk String Reply* that represents a double.
- Optional<double>: double reply that might be *NULL*.
- Output Iterator: *Array Reply*. We use STL-like interface to return array replies.

We use [std::optional](http://en.cppreference.com/w/cpp/utility/optional) as return type, if Redis might return *NULL* reply. Again, since not all compilers support `std::optional` so far, we implement our own simple [version](https://github.com/sewenew/redis-plus-plus/blob/master/src/sw/redis%2B%2B/utils.h#L85).

#### Exception

*redis-plus-plus* throws exceptions if something bad happens. All exceptions derived from `Error` class.

**NOTE**:

*NULL* reply is not taken as an error, e.g. key not found. Instead it returns a null Optional<T> object.

## Author

*redis-plus-plus* is written by sewenew, who is also active on [StackOverflow](https://stackoverflow.com/users/5384363/for-stack).
