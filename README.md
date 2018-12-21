# redis-plus-plus

- [Overview](https://github.com/sewenew/redis-plus-plus#overview)
    - [Features](https://github.com/sewenew/redis-plus-plus#features)
- [Installation](https://github.com/sewenew/redis-plus-plus#installation)
    - [Install hiredis](https://github.com/sewenew/redis-plus-plus#install-hiredis)
    - [Install redis-plus-plus](https://github.com/sewenew/redis-plus-plus#install-redis-plus-plus)
    - [Run Tests (Optional)](https://github.com/sewenew/redis-plus-plus#run-tests-optional)
    - [Use redis-plus-plus In Your Project](https://github.com/sewenew/redis-plus-plus#use-redis-plus-plus-in-your-project)
- [Getting Started](https://github.com/sewenew/redis-plus-plus#getting-started)
- [API Reference](https://github.com/sewenew/redis-plus-plus#api-reference)
    - [Connection](https://github.com/sewenew/redis-plus-plus#connection)
    - [Command](https://github.com/sewenew/redis-plus-plus#command)
    - [Publish/Subscribe](https://github.com/sewenew/redis-plus-plus#publishsubscribe)
    - [Pipeline](https://github.com/sewenew/redis-plus-plus#pipeline)
    - [Transaction](https://github.com/sewenew/redis-plus-plus#transaction)
    - [Redis Cluster](https://github.com/sewenew/redis-plus-plus#redis-cluster)
- [Author](https://github.com/sewenew/redis-plus-plus#author)

## Overview

This is a C++ client for Redis. It's based on [hiredis](https://github.com/redis/hiredis), and written in C++ 11.

### Features
- Most commands for Redis 4.0.
- Connection pool.
- Redis scripting.
- Thread safe unless otherwise stated.
- Redis publish/subscribe.
- Redis pipeline.
- Redis transaction.
- Redis Cluster.
- STL-like interfaces.

## Installation

### Install hiredis

Since **redis-plus-plus** is based on hiredis, you should install hiredis first. The minimum version requirement for hiredis is **v0.12.1**, and you'd better use the latest release of hiredis.

```
git clone https://github.com/redis/hiredis.git

cd hiredis

make

make install
```

If you want to install *hiredis* at non-default location, use the following commands to specify the installation path.

```
make PREFIX=/non/default/path

make PREFIX=/non/default/path install
```

### Install redis-plus-plus

*redis-plus-plus* is built with [CMAKE](https://cmake.org).

```
git clone https://github.com/sewenew/redis-plus-plus.git

cd redis-plus-plus

mkdir compile

cd compile

cmake -DCMAKE_BUILD_TYPE=Release ..

make

make install

cd ..
```

If hiredis is installed at non-default location, you should use `CMAKE_PREFIX_PATH` to specify the installation path of *hiredis*. Also you can use `CMAKE_INSTALL_PREFIX` to install *redis-plus-plus* at non-default location.

```
cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_PREFIX_PATH=/path/to/hiredis -DCMAKE_INSTALL_PREFIX=/path/to/install/redis-plus-plus ..
```

### Run Tests (Optional)

After compiling with cmake, you'll get a test program in `compile/test` directory: `test_redis++`.

In order to run the tests, you need to set up both a Redis instance, and a Redis Cluster. Since the test program will send most of Redis commands to the server and cluster, you need to set up Redis of the latest version (by now, it's 5.0). Otherwise, the tests might fail. For example, if you set up Redis 4.0 for testing, the test program will fail when it tries to send the `ZPOPMAX` command (a Redis 5.0 command) to the server. If you want to run the tests with other Redis versions, you have to comment out commands that haven't been supported by your Redis, from test source files in `redis-plus-plus/test/src/sw/redis++/` directory. Sorry for the inconvenience, and I'll fix this problem to make the test program work with any version of Redis in the future.

**NOTE**: The latest version of Redis is only a requirement for running the tests. In fact, you can use *redis-plus-plus* with Redis of any version, e.g. Redis 2.0, Redis 3.0, Redis 4.0, Redis 5.0.

Then you can run the test program with the following command:

```
./compile/test/test_redis++ -h host -p port -a auth -n cluster_node -c cluster_port
```

- `host` and `port` are the host and port number of the Redis instance.
- `cluster_node` and `cluster_port` are the host and port number of Redis Cluster. You only need to set the host and port number of a single node in the cluster, *redis-plus-plus* will find other nodes automatically.
- `auth` is the password of the Redis instance and Redis Cluster. The Redis instance and Redis Cluster must be configured with the same password. If there's no password configured, don't set this option.

The test program will test running *redis-plus-plus* in multi-threads environment, and this test will cost a long time. If you want to skip it (not recommended), just comment out the following lines in `test/src/sw/redis++/test_main.cpp` file.

```
sw::redis::test::ThreadsTest threads_test(opts, cluster_node_opts);
threads_test.run();
```

If all tests have been passed, the test program will print the following message: *Pass all tests*. Otherwise, it prints the error message.

### Use redis-plus-plus In Your Project

Since *redis-plus-plus* depends on *hiredis*, you need to link both libraries to your Application. Also don't forget to specify the `-std=c++11` and thread-related option. Take GCC as an example.

#### Use Shared Libraries

```
g++ -std=c++11 -lhiredis -lredis++ -pthread -o app app.cpp
```

If *hiredis* and *redis-plus-plus* are installed at non-default location, you should use `-I` and `-L` options to specify the header and library paths.

```
g++ -std=c++11 -I/non-default/install/include/path -L/non-default/install/lib/path -lhiredis -lredis++ -pthread -o app app.cpp
```

#### Use Static Libraries

```
g++ -std=c++11 -pthread -o app app.cpp /path/to/libhiredis.a /path/to/libredis++.a
```

If *hiredis* and *redis-plus-plus* are installed at non-default location, you should use `-I` option to specify the header path.

```
g++ -std=c++11 -pthread -I/non-default/install/include/path -o app app.cpp /path/to/libhiredis.a /path/to/libredis++.a
```

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
    } // else key doesn't exist.

    // LIST commands.
    redis.lpush("list", {"a", "b", "c"});
    std::vector<std::string> list;
    redis.lrange("list", 0, -1, std::back_inserter(list));

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
    std::vector<std::string> vals;
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

    // Pipeline
    auto pipe = redis.pipeline();

    auto pipe_replies = pipe.set("key", "value")
                            .get("key")
                            .rename("key", "new-key")
                            .lpush("list", {"a", "b", "c"})
                            .lrange("list", 0, -1)
                            .exec();

    auto set_result = pipe_replies.get<bool>(0);

    auto get_result = pipe_replies.get<OptionalString>(1);

    // rename result
    pipe_replies.get<void>(2);

    auto lpush_result = pipe_replies.get<long long>(3);

    std::vector<std::string> lrange_result
    pipe_replies.get(4, back_inserter(lrange_result));

    // Transaction
    auto tx = redis.transaction();

    auto tx_replies = tx.incr("num0")
                        .incr("num1")
                        .mget({"num0", "num1"})
                        .exec();

    auto incr_result0 = tx_replies.get<long long>(0);

    auto incr_result1 = tx_replies.get<long long>(1);

    std::vector<OptionalString> mget_result;
    tx_replies.get(2, back_inserter(mget_result));

    // Redis Cluster
    auto redis_cluster = RedisCluster("tcp://127.0.0.1:7000");

    // RedisCluster has similar interface as Redis.
    redis_cluster.set("key", "value");
    auto val = redis_cluster.get("key");
    if (val) {
        std::cout << *val << std::endl;
    }

    // Keys with hash-tag.
    redis_cluster.set("key{tag}1", "val1");
    redis_cluster.set("key{tag}2", "val2");
    redis_cluster.set("key{tag}3", "val3");
    std::vector<OptionalString> hash_tag_res;
    redis_cluster.mget({"key{tag}1", "key{tag}2", "key{tag}3"},
            std::back_inserter(hash_tag_res));

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

// Optional. Timeout before we successfully send request to or receive response from redis.
// By default, the timeout is 0ms, i.e. no timeout and block until we send or receive successfuly.
// NOTE: if any command is timed out, we throw a TimeoutError exception.
connection_options.socket_timeout = std::chrono::milliseconds(200);

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

#### Lazily Create Connection

Connections in the pool are lazily created. When the connection pool is initialized, i.e. the constructor of `Redis`, `Redis` does NOT connect to the server. Instead, it connects to the server only when you try to send command. In this way, we can avoid unnecessary connections. So if the pool size is 5, but the number of max concurrent connections is 3, there will be only 3 connections in the pool.

### Command

You can send [Redis commands](https://redis.io/commands) through `Redis` object. `Redis` has one or more (overloaded) methods for each Redis command. The method has the same name as the corresponding command.

#### Parameter

Most of these methods have the same parameters as the corresponding commands. The following is a list of parameter types:

- `StringView`: We use [string_view](http://en.cppreference.com/w/cpp/string/basic_string_view) as the type of string parameters. However, by now, not all compilers support `string_view`. So when compiling *redis-plus-plus*, CMAKE checks if your compiler supports `string_view`. If not, it uses our own simple [implementation](https://github.com/sewenew/redis-plus-plus/blob/master/src/sw/redis%2B%2B/utils.h#L48). Since there are conversions from `std::string` and c-style string to `StringView`, you can just pass `std::string` or c-style string to methods that need a `StringView` parameter.
- `long long`: Parameters of integer type. Normally used for index or number.
- `double`: Parameters of floating-point type. Normally used for score (`Sorted Set` commands) or latitude/longitude (`GEO` commands).
- `std::chrono::duration` and `std::chrono::time_point`: Time-related parameters.
- some options: See [command_options.h](https://github.com/sewenew/redis-plus-plus/blob/master/src/sw/redis%2B%2B/command_options.h) for details.
- pair of iterators: Use a pair of iterators to specify a batch of input.
- `std::initializer_list<T>`: Use an initializer list to specify a batch of input.

#### Reply

[Redis protocol](https://redis.io/topics/protocol) defines 5 kinds of replies:
- *Status Reply*: Also known as *Simple String Reply*. It's a non-binary string reply.
- *Bulk String Reply*: Binary safe string reply.
- *Integer Reply*: Signed integer reply. Large enough to hold `long long`.
- *Array Reply*: (Nested) Array reply.
- *Error Reply*: Non-binary string reply that gives error info.

Also these replies might be *NULL*. For instance, when you try to `GET` the value of a nonexistent key, Redis returns a *NULL Bulk String Reply*.

*redis-plus-plus* sends commands and receives replies synchronously. Replies are parsed into return values of these methods. The following is a list of return types:

- `void`: *Status Reply* that should always return a string of "OK", e.g. `RENAME`, `SETEX`.
- `std::string`: *Status Reply* that not always return "OK", and *Bulk String Reply*, e.g. `PING`, `INFO`.
- `bool`: *Integer Reply* that always returns 0 or 1, e.g. `EXPIRE`, `SET`.
- `long long`: *Integer Reply* that not always return 0 or 1, e.g. `DEL`, `APPEND`.
- `double`: *Bulk String Reply* that represents a double, e.g. `INCRBYFLOAT`, `ZINCRBY`.
- `std::pair`: *Array Reply* with 2 elements. Since the return value is always an array of 2 elements, we return the 2 elements as a `std::pair`'s first and second elements, e.g. `BLPOP`.
- `std::tuple`: *Array Reply* with fixed length and has more than 2 elements. Since length of the returned array is fixed, we return the array as a `std::tuple`, e.g. `BZPOPMAX`.
- `Optional<T>`: For any reply of type `T` that might be *NULL*, `GET`, `LPOP`, `BLPOP`, `BZPOPMAX`. We'll explain in detail latter.
- Output Iterator: General *Array Reply* with non-fixed length. We use STL-like interface to return array replies, `MGET`, `LRANGE`. Also, sometimes the type of output iterator decides which options to send with the command. See the [Examples](https://github.com/sewenew/redis-plus-plus#command-overloads) part for detail.

We use [std::optional](http://en.cppreference.com/w/cpp/utility/optional) as return type, if Redis might return *NULL* reply. Again, since not all compilers support `std::optional` so far, we implement our own simple [version](https://github.com/sewenew/redis-plus-plus/blob/master/src/sw/redis%2B%2B/utils.h#L85). Take the [GET](https://redis.io/commands/get) and [MGET](https://redis.io/commands/mget) commands for example:

```
// Or just: auto val = redis.get("key");
Optional<std::string> val = redis.get("key");

if (val) {
    // Key exists
    cout << *val << endl;
} else {
    cout << "key doesn't exist." << endl;
}

vector<Optional<std::string>> values;
redis.mget({"key1", "key2", "key3"}, back_inserter(values));
for (const auto &val : values) {
    if (val) {
        // Key exist, process the value.
    }
}
```

We also have some typedefs for some commonly used `Optional<T>`:

```
using OptionalString = Optional<std::string>;

using OptionalLongLong = Optional<long long>;

using OptionalDouble = Optional<double>;

using OptionalStringPair = Optional<std::pair<std::string, std::string>>;
```

#### `command` Method

There're too many Redis commands, we haven't implemented all of them. However, you can use `Redis::command` method to send these commands to Redis.

```
template <typename Cmd, typename ...Args>
ReplyUPtr Redis::command(Cmd cmd, Args &&...args);
```

In order to use this method, you need to pass in a `Cmd` object, which must be a callable object, e.g. function, functor, or lambda. The first argument of `Cmd` is of type `Connection`. `Redis::command` will fetch a connection from the connection pool, and pass the connection and `args` as arguments for `Cmd`. `Cmd` can call the overloaded `Connection::send` methods to send the command to Redis.

`Redis::command` returns a `ReplyUPtr`, i.e. `std::unique_ptr<redisReply, ReplyDeleter>`. Normally you don't need to parse it manually. Instead, you only need to pass the reply to `template <typename T> T reply::parse(redisReply &)` to get a value of type `T`. By now, `T` can be `std::string`, `double`, `long long`, `bool`, `void`, `Optional<T>`, `std::pair`, and `std::tuple`.

Let's see an example:

```
auto lpush_num = [](Connection &connection, const StringView &key, long long num) {
    connection.send("LPUSH %b %lld",
                    key.data(), key.size(),
                    num);
};

auto lpush_nums = [](Connection &connection,
                        const StringView &key,
                        const std::vector<long long> &nums) {
    CmdArgs args;
    args.append("LPUSH").append(key);
    for (auto num : nums) {
        args.append(std::to_string(num));
    }

    connection.send(args);
};

auto reply = redis.command(lpush_num, "list", 1);
assert(reply::parse<long long>(*reply) == 1);

reply = redis.command(lpush_nums, "list", std::vector<long long>{2, 3, 4, 5});
assert(reply::parse<long long>(*reply) == 5);
```

Please see [connection.h](https://github.com/sewenew/redis-plus-plus/blob/master/src/sw/redis%2B%2B/connection.h), [command_args.h](https://github.com/sewenew/redis-plus-plus/blob/master/src/sw/redis%2B%2B/command_args.h), and [command.h](https://github.com/sewenew/redis-plus-plus/blob/master/src/sw/redis%2B%2B/command.h) for details.

#### Exception

*redis-plus-plus* throws exceptions if it receives an *Error Reply* or something bad happens. All exceptions derived from `Error` class. See [errors.h](https://github.com/sewenew/redis-plus-plus/blob/master/src/sw/redis%2B%2B/errors.h) for detail.

**NOTE**: *NULL* reply is not taken as an exception, e.g. key not found. Instead we return it as a null `Optional<T>` object.

#### Examples

##### Various Parameters

```
// Parameters of StringView type.

// Implicitly construct StringView with c-style string.
redis.set("key", "value");

// Implicitly construct StringView with std::string.
string key("key");
string val("value");
redis.set(key, val);

// Explicitly pass StringView as parameter.
vector<char> large_data;
// Avoid copying.
redis.set("key", StringView(large_data.data(), large_data.size()));

// Parameters of long long type.

// For index.
redis.bitcount(key, 1, 3);

// For number.
redis.incrby("num", 100);

// Parameters of double type.

// For score.
redis.zadd("zset", "m1", 2.5);
redis.zadd("zset", "m2", 3.5);
redis.zadd("zset", "m3", 5);

// For latitude/longitude.
redis.geoadd("geo", make_tuple("member", 13.5, 15.6));

// Time-related parameters.
using namespace std::chrono;

redis.expire(key, seconds(1000));

auto tp = time_point_cast<seconds>(system_clock::now() + seconds(100));
redis.expireat(key, tp);

// Some options for commands.
if (redis.set(key, "value", milliseconds(100), UpdateType::NOT_EXIST)) {
cout << "set OK" << endl;
}

redis.linsert("list", InsertPosition::BEFORE, "pivot", "val");

vector<string> res;

// (-inf, inf)
redis.zrangebyscore("zset", UnboundedInterval<double>{}, std::back_inserter(res));

// [3, 6]
redis.zrangebyscore("zset",
    BoundedInterval<double>(3, 6, BoundType::CLOSED),
    std::back_inserter(res));

// (3, 6]
redis.zrangebyscore("zset",
    BoundedInterval<double>(3, 6, BoundType::LEFT_OPEN),
    std::back_inserter(res));

// (3, 6)
redis.zrangebyscore("zset",
    BoundedInterval<double>(3, 6, BoundType::OPEN),
    std::back_inserter(res));

// [3, 6)
redis.zrangebyscore("zset",
    BoundedInterval<double>(3, 6, BoundType::RIGHT_OPEN),
    std::back_inserter(res));

// [3, +inf)
redis.zrangebyscore("zset",
    LeftBoundedInterval<double>(3, BoundType::RIGHT_OPEN),
    std::back_inserter(res));

// (3, +inf)
redis.zrangebyscore("zset",
    LeftBoundedInterval<double>(3, BoundType::OPEN),
    std::back_inserter(res));

// (-inf, 6]
redis.zrangebyscore("zset",
    RightBoundedInterval<double>(6, BoundType::LEFT_OPEN),
    std::back_inserter(res));

// (-inf, 6)
redis.zrangebyscore("zset",
    LeftBoundedInterval<double>(6, BoundType::OPEN),
    std::back_inserter(res));

// Pair of iterators.
vector<pair<string, string>> kvs = {{"k1", "v1"}, {"k2", "v2"}, {"k3", "v3"}};
redis.mset(kvs.begin(), kvs.end());

unordered_map<string, string> kv_map = {{"k1", "v1"}, {"k2", "v2"}, {"k3", "v3"}};
redis.mset(kv_map.begin(), kv_map.end());

unordered_map<string, string> str_map = {{"f1", "v1"}, {"f2", "v2"}, {"f3", "v3"}};
redis.hmset("hash", str_map.begin(), str_map.end());

unordered_map<string, double> score_map = {{"m1", 20}, {"m2", 12.5}, {"m3", 3.14}};
redis.zadd("zset", score_map.begin(), score_map.end());

vector<string> keys = {"k1", "k2", "k3"};
redis.del(keys.begin(), keys.end());

// Parameters of initializer_list type
redis.mset({
    make_pair("k1", "v1"),
    make_pair("k2", "v2"),
    make_pair("k3", "v3")
});

redis.hmset("hash",
    {
        make_pair("f1", "v1"),
        make_pair("f2", "v2"),
        make_pair("f3", "v3")
    });

redis.zadd("zset",
    {
        make_pair("m1", 20.0),
        make_pair("m2", 34.5),
        make_pair("m3", 23.4)
    });

redis.del({"k1", "k2", "k3"});
```

##### Various Return Types

```
// Return void
redis.save();

// Return std::string
auto info = redis.info();

// Return bool
if (!redis.expire("nonexistent", seconds(100))) {
    cerr << "key doesn't exist" << endl;
}

if (redis.setnx("key", "val")) {
    cout << "set OK" << endl;
}

// Return long long
auto len = redis.strlen("key");
auto num = redis.del({"a", "b", "c"});
num = redis.incr("a");

// Return double
auto real = redis.incrbyfloat("b", 23.4);
real = redis.hincrbyfloat("c", "f", 34.5);

// Optional<std::string>, i.e. OptionalString
auto os = redis.get("kk");
if (os) {
    cout << *os << endl;
} else {
    cerr << "key doesn't exist" << endl;
}

os = redis.spop("set");
if (os) {
    cout << *os << endl;
} else {
    cerr << "set is empty" << endl;
}

// Optional<long long>, i.e. OptionalLongLong
auto oll = redis.zrank("zset", "mem");
if (oll) {
    cout << "rank is " << *oll << endl;
} else {
    cerr << "member doesn't exist" << endl;
}

// Optional<double>, i.e. OptionalDouble
auto ob = redis.zscore("zset", "m1");
if (ob) {
    cout << "score is " << *ob << endl;
} else {
    cerr << "member doesn't exist" << endl;
}

// Optional<pair<string, string>>
auto op = redis.blpop({"list1", "list2"}, seconds(2));
if (op) {
    cout << "key is " << op->first << ", value is " << op->second << endl;
} else {
    cerr << "timeout" << endl;
}

// Output iterators.
vector<OptionalString> os_vec;
redis.mget({"k1", "k2", "k3"}, back_inserter(os_vec));

vector<string> s_vec;
redis.lrange("list", 0, -1, back_inserter(s_vec));

unordered_map<string, string> hash;
redis.hmget("hash", {"m1", "m2", "m3"}, inserter(hash, hash.end()));
// You can also save the result in a vecotr of string pair.
vector<pair<string, string>> hash_vec;
redis.hmget("hash", {"m1", "m2", "m3"}, back_inserter(hash_vec));

unordered_set<string> str_set;
redis.smembers("s1", inserter(str_set, str_set.end()));
// You can also save the result in a vecotr of string.
s_vec.clear();
redis.smembers("s1", back_inserter(s_vec));
```

##### SCAN Commands

```
auto cursor = 0LL;
auto pattern = "*pattern*";
auto count = 5;
vector<string> scan_vec;
while (true) {
    cursor = redis.scan(cursor, pattern, count, back_inserter(scan_vec));
    // Default pattern is "*", and default count is 10
    // cursor = redis.scan(cursor, back_inserter(scan_vec));

    if (cursor == 0) {
        break;
    }
}
```

##### Command Overloads

Sometimes the type of output iterator decides which options to send with the command.

```
// If the output iterator is an iterator of a container of string,
// we send *ZRANGE* command without the *WITHSCORES* option.
vector<string> members;
redis.zrange("list", 0, -1, back_inserter(members));

// If it's an iterator of a container of a <string, double> pair,
// we send *ZRANGE* command with *WITHSCORES* option.
unordered_map<string, double> res_with_score;
redis.zrange("list", 0, -1, inserter(res_with_score, res_with_score.end()));

// The above examples also apply to other command with the *WITHSCORES* options,
// e.g. *ZRANGEBYSCORE*, *ZREVRANGE*, *ZREVRANGEBYSCORE*.

// Another example is the *GEORADIUS* command.

// Only get members.
members.clear();
redis.georadius("geo",
            make_pair(10.1, 11.1),
            100,
            GeoUnit::KM,
            10,
            true,
            back_inserter(members));

// If the iterator is an iterator of a container of tuple<string, double>,
// we send the *GEORADIUS* command with *WITHDIST* option.
vector<tuple<string, double>> mem_with_dist;
redis.georadius("geo",
            std::make_pair(10.1, 11.1),
            100,
            GeoUnit::KM,
            10,
            true,
            std::back_inserter(mem_with_dist));

// If the iterator is an iterator of a container of tuple<string, double, string>,
// we send the *GEORADIUS* command with *WITHDIST* and *WITHHASH* options.
vector<tuple<string, double, string>> mem_with_dist_hash;
redis.georadius("geo",
            std::make_pair(10.1, 11.1),
            100,
            GeoUnit::KM,
            10,
            true,
            std::back_inserter(mem_with_dist_hash));

// If the iterator is an iterator of a container of
// tuple<string, string, pair<double, double>, double>,
// we send the *GEORADIUS* command with *WITHHASH*, *WITHCOORD* and *WITHDIST* options.
vector<tuple<string, double, string>> mem_with_hash_coord_dist;
redis.georadius("geo",
            std::make_pair(10.1, 11.1),
            100,
            GeoUnit::KM,
            10,
            true,
            std::back_inserter(mem_with_hash_coord_dist));
```

Please see [redis.h](https://github.com/sewenew/redis-plus-plus/blob/master/src/sw/redis%2B%2B/redis.h) for more API references, and see the [tests](https://github.com/sewenew/redis-plus-plus/tree/master/test/src/sw/redis%2B%2B) for more examples.

### Publish/Subscribe

You can use `Redis::publish` to publish messages to channels. *redis-plus-plus* picks a connection from the connection pool, and publishes message with that connection. So you might publish two messages with two different connections.

When you subscribe to a channel with a connection, all messages published to the channel are sent back to that connection. So there's **NO** `Redis::subscribe` method. Instead, you can use `Redis::subscriber` to create a `Subscriber` and the `Subscriber` maintains a connection to Redis. The underlying connection is a new connection, NOT picked from the connection pool. This new connection has the same `ConnectionOptions` as the `Redis` object.

With `Subscriber`, you can call `Subscriber::subscribe`, `Subscriber::unsubscribe`, `Subscriber::psubscribe` and `Subscriber::punsubscribe` to send *SUBSCRIBE*, *UNSUBSCRIBE*, *PSUBSCRIBE* and *PUNSUBSCRIBE* commands to Redis.

#### Thread Safety

`Subscriber` is **NOT** thread-safe. If you want to call its member functions in multi-thread environment, you need to synchronize between threads manually.

#### Subscriber Callbacks

There are 6 kinds of messages:
- *MESSAGE*: message sent to a channel.
- *PMESSAGE*: message sent to channels of a given pattern.
- *SUBSCRIBE*: message sent when we successfully subscribe to a channel.
- *UNSUBSCRIBE*: message sent when we successfully unsubscribe to a channel.
- *PSUBSCRIBE*: message sent when we successfully subscribe to a channel pattern.
- *PUNSUBSCRIBE*: message sent when we successfully unsubscribe to a channel pattern.

We call messages of *SUBSCRIBE*, *UNSUBSCRIBE*, *PSUBSCRIBE* and *PUNSUBSCRIBE* types as *META MESSAGE*s.

In order to process these messages, you can set callback functions on `Subscriber`:
- `Subscriber::on_message(MsgCallback)`: set callback function for messages of *MESSAGE* type, and the callback interface is: `void (std::string channel, std::string msg)`.
- `Subscriber::on_pmessage(PatternMsgCallback)`: set the callback function for messages of *PMESSAGE* type, and the callback interface is: `void (std::string pattern, std::string channel, std::string msg)`.
- `Subscriber::on_meta(MetaCallback)`: set callback function for messages of *META MESSAGE* type, and the callback interface is: `void (Subscriber::MsgType type, OptionalString channel, long long num)`. If you haven't subscribe/psubscribe to any channel/pattern, and try to unsubscribe/punsubscribe without any parameter, i.e. unsubscribe/punsubscribe all channels/patterns, *channel* will be null. So the second parameter of meta callback is of type *OptionalString*.

All these callback interfaces pass `std::string` by value, and you can take their ownership (i.e. std::move) safely.

#### Consume Messages

You can call `Subscriber::consume` to consume messages published to channels/patterns that the `Subscriber` has been subscribed.

`Subscriber::consume` waits for message from the underlying connection. If the `ConnectionOptions::socket_timeout` is reached, and there's no message sent to this connection, `Subscriber::consume` throws `TimeoutError` exception. If `ConnectionOptions::socket_timeout` is 0ms, `Subscriber::consume` blocks until it receives a message.

After receiving the message, `Subscriber::consume` calls the callback function to process the message based on message type. However, if you don't set callback for a specific kind of message, `Subscriber::consume` will ignore the received message, i.e. no callback will be called.

#### Examples

The following example is a common pattern for using `Subscriber`:

```
// Create a Subscriber.
auto sub = redis.subscriber();

// Set callback functions.
sub.on_message([](std::string channel, std::string msg) {
            // Process message of MESSAGE type.
        });

sub.on_pmessage([](std::string pattern, std::string channel, std::string msg) {
            // Process message of PMESSAGE type.
        });

sub.on_meta([](Subscriber::MsgType type, OptionalString channel, long long num) {
            // Process message of META type.
        });

// Subscribe to channels and patterns.
sub.subscribe("channel1");
sub.subscribe({"channel2", "channel3"});

sub.psubscribe("pattern1*");

// Consume messages in a loop.
while (true) {
    try {
        sub.consume();
    } catch (...) {
        // Handle exceptions.
    }
}
```

If `ConnectionOptions::socket_timeout` is set, you might get `TimeoutError` exception before receiving a message:

```
while (true) {
    try {
        sub.consume();
    } catch (const TimeoutError &e) {
        // Try again.
        continue;
    } catch (...) {
        // Handle other exceptions.
    }
}
```

### Pipeline

[Pipeline](https://redis.io/topics/pipelining) is used to reduce *RTT* (Round Trip Time), and speed up Redis queries. *redis-plus-plus* supports pipeline with the `Pipeline` class.

#### Create Pipeline

You can create a pipeline with `Redis::pipeline` method, which returns a `Pipeline` object.

```
ConnectionOptions connection_options;
ConnectionPoolOptions pool_options;

Redis redis(connection_options, pool_options);

auto pipe = redis.pipeline();
```

When creating a `Pipeline` object, `Redis::pipeline` method creates a new connection to Redis server. This connection is NOT picked from the connection pool, but a newly created connection. This connection has the same `ConnectionOptions` with other connections in the connection pool. `Pipeline` object maintains the new connection, and all piped commands are sent through this connection.

#### Send Commands

You can send Redis commands through the `Pipeline` object. Just like the `Redis` class, `Pipeline` has one or more (overloaded) methods for each Redis command. However, you CANNOT get the replies until you call `Pipeline::exec`. So these methods do NOT return the reply, instead they return the `Pipeline` object itself. And you can chain these methods calls.

```
pipe.set("key", "val").incr("num").lpush("list", {0, 1, 2});
```

#### Get Replies

Once you finish sending commands to Redis, you can call `Pipeline::exec` to get replies of these commands. You can also chain `Pipeline::exec` with other commands.

```
pipe.set("key", "val").incr("num");
auto replies = pipe.exec();

// The same as:
auto another_replies = pipe.set("key", "val").incr("num).exec();
```

In fact, these commands won't be sent to Redis, until you call `Pipeline::exec`. So `Pipeline::exec` does 2 work: send piped commands and get replies from Redis.

Also you can call `Pipeline::discard` to discard those piped commands.

```
pipe.set("key", "val").incr("num");

pipe.discard();
```

#### Use Replies

`Pipeline::exec` returns a `QueuedReplies` object, which contains replies of all commands that have been send to Redis. You can use `QueuedReplies::get` method to get and parse the ith reply. It has two overloads:

- `template <typename Result> Result get(std::size_t idx)`: Return the ith reply as a return value. You need to specify the return type as tempalte parameter.
- `template <typename Output> void get(std::size_t idx, Output output)`: Return the ith reply as an output iterator. Normally, compiler will deduce the type of the output iterator.

You can check [redis.h](https://github.com/sewenew/redis-plus-plus/blob/master/src/sw/redis%2B%2B/redis.h) for the return type of each command.

```
auto replies = pipe.set("key", "val").incr("num").lrange("list", 0, -1).exec();

auto set_result = replies.get<bool>(0);

auto incr_result = replies.get<long long>(1);

vector<string> list_result;
replies.get(2, back_inserter(list_result));
```

#### Exception

If any of `Pipeline`'s method throws an exception, the `Pipeline` object enters an invalid state. And you CANNOT use it any more, but only destroy the object.

#### Thread Safety

`Pipeline` is **NOT** thread-safe. If you want to call its member functions in multi-thread environment, you need to synchronize between threads manually.

### Transaction

[Transaction](https://redis.io/topics/transactions) is used to make multiple commands runs atomically.

#### Create Transaction

You can create a transaction with `Redis::transaction` method, which returns a `Transaction` object.


```
ConnectionOptions connection_options;
ConnectionPoolOptions pool_options;

Redis redis(connection_options, pool_options);

auto tx = redis.transaction();
```

As the `Pipeline` class, `Transaction` maintains a newly created connection to Redis. This connection has the same `ConnectionOptions` with the `Redis` object.

Also you don't need to send [MULTI](https://redis.io/commands/multi) command to Redis. `Transaction` will do that for you automatically.

#### Send Commands

`Transaction` shares parts of implementation with `Pipeline`. It has the same interfaces with `Pipeline`. You can send commands as what you do with `Pipeline` object.

```
tx.set("key", "val").incr("num").lpush("list", {0, 1, 2});
```

#### Execute Transaction

When you call `Transaction::exec`, you explicitly ask Redis to execute those queued commands, and return the replies. Otherwise, these commands won't be executed. Also, you can call `Transaction::discard` to discard the execution, i.e. no command will be executed. Both `Transaction::exec` and `Transaction::discard` can be chained with other commands.

```
auto replies = tx.set("key", "val").incr("num").exec();

tx.set("key", "val").incr("num");

// Discard the transaction.
tx.discard();
```

#### Use Replies

See [Pipeline](https://github.com/sewenew/redis-plus-plus#use-replies)'s doc for how to use the replies.

#### Piped Transaction

Normally, we always send multiple commnds in a transaction. In order to improve the performance, you can send these commands in a pipeline. You can create a piped transaction by passing `true` as parameter of `Redis::transaction` method.

```
// Create a piped transaction
auto tx = redis.transaction(true);
```

With this piped transaction, all commands are sent to Redis in a pipeline.

#### Exception

If any of `Transaction`'s method throws an exception other than `WatchError`, the `Transaction` object enters an invalid state. And you CANNOT use it any more, but only destroy the object.

#### Thread Safety

`Transacation` is **NOT** thread-safe. If you want to call its member functions in multi-thread environment, you need to synchronize between threads manually.

#### Watch

[WATCH is used to provide a check-and-set(CAS) behavior to Redis transactions](https://redis.io/topics/transactions#optimistic-locking-using-check-and-set).

The **WATCH** command must be sent in the same connection as the transaction. And normally after the **WATCH** command, we also need to send some other commands to get data from Redis before executing the transaction. Take the following check-and-set case as an example:

```
WATCH key           // watch a key
val = GET key       // get value of the key
new_val = val + 1   // incr the value
MULTI               // begin the transaction
SET key new_val         // set value only if the value is NOT modified by others
EXEC                // try to execute the transaction.
                    // if val has been modified, the transaction won't be executed.
```

However, with `Transaction` object, you CANNOT get the result of commands until the whole transaction has been finished. Instead, you need to create a `Redis` object from the `Transaction` object. The created `Redis` object shares the connection with `Transaction` object. With this created `Redis` object, you can send **WATCH** command and any other Redis commands to Redis server, and get the result immediately.

Let's see how to implement the above example with *redis-plus-plus*:

```
auto redis = Redis("tcp://127.0.0.1");

// Create a transaction.
auto tx = redis.transaction();

// Create a Redis object from the Transaction object, which share a single connection.
auto r = tx.redis();

// If the watched key has been modified by other clients, the transaction might fail.
// So we need to retry the transaction in a loop.
while (true) {
    try {
        // Watch a key.
        r.watch("key");

        // Get the old value.
        auto val = r.get("key");
        auto num = 0;
        if (val) {
            num = std::stoi(*val);
        } // else use default value, i.e. 0.

        // Incr value.
        ++num;

        // Execute the transaction.
        auto replies = tx.set("key", std::to_string(num)).exec();

        // Transaction has been executed successfully. Check the result and break.

        assert(replies.size() == 1 && replies.get<bool>(0) == true);

        break;
    } catch (const WatchError &err) {
        // Key has been modified by other clients, retry.
        continue;
    } catch (const Error &err) {
        // Something bad happens, and the Transaction object is no longer valid.
        throw;
    }
}
```

### Redis Cluster

*redis-plus-plus* supports [Redis Cluster](https://redis.io/topics/cluster-tutorial). You can use `RedisCluster` class to send commands to Redis Cluster. It has similar interfaces as `Redis` class.

#### Connection

`RedisCluster` connects to all master nodes in the cluster. For each master node, it maintains a connection pool. By now, it doesn't connect to slave nodes.

You can initialize a `RedisCluster` instance with `ConnectionOptions` and `ConnectionPoolOptions`. You only need to set a master node's host & port in `ConnectionOptions`, and `RedisCluster` will get other nodes' info automatically (with the CLUSTER SLOTS command). For each master node, it creates a connection pool with the specified `ConnectionPoolOptions`. If `ConnectionPoolOptions` is not specified, `RedisCluster` maintains a single connection to every master node.

```
// Set a master node's host & port.
ConnectionOptions connection_options;
connection_options.host = "127.0.0.1";  // Required.
connection_options.port = 7000; // Optional. The default port is 6379.
connection_options.password = "auth"; // Optional. No password by default.

// Automatically get other nodes' info,
// and connect to each master node with a single connection.
RedisCluster cluster1(connection_options);

ConnectionPoolOptions pool_options;
pool_options.size = 3;

// For each master node, maintains a connection pool of size 3.
RedisCluster cluster2(connection_options, pool_options);
```

You can also specify connection option with an URI. However, in this way, you can only use default `ConnectionPoolOptions`, i.e. pool of size 1, and CANNOT specify password.

```
// Specify a master node's host & port.
RedisCluster cluster3("tcp://127.0.0.1:7000");

// Use default port, i.e. 6379.
RedisCluster cluster4("tcp://127.0.0.1");
```

##### Others

- `RedisCluster` only works with tcp connection. It CANNOT connect to Unix Domain Socket. If you specify Unix Domain Socket in `ConnectionOptions`, it throws an exception.
- All nodes in the cluster should have the same password.
- Since [Redis Cluster does NOT support multiple databses](https://redis.io/topics/cluster-spec#implemented-subset), `ConnectionOptions::db` is ignored.

#### Interfaces

As we mentioned, `RedisCluster`'s interfaces are similar to `Redis`. It supports most of `Redis`' intefaces (see `Redis`' [API Reference](https://github.com/sewenew/redis-plus-plus#api-reference) for details), except the following:

- Not support commands without key as argument, e.g. [PING](https://redis.io/commands/ping), [INFO](https://redis.io/commands/info).
- Not support Lua script without key parameters.

`RedisCluster` does NOT support these interfaces, because it has no idea to which node these commands should be sent.

Also you can use the [hash tags](https://redis.io/topics/cluster-spec#keys-hash-tags) to send multiple-key commands.

##### Pipeline and Transaction

`RedisCluster` also supports pipeline and transaction, but the interfaces are different from `Redis`. Since all commands in the pipeline and transaction should be sent to a single node in a single connection, we need to tell `RedisCluster` with which node the pipeline or transaction should be created.

Instead of specifing the node's IP and port, `RedisCluster`'s pipeline and transaction interfaces allow you to specify the node with a *hash tag*. `RedisCluster` will calculate the slot number with the given *hash tag*, and create a pipeline or transaction with the node holding the slot.

```
Pipeline RedisCluster::pipeline(const StringView &hash_tag);

Transaction RedisCluster::transaction(const StringView &hash_tag, bool piped = false);
```

With the created pipeline or transaction, you can send commands with keys located on the same node as the given *hash_tag*. See [Examples](https://github.com/sewenew/redis-plus-plus#examples-2) part for an example.

#### Examples

```
#include <sw/redis++/redis++.h>

using namespace sw::redis;

auto redis_cluster = RedisCluster("tcp://127.0.0.1:7000");

redis_cluster.set("key", "value");
auto val = redis_cluster.get("key");
if (val) {
    std::cout << *val << std::endl;
}

// With hash-tag.
redis_cluster.set("key{tag}1", "val1");
redis_cluster.set("key{tag}2", "val2");
redis_cluster.set("key{tag}3", "val3");
std::vector<OptionalString> hash_tag_res;
redis_cluster.mget({"key{tag}1", "key{tag}2", "key{tag}3"},
        std::back_inserter(hash_tag_res));

redis_cluster.lpush("list", {"1", "2", "3"});
std::vector<std::string> list;
redis_cluster.lrange("list", 0, -1, std::back_inserter(list));

// Pipline.
auto pipe = redis_cluster.pipeline("counter");
auto replies = pipe.incr("{counter}:1").incr("{counter}:2").exec();

// Transaction.
auto tx = redis_cluster.transaction("key");
replies = tx.incr("key").get("key").exec();
```

#### Details

`RedisCluster` maintains the newest slot-node mapping, and sends command directly to the right node. Normally it works as fast as `Redis`. If the cluster reshards, `RedisCluster` will follow the redirection, and it will finally update the slot-node mapping. It can correctly handle the following resharding cases:

- Data migration between exist nodes.
- Add new node to the cluster.
- Remove node from the cluster.

`redis-plus-plus` is able to handle both [MOVED](https://redis.io/topics/cluster-spec#moved-redirection) and [ASK](https://redis.io/topics/cluster-spec#ask-redirection) redirections, so it's a complete Redis Cluster client.

If master is down, the cluster will promote one of its replicas to be the new master. *redis-plus-plus* can also handle this case:

- When the master is down, *redis-plus-plus* losts connection to it. In this case, if you try to send commands to this master, *redis-plus-plus* will try to update slot-node mapping from other nodes. If the mapping remains unchanged, i.e. new master hasn't been elected yet, it fails to send command to Redis Cluster and throws exception.
- When the new master has been elected, the slot-node mapping will be updated by the cluster. In this case, if you send commands to the cluster, *redis-plus-plus* can get an update-to-date mapping, and sends commands to the new master.

## Author

*redis-plus-plus* is written by sewenew, who is also active on [StackOverflow](https://stackoverflow.com/users/5384363/for-stack).
