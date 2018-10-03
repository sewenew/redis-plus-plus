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

#include "threads_test.h"
#include <thread>
#include <chrono>
#include <atomic>
#include "utils.h"

namespace sw {

namespace redis {

namespace test {

ThreadsTest::ThreadsTest(const ConnectionOptions &opts) : _opts(opts) {}

void ThreadsTest::run() {
    // 100 * 10000 = 1 million writes
    auto thread_num = 100;
    auto times = 10000;

    // Default pool options: single connection and wait forever.
    _test_multithreads(Redis(_opts), thread_num, times);

    // Pool with 10 connections.
    ConnectionPoolOptions pool_opts;
    pool_opts.size = 10;
    _test_multithreads(Redis(_opts, pool_opts), thread_num, times);

    _test_timeout();
}

void ThreadsTest::_test_multithreads(Redis redis, int thread_num, int times) {
    std::vector<std::string> keys;
    keys.reserve(thread_num);
    for (auto idx = 0; idx != thread_num; ++idx) {
        auto key = test_key("multi-threads::" + std::to_string(idx));
        keys.push_back(key);
    }

    KeyDeleter deleter(redis, keys.begin(), keys.end());

    std::vector<std::thread> workers;
    workers.reserve(thread_num);
    for (const auto &key : keys) {
        workers.emplace_back([&redis, key, times]() {
                                try {
                                    for (auto i = 0; i != times; ++i) {
                                        redis.incr(key);
                                    }
                                } catch (...) {
                                    // Something bad happens.
                                    return;
                                }
                            });
    }

    for (auto &worker : workers) {
        worker.join();
    }

    for (const auto &key : keys) {
        auto val = redis.get(key);
        REDIS_ASSERT(bool(val), "failed to test multithreads, cannot get value of " + key);

        auto num = std::stoi(*val);
        REDIS_ASSERT(num == times, "failed to test multithreads, num: "
                + *val + ", times: " + std::to_string(times));
    }
}

void ThreadsTest::_test_timeout() {
    using namespace std::chrono;

    ConnectionPoolOptions pool_opts;
    pool_opts.size = 1;
    pool_opts.wait_timeout = milliseconds(100);

    auto redis = Redis(_opts, pool_opts);

    std::atomic<bool> slow_ping_is_running{false};
    auto slow_ping = [&slow_ping_is_running](Connection &connection) {
                        slow_ping_is_running = true;

                        // Sleep a while to simulate a slow ping.
                        std::this_thread::sleep_for(seconds(5));
                        connection.send("PING");
                    };
    auto slow_ping_thread = std::thread([&redis, slow_ping]() {
                                            redis.command(slow_ping);
                                        });

    auto ping_thread = std::thread([&redis, &slow_ping_is_running]() {
                                        try {
                                            while (!slow_ping_is_running) {
                                                std::this_thread::sleep_for(milliseconds(10));
                                            }

                                            redis.ping();

                                            // Slow ping is running, this thread should
                                            // timeout before obtaining the connection.
                                            // So it never reaches here.
                                            REDIS_ASSERT(false, "failed to test pool timeout");
                                        } catch (const Error &err) {
                                            // This thread timeout.
                                        }
                                    });

    slow_ping_thread.join();
    ping_thread.join();
}

}

}

}
