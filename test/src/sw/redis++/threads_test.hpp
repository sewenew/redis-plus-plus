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

#ifndef SEWENEW_REDISPLUSPLUS_TEST_THREADS_TEST_HPP
#define SEWENEW_REDISPLUSPLUS_TEST_THREADS_TEST_HPP

#include <thread>
#include <chrono>
#include <atomic>
#include <memory>
#include "utils.h"

namespace sw {

namespace redis {

namespace test {

template <typename RedisInstance>
void ThreadsTest<RedisInstance>::run() {
    {
        auto thread_num = 2;
        auto times = 10000;
    
        // Pool with 2 connections, stream read and write
        ConnectionPoolOptions pool_opts;
        pool_opts.size = 2;
        _test_multithreads_stream_read_and_write(RedisInstance(_opts, pool_opts), thread_num, times);
    }

    // 100 * 10000 = 1 million writes
    auto thread_num = 100;
    auto times = 10000;

    // Default pool options: single connection and wait forever.
    _test_multithreads(RedisInstance(_opts), thread_num, times);

    // Pool with 10 connections.
    ConnectionPoolOptions pool_opts;
    pool_opts.size = 10;
    _test_multithreads(RedisInstance(_opts, pool_opts), thread_num, times);

    _test_timeout();
}

template <typename RedisInstance>
void ThreadsTest<RedisInstance>::_test_multithreads_stream_read_and_write(RedisInstance redis,
        int thread_num,
        int times) {

    std::cout << "Started _test_multithreads_stream_read_and_write" << std::endl;
            
    auto key = test_key("key-name");
    
    using Attrs = std::vector<std::pair<std::string, std::string>>;
    Attrs attrs = { {"f1", "v1"} };
    using Item = std::pair<std::string, Attrs>;
    using ItemStream = std::vector<Item>;
    std::unordered_map<std::string, ItemStream> result;

    std::vector<std::thread> workers;
    redis.del(key);
    // Even threads are writers, odd threads are readers
    workers.reserve(thread_num);
    for( int t = 0; t < thread_num; ++t ) {
        if( t%2 == 0 ) {
            workers.emplace_back([&redis, attrs, key, times]() {
                try {
                    std::cout << "Starting xadd..." << std::endl;
                    for (auto i = 0; i != times; ++i) {
                        auto id = redis.xadd(key, "*", attrs.begin(), attrs.end());
                    }
                    std::cout << "Finished xadd..." << std::endl;
                } catch (...) {
                    // Something bad happens.
                    return;
                }
            });
        } else {
            workers.emplace_back([&redis, &result, attrs, key, times]() {
                try {
                    std::string id = "0";
                    int n_read = 0;
                    int last_result_size = 0;
                    //const int n_shown = 10;
                    std::cout << "Starting xread..." << std::endl;
                    while( n_read < times ) {
                        redis.xread(key, id, std::chrono::seconds(1), times, std::inserter(result, result.end()));
                        if( !result.empty() ) {
                            const auto end_count = result.begin()->second.size();
                            const int new_count = end_count - last_result_size;
                            if( new_count > 0 ) {
                                for( int i = 0; i < new_count; ++i ) {
                                    id = result.begin()->second[last_result_size+i].first;
                                    //if( n_read%(times/n_shown) == 0 ) std::cout << "n_read: " << n_read << std::endl;
                                    std::cout << "n_read: " << n_read << std::endl;
                                    n_read++;
                                }
                                last_result_size = result.begin()->second.size();
                            }
                        }
                    }
                    std::cout << "Finished xread..." << std::endl;
                } catch (...) {
                    // Something bad happens.
                    return;
                }
           });
        }
    }

    for (auto &worker : workers) {
        worker.join();
    }

    // Code never reaches this point due to bug in to_array() code.
}


template <typename RedisInstance>
void ThreadsTest<RedisInstance>::_test_multithreads(RedisInstance redis,
        int thread_num,
        int times) {
    std::vector<std::string> keys;
    keys.reserve(thread_num);
    for (auto idx = 0; idx != thread_num; ++idx) {
        auto key = test_key("multi-threads::" + std::to_string(idx));
        keys.push_back(key);
    }

    using DeleterUPtr = std::unique_ptr<KeyDeleter<RedisInstance>>;
    std::vector<DeleterUPtr> deleters;
    for (const auto &key : keys) {
        deleters.emplace_back(new KeyDeleter<RedisInstance>(redis, key));
    }

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

template <typename RedisInstance>
void ThreadsTest<RedisInstance>::_test_timeout() {
    using namespace std::chrono;

    ConnectionPoolOptions pool_opts;
    pool_opts.size = 1;
    pool_opts.wait_timeout = milliseconds(100);

    auto redis = RedisInstance(_opts, pool_opts);

    auto key = test_key("key");

    std::atomic<bool> slow_get_is_running{false};
    auto slow_get = [&slow_get_is_running](Connection &connection, const StringView &key) {
                        slow_get_is_running = true;

                        // Sleep a while to simulate a slow get.
                        std::this_thread::sleep_for(seconds(5));

                        connection.send("GET %b", key.data(), key.size());
                    };
    auto slow_get_thread = std::thread([&redis, slow_get, &key]() {
                                            redis.command(slow_get, key);
                                        });

    auto get_thread = std::thread([&redis, &slow_get_is_running, &key]() {
                                        try {
                                            while (!slow_get_is_running) {
                                                std::this_thread::sleep_for(milliseconds(10));
                                            }

                                            redis.get(key);

                                            // Slow get is running, this thread should
                                            // timeout before obtaining the connection.
                                            // So it never reaches here.
                                            REDIS_ASSERT(false, "failed to test pool timeout");
                                        } catch (const Error &err) {
                                            // This thread timeout.
                                        }
                                    });

    slow_get_thread.join();
    get_thread.join();
}

}

}

}

#endif // end SEWENEW_REDISPLUSPLUS_TEST_THREADS_TEST_HPP
