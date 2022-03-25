/**************************************************************************
   Copyright (c) 2021 sewenew

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

#ifndef SEWENEW_REDISPLUSPLUS_ASYNC_UTILS_H
#define SEWENEW_REDISPLUSPLUS_ASYNC_UTILS_H

#ifdef __cpp_impl_coroutine

#define BOOST_THREAD_PROVIDES_EXECUTORS
#define BOOST_THREAD_USES_MOVE

#endif

#define BOOST_THREAD_PROVIDES_FUTURE
#define BOOST_THREAD_PROVIDES_FUTURE_CONTINUATION

#include <boost/thread/future.hpp>

namespace sw {

namespace redis {

template <typename T>
using Future = boost::future<T>;

template <typename T>
using Promise = boost::promise<T>;

} // namespace redis

} // namespace sw

#ifdef __cpp_impl_coroutine

#include <coroutine>

#include <boost/thread/executors/basic_thread_pool.hpp>

namespace sw {

namespace redis {

template <typename T>
struct FutureAwaiter {
    FutureAwaiter(boost::future<T>&& future)
        : _future(std::move(future)) {
    }

    FutureAwaiter(boost::future<T>&& future, boost::executors::basic_thread_pool* pool_ptr)
        : _future(std::move(future))
        , _pool_ptr(pool_ptr) {
    }

    bool await_ready() { return false; }
    void await_suspend(std::coroutine_handle<> handle) {
        auto func = [this, handle](sw::redis::Future<T> fut) mutable {
            try {
                _result = fut.get();
            } catch (...) {
                try {
                    boost::rethrow_exception(fut.get_exception_ptr());
                } catch (const std::exception_ptr& ep) {
                    _exception = ep;
                }
            }
            handle.resume();
        };
        if (_pool_ptr != nullptr)
            _future.then(*_pool_ptr, std::move(func));
        else
            _future.then(std::move(func));
    }

    T await_resume() {
        if (_exception)
            std::rethrow_exception(_exception);
        return std::move(_result);
    }

    boost::future<T> _future;
    boost::executors::basic_thread_pool* _pool_ptr{nullptr};
    std::decay_t<decltype(std::declval<boost::future<T>>().get())> _result;
    std::exception_ptr _exception{nullptr};
};

template <>
struct FutureAwaiter<void> {
    FutureAwaiter(boost::future<void>&& future)
        : _future(std::move(future)) {
    }

    FutureAwaiter(boost::future<void>&& future, boost::executors::basic_thread_pool* pool_ptr)
        : _future(std::move(future))
        , _pool_ptr(pool_ptr) {
    }

    bool await_ready() { return false; }
    void await_suspend(std::coroutine_handle<> handle) {
        auto func = [this, handle](sw::redis::Future<void> fut) mutable {
            try {
                fut.wait();
            } catch (...) {
                try {
                    boost::rethrow_exception(fut.get_exception_ptr());
                } catch (const std::exception_ptr& ep) {
                    _exception = ep;
                }
            }
            handle.resume();
        };
        if (_pool_ptr != nullptr)
            _future.then(*_pool_ptr, std::move(func));
        else
            _future.then(std::move(func));
    }

    void await_resume() {
        if (_exception)
            std::rethrow_exception(_exception);
        return;
    }

    boost::future<void> _future;
    boost::executors::basic_thread_pool* _pool_ptr{nullptr};
    std::exception_ptr _exception{nullptr};
};

} // namespace redis

} // namespace sw1

#endif

#endif // end SEWENEW_REDISPLUSPLUS_ASYNC_UTILS_H
