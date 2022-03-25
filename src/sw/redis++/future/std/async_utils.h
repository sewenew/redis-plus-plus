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

#include <future>

namespace sw {

namespace redis {

template <typename T>
using Future = std::future<T>;

template <typename T>
using Promise = std::promise<T>;

} // namespace redis

} // namespace sw

#ifdef __cpp_impl_coroutine

#include <coroutine>
namespace sw {

namespace redis {

class Executor {
public:
    Executor() = default;
    virtual ~Executor() = default;

    virtual void enqueue(std::function<void()>) = 0;
};

template <typename T>
struct FutureAwaiter {
    FutureAwaiter(std::future<T>&& future, Executor* executor_ptr)
        : _future(std::move(future))
        , _executor_ptr(executor_ptr) {
    }

    bool await_ready() { return false; }
    void await_suspend(std::coroutine_handle<> handle) {
        _executor_ptr->enqueue([this, handle] {
            try {
                _result = _future.get();
            } catch (const std::exception&) {
                _exception = std::current_exception();
            }
            handle.resume();
        });
    }

    T await_resume() {
        if (_exception)
            std::rethrow_exception(_exception);
        return std::move(_result);
    }

    std::future<T> _future;
    Executor* _executor_ptr{nullptr};
    std::decay_t<decltype(std::declval<std::future<T>>().get())> _result;
    std::exception_ptr _exception{nullptr};
};

template <>
struct FutureAwaiter<void> {
    FutureAwaiter(std::future<void>&& future, Executor* executor_ptr)
        : _future(std::move(future))
        , _executor_ptr(executor_ptr) {
    }

    bool await_ready() { return false; }
    void await_suspend(std::coroutine_handle<> handle) {
        _executor_ptr->enqueue([this, handle] {
            try {
                _future.wait();
            } catch (const std::exception&) {
                _exception = std::current_exception();
            }
            handle.resume();
        });
    }

    void await_resume() {
        if (_exception)
            std::rethrow_exception(_exception);
        return;
    }

    std::future<void> _future;
    Executor* _executor_ptr{nullptr};
    std::exception_ptr _exception{nullptr};
};

} // namespace redis

} // namespace sw

#endif

#endif // end SEWENEW_REDISPLUSPLUS_ASYNC_UTILS_H
