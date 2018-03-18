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

#ifndef SEWENEW_REDISPLUSPLUS_UTILS_H
#define SEWENEW_REDISPLUSPLUS_UTILS_H

#include <cstring>
#include <string>

namespace sw {

namespace redis {

// By now, not all compilers support std::string_view,
// so we make our own implementation.
class StringView {
public:
    constexpr StringView() noexcept = default;

    constexpr StringView(const char *data, std::size_t size) : _data(data), _size(size) {}

    StringView(const char *data) : _data(data), _size(std::strlen(data)) {}

    StringView(const std::string &str) : _data(str.data()), _size(str.size()) {}

    constexpr StringView(const StringView &) noexcept = default;

    StringView& operator=(const StringView &) noexcept = default;

    constexpr const char* data() const noexcept {
        return _data;
    }

    constexpr std::size_t size() const noexcept {
        return _size;
    }

private:
    const char *_data = nullptr;
    std::size_t _size = 0;
};

template <typename T>
class Optional {
public:
    Optional() = default;

    Optional(const Optional &) = default;
    Optional& operator=(const Optional &) = default;

    Optional(Optional &&) = default;
    Optional& operator=(Optional &&) = default;

    ~Optional() = default;

    template <typename ...Args>
    explicit Optional(Args &&...args) : _value(true, T(std::forward<Args>(args)...)) {}

    explicit operator bool() const {
        return _value.first;
    }

    T& value() {
        return _value.second;
    }

    const T& value() const {
        return _value.second;
    }

    T* operator->() {
        return &(_value.second);
    }

    const T* operator->() const {
        return &(_value.second);
    }

    T& operator*() {
        return _value.second;
    }

    const T& operator*() const {
        return _value.second;
    }

private:
    std::pair<bool, T> _value;
};

using OptionalString = Optional<std::string>;

using OptionalLongLong = Optional<long long>;

using OptionalDouble = Optional<double>;

enum class BoundType {
    CLOSED,
    OPEN,
    LEFT_OPEN,
    RIGHT_OPEN
};

// (-inf, +inf)
template <typename T>
class UnboundedInterval;

// [min, max], (min, max), (min, max], [min, max)
template <typename T>
class BoundedInterval;

// [min, +inf), (min, +inf)
template <typename T>
class LeftBoundedInterval;

// (-inf, max], (-inf, max)
template <typename T>
class RightBoundedInterval;

template <>
class UnboundedInterval<double> {
public:
    const std::string& min() const;

    const std::string& max() const;
};

template <>
class BoundedInterval<double> {
public:
    BoundedInterval(double min, double max, BoundType type);

    const std::string& min() const {
        return _min;
    }

    const std::string& max() const {
        return _max;
    }

private:
    std::string _min;
    std::string _max;
};

template <>
class LeftBoundedInterval<double> {
public:
    LeftBoundedInterval(double min, BoundType type);

    const std::string& min() const {
        return _min;
    }

    const std::string& max() const;

private:
    std::string _min;
};

template <>
class RightBoundedInterval<double> {
public:
    RightBoundedInterval(double max, BoundType type);

    const std::string& min() const;

    const std::string& max() const {
        return _max;
    }

private:
    std::string _max;
};

template <>
class UnboundedInterval<std::string> {
public:
    const std::string& min() const;

    const std::string& max() const;
};

template <>
class BoundedInterval<std::string> {
public:
    BoundedInterval(const std::string &min, const std::string &max, BoundType type);

    const std::string& min() const {
        return _min;
    }

    const std::string& max() const {
        return _max;
    }

private:
    std::string _min;
    std::string _max;
};

template <>
class LeftBoundedInterval<std::string> {
public:
    LeftBoundedInterval(const std::string &min, BoundType type);

    const std::string& min() const {
        return _min;
    }

    const std::string& max() const;

private:
    std::string _min;
};

template <>
class RightBoundedInterval<std::string> {
public:
    RightBoundedInterval(const std::string &max, BoundType type);

    const std::string& min() const;

    const std::string& max() const {
        return _max;
    }

private:
    std::string _max;
};

struct LimitOptions {
    long long offset = 0;
    long long count = -1;
};

template <typename ...>
struct IsKvPair : std::false_type {};

template <typename T, typename U>
struct IsKvPair<std::pair<T, U>> : std::true_type {};

template <typename ...>
using Void = void;

template <typename T, typename U = Void<>>
struct IsInserter : std::false_type {};

template <typename T>
struct IsInserter<T, Void<typename T::container_type>> : std::true_type {};

template <typename T, typename U = Void<>>
struct IsKvPairIter : IsKvPair<typename T::value_type> {};

template <typename T>
struct IsKvPairIter<T, Void<typename T::container_type>> :
                    IsKvPair<typename T::container_type::value_type> {};

}

}

#endif // end SEWENEW_REDISPLUSPLUS_UTILS_H
