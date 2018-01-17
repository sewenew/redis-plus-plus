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

class OptionalString {
public:
    OptionalString() = default;

    OptionalString(const OptionalString &) = default;
    OptionalString& operator=(const OptionalString &) = default;

    OptionalString(OptionalString &&) = default;
    OptionalString& operator=(OptionalString &&) = default;

    ~OptionalString() = default;

    explicit OptionalString(std::string s) : _value(true, std::move(s)) {}

    explicit operator bool() const {
        return _value.first;
    }

    std::string& value() {
        return _value.second;
    }

    const std::string& value() const {
        return _value.second;
    }

    std::string* operator->() {
        return &(_value.second);
    }

    const std::string* operator->() const {
        return &(_value.second);
    }

    std::string& operator*() {
        return _value.second;
    }

    const std::string& operator*() const {
        return _value.second;
    }

private:
    std::pair<bool, std::string> _value;
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

}

}

#endif // end SEWENEW_REDISPLUSPLUS_UTILS_H
