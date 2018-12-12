/**************************************************************************
   Copyright (c) 2018 sewenew

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

#ifndef IS_COMMAND_FUNCTOR_H
#define IS_COMMAND_FUNCTOR_H

#include <type_traits>
#include "connection.h"

namespace sw {

namespace redis {
namespace detail{
    /**
     * return type for good candidates
     */
    template<class...> struct CanBeCalled : std::true_type{
    };

    /**
     * true_type, if Callable can be called with the given parameter pack expanded.
     */
    template<class Callable, class ... Args>
    auto testCallConnection(int)
       ->CanBeCalled<decltype(std::declval<Callable>()(std::declval<Connection&>(),
                                                    std::declval<typename std::add_rvalue_reference<Args>::type>()...))>;

    template<class NotCallable, class ...>
    auto testCallConnection(long)->std::false_type;
}

template<class T, class ... TrailingArgs>
struct CanBeCalledWithConnectionAndTrailingArgs : decltype(detail::testCallConnection<T, TrailingArgs ...>(0)){
};

/**
 * A command functor is something that can be called with a Connection& as first parameter and a given list of trailing arguments.
 * Compile time flag to check whether an instance of the given type T can be used as command functor.
 */
template<class T, class ... TrailingArgs>
constexpr bool isCommandFunctor_v = CanBeCalledWithConnectionAndTrailingArgs<T, TrailingArgs...>::value;
}
}

#endif
