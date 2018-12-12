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
#include <sw/redis++/is_command_functor.h>
#include <vector>

using namespace sw::redis;
namespace {
    void connectionFunction(Connection&){}
    
    struct IsCommandFunctorTest {
        static void testStringNoTrailingArgs() {
            static_assert(!isCommandFunctor_v<std::string>, "a string is not a command functor");
        }
    
        static void testStringIntTrailingArg() {
            static_assert(!isCommandFunctor_v<std::string, int>, "a string is not a command functor");
        }
        
        static void testIsConnectionFunction(){
            static_assert(isCommandFunctor_v<decltype(connectionFunction)>, "connectionFunction is a functor");
        }
        
        static void testLambdaNoTrailingArgs() {
            StringView cmdString = "CLIENT GETNAME";
            auto lambdaCmd = [&cmdString](Connection &){};
            static_assert(isCommandFunctor_v<decltype(lambdaCmd)>,"no trailing arg lambda is a functor");
        }
        
        static void testLambdaWithTrailingArgs() {
            StringView cmdString = "CLIENT SETNAME %s";
            auto lambdaCmd = [&cmdString](Connection &, char const*){};
            static_assert(isCommandFunctor_v<decltype(lambdaCmd),char const*>,"trailing arg lambda is a functor");
        }

        template<class... TrailingArgs>
        static void testLambdaWithTrailingArgs(TrailingArgs&&...) {
            auto lambda = [](Connection&,TrailingArgs&&...){};
            static_assert(isCommandFunctor_v<decltype(lambda),TrailingArgs...>,"trailing arg lambda is a functor");
        }
        
        /**
         * test that rvalue references are supported. Note that currently the Redis::command(StringView const&, ...) 
         * needs to support rvalue references
         */
        static void testLambdaWithTrailingRValueArgs(){
            testLambdaWithTrailingArgs(int());
        }
        
        static void testLambdaWithTrailingLValueArg() {
            std::vector<int> lvalue;
            auto lambda = [](Connection &, std::vector<int>& v){v.push_back(23);};
            static_assert(isCommandFunctor_v<decltype(lambda),std::vector<int>&>,"trailing arg lambda is a functor");
        }
    };
}
