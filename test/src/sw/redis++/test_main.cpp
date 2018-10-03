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

#include <unistd.h>
#include <chrono>
#include <iostream>
#include <sw/redis++/redis++.h>
#include "sanity_test.h"
#include "connection_cmds_test.h"
#include "keys_cmds_test.h"
#include "string_cmds_test.h"
#include "list_cmds_test.h"
#include "hash_cmds_test.h"
#include "set_cmds_test.h"
#include "zset_cmds_test.h"
#include "hyperloglog_cmds_test.h"
#include "geo_cmds_test.h"
#include "script_cmds_test.h"
#include "pubsub_test.h"
#include "pipeline_transaction_test.h"
#include "threads_test.h"

namespace {

void print_help();

sw::redis::ConnectionOptions parse_options(int argc, char **argv);

}

int main(int argc, char **argv) {
    try {
        auto opts = parse_options(argc, argv);

        sw::redis::test::SanityTest sanity_test(opts);
        sanity_test.run();

        std::cout << "Pass sanity tests" << std::endl;

        sw::redis::test::ConnectionCmdTest connection_test(opts);
        connection_test.run();

        std::cout << "Pass connection commands tests" << std::endl;

        sw::redis::test::KeysCmdTest keys_test(opts);
        keys_test.run();

        std::cout << "Pass keys commands tests" << std::endl;

        sw::redis::test::StringCmdTest string_test(opts);
        string_test.run();

        std::cout << "Pass string commands tests" << std::endl;

        sw::redis::test::ListCmdTest list_test(opts);
        list_test.run();

        std::cout << "Pass list commands tests" << std::endl;

        sw::redis::test::HashCmdTest hash_test(opts);
        hash_test.run();

        std::cout << "Pass hash commands tests" << std::endl;

        sw::redis::test::SetCmdTest set_test(opts);
        set_test.run();

        std::cout << "Pass set commands tests" << std::endl;

        sw::redis::test::ZSetCmdTest zset_test(opts);
        zset_test.run();

        std::cout << "Pass zset commands tests" << std::endl;

        sw::redis::test::HyperloglogCmdTest hll_test(opts);
        hll_test.run();

        std::cout << "Pass hyperloglog commands tests" << std::endl;

        sw::redis::test::GeoCmdTest geo_test(opts);
        geo_test.run();

        std::cout << "Pass geo commands tests" << std::endl;

        sw::redis::test::ScriptCmdTest script_test(opts);
        script_test.run();

        std::cout << "Pass script commands tests" << std::endl;

        sw::redis::test::PubSubTest pubsub_test(opts);
        pubsub_test.run();

        std::cout << "Pass pubsub tests" << std::endl;

        sw::redis::test::PipelineTransactionTest pipe_tx_test(opts);
        pipe_tx_test.run();

        std::cout << "Pass pipeline and transaction tests" << std::endl;

        sw::redis::test::ThreadsTest threads_test(opts);
        threads_test.run();

        std::cout << "Pass threads tests" << std::endl;

        std::cout << "Pass all tests" << std::endl;
    } catch (const sw::redis::Error &e) {
        std::cerr << "Test failed: " << e.what() << std::endl;
        return -1;
    }

    return 0;
}

namespace {

void print_help() {
    std::cerr << "Usage: test_redis++ -h host -p port" << std::endl;
}

sw::redis::ConnectionOptions parse_options(int argc, char **argv) {
    std::string host;
    int port = 0;
    std::string auth;

    int opt = 0;
    while ((opt = getopt(argc, argv, "h:p:a:")) != -1) {
        switch (opt) {
        case 'h':
            host = optarg;
            break;

        case 'p':
            port = std::stoi(optarg);
            break;

        case 'a':
            auth = optarg;
            break;

        default:
            print_help();
            throw sw::redis::Error("Failed to parse connection options");
            break;
        }
    }

    if (host.empty() || port <= 0) {
        print_help();
        throw sw::redis::Error("Invalid connection options");
    }

    sw::redis::ConnectionOptions opts;
    opts.host = host;
    opts.port = port;
    opts.password = auth;

    return opts;
}

}
