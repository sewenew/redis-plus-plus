/**************************************************************************
   Copyright (c) 2020 sewenew

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

#include "sw/redis++/tls.h"
#include <cstring>
#include "sw/redis++/errors.h"

namespace sw {

namespace redis {

namespace tls {

bool& auto_init() {
    static bool init = true;

    return init;
}

void disable_auto_init() {
    auto_init() = false;
}

TlsInit::TlsInit() {
    if (auto_init()) {
        redisInitOpenSSL();
    }
}

TlsContextUPtr secure_connection(redisContext &ctx, const TlsOptions &opts) {
    static TlsInit tls_init;

    auto c_str = [](const std::string &s) {
        return s.empty() ? nullptr : s.c_str();
    };

    redisSSLContextError err;
#ifdef REDIS_PLUS_PLUS_TLS_VERIFY_MODE
    redisSSLOptions redis_ssl_opts;
    std::memset(&redis_ssl_opts, 0, sizeof(redis_ssl_opts));
    redis_ssl_opts.cacert_filename = c_str(opts.cacert);
    redis_ssl_opts.capath = c_str(opts.cacertdir);
    redis_ssl_opts.cert_filename = c_str(opts.cert);
    redis_ssl_opts.private_key_filename = c_str(opts.key);
    redis_ssl_opts.server_name = c_str(opts.sni);
    redis_ssl_opts.verify_mode = opts.verify_mode;

    auto tls_ctx = TlsContextUPtr(redisCreateSSLContextWithOptions(&redis_ssl_opts, &err));
#else
    auto tls_ctx = TlsContextUPtr(redisCreateSSLContext(c_str(opts.cacert),
                                                        c_str(opts.cacertdir),
                                                        c_str(opts.cert),
                                                        c_str(opts.key),
                                                        c_str(opts.sni),
                                                        &err));
#endif // end REDIS_PLUS_PLUS_TLS_VERIFY_MODE

    if (!tls_ctx) {
        throw Error(std::string("failed to create TLS context: ")
                    + redisSSLContextGetError(err));
    }

    if (redisInitiateSSLWithContext(&ctx, tls_ctx.get()) != REDIS_OK) {
        throw_error(ctx, "Failed to initialize TLS connection");
    }

    return tls_ctx;
}

}

}

}
