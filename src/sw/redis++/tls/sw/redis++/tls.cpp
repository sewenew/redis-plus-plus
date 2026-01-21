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
#include <openssl/ssl.h>

namespace sw {

namespace redis {

namespace tls {

bool& auto_init() {
    //static bool init = true;
    //For product, main function own to do the init
    static bool init = false;

    return init;
}

void enable_auto_init() {
    auto_init() = true;
}

TlsInit::TlsInit() {
    if (auto_init()) {
        redisInitOpenSSL();
    }
}

SSL_CTX * initSSLCTX(const TlsOptions &opts) {
    SSL_CTX *ssl_ctx = SSL_CTX_new(SSLv23_client_method());
    if (!ssl_ctx)
    {
        throw Error("failed to new SSL_CTX");
    }

    const char *ca_file = NULL;
    const char *ca_path = NULL;
    const char *cert_file = NULL;
    const char *cert_key = NULL;

    if (!opts.cacert.empty())
    {
        ca_file = opts.cacert.c_str();
    }

    if (!opts.cacertdir.empty())
    {
        ca_path = opts.cacertdir.c_str();
    }
    
    if (!opts.cert.empty() && !opts.key.empty())
    {
        cert_file = opts.cert.c_str();
        cert_key =opts.key.c_str();
    }

    if (ca_path || ca_file)
    {
        if (!SSL_CTX_load_verify_locations(ssl_ctx, ca_file, ca_path))
        {
            SSL_CTX_free(ssl_ctx);
            throw Error("SSL CTX CA CERT load failed");
        }
    }

    if (cert_file)
    {
        if (!SSL_CTX_use_certificate_chain_file(ssl_ctx, cert_file))
        {
            SSL_CTX_free(ssl_ctx);
            throw Error("SSL CTX CLIENT CERT load failed");
        }
        if (!SSL_CTX_use_PrivateKey_file(ssl_ctx, cert_key, SSL_FILETYPE_PEM))
        {
            SSL_CTX_free(ssl_ctx);
            throw Error("SSL CTX CLIENT PRIVATE KEY load failed");
        }
    }

    if (!opts.tls_protocol.empty() && isEqualIgnoreCase(opts.tls_protocol, "TLS1.3"))
    {
        SSL_CTX_set_options(ssl_ctx, SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3 | SSL_OP_NO_TLSv1 | SSL_OP_NO_TLSv1_1 | SSL_OP_NO_TLSv1_2);
    } else {
        SSL_CTX_set_options(ssl_ctx, SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3 | SSL_OP_NO_TLSv1 | SSL_OP_NO_TLSv1_1);
    }

    if (!opts.ciphers.empty())
    {
        if (!SSL_CTX_set_cipher_list(ssl_ctx, opts.ciphers.c_str()))
        {
            SSL_CTX_free(ssl_ctx);
            throw Error("Failed to set TLS ciphers");       
        }
    }

#ifdef REDIS_PLUS_PLUS_TLS_VERIFY_MODE
    // Not support SAN checking
    SSL_CTX_set_verify(ssl_ctx, opts.verify_mode, NULL);
#else
    SSL_CTX_set_verify(ssl_ctx, SSL_VERIFY_PEER, NULL);
#endif // end REDIS_PLUS_PLUS_TLS_VERIFY_MODE
    return ssl_ctx;
}

SSL * initSSL(SSL_CTX *ssl_ctx, const TlsOptions &opts) {
    if (!ssl_ctx) {
        throw Error("Invalid SSL context");
    }

    SSL *ssl = SSL_new(ssl_ctx);
    if (!ssl)
    {
        SSL_CTX_free(ssl_ctx); // Free the SSL_CTX object
        throw Error("Failed to create SSL");
    }
    // SNI
    if (!opts.sni.empty())
    {
        if (!SSL_set_tlsext_host_name(ssl, opts.sni.c_str()))
        {
            SSL_free(ssl);
            SSL_CTX_free(ssl_ctx);
            throw Error("Failed to set SNI");
        }
    }

    return ssl;
}

TlsContextUPtr secure_connection(redisContext &ctx, const TlsOptions &opts) {
    static TlsInit tls_init;

//     auto c_str = [](const std::string &s) {
//         return s.empty() ? nullptr : s.c_str();
//     };

//     redisSSLContextError err;
// #ifdef REDIS_PLUS_PLUS_TLS_VERIFY_MODE
//     redisSSLOptions redis_ssl_opts;
//     std::memset(&redis_ssl_opts, 0, sizeof(redis_ssl_opts));
//     redis_ssl_opts.cacert_filename = c_str(opts.cacert);
//     redis_ssl_opts.capath = c_str(opts.cacertdir);
//     redis_ssl_opts.cert_filename = c_str(opts.cert);
//     redis_ssl_opts.private_key_filename = c_str(opts.key);
//     redis_ssl_opts.server_name = c_str(opts.sni);
//     redis_ssl_opts.verify_mode = opts.verify_mode;

//     auto tls_ctx = TlsContextUPtr(redisCreateSSLContextWithOptions(&redis_ssl_opts, &err));
// #else
//     auto tls_ctx = TlsContextUPtr(redisCreateSSLContext(c_str(opts.cacert),
//                                                         c_str(opts.cacertdir),
//                                                         c_str(opts.cert),
//                                                         c_str(opts.key),
//                                                         c_str(opts.sni),
//                                                         &err));
// #endif // end REDIS_PLUS_PLUS_TLS_VERIFY_MODE

    // if (!tls_ctx) {
    //     throw Error(std::string("failed to create TLS context: ")
    //                 + redisSSLContextGetError(err));
    // }
    
    auto ssl_ctx = initSSLCTX(opts);
    auto ssl = initSSL(ssl_ctx, opts);

    if (!ssl)
    {
        throw_error(ctx, "Failed to initialize SSL");
    }

    if (redisInitiateSSL(&ctx, ssl) != REDIS_OK) {
        throw_error(ctx, "Failed to initialize TLS connection");
    }

    auto tls_ctx = TlsContextUPtr(ssl_ctx);
    return tls_ctx;
}

}

}

}
