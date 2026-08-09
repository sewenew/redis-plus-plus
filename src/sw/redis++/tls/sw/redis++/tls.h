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

#ifndef SEWENEW_REDISPLUSPLUS_TLS_H
#define SEWENEW_REDISPLUSPLUS_TLS_H

#include <string>
#include <memory>
#include <hiredis/hiredis.h>
#include <hiredis/hiredis_ssl.h>
#include <openssl/ssl.h>

namespace sw {

namespace redis {

namespace tls {

#ifdef REDIS_SSL_VERIFY_PEER
#define REDIS_PLUS_PLUS_TLS_VERIFY_MODE
#endif // end REDIS_SSL_VERIFY_PEER

// Disable auto initializing OpenSSL.
// You should call it only once and call it before any sw::redis::Redis operation.
// Otherwise, the behavior is undefined.
void enable_auto_init();

class TlsInit {
public:
    TlsInit();
};

struct TlsOptions {
    bool enabled = false;

    std::string cacert;

    std::string cacertdir;

    std::string cert;

    std::string key;

    std::string sni;

    std::string tls_protocol;

    std::string ciphers; //list preferred ciphers, use ":" as separator

#ifdef REDIS_PLUS_PLUS_TLS_VERIFY_MODE
    int verify_mode = REDIS_SSL_VERIFY_PEER;
#endif // end REDIS_PLUS_PLUS_TLS_VERIFY_MODE
};

inline bool enabled(const TlsOptions &opts) {
    return opts.enabled;
}

inline bool isEqualIgnoreCase(const std::string &str1, const std::string &str2)
{
    if (str1.length() != str2.length())
    {
        return false;
    }
    return std::equal(str1.begin(), str1.end(), str2.begin(),
                      [](char c1, char c2)
                      {
                          return std::tolower(c1) == std::tolower(c2);
                      });
}

struct TlsContextDeleter {
    void operator()(SSL_CTX *ssl_context) const {
        if (ssl_context != nullptr) {
            SSL_CTX_free(ssl_context);
        }
    }
};

using TlsContextUPtr = std::unique_ptr<SSL_CTX, TlsContextDeleter>;

TlsContextUPtr secure_connection(redisContext &ctx, const TlsOptions &opts);

SSL_CTX * initSSLCTX(const TlsOptions &opts);
SSL * initSSL(SSL_CTX *ssl_context, const TlsOptions &opts);

}

}

}

#endif // end SEWENEW_REDISPLUSPLUS_TLS_H
