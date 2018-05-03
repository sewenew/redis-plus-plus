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

#ifndef SEWENEW_REDISPLUSPLUS_QUEUED_REDIS_H
#define SEWENEW_REDISPLUSPLUS_QUEUED_REDIS_H

#include <cassert>
#include <chrono>
#include "connection_pool.h"
#include "utils.h"
#include "reply.h"
#include "command.h"

namespace sw {

namespace redis {

namespace chrono = std::chrono;

// TODO: If any command throws, how to ensure that QueuedRedis is still valid?
template <typename CmdPolicy>
class QueuedRedis {
public:
    QueuedRedis(QueuedRedis &&) = default;
    QueuedRedis& operator=(QueuedRedis &&) = default;

    ~QueuedRedis() {
        try {
            discard();
        } catch (const Error &e) {
            // Avoid throwing from destructor.
            return;
        }
    }

    template <typename Cmd, typename ...Args>
    QueuedRedis<CmdPolicy>& command(Cmd cmd, Args &&...args) {
        if (_connection.broken()) {
            throw Error("Connection is broken");
        }

        _policy.command(_connection, cmd, std::forward<Args>(args)...);

        return *this;
    }

    void exec() {
        _policy.exec(_connection);
    }

    void discard() {
        _policy.discard(_pool, _connection);
    }

    template <typename Result>
    Result get() {
        auto reply = _connection.recv();

        return reply::parse<Result>(*reply);
    }

    // CONNECTION commands.

    QueuedRedis<CmdPolicy>& auth(const StringView &password) {
        return command(cmd::auth, password);
    }

    QueuedRedis<CmdPolicy>& echo(const StringView &msg) {
        return command(cmd::echo, msg);
    }

    QueuedRedis<CmdPolicy>& ping() {
        return command<void (*)(Connection &)>(cmd::ping);
    }

    QueuedRedis<CmdPolicy>& ping(const StringView &msg) {
        return command<void (*)(Connection &, const StringView &)>(cmd::ping, msg);
    }

    QueuedRedis<CmdPolicy>& quit() {
        return command(cmd::quit);
    }

    QueuedRedis<CmdPolicy>& select(long long idx) {
        return command(cmd::select, idx);
    }

    QueuedRedis<CmdPolicy>& swapdb(long long idx1, long long idx2) {
        return command(cmd::swapdb, idx1, idx2);
    }

    // SERVER commands.

    QueuedRedis<CmdPolicy>& bgrewriteaof() {
        return command(cmd::bgrewriteaof);
    }

    QueuedRedis<CmdPolicy>& bgsave() {
        return command(cmd::bgsave);
    }

    QueuedRedis<CmdPolicy>& dbsize() {
        return command(cmd::dbsize);
    }

    QueuedRedis<CmdPolicy>& flushall(bool async = false) {
        return command(cmd::flushall, async);
    }

    QueuedRedis<CmdPolicy>& flushdb(bool async = false) {
        return command(cmd::flushdb, async);
    }

    QueuedRedis<CmdPolicy>& info() {
        return command<void (*)(Connection &)>(cmd::info);
    }

    QueuedRedis<CmdPolicy>& info(const StringView &section) {
        return command<void (*)(Connection &, const StringView &)>(cmd::info, section);
    }

    QueuedRedis<CmdPolicy>& lastsave() {
        return command(cmd::lastsave);
    }

    QueuedRedis<CmdPolicy>& save() {
        return command(cmd::save);
    }

    // KEY commands.

    QueuedRedis<CmdPolicy>& del(const StringView &key) {
        return command(cmd::del, key);
    }

    template <typename Input>
    QueuedRedis<CmdPolicy>& del(Input first, Input last) {
        return command(cmd::del_range, first, last);
    }

    QueuedRedis<CmdPolicy>& dump(const StringView &key) {
        return command(cmd::dump, key);
    }

    QueuedRedis<CmdPolicy>& exists(const StringView &key) {
        return command(cmd::exists, key);
    }

    template <typename Input>
    QueuedRedis<CmdPolicy>& exists(Input first, Input last) {
        return command(cmd::exists_range, first, last);
    }

    QueuedRedis<CmdPolicy>& expire(const StringView &key, long long timeout) {
        return command(cmd::expire, key, timeout);
    }

    QueuedRedis<CmdPolicy>& expire(const StringView &key,
                                    const chrono::seconds &timeout) {
        return expire(key, timeout.count());
    }

    QueuedRedis<CmdPolicy>& expireat(const StringView &key, long long timestamp) {
        return command(cmd::expireat, key, timestamp);
    }

    QueuedRedis<CmdPolicy>& expireat(const StringView &key,
                                        const chrono::time_point<chrono::system_clock,
                                                                    chrono::seconds> &tp) {
        return expireat(key, tp.time_since_epoch().count());
    }

    QueuedRedis<CmdPolicy>& keys(const StringView &pattern) {
        return command(cmd::keys, pattern);
    }

    QueuedRedis<CmdPolicy>& move(const StringView &key, long long db) {
        return command(cmd::move, key, db);
    }

    QueuedRedis<CmdPolicy>& persist(const StringView &key) {
        return command(cmd::persist, key);
    }

    QueuedRedis<CmdPolicy>& pexpire(const StringView &key, long long timeout) {
        return command(cmd::pexpire, key, timeout);
    }

    QueuedRedis<CmdPolicy>& pexpire(const StringView &key,
                                    const chrono::milliseconds &timeout) {
        return pexpire(key, timeout.count());
    }

    QueuedRedis<CmdPolicy>& pexpireat(const StringView &key, long long timestamp) {
        return command(cmd::pexpireat, key, timestamp);
    }

    QueuedRedis<CmdPolicy>& pexpireat(const StringView &key,
                                        const chrono::time_point<chrono::system_clock,
                                                                    chrono::milliseconds> &tp) {
        return pexpireat(key, tp.time_since_epoch().count());
    }

    QueuedRedis<CmdPolicy>& pttl(const StringView &key) {
        return command(cmd::pttl, key);
    }

    QueuedRedis<CmdPolicy>& randomkey() {
        return command(cmd::randomkey);
    }

    QueuedRedis<CmdPolicy>& rename(const StringView &key, const StringView &newkey) {
        return command(cmd::rename, key, newkey);
    }

    QueuedRedis<CmdPolicy>& renamenx(const StringView &key, const StringView &newkey) {
        return command(cmd::renamenx, key, newkey);
    }

    QueuedRedis<CmdPolicy>& restore(const StringView &key,
                                    long long ttl,
                                    const StringView &val,
                                    bool replace = false) {
        return command(cmd::restore, key, ttl, val, replace);
    }

    QueuedRedis<CmdPolicy>& restore(const StringView &key,
                                    const chrono::milliseconds &ttl,
                                    const StringView &val,
                                    bool replace = false) {
        return restore(key, ttl.count(), val, replace);
    }

    // TODO: sort

    QueuedRedis<CmdPolicy>& scan(long long cursor,
                                    const StringView &pattern,
                                    long long count) {
        return command(cmd::scan, cursor, pattern, count);
    }

    QueuedRedis<CmdPolicy>& scan(long long cursor) {
        return scan(cursor, "*", 10);
    }

    QueuedRedis<CmdPolicy>& scan(long long cursor,
                                    const StringView &pattern) {
        return scan(cursor, pattern, 10);
    }

    QueuedRedis<CmdPolicy>& scan(long long cursor,
                                    long long count) {
        return scan(cursor, "*", count);
    }

    QueuedRedis<CmdPolicy>& touch(const StringView &key) {
        return command(cmd::touch, key);
    }

    template <typename Input>
    QueuedRedis<CmdPolicy>& touch(Input first, Input last) {
        return command(cmd::touch, first, last);
    }

    QueuedRedis<CmdPolicy>& ttl(const StringView &key) {
        return command(cmd::ttl, key);
    }

    QueuedRedis<CmdPolicy>& type(const StringView &key) {
        return command(cmd::type, key);
    }

    QueuedRedis<CmdPolicy>& unlink(const StringView &key) {
        return command(cmd::unlink, key);
    }

    template <typename Input>
    QueuedRedis<CmdPolicy>& unlink(Input first, Input last) {
        return command(cmd::unlink_range, first, last);
    }

    QueuedRedis<CmdPolicy>& wait(long long numslaves, long long timeout) {
        return command(cmd::wait, numslaves, timeout);
    }

    QueuedRedis<CmdPolicy>& wait(long long numslaves, const chrono::milliseconds &timeout) {
        return wait(numslaves, timeout.count());
    }

    // STRING commands.

    QueuedRedis<CmdPolicy>& get(const StringView &key) {
        return command(cmd::get, key);
    }

    QueuedRedis<CmdPolicy>& set(const StringView &key,
                                const StringView &val,
                                const chrono::milliseconds &ttl = chrono::milliseconds(0),
                                UpdateType type = UpdateType::ALWAYS) {
        return command(cmd::set,
                        key,
                        val,
                        ttl.count(),
                        type);
    }

private:
    friend class Redis;

    template <typename ...Args>
    QueuedRedis(ConnectionPool &pool, Args &&...args) :
                _pool(pool),
                _connection(_pool.fetch()),
                _policy(std::forward<Args>(args)...) {}

    ConnectionPool &_pool;

    Connection _connection;

    CmdPolicy _policy;
};

}

}

#endif // end SEWENEW_REDISPLUSPLUS_QUEUED_REDIS_H
