/**
 *    Copyright (C) 2015 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kASIO

#include "mongo/platform/basic.h"

#include "mongo/executor/connection_pool.h"

#include "mongo/executor/connection_pool_stats.h"
#include "mongo/executor/remote_command_request.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/log.h"
#include "mongo/executor/connection_pool_core.h"

// One interesting implementation note herein concerns how setup() and
// refresh() are invoked outside of the global lock, but setTimeout is not.
// This implementation detail simplifies mocks, allowing them to return
// synchronously sometimes, whereas having timeouts fire instantly adds little
// value. In practice, dumping the locks is always safe (because we restrict
// ourselves to operations over the connection).

namespace mongo {
namespace executor {

static void disposeConnection(ConnectionPoolCore* core, ConnectionPoolCore::Connection* conn) {
    core->rmConn(conn);
    delete conn->val.conn_iface;
    delete conn;
}

static void deleteRequest(ConnectionPoolCore::Request* r) {
    if (r->val.rq_callback) {
        delete r->val.rq_callback;
    }
    delete r;
}

constexpr Milliseconds ConnectionPool::kDefaultHostTimeout;
int const ConnectionPool::kDefaultMaxConns = std::numeric_limits<int>::max();
int const ConnectionPool::kDefaultMinConns = 1;
int const ConnectionPool::kDefaultMaxConnecting = std::numeric_limits<int>::max();
constexpr Milliseconds ConnectionPool::kDefaultRefreshRequirement;
constexpr Milliseconds ConnectionPool::kDefaultRefreshTimeout;

const Status ConnectionPool::kConnectionStateUnknown =
    Status(ErrorCodes::InternalError, "Connection is in an unknown state");

ConnectionPool::ConnectionPool(std::unique_ptr<DependentTypeFactoryInterface> impl,
                               std::string name,
                               Options options)
    : _core(new ConnectionPoolCore(
        options.minConnections,
        options.maxConnections,
        options.maxConnecting,
        options.refreshTimeout,
        options.refreshRequirement,
        options.hostTimeout,
        {}, {},
        0)),
    _requestTimer(impl->makeTimer()),
    _name(std::move(name)),
    _factory(std::move(impl)) {
}

ConnectionPool::~ConnectionPool() = default;

void ConnectionPool::cleanupHost(
        const stdx::unique_lock<stdx::mutex>& lk,
        const HostAndPort& hostAndPort) {

    // Drop (or mark as dropped) connections to the host.
    std::vector<ConnectionPoolCore::Connection*> toMark;
    std::vector<ConnectionPoolCore::Connection*> toDel;
    _core->connsForHost(hostAndPort, [&toDel, &toMark](ConnectionPoolCore::Connection* c) {
        // Connections currently owned by the pool (i.e. those in the READY or
        // PROCESSING states) can be deleted immediately.  Connections that are
        // checked out by clients need to be marked "dropped" and they will be
        // cleaned up when they are returned to us.
        (c->val.conn_state == ConnectionPoolCore::CHECKED_OUT ?
            toMark : toDel).push_back(c);
    });
    for (auto c : toDel) {
        disposeConnection(_core.get(), c);
    }
    for (auto c : toMark) {
        _core->markDropped(c);
    }

    // Drop all requests for the host.
    std::vector<ConnectionPoolCore::Request*> reqs;
    _core->reqsForHost(hostAndPort, [&reqs](ConnectionPoolCore::Request* r) { reqs.push_back(r); });
    Status statusToReport(
        ErrorCodes::PooledConnectionsDropped,
        "Pooled connections dropped");
    for (auto r : reqs) {
        _core->rmReq(r);
        (*(r->val.rq_callback))(statusToReport);
        deleteRequest(r);
    }
}

void ConnectionPool::dropConnections(const HostAndPort& hostAndPort) {
    stdx::unique_lock<stdx::mutex> lk(_mutex);
    cleanupHost(lk, hostAndPort);
}

void ConnectionPool::processingComplete(void* cc, Status status) {
    ConnectionPoolCore::Connection* c = reinterpret_cast<ConnectionPoolCore::Connection*>(cc);
    stdx::unique_lock<stdx::mutex> lk(_mutex);
    const auto& hostAndPort = c->val.conn_host;
    auto* connPtr = c->val.conn_iface;
    connPtr->indicateUsed();
    connPtr->cancelTimeout(); // defensive, and it makes the test fixture happy

    // If the connection refreshed successfully, throw it back in the ready
    // pool
    if (status.isOK()) {
        auto now = _factory->now();
        _core->changeState(c, ConnectionPoolCore::READY, now, now, _core->getRefreshRequirement());
        fulfillReqs(lk, hostAndPort);
        spawnConnections(lk, hostAndPort);
        waitForNextEvent(lk);
        return;
    }

    // If we've exceeded the time limit, start a new connect, rather than
    // failing all operations.  We do this because the various callers have
    // their own time limit which is unrelated to our internal one.
    if (status.code() == ErrorCodes::NetworkInterfaceExceededTimeLimit) {
        log() << "Pending connection to host " << hostAndPort
              << " did not complete within the connection timeout,"
              << " retrying with a new connection;" << _core->openConnections(hostAndPort)
              << " connections to that host remain open";
        disposeConnection(_core.get(), c);
        spawnConnections(lk, hostAndPort);
        waitForNextEvent(lk);
        return;
    }

    // Otherwise pass the failure on through
    cleanupHost(lk, hostAndPort);
}

void ConnectionPool::spawnConnections(
        stdx::unique_lock<stdx::mutex>& lk,
        const HostAndPort& hostAndPort) {
    while (_core->needsMoreConnections(hostAndPort)) {
        ConnectionInterface* connPtr;
        try {
            // make a new connection and put it in processing
            connPtr = _factory->makeConnection(hostAndPort, 0).release();
        } catch (std::system_error& e) {
            severe() << "Failed to construct a new connection object: " << e.what();
            fassertFailed(40336);
        }

        auto c = new ConnectionPoolCore::Connection();
        c->val.conn_state = ConnectionPoolCore::PROCESSING;
        c->val.conn_host  = hostAndPort;
        c->val.conn_iface = connPtr;
        _core->addConn(c);

        lk.unlock();
        connPtr->setup(_core->getRefreshTimeout(),
            [this, c](ConnectionInterface* connPtr, Status status) {
                processingComplete(c, status);
        });
        lk.lock();
    }
}

void ConnectionPool::fulfillReqs(
        stdx::unique_lock<stdx::mutex>& lk,
        const HostAndPort& hostAndPort) {
    // PRE: expired requests have been evicted
    for (;;) {
        auto r = _core->nextReq(hostAndPort);
        if (!r) break;
        auto c = _core->mruConn(hostAndPort);
        if (!c) break;
        _core->requestGranted(r, c);
        lk.unlock();
        (*(r->val.rq_callback))(ConnectionPool::ConnectionHandle(
            c->val.conn_iface,
            ConnectionPool::ConnectionHandleDeleter(this)));
        lk.lock();
        deleteRequest(r);
    }
}

void ConnectionPool::waitForNextEvent(
        const stdx::unique_lock<stdx::mutex>& lk) {
    // wait, then:
    //   - if req. timeout: delete request, send timeout status
    //   - if proc. timeout: delete conn?
    if (!_core->hasEvent()) {
        _requestTimer->cancelTimeout();
        return;
    }
    _requestTimer->setTimeout(_core->nextEvent() - _factory->now(), [this] {
        stdx::unique_lock<stdx::mutex> lk(_mutex);
        auto now = _factory->now();

        // check for expired hosts
        std::vector<HostAndPort> expiredHosts;
        _core->expiredHosts(now, [&expiredHosts](const HostAndPort& p) {
            expiredHosts.push_back(p);
        });
        for (auto& p : expiredHosts) {
            cleanupHost(lk, p);
        }

        // check for expired requests
        std::vector<ConnectionPoolCore::Request*> expired;
        _core->expiredRequests(now, [&expired](ConnectionPoolCore::Request* r) {
            (*(r->val.rq_callback))(Status(
                ErrorCodes::NetworkInterfaceExceededTimeLimit,
                "Couldn't get a connection within the time limit"));
            expired.push_back(r);
        });
        for (auto r : expired) {
            _core->rmReq(r);
            deleteRequest(r);
        }

        // check for connections needing refresh
        std::vector<ConnectionPoolCore::Connection*> toRefresh;
        _core->connectionsToRefresh(now, [&toRefresh](ConnectionPoolCore::Connection* c) {
            toRefresh.push_back(c);
        });
        for (auto c : toRefresh) {
            if (_core->shouldKeepConnection(c, now)) {
                _core->changeState(c, ConnectionPoolCore::PROCESSING, now, now, _core->getRefreshRequirement());
                lk.unlock();
                c->val.conn_iface->refresh(_core->getRefreshTimeout(), [this, c](ConnectionInterface* connPtr, Status status) {
                    processingComplete(c, status);
                });
                lk.lock();
            } else {
                disposeConnection(_core.get(), c);
            }
        }

        // setup timeout again
        waitForNextEvent(lk);
    });
}

void ConnectionPool::get(const HostAndPort& hostAndPort,
                         Milliseconds timeout,
                         GetConnectionCallback cb) {
    stdx::unique_lock<stdx::mutex> lk(_mutex);

    // I'm not sure why this logic is here.  What is the actual connection
    // between `timeout` (how long to wait before this request expires) and
    // `_options.refreshTimeout` (how long to wait before giving up on a
    // connection refresh)?
    auto refreshTimeout = _core->getRefreshTimeout();
    if (timeout < Milliseconds(0) || timeout > refreshTimeout) {
        timeout = refreshTimeout;
    }

    auto r = new ConnectionPoolCore::Request();
    r->val.rq_expiration = _factory->now() + timeout;
    r->val.rq_host = hostAndPort;
    r->val.rq_callback = new GetConnectionCallback(std::move(cb));
    _core->addReq(r);

    spawnConnections(lk, hostAndPort);
    fulfillReqs(lk, hostAndPort);
    waitForNextEvent(lk);
}

void ConnectionPool::appendConnectionStats(ConnectionPoolStats* stats) const {
    stdx::unique_lock<stdx::mutex> lk(_mutex);

    _core->allHosts([this, stats](HostAndPort host) {
        ConnectionStatsPer hostStats{
            static_cast<size_t>(_core->inUseConnections(host)),
            static_cast<size_t>(_core->availableConnections(host)),
            static_cast<size_t>(0), /* todo */
            static_cast<size_t>(_core->refreshingConnections(host))};
        stats->updateStatsForHost(_name, host, hostStats);
    });
}

size_t ConnectionPool::getNumConnectionsPerHost(const HostAndPort& hostAndPort) const {
    stdx::unique_lock<stdx::mutex> lk(_mutex);
    return _core->openConnections(hostAndPort);
}

void ConnectionPool::returnConnection(ConnectionInterface* conn) {
    stdx::unique_lock<stdx::mutex> lk(_mutex);
    auto c = _core->handleForInterface(conn);
    invariant(conn->getStatus() != kConnectionStateUnknown);

    // Was this connection marked dropped while it was checked out?
    if (c->val.conn_dropped) {
        disposeConnection(_core.get(), c);
        return;
    }

    auto hostAndPort = c->val.conn_host;
    if (!conn->getStatus().isOK()) {
        // TODO: alert via some callback if the host is bad
        log() << "Ending connection to host " << hostAndPort << " due to bad connection status; "
              << _core->openConnections(hostAndPort) << " connections to that host remain open";
        disposeConnection(_core.get(), c);
        return;
    }

    _core->changeState(c, ConnectionPoolCore::READY, _factory->now(), conn->getLastUsed(), _core->getRefreshRequirement());
    fulfillReqs(lk, hostAndPort);
    waitForNextEvent(lk);
}

}  // namespace executor
}  // namespace mongo
