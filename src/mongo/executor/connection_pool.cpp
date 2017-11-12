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

#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/executor/connection_pool_stats.h"
#include "mongo/executor/remote_command_request.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/destructor_guard.h"
#include "mongo/util/log.h"
#include "mongo/util/lru_cache.h"
#include "mongo/util/scopeguard.h"
#include "mongo/executor/connection_pool_core.h"

#include <iostream>
using std::cout;
using std::cerr;
using std::endl;

// #define mycheck(cond, dbg) \
//     if (!(cond)) { std::cerr << dbg << std::endl; invariant(false); }
// #define mycheck_eq(x, y, dbg) \
//     mycheck((x) == (y), '@' << (dbg) << " ... " << (#x) << " != " << (#y) << ": " << (x) << " vs " << (y))

// One interesting implementation note herein concerns how setup() and
// refresh() are invoked outside of the global lock, but setTimeout is not.
// This implementation detail simplifies mocks, allowing them to return
// synchronously sometimes, whereas having timeouts fire instantly adds little
// value. In practice, dumping the locks is always safe (because we restrict
// ourselves to operations over the connection).

namespace mongo {
namespace executor {

static void disposeConnection(ConnectionPoolCore* core, ConnectionPoolCore::Connection* conn) {
    cerr << "!!! DISPOSING OF " << conn << " !!!" << endl;
    core->rmConn(conn);
    delete conn->val.conn_iface;
    delete conn;
}

#if 0
/**
 * A pool for a specific HostAndPort
 *
 * Pools come into existance the first time a connection is requested and
 * go out of existence after hostTimeout passes without any of their
 * connections being used.
 */
class ConnectionPool::SpecificPool {
public:
    SpecificPool(ConnectionPool* parent, const HostAndPort& hostAndPort);
    ~SpecificPool();

    /**
     * Gets a connection from the specific pool. Sinks a unique_lock from the
     * parent to preserve the lock on _mutex
     */
    void getConnection(const HostAndPort& hostAndPort,
                       Milliseconds timeout,
                       stdx::unique_lock<stdx::mutex> lk,
                       GetConnectionCallback cb);

    /**
     * Cascades a failure across existing connections and requests. Invoking
     * this function drops all current connections and fails all current
     * requests with the passed status.
     */
    void processFailure(const Status& status, stdx::unique_lock<stdx::mutex> lk);

    /**
     * Returns a connection to a specific pool. Sinks a unique_lock from the
     * parent to preserve the lock on _mutex
     */
    void returnConnection(ConnectionInterface* connection, stdx::unique_lock<stdx::mutex> lk);

    /**
     * Returns the number of connections currently checked out of the pool.
     */
    size_t inUseConnections(const stdx::unique_lock<stdx::mutex>& lk);

    /**
     * Returns the number of available connections in the pool.
     */
    size_t availableConnections(const stdx::unique_lock<stdx::mutex>& lk);

    /**
     * Returns the number of in progress connections in the pool.
     */
    size_t refreshingConnections(const stdx::unique_lock<stdx::mutex>& lk);

    /**
     * Returns the total number of connections ever created in this pool.
     */
    size_t createdConnections(const stdx::unique_lock<stdx::mutex>& lk);

    /**
     * Returns the total number of connections currently open that belong to
     * this pool. This is the sum of refreshingConnections, availableConnections,
     * and inUseConnections.
     */
    size_t openConnections(const stdx::unique_lock<stdx::mutex>& lk);

private:
    using OwnedConnection = ConnectionInterface*;
    using OwnershipPool = stdx::unordered_map<ConnectionInterface*, OwnedConnection>;
    using LRUOwnershipPool = LRUCache<OwnershipPool::key_type, OwnershipPool::mapped_type>;

    void addToReady(stdx::unique_lock<stdx::mutex>& lk, ConnectionPoolCore::Connection* conn);

    void fulfillRequests(stdx::unique_lock<stdx::mutex>& lk);

    void spawnConnections(stdx::unique_lock<stdx::mutex>& lk);

    void shutdown();

    void updateStateInLock();

private:
    ConnectionPool* const _parent;

    const HostAndPort _hostAndPort;

    /**
     * Timer for request timeouts.
     *
     * If there are unfulfilled requests, then this will fire when the next
     * request expires.
     *
     * If there are no unfulfilled requests but there are checked out
     * connections, then this timer is not set (and _requestTimerExpiration is
     * the maximum possible timestamp).
     *
     * If there are no unfilfulled requests and there are no checked out
     * connections, then this will fire when _options.hostTimeout has passed
     * since the last activity.
     */
    std::unique_ptr<TimerInterface> _requestTimer;

    /**
     * The time at which _requestTimer is expected to fire.
     */
    Date_t _requestTimerExpiration;

    size_t _generation;
    bool _inFulfillRequests;
    bool _inSpawnConnections;

    size_t _created;

    /**
     * The current state of the pool
     *
     * The pool begins in a running state. Moves to idle when no requests
     * are pending and no connections are checked out. It finally enters
     * shutdown after hostTimeout has passed (and waits there for current
     * refreshes to process out).
     *
     * At any point a new request sets the state back to running and
     * restarts all timers.
     */
    using State = ConnectionPoolCore::HostState;
    State _state;
};
#endif

std::ostream& operator<<(std::ostream& stream, ConnectionPoolCore::HostState state) {
    switch (state) {
        case ConnectionPoolCore::HOST_RUNNING:
            stream << "RUNNING";
            break;
        case ConnectionPoolCore::HOST_IDLE:
            stream << "IDLE";
            break;
        case ConnectionPoolCore::HOST_IN_SHUTDOWN:
            stream << "IN_SHUTDOWN";
            break;
        default:
            stream << "UNKNOWN_STATE";
            break;
    }
    return stream;
}

static constexpr auto kRunning = ConnectionPoolCore::HOST_RUNNING;
static constexpr auto kIdle = ConnectionPoolCore::HOST_IDLE;
static constexpr auto kInShutdown = ConnectionPoolCore::HOST_IN_SHUTDOWN;

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
        {}, {})),
    _requestTimer(impl->makeTimer()),
    _name(std::move(name)),
    _options(std::move(options)),
    _factory(std::move(impl)) {
}

ConnectionPool::~ConnectionPool() {
    cerr << "DELETED CONNECTION POOL" << endl;
}

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
        cerr << "  cleaning up connection " << c << endl;
        disposeConnection(_core.get(), c);
    }
    for (auto c : toMark) {
        cerr << "  marking dropped: " << c << endl;
        _core->markDropped(c);
    }

    // Drop all requests for the host.
    std::vector<ConnectionPoolCore::Request*> reqs;
    _core->reqsForHost(hostAndPort, [&reqs](ConnectionPoolCore::Request* r) { reqs.push_back(r); });
    Status statusToReport(
        ErrorCodes::PooledConnectionsDropped,
        "Pooled connections dropped");
    for (auto r : reqs) {
        cerr << "  cleaning up request " << r << endl;
        _core->dropRequest(r);
        r->val.rq_callback(statusToReport);
        delete r;
    }

    // auto iter = _pools.find(hostAndPort);

    // if (iter == _pools.end())
    //     return;

    // iter->second.get()->processFailure(
    //     Status(ErrorCodes::PooledConnectionsDropped, "Pooled connections dropped"), std::move(lk));

}

void ConnectionPool::dropConnections(const HostAndPort& hostAndPort) {
    stdx::unique_lock<stdx::mutex> lk(_mutex);

    cerr << "dropConnections(" << hostAndPort << ')' << endl;
    cleanupHost(lk, hostAndPort);
    cerr << "dropConnections finished" << endl;
}

using Factory = ConnectionPool::DependentTypeFactoryInterface;

void ConnectionPool::processingComplete(void* cc, Status status) {
    ConnectionPoolCore::Connection* c = reinterpret_cast<ConnectionPoolCore::Connection*>(cc);
    stdx::unique_lock<stdx::mutex> lk(_mutex);
    cerr << "Setup/refresh finished for " << c << " in state " << c->val.conn_state << endl;
    const auto& hostAndPort = c->val.conn_host;
    auto* connPtr = c->val.conn_iface;
    connPtr->indicateUsed();
    connPtr->cancelTimeout(); // defensive, and it makes the test fixture happy

    // stdx::unique_lock<stdx::mutex> lk(_parent->_mutex);

    // // If the host and port were dropped, let this lapse
    // if (conn->getGeneration() != _generation) {
    //     disposeConnection(_parent->_core.get(), handle);
    //     spawnConnections(lk);
    //     return;
    // }

    // // If we're in shutdown, we don't need refreshed connections
    // if (_state == kInShutdown) {
    //     disposeConnection(_parent->_core.get(), handle);
    //     return;
    // }

    // If the connection refreshed successfully, throw it back in the ready
    // pool
    if (status.isOK()) {
        cerr << "  SUCCESS: " << c << endl;
        auto now = _factory->now();
        _core->changeState(c, ConnectionPoolCore::READY, now, now, _core->getRefreshRequirement());
        cerr << "    (refresh in " << (c->val.conn_next_refresh - now) << "; req=" << _core->getRefreshRequirement() << ")" << endl;
        fulfillReqs(lk, hostAndPort);
        spawnConnections(lk, hostAndPort);
        waitForNextEvent(lk);
        return;
    }

    // If we've exceeded the time limit, start a new connect, rather than
    // failing all operations.  We do this because the various callers have
    // their own time limit which is unrelated to our internal one.
    if (status.code() == ErrorCodes::NetworkInterfaceExceededTimeLimit) {
        cerr << "  Oops! FAILED: " << c << endl;
        log() << "Pending connection to host " << hostAndPort
              << " did not complete within the connection timeout,"
              << " retrying with a new connection;" << _core->openConnections(hostAndPort)
              << " connections to that host remain open";
        disposeConnection(_core.get(), c);
        spawnConnections(lk, hostAndPort);
        waitForNextEvent(lk);
        return;
    }
    cerr << "Unimplemented setup case; " << c << endl;

    // // Otherwise pass the failure on through
    // processFailure(status, std::move(lk));
}

void ConnectionPool::spawnConnection(
        stdx::unique_lock<stdx::mutex>& lk,
        const HostAndPort& hostAndPort,
        size_t generation) {

    decltype(_factory->makeConnection(hostAndPort, generation).release()) connPtr;
    try {
        // make a new connection and put it in processing
        connPtr = _factory->makeConnection(hostAndPort, generation).release();
    } catch (std::system_error& e) {
        severe() << "Failed to construct a new connection object: " << e.what();
        fassertFailed(40336);
    }

    auto c = new ConnectionPoolCore::Connection();
    c->val.conn_state = ConnectionPoolCore::PROCESSING;
    c->val.conn_host  = hostAndPort;
    c->val.conn_iface = connPtr;
    _core->addConn(c);

    cerr << "Setting up connection " << c << " (initial state: " << c->val.conn_state << ')' << endl;
    lk.unlock();
    connPtr->setup(_core->getRefreshTimeout(),
        [this, c](ConnectionInterface* connPtr, Status status) {
            processingComplete(c, status);
    });
    lk.lock();
}


void ConnectionPool::spawnConnections(
        stdx::unique_lock<stdx::mutex>& lk,
        const HostAndPort& hostAndPort) {
    cerr << "now in spawnConnections" << endl;
    cerr << "  conns: " << _core->openConnections(hostAndPort) << '/' << _core->maxConnsPerHost() << endl;
    cerr << "  refrs: " << _core->refreshingConnections(hostAndPort) << '/' << _core->maxRefreshingConnsPerHost() << endl;
    cerr << "  avail+refrshing = " << _core->availableConnections(hostAndPort) << '+' << _core->refreshingConnections(hostAndPort) << endl;
    cerr << "  #reqs: " << _core->numReqs(hostAndPort) << endl;
    cerr << "  min=" << _core->minConnsPerHost() << endl;
    while (_core->needsMoreConnections(hostAndPort)) {
        cerr << "spawning conn to " << hostAndPort << endl;
        spawnConnection(lk, hostAndPort);
        cerr << "   ... now have " << _core->openConnections(hostAndPort) << '/' << _core->maxConnsPerHost() << endl;
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
        cerr << "fulfilling " << r << " w/" << c << " (total reqs: " << _core->numReqs(r->val.rq_host) << ')' << endl;
        _core->requestGranted(r, c);
        lk.unlock();
        r->val.rq_callback(ConnectionPool::ConnectionHandle(
            c->val.conn_iface,
            ConnectionPool::ConnectionHandleDeleter(this)));
        lk.lock();
        cerr << "  (now total reqs: " << _core->numReqs(r->val.rq_host) << ')' << endl;
        delete r;
    }
}

void ConnectionPool::waitForNextEvent(
        const stdx::unique_lock<stdx::mutex>& lk) {
    // wait, then:
    //   - if req. timeout: delete request, send timeout status
    //   - if proc. timeout: delete conn?
    if (!_core->hasEvent()) {
        cerr << "(no event to wait for)" << endl;
        _requestTimer->cancelTimeout();
        return;
    }
    auto time = _core->nextEvent();
    cerr << "next event @" << time << endl;
    auto now = _factory->now();
    cerr << "waiting " << (time-now) << " for next event..." << endl;
    _requestTimer->setTimeout(time - now, [this] {
        cerr << "EVENT!" << endl;
        stdx::unique_lock<stdx::mutex> lk(_mutex);
        auto now = _factory->now();

        // check for expired hosts
        std::vector<HostAndPort> expiredHosts;
        _core->expiredHosts(now, [&expiredHosts](const HostAndPort& p) {
            expiredHosts.push_back(p);
        });
        for (auto& p : expiredHosts) {
            cerr << "HOST EXPIRED: " << p << endl;
            cleanupHost(lk, p);
        }

        // check for expired requests
        std::vector<ConnectionPoolCore::Request*> expired;
        _core->expiredRequests(now, [&expired](ConnectionPoolCore::Request* r) {
            cerr << "  request expired: " << r << endl;
            r->val.rq_callback(Status(
                ErrorCodes::NetworkInterfaceExceededTimeLimit,
                "Couldn't get a connection within the time limit"));
            expired.push_back(r);
        });
        for (auto r : expired) {
            _core->dropRequest(r);
            delete r;
        }

        // check for connections needing refresh
        std::vector<ConnectionPoolCore::Connection*> toRefresh;
        _core->connectionsToRefresh(now, [&toRefresh](ConnectionPoolCore::Connection* c) {
            toRefresh.push_back(c);
        });
        for (auto c : toRefresh) {
            if (_core->shouldKeepConnection(c, now)) {
                cerr << "  conn " << c << " needs refresh!" << endl;
                cerr << "    now it is  " << now << endl;
                cerr << "    returned @ " << c->val.conn_returned << endl;
                cerr << "    timeout  @ " << (c->val.conn_returned + _core->getRefreshRequirement()) << endl;
                _core->changeState(c, ConnectionPoolCore::PROCESSING, now, now, _core->getRefreshRequirement());
                lk.unlock();
                c->val.conn_iface->refresh(_core->getRefreshTimeout(), [this, c](ConnectionInterface* connPtr, Status status) {
                    processingComplete(c, status);
                });
                lk.lock();
            } else {
                cerr << "  conn " << c << " is no longer needed" << endl;
                disposeConnection(_core.get(), c);
            }
        }
        // fulfillReqs(lk, hostAndPort);
        // TODO: process ready conns
        waitForNextEvent(lk);
    });
    cerr << "  timeout set!" << endl;
}

void ConnectionPool::get(const HostAndPort& hostAndPort,
                         Milliseconds timeout,
                         GetConnectionCallback cb) {
    stdx::unique_lock<stdx::mutex> lk(_mutex);
#if 1
    // I'm not sure why this logic is here.  What is the actual connection
    // between `timeout` (how long to wait before this request expires) and
    // `_options.refreshTimeout` (how long to wait before giving up on a
    // connection refresh)?
    if (timeout < Milliseconds(0) || timeout > _options.refreshTimeout) {
        timeout = _options.refreshTimeout;
    }

    cerr << "Request for " << hostAndPort << "; timeout=" << timeout << endl;

    const auto now = _factory->now();
    const auto expiration = now + timeout;

    auto r = new ConnectionPoolCore::Request();
    r->val.rq_expiration = expiration;
    r->val.rq_host = hostAndPort;
    r->val.rq_callback = std::move(cb);
    _core->addReq(r);

    // updateStateInLock();
    spawnConnections(lk, hostAndPort);
    fulfillReqs(lk, hostAndPort);
    waitForNextEvent(lk);
    // fulfillRequests(lk);
#else
    SpecificPool* pool;


    auto iter = _pools.find(hostAndPort);

    if (iter == _pools.end()) {
        auto handle = stdx::make_unique<SpecificPool>(this, hostAndPort);
        pool = handle.get();
        _pools[hostAndPort] = std::move(handle);
    } else {
        pool = iter->second.get();
    }

    invariant(pool);

    mycheck_eq(_core->openConnections(hostAndPort), pool->openConnections(lk), "ConnectionPool::get");
    pool->getConnection(hostAndPort, timeout, std::move(lk), std::move(cb));
    // mycheck_eq(_core->openConnections(hostAndPort), pool->openConnections(lk));
#endif
}

void ConnectionPool::appendConnectionStats(ConnectionPoolStats* stats) const {
    stdx::unique_lock<stdx::mutex> lk(_mutex);

    _core->allHosts([this, stats](HostAndPort host) {
        ConnectionStatsPer hostStats{
            static_cast<size_t>(_core->inUseConnections(host)),       //pool->inUseConnections(lk),
            static_cast<size_t>(_core->availableConnections(host)),   //pool->availableConnections(lk),
            static_cast<size_t>(0), /* todo */                        //pool->createdConnections(lk),
            static_cast<size_t>(_core->refreshingConnections(host))}; //pool->refreshingConnections(lk)};
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
    cerr << "conn returned: " << c << " (state==" << c->val.conn_state << ')' << endl;
    invariant(conn->getStatus() != kConnectionStateUnknown);

    // Was this connection marked dropped while it was checked out?
    if (c->val.conn_dropped) {
        cerr << (c->val.conn_dropped ? "  was dropped!" : "  not needed!") << endl;
        disposeConnection(_core.get(), c);
        return;
    }

    _core->changeState(c, ConnectionPoolCore::READY, _factory->now(), conn->getLastUsed(), _core->getRefreshRequirement());
    auto hostAndPort = c->val.conn_host;

    // if (_core->hasExcessConnections(hostAndPort)) {
    //     // drop it!
    //     cerr << "  dropping due to excess" << endl;
    //     disposeConnection(_core.get(), c);
    //     return;
    // }

    if (!conn->getStatus().isOK()) {
        // TODO: alert via some callback if the host is bad
        log() << "Ending connection to host " << hostAndPort << " due to bad connection status; "
              << _core->openConnections(hostAndPort) << " connections to that host remain open";
        disposeConnection(_core.get(), c);
        return;
    }

    // // Drop LRU connections
    // cerr << "checking for unnecessary connections..." << endl;
    // while (_core->hasExcessConnections(hostAndPort)) {
    //     auto lru = _core->lruOwnedConn(hostAndPort);
    //     cerr << "  dropping unnecessary conn " << lru << endl;
    //     cerr << "    state = " << lru->val.conn_state << endl;
    //     invariant(lru);
    //     disposeConnection(_core.get(), lru);
    // }

    fulfillReqs(lk, hostAndPort);
    waitForNextEvent(lk);

    // auto iter = _pools.find(conn->getHostAndPort());
    // invariant(iter != _pools.end());
    // iter->second.get()->returnConnection(conn, std::move(lk));
}

#if 0
ConnectionPool::SpecificPool::SpecificPool(ConnectionPool* parent, const HostAndPort& hostAndPort)
    : _parent(parent),
      _hostAndPort(hostAndPort),
      _requestTimer(parent->_factory->makeTimer()),
      _generation(0),
      _inFulfillRequests(false),
      _inSpawnConnections(false),
      _created(0),
      _state(kRunning) {}

ConnectionPool::SpecificPool::~SpecificPool() {
    DESTRUCTOR_GUARD(_requestTimer->cancelTimeout();)
}

size_t ConnectionPool::SpecificPool::inUseConnections(const stdx::unique_lock<stdx::mutex>& lk) {
    return _parent->_core->inUseConnections(_hostAndPort);
}

size_t ConnectionPool::SpecificPool::availableConnections(
    const stdx::unique_lock<stdx::mutex>& lk) {
    return _parent->_core->availableConnections(_hostAndPort);
}

size_t ConnectionPool::SpecificPool::refreshingConnections(
    const stdx::unique_lock<stdx::mutex>& lk) {
    return _parent->_core->refreshingConnections(_hostAndPort);
}

size_t ConnectionPool::SpecificPool::createdConnections(const stdx::unique_lock<stdx::mutex>& lk) {
    return _created;
}

size_t ConnectionPool::SpecificPool::openConnections(const stdx::unique_lock<stdx::mutex>& lk) {
    return _parent->_core->openConnections(_hostAndPort);
}

void ConnectionPool::SpecificPool::getConnection(const HostAndPort& hostAndPort,
                                                 Milliseconds timeout,
                                                 stdx::unique_lock<stdx::mutex> lk,
                                                 GetConnectionCallback cb) {
    if (timeout < Milliseconds(0) || timeout > _parent->_options.refreshTimeout) {
        timeout = _parent->_options.refreshTimeout;
    }

    const auto expiration = _parent->_factory->now() + timeout;

    auto r = new ConnectionPoolCore::Request();
    r->val.rq_expiration = expiration;
    r->val.rq_host = hostAndPort;
    r->val.rq_callback = std::move(cb);
    _parent->_core->addReq(r);

    updateStateInLock();

    spawnConnections(lk);
    fulfillRequests(lk);
}

void ConnectionPool::SpecificPool::returnConnection(ConnectionInterface* connPtr,
                                                    stdx::unique_lock<stdx::mutex> lk) {

    auto needsRefreshTP = connPtr->getLastUsed() + _parent->_options.refreshRequirement;

    auto handle = _parent->_core->handleForInterface(connPtr);
    auto conn = handle->val.conn_iface;

    updateStateInLock();

    // Users are required to call indicateSuccess() or indicateFailure() before allowing
    // a connection to be returned. Otherwise, we have entered an unknown state.
    invariant(conn->getStatus() != kConnectionStateUnknown);

    if (conn->getGeneration() != _generation) {
        // If the connection is from an older generation, just return.
        disposeConnection(_parent->_core.get(), handle);
        return;
    }

    if (!conn->getStatus().isOK()) {
        // TODO: alert via some callback if the host is bad
        log() << "Ending connection to host " << _hostAndPort << " due to bad connection status; "
              << openConnections(lk) << " connections to that host remain open";
        disposeConnection(_parent->_core.get(), handle);
        return;
    }

    auto now = _parent->_factory->now();
    if (needsRefreshTP <= now) {
        // If we need to refresh this connection

        if (_parent->_core->openConnections(_hostAndPort) >  _parent->_options.minConnections) {
            // If we already have minConnections, just let the connection lapse
            log() << "Ending idle connection to host " << _hostAndPort
                  << " because the pool meets constraints; " << openConnections(lk)
                  << " connections to that host remain open";
            disposeConnection(_parent->_core.get(), handle);
            return;
        }

        _parent->_core->changeState(handle, ConnectionPoolCore::PROCESSING);

        // Unlock in case refresh can occur immediately
        lk.unlock();
        connPtr->refresh(_parent->_options.refreshTimeout,
                         [this, conn, handle](ConnectionInterface* connPtr, Status status) {
                             connPtr->indicateUsed();

                             stdx::unique_lock<stdx::mutex> lk(_parent->_mutex);

                             // If the host and port were dropped, let this lapse
                             if (conn->getGeneration() != _generation) {
                                 disposeConnection(_parent->_core.get(), handle);
                                 spawnConnections(lk);
                                 return;
                             }

                             // If we're in shutdown, we don't need refreshed connections
                             if (_state == kInShutdown) {
                                 disposeConnection(_parent->_core.get(), handle);
                                 return;
                             }

                             // If the connection refreshed successfully, throw it back in the ready
                             // pool
                             if (status.isOK()) {
                                 addToReady(lk, handle);
                                 spawnConnections(lk);
                                 return;
                             }

                             // If we've exceeded the time limit, start a new connect, rather than
                             // failing all operations.  We do this because the various callers have
                             // their own time limit which is unrelated to our internal one.
                             if (status.code() == ErrorCodes::NetworkInterfaceExceededTimeLimit) {
                                 log() << "Pending connection to host " << _hostAndPort
                                       << " did not complete within the connection timeout,"
                                       << " retrying with a new connection;" << openConnections(lk)
                                       << " connections to that host remain open";
                                 disposeConnection(_parent->_core.get(), handle);
                                 spawnConnections(lk);
                                 return;
                             }

                             // Otherwise pass the failure on through
                             processFailure(status, std::move(lk));
                         });
        lk.lock();
    } else {
        // If it's fine as it is, just put it in the ready queue
        addToReady(lk, handle);
    }

    updateStateInLock();
}

// Adds a live connection to the ready pool
void ConnectionPool::SpecificPool::addToReady(stdx::unique_lock<stdx::mutex>& lk,
                                              ConnectionPoolCore::Connection* handle) {
    auto conn = handle->val.conn_iface;

    // This makes the connection the new most-recently-used connection.
    _parent->_core->changeState(handle, ConnectionPoolCore::READY);

    // Our strategy for refreshing connections is to check them out and
    // immediately check them back in (which kicks off the refresh logic in
    // returnConnection
    conn->setTimeout(_parent->_options.refreshRequirement, [this, handle, conn]() {
        stdx::unique_lock<stdx::mutex> lk(_parent->_mutex);

        if (handle->val.conn_state != ConnectionPoolCore::READY) {
            mycheck(handle->val.conn_state != ConnectionPoolCore::READY, "state is " << handle->val.conn_state << "; should be " << ConnectionPoolCore::READY);
            // We've already been checked out. We don't need to refresh
            // ourselves.
            return;
        }

        // If we're in shutdown, we don't need to refresh connections
        if (_state == kInShutdown) {
            disposeConnection(_parent->_core.get(), handle);
            return;
        }

        _parent->_core->changeState(handle, ConnectionPoolCore::CHECKED_OUT);

        conn->indicateSuccess();

        returnConnection(conn, std::move(lk));
    });

    fulfillRequests(lk);
}

// Drop connections and fail all requests
void ConnectionPool::SpecificPool::processFailure(const Status& status,
                                                  stdx::unique_lock<stdx::mutex> lk) {
    // Bump the generation so we don't reuse any pending or checked out
    // connections
    _generation++;

    // Log something helpful
    log() << "Dropping all pooled connections to " << _hostAndPort
          << " due to failed operation on a connection";

    // TODO: also delete them!
    _parent->_core->dropAllReadyOrProcessingConnections(_hostAndPort);

    std::vector<ConnectionPoolCore::Request*> toDel;
    _parent->_core->reqsForHost(_hostAndPort, [&](ConnectionPoolCore::Request* r) { toDel.push_back(r); });
    _parent->_core->clearReqs(_hostAndPort);

    // Update state to reflect the lack of requests
    updateStateInLock();

    // Drop the lock and process all of the requests
    // with the same failed status
    lk.unlock();

    for (auto* r : toDel) {
        r->val.rq_callback(status);
        delete r;
    }
}

// fulfills as many outstanding requests as possible
void ConnectionPool::SpecificPool::fulfillRequests(stdx::unique_lock<stdx::mutex>& lk) {
    // If some other thread (possibly this thread) is fulfilling requests,
    // don't keep padding the callstack.
    if (_inFulfillRequests)
        return;

    _inFulfillRequests = true;
    auto guard = MakeGuard([&] { _inFulfillRequests = false; });
    mycheck_eq(_parent->_core->openConnections(_hostAndPort), openConnections(lk), "fulfillRequests");

    while (_parent->_core->hasReq(_hostAndPort)) {
        auto req = _parent->_core->nextReq(_hostAndPort);
        auto handle = _parent->_core->mruConn(_hostAndPort);
        if (!handle) break;
        auto connPtr = handle->val.conn_iface;
        mycheck_eq(handle->val.conn_iface, connPtr, "fulfillRequests 3");

        // Cancel its timeout
        connPtr->cancelTimeout();

        if (!connPtr->isHealthy()) {
            log() << "dropping unhealthy pooled connection to " << connPtr->getHostAndPort();
            disposeConnection(_parent->_core.get(), handle);

            if (_parent->_core->availableConnections(_hostAndPort) == 0) {
                log() << "after drop, pool was empty, going to spawn some connections";
                // Spawn some more connections to the bad host if we're all out.
                spawnConnections(lk);
            }

            // Retry.
            continue;
        }

        // Grab the request and callback
        auto cb = std::move(req->val.rq_callback);
        _parent->_core->popReq(_hostAndPort);
        delete req;

        // check out the connection
        _parent->_core->changeState(handle, ConnectionPoolCore::CHECKED_OUT);

        updateStateInLock();

        // pass it to the user
        connPtr->resetToUnknown();
        lk.unlock();
        cb(ConnectionHandle(connPtr, ConnectionHandleDeleter(_parent)));
        lk.lock();
    }
    mycheck_eq(_parent->_core->openConnections(_hostAndPort), openConnections(lk), "fulfillRequests final");
}

// spawn enough connections to satisfy open requests and minpool, while
// honoring maxpool
void ConnectionPool::SpecificPool::spawnConnections(stdx::unique_lock<stdx::mutex>& lk) {
    // If some other thread (possibly this thread) is spawning connections,
    // don't keep padding the callstack.
    if (_inSpawnConnections)
        return;

    _inSpawnConnections = true;
    auto guard = MakeGuard([&] { _inSpawnConnections = false; });

    // We want minConnections <= outstanding requests <= maxConnections
    auto target = [&] {
        return std::max(
            _parent->_options.minConnections,
            std::min(
                (size_t)(_parent->_core->numReqs(_hostAndPort) + _parent->_core->inUseConnections(_hostAndPort)),
                _parent->_options.maxConnections));
    };

    // While all of our inflight connections are less than our target


    while ((_parent->_core->openConnections(_hostAndPort) < target()) &&
           (_parent->_core->refreshingConnections(_hostAndPort) < _parent->_options.maxConnecting)) {
        OwnedConnection handle;
        try {
            // make a new connection and put it in processing
            handle = _parent->_factory->makeConnection(_hostAndPort, _generation).release();
        } catch (std::system_error& e) {
            severe() << "Failed to construct a new connection object: " << e.what();
            // fassertFailed(40 336);
        }

        auto connPtr = handle;
        auto c = new ConnectionPoolCore::Connection();
        c->val.conn_state = ConnectionPoolCore::PROCESSING;
        c->val.conn_host  = _hostAndPort;
        c->val.conn_iface = connPtr;
        _parent->_core->addConn(c);

        ++_created;

        // Run the setup callback
        lk.unlock();
        connPtr->setup(
            _parent->_options.refreshTimeout, [this, c](ConnectionInterface* connPtr, Status status) {
                connPtr->indicateUsed();
                stdx::unique_lock<stdx::mutex> lk(_parent->_mutex);
                if (connPtr->getGeneration() != _generation) {
                    // If the host and port was dropped, let the
                    // connection lapse
                    disposeConnection(_parent->_core.get(), c);
                    spawnConnections(lk);
                } else if (status.isOK()) {
                    addToReady(lk, c);
                    spawnConnections(lk);
                } else if (status.code() == ErrorCodes::NetworkInterfaceExceededTimeLimit) {
                    disposeConnection(_parent->_core.get(), c);
                    // If we've exceeded the time limit, restart the connect, rather than
                    // failing all operations.  We do this because the various callers
                    // have their own time limit which is unrelated to our internal one.
                    spawnConnections(lk);
                } else {
                    // If the setup failed, cascade the failure edge
                    processFailure(status, std::move(lk));
                }
            });
        // Note that this assumes that the refreshTimeout is sound for the
        // setupTimeout

        lk.lock();
    }
}

// Called every second after hostTimeout until all processing connections reap
void ConnectionPool::SpecificPool::shutdown() {
    stdx::unique_lock<stdx::mutex> lk(_parent->_mutex);
    // mycheck_eq(_state, _parent->_core->hostState(_hostAndPort), "enter shutdown");

    // We're racing:
    //
    // Thread A (this thread)
    //   * Fired the shutdown timer
    //   * Came into shutdown() and blocked
    //
    // Thread B (some new consumer)
    //   * Requested a new connection
    //   * Beat thread A to the mutex
    //   * Cancelled timer (but thread A already made it in)
    //   * Set state to running
    //   * released the mutex
    //
    // So we end up in shutdown, but with kRunning.  If we're here we raced and
    // we should just bail.
    mycheck_eq(
        _state == kRunning,
        _parent->_core->hostState(_hostAndPort) == kRunning,
        "shutdown guard");
    if (_state == kRunning) {
        return;
    }

    _state = kInShutdown;

    // If we have processing connections, wait for them to finish or timeout
    // before shutdown
    if (_parent->_core->refreshingConnections(_hostAndPort)) {
        _requestTimer->setTimeout(Seconds(1), [this]() { shutdown(); });

        return;
    }

    invariant(!_parent->_core->hasReq(_hostAndPort));

    _parent->_core->shutdown(_hostAndPort);
    // _parent->_pools.erase(_hostAndPort);
}

// Updates our state and manages the request timer
void ConnectionPool::SpecificPool::updateStateInLock() {
    if (_parent->_core->hasReq(_hostAndPort)) {
        // We have some outstanding requests, we're live

        // If we were already running and the timer is the same as it was
        // before, nothing to do
        auto nextReqExpiration = _parent->_core->nextReq(_hostAndPort)->val.rq_expiration;
        if (_state == kRunning && _requestTimerExpiration == nextReqExpiration)
            return;

        _state = kRunning;

        _requestTimer->cancelTimeout();

        _requestTimerExpiration = nextReqExpiration;

        auto timeout = nextReqExpiration - _parent->_factory->now();

        // We set a timer for the most recent request, then invoke each timed
        // out request we couldn't service
        _requestTimer->setTimeout(timeout, [this]() {
            stdx::unique_lock<stdx::mutex> lk(_parent->_mutex);

            auto now = _parent->_factory->now();

            while (_parent->_core->hasReq(_hostAndPort)) {
                auto* x = _parent->_core->nextReq(_hostAndPort);

                if (x->val.rq_expiration <= now) {
                    auto cb = std::move(x->val.rq_callback);
                    _parent->_core->popReq(_hostAndPort);
                    delete x;

                    lk.unlock();
                    cb(Status(ErrorCodes::NetworkInterfaceExceededTimeLimit,
                              "Couldn't get a connection within the time limit"));
                    lk.lock();
                } else {
                    break;
                }
            }

            updateStateInLock();
        });
    } else if (_parent->_core->inUseConnections(_hostAndPort)) {
        // If we have no requests, but someone's using a connection, we just
        // hang around until the next request or a return

        _requestTimer->cancelTimeout();
        _state = kRunning;
        _requestTimerExpiration = _requestTimerExpiration.max();
    } else {
        // If we don't have any live requests and no one has checked out connections

        // If we used to be idle, just bail
        if (_state == kIdle)
            return;

        _state = kIdle;

        _requestTimer->cancelTimeout();

        _requestTimerExpiration = _parent->_factory->now() + _parent->_options.hostTimeout;

        auto timeout = _parent->_options.hostTimeout;

        // Set the shutdown timer
        _requestTimer->setTimeout(timeout, [this]() { shutdown(); });
    }
}
#endif

}  // namespace executor
}  // namespace mongo
