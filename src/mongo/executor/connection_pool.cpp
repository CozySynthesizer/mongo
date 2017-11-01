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
#define mycheck(cond, dbg) \
    if (!(cond)) { std::cerr << dbg << std::endl; invariant(false); }
#define mycheck_eq(x, y, dbg) \
    mycheck((x) == (y), '@' << (dbg) << " ... " << (#x) << " != " << (#y) << ": " << (x) << " vs " << (y))

// One interesting implementation note herein concerns how setup() and
// refresh() are invoked outside of the global lock, but setTimeout is not.
// This implementation detail simplifies mocks, allowing them to return
// synchronously sometimes, whereas having timeouts fire instantly adds little
// value. In practice, dumping the locks is always safe (because we restrict
// ourselves to operations over the connection).

namespace mongo {
namespace executor {

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
    using OwnedConnection = std::unique_ptr<ConnectionInterface>;
    using OwnershipPool = stdx::unordered_map<ConnectionInterface*, OwnedConnection>;
    using LRUOwnershipPool = LRUCache<OwnershipPool::key_type, OwnershipPool::mapped_type>;
    using Request = std::pair<Date_t, GetConnectionCallback>;
    struct RequestComparator {
        bool operator()(const Request& a, const Request& b) {
            return a.first > b.first;
        }
    };

    void addToReady(stdx::unique_lock<stdx::mutex>& lk, OwnedConnection conn);

    void fulfillRequests(stdx::unique_lock<stdx::mutex>& lk);

    void spawnConnections(stdx::unique_lock<stdx::mutex>& lk);

    void shutdown();

    template <typename OwnershipPoolType>
    typename OwnershipPoolType::mapped_type takeFromPool(
        OwnershipPoolType& pool, typename OwnershipPoolType::key_type connPtr);

    OwnedConnection takeFromProcessingPool(ConnectionInterface* connection);

    void updateStateInLock();

private:
    ConnectionPool* const _parent;

    const HostAndPort _hostAndPort;

    LRUOwnershipPool _readyPool;
    OwnershipPool _processingPool;
    OwnershipPool _droppedProcessingPool;
    OwnershipPool _checkedOutPool;

    std::priority_queue<Request, std::vector<Request>, RequestComparator> _requests;

    std::unique_ptr<TimerInterface> _requestTimer;
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
    enum class State {
        // The pool is active
        kRunning,

        // No current activity, waiting for hostTimeout to pass
        kIdle,

        // hostTimeout is passed, we're waiting for any processing
        // connections to finish before shutting down
        kInShutdown,
    };

    State _state;
};

constexpr Milliseconds ConnectionPool::kDefaultHostTimeout;
size_t const ConnectionPool::kDefaultMaxConns = std::numeric_limits<size_t>::max();
size_t const ConnectionPool::kDefaultMinConns = 1;
size_t const ConnectionPool::kDefaultMaxConnecting = std::numeric_limits<size_t>::max();
constexpr Milliseconds ConnectionPool::kDefaultRefreshRequirement;
constexpr Milliseconds ConnectionPool::kDefaultRefreshTimeout;

const Status ConnectionPool::kConnectionStateUnknown =
    Status(ErrorCodes::InternalError, "Connection is in an unknown state");

ConnectionPool::ConnectionPool(std::unique_ptr<DependentTypeFactoryInterface> impl,
                               std::string name,
                               Options options)
    : _core(new ConnectionPoolCore()), _name(std::move(name)), _options(std::move(options)), _factory(std::move(impl)) {
    _core->markActivity(Date_t::now());
}

ConnectionPool::~ConnectionPool() = default;

void ConnectionPool::dropConnections(const HostAndPort& hostAndPort) {
    stdx::unique_lock<stdx::mutex> lk(_mutex);

    auto iter = _pools.find(hostAndPort);

    if (iter == _pools.end())
        return;

    iter->second.get()->processFailure(
        Status(ErrorCodes::PooledConnectionsDropped, "Pooled connections dropped"), std::move(lk));
}

void ConnectionPool::get(const HostAndPort& hostAndPort,
                         Milliseconds timeout,
                         GetConnectionCallback cb) {
    SpecificPool* pool;

    stdx::unique_lock<stdx::mutex> lk(_mutex);

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
    size_t ret = 0;
    auto iter = _pools.find(hostAndPort);
    if (iter != _pools.end()) {
        ret = iter->second->openConnections(lk);
    }

    mycheck_eq(_core->openConnections(hostAndPort), ret, "getNumConnectionsPerHost");
    return _core->openConnections(hostAndPort);
}

void ConnectionPool::returnConnection(ConnectionInterface* conn) {
    stdx::unique_lock<stdx::mutex> lk(_mutex);

    auto iter = _pools.find(conn->getHostAndPort());

    invariant(iter != _pools.end());

    iter->second.get()->returnConnection(conn, std::move(lk));
}

ConnectionPool::SpecificPool::SpecificPool(ConnectionPool* parent, const HostAndPort& hostAndPort)
    : _parent(parent),
      _hostAndPort(hostAndPort),
      _readyPool(std::numeric_limits<size_t>::max()),
      _requestTimer(parent->_factory->makeTimer()),
      _generation(0),
      _inFulfillRequests(false),
      _inSpawnConnections(false),
      _created(0),
      _state(State::kRunning) {}

ConnectionPool::SpecificPool::~SpecificPool() {
    DESTRUCTOR_GUARD(_requestTimer->cancelTimeout();)
}

size_t ConnectionPool::SpecificPool::inUseConnections(const stdx::unique_lock<stdx::mutex>& lk) {
    return _checkedOutPool.size();
}

size_t ConnectionPool::SpecificPool::availableConnections(
    const stdx::unique_lock<stdx::mutex>& lk) {
    return _readyPool.size();
}

size_t ConnectionPool::SpecificPool::refreshingConnections(
    const stdx::unique_lock<stdx::mutex>& lk) {
    return _processingPool.size();
}

size_t ConnectionPool::SpecificPool::createdConnections(const stdx::unique_lock<stdx::mutex>& lk) {
    return _created;
}

size_t ConnectionPool::SpecificPool::openConnections(const stdx::unique_lock<stdx::mutex>& lk) {
    return _checkedOutPool.size() + _readyPool.size() + _processingPool.size();
}

void ConnectionPool::SpecificPool::getConnection(const HostAndPort& hostAndPort,
                                                 Milliseconds timeout,
                                                 stdx::unique_lock<stdx::mutex> lk,
                                                 GetConnectionCallback cb) {
    if (timeout < Milliseconds(0) || timeout > _parent->_options.refreshTimeout) {
        timeout = _parent->_options.refreshTimeout;
    }

    const auto expiration = _parent->_factory->now() + timeout;

    _requests.push(make_pair(expiration, std::move(cb)));

    updateStateInLock();

    spawnConnections(lk);
    fulfillRequests(lk);
}

template <class F>
class AtBlockExit {
    F callback;
public:
    explicit AtBlockExit(F f) : callback(std::move(f)) { }
    ~AtBlockExit() { callback(); }
};

template <class F>
AtBlockExit<F> atBlockExit(F f) { return AtBlockExit<F>(std::move(f)); }

void ConnectionPool::SpecificPool::returnConnection(ConnectionInterface* connPtr,
                                                    stdx::unique_lock<stdx::mutex> lk) {
    auto needsRefreshTP = connPtr->getLastUsed() + _parent->_options.refreshRequirement;

    auto conn = takeFromPool(_checkedOutPool, connPtr);
    auto handle = _parent->_core->handleForInterface(connPtr);

    updateStateInLock();

    // Users are required to call indicateSuccess() or indicateFailure() before allowing
    // a connection to be returned. Otherwise, we have entered an unknown state.
    invariant(conn->getStatus() != kConnectionStateUnknown);

    if (conn->getGeneration() != _generation) {
        // If the connection is from an older generation, just return.
        _parent->_core->rmConn(handle);
        return;
    }

    if (!conn->getStatus().isOK()) {
        // TODO: alert via some callback if the host is bad
        log() << "Ending connection to host " << _hostAndPort << " due to bad connection status; "
              << openConnections(lk) << " connections to that host remain open";
        _parent->_core->rmConn(handle);
        return;
    }

    auto now = _parent->_factory->now();
    if (needsRefreshTP <= now) {
        // If we need to refresh this connection

        if (_readyPool.size() + _processingPool.size() + _checkedOutPool.size() >=
            _parent->_options.minConnections) {
            // If we already have minConnections, just let the connection lapse
            log() << "Ending idle connection to host " << _hostAndPort
                  << " because the pool meets constraints; " << openConnections(lk)
                  << " connections to that host remain open";
            _parent->_core->rmConn(handle);
            return;
        }

        _processingPool[connPtr] = std::move(conn);
        _parent->_core->changeState(handle, ConnectionPoolCore::PROCESSING);

        // Unlock in case refresh can occur immediately
        lk.unlock();
        connPtr->refresh(_parent->_options.refreshTimeout,
                         [this, handle](ConnectionInterface* connPtr, Status status) {
                             cerr << "REFRESH CALLBACK: " << connPtr << endl;
                             connPtr->indicateUsed();

                             stdx::unique_lock<stdx::mutex> lk(_parent->_mutex);

                             mycheck_eq(_parent->_core->openConnections(_hostAndPort), openConnections(lk), "pre-refresh");
                             auto _be = atBlockExit([this, &lk]() {
                                mycheck_eq(_parent->_core->openConnections(_hostAndPort), openConnections(lk), "post-refresh");
                             });

                             auto conn = takeFromProcessingPool(connPtr);

                             // If the host and port were dropped, let this lapse
                             if (conn->getGeneration() != _generation) {
                                 _parent->_core->rmConn(handle);
                                 spawnConnections(lk);
                                 return;
                             }

                             // If we're in shutdown, we don't need refreshed connections
                             if (_state == State::kInShutdown) {
                                 _parent->_core->rmConn(handle);
                                 return;
                             }

                             // If the connection refreshed successfully, throw it back in the ready
                             // pool
                             if (status.isOK()) {
                                 addToReady(lk, std::move(conn));
                                 _parent->_core->changeState(handle, ConnectionPoolCore::READY);
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
                                 _parent->_core->rmConn(handle);
                                 spawnConnections(lk);
                                 return;
                             }

                             // Otherwise pass the failure on through
                             processFailure(status, std::move(lk));
                         });
        lk.lock();
    } else {
        // If it's fine as it is, just put it in the ready queue
        addToReady(lk, std::move(conn));
        _parent->_core->changeState(handle, ConnectionPoolCore::READY);
    }

    updateStateInLock();
}

// Adds a live connection to the ready pool
void ConnectionPool::SpecificPool::addToReady(stdx::unique_lock<stdx::mutex>& lk,
                                              OwnedConnection conn) {
    auto connPtr = conn.get();

    // This makes the connection the new most-recently-used connection.
    _readyPool.add(connPtr, std::move(conn));
    _parent->_core->changeState(
        _parent->_core->handleForInterface(connPtr),
        ConnectionPoolCore::READY);

    // Our strategy for refreshing connections is to check them out and
    // immediately check them back in (which kicks off the refresh logic in
    // returnConnection
    connPtr->setTimeout(_parent->_options.refreshRequirement, [this, connPtr]() {
        OwnedConnection conn;

        stdx::unique_lock<stdx::mutex> lk(_parent->_mutex);
        auto handle = _parent->_core->handleForInterface(connPtr);

        if (!_readyPool.count(connPtr)) {
            mycheck(handle->val.conn_state != ConnectionPoolCore::READY, "state is " << handle->val.conn_state << "; should be " << ConnectionPoolCore::READY);
            // We've already been checked out. We don't need to refresh
            // ourselves.
            return;
        }

        conn = takeFromPool(_readyPool, connPtr);

        // If we're in shutdown, we don't need to refresh connections
        if (_state == State::kInShutdown) {
            _parent->_core->rmConn(handle);
            return;
        }

        _checkedOutPool[connPtr] = std::move(conn);
        _parent->_core->changeState(handle, ConnectionPoolCore::CHECKED_OUT);

        connPtr->indicateSuccess();

        returnConnection(connPtr, std::move(lk));
    });

    fulfillRequests(lk);
}

// Drop connections and fail all requests
void ConnectionPool::SpecificPool::processFailure(const Status& status,
                                                  stdx::unique_lock<stdx::mutex> lk) {
    // Bump the generation so we don't reuse any pending or checked out
    // connections
    _generation++;

    // Drop ready connections
    mycheck_eq(_readyPool.size(), _parent->_core->availableConnections(_hostAndPort), "processFailure 1");
    _readyPool.clear();

    // Log something helpful
    log() << "Dropping all pooled connections to " << _hostAndPort
          << " due to failed operation on a connection";

    // Migrate processing connections to the dropped pool
    for (auto&& x : _processingPool) {
        _droppedProcessingPool[x.first] = std::move(x.second);
    }
    mycheck_eq(_processingPool.size(), _parent->_core->refreshingConnections(_hostAndPort), "processFailure 2");
    _processingPool.clear();
    _parent->_core->dropAllReadyOrProcessingConnections(_hostAndPort);
    mycheck_eq(_readyPool.size(), _parent->_core->availableConnections(_hostAndPort), "processFailure 3");
    mycheck_eq(_processingPool.size(), _parent->_core->refreshingConnections(_hostAndPort), "processFailure 4");

    // Move the requests out so they aren't visible
    // in other threads
    decltype(_requests) requestsToFail;
    {
        using std::swap;
        swap(requestsToFail, _requests);
    }

    // Update state to reflect the lack of requests
    updateStateInLock();

    // Drop the lock and process all of the requests
    // with the same failed status
    lk.unlock();

    while (requestsToFail.size()) {
        requestsToFail.top().second(status);
        requestsToFail.pop();
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

    while (_requests.size()) {
        // _readyPool is an LRUCache, so its begin() object is the MRU item.
        auto iter = _readyPool.begin();

        if (iter == _readyPool.end())
            break;

        // Grab the connection and cancel its timeout
        auto conn = std::move(iter->second);
        auto connPtr = conn.get();
        auto handle = _parent->_core->handleForInterface(connPtr);
        _readyPool.erase(iter);
        conn->cancelTimeout();

        if (!conn->isHealthy()) {
            log() << "dropping unhealthy pooled connection to " << conn->getHostAndPort();
            _parent->_core->rmConn(handle);

            if (_readyPool.empty()) {
                log() << "after drop, pool was empty, going to spawn some connections";
                // Spawn some more connections to the bad host if we're all out.
                spawnConnections(lk);
            }

            // Drop the bad connection.
            conn.reset();
            // Retry.
            continue;
        }

        // Grab the request and callback
        auto cb = std::move(_requests.top().second);
        _requests.pop();


        // check out the connection
        _checkedOutPool[connPtr] = std::move(conn);
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
            std::min(_requests.size() + _checkedOutPool.size(), _parent->_options.maxConnections));
    };

    // While all of our inflight connections are less than our target


    while ((_readyPool.size() + _processingPool.size() + _checkedOutPool.size() < target()) &&
           (_processingPool.size() < _parent->_options.maxConnecting)) {
        mycheck_eq(_parent->_core->openConnections(_hostAndPort), _readyPool.size() + _processingPool.size() + _checkedOutPool.size(), "spawnConnections 1");
        mycheck_eq(_parent->_core->refreshingConnections(_hostAndPort), _processingPool.size(), "spawnConnections 2");

        std::unique_ptr<ConnectionPool::ConnectionInterface> handle;
        try {
            // make a new connection and put it in processing
            handle = _parent->_factory->makeConnection(_hostAndPort, _generation);
        } catch (std::system_error& e) {
            severe() << "Failed to construct a new connection object: " << e.what();
            fassertFailed(40336);
        }

        auto connPtr = handle.get();
        _processingPool[connPtr] = std::move(handle);

        auto c = new ConnectionPoolCore::Connection();
        c->val.conn_state = ConnectionPoolCore::PROCESSING;
        c->val.conn_host  = _hostAndPort;
        c->val.conn_iface = connPtr;
        _parent->_core->addConn(c);

        ++_created;

        // Run the setup callback
        lk.unlock();
        connPtr->setup(
            _parent->_options.refreshTimeout, [this](ConnectionInterface* connPtr, Status status) {
                connPtr->indicateUsed();

                stdx::unique_lock<stdx::mutex> lk(_parent->_mutex);

                cerr << "SETUP CALLBACK: " << connPtr << endl;

                mycheck_eq(_parent->_core->openConnections(_hostAndPort), openConnections(lk), "pre-setup");
                auto _be = atBlockExit([this, connPtr, &lk]() {
                    cerr << "SETUP CALLBACK COMPLETED FOR " << connPtr << endl;
                    mycheck_eq(_parent->_core->openConnections(_hostAndPort), openConnections(lk), "post-setup");
                });

                auto conn = takeFromProcessingPool(connPtr);

                auto handle = _parent->_core->handleForInterface(connPtr);
                if (conn->getGeneration() != _generation) {
                    // If the host and port was dropped, let the
                    // connection lapse
                    _parent->_core->rmConn(handle);
                    spawnConnections(lk);
                } else if (status.isOK()) {
                    addToReady(lk, std::move(conn));
                    spawnConnections(lk);
                } else if (status.code() == ErrorCodes::NetworkInterfaceExceededTimeLimit) {
                    _parent->_core->rmConn(handle);
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
    if (_state == State::kRunning) {
        cerr << "ABORTING SHUTDOWN (still running)" << endl;
        return;
    }

    cerr << "SHUTTING DOWN" << endl;
    _state = State::kInShutdown;

    // If we have processing connections, wait for them to finish or timeout
    // before shutdown
    if (_processingPool.size() || _droppedProcessingPool.size()) {
        cerr << "SHUTDOWN DELAYED..." << endl;
        _requestTimer->setTimeout(Seconds(1), [this]() { shutdown(); });

        return;
    }

    invariant(_requests.empty());
    invariant(_checkedOutPool.empty());

    _parent->_core->shutdown(_hostAndPort);
    _parent->_pools.erase(_hostAndPort);
    cerr << "SHUTDOWN COMPLETE" << endl;
}

template <typename OwnershipPoolType>
typename OwnershipPoolType::mapped_type ConnectionPool::SpecificPool::takeFromPool(
    OwnershipPoolType& pool, typename OwnershipPoolType::key_type connPtr) {
    auto iter = pool.find(connPtr);
    invariant(iter != pool.end());

    auto conn = std::move(iter->second);
    pool.erase(iter);
    return conn;
}

ConnectionPool::SpecificPool::OwnedConnection ConnectionPool::SpecificPool::takeFromProcessingPool(
    ConnectionInterface* connPtr) {
    if (_processingPool.count(connPtr))
        return takeFromPool(_processingPool, connPtr);

    return takeFromPool(_droppedProcessingPool, connPtr);
}


// Updates our state and manages the request timer
void ConnectionPool::SpecificPool::updateStateInLock() {
    if (_requests.size()) {
        // We have some outstanding requests, we're live

        // If we were already running and the timer is the same as it was
        // before, nothing to do
        if (_state == State::kRunning && _requestTimerExpiration == _requests.top().first)
            return;

        cerr << "UPDATE STATE: kRunning (requests)" << endl;
        _state = State::kRunning;

        _requestTimer->cancelTimeout();

        _requestTimerExpiration = _requests.top().first;

        auto timeout = _requests.top().first - _parent->_factory->now();

        // We set a timer for the most recent request, then invoke each timed
        // out request we couldn't service
        _requestTimer->setTimeout(timeout, [this]() {
            stdx::unique_lock<stdx::mutex> lk(_parent->_mutex);

            auto now = _parent->_factory->now();

            while (_requests.size()) {
                auto& x = _requests.top();

                if (x.first <= now) {
                    auto cb = std::move(x.second);
                    _requests.pop();

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
    } else if (_checkedOutPool.size()) {
        // If we have no requests, but someone's using a connection, we just
        // hang around until the next request or a return

        _requestTimer->cancelTimeout();
        cerr << "UPDATE STATE: kRunning (checkedout)" << endl;
        _state = State::kRunning;
        _requestTimerExpiration = _requestTimerExpiration.max();
    } else {
        // If we don't have any live requests and no one has checked out connections

        // If we used to be idle, just bail
        if (_state == State::kIdle)
            return;

        cerr << "UPDATE STATE: kIdle (eh)" << endl;
        _state = State::kIdle;

        _requestTimer->cancelTimeout();

        _requestTimerExpiration = _parent->_factory->now() + _parent->_options.hostTimeout;

        auto timeout = _parent->_options.hostTimeout;

        // Set the shutdown timer
        _requestTimer->setTimeout(timeout, [this]() { shutdown(); });
    }
}

}  // namespace executor
}  // namespace mongo
