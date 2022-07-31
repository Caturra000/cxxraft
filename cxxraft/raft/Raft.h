#pragma once
#include <random>
#include <memory>
#include <cstdlib>
#include <tuple>
#include <vector>
#include <optional>
#include "co.hpp"
#include "trpc.hpp"
#include "dlog.hpp"
#include "raft/Reply.h"
#include "raft/Storage.h"
#include "raft/Peer.h"
#include "raft/Worker.h"
#include "raft/Debugger.h"
namespace cxxraft {

struct Config;

class Raft: public std::enable_shared_from_this<Raft>,
            private Debugger<Raft> {

// public API
public:

    void start();

    static std::shared_ptr<Raft> make(Config &config, int id);

    // Ask a Raft for its current term, and whether it thinks it is leader
    // return: [term, isLeader] <std::tuple<int, bool>>
    //
    // (Note: not state machine `raft::state`)
    auto getState();

public:

    Raft(const Raft&) = delete;
    Raft(Raft&&) = default;
    Raft& operator=(const Raft&) = delete;
    Raft& operator=(Raft&&) = default;
    ~Raft() = default;

// public for std::make_shared
public:

    Raft(Config &config, int id);

public:

    // Abstract raft state (leader / follower / candidate)
    struct State;

private:

// RPC
// NOTE:
//
// If a server receives a request with a stale term number, it rejects the request (§5.1)
//
// @fail
// Servers retry RPCs if they do not receive a response in a timely manner
//
// TODO:
// And they issue RPCs in parallel for best performance
// (but we use coroutines in a single thread, which is not parallel but concurrent)
private:

    // Invoked by leader to replicate log entries (§5.3);
    // also used as heartbeat (§5.2).
    // return: [term, success]
    Reply<int, bool> appendEntryRPC(int term, int leaderId, int prevLogIndex, int prevLogTerm /*, null log*/);

    // Invoked by candidates to gather votes (§5.2).
    // return: [term, voteGranted]
    Reply<int, bool> requestVoteRPC(int term, int candidateId /*...*/);

private:

    // See below
    template <typename NextState>
    void become();

    // @action
    // Leaders send periodic heartbeats (AppendEntriesRPCs that carry no log entries)
    // to all followers in order to maintain their authority.
    void becomeLeader();

    // (§5.2)
    // @when
    // When servers start up, they begin as followers
    //
    // @until
    // A server remains in follower state as long as it receives valid
    //
    // @action
    // If a follower receives no communication over a period of time
    // called the election timeout, then it assumes there is no viable
    // leader and begins an election to choose a new leader.
    void becomeFollower();

    void becomeCandidate();

// perform can be called by state machine only
// all perform functions should be executed endless
private:

    void performElection();

    // Waiting for any RPC response
    // If failed, follower will be converted to candidate
    void performKeepAlive(std::shared_ptr<size_t> watchdog);

    void performHeartBeat();

private:

    // Least majority of fixed-size cluster
    size_t majority();

    // used in performElection()
    // return: voted
    std::shared_ptr<int> prepareElection();

    void maintainAuthority();

    size_t getTransaction();

    bool isValidTransaction(size_t transaction);

public:

    constexpr static const char *RAFT_APPEND_ENTRY_RPC
        {"appendEntryRPC"};

    constexpr static const char *RAFT_REQUEST_VOTE_RPC
        {"requestVoteRPC"};

    using AppendEntryReply = Reply<int, bool>;
    using RequestVoteReply = Reply<int, bool>;

    // @paper
    //
    // Raft uses randomized election timeouts to ensure that
    // split votes are rare and that they are resolved quickly. To
    // prevent split votes in the first place, election timeouts are
    // chosen randomly from a fixed interval (e.g., 150–300ms)
    //
    // @refs
    //
    // MIT-6.824 recommends that 1 second is better
    constexpr static auto RAFT_ELECTION_TIMEOUT
        { std::chrono::milliseconds(300) };

    constexpr static auto RAFT_ELECTION_TIMEOUT_MIN
        { std::chrono::milliseconds(150) };

    constexpr static auto RAFT_ELECTION_TIMEOUT_MAX
        { std::chrono::milliseconds(300) };

    // Same as etcd-raft heartbeat interval
    // (less than RAFT_ELECTION_TIMEOUT_MIN)
    constexpr static auto RAFT_HEARTBEAT_INTERVAL
        { std::chrono::milliseconds(100) };

    using Bitmask = uint64_t;

    // See papaer Figure 4
    // It shows the states and transitions

    // (§5.1)
    //
    // In normal operation there is exactly
    // one leader and all of the other servers are followers.
    //
    // Raft ensures that there is at most one leader in a given term.
    //
    // The leader handles all client requests (if
    // a client contacts a follower the follower redirects it to the leader)
    constexpr static Bitmask FLAGS_LEADER     = 1ULL << 1;

    // When this raft server receives a term greater than _currentTerm,
    // it invokes `becomeFollower()`
    //
    // (§5.1)
    // If a candidate or leader discovers that its term is out of date,
    // it immediately reverts to follower state.
    constexpr static Bitmask FLAGS_FOLLOWER   = 1ULL << 2;

    // used to elect a new leader
    //
    // (§5.2)
    //
    // @until
    // A candidate continues in this state until one of three things happens:
    // (a) it wins the election,
    // (b) another server establishes itself as leader, or
    // (c) a period of time goes by with no winner.
    constexpr static Bitmask FLAGS_CANDIDATE  = 1ULL << 3;

private:

    trpc::Endpoint _self;
    std::optional<trpc::Server> _rpcServer;

    // raft peers, NOT include this one
    std::vector<Peer> _peers;

    // node id for RPC message
    int _id;

    // Log or configuration
    Storage _storage;

    // latest term server has seen (initialized to 0
    // on first boot, increases monotonically)
    //
    // @paper
    //
    // Each server stores a current term number,
    // which increases monotonically over time
    //
    // Terms act as a logical clock in Raft,
    // and they allow servers to detect obsolete
    // information such as stale leaders.
    //
    // Current terms are exchanged whenever servers communicate
    int _currentTerm;

    std::optional<int> _voteFor;

    // TODO log

    // TODO
    // int _commitIndex {};

    // // TODO
    // int _lastApplied {};

    // Finite State Machine
    std::shared_ptr<State> _fsm;

    // A coroutine where FSM runs on
    Worker _transitioner;

    // A transaction ID for state machine
    // When invokes become...(), _transaction will add one
    // and pending coroutines will know themselves are outdated and then abort
    //
    // _transaction will be checked in every context switch
    //
    // _transaction and _currentTerm are not strictly related
    size_t _transaction;

private:

    friend struct Config;
    friend struct Leader;
    friend struct Follower;
    friend struct Candidate;

    friend struct Debugger<Raft>;
};






////////////////////////////////// State Machine //////////////////////////////////






struct Raft::State {

public:

    // Tag class
    // Return string literal
    struct Literal {};

    State(Raft *master)
        : _master{master},
          _flags{},
          _transaction(master->getTransaction())
    {}

    virtual Raft::Bitmask type() { return 0; };
    virtual const char*   type(Literal) { return "unknown"; }
    virtual Raft::Bitmask flags() { return _flags; }
    virtual const char*   flags(Literal) { return "TODO"; }

    bool isValidTransaction() { return _master->isValidTransaction(_transaction); }

    // it runs on the `_transitioner` coroutine
    virtual void onBecome(std::shared_ptr<Raft::State> previous) = 0;

    // runs on a RPC server coroutine
    virtual Reply<int, bool> onReceiveAppendEntryRPC(int term, int leaderId, int prevLogIndex, int prevLogTerm) = 0;

    // runs on a RPC server coroutine
    virtual Reply<int, bool> onReceiveRequestVoteRPC(int term, int candidateId) = 0;

protected:
    Raft *_master;
    Raft::Bitmask _flags;
    size_t _transaction;

};

struct Leader: public Raft::State {

    Leader(Raft *master);

    Raft::Bitmask type() override { return Raft::FLAGS_LEADER; }
    const char* type(Raft::State::Literal) override { return "leader"; }

    void onBecome(std::shared_ptr<Raft::State> previous) override;

    Reply<int, bool> onReceiveAppendEntryRPC(int term, int leaderId, int prevLogIndex, int prevLogTerm) override;
    Reply<int, bool> onReceiveRequestVoteRPC(int term, int candidateId) override;

};

struct Follower: public Raft::State {

    Follower(Raft *master);

    Raft::Bitmask type() override { return Raft::FLAGS_FOLLOWER; }
    const char* type(Raft::State::Literal) override { return "follower"; }

    // running on a transitioner coroutine (1)
    void onBecome(std::shared_ptr<Raft::State> previous) override;

    // running on a RPC coroutine (0)
    Reply<int, bool> onReceiveAppendEntryRPC(int term, int leaderId, int prevLogIndex, int prevLogTerm) override;

    Reply<int, bool> onReceiveRequestVoteRPC(int term, int candidateId) override;

private:
    std::shared_ptr<size_t> _watchdog;
};

struct Candidate: public Raft::State {

    Candidate(Raft *master);

    Raft::Bitmask type() override { return Raft::FLAGS_CANDIDATE; }
    const char* type(Raft::State::Literal) override { return "candidate"; }

    void onBecome(std::shared_ptr<Raft::State> previous) override;

    Reply<int, bool> onReceiveAppendEntryRPC(int term, int leaderId, int prevLogIndex, int prevLogTerm) override;

    Reply<int, bool> onReceiveRequestVoteRPC(int term, int candidateId) override;
};






////////////////////////////////// Config and Test //////////////////////////////////






struct Config {

    Config(std::vector<trpc::Endpoint> peers);
    static std::shared_ptr<Config> make(std::vector<trpc::Endpoint> peers);

    // connect or disconnect to local debug network
    // direct call to raft server
    void connect(int index);
    void disconnect(int index);


    // override version of ::abort()
    // will crash later than asynchronous logging operation
    void abort();



//////////// for 2A

    // Check that there's exactly one leader.
    // Try a few times in case re-elections are needed.
    int checkOneLeader();

    // Check that there's no leader
    void checkNoLeader();

    // Check that everyone agrees on the term.
    int checkTerms();

    void begin() { std::cout << "begin" << std::endl; }
    void end() { std::cout << "done" << std::endl;}


    // Whether each server is on the net
    std::vector<char> _connected {};

    std::vector<trpc::Endpoint> _peers;

    std::vector<std::shared_ptr<Raft>> _rafts;

};






////////////////////////////////// Implementation //////////////////////////////////






inline Raft::Raft(Config &config, int id)
    : _self(config._peers[id]),
      _id(id),
      _currentTerm(0),
      _fsm(nullptr),
      _transaction(0)
{
    for(size_t i = 0; i < config._peers.size(); i++) {
        if(i != id) _peers.emplace_back(config._peers[i]);
    }

}

inline void Raft::start() {
    auto &env = co::open();

    _rpcServer = trpc::Server::make(_self);

    if(!_rpcServer) {
        CXXRAFT_LOG_WARN(simpleInfo(), "server start failed.");
        return;
    }

    CXXRAFT_LOG_INFO(simpleInfo(), "rpc start.");

    // NOTE: `this` pointer will be moved in move semantic
    // you should construct raft object by make()

    _rpcServer->bind(RAFT_APPEND_ENTRY_RPC, [this](int term, int leaderId, int prevLogIndex, int prevLogTerm) {
        return this->appendEntryRPC(term, leaderId, prevLogIndex, prevLogTerm);
    });

    _rpcServer->bind(RAFT_REQUEST_VOTE_RPC, [this](int term, int candidateId) {
        return this->requestVoteRPC(term, candidateId);
    });

    // TODO
    // add unreliable network

    // coroutine0: RPC reply
    env.createCoroutine([this] {
        _rpcServer->start();
    })->resume();

    // coroutine1: state machine transitions
    _transitioner.post([this] {
        // preivous state == nullptr
        becomeFollower();
    });

    // client coroutines will start in further state
}

inline std::shared_ptr<Raft> Raft::make(Config &config, int id) {
    auto raft = std::make_shared<Raft>(config, id);
    // for test
    config._rafts.emplace_back(raft);
    config._connected[id] = true;
    return raft;
}

inline auto Raft::getState() {
    return std::make_tuple(_currentTerm, bool(_fsm->type() == FLAGS_LEADER));
}

inline Reply<int, bool> Raft::appendEntryRPC(int term, int leaderId, int prevLogIndex, int prevLogTerm) {
    return _fsm->onReceiveAppendEntryRPC(term, leaderId, prevLogIndex, prevLogTerm);
}

inline Reply<int, bool> Raft::requestVoteRPC(int term, int candidateId /*...*/) {

    return _fsm->onReceiveRequestVoteRPC(term, candidateId);
}

template <typename NextState>
inline void Raft::become() {
    auto previous = std::move(_fsm);
    // abort all the old transactions / pending coroutine jobs requested by `previous`
    // (or `previous` of `previous`...)
    _transaction++;
    _fsm = std::make_shared<NextState>(this);
    _fsm->onBecome(std::move(previous));
}

inline void Raft::becomeLeader() {
    CXXRAFT_LOG_DEBUG(simpleInfo(), "become leader");
    become<Leader>();
}

inline void Raft::becomeFollower() {
    CXXRAFT_LOG_DEBUG(simpleInfo(), "become follower");
    become<Follower>();
}

inline void Raft::becomeCandidate() {
    CXXRAFT_LOG_DEBUG(simpleInfo(), "become candidate");
    become<Candidate>();
}

inline void Raft::performElection() {

    const auto transaction = getTransaction();

    CXXRAFT_LOG_DEBUG(simpleInfo(), "perform election");
    std::random_device rd;
    std::mt19937 engine(rd());
    using ToMicro = std::chrono::duration<useconds_t, std::micro>;
    std::uniform_int_distribution<> dist(
        ToMicro{Raft::RAFT_ELECTION_TIMEOUT_MIN}.count(),
        ToMicro{Raft::RAFT_ELECTION_TIMEOUT_MAX}.count()
    );


    // @paper
    //
    // In some situations an election will result in a split vote.
    // In this case the term will end with no leader;
    // a new term (with a new election) will begin shortly.
    while(isValidTransaction(transaction)) {
        auto voting = prepareElection();
        // if failed, wait for a while
        co::usleep(dist(engine));
        int voted = *voting;
        // abort!
        *voting = -1;
        if(voted >= majority()) {
            break;
        }
    }

    if(!isValidTransaction(transaction)) {
        return;
    }

    CXXRAFT_LOG_DEBUG(simpleInfo(), "post: candidate -> leader");

    // See performKeepAlive
    _transitioner.post([this] {
        becomeLeader();
    });
}

inline void Raft::performKeepAlive(std::shared_ptr<size_t> watchdog) {
    const auto transaction = getTransaction();
    CXXRAFT_LOG_DEBUG(simpleInfo(), "perform keepalive, node id:");
    while(isValidTransaction(transaction)) {
        size_t old = *watchdog;
        co::usleep(std::chrono::duration<useconds_t, std::micro>(
            Raft::RAFT_ELECTION_TIMEOUT_MAX).count());
        // no RPC response
        if(old == *watchdog) {
            CXXRAFT_LOG_DEBUG(simpleInfo(), "no RPC response");
            break;
        }
        // else keep follower
    }

    if(!isValidTransaction(transaction)) {
        return;
    }

    CXXRAFT_LOG_DEBUG(simpleInfo(), "post: follower -> candidate");
    // waiting RPC for a long time
    _transitioner.post([this] {
        // We cannot call this function without post()
        // Because state transitions are implicit recursive
        // They will raise stackoverflow
        becomeCandidate();
    });
}

inline void Raft::performHeartBeat() {
    const auto transaction = getTransaction();
    CXXRAFT_LOG_DEBUG(simpleInfo(), "perform heart beat");
    // This routine will be aborted
    // when RPC coroutines receive a return state operation
    while(isValidTransaction(transaction)) {
        maintainAuthority();
        co::usleep(std::chrono::duration<useconds_t, std::micro>(
            RAFT_HEARTBEAT_INTERVAL).count());
    }
}

inline size_t Raft::majority() {
    return (1 + _peers.size()) / 2 + 1;
}

inline std::shared_ptr<int> Raft::prepareElection() {

    const auto transaction = getTransaction();

    // @paper
    //
    // To begin an election, a follower increments its current
    // term and transitions to candidate state
    _currentTerm++;
    CXXRAFT_LOG_DEBUG(simpleInfo(), "begin electionm, add term to:", _currentTerm);

    // @paper
    //
    // It then votes for itself and
    // issues RequestVote RPCs in parallel to each of
    // the other servers in the cluster.

    // (voted == -1) means aborted
    // 1: vote for itself
    auto voted = std::make_shared<int>(1);

    auto gatherVote = [this, voted, transaction](int index) {

        if(!isValidTransaction(transaction)) {
            return;
        }

        auto &peer = _peers[index];

        // clean up pending jobs
        peer.executor.strike();

        // abort or optimize
        if(auto v = *voted; v == -1 || v >= majority()) {
            return;
        }


        // Try to connect to a raft node
        // (Although it is a long connection, it may crash before)
        //
        // It may be unavailable and make() / connect() spends a lot of time
        // But we can run this `gatherVote` on many client coroutines
        // While `prepareElection` runs on fsm coroutine
        bool reconnect = !peer.client || (peer.client && peer.client->fd() < 0);

        // what happened?
        if(reconnect && !(peer.client = trpc::Client::make(peer.endpoint))) {
            return;
        }

        if(!isValidTransaction(transaction)) {
            return;
        }

        auto reply = peer.client->call<RequestVoteReply>(RAFT_REQUEST_VOTE_RPC, _currentTerm, _id);

        if(!isValidTransaction(transaction)) {
            return;
        }

        // check again after network IO
        if(auto v = *voted; v == -1 || v >= majority()) {
            return;
        }

        if(!reply) {
            // TODO print endpoint
            CXXRAFT_LOG_DEBUG(simpleInfo(), "no reply in client");
            return;
        }

        auto [term, voteGranted] = reply->cast();

        if(!voteGranted) {
            CXXRAFT_LOG_DEBUG(simpleInfo(), "vote rejected");
            return;
        }

        // TODO
        if(term < _currentTerm) {
            CXXRAFT_LOG_DEBUG(simpleInfo(), "old term vote:", term);
            return;
        }

        (*voted)++;

        CXXRAFT_LOG_DEBUG(simpleInfo(), "current voted:", *voted);

        // @paper
        //
        // A candidate wins an election if it receives votes from
        // a majority of the servers in the full cluster for the same
        // term.

        // Question. what about term > _currentTerm ?
    };

    // don't use for-each &&
    for(size_t i = 0; i < _peers.size(); ++i) {
        _peers[i].executor.post([=] {gatherVote(i);});
    }

    return voted;
}

inline void Raft::maintainAuthority() {

    const auto transaction = getTransaction();


    auto sendHeartbeat = [this, transaction](int index) {

        if(!isValidTransaction(transaction)) return;

        auto &peer = _peers[index];
        peer.executor.strike();

        // See prepareElection()
        bool reconnect = !peer.client || (peer.client && peer.client->fd() < 0);

        if(reconnect && !(peer.client = trpc::Client::make(peer.endpoint))) {
            return;
        }

        if(!isValidTransaction(transaction)) return;

        auto reply = peer.client->call<AppendEntryReply>(RAFT_APPEND_ENTRY_RPC, _currentTerm,_id, 0, 0);

        if(!isValidTransaction(transaction)) return;

        if(!reply) {
            CXXRAFT_LOG_DEBUG(simpleInfo(), "no reply in client");
        }
    };

    for(size_t i = 0; i < _peers.size(); ++i) {
        _peers[i].executor.post([=] { sendHeartbeat(i); });
    }
}

inline size_t Raft::getTransaction() {
    return _transaction;
}

inline bool Raft::isValidTransaction(size_t transaction) {
    // a very simple method
    // because we use coroutines
    return _transaction == transaction;
}

inline Config::Config(std::vector<trpc::Endpoint> peers)
    : _peers(peers), _connected(peers.size())
{}

inline std::shared_ptr<Config> Config::make(std::vector<trpc::Endpoint> peers) {
    return std::make_shared<Config>(std::move(peers));
}

inline void Config::connect(int index) {
    CXXRAFT_LOG_DEBUG("connect server", index, "to virtual network");
    auto raft = _rafts[index];
    auto &pServer = raft->_rpcServer;
    if(!pServer) {
        CXXRAFT_LOG_WTF("no server");
        return;
    }
    auto dummy = std::function<bool(trpc::Server::ProtocolType &)>{};
    pServer->onRequest(dummy);
    _connected[index] = true;
}

inline void Config::disconnect(int index) {
    CXXRAFT_LOG_DEBUG("disconnect server", index, "to virtual network");
    auto raft = _rafts[index];
    auto &pServer = raft->_rpcServer;
    if(!pServer) {
        CXXRAFT_LOG_WTF("no server");
        return;
    }
    pServer->onRequest([](auto&&) {
        return false;
    });
    _connected[index] = false;
}

inline void Config::abort() {
    // actually sleep
    ::usleep(50 * 1000);
    // acutally abort execution
    ::abort();
}

inline int Config::checkOneLeader() {
    for(auto iter {0}; iter < 10; ++iter) {
        // ms: [450, 550)
        auto ms = 450 + (::rand() % 100);
        co::poll(nullptr, 0, ms);

        auto leaders = std::unordered_map<int, std::vector<int>>{};

        for(auto i {0}; i < _peers.size(); ++i) {
            if(_connected[i]) {
                if(auto [term, leader] = _rafts[i]->getState(); leader) {
                    leaders[term].emplace_back(i);
                }
            } else {
                CXXRAFT_LOG_DEBUG("server", i, "disconnected");
            }
        }

        int lastTermWithLeader = -1;
        for(auto &&[term, leaders] : leaders) {
            if(leaders.size() > 1) {
                CXXRAFT_LOG_WTF("term", term, "has", leaders.size(), "(>1) leaders");
                abort();
            }
            if(term > lastTermWithLeader) {
                lastTermWithLeader = term;
            }
        }

        if(!leaders.empty()) {
            return leaders[lastTermWithLeader][0];
        }
    }

    CXXRAFT_LOG_WTF("expected one leader, got none");
    return -1;
}

inline void Config::checkNoLeader() {
    for(auto i {0}; i < _peers.size(); ++i) {
        if(_connected[i]) {
            auto [_, isLeader] = _rafts[i]->getState();
            if(isLeader) {
                CXXRAFT_LOG_WTF("expected no leader, but", i, "claims to be leader");
                abort();
            }
        } else {
            CXXRAFT_LOG_DEBUG("server", i, "disconnected");
        }
    }
}

inline int Config::checkTerms() {
    CXXRAFT_LOG_DEBUG("checkTerms");
    int term = -1;
    for(auto i {0}; i < _peers.size(); ++i) {
        if(_connected[i]) {
            auto [xterm, _] = _rafts[i]->getState();
            CXXRAFT_LOG_DEBUG("get xterm:", xterm);
            if(term == -1) {
                term = xterm;
                CXXRAFT_LOG_DEBUG("set term:", xterm);
            } else if(term != xterm) {
                CXXRAFT_LOG_WTF("servers disagree on term");
            }
        } else {
            CXXRAFT_LOG_DEBUG("server", i, "disconnected");
        }
    }
    return term;
}

inline Leader::Leader(Raft *master)
    : Raft::State(master)
{
    _flags |= Raft::FLAGS_LEADER;
}

inline void Leader::onBecome(std::shared_ptr<Raft::State> previous) {
    _master->performHeartBeat();
}

inline Reply<int, bool> Leader::onReceiveAppendEntryRPC(int term, int leaderId, int prevLogIndex, int prevLogTerm) {

    CXXRAFT_LOG_DEBUG(_master->simpleInfo(), "receive appendEntryRPC: ", term, leaderId, prevLogIndex, prevLogTerm);

    // currently peers don't care about the reply

    if(!isValidTransaction()) {
        return std::make_tuple(_master->_currentTerm, false);
    }

    // World has changed
    if(term > _master->_currentTerm) {

        CXXRAFT_LOG_DEBUG(_master->simpleInfo(), "world has changed");
        CXXRAFT_LOG_DEBUG(_master->simpleInfo(), "update to latest term:", term);

        // update currentTerm, see "Rules for Servers"
        _master->_currentTerm = term;

        CXXRAFT_LOG_DEBUG(_master->simpleInfo(), "post: leader -> follower");
        _master->_transitioner.post([master = this->_master] {
            master->becomeFollower();
        });
        return std::make_tuple(_master->_currentTerm, true);
    } else if(term == _master->_currentTerm) {
        CXXRAFT_LOG_WTF(_master->simpleInfo(), "I am the only leader in this term, you too?");
    }
    return std::make_tuple(_master->_currentTerm, false);
}

inline Reply<int, bool> Leader::onReceiveRequestVoteRPC(int term, int candidateId) {

    CXXRAFT_LOG_DEBUG(_master->simpleInfo(), "receive requestVoteRPC:", term, candidateId);

    if(!isValidTransaction()) {
        return std::make_tuple(_master->_currentTerm, false);
    }

    // reject stale request
    if(term < _master->_currentTerm) {
        CXXRAFT_LOG_DEBUG(_master->simpleInfo(), "reject stale request, term:", term);
        return std::make_tuple(_master->_currentTerm, false);
    }

    // check stale leader
    if(term > _master->_currentTerm) {
        // update currentTerm, see "Rules for Servers"
        _master->_currentTerm = term;
        CXXRAFT_LOG_DEBUG(_master->simpleInfo(), "post: leader -> follower");
        _master->_transitioner.post([master = _master] {
            master->becomeFollower();
        });
    }

    bool voteGranted = false;
    if(!_master->_voteFor || *_master->_voteFor == candidateId) {
        CXXRAFT_LOG_DEBUG(_master->simpleInfo(), "granted. vote for:", candidateId);
        _master->_voteFor = candidateId;
        voteGranted = true;
    }

    return std::make_tuple(_master->_currentTerm, voteGranted);
}

inline Follower::Follower(Raft *master)
    : Raft::State(master),
        _watchdog(std::make_shared<size_t>(0))
{
    _flags |= Raft::FLAGS_FOLLOWER;
}

inline void Follower::onBecome(std::shared_ptr<Raft::State> previous) {
    _master->performKeepAlive(_watchdog);
}

inline Reply<int, bool> Follower::onReceiveAppendEntryRPC(int term, int leaderId, int prevLogIndex, int prevLogTerm) {

    CXXRAFT_LOG_DEBUG(_master->simpleInfo(), "receive appendEntryRPC: ", term, leaderId, prevLogIndex, prevLogTerm);

    if(!isValidTransaction()) {
        return std::make_tuple(_master->_currentTerm, false);
    }

    ++(*_watchdog);


    if(term > _master->_currentTerm) {
        CXXRAFT_LOG_DEBUG(_master->simpleInfo(), "update to latest term:", term);
        _master->_currentTerm = term;
    }


    return std::make_tuple(_master->_currentTerm, false);
}

inline Reply<int, bool> Follower::onReceiveRequestVoteRPC(int term, int candidateId) {

    CXXRAFT_LOG_DEBUG(_master->simpleInfo(), "receive requestVoteRPC:", term, candidateId);

    if(!isValidTransaction()) {
        return std::make_tuple(_master->_currentTerm, false);
    }

    ++(*_watchdog);

    // reject stale request
    if(term < _master->_currentTerm) {
        CXXRAFT_LOG_DEBUG(_master->simpleInfo(), "reject stale request, term:", term);
        return std::make_tuple(_master->_currentTerm, false);
    }

    // update currentTerm, see "Rules for Servers"
    if(term > _master->_currentTerm) {
        CXXRAFT_LOG_DEBUG(_master->simpleInfo(), "update to latest term:", term);
        _master->_currentTerm = term;
    }

    bool voteGranted = false;
    if(!_master->_voteFor || *_master->_voteFor == candidateId) {
        CXXRAFT_LOG_DEBUG(_master->simpleInfo(), "granted. vote for:", candidateId);
        _master->_voteFor = candidateId;
        voteGranted = true;
    }
    return std::make_tuple(_master->_currentTerm, voteGranted);
}


inline Candidate::Candidate(Raft *master)
    : Raft::State(master) { _flags |= Raft::FLAGS_CANDIDATE; }

inline void Candidate::onBecome(std::shared_ptr<Raft::State> previous) {
    _master->performElection();
}

inline Reply<int, bool> Candidate::onReceiveAppendEntryRPC(int term, int leaderId, int prevLogIndex, int prevLogTerm) {

    CXXRAFT_LOG_DEBUG(_master->simpleInfo(), "receive appendEntryRPC: ", term, leaderId, prevLogIndex, prevLogTerm);

    if(!isValidTransaction()) {
        return std::make_tuple(_master->_currentTerm, false);
    }

    // @paper
    //
    // While waiting for votes, a candidate may receive an
    // AppendEntries RPC from another server claiming to be
    // leader. If the leader’s term (included in its RPC) is at least
    // as large as the candidate’s current term, then the candidate
    // recognizes the leader as legitimate and returns to follower
    // state.

    if(term >= _master->_currentTerm) {

        if(term > _master->_currentTerm) {
            CXXRAFT_LOG_DEBUG(_master->simpleInfo(), "update to latest term:", term);
            // update latest(largest) term candidate has seen
            // but paper said the result of AppendEntries RPC is "for **leader** to update itself" only?
            _master->_currentTerm = term;
        }

        CXXRAFT_LOG_DEBUG(_master->simpleInfo(), "post: candidate -> follower");
        _master->_transitioner.post([master = this->_master] {
            master->becomeFollower();
        });
        return std::make_tuple(_master->_currentTerm, true);
    }

    // @paper
    //
    // If the term in the RPC is smaller than the candidate’s current term,
    // then the candidate rejects the RPC and continues in candidate state
    return std::make_tuple(_master->_currentTerm, false);
}

inline Reply<int, bool> Candidate::onReceiveRequestVoteRPC(int term, int candidateId) {

    CXXRAFT_LOG_DEBUG(_master->simpleInfo(), "receive requestVoteRPC:", term, candidateId);

    if(!isValidTransaction()) {
        return std::make_tuple(_master->_currentTerm, false);
    }

    // reject stale request
    if(term < _master->_currentTerm) {
        CXXRAFT_LOG_DEBUG(_master->simpleInfo(), "reject stale request, term:", term);
        return std::make_tuple(_master->_currentTerm, false);
    }

    // update currentTerm, see "Rules for Servers"
    if(term > _master->_currentTerm) {
        CXXRAFT_LOG_DEBUG(_master->simpleInfo(), "update to latest term:", term);
        _master->_currentTerm = term;
    }

    bool voteGranted = false;
    if(!_master->_voteFor || *_master->_voteFor == candidateId) {
        CXXRAFT_LOG_DEBUG(_master->simpleInfo(), "granted. vote for:", candidateId);
        _master->_voteFor = candidateId;
        voteGranted = true;
    }

    return std::make_tuple(_master->_currentTerm, voteGranted);
}

} // cxxraft
