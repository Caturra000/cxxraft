#pragma once
#include <random>
#include <memory>
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
namespace cxxraft {

struct Config;

class Raft: public std::enable_shared_from_this<Raft> {

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

    Raft(Config &config, int id);
    Raft(const Raft&) = delete;
    Raft(Raft&&) = default;
    Raft& operator=(const Raft&) = delete;
    Raft& operator=(Raft&&) = default;
    ~Raft() = default;


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
private:

    // return: voted
    std::shared_ptr<int> performElection();

    // Waiting for any RPC response
    // If failed, follower will be converted to candidate
    void performKeepAlive(size_t *watchdog);

public:

    constexpr static const char *RAFT_APPEND_ENTRY_RPC
        {"appendEntryRPC"};

    constexpr static const char *RAFT_REQUEST_VOTE_RPC
        {"requestVoteRPC"};

    using AppendEntryReply = Reply<int, bool>;
    using RequestVoteReply = Reply<int, bool>;

    // Raft uses randomized election timeouts to ensure that
    // split votes are rare and that they are resolved quickly. To
    // prevent split votes in the first place, election timeouts are
    // chosen randomly from a fixed interval (e.g., 150–300ms)
    constexpr static auto RAFT_ELECTION_TIMEOUT
        { std::chrono::milliseconds(300) };

    constexpr static auto RAFT_ELECTION_TIMEOUT_MIN
        { std::chrono::milliseconds(150) };

    constexpr static auto RAFT_ELECTION_TIMEOUT_MAX
        { std::chrono::milliseconds(300) };

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
    // In some situations an election will result in a split vote.
    // In this case the term will end with no leader;
    // a new term (with a new election) will begin shortly.
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

    // TODO same as FLAGS_CANDIDATE?
    constexpr static Bitmask FLAGS_WAITING_FOR_VOTE = 1ULL << 4;

private:

    trpc::Endpoint _self;
    std::optional<trpc::Server> _rpcServer;

    // raft peers, NOT include this one
    std::vector<Peer> _peers;

    // node id for RPC message
    int _id;

    // Log or configuration
    Storage _storage;

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

    // Bitmask _flags;

    // TODO idle worker

    // Finite State Machine
    std::shared_ptr<State> _fsm;

    // a coroutine where fsm runs on
    Worker _controller;

private:

    friend struct Config;
    friend struct Leader;
    friend struct Follower;
    friend struct Candidate;
};






////////////////////////////////// State Machine //////////////////////////////////





// TODO optional interface
struct Raft::State {
    struct Literal {};

    State(Raft *master): _master{master}, _flags{} {}

    virtual Raft::Bitmask type() { return 0; };
    virtual const char*   type(Literal) { return "unknown"; }
    virtual Raft::Bitmask flags() { return _flags; }
    virtual const char*   flags(Literal) { return "TODO"; }

    // it runs on the `_controller` coroutine
    virtual void onBecome(std::shared_ptr<Raft::State> previous) = 0;

    // runs on a RPC server coroutine
    virtual Reply<int, bool> onReceiveAppendEntryRPC(int term, int leaderId, int prevLogIndex, int prevLogTerm) = 0;

    // runs on a RPC server coroutine
    virtual Reply<int, bool> onReceiveRequestVoteRPC(int term, int candidateId) = 0;

protected:
    Raft *_master;
    Raft::Bitmask _flags;
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

    void begin() {}
    void end() {}


    // Whether each server is on the net
    std::vector<char> _connected;

    std::vector<trpc::Endpoint> _peers;

    std::vector<std::shared_ptr<Raft>> _rafts;

};






////////////////////////////////// Implementation //////////////////////////////////







inline Raft::Raft(Config &config, int id)
    : _self(config._peers[id]),
      _id(id),
      _currentTerm(0),
      _fsm(nullptr)
{
    for(size_t i = 0; i < config._peers.size(); i++) {
        if(i != id) _peers.emplace_back(config._peers[i]);
    }

    // for test
    config._rafts.emplace_back(shared_from_this());
    // config._connected[id] = true;
}

inline void Raft::start() {
    auto &env = co::open();

    _rpcServer = trpc::Server::make(_self);

    if(!_rpcServer) {
        CXXRAFT_LOG_WARN("server start failed.");
        return;
    }

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

    // coroutine1~N: client
    // TODO remove code
    // check bool(client)
    for(size_t i = 0; i < _peers.size(); ++i) {

        _peers[i].proactive.post([this, i] {
            auto &client = _peers[i].client;
            client = trpc::Client::make(_peers[i].endpoint);

            if(!client) {
                CXXRAFT_LOG_WARN("client", i, "start failed.");
                return;
            }
        });
    }

    // coroutineN+1 : state machine control
    _controller.post([this] {
        // preivous state == nullptr
        becomeFollower();
    });
}

inline std::shared_ptr<Raft> Raft::make(Config &config, int id) {
    return std::make_shared<Raft>(config, id);
}

inline auto Raft::getState() {
    return std::make_tuple(_currentTerm, bool(_fsm->flags() & FLAGS_FOLLOWER));
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
    _fsm = std::make_shared<NextState>(this);
    _fsm->onBecome(std::move(previous));
}

inline void Raft::becomeLeader() {
    CXXRAFT_LOG_DEBUG("become leader. node id:", _id);
    become<Leader>();
}

inline void Raft::becomeFollower() {
    CXXRAFT_LOG_DEBUG("become follower. node id:", _id);
    become<Follower>();
}

inline void Raft::becomeCandidate() {
    CXXRAFT_LOG_DEBUG("become candidate. node id:", _id);
    become<Candidate>();
}

inline std::shared_ptr<int> Raft::performElection() {

    // To begin an election, a follower increments its current
    // term and transitions to candidate state
    _currentTerm++;

    // It then votes for itself and
    // issues RequestVote RPCs in parallel to each of
    // the other servers in the cluster.
    //
    // (voted == -1) means aborted
    auto voted = std::make_shared<int>(1);

    auto gatherVote = [this, currentTerm = _currentTerm, voted](int index) {
        // abort
        if(*voted == -1) return;
        // optimize
        if(*voted > (1 + _peers.size()) / 2) return;

        auto &peer = _peers[index];

        // Try to connect to a raft node
        // (Although it is a long connection, it may crash before)
        //
        // It may be unavailable and make() / connect() spends a lot of time
        // But we can run this `gatherVote` on many client coroutines
        // While `performElection` runs on fsm coroutine
        if(!peer.client && !(peer.client = trpc::Client::make(peer.endpoint))) {
            return;
        }

        auto reply = peer.client->call<RequestVoteReply>(RAFT_REQUEST_VOTE_RPC, currentTerm, _id, 0, 0);

        if(!reply) {
            // TODO print endpoint
            CXXRAFT_LOG_DEBUG("no reply in client");
        }

        // While waiting for votes, a candidate may receive an
        // AppendEntries RPC from another server claiming to be
        // leader.
        // If the leader’s term (included in its RPC) is at least
        // as large as the candidate’s current term, then the candidate
        // recognizes the leader as legitimate and returns to follower
        // state.
        // If the term in the RPC is smaller than the candidate’s
        // current term, then the candidate rejects the RPC and continues in candidate state.

        auto [term, voteGranted] = reply->cast();
        if(!voteGranted) {
            CXXRAFT_LOG_DEBUG("vote rejected");
        }
        // `_currentTerm`, not `currentTerm`
        if(term < _currentTerm) {
            return;
        }

        (*voted)++;
    };

    // don't use for-each &&
    for(size_t i = 0; i < _peers.size(); ++i) {
        _peers[i].proactive.post([=] {gatherVote(i);});
    }

    return voted;
}

inline void Raft::performKeepAlive(size_t *watchdog) {
    CXXRAFT_LOG_DEBUG("follower keepalive, node id:", _id);
    while(1) {
        size_t old = *watchdog;
        co::usleep(std::chrono::duration<useconds_t, std::micro>(
            Raft::RAFT_ELECTION_TIMEOUT_MAX).count());
        // no RPC response
        if(old == *watchdog) {
            CXXRAFT_LOG_DEBUG("no RPC response, node id:", _id);
            break;
        }
        // else keep follower
    }

    CXXRAFT_LOG_DEBUG("post: follower -> candidate. node id:", _id);
    // waiting RPC for a long time
    _controller.post([this] {
        becomeCandidate();
    });
}

inline Config::Config(std::vector<trpc::Endpoint> peers)
    : _peers(peers)
{}

inline std::shared_ptr<Config> Config::make(std::vector<trpc::Endpoint> peers) {
    return std::make_shared<Config>(std::move(peers));
}

inline void Config::connect(int index) {
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
    int term = -1;
    for(auto i {0}; i < _peers.size(); ++i) {
        if(_connected[i]) {
            auto [xterm, _] = _rafts[i]->getState();
            if(term == -1) {
                term = xterm;
            } else if(term != xterm) {
                CXXRAFT_LOG_WTF("servers disagree on term");
            }
        } else {
            CXXRAFT_LOG_DEBUG("server", i, "disconnected");
        }
    }
    return term;
}




struct Leader: public Raft::State {

    Leader(Raft *master): Raft::State(master) { _flags |= Raft::FLAGS_LEADER; };

    Raft::Bitmask type() override { return Raft::FLAGS_LEADER; }
    const char* type(Raft::State::Literal) override { return "leader"; }

    void onBecome(std::shared_ptr<Raft::State> previous) override {
        // TODO abort pending jobs in previous state?
        // master->abort...()
        // it can be simplified...
        // because we are in the same coroutine environment
        // jobs in worker queue are all pending
        // for(auto &&client: ...) client.worker.strike();

        // start leader job
        // example: appendEntry
        // master->perform...()
    }

    Reply<int, bool> onReceiveAppendEntryRPC(int term, int leaderId, int prevLogIndex, int prevLogTerm) override;
    Reply<int, bool> onReceiveRequestVoteRPC(int term, int candidateId) override {

        // TODO return _master->performLeaderReceiveRequestVote...

        // reject stale request
        if(term < _master->_currentTerm) {
            return std::make_tuple(_master->_currentTerm, false);
        }

        // TODO check stale leader?
        // if(term > _master->_currentTerm) {
        //     // post(Later{}, becomFollower) ?
        //     _master->becomeFollower();
        //     return ?
        // }
    }
};

struct Follower: public Raft::State {

    Follower(Raft *master): Raft::State(master) { _flags |= Raft::FLAGS_FOLLOWER; }

    Raft::Bitmask type() override { return Raft::FLAGS_FOLLOWER; }
    const char* type(Raft::State::Literal) override { return "follower"; }

    // running on a control coroutine (N+1)
    void onBecome(std::shared_ptr<Raft::State> previous) override {
        _master->performKeepAlive(&_watchdog);
    }

    // running on a RPC coroutine (0)
    Reply<int, bool> onReceiveAppendEntryRPC(int term, int leaderId, int prevLogIndex, int prevLogTerm) override {
        ++_watchdog;

        // TODO

        return std::make_tuple(_master->_currentTerm, false);
    }

    Reply<int, bool> onReceiveRequestVoteRPC(int term, int candidateId) override {
        ++_watchdog;

        // reject stale request
        if(term < _master->_currentTerm) {
            return std::make_tuple(_master->_currentTerm, false);
        }

        bool voteGranted = false;
        if(!_master->_voteFor || *_master->_voteFor == candidateId) {
            _master->_voteFor = candidateId;
            voteGranted = true;
        }
        return std::make_tuple(_master->_currentTerm, voteGranted);
    }

private:
    size_t _watchdog {};
};


struct Candidate: public Raft::State {

    Candidate(Raft *master): Raft::State(master) { _flags |= Raft::FLAGS_CANDIDATE; }

    Raft::Bitmask type() override { return Raft::FLAGS_CANDIDATE; }
    const char* type(Raft::State::Literal) override { return "candidate"; }

    void onBecome(std::shared_ptr<Raft::State> previous) override {

        std::random_device rd;
        std::mt19937 engine(rd());
        using ToMicro = std::chrono::duration<useconds_t, std::micro>;
        std::uniform_int_distribution<> dist(
            ToMicro{Raft::RAFT_ELECTION_TIMEOUT_MIN}.count(),
            ToMicro{Raft::RAFT_ELECTION_TIMEOUT_MAX}.count()
        );

        bool win = false;
        while(1) {
            auto voting = _master->performElection();
            // if failed, wait a while
            co::usleep(dist(engine));
            int voted = *voting;
            // abort!
            *voting = -1;
            if(voted > (1 + _master->_peers.size()) / 2) {
                win = true;
                break;
            }
        }

        if(win) {
            _master->becomeLeader();
        }
    }

    Reply<int, bool> onReceiveAppendEntryRPC(int term, int leaderId, int prevLogIndex, int prevLogTerm) override;
    Reply<int, bool> onReceiveRequestVoteRPC(int term, int candidateId) override;
};

} // cxxraft
