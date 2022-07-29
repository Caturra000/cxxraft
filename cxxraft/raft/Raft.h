#pragma once
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

    friend struct Config;

// public API
public:

    void start();

    static std::shared_ptr<Raft> make(Config &config, int id);

    // Ask a Raft for its current term, and whether it thinks it is leader
    // return: [term, isLeader] <std::tuple<int, bool>>
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

    void step();

    // TODO
    // Invoked by leader to replicate log entries (§5.3);
    // also used as heartbeat (§5.2).
    // return: [term, success]
    Reply<int, bool> appendEntryRPC(int term, int leaderId, int prevLogIndex, int prevLogTerm /*, null log*/);

    // Invoked by candidates to gather votes (§5.2).
    // return: [term, voteGranted]
    Reply<int, bool> requestVoteRPC(int term, int candidateId /*...*/);

// TODO state machine object
// state.onReceiveVote() / state.onReceiveAppend()...
// state.onBecome(/*lastState*/)
private:

    // @action
    // Leaders send periodic heartbeats (AppendEntriesRPCs that carry no log entries)
    // to all followers in order to maintain their authority.
    bool becomeLeader();

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
    bool becomeFollower();

    bool becomeCandidate();

private:

    void performElection();

public:

    constexpr static const char *RAFT_APPEND_ENTRY_RPC
        {"appendEntryRPC"};

    constexpr static const char *RAFT_REQUEST_VOTE_RPC
        {"requestVoteRPC"};

    using AppendEntryReply = Reply<int, bool>;
    using RequestVoteReply = Reply<int, bool>;

    constexpr static auto RAFT_ELECTION_TIMEOUT
        { std::chrono::seconds(1) };

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

    Bitmask _flags;

    Worker _worker;

};






////////////////////////////////// State Machine //////////////////////////////////






struct Raft::State {
    struct Literal {};
    virtual Raft::Bitmask type() { return 0; };
    virtual const char* type(Literal) { return "unknown"; }
    virtual void onBecome(std::shared_ptr<Raft::State> previous) = 0;
    // return: next state? or null
    virtual std::shared_ptr<Raft::State> onReceiveRPC() = 0;

// TODO
// Raft *master;
// Bitmask flags;
};

struct Leader: public Raft::State {
    Raft::Bitmask type() override { return Raft::FLAGS_LEADER; };
    const char* type(Raft::State::Literal) override { return "leader"; };
    void onBecome(std::shared_ptr<Raft::State> previous) override {
        // TODO abort pending jobs in previous state?
        // master->abort...()

        // start leader job
        // example: appendEntry
        // master->perform...()
    }
    std::shared_ptr<Raft::State> onReceiveRPC() override { return nullptr; }
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
      _flags(FLAGS_FOLLOWER)
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

    auto &server = *_rpcServer;

    server.bind(RAFT_APPEND_ENTRY_RPC, [this](int term, int leaderId, int prevLogIndex, int prevLogTerm) {
        return this->appendEntryRPC(term, leaderId, prevLogIndex, prevLogTerm);
    });

    server.bind(RAFT_REQUEST_VOTE_RPC, [this](int term, int candidateId) {
        return this->requestVoteRPC(term, candidateId);
    });

    // bind state machine

    // if isLeader ....


    // add unreliable network

    env.createCoroutine([this] {
        _rpcServer->start();
    })->resume();

    for(size_t i = 0; i < _peers.size(); ++i) {

        _peers[i].proactive.post([this, i] {
            auto &client = _peers[i].client;
            client = trpc::Client::make(_peers[i].endpoint);

            if(!client) {
                CXXRAFT_LOG_WARN("client", i, "start failed.");
                return;
            }

            // TODO 随机 config
            // useconds_t interval = RAFT_ELECTION_TIMEOUT;
            co::usleep(12345);

        });

        // // election?

        // _peers[i].proactive.post([this, i] {
        //     auto &heartbeat = _peers[i].heartbeat;
        //     heartbeat = trpc::Client::make(_peers[i].endpoint);
        // });

        // if(!presist) becomeFollower();
    }

    //
}

inline std::shared_ptr<Raft> Raft::make(Config &config, int id) {
    return std::make_shared<Raft>(config, id);
}

inline auto Raft::getState() {
    return std::make_tuple(_currentTerm, bool(_flags & FLAGS_LEADER));
}

inline Reply<int, bool> Raft::appendEntryRPC(int term, int leaderId, int prevLogIndex, int prevLogTerm) {
}

inline Reply<int, bool> Raft::requestVoteRPC(int term, int candidateId /*...*/) {
    // reject stale request
    if(term < _currentTerm) {
        return std::make_tuple(_currentTerm, false);
    }

    // TODO check stale leader?

    bool voteGranted = false;
    if(!_voteFor || *_voteFor == candidateId) {
        _voteFor = candidateId;
        voteGranted = true;
    }
    return std::make_tuple(_currentTerm, voteGranted);
}

inline void Raft::performElection() {

    // To begin an election, a follower increments its current
    // term and transitions to candidate state
    _currentTerm++;

    // It then votes for itself and
    // issues RequestVote RPCs in parallel to each of
    // the other servers in the cluster.
    int voted = 1;

    // TODO coroutine
    // Question. fd shared in different coroutines?
    // Fix. use Raft::Worker
    for(auto &&peer : _peers) {
        if(peer.client) {
            auto reply = peer.client->call<RequestVoteReply>(RAFT_REQUEST_VOTE_RPC, _currentTerm, _id, 0, 0);
            if(!reply) {
                // TODO print endpoint
                CXXRAFT_LOG_DEBUG("no reply in client");
            }
            auto [term, voteGranted] = reply->cast();
            if(!voteGranted) {
                CXXRAFT_LOG_DEBUG("vote rejected");
            }
            // if(term < _...)
            // if term ?
            voted++;
        }
    }
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

} // cxxraft
