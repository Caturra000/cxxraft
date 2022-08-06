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
#include "raft/Worker.h"
#include "raft/Debugger.h"
#include "raft/Command.h"
#include "raft/Log.h"
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
    auto getState() -> std::tuple<int, bool>;

    // the service using Raft (e.g. a k/v server) wants to start
    // agreement on the next command to be appended to Raft's log. if this
    // server isn't the leader, returns false. otherwise start the
    // agreement and return immediately. there is no guarantee that this
    // command will ever be committed to the Raft log, since the leader
    // may fail or lose an election. even if the Raft instance has been killed,
    // this function should return gracefully.
    //
    // the first return value is the index that the command will appear at
    // if it's ever committed. the second return value is the current
    // term. the third return value is true if this server believes it is
    // the leader.
    //
    // return: [index of command, term, isLeader]
    auto startCommand(Command command) -> std::tuple<int, int, bool>;

public:

    Raft(const Raft&) = delete;
    Raft(Raft&&) = delete;
    Raft& operator=(const Raft&) = delete;
    Raft& operator=(Raft&&) = delete;
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
    // TODO static member function
    Reply<int, bool> appendEntryRPC(int term, int leaderId,
                                    int prevLogIndex, int prevLogTerm,
                                    Log::EntriesArray entries, int leaderCommit);

    // Invoked by candidates to gather votes (§5.2).
    // return: [term, voteGranted]
    Reply<int, bool> requestVoteRPC(int term, int candidateId,
                                    int lastLogIndex, int lastLogTerm);

    // TODO junk
    // static bool defaultResponseProxy(trpc::Server::ProtocolType &response);

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

    // A wrapper of transactional post
    // used to safely queue `f` to `fsm` coroutine
    template <typename F>
    void statePost(F f);

// perform can be called by state machine only
// all perform functions should be executed endless
// TODO move to FSM
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
    std::shared_ptr<std::tuple<int, int>> gatherVotesFromClients(size_t transaction);

    void maintainAuthorityToClients(size_t transaction);

    void updateTransaction();

    size_t getTransaction();

    bool isValidTransaction(size_t transaction);

    // return: voteGranted
    bool vote(int candidateId);

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
        { std::chrono::milliseconds(800) };

    constexpr static auto RAFT_ELECTION_TIMEOUT_MIN
        { std::chrono::milliseconds(400) };

    constexpr static auto RAFT_ELECTION_TIMEOUT_MAX
        { std::chrono::milliseconds(800) };

    // Same as etcd-raft heartbeat interval
    // (less than RAFT_ELECTION_TIMEOUT_MIN)
    constexpr static auto RAFT_HEARTBEAT_INTERVAL
        { std::chrono::milliseconds(100) };

    // When post a request to FSM but not apply
    // RPCs are still running and may reply to peers
    // given a junk term less than 0 will be omitted by peers
    // TODO rpc.onResponse (defaultResponseProxy)
    constexpr static int JUNK_TERM = -1;

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
    std::vector<trpc::Endpoint> _peers;

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

    Log _log;

    // TODO
    int _commitIndex {};

    // // TODO
    int _lastApplied {};

    // Finite State Machine
    std::shared_ptr<State> _fsm;

    // A coroutine where FSM runs on
    Worker _transitioner;

    // A transaction ID for state machine
    // When invokes become...(), _transaction will add one
    // and pending coroutines will know themselves are outdated and then abort
    //
    // In breif, it looks like pthread_cancel()
    // _transaction will be checked in every context switch as a cancellation point
    // Cancellation of outdated state machine either happens immediately or when it reaches a cancellation point
    //
    // _transaction and _currentTerm are not strictly related
    size_t _transaction;

// virtual network
// for config test
private:

    bool callDisabled() { return _callDisabled; }
    bool _callDisabled {false};

private:

    friend struct Config;
    friend struct Leader;
    friend struct Follower;
    friend struct Candidate;

    friend struct Debugger<Raft>;
};




















////////////////////////////////// State Machine //////////////////////////////////



















// FSM class
struct Raft::State {

// supports polymorphism
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

    // it runs on the `_transitioner` coroutine
    virtual void onBecome(std::shared_ptr<Raft::State> previous) = 0;

    // runs on a RPC server coroutine
    virtual Reply<int, bool> onAppendEntryRPC(int term, int leaderId,
                                              int prevLogIndex, int prevLogTerm,
                                              Log::EntriesArray entries, int leaderCommit) = 0;

    // runs on a RPC server coroutine
    virtual Reply<int, bool> onRequestVoteRPC(int term, int candidateId,
                                              int lastLogIndex, int lastLogTerm) = 0;

    // @paper
    //
    // All Servers:
    // If RPC request or response contains term T > currentTerm:
    // set currentTerm = T, convert to follower (§5.1)
    // return: convert to follower
    virtual bool updateLatestTerm(int term);

protected:

    bool isValidTransaction() { return _master->isValidTransaction(_transaction); }

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

    Reply<int, bool> onAppendEntryRPC(int term, int leaderId,
                                      int prevLogIndex, int prevLogTerm,
                                      Log::EntriesArray entries, int leaderCommit) override;
    Reply<int, bool> onRequestVoteRPC(int term, int candidateId,
                                      int lastLogIndex, int lastLogTerm) override;

};

struct Follower: public Raft::State {

    Follower(Raft *master);

    Raft::Bitmask type() override { return Raft::FLAGS_FOLLOWER; }
    const char* type(Raft::State::Literal) override { return "follower"; }

    // running on a transitioner coroutine (1)
    void onBecome(std::shared_ptr<Raft::State> previous) override;

    // running on a RPC coroutine (0)
    Reply<int, bool> onAppendEntryRPC(int term, int leaderId,
                                      int prevLogIndex, int prevLogTerm,
                                      Log::EntriesArray entries, int leaderCommit) override;

    Reply<int, bool> onRequestVoteRPC(int term, int candidateId,
                                      int lastLogIndex, int lastLogTerm) override;

    bool updateLatestTerm(int term) override;

private:
    std::shared_ptr<size_t> _watchdog;
};

struct Candidate: public Raft::State {

    Candidate(Raft *master);

    Raft::Bitmask type() override { return Raft::FLAGS_CANDIDATE; }
    const char* type(Raft::State::Literal) override { return "candidate"; }

    void onBecome(std::shared_ptr<Raft::State> previous) override;

    Reply<int, bool> onAppendEntryRPC(int term, int leaderId,
                                      int prevLogIndex, int prevLogTerm,
                                      Log::EntriesArray entries, int leaderCommit) override;

    Reply<int, bool> onRequestVoteRPC(int term, int candidateId,
                                      int lastLogIndex, int lastLogTerm) override;
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

//////////// for 2B

    // how many servers think a log entry is committed?
    // [count, command]
    // TODO
    auto nCommitted(int index) -> std::tuple<int, Command>;

    // do a complete agreement.
    // it might choose the wrong leader initially,
    // and have to re-submit after giving up.
    // entirely gives up after about 10 seconds.
    // indirectly checks that the servers agree on the
    // same value, since nCommitted() checks this,
    // as do the threads that read from applyCh.
    // returns index.
    // if retry==true, may submit the command multiple
    // times, in case a leader fails just after Start().
    // if retry==false, calls Start() only once, in order
    // to simplify the early Lab 2B tests.
    int one(Command command, int expectedServers, bool retry);

    void begin(const char *test) { std::cout << "begin: " << test << std::endl; }
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

    auto appendEntry = [this](int term, int leaderId,
                              int prevLogIndex, int prevLogTerm,
                              Log::EntriesArray entries, int leaderCommit) {
        return this->appendEntryRPC(term, leaderId, prevLogIndex, prevLogTerm, std::move(entries), leaderCommit);
    };

    auto requestVote = [this](int term, int candidateId,
                              int lastLogIndex, int lastLogTerm) {
        return this->requestVoteRPC(term, candidateId, lastLogIndex, lastLogTerm);
    };

    _rpcServer->bind(RAFT_APPEND_ENTRY_RPC, appendEntry);
    _rpcServer->bind(RAFT_REQUEST_VOTE_RPC, requestVote);

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

inline auto Raft::getState() -> std::tuple<int, bool> {
    return std::make_tuple(_currentTerm, bool(_fsm->type() == FLAGS_LEADER));
}

inline auto Raft::startCommand(Command command) -> std::tuple<int, int, bool> {

    auto [term, isLeader] = getState();

    if(!isLeader) return {0, term, false};

    // _logs.append(..., std::move(command));

    return {1, 2, false};
}

inline Reply<int, bool> Raft::appendEntryRPC(int term, int leaderId,
                                             int prevLogIndex, int prevLogTerm,
                                             Log::EntriesArray entries, int leaderCommit) {
    return _fsm->onAppendEntryRPC(term, leaderId, prevLogIndex, prevLogTerm, std::move(entries), leaderCommit);
}

inline Reply<int, bool> Raft::requestVoteRPC(int term, int candidateId, int lastLogIndex, int lastLogTerm) {
    return _fsm->onRequestVoteRPC(term, candidateId, lastLogIndex, lastLogTerm);
}

template <typename NextState>
inline void Raft::become() {
    auto previous = std::move(_fsm);
    // abort all the old transactions / pending coroutine jobs requested by `previous`
    // (or `previous` of `previous`...)
    updateTransaction();
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

template <typename F>
inline void Raft::statePost(F f) {
    updateTransaction();
    _transitioner.post(std::move(f));
}

inline void Raft::performElection() {

    constexpr static int VOTE_ABORTED = -1;

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

        // In order to become a new leader
        // We need a new term
        //
        // @paper
        //
        // To begin an election, a follower increments its current
        // term and transitions to candidate state
        _currentTerm++;
        CXXRAFT_LOG_DEBUG(simpleInfo(), "begin election, add term to:", _currentTerm);

        // @paper
        //
        // It then votes for itself and
        // issues RequestVote RPCs in parallel to each of
        // the other servers in the cluster.
        _voteFor = _id;

        // Create asynchronous client coroutines to gather votes
        auto voteInfo = gatherVotesFromClients(transaction);

        // Try to poll faster
        for(size_t step = 0; step < 5; ++step) {
            CXXRAFT_LOG_DEBUG(simpleInfo(), "co::sleep down");
            co::usleep(dist(engine) / 5);
            CXXRAFT_LOG_DEBUG(simpleInfo(), "co::sleep up");
            if(!isValidTransaction(transaction)) {
                CXXRAFT_LOG_DEBUG(simpleInfo(), "invalid transaction");
                return;
            }
            auto [voted, rejected] = *voteInfo;
            // confirmed
            if(voted >= majority() || rejected >= majority()) {
                break;
            }
        }

        auto &[voted, rejected] = *voteInfo;

        CXXRAFT_LOG_DEBUG(simpleInfo(), "gathered:", voted, rejected);


        if(voted >= majority()) {
            break;
        }

        if(rejected < majority()) {
            CXXRAFT_LOG_DEBUG(simpleInfo(), "times out, new election");
        } else {
            CXXRAFT_LOG_DEBUG(simpleInfo(), "rejected, new election");
        }

        // abort this round!
        voted = VOTE_ABORTED;
    }

    if(!isValidTransaction(transaction)) {
        CXXRAFT_LOG_DEBUG(simpleInfo(), "invalid transaction");
        return;
    }

    CXXRAFT_LOG_DEBUG(simpleInfo(), "post: candidate -> leader.",
        "Reason: receive votes from majority of servers");

    // See performKeepAlive
    statePost([this] {
        becomeLeader();
    });
}

inline void Raft::performKeepAlive(std::shared_ptr<size_t> watchdog) {
    const auto transaction = getTransaction();
    CXXRAFT_LOG_DEBUG(simpleInfo(), "perform keepalive");
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

    CXXRAFT_LOG_DEBUG(simpleInfo(), "post: follower -> candidate. Reason: times out");
    // waiting RPC for a long time
    statePost([this] {
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
        maintainAuthorityToClients(transaction);
        co::usleep(std::chrono::duration<useconds_t, std::micro>(
            RAFT_HEARTBEAT_INTERVAL).count());
    }
    CXXRAFT_LOG_DEBUG(simpleInfo(), "abort heartbeat: invalid transcation");
}

inline size_t Raft::majority() {
    return (1 + _peers.size()) / 2 + 1;
}

inline std::shared_ptr<std::tuple<int, int>> Raft::gatherVotesFromClients(size_t transaction) {

    CXXRAFT_LOG_DEBUG(simpleInfo(), "gatherVotesFromClients");

    // info: [voted, rejected]
    // (voted == -1) means aborted
    // 1: vote for itself
    auto voteInfo = std::make_shared<std::tuple<int, int>>(1, 0);

    constexpr static int VOTE_ABORTED = -1;

    auto gatherVote = [this, voteInfo, transaction](int index) {

        CXXRAFT_LOG_DEBUG(simpleInfo(), "gatherVote, index:", index);

        if(!isValidTransaction(transaction)) {
            CXXRAFT_LOG_DEBUG(simpleInfo(), "invalid transaction");
            return;
        }

        auto &peer = _peers[index];

        // abort or optimize
        if(auto [voted, rejected] = *voteInfo;
            voted == VOTE_ABORTED || voted >= majority() || rejected >= majority()) {
                CXXRAFT_LOG_DEBUG(simpleInfo(), "gatherVote return. info:", voted, rejected);
            return;
        }


        auto client = trpc::Client::make(peer);

        if(!client) {
            return;
        }

        // cancellation point after client connect
        if(!isValidTransaction(transaction)) {
            return;
        }

        CXXRAFT_LOG_DEBUG(simpleInfo(), "call requset vote:", _currentTerm, _id);

        // for config test
        if(callDisabled()) return;

        auto reply = client->call<RequestVoteReply>(RAFT_REQUEST_VOTE_RPC, _currentTerm, _id, 0, 0);

        // cancellation point after client call
        if(!isValidTransaction(transaction)) {
            return;
        }

        // check again after network IO
        if(auto [voted, rejected] = *voteInfo;
                voted == VOTE_ABORTED || voted >= majority() || rejected >= majority()) {
            CXXRAFT_LOG_DEBUG(simpleInfo(), "gatherVote return. info:", voted, rejected);
            return;
        }

        if(!reply) {
            CXXRAFT_LOG_DEBUG(simpleInfo(), "no reply in client");
            return;
        }

        auto [term, voteGranted] = reply->cast();
        CXXRAFT_LOG_DEBUG(simpleInfo(), "gatherVote. vote reply:", term, voteGranted);

        if(term > _currentTerm) {
            CXXRAFT_LOG_DEBUG(simpleInfo(), "update to latest term:", term);
            _currentTerm = term;
            CXXRAFT_LOG_DEBUG(simpleInfo(), "reset voteFor");
            _voteFor = std::nullopt;
            CXXRAFT_LOG_DEBUG(simpleInfo(), "post: candidate -> follower.",
                "Reason: discovers new term");
            statePost([this] {
                becomeFollower();
            });
            return;
        }

        auto &[voted, rejected] = *voteInfo;

        if(voted == VOTE_ABORTED) {
            return;
        }

        if(!voteGranted) {
            rejected++;
            CXXRAFT_LOG_DEBUG(simpleInfo(), "gatherVote. vote rejected+1,", rejected);
            return;
        }

        if(term < _currentTerm) {
            CXXRAFT_LOG_DEBUG(simpleInfo(), "gatherVote. old term vote:", term);
            return;
        }

        // @paper
        //
        // A candidate wins an election if it receives votes from
        // a majority of the servers in the full cluster for the same
        // term.
        if(term == _currentTerm) {
            voted++;
        }

        CXXRAFT_LOG_DEBUG(simpleInfo(), "gatherVote. current voteInfo:", voted, rejected);
    };

    // don't use for-each &&
    for(size_t i = 0; i < _peers.size(); ++i) {
        auto &env = co::open();
        env.createCoroutine([=] {gatherVote(i);})
            ->resume();
    }

    return voteInfo;
}

inline void Raft::maintainAuthorityToClients(size_t transaction) {

    auto sendHeartbeat = [this, transaction](int index) {

        CXXRAFT_LOG_DEBUG(simpleInfo(), "maintainAuthorityToClients");

        if(!isValidTransaction(transaction)) return;

        auto &peer = _peers[index];

        auto client = trpc::Client::make(peer);

        if(!client) return;

        if(!isValidTransaction(transaction)) return;

        // for config test
        if(callDisabled()) return;

        CXXRAFT_LOG_DEBUG(simpleInfo(), "maintainAuthority. call append entey:", _currentTerm, _id);

        // TODO prevLogIndex...
        Log::EntriesSlice slice = _log.fork(Log::ByReference{});
        auto reply = client->call<AppendEntryReply>(RAFT_APPEND_ENTRY_RPC, _currentTerm, _id, 0, 0, slice, 0);

        if(!isValidTransaction(transaction)) return;

        if(!reply) {
            CXXRAFT_LOG_DEBUG(simpleInfo(), "no reply in client");
        }

        auto [term, _] = reply->cast();

        if(term > _currentTerm) {
            CXXRAFT_LOG_DEBUG(simpleInfo(), "update to latest term:", term);
            _currentTerm = term;
            CXXRAFT_LOG_DEBUG(simpleInfo(), "reset voteFor");
            _voteFor = std::nullopt;
            CXXRAFT_LOG_DEBUG(simpleInfo(), "post: leader -> follower",
                "Reason: discovers server with higher term");
            statePost([this] {
                becomeFollower();
            });
        }
    };

    for(size_t i = 0; i < _peers.size(); ++i) {
        auto &env = co::open();
        env.createCoroutine([=] {sendHeartbeat(i);})
            ->resume();
    }
}

inline void Raft::updateTransaction() {
    // currently it is a simple operation
    // further transaction may be a complex obejct
    _transaction++;
}


inline size_t Raft::getTransaction() {
    return _transaction;
}

inline bool Raft::isValidTransaction(size_t transaction) {
    // a very simple method
    // because we use coroutines
    // TODO atomic in multithreads
    return _transaction == transaction;
}

inline bool Raft::vote(int candidateId) {
    bool voteGranted = false;
    if(!_voteFor || *_voteFor == candidateId) {
        CXXRAFT_LOG_DEBUG(simpleInfo(), "vote granted. vote for:", candidateId);
        _voteFor = candidateId;
        voteGranted = true;
    } else {
        CXXRAFT_LOG_DEBUG(simpleInfo(), "reject this vote. voted to:", *_voteFor);
    }
    return voteGranted;
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
    CXXRAFT_LOG_DEBUG("server info:", raft->simpleInfo());
    auto &pServer = raft->_rpcServer;
    if(!pServer) {
        CXXRAFT_LOG_WTF("no server");
        return;
    }
    auto dummy = std::function<bool(trpc::Server::ProtocolType &)>{};
    pServer->onRequest(dummy);
    _connected[index] = true;

    raft->_callDisabled = false;
    raft->statePost([index, this] {
        auto raft = _rafts[index];
        Raft::Bitmask flags = raft->_fsm->flags();
        if(flags & Raft::FLAGS_LEADER) {
            raft->becomeLeader();
        } else if(flags & Raft::FLAGS_FOLLOWER) {
            raft->becomeFollower();
        } else {
            raft->becomeCandidate();
        }
    });
}

inline void Config::disconnect(int index) {
    CXXRAFT_LOG_DEBUG("disconnect server", index, "to virtual network");
    auto raft = _rafts[index];
    CXXRAFT_LOG_DEBUG("server info:", raft->simpleInfo());
    auto &pServer = raft->_rpcServer;
    if(!pServer) {
        CXXRAFT_LOG_WTF("no server");
        return;
    }
    pServer->onRequest([](auto&&) {
        return false;
    });
    _connected[index] = false;
    // cancel all jobs
    raft->updateTransaction();
    raft->_callDisabled = true;
}

inline void Config::abort() {
    // actually sleep
    ::usleep(50 * 1000);
    // acutally abort execution
    ::abort();
}

inline int Config::checkOneLeader() {
    CXXRAFT_LOG_DEBUG("checkOneLeader");
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
            CXXRAFT_LOG_DEBUG("last term:", lastTermWithLeader, "leader:", leaders[lastTermWithLeader][0]);
            return leaders[lastTermWithLeader][0];
        }
    }

    CXXRAFT_LOG_WTF("expected one leader, got none");
    abort();
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
                abort();
            }
        } else {
            CXXRAFT_LOG_DEBUG("server", i, "disconnected");
        }
    }
    return term;
}

inline auto Config::nCommitted(int index) -> std::tuple<int, Command> {
    int count = 0;
    Command cmd;
    for(int i = 0; i < _rafts.size(); ++i) {
        std::optional<Log::Entry> pEntry
            { _rafts[i]->_log.get(index, Log::Optional{}) };
        if(pEntry) {
            auto &[_, cmd1] = *pEntry;
            if(count > 0 && !equal(cmd1, cmd)) {
                CXXRAFT_LOG_WTF("committed values do not match: index",
                    index);
                abort();
            }
            count++;
            cmd = cmd1;
        }
    }
    return {count, cmd};
}

inline int Config::one(Command command, int expectedServers, bool retry) {
    using namespace std::chrono_literals;
    auto now = std::chrono::system_clock::now;
    auto t0 = now();
    int starts = 0;
    while(now() - t0 < 10s) {
        // try all the servers, maybe one is the leader.
        int index = -1;
        for(size_t si = 0, n = _peers.size(); si < n; ++si) {
            starts = (starts + 1) % n;
            cxxraft::Raft *raft {};
            if(_connected[starts]) {
                raft = _rafts[starts].get();
            }
            if(raft) {
                auto [index1, _, ok] = raft->startCommand(command);
                if(ok) {
                    index = index1;
                    break;
                }
            }
        }

        if(index != -1) {
            // somebody claimed to be the leader and to have
            // submitted our command; wait a while for agreement.
            auto t1 = now();
            while(now() - t1 < 2s) {
                auto [nd, cmd1] = nCommitted(index);
                if(nd > 0 && nd >= expectedServers) {
                    // commited
                    if(equal(cmd1,command)) {
                        // and it was the command we submitted.
                        return index;
                    }
                }
                co::usleep(20 * 1000);
            }
            if(!retry) {
                CXXRAFT_LOG_WTF("one", command, "failed to reach agreement");
                abort();
            }
        } else {
            co::usleep(50 * 1000);
        }
        CXXRAFT_LOG_WTF("one", command, "failed to reach agreement");
        return -1;
    }
}


















inline bool Raft::State::updateLatestTerm(int term) {
    if(term > _master->_currentTerm) {
        CXXRAFT_LOG_DEBUG(_master->simpleInfo(), "update to latest term:", term);
        _master->_currentTerm = term;
        CXXRAFT_LOG_DEBUG(_master->simpleInfo(), "reset vote");
        _master->_voteFor = std::nullopt;
        CXXRAFT_LOG_DEBUG(_master->simpleInfo(), "post:", type(Literal{}), "-> follower");
        _master->statePost([this] {
            _master->becomeFollower();
        });
        return true;
    }
    return false;
}






















inline Leader::Leader(Raft *master)
    : Raft::State(master)
{
    _flags |= Raft::FLAGS_LEADER;
}

inline void Leader::onBecome(std::shared_ptr<Raft::State> previous) {
    _master->performHeartBeat();
}

inline Reply<int, bool> Leader::onAppendEntryRPC(int term, int leaderId,
                                                 int prevLogIndex, int prevLogTerm,
                                                 Log::EntriesArray entries, int leaderCommit) {

    CXXRAFT_LOG_DEBUG(_master->simpleInfo(), "onAppendEntryRPC: ", term, leaderId, prevLogIndex, prevLogTerm);

    // currently peers don't care about the reply

    if(!isValidTransaction()) {
        return std::make_tuple(Raft::JUNK_TERM, false);
    }

    if(term < _master->_currentTerm) {
        return std::make_tuple(_master->_currentTerm, false);
    }

    bool changed = updateLatestTerm(term);

    // World has changed
    if(changed) {
        return std::make_tuple(_master->_currentTerm, false);
    }

    CXXRAFT_LOG_WTF(_master->simpleInfo(), "I am the only leader in this term, you too?");

    return std::make_tuple(_master->_currentTerm, false);
}

inline Reply<int, bool> Leader::onRequestVoteRPC(int term, int candidateId,
                                                 int lastLogIndex, int lastLogTerm) {

    CXXRAFT_LOG_DEBUG(_master->simpleInfo(), "onRequestVoteRPC:", term, candidateId);

    if(!isValidTransaction()) {
        return std::make_tuple(Raft::JUNK_TERM, false);
    }

    // reject stale request
    if(term < _master->_currentTerm) {
        CXXRAFT_LOG_DEBUG(_master->simpleInfo(), "reject stale request, term:", term);
        return std::make_tuple(_master->_currentTerm, false);
    }

    // check stale leader
    updateLatestTerm(term);

    bool voteGranted = _master->vote(candidateId);

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

inline Reply<int, bool> Follower::onAppendEntryRPC(int term, int leaderId,
                                                   int prevLogIndex, int prevLogTerm,
                                                   Log::EntriesArray entries, int leaderCommit) {

    CXXRAFT_LOG_DEBUG(_master->simpleInfo(), "onAppendEntryRPC: ", term, leaderId, prevLogIndex, prevLogTerm);

    if(!isValidTransaction()) {
        return std::make_tuple(Raft::JUNK_TERM, false);
    }

    if(term < _master->_currentTerm) {
        return std::make_tuple(_master->_currentTerm, false);
    }

    ++(*_watchdog);

    updateLatestTerm(term);


    return std::make_tuple(_master->_currentTerm, false);
}

inline Reply<int, bool> Follower::onRequestVoteRPC(int term, int candidateId,
                                                   int lastLogIndex, int lastLogTerm) {

    CXXRAFT_LOG_DEBUG(_master->simpleInfo(), "onRequestVoteRPC:", term, candidateId);

    if(!isValidTransaction()) {
        return std::make_tuple(Raft::JUNK_TERM, false);
    }


    // reject stale request
    if(term < _master->_currentTerm) {
        CXXRAFT_LOG_DEBUG(_master->simpleInfo(), "reject stale request, term:", term);
        return std::make_tuple(_master->_currentTerm, false);
    }

    ++(*_watchdog);

    updateLatestTerm(term);

    bool voteGranted = _master->vote(candidateId);
    return std::make_tuple(_master->_currentTerm, voteGranted);
}

inline bool Follower::updateLatestTerm(int term) {
    if(term > _master->_currentTerm) {
        CXXRAFT_LOG_DEBUG(_master->simpleInfo(), "update to latest term:", term);
        _master->_currentTerm = term;
        CXXRAFT_LOG_DEBUG(_master->simpleInfo(), "reset vote");
        _master->_voteFor = std::nullopt;
        return true;
    }
    return false;
}




















inline Candidate::Candidate(Raft *master)
    : Raft::State(master) { _flags |= Raft::FLAGS_CANDIDATE; }

inline void Candidate::onBecome(std::shared_ptr<Raft::State> previous) {
    _master->performElection();
}

inline Reply<int, bool> Candidate::onAppendEntryRPC(int term, int leaderId,
                                                    int prevLogIndex, int prevLogTerm,
                                                    Log::EntriesArray entries, int leaderCommit) {

    CXXRAFT_LOG_DEBUG(_master->simpleInfo(), "onAppendEntryRPC:", term, leaderId, prevLogIndex, prevLogTerm);

    if(!isValidTransaction()) {
        return std::make_tuple(Raft::JUNK_TERM, false);
    }

    // @paper
    //
    // While waiting for votes, a candidate may receive an
    // AppendEntries RPC from another server claiming to be
    // leader. If the leader’s term (included in its RPC) is at least
    // **as large as** the candidate’s current term, then the candidate
    // recognizes the leader as legitimate and returns to follower
    // state.

    if(term < _master->_currentTerm) {
        return std::make_tuple(_master->_currentTerm, false);
    }


    if(term > _master->_currentTerm) {
        CXXRAFT_LOG_DEBUG(_master->simpleInfo(), "update to latest term:", term);
        // update latest(largest) term candidate has seen
        // but paper said the result of AppendEntries RPC is "for **leader** to update itself" only?
        _master->_currentTerm = term;
        CXXRAFT_LOG_DEBUG(_master->simpleInfo(), "reset voteFor");
        _master->_voteFor = std::nullopt;
    }

    CXXRAFT_LOG_DEBUG(_master->simpleInfo(), "post: candidate -> follower.",
        "Reason: discovers current leader");

    _master->statePost([this] {
        _master->becomeFollower();
    });

    return std::make_tuple(_master->_currentTerm, true);

    // @paper
    //
    // If the term in the RPC is smaller than the candidate’s current term,
    // then the candidate rejects the RPC and continues in candidate state
    return std::make_tuple(_master->_currentTerm, false);
}

inline Reply<int, bool> Candidate::onRequestVoteRPC(int term, int candidateId,
                                                    int lastLogIndex, int lastLogTerm) {

    // Question.
    // candidates have voted to themselves
    // but an incoming new term will reset voteFor

    CXXRAFT_LOG_DEBUG(_master->simpleInfo(), "onRequestVoteRPC:", term, candidateId);

    if(!isValidTransaction()) {
        return std::make_tuple(Raft::JUNK_TERM, false);
    }

    // reject stale request
    if(term < _master->_currentTerm) {
        CXXRAFT_LOG_DEBUG(_master->simpleInfo(), "reject stale request, term:", term);
        return std::make_tuple(_master->_currentTerm, false);
    }

    updateLatestTerm(term);

    bool voteGranted = _master->vote(candidateId);
    CXXRAFT_LOG_DEBUG(_master->simpleInfo(), "onRequestVoteRPC result:", voteGranted);
    return std::make_tuple(_master->_currentTerm, voteGranted);
}

} // cxxraft
