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
#include "raft/Peer.h"
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
    // agreement and return **immediately**. there is **no guarantee** that this
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
    // return: [voted, rejected, aborted]
    auto gatherVotesFromClients(size_t transaction) -> std::shared_ptr<std::tuple<int, int, bool>>;

    void maintainAuthorityToClients(size_t transaction);

    void updateTransaction();

    size_t getTransaction();

    bool isValidTransaction(size_t transaction);

    // return: voteGranted
    bool vote(int candidateId, int candidateLastLogIndex, int candidateLastLogTerm);

// for log replication
private:

    void applyEntry(int index);

    int appendEntry(Command command);

    // The second property(If two entries in different logs have the same index
    // and term, then the logs are identical in all preceding
    // entries) is guaranteed by a simple consistency check performed by AppendEntries
    // void consistencyCheck();

    // For leader:
    //
    // If there exists an N such that N > commitIndex, a majority
    // of matchIndex[i] ≥ N, and log[N].term == currentTerm:
    // set commitIndex = N (§5.3, §5.4).
    void updateCommitIndexForSender();

    std::optional<Log::Entry> getCommittedCopy(int index);

    bool updateLog(int prevLogIndex, int prevLogTerm, Log::EntriesArray entries);

    void updateCommitIndexForReceiver(int leaderCommit);

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
    // std::vector<trpc::Endpoint> _peers;
    std::map<int, Peer> _peers;

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

    // index of highest log entry known to be committed
    int _commitIndex {};

    // index of highest log entry applied to state machine
    // Note: unused!
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

} // cxxraft
