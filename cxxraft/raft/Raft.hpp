#pragma once
#include "raft/Raft.h"
#include "raft/Config.h"
#include "raft/State.h"
namespace cxxraft {

inline Raft::Raft(Config &config, int id)
    : _self(config._peers[id]),
      _id(id),
      _currentTerm(0),
      _fsm(nullptr),
      _transaction(0)
{
    for(size_t i = 0; i < config._peers.size(); i++) {
        if(i != id) {
            _peers.try_emplace(i, i, config._peers[i]);
        }
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

    int index = appendEntry(std::move(command));

    return {index, term, true};
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
        auto &[voted, rejected, aborted] = *voteInfo;

        // Try to poll faster
        constexpr static size_t NOP_COUNT = 5;
        for(size_t nop = NOP_COUNT; nop--;) {
            CXXRAFT_LOG_DEBUG(simpleInfo(), "co::sleep down");
            co::usleep(dist(engine) / NOP_COUNT);
            CXXRAFT_LOG_DEBUG(simpleInfo(), "co::sleep up");
            if(!isValidTransaction(transaction)) {
                CXXRAFT_LOG_DEBUG(simpleInfo(), "invalid transaction");
                return;
            }
            // confirmed
            if(voted >= majority() || rejected >= majority()) {
                break;
            }
        }

        aborted = true;

        CXXRAFT_LOG_DEBUG(simpleInfo(), "gathered:", voted, rejected);

        if(voted >= majority()) {
            break;
        }

        if(rejected < majority()) {
            CXXRAFT_LOG_DEBUG(simpleInfo(), "times out, new election");
        } else {
            CXXRAFT_LOG_DEBUG(simpleInfo(), "rejected, new election");
        }
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

inline auto Raft::gatherVotesFromClients(size_t transaction) -> std::shared_ptr<std::tuple<int, int, bool>> {

    CXXRAFT_LOG_DEBUG(simpleInfo(), "gatherVotesFromClients");

    // info: [voted, rejected, aborted]
    // 1: vote for itself
    auto voteInfo = std::make_shared<std::tuple<int, int, bool>>(1, 0, false);

    auto gatherVote = [this, voteInfo, transaction](int id) {

        CXXRAFT_LOG_DEBUG(simpleInfo(), "gatherVote, id:", id);

        if(!isValidTransaction(transaction)) {
            CXXRAFT_LOG_DEBUG(simpleInfo(), "invalid transaction");
            return;
        }

        auto &peer = _peers[id];

        auto &[voted, rejected, aborted] = *voteInfo;

        // abort or optimize
        if(aborted || voted >= majority() || rejected >= majority()) {
            CXXRAFT_LOG_DEBUG(simpleInfo(), "gatherVote return. info:", voted, rejected);
            return;
        }

        auto client = trpc::Client::make(peer.endpoint);

        if(!client) {
            return;
        }

        // cancellation point after client connect
        if(!isValidTransaction(transaction)) {
            return;
        }

        CXXRAFT_LOG_DEBUG(simpleInfo(), "call requset vote:", _currentTerm, _id);

        // for config test
        if(callDisabled()) {
            return;
        }

        // TODO snapshot
        auto [lastLogIndex, lastLogTerm] = [this] {
            // hide unnecessary symbols exposed in function
            const auto &entry = _log.back();
            const auto &metadata = std::get<0>(entry);
            return metadata;
        } ().cast();

        auto reply = client->call<RequestVoteReply>(RAFT_REQUEST_VOTE_RPC, _currentTerm, _id,
                        lastLogIndex, lastLogTerm);

        // cancellation point after client call
        if(!isValidTransaction(transaction)) {
            return;
        }

        // check again after network IO
        if(aborted || voted >= majority() || rejected >= majority()) {
            CXXRAFT_LOG_DEBUG(simpleInfo(), "gatherVote return. info:", voted, rejected);
            return;
        }

        if(!reply) {
            CXXRAFT_LOG_DEBUG(simpleInfo(), "no reply in client");
            return;
        }

        auto [term, voteGranted] = reply->cast();
        CXXRAFT_LOG_DEBUG(simpleInfo(), "gatherVote. vote reply:", term, voteGranted);

        // junk
        if(term < 0) {
            return;
        }

        if(_fsm->followUp(term)) {
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

    for(auto &&kv : _peers) {
        auto &env = co::open();
        auto id = kv.first;
        env.createCoroutine([=] {gatherVote(id);})
            ->resume();
    }

    return voteInfo;
}

inline void Raft::maintainAuthorityToClients(size_t transaction) {

    auto ping = [this, transaction](int id) {

        CXXRAFT_LOG_DEBUG(simpleInfo(), "maintainAuthorityToClients");

        if(!isValidTransaction(transaction)) return;

        auto &peer = _peers[id];

        auto client = trpc::Client::make(peer.endpoint);

        if(!client) return;

        if(!isValidTransaction(transaction)) return;

        // for config test
        if(callDisabled()) return;

        CXXRAFT_LOG_DEBUG(simpleInfo(), "maintainAuthority. call append entey:", _currentTerm, _id);

        // TODO conflict log

        // If last log index ≥ nextIndex for a follower: send
        // AppendEntries RPC with log entries starting at nextIndex
        int prevLogIndex = 0;
        int prevLogTerm = 0;
        Log::EntriesSlice slice;
        if(_log.lastIndex() >= peer.nextIndex) {
            int nextIndex = peer.nextIndex;
            prevLogIndex = nextIndex - 1;
            prevLogTerm = Log::getTerm(_log.get(prevLogIndex));
            slice = _log.fork(nextIndex, nextIndex + 1, Log::ByReference{});
            CXXRAFT_LOG_DEBUG(simpleInfo(), "send AppendEntries RPC with log entries starting at",
                "nextIndex:", nextIndex,
                "to node:", id,
                "entries:", dump(slice));
        }

        auto reply = client->call<AppendEntryReply>(RAFT_APPEND_ENTRY_RPC, _currentTerm, _id,
            prevLogIndex, prevLogTerm, slice, _commitIndex);

        if(!isValidTransaction(transaction)) return;

        if(!reply) {
            CXXRAFT_LOG_DEBUG(simpleInfo(), "no reply in client");
        }

        auto [term, success] = reply->cast();

        // junk
        if(term < 0) {
            return;
        }

        if(_fsm->followUp(term)) {
            return;
        }

        // * If successful: update nextIndex and matchIndex for
        //   follower (§5.3)
        // * If AppendEntries fails because of log inconsistency:
        //   decrement nextIndex and retry (§5.3)

        // Note. ignore old term commit
        if(success) {
            peer.matchIndex = std::max(peer.matchIndex, peer.nextIndex);
            peer.nextIndex = std::min(peer.nextIndex + 1, _log.lastIndex() + 1);
            // FIXME performance
            updateCommitIndex();
        } else {
            CXXRAFT_LOG_DEBUG(simpleInfo(), "AppendEntries fails because of log inconsistency. peer id:", id);
            if(peer.nextIndex <= 0) {
                CXXRAFT_LOG_WTF(simpleInfo(), "empty log failed? nextIndex:", peer.nextIndex);
            }
            peer.nextIndex--;
        }
    };

    for(auto &&kv : _peers) {
        auto &env = co::open();
        auto id = kv.first;
        env.createCoroutine([=] {ping(id);})
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

inline bool Raft::vote(int candidateId, int candidateLastLogIndex, int candidateLastLogTerm) {
    bool voteGranted = false;
    // candidate is as least as up-to-date
    // compared by last log
    auto asLeastAsUpToDate = [this, candidateLastLogIndex, candidateLastLogTerm] {
        const auto &myLastLog = _log.back();
        const auto &metadata = std::get<0>(myLastLog);
        auto [index, term] = metadata.cast();

        return candidateLastLogTerm > term ||
            (candidateLastLogTerm == term && candidateLastLogIndex >= index);
    };
    // If votedFor is null or candidateId, and candidate’s log is at
    // least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
    if((!_voteFor || *_voteFor == candidateId) && asLeastAsUpToDate()) {
        CXXRAFT_LOG_DEBUG(simpleInfo(), "vote granted. vote for:", candidateId);
        _voteFor = candidateId;
        voteGranted = true;
    } else {
        CXXRAFT_LOG_DEBUG(simpleInfo(), "reject this vote. voted to:", *_voteFor);
    }
    return voteGranted;
}

inline void Raft::applyEntry(int index) {
    _lastApplied = index;
}

inline int Raft::appendEntry(Command command) {
    int nextIndex = _log.lastIndex() + 1;
    Log::Metadata metadata = std::make_tuple(nextIndex, _currentTerm);
    _log.append(metadata, std::move(command));
    return nextIndex;
}

inline void Raft::updateCommitIndex() {
    // performed by leader

    // a stupid O(n) algorithm
    // TODO binary search

    int last = _log.lastIndex();
    auto isMajorityMatch = [this](int index) {
        // 1 : leader himself included (pre-committed)
        int committed = 1;
        for(auto &&[id, peer] : _peers) {
            if(peer.matchIndex >= index && ++committed >= majority()) {
                return true;
            }
        }
        return committed >= majority();
    };
    for(int N = last; N > _commitIndex; N--) {
        const auto &entry = _log.get(N);
        const auto &metadata = std::get<0>(entry);
        auto [_, term] = metadata.cast();
        // §5.4 Safety
        // a leader cannot determine commitment
        // using log entries from older terms
        if(term < _currentTerm) {
            break;
        }
        if(isMajorityMatch(N)) {
            _commitIndex = N;
            // TODO lastApplied

            CXXRAFT_LOG_DEBUG(simpleInfo(), "got latest commit index:", N);
            break;
        }
    }
}

} // cxxraft
