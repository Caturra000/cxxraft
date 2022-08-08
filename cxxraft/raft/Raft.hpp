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

    int index = _log.size();

    // [index, term]
    Log::Metadata metadata = std::make_tuple(index, term);
    _log.append(metadata, std::move(command));

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
            auto [voted, rejected, _] = *voteInfo;
            // confirmed
            if(voted >= majority() || rejected >= majority()) {
                break;
            }
        }

        auto &[voted, rejected, aborted] = *voteInfo;

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

        // abort or optimize
        if(auto [voted, rejected, aborted] = *voteInfo;
            aborted || voted >= majority() || rejected >= majority()) {
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
        const auto &entry = _log.back();
        const auto &metadata = std::get<0>(entry);
        auto [lastLogIndex, lastLogTerm] = metadata.cast();
        auto reply = client->call<RequestVoteReply>(RAFT_REQUEST_VOTE_RPC, _currentTerm, _id,
                        lastLogIndex, lastLogTerm);

        // cancellation point after client call
        if(!isValidTransaction(transaction)) {
            return;
        }

        // check again after network IO
        if(auto [voted, rejected, aborted] = *voteInfo;
                aborted || voted >= majority() || rejected >= majority()) {
            CXXRAFT_LOG_DEBUG(simpleInfo(), "gatherVote return. info:", voted, rejected);
            return;
        }

        if(!reply) {
            CXXRAFT_LOG_DEBUG(simpleInfo(), "no reply in client");
            return;
        }

        auto [term, voteGranted] = reply->cast();
        CXXRAFT_LOG_DEBUG(simpleInfo(), "gatherVote. vote reply:", term, voteGranted);

        if(_fsm->followUp(term)) {
            return;
        }

        auto &[voted, rejected, aborted] = *voteInfo;

        if(aborted) {
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
    for(auto &&kv : _peers) {
        auto &env = co::open();
        auto id = kv.first;
        env.createCoroutine([=] {gatherVote(id);})
            ->resume();
    }

    return voteInfo;
}

inline void Raft::maintainAuthorityToClients(size_t transaction) {

    auto sendHeartbeat = [this, transaction](int id) {

        CXXRAFT_LOG_DEBUG(simpleInfo(), "maintainAuthorityToClients");

        if(!isValidTransaction(transaction)) return;

        auto &peer = _peers[id];

        auto client = trpc::Client::make(peer.endpoint);

        if(!client) return;

        if(!isValidTransaction(transaction)) return;

        // for config test
        if(callDisabled()) return;

        CXXRAFT_LOG_DEBUG(simpleInfo(), "maintainAuthority. call append entey:", _currentTerm, _id);

        // TODO generateAppendEntriesArguments()
        int nextIndex = peer.nextIndex;
        int prevLogIndex = nextIndex - 1;
        const auto &entry = _log.back();
        const auto &metadata = std::get<0>(entry);
        int prevLogTerm = std::get<1>(metadata);
        Log::EntriesSlice slice = _log.fork(nextIndex, nextIndex + 1, Log::ByReference{});
        auto reply = client->call<AppendEntryReply>(RAFT_APPEND_ENTRY_RPC, _currentTerm, _id,
            prevLogIndex, prevLogTerm, slice, _commitIndex);

        if(!isValidTransaction(transaction)) return;

        if(!reply) {
            CXXRAFT_LOG_DEBUG(simpleInfo(), "no reply in client");
        }

        auto [term, success] = reply->cast();

        if(_fsm->followUp(term)) {
            return;
        }

        // 1. Reply false if term < currentTerm (§5.1)
        // 2. Reply false if log doesn’t contain an entry at prevLogIndex
        //    whose term matches prevLogTerm (§5.3)
        if(!success /*&& term >= _currentTerm*/) {
            // follower(receiver) can be outdated
            // TODO update index
        }
    };

    for(auto &&kv : _peers) {
        auto &env = co::open();
        auto id = kv.first;
        env.createCoroutine([=] {sendHeartbeat(id);})
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
        // TODO match log
        CXXRAFT_LOG_DEBUG(simpleInfo(), "vote granted. vote for:", candidateId);
        _voteFor = candidateId;
        voteGranted = true;
    } else {
        CXXRAFT_LOG_DEBUG(simpleInfo(), "reject this vote. voted to:", *_voteFor);
    }
    return voteGranted;
}

} // cxxraft