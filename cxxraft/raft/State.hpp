#pragma once
#include "raft/State.h"
namespace cxxraft {

inline bool Raft::State::followUp(int term) {
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
    // TODO log index init
    for(auto &&[id, peer] : _master->_peers) {
        // initialized to leader last log index + 1
        peer.matchIndex = _master->_log.size();
        // initialized to 0, increases monotonically
        peer.nextIndex = 0;
    }
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

    bool changed = followUp(term);

    // World has changed
    if(changed) {
        return std::make_tuple(_master->_currentTerm, true);
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
    followUp(term);

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

    followUp(term);


    // TODO log

    return std::make_tuple(_master->_currentTerm, true);
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

    followUp(term);

    bool voteGranted = _master->vote(candidateId);
    return std::make_tuple(_master->_currentTerm, voteGranted);
}

inline bool Follower::followUp(int term) {
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

    if(followUp(term)) {
        return std::make_tuple(_master->_currentTerm, true);
    }

    // leader && same term
    _master->statePost([this] {
        _master->becomeFollower();
    });

    return std::make_tuple(_master->_currentTerm, true);
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

    followUp(term);

    bool voteGranted = _master->vote(candidateId);
    CXXRAFT_LOG_DEBUG(_master->simpleInfo(), "onRequestVoteRPC result:", voteGranted);
    return std::make_tuple(_master->_currentTerm, voteGranted);
}

} // cxxraft
