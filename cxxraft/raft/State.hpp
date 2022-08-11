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

inline bool Raft::State::updateLog(int prevLogIndex, int prevLogTerm, Log::EntriesArray entries, int leaderCommit) {

    CXXRAFT_LOG_DEBUG(_master->simpleInfo(), "updateLog:", _master->dump(entries));

    Log::Entry *pEntry;

    // Reply false if log doesn’t contain an entry at prevLogIndex
    // whose term matches prevLogTerm
    pEntry = _master->_log.get(prevLogIndex, Log::ByPointer{});
    if(pEntry && Log::getTerm(*pEntry) != prevLogTerm) {
        CXXRAFT_LOG_DEBUG(_master->simpleInfo(), "reply false. prevLogTerm dismatch.",
            "prevLogIndex:", prevLogIndex,
            "prevLogTerm:", prevLogTerm,
            "localLogTerm:", Log::getTerm(*pEntry));
        return false;
    }

    // If an existing entry conflicts with a new one (same index
    // but different terms), delete the existing entry and all that
    // follow it (§5.3)
    for(auto &&remoteEntry : entries) {
        int index = Log::getIndex(remoteEntry);
        int remoteTerm = Log::getTerm(remoteEntry);
        pEntry = _master->_log.get(index, Log::ByPointer{});
        if(pEntry && Log::getTerm(*pEntry) != remoteTerm) {
            CXXRAFT_LOG_DEBUG(_master->simpleInfo(), "conflict log, index:", index,
                "last:", _master->_log.lastIndex(),
                "local entries:", _master->dump(_master->_log.fork()));

            _master->_log.truncate(index);

            CXXRAFT_LOG_DEBUG(_master->simpleInfo(), "truncated:", index,
                "last:", _master->_log.lastIndex(),
                "local entries:", _master->dump(_master->_log.fork()));
            break;
        }
    }

    // Append
    for(auto &&entry : entries) {
        // Note: `nextIndex` and `matchIndex` are volatile
        if(_master->_log.lastIndex() >= Log::getIndex(entry)) {
            continue;
        }
        _master->_log.append(std::move(entry));
    }

    return true;
}

inline void Raft::State::updateCommitIndexForReceiver(int leaderCommit) {
    // If leaderCommit > commitIndex, set commitIndex =
    // min(leaderCommit, index of last new entry)
    if(leaderCommit > _master->_commitIndex) {
        _master->_commitIndex = std::min(leaderCommit, _master->_log.lastIndex());
        CXXRAFT_LOG_DEBUG(_master->simpleInfo(), "got latest commit index:", _master->_commitIndex);
        CXXRAFT_LOG_DEBUG(_master->simpleInfo(), "dump log", _master->dump(_master->_log.fork()));
        // _lastApplied...
    }
}




















inline Leader::Leader(Raft *master)
    : Raft::State(master)
{
    _flags |= Raft::FLAGS_LEADER;
}

inline void Leader::onBecome(std::shared_ptr<Raft::State> previous) {
    for(auto &&[id, peer] : _master->_peers) {
        // initialized to leader last log index + 1
        peer.nextIndex = _master->_log.lastIndex() + 1;
        // initialized to 0, increases monotonically
        peer.matchIndex = 0;
    }
    _master->performHeartBeat();
}

inline Reply<int, bool> Leader::onAppendEntryRPC(int term, int leaderId,
                                                 int prevLogIndex, int prevLogTerm,
                                                 Log::EntriesArray entries, int leaderCommit) {

    CXXRAFT_LOG_DEBUG(_master->simpleInfo(), "onAppendEntryRPC: ", term, leaderId, prevLogIndex, prevLogTerm);

    // TODO delete
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
        // if emtpy entry, return true for up-to-date heartbeat
        // else return what updateLog() returns
        bool ret = entries.empty() ||
            updateLog(prevLogIndex, prevLogTerm, std::move(entries), leaderCommit);

        updateCommitIndexForReceiver(leaderCommit);
        return std::make_tuple(_master->_currentTerm, ret);
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

    bool voteGranted = _master->vote(candidateId, lastLogIndex, lastLogTerm);

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

    bool ret = entries.empty() ||
        updateLog(prevLogIndex, prevLogTerm, std::move(entries), leaderCommit);

    updateCommitIndexForReceiver(leaderCommit);

    return std::make_tuple(_master->_currentTerm, ret);
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

    followUp(term);

    bool voteGranted = _master->vote(candidateId, lastLogIndex, lastLogTerm);
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

    bool changed = followUp(term);

    // sender is a new (and same-term) leader
    if(!changed) {
        _master->statePost([this] {
            _master->becomeFollower();
        });
    }

    bool ret = entries.empty() ||
        updateLog(prevLogIndex, prevLogTerm, std::move(entries), leaderCommit);

    updateCommitIndexForReceiver(leaderCommit);

    return std::make_tuple(_master->_currentTerm, ret);
}

inline Reply<int, bool> Candidate::onRequestVoteRPC(int term, int candidateId,
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

    followUp(term);

    bool voteGranted = _master->vote(candidateId, lastLogIndex, lastLogTerm);
    CXXRAFT_LOG_DEBUG(_master->simpleInfo(), "onRequestVoteRPC result:", voteGranted);
    return std::make_tuple(_master->_currentTerm, voteGranted);
}

} // cxxraft
