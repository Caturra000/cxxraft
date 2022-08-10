#pragma once
#include "raft/Raft.h"
namespace cxxraft {

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
    //
    // Note: follower will not convert to follower again in cxxraft.
    // See Follower::followUp()
    virtual bool followUp(int term);

    // TODO move to Raft
    // prefer simple function rather than virtual interface
    virtual bool updateLog(int prevLogIndex, int prevLogTerm,
                           Log::EntriesArray entries, int leaderCommit);

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

    bool followUp(int term) override;

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

} // cxxraft
