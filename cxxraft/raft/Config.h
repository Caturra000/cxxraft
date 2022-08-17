#pragma once
#include "raft/Debugger.h"
#include "raft/Raft.h"
namespace cxxraft {

struct Config: private Debugger<Config> {

    struct EnablePersistent {};

    // nanosecond is enough
    using Uuid = std::chrono::nanoseconds::rep;

    Config(std::vector<trpc::Endpoint> peers);
    Config(std::vector<trpc::Endpoint> peers, EnablePersistent);
    static std::shared_ptr<Config> make(std::vector<trpc::Endpoint> peers);
    static std::shared_ptr<Config> make(std::vector<trpc::Endpoint> peers, EnablePersistent);

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

//////////// for 2C

    // shut down a Raft server but **NOT** save its persistent state.
    //
    // NOTE:
    // it is slightly different from 6.824.
    // `crash` does NOT explicitly save its persistent state
    // Raft server will restore commited log from WAL
    void crash(int id);

    // start or re-start a Raft.
    // if one already exists, "kill" it first.
    void start(int id);

    // wait for at least n servers to commit.
    // but don't wait forever.
    cxxraft::Command wait(int index, int n, int startTerm);

//////////// verbose

    void begin(const char *test) { std::cout << "begin: " << test << std::endl; }
    void end() { std::cout << "done" << std::endl;}

    // Whether each server is on the net
    std::vector<char> _connected {};

    std::vector<trpc::Endpoint> _peers;

    std::map<int, std::shared_ptr<Raft>> _rafts;

    std::vector<std::shared_ptr<Raft>> _killed;

    bool _persistent;

    Uuid _uuid {std::chrono::system_clock::now().time_since_epoch().count()};

};

} // cxxraft
