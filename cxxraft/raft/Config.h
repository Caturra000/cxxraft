#pragma once
#include "raft/Debugger.h"
#include "raft/Raft.h"
namespace cxxraft {

struct Config: private Debugger<Config> {

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

//////////// verbose

    void begin(const char *test) { std::cout << "begin: " << test << std::endl; }
    void end() { std::cout << "done" << std::endl;}

    // Whether each server is on the net
    std::vector<char> _connected {};

    std::vector<trpc::Endpoint> _peers;

    std::vector<std::shared_ptr<Raft>> _rafts;

};

} // cxxraft
