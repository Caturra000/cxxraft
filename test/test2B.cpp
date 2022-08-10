#include "cxxraft.hpp"
#include "co.hpp"
#include "test.h"

void testBasicAgree2B() {
    auto peers = createPeers(3);
    int servers = peers.size();
    auto config = cxxraft::Config::make(peers);
    for(size_t i = 0; i < peers.size(); ++i) {
        auto raft = cxxraft::Raft::make(*config, i);
        raft->start();
    }
    config->begin("Test (2B): basic agreement");

    int iters = 3;
    for(int index = 1; index < iters + 1; index++) {
        auto [nd, _] = config->nCommitted(index);
        if(nd > 0) {
            std::cerr << "some have committed before startCommand()" << std::endl;
            config->abort();
        }

        cxxraft::Command cmd;
        cmd["op"] = index * 100;
        auto xindex = config->one(cmd, servers, false);
        if(xindex != index) {
            std::cerr << "got index " << xindex << " but expected " << index << std::endl;
            config->abort();
        }
    }

    config->end();
    ::exit(0);
}

void testFailAgree2B() {
    int servers = 3;
    auto peers = createPeers(servers);
    auto config = cxxraft::Config::make(peers);
    for(size_t i = 0; i < peers.size(); ++i) {
        auto raft = cxxraft::Raft::make(*config, i);
        raft->start();
    }

    config->begin("Test (2B): agreement despite follower disconnection");

    cxxraft::Command cmd;
    int op = 101;
    cmd["op"] = op;
    config->one(cmd, servers, false);

    // disconnect one follower from the network.
    auto leader = config->checkOneLeader();
    config->disconnect((leader + 1) % servers);

    // the leader and remaining follower should be
    // able to agree despite the disconnected follower.
    cmd["op"] = ++op;
    config->one(cmd, servers - 1, false);
    cmd["op"] = ++op;
    config->one(cmd, servers - 1, false);
    co::poll(nullptr, 0, cxxraft::Raft::RAFT_ELECTION_TIMEOUT.count());
    cmd["op"] = ++op;
    config->one(cmd, servers - 1, false);
    cmd["op"] = ++op;
    config->one(cmd, servers - 1, false);

    // re-connect
    config->connect((leader + 1) % servers);

    // the full set of servers should preserve
    // previous agreements, and be able to agree
    // on new commands.
    cmd["op"] = ++op;
    config->one(cmd, servers, true);
    co::poll(nullptr, 0, cxxraft::Raft::RAFT_ELECTION_TIMEOUT.count());
    cmd["op"] = ++op;
    config->one(cmd, servers, true);

    config->end();
    ::exit(0);
}

void testFailNoAgree2B() {
    int servers = 5;
    auto peers = createPeers(servers);
    auto config = cxxraft::Config::make(peers);

    for(size_t i = 0; i < peers.size(); ++i) {
        auto raft = cxxraft::Raft::make(*config, i);
        raft->start();
    }

    config->begin("Test (2B): no agreement if too many followers disconnect");

    cxxraft::Command cmd;
    cmd["op"] = 10;

    config->one(cmd, servers, false);

    // 3 of 5 followers disconnect
    auto leader = config->checkOneLeader();
    config->disconnect((leader + 1) % servers);
    config->disconnect((leader + 2) % servers);
    config->disconnect((leader + 3) % servers);

    cmd["op"] = 20;
    auto [index, _1, ok] = config->_rafts[leader]->startCommand(cmd);
    if(!ok) {
        std::cerr << "leader rejected Start()" << std::endl;
        config->abort();
    }
    if(index != 2) {
        std::cerr << "expected index 2, got " << index << std::endl;
        config->abort();
    }

    co::poll(nullptr, 0, 2 * cxxraft::Raft::RAFT_ELECTION_TIMEOUT.count());

    auto [n, _2] = config->nCommitted(index);
    if(n > 0) {
        std::cerr << n << " committed but no majority" << std::endl;
        config->abort();
    }

    // repair
    config->connect((leader + 1) % servers);
    config->connect((leader + 2) % servers);
    config->connect((leader + 3) % servers);

    // the disconnected majority may have chosen a leader from
    // among their own ranks, forgetting index 2.
    auto leader2 = config->checkOneLeader();
    cmd["op"] = 30;
    auto [index2, _3, ok2] = config->_rafts[leader2]->startCommand(cmd);
    if(!ok2) {
        std::cerr << "leader2 rejected startCommand()" << std::endl;
        config->abort();
    }
    if(index2 < 2 || index2 > 3) {
        std::cerr << "unexpected index " << index2 << std::endl;
        config->abort();
    }

    cmd["op"] = 1000;
    config->one(cmd, servers, true);

    config->end();
    ::exit(0);
}

void testRejoin2B() {
    int servers = 3;
    auto peers = createPeers(servers);
    auto config = cxxraft::Config::make(peers);

    for(size_t i = 0; i < peers.size(); ++i) {
        auto raft = cxxraft::Raft::make(*config, i);
        raft->start();
    }

    config->begin("Test (2B): rejoin of partitioned leader");

    cxxraft::Command cmd;
    cmd["op"] = 101;
    config->one(cmd, servers, true);

    // leader network failure
    auto leader1 = config->checkOneLeader();
    config->disconnect(leader1);

    // make old leader try to agree on some entries
    cmd["op"] = 102;
    config->_rafts[leader1]->startCommand(cmd);
    cmd["op"] = 103;
    config->_rafts[leader1]->startCommand(cmd);
    cmd["op"] = 104;
    config->_rafts[leader1]->startCommand(cmd);

    // new leader commits, also for index=2

    cmd["op"] = 103;
    config->one(cmd, 2, true);

    // new leader network failure
    auto leader2 = config->checkOneLeader();
    config->disconnect(leader2);

    // old leader connected again
    config->connect(leader1);

    cmd["op"] = 104;
    config->one(cmd, 2, true);

    // all together now
    config->connect(leader2);

    cmd["op"] = 105;
    config->one(cmd, servers, true);

    config->end();
    ::exit(0);
}

void testBackup2B() {
    int servers = 5;
    auto peers = createPeers(servers);
    auto config = cxxraft::Config::make(peers);

    for(size_t i = 0; i < peers.size(); ++i) {
        auto raft = cxxraft::Raft::make(*config, i);
        raft->start();
    }

    config->begin("Test (2B): leader backs up quickly over incorrect follower logs");

    cxxraft::Command cmd;
    cmd["op"] = ::rand();
    config->one(cmd, servers, true);

    // put leader and one follower in a partition
    auto leader1 = config->checkOneLeader();
    config->disconnect((leader1 + 2) % servers);
    config->disconnect((leader1 + 3) % servers);
    config->disconnect((leader1 + 4) % servers);

    // submit lots of commands that won't commit
    for(int i = 0; i < 50; i++) {
        cmd["op"] = ::rand();
        config->_rafts[leader1]->startCommand(cmd);
    }

    co::poll(nullptr, 0, cxxraft::Raft::RAFT_ELECTION_TIMEOUT.count() / 2);

    config->disconnect((leader1 + 0) % servers);
    config->disconnect((leader1 + 1) % servers);

    // allow other partition to recover
    config->connect((leader1 + 2) % servers);
    config->connect((leader1 + 3) % servers);
    config->connect((leader1 + 4) % servers);

    // lots of successful commands to new group.
    for(int i = 0; i < 50; i++) {
        cmd["op"] = ::rand();
        config->one(cmd, 3, true);
    }

    // now another partitioned leader and one follower
    auto leader2 = config->checkOneLeader();
    auto other = (leader1 + 2) % servers;
    if(leader2 == other) {
        other = (leader2 + 1) % servers;
    }
    config->disconnect(other);

    // lots more commands that won't commit
    for(int i = 0; i < 50; i++) {
        cmd["op"] = ::rand();
        config->_rafts[leader2]->startCommand(cmd);
    }

    co::poll(nullptr, 0, cxxraft::Raft::RAFT_ELECTION_TIMEOUT.count() / 2);

    // bring original leader back to life,
    for(int i = 0; i < servers; i++) {
        config->disconnect(i);
    }
    config->connect((leader1 + 0) % servers);
    config->connect((leader1 + 1) % servers);
    config->connect(other);

    // lots of successful commands to new group.
    for(int i = 0; i < 50; i++) {
        cmd["op"] = ::rand();
        config->one(cmd, 3, true);
    }

    // now everyone
    for(int i = 0; i < servers; i++) {
        config->connect(i);
    }
    cmd["op"] = ::rand();
    config->one(cmd, servers, true);

    config->end();
    ::exit(0);
}

int main() {

}