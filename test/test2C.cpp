#include "cxxraft.hpp"
#include "co.hpp"
#include "test.h"

void testPersist12C() {
    int servers = 3;
    auto peers = createPeers(servers);
    auto enablePersistent = cxxraft::Config::EnablePersistent {};
    auto config = cxxraft::Config::make(peers, enablePersistent);
    for(size_t i = 0; i < peers.size(); ++i) {
        config->start(i);
    }

    config->begin("Test (2C): basic persistence");

    cxxraft::Command cmd;

    cmd["op"] = 11;
    config->one(cmd, servers, true);

    // crash and re-start all
    for(int i = 0; i < servers; ++i) {
        config->start(i);
    }

    cmd["op"] = 12;
    config->one(cmd, servers, true);

    auto leader1 = config->checkOneLeader();
    config->start(leader1);

    cmd["op"] = 13;
    config->one(cmd, servers, true);

    auto leader2 = config->checkOneLeader();
    config->disconnect(leader2);
    cmd["op"] = 14;
    config->one(cmd, servers-1, true);
    config->start(leader2);

    // wait for leader2 to join before killing i3
    config->wait(4, servers, -1);

    auto i3 = (config->checkOneLeader() + 1) % servers;
    config->disconnect(i3);
    cmd["op"] = 15;
    config->one(cmd, servers-1, true);
    config->start(i3);
    cmd["op"] = 16;
    config->one(cmd, servers, true);

    config->end();
    ::exit(0);
}

void testPersist22C() {
    int servers = 5;
    auto peers = createPeers(servers);
    auto enablePersistent = cxxraft::Config::EnablePersistent {};
    auto config = cxxraft::Config::make(peers, enablePersistent);
    for(size_t i = 0; i < peers.size(); ++i) {
        config->start(i);
    }

    config->begin("Test (2C): more persistence");

    int index = 1;
    cxxraft::Command cmd;
    for(int iters = 0; iters < 5; iters++) {
        cmd["op"] = 10 + index;
        config->one(cmd, servers, true);
        index++;

        auto leader1 = config->checkOneLeader();

        config->disconnect((leader1 + 1) % servers);
        config->disconnect((leader1 + 2) % servers);

        cmd["op"] = 10 + index;
        config->one(cmd, servers - 2, true);
        index++;

        config->disconnect((leader1 + 0) % servers);
        config->disconnect((leader1 + 3) % servers);
        config->disconnect((leader1 + 4) % servers);

        config->start((leader1 + 1) % servers);
        config->start((leader1 + 2) % servers);

        co::poll(nullptr, 0, cxxraft::Raft::RAFT_ELECTION_TIMEOUT.count());

        config->start((leader1 + 3) % servers);

        // XXXXXXX
        cmd["op"] = 10 + index;
        config->one(cmd, servers - 2, true);
        index++;

        config->connect((leader1 + 4) % servers);
        config->connect((leader1 + 0) % servers);
    }

    cmd["op"] = 1000;
    config->one(cmd, servers, true);

    config->end();
    ::exit(0);
}

void testPersist32C() {
    int servers = 3;
    auto peers = createPeers(servers);
    auto enablePersistent = cxxraft::Config::EnablePersistent {};
    auto config = cxxraft::Config::make(peers, enablePersistent);
    for(size_t i = 0; i < peers.size(); ++i) {
        config->start(i);
    }

    config->begin("Test (2C): partitioned leader and one follower crash, leader restarts");

    cxxraft::Command cmd;

    cmd["op"] = 101;
    config->one(cmd, 3, true);

    auto leader = config->checkOneLeader();
    config->disconnect((leader + 2) % servers);

    cmd["op"] = 102;
    config->one(cmd, 2, true);

    config->crash((leader + 0) % servers);
    config->crash((leader + 1) % servers);
    config->connect((leader + 2) % servers);
    config->start((leader + 0) % servers);

    cmd["op"] = 103;
    config->one(cmd, 2, true);

    config->start((leader + 1) % servers);

    cmd["op"] = 104;
    config->one(cmd, servers, true);

    config->end();
    ::exit(0);
}


int main() {

    TestFunction tests[] {
        testPersist12C,
        testPersist22C,
        testPersist32C
    };

    constexpr auto round = 10;

    runTestsAndReport(tests, round);

    return 0;
}
