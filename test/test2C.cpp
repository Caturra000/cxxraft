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

// Test the scenarios described in Figure 8 of the extended Raft paper. Each
// iteration asks a leader, if there is one, to insert a command in the Raft
// log.  If there is a leader, that leader will fail quickly with a high
// probability (perhaps without committing the command), or crash after a while
// with low probability (most likey committing the command).  If the number of
// alive servers isn't enough to form a majority, perhaps start a new server.
// The leader in a new term may try to finish replicating log entries that
// haven't been committed yet.
void testFigure82C() {
    int servers = 5;
    auto peers = createPeers(servers);
    auto enablePersistent = cxxraft::Config::EnablePersistent {};
    auto config = cxxraft::Config::make(peers, enablePersistent);
    for(size_t i = 0; i < peers.size(); ++i) {
        config->start(i);
    }

    config->begin("Test (2C): Figure 8");

    cxxraft::Command cmd;

    int op = 1;

    cmd["op"] = op++;
    config->one(cmd, 1, true);

    int nup = servers;
    for(int iters = 0; iters < 1000; iters++) {
        int leader = -1;
        for(int i = 0; i < servers; i++) {
            if(!config->_killed[i]) {
                auto raft = config->_rafts[i];
                cmd["op"] = op++;
                auto [_1, _2, ok] = raft->startCommand(cmd);
                if(ok) {
                    leader = i;
                }
            }
        }

        if((::rand() % 1000) < 100) {
            auto ms = ::rand() % (cxxraft::Raft::RAFT_ELECTION_TIMEOUT.count() / 2);
            co::poll(nullptr, 0, ms);
        } else {
            auto ms = ::rand() % 13;
            co::poll(nullptr, 0, ms);
        }

        if(leader != -1) {
            config->crash(leader);
            nup -= 1;
        }

        if(nup < 3) {
            int s = ::rand() % servers;
            if(config->_killed[s]) {
                config->start(s);
                nup += 1;
            }
        }
    }

    for(int i = 0; i < servers; i++) {
        if(config->_killed[i]) {
            config->start(i);
        }
    }

    cmd["op"] = op++;
    config->one(cmd, servers, true);

    config->end();
    ::exit(0);
}

void testUnreliableAgree2C() {
    int servers = 5;
    auto peers = createPeers(servers);
    auto config = cxxraft::Config::make(peers);
    config->setUnreliable();
    for(size_t i = 0; i < peers.size(); ++i) {
        config->start(i);
    }

    config->begin("Test (2C): unreliable agreement");

    int wg = 0;
    auto &env = co::open();
    cxxraft::Command cmd;

    for(int iters = 1; iters < 50; iters++) {
        for(int j = 0; j < 4; j++) {
            wg++;
            env.createCoroutine([&, iters, j] {
                cxxraft::Command cmd;
                cmd["op"] = 100 * iters + j;
                config->one(cmd, 1, true);
                wg--;
            })->resume();
        }
        cmd["op"] = iters;
        config->one(cmd, 1, true);
    }

    config->setReliable();

    while(wg) {
        co::poll(nullptr, 0, 50);
    }

    cmd["op"] = 100;
    config->one(cmd, servers, true);

    config->end();
    ::exit(0);
}

void testFigure8Unreliable2C() {
    int servers = 5;
    auto peers = createPeers(servers);
    auto enablePersistent = cxxraft::Config::EnablePersistent {};
    auto config = cxxraft::Config::make(peers, enablePersistent);
    config->setUnreliable();
    for(size_t i = 0; i < peers.size(); ++i) {
        config->start(i);
    }

    config->begin("Test (2C): Figure 8 (unreliable)");

    cxxraft::Command cmd;

    int op = 1;

    cmd["op"] = op++;
    config->one(cmd, 1, true);

    int nup = servers;
    for(int iters = 0; iters < 1000; iters++) {
        int leader = -1;
        for(int i = 0; i < servers; i++) {
            if(!config->_killed[i]) {
                auto raft = config->_rafts[i];
                cmd["op"] = op++;
                auto [_1, _2, ok] = raft->startCommand(cmd);
                if(ok) {
                    leader = i;
                }
            }
        }

        if((::rand() % 1000) < 100) {
            auto ms = ::rand() % (cxxraft::Raft::RAFT_ELECTION_TIMEOUT.count() / 2);
            co::poll(nullptr, 0, ms);
        } else {
            auto ms = ::rand() % 13;
            co::poll(nullptr, 0, ms);
        }

        if(leader != -1) {
            config->crash(leader);
            nup -= 1;
        }

        if(nup < 3) {
            int s = ::rand() % servers;
            if(config->_killed[s]) {
                config->start(s);
                nup += 1;
            }
        }
    }

    for(int i = 0; i < servers; i++) {
        if(config->_killed[i]) {
            config->start(i);
        }
    }

    cmd["op"] = op++;
    config->one(cmd, servers, true);

    config->end();
    ::exit(0);
}

void internalChurn(bool unreliable) {
    int servers = 5;
    auto peers = createPeers(servers);
    auto enablePersistent = cxxraft::Config::EnablePersistent {};
    auto config = cxxraft::Config::make(peers, enablePersistent);

    if(unreliable) {
        config->setUnreliable();
    }
    for(size_t i = 0; i < peers.size(); ++i) {
        config->start(i);
    }

    if(unreliable) {
        config->begin("Test (2C): unreliable churn");
    } else {
        config->begin("Test (2C): churn");
    }

    int stop = 0;

    // create concurrent clients
    auto cfn = [=, &stop](int me, std::vector<int> &ch) {
        std::vector<int> values;

        while(!stop) {
            int x = ::rand();
            int index = -1;
            bool ok = false;
            for(int i = 0; i < servers; ++i) {
                // try them all, maybe one of them is a leader
                auto raft = config->_rafts[i];
                if(!config->_killed[i]) {
                    cxxraft::Command cmd;
                    cmd["op"] = x;
                    auto [index1, _, ok1] = raft->startCommand(cmd);
                    if(ok1) {
                        ok = ok1;
                        index = index1;
                    }
                }
            }
            if(ok) {
                // maybe leader will commit our value, maybe not.
                // but don't wat forever.
                std::vector<int> ranges {10, 20, 50, 100, 200};
                for(auto to : ranges) {
                    auto [nd, cmd] = config->nCommitted(index);
                    if(nd > 0) {
                        if(cmd["op"].to<int>() == x) {
                            values.emplace_back(x);
                        }
                        break;
                    }
                    co::poll(nullptr, 0, to);
                }
            } else {
                co::poll(nullptr, 0, 79 + me * 17);
            }
        }
        ch = std::move(values);
    };

    int ncli = 3;
    std::vector<std::vector<int>> cha(ncli);
    auto &env = co::open();
    for(int i = 0; i < ncli; i++) {
        env.createCoroutine(cfn, i, std::ref(cha[i]))
            ->resume();
    }

    for(int iters = 0; iters < 20; iters++) {
        if(::rand() % 1000 < 200) {
            int i = ::rand() % servers;
            config->disconnect(i);
        }

        if(::rand() % 1000 < 500) {
            int i = ::rand() % servers;
            if(config->_killed[i]) {
                config->start(i);
            }
        }

        if(::rand() % 1000 < 200) {
            int i = ::rand() % servers;
            if(!config->_killed[i]) {
                config->crash(i);
            }
        }

        // Make crash/restart infrequent enough that the peers can often
        // keep up, but not so infrequent that everything has settled
        // down from one change to the next. Pick a value smaller than
        // the election timeout, but not hugely smaller.
        co::poll(nullptr, 0, cxxraft::Raft::RAFT_ELECTION_TIMEOUT.count() * 7 / 10);
    }

    co::poll(nullptr, 0, cxxraft::Raft::RAFT_ELECTION_TIMEOUT.count());
    config->setReliable();
    for(int i = 0; i < servers; i++) {
        if(config->_killed[i]) {
            config->start(i);
        }
        if(!config->_connected[i]) {
            config->connect(i);
        }
    }

    stop = 1;

    std::vector<int> values;

    for(int i = 0; i < ncli; i++) {
        for(auto vv : cha[i]) {
            values.emplace_back(vv);
        }
    }

    co::poll(nullptr, 0, cxxraft::Raft::RAFT_ELECTION_TIMEOUT.count());

    cxxraft::Command cmd;
    cmd["op"] = ::rand();
    int lastIndex = config->one(cmd, servers, true);

    std::vector<int> really(lastIndex + 1);
    for(int index = 1; index <= lastIndex; index++) {
        auto cmdv = config->wait(index, servers, -1);
        really.emplace_back(cmdv["op"].to<int>());
    }

    for(auto v1 : values) {
        bool ok = false;
        for(auto v2 : really) {
            if(v1 == v2) {
                ok = true;
            }
        }
        if(!ok) {
            std::cerr << "didn't find a value" << std::endl;
            config->abort();
        }
    }

    config->end();
    ::exit(0);
}

void testReliableChurn2C() {
    internalChurn(false);
}

void testUnreliableChurn2C() {
    internalChurn(true);
}

int main() {

    TestFunction tests[] {
        testPersist12C,
        testPersist22C,
        testPersist32C,
        testFigure82C,
        testUnreliableAgree2C,
        testFigure8Unreliable2C,
        testReliableChurn2C,
        testUnreliableChurn2C
    };

    constexpr auto round = 10;

    runTestsAndReport(tests, round);

    return 0;
}
