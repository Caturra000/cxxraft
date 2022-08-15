#include "cxxraft.hpp"
#include "co.hpp"
#include "test.h"

void testInitialElection2A() {

    auto &env = co::open();

    if(!co::test()) {
        std::cerr << "not a coroutine" << std::endl;
        return;
    }

    int servers = 3;
    auto peers = createPeers(servers);
    std::vector<std::shared_ptr<cxxraft::Raft>> rafts;
    auto config = cxxraft::Config::make(peers);

    for(size_t i = 0; i < peers.size(); ++i) {
        config->start(i);
    }

    config->begin("Test (2A): initial election");

    config->checkOneLeader();

    // 50ms
    co::usleep(50 * 1000);

    auto term1 = config->checkTerms();

    if(term1 < 1) {
        std::cerr << "term1 < 1" << std::endl;
        std::cerr << "term1 == " << term1 << std::endl;
    }

    co::poll(nullptr, 0, std::chrono::duration<useconds_t, std::milli>
        (2 * cxxraft::Raft::RAFT_ELECTION_TIMEOUT).count());

    auto term2 = config->checkTerms();

    if(term1 != term2) {
        // warning
        std::cerr << "term1 != term2" << std::endl;
    }

    config->checkOneLeader();

    config->end();

    ::exit(0);
}

void testReElection2A() {

    auto &env = co::open();

    if(!co::test()) {
        std::cerr << "not a coroutine" << std::endl;
        return;
    }

    int servers = 3;
    auto peers = createPeers(servers);
    auto config = cxxraft::Config::make(peers);

    for(size_t i = 0; i < peers.size(); ++i) {
        config->start(i);
    }

    config->begin("Test (2A): election after network failure");

    auto leader1 = config->checkOneLeader();

    // if the leader disconnects, a new one should be elected.
    config->disconnect(leader1);
    config->checkOneLeader();

    // if the old leader rejoins, that shouldn't
    // disturb the new leader.
    config->connect(leader1);
    auto leader2 = config->checkOneLeader();

    // if there's no quorum, no leader should
    // be elected.
    config->disconnect(leader2);
    config->disconnect((leader2 + 1) % servers);

    co::poll(nullptr, 0, std::chrono::milliseconds
        {2 * cxxraft::Raft::RAFT_ELECTION_TIMEOUT}.count());

    config->checkNoLeader();

    // if a quorum arises, it should elect a leader.
    config->connect((leader2 + 1) % servers);
    config->checkOneLeader();

    // re-join of last node shouldn't prevent leader from existing.
    config->connect(leader2);
    config->checkOneLeader();

    config->end();

    ::exit(0);
}

void testManyElections2A() {

    auto &env = co::open();

    if(!co::test()) {
        std::cerr << "not a coroutine" << std::endl;
        return;
    }

    int servers = 7;
    auto peers = createPeers(servers);

    auto config = cxxraft::Config::make(peers);

    for(size_t i = 0; i < peers.size(); ++i) {
        config->start(i);
    }

    config->begin("Test (2A): multiple elections");

    config->checkOneLeader();

    int iters = 10;
    for(auto ii = 1; ii < iters; ++ii) {

        co::usleep(50 * 1000);

        // disconnect three nodes
        auto i1 = ::rand() % servers;
        auto i2 = ::rand() % servers;
        auto i3 = ::rand() % servers;
        while(i1 == i2) i2 = ::rand() % servers;
        while(i1 == i3 || i2 == i3) i3 = ::rand() %servers;

        config->disconnect(i1);
        config->disconnect(i2);
        config->disconnect(i3);

        // either the current leader should still be alive,
        // or the remaining four should elect a new one.
        config->checkOneLeader();

        config->connect(i1);
        config->connect(i2);
        config->connect(i3);
    }

    config->checkOneLeader();

    config->end();

    ::exit(0);
}

int main() {

    TestFunction tests[] {
        testInitialElection2A,
        testReElection2A,
        testManyElections2A
    };

    constexpr auto round = 10;

    runTestsAndReport(tests, round);

    return 0;
}