#include "raft/Raft.h"
#include "co.hpp"

void testInitialElection2A() {

    auto &env = co::open();

    if(!co::test()) {
        std::cerr << "not a coroutine" << std::endl;
        return;
    }

    int servers = 3;
    std::vector<trpc::Endpoint> peers {
        {"127.0.0.1", 2335},
        {"127.0.0.1", 2336},
        {"127.0.0.1", 2337}
    };
    std::vector<std::shared_ptr<cxxraft::Raft>> rafts;
    auto config = cxxraft::Config::make(peers);

    for(size_t i = 0; i < peers.size(); ++i) {
        auto raft = cxxraft::Raft::make(*config, i);
        raft->start();
    }

    config->begin();

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
}

void testReElection2A() {

    auto &env = co::open();

    if(!co::test()) {
        std::cerr << "not a coroutine" << std::endl;
        return;
    }

    int servers = 3;
    std::vector<trpc::Endpoint> peers {
        {"127.0.0.1", 2338},
        {"127.0.0.1", 2339},
        {"127.0.0.1", 2340}
    };
    auto config = cxxraft::Config::make(peers);

    for(size_t i = 0; i < peers.size(); ++i) {
        auto raft = cxxraft::Raft::make(*config, i);
        raft->start();
    }

    config->begin();

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
}

void testManyElections2A() {

    auto &env = co::open();

    if(!co::test()) {
        std::cerr << "not a coroutine" << std::endl;
        return;
    }

    int servers = 7;
    std::vector<trpc::Endpoint> peers;
    for(auto i {0}; i < servers; ++i) {
        peers.emplace_back("127.0.0.1", 2333 + i);
    }

    auto config = cxxraft::Config::make(peers);

    for(size_t i = 0; i < peers.size(); ++i) {
        auto raft = cxxraft::Raft::make(*config, i);
        raft->start();
    }

    config->begin();

    config->checkOneLeader();

    int iters = 10;
    for(auto ii = 1; ii < iters; ++ii) {
        // disconnect three nodes
        auto i1 = ::rand() % servers;
        auto i2 = ::rand() % servers;
        auto i3 = ::rand() % servers;
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
}

int main() {
    dlog::Log::init();
    auto &env = co::open();
    env.createCoroutine(testReElection2A)
        ->resume();
    co::loop();

}