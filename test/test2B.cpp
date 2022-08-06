#include "raft/Raft.h"
#include "co.hpp"

void testBasicAgree2B() {
    std::vector<trpc::Endpoint> peers {
        {"127.0.0.1", 2333},
        {"127.0.0.1", 2334},
        {"127.0.0.1", 2335},
    };
    int servers = peers.size();
    auto config = cxxraft::Config::make(peers);
    for(size_t i = 0; i < peers.size(); ++i) {
        auto raft = cxxraft::Raft::make(*config, i);
        raft->start();
    }

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

void testRPCBytes2B() {
}

int main() {

}