#pragma once
#include <vector>
#include "co.hpp"
#include "trpc.hpp"
#include "raft/Worker.h"
namespace cxxraft {

struct Peer {
    trpc::Endpoint endpoint;
    std::optional<trpc::Client> client;
    std::optional<trpc::Client> votes;

    Worker executor;
    Worker votesExecutor;

    Peer(trpc::Endpoint endpoint): endpoint(endpoint) {}
};

} // cxxraft
