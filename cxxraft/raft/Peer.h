#pragma once
#include "trpc.hpp"
namespace cxxraft {

struct Peer {

    // peer id in cluster
    int             id;

    trpc::Endpoint  endpoint;

    // index of the next log entry
    // to send to that server
    //
    // for leader only
    int             nextIndex;

    // index of highest log entry
    // known to be replicated on server
    //
    // for leader only
    int             matchIndex;

    Peer() = default;
    Peer(int id, trpc::Endpoint endpoint)
        : id(id),
          endpoint(endpoint) {}
};

} // cxxraft
