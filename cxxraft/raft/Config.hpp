#pragma once
#include "raft/Config.h"
namespace cxxraft {

inline Config::Config(std::vector<trpc::Endpoint> peers)
    : _peers(peers), _connected(peers.size())
{}

inline std::shared_ptr<Config> Config::make(std::vector<trpc::Endpoint> peers) {
    return std::make_shared<Config>(std::move(peers));
}

inline void Config::connect(int index) {
    if(_connected[index]) return;
    CXXRAFT_LOG_DEBUG("connect server", index, "to virtual network");
    auto raft = _rafts[index];
    CXXRAFT_LOG_DEBUG("server info:", raft->simpleInfo());
    auto &pServer = raft->_rpcServer;
    if(!pServer) {
        CXXRAFT_LOG_WTF("no server");
        return;
    }
    auto dummy = std::function<bool(trpc::Server::ProtocolType &)>{};
    pServer->onRequest(dummy);
    _connected[index] = true;

    raft->_callDisabled = false;
    raft->statePost([index, this] {
        auto raft = _rafts[index];
        Raft::Bitmask flags = raft->_fsm->flags();
        if(flags & Raft::FLAGS_LEADER) {
            raft->becomeLeader();
        } else if(flags & Raft::FLAGS_FOLLOWER) {
            raft->becomeFollower();
        } else {
            raft->becomeCandidate();
        }
    });
}

inline void Config::disconnect(int index) {
    if(!_connected[index]) return;
    CXXRAFT_LOG_DEBUG("disconnect server", index, "to virtual network");
    auto raft = _rafts[index];
    CXXRAFT_LOG_DEBUG("server info:", raft->simpleInfo());
    auto &pServer = raft->_rpcServer;
    if(!pServer) {
        CXXRAFT_LOG_WTF("no server");
        return;
    }
    pServer->onRequest([](auto&&) {
        return false;
    });
    _connected[index] = false;
    // cancel all jobs
    raft->updateTransaction();
    raft->_callDisabled = true;
}

inline void Config::abort() {
    // actually sleep
    ::usleep(50 * 1000);
    // acutally abort execution
    ::abort();
}

inline int Config::checkOneLeader() {
    CXXRAFT_LOG_DEBUG("checkOneLeader");
    for(auto iter {0}; iter < 10; ++iter) {
        // ms: [450, 550)
        auto ms = 450 + (::rand() % 100);
        co::poll(nullptr, 0, ms);

        auto leaders = std::unordered_map<int, std::vector<int>>{};

        for(auto i {0}; i < _peers.size(); ++i) {
            if(_connected[i]) {
                if(auto [term, leader] = _rafts[i]->getState(); leader) {
                    leaders[term].emplace_back(i);
                }
            } else {
                CXXRAFT_LOG_DEBUG("server", i, "disconnected");
            }
        }

        int lastTermWithLeader = -1;
        for(auto &&[term, leaders] : leaders) {
            if(leaders.size() > 1) {
                CXXRAFT_LOG_WTF("term", term, "has", leaders.size(), "(>1) leaders");
                abort();
            }
            if(term > lastTermWithLeader) {
                lastTermWithLeader = term;
            }
        }

        if(!leaders.empty()) {
            CXXRAFT_LOG_DEBUG("last term:", lastTermWithLeader, "leader:", leaders[lastTermWithLeader][0]);
            return leaders[lastTermWithLeader][0];
        }
    }

    CXXRAFT_LOG_WTF("expected one leader, got none");
    abort();
    return -1;
}

inline void Config::checkNoLeader() {
    for(auto i {0}; i < _peers.size(); ++i) {
        if(_connected[i]) {
            auto [_, isLeader] = _rafts[i]->getState();
            if(isLeader) {
                CXXRAFT_LOG_WTF("expected no leader, but", i, "claims to be leader");
                abort();
            }
        } else {
            CXXRAFT_LOG_DEBUG("server", i, "disconnected");
        }
    }
}

inline int Config::checkTerms() {
    CXXRAFT_LOG_DEBUG("checkTerms");
    int term = -1;
    for(auto i {0}; i < _peers.size(); ++i) {
        if(_connected[i]) {
            auto [xterm, _] = _rafts[i]->getState();
            CXXRAFT_LOG_DEBUG("get xterm:", xterm);
            if(term == -1) {
                term = xterm;
                CXXRAFT_LOG_DEBUG("set term:", xterm);
            } else if(term != xterm) {
                CXXRAFT_LOG_WTF("servers disagree on term");
                abort();
            }
        } else {
            CXXRAFT_LOG_DEBUG("server", i, "disconnected");
        }
    }
    return term;
}

inline auto Config::nCommitted(int index) -> std::tuple<int, Command> {
    int count = 0;
    Command cmd;
    for(int i = 0; i < _rafts.size(); ++i) {
        std::optional<Log::Entry> pEntry
            { _rafts[i]->getCommittedCopy(index) };
        if(pEntry) {
            auto &[_, cmd1] = *pEntry;
            if(count > 0 && !equal(cmd1, cmd)) {
                CXXRAFT_LOG_WTF("committed values do not match: index",
                    index);
                abort();
            }
            count++;
            cmd = cmd1;
        }
    }
    return {count, cmd};
}

inline int Config::one(Command command, int expectedServers, bool retry) {
    using namespace std::chrono_literals;
    auto now = std::chrono::system_clock::now;
    auto t0 = now();
    int starts = 0;
    while(now() - t0 < 10s) {
        // try all the servers, maybe one is the leader.
        int index = -1;
        for(size_t si = 0, n = _peers.size(); si < n; ++si) {
            starts = (starts + 1) % n;
            cxxraft::Raft *raft {};
            if(_connected[starts]) {
                raft = _rafts[starts].get();
            }
            if(raft) {
                auto [index1, _, ok] = raft->startCommand(command);
                if(ok) {
                    index = index1;
                    break;
                }
            }
        }

        if(index != -1) {
            // somebody claimed to be the leader and to have
            // submitted our command; wait a while for agreement.
            auto t1 = now();
            while(now() - t1 < 2s) {
                auto [nd, cmd1] = nCommitted(index);
                if(nd > 0 && nd >= expectedServers) {
                    // commited
                    if(equal(cmd1,command)) {
                        // and it was the command we submitted.
                        return index;
                    }
                }
                co::usleep(20 * 1000);
            }
            if(!retry) {
                CXXRAFT_LOG_WTF("one", dump(command), "failed to reach agreement");
                abort();
            }
        } else {
            co::usleep(50 * 1000);
        }
    }
    CXXRAFT_LOG_WTF("one", dump(command), "failed to reach agreement");
    abort();
    return -1;
}

} // cxxraft