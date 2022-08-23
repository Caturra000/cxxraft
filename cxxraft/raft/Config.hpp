#pragma once
#include "raft/Config.h"
namespace cxxraft {

inline Config::Config(std::vector<trpc::Endpoint> peers)
    : _peers(peers), _connected(peers.size()), _killed(peers.size())
{}

inline Config::Config(std::vector<trpc::Endpoint> peers, EnablePersistent)
    : _peers(peers), _connected(peers.size()), _killed(peers.size()), _persistent(true)
{}

inline std::shared_ptr<Config> Config::make(std::vector<trpc::Endpoint> peers) {
    return std::make_shared<Config>(std::move(peers));
}

inline std::shared_ptr<Config> Config::make(std::vector<trpc::Endpoint> peers, EnablePersistent) {
    return std::make_shared<Config>(std::move(peers), EnablePersistent{});
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
    // config
    _connected[index] = true;
    // server
    flushVnet(index);
    // client
    // FIXME elegant code
    raft->_callDisabled = false;
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
    // config
    _connected[index] = false;
    // server
    flushVnet(index);
    // client
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
    CXXRAFT_LOG_DEBUG("nCommitted", index);
    for(int i = 0; i < _rafts.size(); ++i) {
        std::optional<Log::Entry> pEntry
            { _rafts[i]->getCommittedCopy(index) };
        if(pEntry) {
            auto &[_, cmd1] = *pEntry;
            CXXRAFT_LOG_DEBUG("nCommitted i:", i, dump(cmd1));
            if(count > 0 && !equal(cmd1, cmd)) {
                CXXRAFT_LOG_WTF("committed values do not match: index",
                    index);
                abort();
            }
            count++;
            cmd = cmd1;
        } else {
            CXXRAFT_LOG_DEBUG("nCommitted i:", i, "nullopt");
        }
    }
    return {count, cmd};
}

inline int Config::one(Command command, int expectedServers, bool retry) {
    CXXRAFT_LOG_DEBUG("one. command:", dump(command), "expectedServers:", expectedServers, "retry:", retry);
    using namespace std::chrono_literals;
    auto now = std::chrono::system_clock::now;
    auto t0 = now();
    int starts = 0;
    Command cmd2;
    while(now() - t0 < 10s) {
        // try all the servers, maybe one is the leader.
        int index = -1;
        for(size_t si = 0, n = _peers.size(); si < n; ++si) {
            starts = (starts + 1) % n;
            cxxraft::Raft *raft {};
            if(_connected[starts] && !_killed[starts]) {
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
                CXXRAFT_LOG_DEBUG("one nCommited", nd, dump(cmd1));
                if(nd > 0 && nd >= expectedServers) {
                    // commited
                    if(equal(cmd1,command)) {
                        // and it was the command we submitted.
                        return index;
                    }
                    cmd2 = cmd1;
                }
                co::usleep(20 * 1000);
            }
            if(!retry) {
                CXXRAFT_LOG_WTF("one", dump(command), "failed to reach agreement");
                CXXRAFT_LOG_WTF("dump:", dump(cmd2), dump(command));
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

inline bool Config::crash(int id) {
    if(!_killed[id]) {
        return false;
    }

    CXXRAFT_LOG_DEBUG("crash id:", id);

    auto pRaft = _rafts[id];

    // stop pushing messages
    pRaft->statePost([pRaft] {
        pRaft->becomeFollower();
    });

    // receive but ignore messages
    disconnect(id);

    pRaft->resetMemory();

    _killed[id] = true;

    return true;
}

inline void Config::start(int id) {
    CXXRAFT_LOG_DEBUG("start id:", id);
    if(crash(id)) {
        CXXRAFT_LOG_DEBUG("restart id:", id);
    }

    auto makeStorage = [this, id] {
        auto path = std::to_string(_uuid) + "_" + std::to_string(id) + ".wal";
        auto storage = std::make_shared<Storage>(path);
        return storage;
    };

    auto &raft = _rafts[id];

    // first time
    if(_persistent && !raft) {
        raft = Raft::make(_peers, id, makeStorage());
        _connected[id] = true;
        raft->start();
        flushVnet(id);
    // restart
    } else if(_persistent && raft) {
        raft->restore(makeStorage());
        connect(id);
        _killed[id] = false;
    // in-memory, first time
    } else if(!raft) {
        raft = Raft::make(_peers, id);
        _connected[id] = true;
        raft->start();
        flushVnet(id);
    } else {
        CXXRAFT_LOG_WTF("cannot restart a raft server with in-memory mode");
        abort();
    }
}

inline cxxraft::Command Config::wait(int index, int n, int startTerm) {
    CXXRAFT_LOG_DEBUG("wait. index:", index, "n:", n, "startTerm:", startTerm);
    using namespace std::chrono_literals;
    auto to = 10ms;
    for(auto iter = 0; iter < 30; ++iter) {
        auto [nd, _] = nCommitted(index);
        if(nd >= n) {
            break;
        }
        co::poll(nullptr, 0, to.count());
        if(to < 1s) {
            to *= 2;
        }
        if(startTerm > -1) {
            for(auto &&[id, raft] : _rafts) {
                if(auto [t, _] = raft->getState(); t > startTerm) {
                    // someone has moved on
                    // can no longer guarantee that we'll "win"
                    cxxraft::Command cmd;
                    cmd["op"] = -1;
                    return cmd;
                }
            }
        }
    }
    auto [nd, cmd] = nCommitted(index);
    if(nd < n) {
        CXXRAFT_LOG_WTF("only", nd, "decided for index", index, "wanted", n);
        abort();
    }
    return cmd;
}

inline void Config::setUnreliable() {
    _reliable = false;
    flushVnet();
}

inline void Config::setReliable() {
    _reliable = true;
    flushVnet();
}

inline bool Config::vnetProxy(int id) {
    if(!_connected[id]) {
        return false;
    }
    if(_reliable) {
        return true;
    }
    auto randRTT = [&] { return _vnet.distRTT(_vnet.randomEngine); };
    auto randLost = [&] { return _vnet.distLost(_vnet.randomEngine); };
    using namespace std::chrono_literals;
    if(Vnet::Milli delay {randRTT()}; delay != 0ms) {
        co::poll(nullptr, 0, delay.count());
    }
    bool lost = randLost() < _vnet.lostRate;
    return !lost;
}

inline void Config::flushVnet() {
    for(auto &&[id, _] : _rafts) {
        flushVnet(id);
    }
}

inline void Config::flushVnet(int id) {
    auto raft = _rafts[id];
    if(!raft) return;
    if(!raft->_rpcServer) return;
    raft->_rpcServer->onRequest([this, id](auto &&) {
        return vnetProxy(id);
    });
}

} // cxxraft
