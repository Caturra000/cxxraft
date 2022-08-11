#pragma once
#include <string>
#include "raft/Log.h"

namespace cxxraft {

template <typename Self>
struct Debugger {

    // (node_id, node_status, node_term, node_commit)
    std::string simpleInfo() {
        std::string info;
        using Literal = typename Self::State::Literal;
        info.append("(")
            .append(std::to_string(crtp()->_id))
            .append(", ")
            .append(!crtp()->_fsm ? "null" : crtp()->_fsm->type(Literal{}))
            .append(", ")
            .append(std::to_string(crtp()->_currentTerm))
            .append(", ")
            .append(std::to_string(crtp()->_commitIndex))
            .append(")");
        return info;
    }


    std::string dump(Log::EntriesArray entries) {
        vsjson::Json json = entries;
        return chatty(json.dump());
    }

    std::string dump(Log::EntriesSlice entries) {
        vsjson::Json json = entries;
        return chatty(json.dump());
    }

    std::string dump(Command command) {
        vsjson::Json json = command;
        return chatty(json.dump());
    }

private:

    Self* crtp() { return static_cast<Self*>(this); }

    std::string chatty(std::string s, size_t size = 128) {
        if(s.size() <= size) {
            return s;
        }
        return s.substr(0, size / 2) + "<IGNORE>" + s.substr(s.size() - size / 2, size / 2);
    }

};

} // cxxraft