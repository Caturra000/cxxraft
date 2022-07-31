#pragma once
#include <string>

namespace cxxraft {

template <typename Self>
struct Debugger {

    // (node_id, node_status, node_term)
    std::string simpleInfo() {
        std::string info;
        using Literal = typename Self::State::Literal;
        info.append("(")
            .append(std::to_string(crtp()->_id))
            .append(", ")
            .append(!crtp()->_fsm ? "null" : crtp()->_fsm->type(Literal{}))
            .append(", ")
            .append(std::to_string(crtp()->_currentTerm))
            .append(")");
        return info;
    }

private:

    Self* crtp() { return static_cast<Self*>(this); }

};

} // cxxraft