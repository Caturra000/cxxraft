#pragma once
#include <map>
#include <string>
#include "vsjson.hpp"

namespace cxxraft {

// serializable user command
using Command = std::map<std::string, vsjson::Json>;

inline bool equal(const Command &l, const Command &r) {
    if(l.size() != r.size()) return false;
    for(auto &&[key, value] : l) {
        auto iter = r.find(key);
        if(iter == r.end()) return false;
        // ugly code
        auto cast = [](auto&& obj) -> decltype(auto)
            { return const_cast<vsjson::Json&>(obj); };
        // char-by-char comparison
        if(cast(value).dump() != cast(iter->second).dump()) {
            return false;
        }
    }
    return true;
}

} // cxxraft
