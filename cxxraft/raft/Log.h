#pragma once
#include <vector>
#include "raft/Entry.h"
namespace cxxraft {

class Log {
    std::vector<Entry> logs;
};

} // cxxraft
