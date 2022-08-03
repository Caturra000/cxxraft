#pragma once
#include "raft/Message.h"
namespace cxxraft {

// [index, term, commited]
using Entry = Message<int, int, bool>;

} // cxxraft
