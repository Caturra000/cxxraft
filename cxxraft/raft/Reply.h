#pragma once
#include "raft/Message.h"
namespace cxxraft {

template <typename ...Ts>
using Reply = Message<Ts...>;

} // cxxraft
