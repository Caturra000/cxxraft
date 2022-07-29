#pragma once
#include <cstddef>

namespace trpc {
namespace detail {

// TODO thread local
struct TokenGenerator {
public:
    using Token = int;
public:
    Token acquire() { return ++_token; }
    void release(Token) {}

private:
    // uninitilized, random ISN
    Token _token;
};

} // detail
} // trpc