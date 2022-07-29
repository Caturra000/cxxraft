#pragma once
#include <unistd.h>
#include <cstddef>
#include "Codec.h"

namespace trpc {
namespace detail {

class Health {
public:

    enum State {
        UNDEFINED,
        // initial state
        // no byte to read and abort
        NO_READ,
        // `some` is greater than or equal to 0
        // header is unknown
        HEADER_READ_SOME,
        // `some` may be 0 byte
        // header is known
        CONTENT_READ_SOME,
    };

public:

    // read and abort previous response
    bool check(int fd);

    // called by user
    // update next stage
    void set(ssize_t some, State state, int content);

    ssize_t some() { return _some; }

private:

    // internal check
    // error case will fall back to UNDEFINED
    void stateMachine(int fd, char *buf, size_t boundary, State nextState);

public:

    constexpr static size_t BUF_SIZE = 1 << 12;

private:

    ssize_t _some {};
    // used in CONTENT_READ_SOME only
    ssize_t _content {};
    State   _state {NO_READ};
    Codec   _codec;
};

inline bool Health::check(int fd) {

    if(fd < 0) /*[[unlikely]]*/ {
        return false;
    }

    // optimized fast check
    if(_state == NO_READ) /*[[likely]]*/ {
        return true;
    }

    char buf[BUF_SIZE];

    // TODO reduce overhead of system call

    // if you can't do it in `deathCountdown` steps (too slow!)
    // you will die
    size_t deathCountdown = 5;

    // while(1) {
    while(deathCountdown--) {
        switch(_state) {
        case NO_READ:
            return true;
        case HEADER_READ_SOME:
            stateMachine(fd, buf, sizeof(Codec::Header) - _some, CONTENT_READ_SOME);
            if(_state == CONTENT_READ_SOME) {
                auto [_, length] = _codec.contentLength(buf, sizeof(Codec::Header));
                if((_content = length) > sizeof buf) {
                    // huge frame
                    // don't waste your time
                    _state = UNDEFINED;
                }
            }
            continue;
        // case NO_CONTENT_READ:
        case CONTENT_READ_SOME:
            stateMachine(fd, buf, _content - _some, NO_READ);
            continue;
        default:
            break;
        }
    }
    return false;
}

inline void Health::set(ssize_t some, Health::State state, int content) {
    _some = some;
    _state = state;
    _content = content;
}

inline void Health::stateMachine(int fd, char *buf, size_t boundary, Health::State nextState) {
    // NOT co::read
    // `Health` doesn't "wait" for any byte
    ssize_t ret = ::read(fd, buf, boundary);
    if(ret <= 0) {
        _state = UNDEFINED;
        return;
    }
    if(ret < boundary) {
        _some += ret;
    } else {
        _some = 0;
        _state = nextState;
    }
}

} // detail
} // trpc