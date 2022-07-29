#pragma once
#include <unistd.h>
#include <poll.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <cstddef>
#include <chrono>
#include "co.hpp"
namespace trpc {
namespace detail {

using Milliseconds = std::chrono::milliseconds;

ssize_t bestEffortRead(int fd, const void *buf, size_t size, Milliseconds timeout, size_t maxRetries);
ssize_t bestEffortWrite(int fd, const void *buf, size_t size, Milliseconds timeout, size_t maxRetries);

template <typename CoPosixFunc>
ssize_t bestEffortTemplate(CoPosixFunc func, int event,
    int fd, const void *buf, size_t size, Milliseconds timeout, size_t maxRetries);







inline ssize_t bestEffortRead(int fd, const void *buf, size_t size, Milliseconds timeout, size_t maxRetries) {
    return bestEffortTemplate(co::read, POLLIN, fd, buf, size, timeout, maxRetries);
}

inline ssize_t bestEffortWrite(int fd, const void *buf, size_t size, Milliseconds timeout, size_t maxRetries) {
    return bestEffortTemplate(co::write, POLLOUT, fd, buf, size, timeout, maxRetries);
}

template <typename CoPosixFunc>
inline ssize_t bestEffortTemplate(CoPosixFunc func, int event, int fd, const void *buf, size_t size, Milliseconds timeout, size_t maxRetries) {
    auto tick = [] { return std::chrono::steady_clock::now(); };
    auto start = tick();
    size_t retries = 0;
    size_t offset = 0;
    int interval = timeout.count() / maxRetries;
    while(retries++ < maxRetries) {
        if(tick() - start > timeout) {
            errno = ETIMEDOUT;
            return -1;
        }

        pollfd pfd {};
        pfd.fd = fd;
        pfd.events = event;
        int pret;
        if((pret = co::poll(&pfd, 1, interval)) <= 0) {
            if(pret < 0) return -1;
            continue;
        }

        ssize_t ret = func(fd, (char*)(buf) + offset, size - offset);

        if(ret < 0) switch(errno) {
            // interrupted
            case EINTR:
                continue;

            // temporarily unavailable
            // https://github.com/apache/incubator-brpc/blob/master/docs/cn/error_code.md
            case EAGAIN:
                // FIXME: not a good idea, but it is a rare case
                co::usleep(1);
                continue;

            // error
            default:
                return -1;
        }
        // FIN
        if(ret == 0) {
            return offset;
        }
        offset += ret;
        if(offset == size) {
            return size;
        }
    }
    // upper layer error
    if(offset == 0) {
        errno = ETIMEDOUT;
        return -1;
    }
    return offset;
}

} // detail
} // trpc
