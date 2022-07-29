#pragma once
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <cstring>
#include <string>
namespace trpc {

// a simple wrapper for sockaddr_in with compatibility of POSIX interface
// it is guaranteed that Endpoint has the same memory layout as `sockaddr_in`
struct Endpoint final {
    Endpoint() = default;
    Endpoint(const std::string &ip, uint16_t port);
    Endpoint(const char *ip, uint16_t port);
    Endpoint(uint32_t ip, uint16_t port);
    // TODO EndPoint("tcp://....")

    sockaddr_in addr;
};

inline Endpoint::Endpoint(const std::string &ip, uint16_t port)
    : Endpoint(ip.c_str(), port) {}

inline Endpoint::Endpoint(const char *ip, uint16_t port) {
    ::memset(&addr, 0, sizeof addr);
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = ::inet_addr(ip);
    addr.sin_port = ::htons(port);
}

inline Endpoint::Endpoint(uint32_t ip, uint16_t port) {
    ::memset(&addr, 0, sizeof addr);
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = ::htonl(ip);
    addr.sin_port = ::htons(port);
}

} // trcp
