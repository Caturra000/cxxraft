#pragma once
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <algorithm>
#include <optional>
#include <chrono>
#include <string>
#include "co.hpp"
#include "detail/Codec.h"
#include "detail/resolve.h"
#include "detail/TokenGenerator.h"
#include "detail/bestEffort.h"
#include "detail/Health.h"
#include "Endpoint.h"
namespace trpc {

class Client {

// call
public:

    // core RPC interface
    //
    // `nullopt` if failed
    // most of the time we don't care about the error type
    // if you really need it, check client::error()
    template <typename T, typename ...Args>
    std::optional<T> call(const std::string &function, Args &&...arguemnts);

    // global client timeout
    void setTimeout(std::chrono::milliseconds timeout);

    // last errno
    int error();

    int fd() const;

// connection
public:

    // true if connected to endpoint
    bool connect(Endpoint endpoint);

    // void shutdown(int WHERE);

    // it is safe to close() before ~Client()
    void close();

private:

    std::tuple<bool, ssize_t> bestEffortRead(const void *buf, size_t size, size_t maxRetries = 10);
    std::tuple<bool, ssize_t> bestEffortWrite(const void *buf, size_t size, size_t maxRetries = 10);

// class attributes
public:

    // it is recommended to use client::make() instead of client::Client()
    // 1. if we initialize socket fd in ctor
    //    it is too heavy for a ctor using system call
    // 2. if we use two phase construct (Client() and then client.init())
    //    it is weird, you know
    Client();

    Client(const Client&) = delete;
    Client(Client&&);

    Client& operator=(Client);

    ~Client();

    // two phase construction
    void init();

    void swap(Client&);

    // factory method: makes ready client
    static std::optional<Client> make();

    // factory method: makes connected client
    // but must be in coroutine environment
    static std::optional<Client> make(Endpoint endpoint);

public:

    // 1. avoid double close
    // 2. any further function call later raise an error and catch errno
    constexpr static int SOCKET_INVALID = -1;

    // 16KiB
    constexpr static size_t BUF_SIZE_ON_STACK = 1 << 14;

    constexpr static std::chrono::milliseconds NO_TIMEDOUT
        {std::chrono::hours {1<<9}};

private:
    // owned socket fd
    int _socket;

    // **soft** limit timeout
    std::chrono::milliseconds _timeout {NO_TIMEDOUT};

    // cached system call errno
    // or timeout in application layer (ETIMEDOUT)
    int _errno;

    // inspired by GFS
    // detail::Lru<vsjson::Json> _lru;

    // very simple token generator
    detail::TokenGenerator _tokens;

    detail::Codec _codec;

    // health (or consistency?) check
    detail::Health _health;
};

template <typename T, typename ...Args>
inline std::optional<T> Client::call(const std::string &function, Args &&...arguments) {

    using Header = detail::Codec::Header;

    if(!_health.check(_socket)) {
        return std::nullopt;
    }

    auto token = _tokens.acquire();
    auto request = detail::makeRequest(token, function, std::forward<Args>(arguments)...);

    auto [dump, length, beLength] = _codec.dump(request);

    // sacrificing availability for consistency
    //
    // if write (request) failed
    // client/connection will not maintain consistency
    // close directly
    //
    // write request header
    if(auto [success, written] = bestEffortWrite(&beLength, sizeof beLength); !success) {
        if(written > 0) close();
        return std::nullopt;
    }

    // write request content
    if(auto [success, _] = bestEffortWrite(dump.c_str(), length); !success) {
        // prefix bytes has written
        close();
        return std::nullopt;
    }

    // TODO buffer allocate policy: BufferAllocator
    char buf[BUF_SIZE_ON_STACK];

    // if read (response) failed
    // this connection is still alive
    //
    // read response header
    if(auto [success, some] = bestEffortRead(buf, sizeof(Header)); !success) {
        _health.set(std::max<ssize_t>(0, some), detail::Health::HEADER_READ_SOME, 0 /*unused*/);
        return std::nullopt;
    }

    auto [/*fastVerify*/ _, contentLength] = _codec.contentLength(buf, sizeof(Header));

    // what happened?
    // if(!fastVerify) {
    //     close();
    //     return std::nullopt;
    // }

    // out of stack
    // resize is easy, but I don't want to do
    if(sizeof(Header) + contentLength > sizeof buf) {
        _errno = ENOMEM;
        close();
        return std::nullopt;
    }

    // read response content
    if(auto [success, some] = bestEffortRead(buf + sizeof(Header), contentLength); !success) {
        _health.set(std::max<ssize_t>(0, some), detail::Health::CONTENT_READ_SOME, contentLength);
        return std::nullopt;
    }

    if(!_codec.verify(buf, sizeof(Header) + contentLength)) {
        _errno = EINVAL;
        return std::nullopt;
    }

    auto response = _codec.decode(buf, sizeof(Header) + contentLength);
    return detail::makeResult<T>(response);

    // we will not capture exceptions
    // because user may explicitly call a function throwing exceptions
}

inline void Client::setTimeout(std::chrono::milliseconds timeout) {
    _timeout = timeout;
}

inline int Client::error() {
    int ret = _errno;
    _errno = 0;
    return ret;
}

inline int Client::fd() const {
    return _socket;
}

inline bool Client::connect(Endpoint endpoint) {
    int ret = co::connect(_socket, (const sockaddr*)&endpoint, sizeof endpoint);
    if(ret) _errno = errno;
    return !ret;
}

inline Client::Client()
    // system call ::socket() is too heavy
    // and we dont want to construct an incomplete object if ::socket() return errno
    //
    // if you need a socket in ready
    // use Client::make()
    : _socket(SOCKET_INVALID),
      _errno(0)
{}

inline std::optional<Client> Client::make() {
    Client client;
    client.init();
    if(client.error()) /*[[unlikely]]*/ {
        return std::nullopt;
    }
    return client;
}

inline std::optional<Client> Client::make(Endpoint endpoint) {
    if(!co::test()) /*[[unlikely]]*/ {
        return std::nullopt;
    }
    auto opt = make();
    if(!opt || !opt->connect(endpoint)) /*[[unlikely]]*/ {
        return std::nullopt;
    }
    return opt;
}


inline Client::Client(Client &&rhs)
    : _socket(rhs._socket),
      _timeout(rhs._timeout),
      _errno(rhs._errno),
      _health(rhs._health)
{
    rhs._socket = SOCKET_INVALID;
}

inline Client& Client::operator=(Client that) {
    that.swap(*this);
    return *this;
}

inline Client::~Client() {
    this->close();
}

inline void Client::init() {
    _socket = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if(_socket < 0) _errno = errno;
}

inline void Client::swap(Client &that) {
    using std::swap;
    swap(this->_socket, that._socket);
    swap(this->_timeout, that._timeout);
    swap(this->_errno, that._errno);
    swap(this->_tokens, that._tokens);
    swap(this->_codec, that._codec);
    swap(this->_health, that._health);
}

inline void Client::close() {
    if(_socket != SOCKET_INVALID) {
        ::close(_socket);
        _socket = SOCKET_INVALID;
    }
}

inline std::tuple<bool, ssize_t> Client::bestEffortRead(const void *buf, size_t size, size_t maxRetries) {
    ssize_t ret = detail::bestEffortRead(_socket, buf, size, _timeout, maxRetries);
    if(ret == size) {
        return {true, size};
    }
    if(!(_errno = errno)) {
        // if read m bytes but n > m (active but too slow)
        // it is also ETIMEDOUT
        _errno = ETIMEDOUT;
    }
    return {false, ret};
}

inline std::tuple<bool, ssize_t> Client::bestEffortWrite(const void *buf, size_t size, size_t maxRetries) {
    ssize_t ret = detail::bestEffortWrite(_socket, buf, size, _timeout, maxRetries);
    if(ret == size) {
        return {true, size};
    }
    if(!(_errno = errno)) {
        _errno = ETIMEDOUT;
    }
    return {false, ret};
}

} // trcp
