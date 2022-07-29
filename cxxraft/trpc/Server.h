#pragma once
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <algorithm>
#include <optional>
#include <chrono>
#include <cstddef>
#include <string>
#include <unordered_map>
#include "co.hpp"
#include "Endpoint.h"
#include "detail/CallProxy.h"
#include "detail/Codec.h"
#include "detail/resolve.h"
#include "detail/bestEffort.h"
namespace trpc {

class Server {
public:

    using ProtocolType = detail::Codec::ProtocolType;

public:

    // resume in coroutine
    void start();

    void close();

    template <typename F>
    void bind(const std::string &method, const F &func);

    int fd() const { return _fd; }

    int error();

    void setTimeout(std::chrono::milliseconds timeout);
    void setPending(std::chrono::milliseconds timeout);

    // require: bool(ProtocolType &)
    // TODO: abstract context, not ProtocolType
    template <typename Func>
    void onRequest(Func &&requestCallback);

    // require: bool(ProtocolType &)
    template <typename Func>
    void onResponse(Func &&responseCallback);

public:

    // see Client::Client()
    explicit Server(Endpoint);

    Server(const Server&) = delete;
    Server(Server&&);

    Server& operator=(Server);

    ~Server();

    // two phase construction
    void init();

    void swap(Server&);

    // factory method: makes listened server
    static std::optional<Server> make(Endpoint);

private:

    ProtocolType netCall(const std::string &method, ProtocolType args);

    void onAccept(int peerFd, Endpoint peerEndpoint);

    bool bestEffortRead(int peer, const void *buf, size_t size, size_t maxRetries = 6);
    bool bestEffortWrite(int peer, const void *buf, size_t size, size_t maxRetries = 6);
    // used in first byte
    bool bestEffortPending(int peer);

public:

    // 1. avoid double close
    // 2. any further function call later raise an error and catch errno
    constexpr static int SOCKET_INVALID = -1;

// configurations
public:

    // 16KiB
    constexpr static size_t BUF_SIZE_ON_STACK = 1 << 14;

    constexpr static std::chrono::milliseconds NO_TIMEDOUT
        {std::chrono::hours {1<<9}};

    constexpr static std::chrono::milliseconds MIN_LONG_CONNECTION_PENDING
        {std::chrono::hours {1}};

private:

    // listen fd
    // owned by server
    int _fd;

    // server {ip : port}
    Endpoint _endpoint;

    // bound function
    using Proxy = std::function<ProtocolType(ProtocolType)>;
    std::unordered_map<std::string, Proxy> _table;

    // system call errno or application layer error
    int _errno;

    // **soft** limit timeout
    std::chrono::milliseconds _timeout {NO_TIMEDOUT};

    // waiting for first byte (per iteration) in long connection
    std::chrono::milliseconds _pending {MIN_LONG_CONNECTION_PENDING};

    detail::Codec _codec;

    std::function<bool(ProtocolType &)> _requestCallback;
    std::function<bool(ProtocolType &)> _responseCallback;
};

inline void Server::start() {
    // if(!co::test()) warn();
    auto &env = co::open();
    if(::listen(_fd, SOMAXCONN)) {
        _errno = errno;
        return;
    }
    while(1) {
        Endpoint peerEndpoint;
        socklen_t len = sizeof peerEndpoint;
        int peerFd = co::accept4(_fd, (sockaddr*)&peerEndpoint, &len,
            SOCK_CLOEXEC | SOCK_NONBLOCK);
        if(peerFd < 0) continue;
        auto worker = env.createCoroutine([=] {
            onAccept(peerFd, peerEndpoint);
            ::close(peerFd);
        });
        worker->resume();
    }
}

inline void Server::close() {
    if(_fd != SOCKET_INVALID) {
        ::close(_fd);
        _fd = SOCKET_INVALID;
    }
}

template <typename F>
inline void Server::bind(const std::string &method, const F &func) {
    _table[method] = detail::CallProxy<F>(func);
}

inline int Server::error() {
    int err = _errno;
    _errno = 0;
    return err;
}

inline void Server::setTimeout(std::chrono::milliseconds timeout) {
    _timeout = timeout;
}

inline void Server::setPending(std::chrono::milliseconds pending) {
    _pending = pending;
}

template <typename Func>
inline void Server::onRequest(Func &&requestCallback) {
    _requestCallback = std::forward<Func>(requestCallback);
}

template <typename Func>
inline void Server::onResponse(Func &&responseCallback) {
    _responseCallback = std::forward<Func>(responseCallback);
}

inline Server::Server(Endpoint endpoint)
    : _fd(SOCKET_INVALID),
      _endpoint(endpoint),
      _errno(0)
{}

inline Server::Server(Server &&rhs)
    : _fd(rhs._fd),
      _endpoint(rhs._endpoint),
      _errno(rhs._errno),
      _table(std::move(rhs._table)),
      _timeout(rhs._timeout),
      _pending(rhs._pending),
      _codec(rhs._codec),
      _requestCallback(std::move(rhs._requestCallback)),
      _responseCallback(std::move(rhs._responseCallback))
{
    rhs._fd = SOCKET_INVALID;
}

inline Server& Server::operator=(Server that) {
    that.swap(*this);
    return *this;
}

inline Server::~Server() {
    this->close();
}

inline void Server::init() {
    _fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if(_fd < 0) {
        _errno = errno;
        return;
    }
    int opt = 1;
    if(::setsockopt(_fd, SOL_SOCKET, SO_REUSEADDR, &opt,
            static_cast<socklen_t>(sizeof opt))) {
        _errno = errno;
        return;
    }
    if(::setsockopt(_fd, SOL_SOCKET, SO_REUSEPORT, &opt,
            static_cast<socklen_t>(sizeof opt))) {
        _errno = errno;
        return;
    }
    if(::bind(_fd, (const sockaddr*)&_endpoint, sizeof _endpoint)) {
        _errno = errno;
        return;
    }
}

inline void Server::swap(Server &that) {
    using std::swap;
    swap(this->_fd, that._fd);
    swap(this->_endpoint, that._endpoint);
    swap(this->_table, that._table);
    swap(this->_errno, that._errno);
    swap(this->_timeout, that._timeout);
    swap(this->_pending, that._pending);
    swap(this->_codec, that._codec);
    swap(this->_requestCallback, that._requestCallback);
    swap(this->_responseCallback, that._responseCallback);
}

inline std::optional<Server> Server::make(Endpoint endpoint) {
    Server server {endpoint};
    server.init();
    if(server.error()) return std::nullopt;
    return server;
}

inline Server::ProtocolType Server::netCall(const std::string &method, ProtocolType args) {
    auto methodHandle = _table.find(method);
    if(methodHandle != _table.end()) {
        auto &proxy = methodHandle->second;
        return proxy(std::move(args));
    } else {
        throw detail::protocol::Exception::makeMethodNotFoundException();
    }
}

inline void Server::onAccept(int peerFd, Endpoint peerEndpoint) {
    char buf[BUF_SIZE_ON_STACK];
    while(1) {
        char *cur = buf;
        using Header = detail::Codec::Header;
        // TODO long connection should enlarge timeout here
        if(!bestEffortPending(peerFd)) {
            break;
        }
        if(!bestEffortRead(peerFd, buf, sizeof(Header))) {
            break;
        }
        auto [headerVerified, contentLength] = _codec.contentLength(cur, sizeof(Header));
        if(!headerVerified) {
            _errno = EPROTO;
            break;
        }

        cur += sizeof(Header);

        if(!bestEffortRead(peerFd, cur, contentLength)) {
            break;
        }

        auto totalLength = sizeof(Header) + contentLength;
        if(!_codec.verify(buf, totalLength)) {
            _errno = EPROTO;
            break;
        }

        auto request = _codec.decode(buf, totalLength);

        if(_requestCallback && !_requestCallback(request)) {
            continue;
        }

        auto response = detail::makeEmptyResponse(request);

        // TODO lvalue
        auto [method, args] = _codec.prepareNetCall(std::move(request));

        // try-catch can capture all the exceptions without modifying CallProxy function signatures
        //     and remote exceptions in any bound function can be rethrown to RPC client
        // TODO auto [result, err, errorLayer] = netCall(...)
        try {
            auto result = netCall(method, std::move(args));
            _codec.fillResultToResponse(response, std::move(result));
        } catch(const detail::protocol::Exception &e) {
            _codec.reportError(response, e);
        } catch(const detail::Codec::InstanceException &e) {
            _codec.reportError(response, detail::protocol::Exception::makeParseErrorException());
        } catch(const std::exception &e) {
            _codec.reportError(response, detail::protocol::Exception::makeInternalErrorException());
        }

        if(_responseCallback && !_responseCallback(response)) {
            continue;
        }

        auto [dump, responseLength, responseBeLength] = _codec.dump(response);

        if(!bestEffortWrite(peerFd, &responseBeLength, sizeof(Header))
                || !bestEffortWrite(peerFd, dump.c_str(), responseLength)) {
            break;
        }

    }
}

inline bool Server::bestEffortRead(int peer, const void *buf, size_t size, size_t maxRetries) {
    if(detail::bestEffortRead(peer, buf, size, _timeout, maxRetries) == size) {
        return true;
    }
    if(!(_errno = errno)) {
        // if read m bytes but n > m
        // it is also ETIMEDOUT
        _errno = ETIMEDOUT;
    }
    return false;
}

inline bool Server::bestEffortWrite(int peer, const void *buf, size_t size, size_t maxRetries) {
    if(detail::bestEffortWrite(peer, buf, size, _timeout, maxRetries) == size) {
        return true;
    }
    if(!(_errno = errno)) {
        _errno = ETIMEDOUT;
    }
    return false;
}

inline bool Server::bestEffortPending(int peer) {
    constexpr static ssize_t FIRST_BYTE = 1;
    auto hook = [=](int peer, const void *, size_t /*same as return value*/) {
        return FIRST_BYTE;
    };
    // internal poll mode must be LT
    // because we don't actually read 1 byte
    if(detail::bestEffortTemplate(hook, POLLIN, peer, nullptr, FIRST_BYTE, _pending, 1) == FIRST_BYTE) {
        return true;
    }
    if(!(_errno = errno)) {
        _errno = ETIMEDOUT;
    }
    return false;
}

} // trpc
