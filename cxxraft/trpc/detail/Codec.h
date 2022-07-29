#pragma once
#include <netinet/in.h>
#include <cstddef>
#include "vsjson.hpp"
#include "protocol.h"
namespace trpc {
namespace detail {

// TODO merge `codec` and `resolve`
class Codec {

// type alias
// hide json implementation and make abstract RPC protocol easy to migrate
public:

    using Header = uint32_t;
    constexpr static Header INVALID_HEADER = -1;

    using ProtocolType = vsjson::Json;

    using InstanceException = vsjson::JsonException;

// json stream
public:

    std::tuple<bool, Header> contentLength(const char *buf, size_t N) const;

    bool verify(const char *buf, size_t N) const;

    vsjson::Json decode(const char *buf, size_t N) const;

    std::tuple<std::string, Header, Header> dump(vsjson::Json &response) const;

// protocol
public:

    void reportError(vsjson::Json &response, const protocol::Exception &e) const;

    std::tuple<std::string, vsjson::Json> prepareNetCall(vsjson::Json request) const;

    void fillResultToResponse(vsjson::Json &response, vsjson::Json result) const;
};

inline std::tuple<bool, Codec::Header> Codec::contentLength(const char *buf, size_t N) const {
    if(N < sizeof(uint32_t)) return {false, INVALID_HEADER};
    Header beLength = *(Header*)(buf);
    Header length = ::ntohl(beLength);
    return {true, length};
}

inline bool Codec::verify(const char *buf, size_t N) const {
    auto [succ, length] = contentLength(buf, N);
    return succ && N >= sizeof(uint32_t) + length;
}

inline vsjson::Json Codec::decode(const char *buf, size_t N) const {
    return vsjson::parse(buf + sizeof(uint32_t));
}

inline void Codec::reportError(vsjson::Json &response, const protocol::Exception &e) const {
    if(response.contains(protocol::Field::result)) {
        auto &obj = response.as<vsjson::ObjectImpl>();
        obj.erase(protocol::Field::result);
    }
    response[protocol::Field::error] = {
        {protocol::Field::code, e.code()}
    };
    if(!e.message().empty()) {
        auto &err = response[protocol::Field::error];
        err[protocol::Field::message] = e.message();
    }
}

inline std::tuple<std::string, Codec::Header, Codec::Header>
Codec::dump(vsjson::Json &response) const {
    std::string dump = response.dump();
    Header responseLength = dump.length();
    Header responseLengthBeLength = ::htonl(responseLength);
    return {std::move(dump), responseLength, responseLengthBeLength};
}


inline std::tuple<std::string, vsjson::Json> Codec::prepareNetCall(vsjson::Json request) const {
    auto method = std::move(request[detail::protocol::Field::method]).to<std::string>();
    auto args = std::move(request[detail::protocol::Field::params]);
    return std::make_tuple(std::move(method), std::move(args));
}

inline void Codec::fillResultToResponse(vsjson::Json &response, vsjson::Json result) const {
    response[detail::protocol::Field::result] = std::move(result);
}

} // detail
} // trpc
