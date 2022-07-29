#pragma once
#include <string>
#include "vsjson.hpp"
#include "protocol.h"

namespace trpc {
namespace detail {

// request -> response -> result


/// request

template <typename ...Args>
vsjson::Json makeRequest(int token, const std::string &method, Args &&...params);

template <typename Arg, typename ...Args>
void makeRequestImpl(vsjson::Json &json, Arg &&arg, Args &&...args);

template <typename ...Args>
void makeRequestImpl(vsjson::Json &json, Args &&...args);

/// response

vsjson::Json makeEmptyResponse(vsjson::Json &request);

/// result

template <typename T>
std::optional<T> makeResult(vsjson::Json &response);


template <typename ...Args>
inline vsjson::Json makeRequest(int token, const std::string &method, Args &&...params) {
    vsjson::Json json =
    {
        {protocol::Field::jsonrpc, protocol::Attribute::version},
        {protocol::Field::id, token},
        {protocol::Field::method, method},
        {protocol::Field::params, vsjson::Json::array()}
    };
    vsjson::Json &argsJson = json[protocol::Field::params];
    makeRequestImpl(argsJson, std::forward<Args>(params)...);
    return json;
}

template <typename Arg, typename ...Args>
inline void makeRequestImpl(vsjson::Json &json, Arg &&arg, Args &&...args) {
    json.append(std::forward<Arg>(arg));
    makeRequestImpl(json, std::forward<Args>(args)...);
}

template <typename ...Args>
inline void makeRequestImpl(vsjson::Json &json, Args &&...args) {}

inline vsjson::Json makeEmptyResponse(vsjson::Json &request) {
    vsjson::Json response =
    {
        {detail::protocol::Field::jsonrpc, detail::protocol::Attribute::version},
        {detail::protocol::Field::id, request[detail::protocol::Field::id].to<int>()},
    };
    return response;
}

template <typename T>
inline std::optional<T> makeResult(vsjson::Json &response) {
    if(!response.contains(protocol::Field::error)) {
        return response[protocol::Field::result].to<T>();
    }
    auto &errorObject = response[protocol::Field::error];
    if(errorObject.contains(protocol::Field::message)) {
        // TODO parse message
        return std::nullopt;
    }
    return std::nullopt;
}


} // detail
} // trpc
