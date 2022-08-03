#pragma once
#include <type_traits>
#include <tuple>
#include <vector>
#include "vsjson.hpp"

namespace cxxraft {

// A serializable abstract tuple, similar to std::tuple
//
// You can construct a Message from std::make_tuple
// For example:
// Message<int, std::string> message = std::make_tuple(x, y);
//
// It is safe to down-cast Message to std::tuple by reference and pointer
// (in general, std::tuple and Message can be converted to each other at zero-overhead)
// For example:
// std::tuple<int, std::string> &tup = message.cast();
//
// Also you can get result by structured binding and std::get<>()
// Example 1:
// auto [x, y] = message.cast();
// Example 2:
// auto x = std::get<0>(message);
//
// And it can be used in RPC services directly
// See cxxraft::Reply, raft::appendEntryRPC and raft::requestVoteRPC
template <typename ...Ts>
class Message: public std::tuple<Ts...> {
public:
    using Base = std::tuple<Ts...>;
    Message() = default;
    Message(Base &&base): Base(static_cast<Base&&>(base)) {}
    Message(const Base &base): Base(base) {}

    // tuple to json
    operator vsjson::Json() { return makeJsonArray(S{}); }

    // json (array) to tuple
    Message(const vsjson::ArrayImpl &array): Message(array, S{}) {}
    Message(vsjson::ArrayImpl &&array): Message(std::move(array), S{}) {}


    // help structured binding
    // example: auto [x, y, z] = message.cast();
    // (I don't know why it can't be deducted automatically...)
    Base&       cast()       & { return *this; }
    const Base& cast() const & { return const_cast<const Base&>(*this); }
    Base&&      cast()      && { return static_cast<Base&&>(*this); }

// what you don't care about
private:

    using S = std::make_index_sequence<sizeof...(Ts)>;

    template <size_t ...Is>
    Message(const vsjson::ArrayImpl &array, std::index_sequence<Is...>)
        : Base{array[Is].to<std::tuple_element_t<Is, Base>>()...} {}

    template <size_t ...Is>
    Message(vsjson::ArrayImpl &&array, std::index_sequence<Is...>)
        : Base{std::move(array[Is]).to<std::tuple_element_t<Is, Base>>()...} {}

    template <size_t ...Is>
    vsjson::ArrayImpl makeJsonArray(std::index_sequence<Is...>) const & {
        vsjson::ArrayImpl array;
        using ForEach = std::initializer_list<const char*>;
        ForEach { (array.emplace_back(std::get<Is>(*this)), "xiaomi jin tian dao bi le ma?")... };
        return array;
    }

    template <size_t ...Is>
    vsjson::ArrayImpl makeJsonArray(std::index_sequence<Is...>) && {
        vsjson::ArrayImpl array;
        using ForEach = std::initializer_list<const char*>;
        ForEach { (array.emplace_back(std::move(std::get<Is>(*this))), "xiaomi dao bi la!")... };
        return array;
    }
};

} // cxxraft
