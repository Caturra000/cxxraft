#pragma once
#include "vsjson.hpp"
#include "FunctionTraits.h"
#include "protocol.h"
namespace trpc {
namespace detail {

template <typename F>
class CallProxy final {
public:
    CallProxy(F func): _func(std::move(func)) {}

    vsjson::Json operator()(vsjson::Json json) { return dispatch(json); }

private:

    template <typename WrappedRet = std::conditional_t<
        std::is_same<typename FunctionTraits<F>::ReturnType, void>::value,
            nullptr_t, typename FunctionTraits<F>::ReturnType>>
    WrappedRet dispatch(vsjson::Json &args) {
        using Ret = typename FunctionTraits<F>::ReturnType;
        using ArgsTuple = typename FunctionTraits<F>::ArgsTuple;
        constexpr size_t N = FunctionTraits<F>::ArgsSize;
        if(N != args.arraySize()) {
            throw protocol::Exception::makeInvalidParamsException();
        }
        ArgsTuple argsTuple = make<ArgsTuple>(args, std::make_index_sequence<N>{});
        WrappedRet result = invoke<Ret>(std::move(argsTuple));
        return result;
    }

    template <typename Tuple, size_t ...Is>
    Tuple make(vsjson::Json &json, std::index_sequence<Is...>) {
        Tuple tuple;
        std::initializer_list<int> { (get<Tuple, Is>(json, tuple), 0)... };
        return tuple;
    }

    template <typename Tuple, size_t I>
    void get(vsjson::Json &from, Tuple &to) {
        using ElemType = std::decay_t<decltype(std::get<I>(to))>;
        std::get<I>(to) = std::move(from[I]).to<ElemType>();
    }

    template <typename Ret, typename Tuple,
        typename = std::enable_if_t<!std::is_same<Ret, void>::value>>
    Ret invoke(Tuple &&tuple) {
        constexpr size_t N = std::tuple_size<std::decay_t<Tuple>>::value;
        return invokeImpl<Ret>(std::forward<Tuple>(tuple), std::make_index_sequence<N>{});
    }

    template <typename Ret, typename Tuple,
        typename = std::enable_if_t<std::is_same<Ret, void>::value>>
    nullptr_t invoke(Tuple &&tuple) {
        constexpr size_t N = std::tuple_size<std::decay_t<Tuple>>::value;
        invokeImpl<Ret>(std::forward<Tuple>(tuple), std::make_index_sequence<N>{});
        return nullptr;
    }

    template <typename Ret, typename Tuple, size_t ...Is>
    Ret invokeImpl(Tuple &&tuple, std::index_sequence<Is...>) {
        return _func(std::get<Is>(std::forward<Tuple>(tuple))...);
    }

private:
    std::decay_t<F> _func;
};

// template <typename F>
// inline CallProxy<F>::error() {
//     int err = _errno;
//     _errno = 0;
//     return err;
// }

} // detail
} // trpc
