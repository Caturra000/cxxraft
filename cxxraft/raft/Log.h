#pragma once
#include <vector>
#include <tuple>
#include <optional>
#include "vsjson.hpp"
#include "raft/Message.h"
#include "raft/Command.h"
namespace cxxraft {

// In memory log
// Logs are composed of entries, which are numbered
// sequentially. Each entry contains the term in which it was
// created (the number in each box) and a command for the state
// machine. An entry is considered committed if it is safe for that
// entry to be applied to state machines.
class Log {

public:

    // [index, term]
    //
    // @index
    // identifies its position in the log
    //
    // @term
    // used to detect inconsistencies between logs
    // and to ensure some of the properties in Figure 3
    //
    // <del>@commited</del>
    // <del>whether it is safe for that entry to be applied
    // to state machines</del>
    //
    using Metadata = Message<int, int>;

    // raft metadata + user command
    using Entry = std::tuple<Metadata, Command>;

    // serializable vector for RPC
    class EntriesArray;

    // !EXPERIMENTAL
    // serializable referece for RPC
    class EntriesSlice;

    struct ByReference {};
    struct ByPointer {};
    struct Optional {};

public:

    // get immutable reference by index
    //     const auto &entry = log.get(...)
    // or copy value:
    //     auto entry = log.get(...)
    const Entry& get(int index) const;

    Entry& get(int index, ByReference);
    Entry* get(int index, ByPointer);
    auto   get(int index, Optional) const -> std::optional<Entry>;

    void append(Entry entry);
    void append(Metadata, Command);

    int size() { return _entries.size(); }

    const Entry& back() const;
    Entry& back(ByReference);

    // TODO overwrite
    // assign and truncate
    // void overwrite();

    // TODO apply
    // bool apply(int index);

    EntriesArray fork();
    EntriesSlice fork(ByReference);

    // [first, last)
    EntriesArray fork(int first, int last);
    EntriesSlice fork(int first, int last, ByReference);

public:

    // TODO restore from backing devices
    Log();

private:

    // EntriesArray _entries;

    std::vector<Entry> _entries;
};

class Log::EntriesArray: public std::vector<Log::Entry> {
public:
    using Metadata = Log::Metadata;
    using Entry = Log::Entry;
    using Base = std::vector<Log::Entry>;

    using Base::Base;

    // vector to json
    operator vsjson::Json();

    // json array to vector
    EntriesArray(const vsjson::ArrayImpl &array);
    EntriesArray(vsjson::ArrayImpl &&array);
};

// [first, last)
class Log::EntriesSlice {
public:
    EntriesSlice(int first, int last, std::vector<Entry> *self)
        : _first(first), _last(last), _entries(self) {}

    operator vsjson::Json() {
        vsjson::ArrayImpl array;
        for(int index = _first; index < _last; ++index) {
            auto &entry = _entries->operator[](index);
            auto &[metadata, command] = entry;
            vsjson::ArrayImpl impl;
            impl.push_back(metadata);
            impl.push_back(command);
            array.emplace_back(std::move(impl));
        }
        return array;
    }

private:
    int _first;
    int _last;
    std::vector<Entry> *_entries;
};

inline const Log::Entry& Log::get(int index) const {
    return _entries[index];
}

inline Log::Entry& Log::get(int index, Log::ByReference) {
    return _entries[index];
}

inline Log::Entry* Log::get(int index, Log::ByPointer) {
    if(index >= _entries.size() || index < 0) {
        return nullptr;
    }
    return &_entries[index];
}

inline auto Log::get(int index, Log::Optional) const -> std::optional<Entry> {
    if(index >= _entries.size() || index < 0) {
        return std::nullopt;
    }
    return _entries[index];
}

inline void Log::append(Entry entry) {
    _entries.emplace_back(std::move(entry));
}

inline void Log::append(Metadata metadata, Command command) {
    append(std::make_tuple(metadata, std::move(command)));
}

inline const Log::Entry& Log::back() const {
    return _entries.back();
}

inline Log::Entry& Log::back(Log::ByReference) {
    return _entries.back();
}

inline Log::EntriesArray Log::fork() {
    return this->fork(0, _entries.size());
}

inline Log::EntriesSlice Log::fork(Log::ByReference) {
    return this->fork(0, _entries.size(), ByReference{});
}

inline Log::EntriesArray Log::fork(int first, int last) {
    Log::EntriesArray child;
    child.reserve(last - first);
    for(auto index = first; index < last; ++index) {
        child.emplace_back(_entries[index]);
    }
    return child;
}

inline Log::EntriesSlice Log::fork(int first, int last, Log::ByReference) {
    Log::EntriesSlice slice(first, last, &_entries);
    return slice;
}

inline Log::Log() {
    // index 0
    Command placeholder = {
        {"jojo", "dio"}
    };
    Metadata metadata = std::make_tuple(0, -1);
    append(metadata, std::move(placeholder));
}

// vector to json
inline Log::EntriesArray::operator vsjson::Json() {
    vsjson::ArrayImpl array;
    for(auto &&entry : *this) {
        auto &[metadata, command] = entry;
        vsjson::ArrayImpl impl;
        impl.push_back(metadata);
        impl.push_back(command);
        array.emplace_back(std::move(impl));
    }
    return array;
}

// json array to vector
inline Log::EntriesArray::EntriesArray(const vsjson::ArrayImpl &array) {
    std::for_each(array.begin(), array.end(), [this](vsjson::ArrayImpl::const_reference elem) {
        emplace_back(elem[0].to<Metadata>(), elem[1].to<Command>());
    });
}

inline Log::EntriesArray::EntriesArray(vsjson::ArrayImpl &&array) {
    std::for_each(array.begin(), array.end(), [this](vsjson::ArrayImpl::reference elem) {
        emplace_back(std::move(elem[0]).to<Metadata>(), std::move(elem[1]).to<Command>());
    });
}

} // cxxraft
