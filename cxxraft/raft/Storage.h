#pragma once
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <string>
#include <vector>
#include <tuple>
#include <optional>
#include "raft/Log.h"

namespace cxxraft {

// persistant storage using write-ahead log
class Storage {
public:

    // full backup for [term, voteFor, log]
    bool backup(int currentTerm, int voteFor, Log::EntriesSlice entries);

    // full restore for [term, voteFor, log]
    // redo everything
    auto restore() -> std::optional<std::tuple<int, int, Log::EntriesArray>>;

    bool writeCurrentTerm(int currentTerm);
    bool writeVoteFor(int voteFor);
    bool truncateLog(int index);
    bool writeLog(Log::EntriesSlice slice);
    bool writeLog(Log::EntriesArray array);
    bool appendLog(Log::EntriesSlice slice);
    bool appendLog(Log::EntriesArray array);

    // commit all write operations
    // Note: write and sync are separate for performance
    void sync();

    bool isInMemory();

    /// for WAL compress

    void setCompress(bool on);
    bool isCompress();
    bool ftruncate();

public:

    // in memory storage
    Storage();

    Storage(std::string path);

    ~Storage();

public:

    // whatever...
    constexpr static int OPEN_MODE = 0644;

    // **blocking** and **filesystem** IO
    // O_APPEND for write-ahead log (WAL)
    // O_DIRECT for direct disk access without page cache
    constexpr static int OPEN_FLAGS =
        O_CREAT | O_RDWR | O_APPEND | O_DIRECT;

    constexpr static int IN_MEMORY_OR_NO_FD = -1;

    /// opeartion code

    constexpr static char OP_APPEND         = 'A';
    constexpr static char OP_CURRENT_TERM   = 'C';
    constexpr static char OP_LOG            = 'L';
    constexpr static char OP_TRUNCATE       = 'T';
    constexpr static char OP_VOTE_FOR       = 'V';

private:

    // TODO writePodType<T>

    bool writeInt(char opcode, int integer);
    bool writeJson(char opcode, vsjson::Json json);

private:
    std::string _path;
    int _fd;
    bool _compress;
};

inline bool Storage::backup(int currentTerm, int voteFor, Log::EntriesSlice entries) {
    return writeCurrentTerm(currentTerm)
        && writeVoteFor(voteFor)
        && writeLog(entries);
}

inline auto Storage::restore() -> std::optional<std::tuple<int, int, Log::EntriesArray>> {
    if(isInMemory()) return std::nullopt;

    /// get file size

    struct stat st;
    ::fstat(_fd, &st);
    off_t size = st.st_size;

    if(size == 0) {
        CXXRAFT_LOG_DEBUG("empty file. ignored");
        return std::nullopt;
    }

    /// read file and make buffer

    std::string sbuf;
    sbuf.reserve(size);
    char *buf = sbuf.data();

    if(int n = ::read(_fd, buf, sbuf.size()); n ^ sbuf.size()) {
        CXXRAFT_LOG_ERROR("read failed. read bytes:", n);
        return std::nullopt;
    }

    /// redo

    off_t consumed = 0;

    int currentTerm = -1;
    // TODO optional voteFor
    int voteFor = -1;
    Log::EntriesArray entries;

    auto consumeInt = [&] {
        int v = *reinterpret_cast<int*>(buf + consumed);
        consumed += sizeof(int);
        return v;
    };

    auto consumeJson = [&consumed](const char *buf) {
        const char *cursor = buf;
        // TODO refactor
        auto json = vsjson::parser::parseImpl(cursor);
        consumed += std::distance(buf, cursor);
        return json;
    };

    auto redoAppend = [&] {
        auto json = consumeJson(buf + consumed);
        Log::EntriesArray parsed {json.as<vsjson::ArrayImpl>()};
        for(auto &&entry : parsed) {
            entries.emplace_back(std::move(entry));
        }
    };

    auto redoLog = [&] {
        auto json = consumeJson(buf + consumed);
        entries = json.as<vsjson::ArrayImpl>();
    };

    auto redoTruncate = [&] {
        int index = consumeInt();
        while(!entries.empty()
                && Log::getIndex(entries.back()) >= index) {
            entries.pop_back();
        }
    };

    while(consumed < sbuf.size()) {
        switch(buf[consumed++]) {
        case OP_APPEND:
            redoAppend();
            break;
        case OP_CURRENT_TERM:
            currentTerm = consumeInt();
            break;
        case OP_LOG:
            redoLog();
            break;
        case OP_TRUNCATE:
            redoTruncate();
            break;
        case OP_VOTE_FOR:
            voteFor = consumeInt();
            break;
        default:
            CXXRAFT_LOG_WTF("unknwon opcode:", buf[consumed - 1]);
        }
    }
    return std::make_tuple(currentTerm, voteFor, std::move(entries));
}

inline bool Storage::writeCurrentTerm(int currentTerm) {
    if(isInMemory()) return true;
    return writeInt(OP_VOTE_FOR, currentTerm);
}

inline bool Storage::writeVoteFor(int voteFor) {
    if(isInMemory()) return true;
    return writeInt(OP_VOTE_FOR, voteFor);
}

inline bool Storage::truncateLog(int index) {
    if(isInMemory()) return true;
    return writeInt(OP_TRUNCATE, index);
}

inline bool Storage::writeLog(Log::EntriesSlice slice) {
    if(isInMemory()) return true;
    vsjson::Json json = slice;
    return writeJson(OP_LOG, std::move(json));
}

inline bool Storage::writeLog(Log::EntriesArray array) {
    if(isInMemory()) return true;
    vsjson::Json json = array;
    return writeJson(OP_LOG, std::move(json));
}

inline bool Storage::appendLog(Log::EntriesSlice slice) {
    if(isInMemory()) return true;
    vsjson::Json json = slice;
    return writeJson(OP_APPEND, std::move(json));
}

inline bool Storage::appendLog(Log::EntriesArray array) {
    if(isInMemory()) return true;
    vsjson::Json json = array;
    return writeJson(OP_APPEND, std::move(json));
}

inline void Storage::sync() {
    if(isInMemory()) return;
    ::fsync(_fd);
}

inline bool Storage::isInMemory() {
    return _fd == IN_MEMORY_OR_NO_FD;
}

inline void Storage::setCompress(bool on) {
    _compress = on;
}

inline bool Storage::isCompress() {
    return _compress;
}

inline bool Storage::ftruncate() {
    if(isInMemory() || !_compress) return true;
    if(::ftruncate(_fd, 0)) {
        CXXRAFT_LOG_DEBUG("ftruncate failed. fd:", _fd, "reason:", ::strerror(errno));
        return false;
    }
    return true;
}

inline Storage::Storage()
    : _fd(IN_MEMORY_OR_NO_FD),
      _compress(false)
{
    CXXRAFT_LOG_INFO("in-memory mode or no backing device");
}

inline Storage::Storage(std::string path)
    : _path(std::move(path)),
      _fd(::open(_path.c_str(), OPEN_FLAGS, OPEN_MODE)),
      _compress(false)
{
    if(_fd < 0) {
        CXXRAFT_LOG_WARN("cannot open file. path:", _path, "reason:", ::strerror(errno));
        CXXRAFT_LOG_WARN("switch to in-memory mode");
        _fd = IN_MEMORY_OR_NO_FD;
    } else {
        CXXRAFT_LOG_INFO("preapre for storage. path:", _path);
    }
}

inline Storage::~Storage() {
    if(_fd != IN_MEMORY_OR_NO_FD) {
        ::close(_fd);
    }
}

inline bool Storage::writeInt(char opcode, int integer) {
    char buf[sizeof "xiaomidaobilema"];
    int *fill = reinterpret_cast<int*>(buf + sizeof(char));

    *buf = opcode;
    *fill = integer;

    constexpr ssize_t size = sizeof(char) + sizeof(int);
    if(size != ::write(_fd, buf, size)) {
        CXXRAFT_LOG_ERROR("write failed. reason:", ::strerror(errno));
        return false;
    }
    return true;
}

inline bool Storage::writeJson(char opcode, vsjson::Json json) {
    std::string buf = json.dump();
    iovec v[2] {
        {.iov_base = &opcode, .iov_len = 1},
        {.iov_base = buf.data(), .iov_len = buf.size()}
    };
    ssize_t size = v[0].iov_len + v[1].iov_len;
    if(::writev(_fd, v, 2) != size) {
        CXXRAFT_LOG_ERROR("write failed. reason:", ::strerror(errno));
        return false;
    }
    return true;
}

} // cxxraft
