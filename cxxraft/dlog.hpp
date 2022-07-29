#pragma once

#ifdef CXXRAFT_FLAG_DLOG_ENABLE
    #include "dlog/Log.hpp"
    #define LOG_DEBUG(...) DLOG_DEBUG_ALIGN(__VA_ARGS__)
    #define LOG_INFO(...)  DLOG_INFO_ALIGN(__VA_ARGS__)
    #define LOG_WARN(...)  DLOG_WARN_ALIGN(__VA_ARGS__)
    #define LOG_ERROR(...) DLOG_ERROR_ALIGN(__VA_ARGS__)
    #define LOG_WTF(...)   DLOG_WTF_ALIGN(__VA_ARGS__)
    namespace cxxraft { using dlog::Log; }
    namespace cxxraft { using dlog::IoVector; }
    namespace cxxraft { using dlog::filename; }
#else
    #define LOG_DEBUG(...) (void)0
    #define LOG_INFO(...)  (void)0
    #define LOG_WARN(...)  (void)0
    #define LOG_ERROR(...) (void)0
    #define LOG_WTF(...)   (void)0
    #define DLOG_CONF_PATH "null.conf"
#endif

#define CXXRAFT_LOG_DEBUG(...) LOG_DEBUG("[cxxraft]", __VA_ARGS__)
#define CXXRAFT_LOG_INFO(...)  LOG_INFO("[cxxraft]", __VA_ARGS__)
#define CXXRAFT_LOG_WARN(...)  LOG_WARN("[cxxraft]", __VA_ARGS__)
#define CXXRAFT_LOG_ERROR(...) LOG_ERROR("[cxxraft]", __VA_ARGS__)
#define CXXRAFT_LOG_WTF(...)   LOG_WTF("[cxxraft]", __VA_ARGS__)
