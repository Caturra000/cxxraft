#pragma once
#include <unistd.h>
#include <sys/wait.h>
#include <cstddef>
#include <vector>
#include <tuple>
#include "trpc.hpp"
#include "dlog.hpp"
#include "co.hpp"

using TestFunction = void(*)();
using Result = std::tuple<int, int>;

inline auto createPeers(size_t size, uint16_t firstPort = 2333) {
    std::vector<trpc::Endpoint> endpoints;
    while(size--) {
        endpoints.emplace_back("127.0.0.1", firstPort++);
    }
    return endpoints;
}

// [testcase][success, failed]
inline auto prepareResults(size_t size) {
    std::vector<Result> results(size);
    return results;
}

inline void runTest(TestFunction func) {
    dlog::Log::init();
    auto &env = co::open();
    env.createCoroutine(func)->resume();
    co::loop();
};

inline void done(std::vector<Result> &results, int testcase, int ret) {
    auto &[success, failed] = results[testcase];
    if(ret) {
        failed++;
        std::cerr << "====FAILED====" << std::endl;
    }
    else success++;
}

inline void report(std::vector<Result> &results) {
    for(int testcase = 0; testcase < results.size(); ++testcase) {
        auto [success, failed] = results[testcase];
        std::cout << "====TEST CASE " << testcase << "====" << std::endl
                  << "Done:    " << success + failed << std::endl
                  << "Success: " << success << std::endl
                  << "Failed:  " << failed << std::endl;
    }
}

// test routine
template <size_t N>
inline void runTestsAndReport(TestFunction (&tests)[N], int count) {

    auto results = prepareResults(N);

    for(int testcase = 0; testcase < N; ++testcase) {
        for(int round = 0; round < count; round++) {
            if(int pid, ret; pid = ::fork()) {
                ::waitpid(pid, &ret, 0);
                done(results, testcase, ret);
            } else {
                std::cout << "case: " << testcase << ", "
                          << "round: " << round << std::endl;
                runTest(tests[testcase]);
            }
        }
    }

    // report
    report(results);
}
