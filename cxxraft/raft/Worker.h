#pragma once
#include <queue>
#include "co.hpp"
namespace cxxraft {

// Simple job scheduler
// Running coroutines in a synchronized way
//
// Note:
// * DON'T forget to co::loop()!
// * don't co::this_coroutine::yield() if you don't know what to do
class Worker {

public:

    // A general function entry
    //
    // You can capture std::shared_ptr<int> as a token in lambda
    // It can abort this job before start if necessary
    // (or better weak_ptr, you can know job is done if .lock() failed)
    using Job = std::function<void()>;

    // hint for schedule
    struct Later {};

public:

    // main interface
    // may start immediately
    // you can post a default empty std::function<> to wake up worker
    void post(Job &&job) {
        post(Later{}, std::move(job));
        wakeup();
    }

    // lvalue reference version
    void post(const Job &job) {
        post(Later{}, job);
        wakeup();
    }

    // it won't schedule immediately
    // just post a job
    void post(Later, Job job) {
        if(job) {
            _jobs.emplace(std::move(job));
        }
    }

    // note: you can't wakeup two or more workers
    void wakeup() {
        // if no job to do
        // this worker will be killed by capitalist
        // it means “降本增效” in Chinese
        if(!_worker || _worker->exit()) {
            auto &env = co::open();
            // create or restart
            _worker = env.createCoroutine([this] {
                while(!_jobs.empty()) {
                    auto nextJob = std::move(_jobs.front());
                    _jobs.pop();
                    nextJob();
                }
                // _worker = nullptr;
            });
            _worker->resume();
        }
    }

private:

    std::shared_ptr<co::Coroutine> _worker;

    std::queue<Job> _jobs;
};

} // cxxraft
