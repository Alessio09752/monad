#pragma once

#include <category/core/assert.h>
#include <category/core/config.hpp>
#include <category/core/fiber/priority_task.hpp>
#include <category/core/likely.h>

#include <blockingconcurrentqueue.h>

#include <cstdio>
#include <stop_token>
#include <thread>
#include <vector>

#include <pthread.h>

MONAD_NAMESPACE_BEGIN

static_assert(std::is_same_v<std::jthread::native_handle_type, pthread_t>);

class ThreadPool
{
    unsigned const nthreads_;

    moodycamel::BlockingConcurrentQueue<fiber::PriorityTask> queue_{};

    std::vector<std::jthread> threads_{};

    std::vector<int64_t> priorities_{};

public:
    ThreadPool(unsigned const nthreads)
        : nthreads_{nthreads}
        , priorities_(nthreads_, INT64_MAX)
    {
        threads_.reserve(nthreads_);
        for (unsigned i = 0; i < nthreads_; ++i) {
            std::jthread thread{[this, i](std::stop_token const stop_token) {
                char name[16];
                std::snprintf(name, 16, "worker %u", i);
                pthread_setname_np(pthread_self(), name);

                while (!stop_token.stop_requested()) {
                    fiber::PriorityTask task{};
                    if (MONAD_LIKELY(queue_.wait_dequeue_timed(task, 100000))) {
                        // TODO set priority
                        task.task();
                    }
                }
            }};
            threads_.emplace_back(std::move(thread));
        }
    }

    ~ThreadPool()
    {
        for (auto &thread : threads_) {
            thread.request_stop();
        }
        while (!threads_.empty()) {
            auto &thread = threads_.back();
            thread.join();
            threads_.pop_back();
        }
    }

    void submit(uint64_t const priority, std::function<void()> task)
    {
        MONAD_ASSERT(queue_.enqueue({priority, std::move(task)}));
    }
};

MONAD_NAMESPACE_END
