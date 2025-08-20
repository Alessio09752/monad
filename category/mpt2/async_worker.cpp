#include <category/mpt2/async_worker.hpp>
#include <category/mpt2/blocking_spsc.hpp>
#include <category/mpt2/trie.hpp>

#include <atomic>
#include <chrono>
#include <thread>

MONAD_MPT2_NAMESPACE_BEGIN

AsyncWorker::AsyncWorker(UpdateAux &aux)
    : aux_(aux)
    , running_{true}
    , worker_thread_([&] { run(); })
{
}

void AsyncWorker::run()
{
    using namespace std::chrono_literals;

    // Run until told to stop.
    unsigned did_nothing_count = 0;
    bool did_nothing = false;
    while (running_.load(std::memory_order_acquire)) {
        if (aux_.async_queue_.empty()) {
            if (did_nothing) {
                did_nothing_count++;
            }
            else {
                did_nothing_count = 0;
            }
            if (did_nothing_count > 1000000) {
                std::this_thread::sleep_for(100ms);
            }
            continue;
        }
        did_nothing = false;
        MONAD_ASSERT(aux_.async_queue_.consume_one_notify_drained(
            [&](auto const &f) { f(); }));
    }

    // drain after stop (force durable).
    while (aux_.async_queue_.consume_one_notify_drained(
        [&](auto const &f) { f(); })) {
    }
}

void AsyncWorker::drain_and_stop()
{
    running_.store(false, std::memory_order_release);
    if (worker_thread_.joinable()) {
        worker_thread_.join();
    }
    MONAD_ASSERT(aux_.async_queue_.empty());
}

MONAD_MPT2_NAMESPACE_END
