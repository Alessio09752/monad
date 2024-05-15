#include <monad/mpt/config.hpp>

MONAD_MPT_NAMESPACE_BEGIN

// Turn on to collect stats
#define MONAD_MPT_COLLECT_STATS 1

namespace detail
{
    struct TrieUpdateCollectedStats
    {
        // counters
        unsigned num_nodes_created{0};
        unsigned num_nodes_copied{0};
        unsigned num_compaction_reads{0};

        unsigned nodes_to_compact_in_fast{0}; // fast to slow
        unsigned nodes_to_compact_in_slow{0}; // slow to slow
        unsigned nodes_copied_fast_to_fast_for_fast{0};
        unsigned nodes_copied_fast_to_fast_for_slow{0};
        unsigned nodes_copied_slow_to_slow_for_slow{0};
        // sum of the three below equals the current block slow ring
        // growth
        unsigned bytes_to_keep_in_fast{0}; // copied from fast to slow
        unsigned bytes_to_keep_in_slow{0};
        unsigned bytes_copied_slow_to_slow_for_slow{0};

        // [0]: fast, [1]: slow
        unsigned nreads_before_offset[2] = {0, 0};
        unsigned nreads_after_offset[2] = {0, 0};
        unsigned bytes_read_before_offset[2] = {0, 0};
        unsigned bytes_read_after_offset[2] = {0, 0};

        void reset()
        {
            this->~TrieUpdateCollectedStats();
            new (this) TrieUpdateCollectedStats();
        }
    };

    static_assert(sizeof(TrieUpdateCollectedStats) == 76);
    static_assert(alignof(TrieUpdateCollectedStats) == 4);
    static_assert(std::is_trivially_copyable_v<TrieUpdateCollectedStats>);
}

MONAD_MPT_NAMESPACE_END
