#pragma once

#include <category/mpt2/blocking_spsc.hpp>
#include <category/mpt2/node.hpp>
#include <category/mpt2/node_cursor.hpp>
#include <category/mpt2/state_machine.hpp>
#include <category/mpt2/update.hpp>
#include <category/mpt2/util.hpp>
#include <category/storage/db_storage.hpp>

#include <cstdint>

#include <sys/mman.h>

MONAD_MPT2_NAMESPACE_BEGIN

class WriteTransaction;
class AsyncWorker;

struct MsyncJob
{
    chunk_offset_t fast_start{INVALID_OFFSET};
    chunk_offset_t fast_end{INVALID_OFFSET};
    chunk_offset_t slow_start{INVALID_OFFSET};
    chunk_offset_t slow_end{INVALID_OFFSET};
    chunk_offset_t root_offset{INVALID_OFFSET};
    uint64_t version{INVALID_BLOCK_NUM};
};

// auxiliary structure for upsert, WriteTransaction guarantees at most one
// thread modify it at a time, but multiple threads can read
class UpdateAux
{
    static constexpr unsigned msync_queue_capacity = 2000;

    friend class AsyncWorker;
    friend class WriteTransaction;

    bool enable_dynamic_history_length_{true};
    // bool can_write_to_fast_{true};

    /******** Compaction ********/
    uint32_t chunks_to_remove_before_count_fast_{0};
    uint32_t chunks_to_remove_before_count_slow_{0};
    // speed control var
    compact_virtual_chunk_offset_t last_block_end_offset_fast_{
        MIN_COMPACT_VIRTUAL_OFFSET};
    compact_virtual_chunk_offset_t last_block_end_offset_slow_{
        MIN_COMPACT_VIRTUAL_OFFSET};
    compact_virtual_chunk_offset_t last_block_disk_growth_fast_{
        MIN_COMPACT_VIRTUAL_OFFSET};
    compact_virtual_chunk_offset_t last_block_disk_growth_slow_{
        MIN_COMPACT_VIRTUAL_OFFSET};
    // compaction range
    compact_virtual_chunk_offset_t compact_offset_range_fast_{
        MIN_COMPACT_VIRTUAL_OFFSET};
    compact_virtual_chunk_offset_t compact_offset_range_slow_{
        MIN_COMPACT_VIRTUAL_OFFSET};

    MONAD_STORAGE_NAMESPACE::DbStorage &db_storage_;

    std::mutex write_mutex_;

    std::unique_ptr<AsyncWorker> async_worker_;

    // preload progress
    chunk_offset_t preload_offset_fast_{INVALID_OFFSET};
    chunk_offset_t preload_offset_slow_{INVALID_OFFSET};

    void clear_root_offsets_up_to_and_including(uint64_t version);
    void erase_versions_up_to_and_including(uint64_t version);
    void release_unreferenced_chunks();
    void free_compacted_chunks();

    void advance_compact_offsets();

    uint32_t num_chunks(
        MONAD_STORAGE_NAMESPACE::DbStorage::chunk_list list) const noexcept;
    double disk_usage() const noexcept;
    double disk_usage(
        MONAD_STORAGE_NAMESPACE::DbStorage::chunk_list list) const noexcept;

    chunk_offset_t do_upsert(
        chunk_offset_t root_offset, StateMachine &, UpdateList &&,
        uint64_t version, bool compaction, bool can_write_to_fast);

    chunk_offset_t copy_trie_to_dest(
        chunk_offset_t src_root, NibblesView src_prefix, uint64_t dest_version,
        NibblesView dest_prefix);

    void switch_writer_to_new_chunk(
        chunk_offset_t &node_writer_offset, bool from_fast_list) noexcept;

    BlockingSPSC<std::function<void()>> async_queue_{msync_queue_capacity};

    void async_preload_helper(bool is_fast);

public:
    // int64_t curr_upsert_auto_expire_version{0};
    compact_virtual_chunk_offset_t compact_offset_fast{
        MIN_COMPACT_VIRTUAL_OFFSET};
    compact_virtual_chunk_offset_t compact_offset_slow{
        MIN_COMPACT_VIRTUAL_OFFSET};

    chunk_offset_t node_writer_offset_fast{INVALID_OFFSET};
    chunk_offset_t node_writer_offset_slow{INVALID_OFFSET};

    UpdateAux(
        MONAD_STORAGE_NAMESPACE::DbStorage &,
        std::optional<uint64_t> history_len = {});

    ~UpdateAux();

    // TODO: add db stats

    storage::DbStorage const &db_storage() const noexcept
    {
        return db_storage_;
    }

    bool is_read_only() const noexcept
    {
        return db_storage_.is_read_only() ||
               db_storage_.is_read_only_allow_dirty();
    }

    bool is_on_disk() const noexcept
    {
        return true;
    }

    virtual_chunk_offset_t
    physical_to_virtual(chunk_offset_t offset) const noexcept;

    Node *parse_node(chunk_offset_t offset) const noexcept;

    Node::UniquePtr
    parse_node_weak(chunk_offset_t offset, uint64_t version) const noexcept;

    chunk_offset_t write_node_to_disk(Node const &node, bool to_fast_list);

    bool exists_version(uint64_t version) const noexcept;

    chunk_offset_t get_root_offset_at_version(uint64_t version) const noexcept;

    // set: sync update the unsync metadata, async update the durable metadata
    // get: input should specify durable or unsynced metadata
    void set_latest_finalized_version(uint64_t version) noexcept;
    uint64_t get_latest_finalized_version(bool synced = false) const noexcept;
    void set_latest_verified_version(uint64_t version) noexcept;
    uint64_t get_latest_verified_version(bool synced = false) const noexcept;
    uint64_t db_history_max_version(bool synced = false) const noexcept;
    // The minimum valid version does not differ between synced and unsynced
    // metadata; erased versions are always immediately recorded durably, since
    // the erased disk chunk is not accessible to the user, so should prevent
    // other running process from accessing the version immediately instead of
    // updating it asynchronously.
    uint64_t db_history_min_valid_version() const noexcept;
    void set_latest_voted(
        uint64_t const version, bytes32_t const &block_id) noexcept;
    uint64_t get_latest_voted_version() const noexcept;
    bytes32_t get_latest_voted_block_id() const noexcept;
    // ?? do we need sync vs async?
    uint64_t version_history_length() const noexcept;
    // TODO: set_history_length

    void finalize_transaction(chunk_offset_t root_offset, uint64_t version);

    void do_msync(
        chunk_offset_t fast_start, chunk_offset_t fast_end,
        chunk_offset_t slow_start, chunk_offset_t slow_end);
};

class WriteTransaction
{
    UpdateAux &aux_;

    chunk_offset_t fast_offset_start{INVALID_OFFSET};
    chunk_offset_t slow_offset_start{INVALID_OFFSET};

public:
    explicit WriteTransaction(UpdateAux &aux)
        : aux_(aux)
        , fast_offset_start(aux.node_writer_offset_fast)
        , slow_offset_start(aux.node_writer_offset_slow)
    {
        aux_.write_mutex_.lock();
        MONAD_ASSERT(!aux_.is_read_only());
    }

    ~WriteTransaction()
    {
        aux_.write_mutex_.unlock();
    }

    // upsert on top of the given root into a specific version
    chunk_offset_t do_upsert(
        chunk_offset_t const root_offset, StateMachine &sm,
        UpdateList &&updates, uint64_t const version, bool const compaction,
        bool const can_write_to_fast)
    {
        return aux_.do_upsert(
            root_offset,
            sm,
            std::move(updates),
            version,
            compaction,
            can_write_to_fast);
    };

    chunk_offset_t copy_trie(
        chunk_offset_t const src_root, NibblesView const src_prefix,
        uint64_t const dest_version, NibblesView const dest_prefix)
    {
        auto const offset = aux_.copy_trie_to_dest(
            src_root, src_prefix, dest_version, dest_prefix);
        MONAD_ASSERT(
            aux_.parse_node(offset)->value_len == 2 * sizeof(uint32_t));
        return offset;
    }

    void finish(
        chunk_offset_t root_offset, uint64_t version,
        bool const blocking = false)
    {
        aux_.finalize_transaction(root_offset, version);

        using namespace MONAD_STORAGE_NAMESPACE;

        auto const *m = aux_.db_storage_.db_metadata();
        auto sync_ = [&](chunk_offset_t const start, chunk_offset_t const end) {
            if (start != end) {
                auto const *si = m->at(start.id);
                auto const *ei = m->at(end.id);
                MONAD_ASSERT(si->insertion_count() <= ei->insertion_count());
                MONAD_DEBUG_ASSERT(si->in_fast_list == ei->in_fast_list);
                MONAD_DEBUG_ASSERT(si->in_slow_list == ei->in_slow_list);

                for (auto const *ci = si;; ci = ci->next(m)) {
                    auto const idx = ci->index(m);
                    uint32_t offset = 0;
                    if (ci == si) {
                        offset = round_down_align<CPU_PAGE_BITS>(
                            (uint32_t)start.offset);
                    }
                    else if (ci == ei) {
                        offset =
                            round_up_align<CPU_PAGE_BITS>((uint32_t)end.offset);
                    }
                    auto *const data = aux_.db_storage_.get_data({idx, offset});
                    auto const len = DbStorage::chunk_capacity - offset;
                    // keep in memory
                    MONAD_ASSERT(-1 != madvise(data, len, MADV_WILLNEED));
                    if (ci == ei) {
                        break;
                    }
                }
            }
        };

        sync_(fast_offset_start, aux_.node_writer_offset_fast);
        sync_(slow_offset_start, aux_.node_writer_offset_slow);

        // push job to the worker queue
        aux_.async_queue_.push_blocking(
            [aux = &aux_,
             fast_start = fast_offset_start,
             fast_end = aux_.node_writer_offset_fast,
             slow_start = slow_offset_start,
             slow_end = aux_.node_writer_offset_slow]() {
                aux->do_msync(fast_start, fast_end, slow_start, slow_end);
            });

        if (blocking) {
            aux_.async_queue_.wait_until_drained();
            MONAD_DEBUG_ASSERT(
                aux_.get_root_offset_at_version(version) == root_offset);
        }
    }
};

chunk_offset_t
upsert(UpdateAux &, StateMachine &, chunk_offset_t old_offset, UpdateList &&);

//////////////////////////////////////////////////////////////////////////////
// find

enum class find_result : uint8_t
{
    unknown,
    success,
    version_no_longer_exist,
    root_node_is_null_failure,
    key_mismatch_failure,
    branch_not_exist_failure,
    key_ends_earlier_than_node_failure,
    need_to_continue_in_io_thread
};
template <class T>
using find_result_type = std::pair<T, find_result>;

using find_cursor_result_type = find_result_type<NodeCursor>;

find_cursor_result_type
find(UpdateAux const &, NodeCursor, NibblesView key, uint64_t version = 0);

find_result_type<OwningNodeCursor> find_weak(
    UpdateAux const &aux, OwningNodeCursor cursor, NibblesView key,
    uint64_t version);

std::pair<compact_virtual_chunk_offset_t, compact_virtual_chunk_offset_t>
calc_min_offsets(
    Node const &node,
    virtual_chunk_offset_t node_virtual_offset = INVALID_VIRTUAL_OFFSET);

MONAD_MPT2_NAMESPACE_END
