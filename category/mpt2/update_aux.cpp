#include <category/core/unaligned.hpp>
#include <category/mpt2/async_worker.hpp>
#include <category/mpt2/child_data.hpp>
#include <category/mpt2/config.hpp>
#include <category/mpt2/node.hpp>
#include <category/mpt2/state_machine.hpp>
#include <category/mpt2/trie.hpp>
#include <category/mpt2/update.hpp>
#include <category/storage/db_storage.hpp>
#include <category/storage/util.hpp>

#include <cmath>
#include <cstdint>
#include <mutex>
#include <optional>
#include <stack>
#include <vector>

MONAD_MPT2_NAMESPACE_BEGIN

using namespace MONAD_STORAGE_NAMESPACE;

namespace
{

    // Only preload when writer is close to the end of already-preloaded area
    constexpr unsigned PRELOAD_SIZE_BITS = 16; // 64 KiB
    constexpr size_t PRELOAD_SIZE = 1UL << PRELOAD_SIZE_BITS;

    uint32_t divide_and_round(uint32_t const dividend, uint64_t const divisor)
    {
        double const result = dividend / static_cast<double>(divisor);
        auto const result_floor = static_cast<uint32_t>(std::floor(result));
        double const fractional = result - result_floor;
        auto const r = static_cast<double>(rand()) / RAND_MAX;
        return result_floor + static_cast<uint32_t>(r <= fractional);
    }

    std::pair<compact_virtual_chunk_offset_t, compact_virtual_chunk_offset_t>
    deserialize_compaction_offsets(byte_string_view const bytes)
    {
        MONAD_ASSERT(bytes.size() == 2 * sizeof(uint32_t));
        compact_virtual_chunk_offset_t fast_offset{
            INVALID_COMPACT_VIRTUAL_OFFSET};
        compact_virtual_chunk_offset_t slow_offset{
            INVALID_COMPACT_VIRTUAL_OFFSET};
        fast_offset.set_value(unaligned_load<uint32_t>(bytes.data()));
        slow_offset.set_value(
            unaligned_load<uint32_t>(bytes.data() + sizeof(uint32_t)));
        return {fast_offset, slow_offset};
    }

    Node::UniquePtr create_node_add_new_branch(
        UpdateAux &aux, Node *const node, unsigned char const new_branch,
        Node::UniquePtr new_child, uint64_t const new_version,
        std::optional<byte_string_view> opt_value)
    {
        uint16_t const mask =
            static_cast<uint16_t>(node->mask | (1u << new_branch));
        std::vector<ChildData> children(
            static_cast<unsigned>(std::popcount(mask)));
        for (unsigned i = 0, j = 0, old_j = 0, bit = 1; i < 16;
             ++i, bit <<= 1) {
            if (i == new_branch) {
                auto &child = children[j];
                child.branch = (unsigned char)i;
                child.subtrie_min_version = calc_min_version(*new_child);
                child.offset = aux.write_node_to_disk(*new_child, true);
                std::tie(child.min_offset_fast, child.min_offset_slow) =
                    calc_min_offsets(
                        *new_child, aux.physical_to_virtual(child.offset));
                ++j;
            }
            else if (mask & bit) { // existing branches
                auto &child = children[j];
                child.branch = (unsigned char)i;
                child.subtrie_min_version = node->subtrie_min_version(old_j);
                child.min_offset_fast = node->min_offset_fast(old_j);
                child.min_offset_slow = node->min_offset_slow(old_j);
                child.offset = node->fnext(old_j);
                ++old_j;
                ++j;
            }
        }
        return make_node(
            mask,
            children,
            node->path_nibble_view(),
            opt_value,
            0,
            static_cast<int64_t>(new_version));
    }

    Node::UniquePtr create_node_with_two_children(
        UpdateAux &aux, NibblesView const path, unsigned char const branch0,
        Node::UniquePtr child0, unsigned char const branch1,
        Node::UniquePtr child1, uint64_t const new_version,
        std::optional<byte_string_view> opt_value)
    {
        // mismatch: split node's path: turn node to a branch node with two
        // children
        uint16_t const mask =
            static_cast<uint16_t>((1u << branch0) | (1u << branch1));
        bool const zero_comes_first = branch0 < branch1;
        ChildData children[2];
        {
            auto &child = children[!zero_comes_first];
            child.subtrie_min_version = calc_min_version(*child0);
            child.branch = branch0;
            child.offset = aux.write_node_to_disk(*child0, true);
            std::tie(child.min_offset_fast, child.min_offset_slow) =
                calc_min_offsets(*child0);
        }
        {
            auto &child = children[zero_comes_first];
            child.subtrie_min_version = calc_min_version(*child1);
            child.branch = branch1;
            child.offset = aux.write_node_to_disk(*child1, true);
            std::tie(child.min_offset_fast, child.min_offset_slow) =
                calc_min_offsets(*child1);
        }
        return make_node(
            mask,
            std::span(children),
            path,
            opt_value,
            0,
            static_cast<int64_t>(new_version));
    }
}

UpdateAux::UpdateAux(
    DbStorage &db_storage, std::optional<uint64_t> const history_len)
    : enable_dynamic_history_length_(!history_len.has_value())
    , db_storage_(db_storage)
{
    if (history_len.has_value() && !is_read_only()) {
        // reset history length
        if (history_len < version_history_length() &&
            history_len <= db_history_max_version(true)) {
            // we invalidate earlier blocks that fall outside of the
            // history window when shortening history length
            erase_versions_up_to_and_including(
                db_history_max_version(true) - *history_len);
        }
        db_storage_.set_history_length(*history_len, true);
        db_storage_.set_history_length(*history_len, false);
    }

    // init node writers
    node_writer_offset_fast =
        db_storage_.db_metadata()->db_offsets.start_of_wip_offset_fast;
    node_writer_offset_slow =
        db_storage_.db_metadata()->db_offsets.start_of_wip_offset_slow;
    // populate_read for current and last chunk
    // fast list only
    chunk_offset_t populate_read_offset = node_writer_offset_fast;
    if (populate_read_offset.offset != 0) {
        populate_read_offset.offset =
            round_down_align<CPU_PAGE_BITS>(populate_read_offset.offset);
        auto *const addr = db_storage_.get_data(populate_read_offset);
        madvise(
            addr,
            DbStorage::chunk_capacity - populate_read_offset.offset,
            MADV_POPULATE_READ);
        madvise(
            addr,
            DbStorage::chunk_capacity - populate_read_offset.offset,
            MADV_WILLNEED);
    }
    // populate recently written chunks
    auto const id = (uint32_t)populate_read_offset.id;
    unsigned nloaded = 1;
    uint32_t prev_chunk_id =
        (uint32_t)db_storage_.db_metadata()->at(id)->prev_chunk_id;
    while (prev_chunk_id !=
           detail::db_metadata_t::chunk_info_t::INVALID_CHUNK_ID) {
        populate_read_offset = {prev_chunk_id, 0};
        auto *const addr = db_storage_.get_data(populate_read_offset);
        madvise(addr, DbStorage::chunk_capacity, MADV_POPULATE_READ);
        madvise(addr, DbStorage::chunk_capacity, MADV_WILLNEED);
        ++nloaded;
        if (nloaded > 5) {
            break;
        }
        prev_chunk_id = (uint32_t)db_storage_.db_metadata()
                            ->at(prev_chunk_id)
                            ->prev_chunk_id;
    }

    // preload for write
    preload_offset_fast_ = {
        (uint32_t)node_writer_offset_fast.id,
        round_down_align<PRELOAD_SIZE_BITS>(node_writer_offset_fast.offset)};
    preload_offset_slow_ = {
        (uint32_t)node_writer_offset_slow.id,
        round_down_align<PRELOAD_SIZE_BITS>(node_writer_offset_slow.offset)};
    preload_helper(true);
    preload_helper(false);

    // init last block end offsets
    last_block_end_offset_fast_ = compact_virtual_chunk_offset_t{
        physical_to_virtual(node_writer_offset_fast)};
    last_block_end_offset_slow_ = compact_virtual_chunk_offset_t{
        physical_to_virtual(node_writer_offset_slow)};

    if (!is_read_only()) {
        async_worker_ = std::make_unique<AsyncWorker>(*this);
    }
}

void UpdateAux::preload_helper(bool const is_fast)
{
    auto const writer_offset =
        is_fast ? node_writer_offset_fast : node_writer_offset_slow;
    auto &preloaded_offset =
        is_fast ? preload_offset_fast_ : preload_offset_slow_;
    if (preloaded_offset.id != writer_offset.id) {
        MONAD_ASSERT(
            db_storage_.db_metadata()->free_list.end == preloaded_offset.id);
        if (writer_offset.offset + PRELOAD_SIZE >= DbStorage::chunk_capacity) {
            MONAD_ASSERT(
                preloaded_offset.offset == 0 ||
                preloaded_offset.offset == PRELOAD_SIZE);
            auto *const addr = db_storage_.get_data(preloaded_offset);
            MONAD_ASSERT(madvise(addr, PRELOAD_SIZE, MADV_WILLNEED) != -1);
            preloaded_offset.offset = PRELOAD_SIZE;
        }
        return;
    }
    if (writer_offset.offset + PRELOAD_SIZE >= preloaded_offset.offset) {
        auto *const addr = db_storage_.get_data(preloaded_offset);
        MONAD_ASSERT(madvise(addr, PRELOAD_SIZE, MADV_WILLNEED) != -1);
        auto const end_offset = preloaded_offset.offset + PRELOAD_SIZE;
        if (end_offset >= DbStorage::chunk_capacity) {
            // switch to free chunk
            preloaded_offset = {db_storage_.db_metadata()->free_list.end, 0};
        }
        else {
            preloaded_offset.offset = end_offset;
        }
    }
}

UpdateAux::~UpdateAux()
{
    // must destruct async worker before db_storage_ to avoid use after free
    async_worker_.reset();
}

void UpdateAux::clear_root_offsets_up_to_and_including(uint64_t const version)
{
    uint64_t v = db_storage_.db_metadata()->version_lower_bound_;
    for (; v <= version; ++v) {
        db_storage_.update_root_offset(v, INVALID_OFFSET);
    }
    MONAD_ASSERT(db_storage_.db_metadata()->version_lower_bound_ > version);
}

void UpdateAux::erase_versions_up_to_and_including(uint64_t const version)
{
    clear_root_offsets_up_to_and_including(version);
    release_unreferenced_chunks();
}

void UpdateAux::release_unreferenced_chunks()
{
    auto const min_valid_version = db_history_min_valid_version();
    Node const *const min_valid_root =
        parse_node(get_root_offset_at_version(min_valid_version));
    MONAD_ASSERT(min_valid_root != nullptr);
    auto const [min_offset_fast, min_offset_slow] =
        deserialize_compaction_offsets(min_valid_root->value());
    MONAD_ASSERT(
        min_offset_fast != INVALID_COMPACT_VIRTUAL_OFFSET &&
        min_offset_slow != INVALID_COMPACT_VIRTUAL_OFFSET);
    chunks_to_remove_before_count_fast_ = min_offset_fast.get_count();
    chunks_to_remove_before_count_slow_ = min_offset_slow.get_count();
    MONAD_ASSERT(
        db_storage_.db_metadata()->version_lower_bound_ >= min_valid_version);

    free_compacted_chunks();
}

void UpdateAux::free_compacted_chunks()
{
    auto const *const m = db_storage_.db_metadata();
    auto free_chunks_from_ci_till_count =
        [&](detail::db_metadata_t::chunk_info_t const *ci,
            uint32_t const count_before) {
            uint32_t idx = ci->index(m);
            uint32_t count = (uint32_t)m->at(idx)->insertion_count();
            for (; count < count_before && ci != nullptr;
                 idx = ci->index(m),
                 count = (uint32_t)m->at(idx)->insertion_count()) {
                ci = ci->next(m); // must be in this order
                db_storage_.remove(idx);
                db_storage_.destroy_contents(idx);
                db_storage_.append(DbStorage::chunk_list::free, idx);
            }
        };
    MONAD_ASSERT(
        chunks_to_remove_before_count_fast_ <=
        m->fast_list_end()->insertion_count());
    MONAD_ASSERT(
        chunks_to_remove_before_count_slow_ <=
        m->slow_list_end()->insertion_count());
    free_chunks_from_ci_till_count(
        m->fast_list_begin(), chunks_to_remove_before_count_fast_);
    free_chunks_from_ci_till_count(
        m->slow_list_begin(), chunks_to_remove_before_count_slow_);
}

// Returns a virtual offset on successful translation; returns
// INVALID_VIRTUAL_OFFSET if the input offset is invalid or the offset refers to
// a chunk in the free list.
virtual_chunk_offset_t
UpdateAux::physical_to_virtual(chunk_offset_t const offset) const noexcept
{
    if (offset == INVALID_OFFSET) {
        return INVALID_VIRTUAL_OFFSET;
    }
    auto const chunk_info = db_storage_.db_metadata()->atomic_load_chunk_info(
        offset.id, std::memory_order_acquire);
    if (chunk_info.in_fast_list || chunk_info.in_slow_list) {
        return virtual_chunk_offset_t{
            uint32_t(chunk_info.insertion_count()),
            offset.offset,
            chunk_info.in_fast_list,
            0};
    }
    // return invalid virtual offset when translate an offset from free list
    return INVALID_VIRTUAL_OFFSET;
}

Node *UpdateAux::parse_node(chunk_offset_t const offset) const noexcept
{
    if (offset == INVALID_OFFSET) {
        return nullptr;
    }
    return ::monad::mpt2::parse_node(db_storage_.get_data(offset));
}

Node::UniquePtr UpdateAux::parse_node_weak(
    chunk_offset_t const offset, uint64_t const version) const noexcept
{
    if (offset == INVALID_OFFSET) {
        return nullptr;
    }
    auto p = db_storage_.get_data(offset);

    // Load size, check version afterward which guarantees that node
    // data can be safely accessed.
    auto size = unaligned_load<uint32_t>(p);
    if (!exists_version(version)) {
        return nullptr;
    }

    p += Node::disk_size_bytes;

    auto node = Node::make(size);
    memcpy(static_cast<void *>(node.get()), p, size);

    // Check version again after copying, if version still exists copied data is
    // valid.
    if (!exists_version(version)) {
        return nullptr;
    }

    return node;
}

// TODO: try buffered aligned writes
chunk_offset_t
UpdateAux::write_node_to_disk(Node const &node, bool const to_fast_list)
{
    // Append node to the specified chunk list
    // If node size fit the remaining bytes, append to the current chunk,
    // otherwise start a new chunk and write there. Node max size guarantees it
    // can always fit in a chunk of default capacity
    auto &node_writer_offset =
        to_fast_list ? node_writer_offset_fast : node_writer_offset_slow;
    auto const chunk_bytes_written =
        db_storage_.chunk_bytes_used(node_writer_offset.id);
    MONAD_ASSERT_PRINTF(
        chunk_bytes_written == node_writer_offset.offset,
        "where we are appending %u is not where we are supposed to be "
        "appending %u, check if there are multiple writers writes to db "
        "concurrently. Chunk id is %u",
        node_writer_offset.offset,
        chunk_bytes_written,
        node_writer_offset.id);

    MONAD_ASSERT(DbStorage::chunk_capacity > node_writer_offset.offset);
    unsigned const chunk_remaining_bytes =
        DbStorage::chunk_capacity - node_writer_offset.offset;
    unsigned const bytes_to_append = node.get_disk_size();
    if (bytes_to_append > chunk_remaining_bytes) {
        // we need to switch to a new chunk
        switch_writer_to_new_chunk(node_writer_offset, to_fast_list);
    }
    chunk_offset_t const ret_offset = node_writer_offset;
    serialize_node(db_storage_.get_data(ret_offset), node);
    // verify that the node was serialized correctly
    MONAD_DEBUG_ASSERT(
        node.get_disk_size() == parse_node(ret_offset)->get_disk_size());
    db_storage_.advance_chunk_bytes_used(ret_offset.id, bytes_to_append);
    if (bytes_to_append == chunk_remaining_bytes) {
        switch_writer_to_new_chunk(node_writer_offset, to_fast_list);
    }
    else {
        node_writer_offset = node_writer_offset.add_to_offset(bytes_to_append);
    }
    // preload if current writer offset is close to preload boundary
    preload_helper(to_fast_list);
    return ret_offset;
}

void UpdateAux::switch_writer_to_new_chunk(
    chunk_offset_t &node_writer_offset, bool const from_fast_list) noexcept
{
    // allocate a new chunk from free list to the specified list and update
    // node_writer_offset to the start of the new chunk
    detail::db_metadata_t::chunk_info_t const *ci =
        db_storage_.db_metadata()->free_list_end();
    MONAD_ASSERT_PRINTF(
        ci != nullptr, "disk is full, we are out of free blocks");
    uint32_t const idx = ci->index(db_storage_.db_metadata());
    db_storage_.remove(idx);
    db_storage_.append(
        from_fast_list ? DbStorage::chunk_list::fast
                       : DbStorage::chunk_list::slow,
        idx);
    node_writer_offset.id = idx & 0xfffffU;
    node_writer_offset.offset = 0;
    MONAD_ASSERT(db_storage_.chunk_bytes_used(idx) == 0);
}

void UpdateAux::do_msync(
    chunk_offset_t const fast_start, chunk_offset_t const fast_end,
    chunk_offset_t const slow_start, chunk_offset_t const slow_end)
{
    using namespace MONAD_STORAGE_NAMESPACE;

    auto const *m = db_storage_.db_metadata();
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
                    offset =
                        round_down_align<CPU_PAGE_BITS>((uint32_t)start.offset);
                }
                else if (ci == ei) {
                    offset =
                        round_up_align<CPU_PAGE_BITS>((uint32_t)end.offset);
                }
                MONAD_ASSERT_PRINTF(
                    -1 != ::msync(
                              db_storage_.get_data({idx, offset}),
                              DbStorage::chunk_capacity - offset,
                              MS_SYNC),
                    "msync failed: %s",
                    strerror(errno));
                if (ci == ei) {
                    break;
                }
            }
        }
    };
    sync_(fast_start, fast_end);
    sync_(slow_start, slow_end);
}

// called synchronously
void UpdateAux::finalize_transaction(
    chunk_offset_t const root_offset, uint64_t const version)
{
    // update writer offset trackers
    db_storage_.advance_db_offsets_to(
        node_writer_offset_fast, node_writer_offset_slow);

    // update root offset in ring buffer
    auto const max_version = db_history_max_version();
    auto const history_length = version_history_length();
    if (MONAD_UNLIKELY(max_version == INVALID_VERSION)) {
        db_storage_.fast_forward_next_version(version);
        db_storage_.append_root_offset(root_offset);
        MONAD_ASSERT(db_storage_.db_history_range_lower_bound() == version);
    }
    else if (version <= max_version) {
        MONAD_ASSERT(
            version >= ((max_version >= history_length)
                            ? max_version - history_length + 1
                            : 0));
        auto const prev_lower_bound =
            db_storage_.db_history_range_lower_bound();
        db_storage_.update_root_offset(version, root_offset);
        MONAD_ASSERT(
            db_storage_.db_history_range_lower_bound() ==
            std::min(version, prev_lower_bound));
    }
    else { // append
        // prune versions that are going to fall out of history window
        if (version - db_history_min_valid_version() >=
            history_length) { // exceed history length
            // erase min_valid_version, must happen before appending the new
            // root offset because that offset slot in ring buffer may be
            // overwritten thus invalidated in the append
            erase_versions_up_to_and_including(version - history_length);
            MONAD_ASSERT(
                version - db_history_min_valid_version() < history_length);
        }
        MONAD_ASSERT(version == max_version + 1);
        db_storage_.append_root_offset(root_offset);
    }
}

chunk_offset_t UpdateAux::do_upsert(
    chunk_offset_t const root_offset, StateMachine &sm, UpdateList &&updates,
    uint64_t const version, bool const compaction,
    bool const /*can_write_to_fast*/)
{
    MONAD_ASSERT(!is_read_only());
    // root->value() stores compaction offsets
    if (root_offset != INVALID_OFFSET) {
        std::tie(compact_offset_fast, compact_offset_slow) =
            deserialize_compaction_offsets(parse_node(root_offset)->value());
    }
    if (compaction) {
        // TODO: dynamically adjust history length
        if (!exists_version(version)) {
            // advance_compact_offsets();
        }
    }
    UpdateList root_updates;
    byte_string const compact_offsets_bytes =
        serialize((uint32_t)compact_offset_fast) +
        serialize((uint32_t)compact_offset_slow);
    auto root_update = make_update(
        {}, compact_offsets_bytes, false, std::move(updates), version);
    root_updates.push_front(root_update);
    auto const offset = upsert(*this, sm, root_offset, std::move(root_updates));
    MONAD_ASSERT(parse_node(offset)->value() == compact_offsets_bytes);
    return offset;
}

chunk_offset_t UpdateAux::copy_trie_to_dest(
    chunk_offset_t const src_root_offset, NibblesView const src_prefix,
    uint64_t const dest_version, NibblesView const dest_prefix)
{
    MONAD_ASSERT(!is_read_only());

    MONAD_ASSERT(src_root_offset != INVALID_OFFSET);
    Node *const src_root = parse_node(src_root_offset);
    auto [src_cursor, res] = find(*this, NodeCursor{*src_root}, src_prefix);
    MONAD_ASSERT(res == find_result::success);
    Node &src_node = *src_cursor.node;

    if (!exists_version(dest_version)) {
        auto new_node = make_node(
            src_node,
            dest_prefix.substr(1),
            src_node.opt_value(),
            static_cast<int64_t>(dest_version));
        ChildData child{
            .node = std::move(new_node), .branch = dest_prefix.get(0)};
        child.subtrie_min_version = calc_min_version(*child.node);
        child.offset = write_node_to_disk(*child.node, true);
        std::tie(child.min_offset_fast, child.min_offset_slow) =
            calc_min_offsets(*child.node, physical_to_virtual(child.offset));
        auto root = make_node(
            static_cast<uint16_t>(1u << child.branch),
            {&child, 1},
            {},
            src_root->opt_value(),
            0,
            static_cast<int64_t>(dest_version));
        return write_node_to_disk(*root, true);
    }
    // if dest version exists
    Node *parent = nullptr;
    unsigned char branch = INVALID_BRANCH;
    Node *const root =
        parse_node(db_storage_.get_root_offset_at_version(dest_version));
    Node *node = root;
    Node::UniquePtr new_node{};
    unsigned prefix_index = 0;
    unsigned node_prefix_index = 0;

    using ParentIndexPair = std::pair<Node *, unsigned char>;
    std::vector<ParentIndexPair> vec_pairs;
    vec_pairs.reserve(16);
    std::stack<ParentIndexPair, std::vector<ParentIndexPair>>
        parents_and_indexes{std::move(vec_pairs)};

    // Insert `dest` to trie, create the `dest` node to have the same
    // children as node at `src`. Disconnect src_node's in memory children to
    // avoid double references
    while (prefix_index < dest_prefix.nibble_size()) {
        auto const nibble = dest_prefix.get(prefix_index);
        if (node_prefix_index < node->path_nibbles_len()) {
            // not yet end of path in node
            auto const node_nibble =
                node->path_nibble_view().get(node_prefix_index);
            if (nibble == node_nibble) {
                ++prefix_index;
                ++node_prefix_index;
                continue;
            }
            MONAD_DEBUG_ASSERT(
                prefix_index < std::numeric_limits<unsigned char>::max());
            auto const node_path = node->path_nibble_view();
            // copy children of src_node to under `dest` prefix, move the in
            // memory children to `dest` node
            auto dest_latter_half = make_node(
                src_node,
                dest_prefix.substr(prefix_index + 1),
                src_node.opt_value(),
                src_node.version);
            auto node_latter_half = make_node(
                *node,
                node_path.substr(node_prefix_index + 1),
                node->opt_value(),
                node->version);
            new_node = create_node_with_two_children(
                *this,
                node_path.substr(0, node_prefix_index),
                nibble,
                std::move(dest_latter_half),
                node_nibble,
                std::move(node_latter_half),
                dest_version,
                node == root ? std::make_optional(src_root->value())
                             : std::nullopt);
            break;
        }
        // end of node path
        if (node->mask & (1u << nibble)) {
            auto const index = node->to_child_index(nibble);
            // there is a matched branch, go to next child
            parent = node;
            branch = nibble;
            parents_and_indexes.emplace(std::make_pair(parent, index));
            node = parse_node(node->fnext(index));
            node_prefix_index = 0;
            ++prefix_index;
            continue;
        }
        MONAD_DEBUG_ASSERT(
            prefix_index < std::numeric_limits<unsigned char>::max());
        auto dest_node = make_node(
            src_node,
            dest_prefix.substr(prefix_index + 1),
            src_node.opt_value(),
            src_node.version);
        new_node = create_node_add_new_branch(
            *this,
            node,
            nibble,
            std::move(dest_node),
            dest_version,
            node == root ? std::make_optional(src_root->value())
                         : std::nullopt);
        break;
    }

    if (prefix_index ==
        dest_prefix.nibble_size()) { // replace existing `dest` trie
        MONAD_ASSERT(node_prefix_index == node->path_nibbles_len());
        new_node = make_node(
            src_node,
            node->path_nibble_view(),
            src_node.opt_value(),
            static_cast<int64_t>(dest_version));
    }
    if (node == root) {
        MONAD_ASSERT(parents_and_indexes.empty());
        // write new_node and return the offset
        return write_node_to_disk(*new_node, true);
    }
    else {
        MONAD_ASSERT(parent != nullptr);
        auto const child_index = parent->to_child_index(branch);
        // reset child at `branch` to the new_node
        parent->set_fnext(child_index, write_node_to_disk(*new_node, true));
        parents_and_indexes.emplace(std::make_pair(parent, child_index));
        // serialize nodes of insert path up until root (excludes root)
        while (!parents_and_indexes.empty()) {
            auto const &[p, i] = parents_and_indexes.top();
            auto &node = *parse_node(p->fnext(i));
            p->set_fnext(i, write_node_to_disk(node, true));
            auto const [min_offset_fast, min_offset_slow] =
                calc_min_offsets(node);
            p->set_min_offset_fast(i, min_offset_fast);
            p->set_min_offset_slow(i, min_offset_slow);
            p->set_subtrie_min_version(i, calc_min_version(node));
            parents_and_indexes.pop();
        }
        return write_node_to_disk(*root, true);
    }
}

uint32_t UpdateAux::num_chunks(DbStorage::chunk_list const list) const noexcept
{
    auto const *const m = db_storage_.db_metadata();
    switch (list) {
    case DbStorage::chunk_list::free:
        // Triggers when out of storage
        MONAD_ASSERT(m->free_list_begin() != nullptr);
        MONAD_ASSERT(m->free_list_end() != nullptr);

        return (uint32_t)(m->free_list_end()->insertion_count() -
                          m->free_list_begin()->insertion_count()) +
               1;
    case DbStorage::chunk_list::fast:
        // Triggers when out of storage
        MONAD_ASSERT(m->fast_list_begin() != nullptr);
        MONAD_ASSERT(m->fast_list_end() != nullptr);

        return (uint32_t)(m->fast_list_end()->insertion_count() -
                          m->fast_list_begin()->insertion_count()) +
               1;
    case DbStorage::chunk_list::slow:
        // Triggers when out of storage
        MONAD_ASSERT(m->slow_list_begin() != nullptr);
        MONAD_ASSERT(m->slow_list_end() != nullptr);

        return (uint32_t)(m->slow_list_end()->insertion_count() -
                          m->slow_list_begin()->insertion_count()) +
               1;
    }
    return 0;
}

// TODO: can be more fine-grained
double UpdateAux::disk_usage() const noexcept
{
    return 1.0 - num_chunks(DbStorage::chunk_list::free) /
                     (double)db_storage_.num_chunks();
}

double UpdateAux::disk_usage(DbStorage::chunk_list const list) const noexcept
{
    return num_chunks(list) / (double)db_storage_.num_chunks();
}

void UpdateAux::advance_compact_offsets()
{
    /* Note on ring based compaction:
    Fast list compaction is steady pace based on disk growth over recent blocks,
    and we assume no large sets of upsert work directly committed to fast list,
    meaning no greater than per block updates, otherwise there could be large
    amount of data compacted in one block.
    Large set of states upsert, like snapshot loading or state sync, should be
    written in slow ring. It is under the assumption that only small set of
    states are updated often, majority is not going to be updated in a while, so
    when block execution starts we don’t need to waste disk bandwidth to copy
    them from fast to slow.

    Compaction offset update algo:
    The fast ring is compacted at a steady pace based on the average disk growth
    observed over recent blocks. We define two disk usage thresholds:
    `usage_limit_start_compact_slow` and `usage_limit`. When disk usage reaches
    `usage_limit_start_compact_slow`, slow ring compaction begins, guided by the
    slow ring garbage collection ratio from the last block. If disk usage
    exceeds `usage_limit`, the system will start shortening the history until
    disk usage is brought back within the threshold.
    */
    constexpr auto fast_usage_limit_start_compaction = 0.1;
    auto const fast_disk_usage = disk_usage(DbStorage::chunk_list::fast);
    if (fast_disk_usage < fast_usage_limit_start_compaction) {
        return;
    }

    MONAD_ASSERT(
        compact_offset_fast != INVALID_COMPACT_VIRTUAL_OFFSET &&
        compact_offset_slow != INVALID_COMPACT_VIRTUAL_OFFSET);
    compact_offset_range_fast_ = MIN_COMPACT_VIRTUAL_OFFSET;

    /* Compact the fast ring based on average disk growth over recent blocks. */
    if (compact_offset_fast < last_block_end_offset_fast_) {
        auto const valid_history_length =
            db_history_max_version() - db_history_min_valid_version() + 1;
        compact_offset_range_fast_.set_value(divide_and_round(
            last_block_end_offset_fast_ - compact_offset_fast,
            valid_history_length));
        compact_offset_fast += compact_offset_range_fast_;
    }
    /* TODO: uncomment this after adding db stats
    constexpr double usage_limit_start_compact_slow = 0.6;
    constexpr double slow_usage_limit_start_compact_slow = 0.2;
    auto const slow_list_usage = disk_usage(DbStorage::chunk_list::slow);
    // we won't start compacting slow list when slow list usage is low or total
    // usage is below 60%
    if (disk_usage() > usage_limit_start_compact_slow &&
        slow_list_usage > slow_usage_limit_start_compact_slow) {
        // Compact slow ring: the offset is based on slow list garbage
        // collection ratio of last block
        compact_offset_range_slow_.set_value(
            (stats.compacted_bytes_in_slow != 0 &&
             compact_offset_range_slow_ != 0)
                ? std::min(
                      (uint32_t)last_block_disk_growth_slow_ + 1,
                      (uint32_t)std::round(
                          double(compact_offset_range_slow_ << 16) /
                          stats.compacted_bytes_in_slow))
                : 1);
        compact_offset_slow += compact_offset_range_slow_;
    }
    else {
        compact_offset_range_slow_ = MIN_COMPACT_VIRTUAL_OFFSET;
    }
    */
}

bool UpdateAux::exists_version(uint64_t const version) const noexcept
{
    return db_storage_.get_root_offset_at_version(version) != INVALID_OFFSET;
}

chunk_offset_t
UpdateAux::get_root_offset_at_version(uint64_t const version) const noexcept
{
    return db_storage_.get_root_offset_at_version(version);
}

void UpdateAux::set_latest_finalized_version(uint64_t version) noexcept
{
    db_storage_.set_latest_finalized_version(version, false); // unsynced
    // asynchronous update durable metadata
    async_queue_.push_blocking([this, version]() {
        db_storage_.set_latest_finalized_version(version, true); // durable
    });
}

uint64_t
UpdateAux::get_latest_finalized_version(bool const synced) const noexcept
{
    return db_storage_.get_latest_finalized_version(synced);
}

void UpdateAux::set_latest_verified_version(uint64_t version) noexcept
{
    db_storage_.set_latest_verified_version(version, false);
    async_queue_.push_blocking([this, version]() {
        db_storage_.set_latest_verified_version(version, true); // durable
    });
}

uint64_t
UpdateAux::get_latest_verified_version(bool const synced) const noexcept
{
    return db_storage_.get_latest_verified_version(synced);
}

uint64_t UpdateAux::db_history_max_version(bool const synced) const noexcept
{
    return db_storage_.db_history_max_version(synced);
}

uint64_t UpdateAux::db_history_min_valid_version() const noexcept
{
    return db_storage_.db_history_min_valid_version();
}

void UpdateAux::set_latest_voted(
    uint64_t const version, bytes32_t const &block_id) noexcept
{
    db_storage_.set_latest_voted(version, block_id);
}

uint64_t UpdateAux::get_latest_voted_version() const noexcept
{
    return db_storage_.get_latest_voted_version();
}

bytes32_t UpdateAux::get_latest_voted_block_id() const noexcept
{
    return db_storage_.get_latest_voted_block_id();
}

uint64_t UpdateAux::version_history_length() const noexcept
{
    return db_storage_.version_history_length();
}

MONAD_MPT2_NAMESPACE_END
