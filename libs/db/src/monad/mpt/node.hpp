#pragma once

#include <monad/async/storage_pool.hpp>
#include <monad/core/byte_string.hpp>
#include <monad/core/endian.hpp> // NOLINT
#include <monad/core/keccak.h>
#include <monad/core/math.hpp>
#include <monad/mem/allocators.hpp>
#include <monad/mpt/detail/unsigned_20.hpp>
#include <monad/mpt/util.hpp>
#include <monad/rlp/encode.hpp>

#include <cstdint>
#include <optional>
#include <span>

MONAD_MPT_NAMESPACE_BEGIN

// #define MONAD_MPT_READ_STATS 1
#define MONAD_MPT_NODE_COUNTER 1

struct Compute;
class NibblesView;

struct node_disk_pages_spare_15
{
    struct spare_type_15
    {
        uint16_t count : 10;
        uint16_t shift : 5;
        uint16_t reserved0_ : 1;
    };

    union spare_dual
    {
        spare_type_15 spare;
        uint16_t value;

        constexpr spare_dual(uint16_t v)
            : value(v)
        {
        }
    } value;

    static constexpr unsigned count_bits = 10;
    static constexpr size_t max_count = (1UL << 10) - 1;
    static constexpr uint16_t max_shift = (1U << 5) - 1;

    explicit constexpr node_disk_pages_spare_15(chunk_offset_t const offset)
        : value(static_cast<uint16_t>(offset.spare))
    {
    }

    explicit constexpr node_disk_pages_spare_15(unsigned const pages)
        : value{0}
    {
        unsigned const exp = pages >> count_bits;
        auto const shift = static_cast<uint16_t>(
            std::numeric_limits<decltype(exp)>::digits - std::countl_zero(exp));
        value.spare.count =
            ((pages >> shift) + bool(pages & ((1u << shift) - 1))) & max_count;
        value.spare.shift = shift & max_shift;
    }

    constexpr unsigned to_pages() const noexcept
    {
        return static_cast<unsigned>(value.spare.count) << value.spare.shift;
    }

    // conversion to uint16_t
    explicit constexpr operator uint16_t() const noexcept
    {
        return value.value;
    }
};

/* A note on generic trie

In Ethereum merkle patricia trie:
- Node is a extension if path len > 0, it only has one child, a branch node
- Node is a branch if mask > 0 && path len == 0, branch can have leaf value
- Node is a leaf node if it has no child

In generic trie, a node can have dual identity of ext and branch node, and
branch node can have vt (value) and be a leaf node at the same time. Branch node
with leaf data can have 1 child or more.
- A node with non-empty path is either an ext node or a leaf node
- A leaf node has has_value = true, however not necessarily value_len > 0
- A branch node with leaf can mean it's the end of an internal trie, making
itself also the root of the trie underneath, for example a leaf of an
account trie, where the account has an underlying storage trie. It can also
simply mean it's a branch node inside an internal trie, for example a branch
node with value in a receipt trie (var key length). Such branch node with leaf
will cache an intemediate hash inline.

Similar like a merkle patricia trie, each node's data is computed from its child
nodes data. Triedb is divided into different sections, indexed by unique
prefixes (i.e. sections for accounts, storages, receipts, etc.), node data is
defined differently in each section, and we leave the actual computation to the
`class Compute` to handle it.
We store node data to its parent's storage to avoid an extra read of child node
to retrieve child data.
*/

class LruList;

class Node
{
    struct prevent_public_construction_tag
    {
    };

public:
#ifdef MONAD_MPT_NODE_COUNTER
    static uint64_t num_nodes;
    static uint64_t bytes_allocated;
#endif
    static constexpr size_t max_size_for_boost_pools = 66544;
    static constexpr size_t max_number_of_children = 16;
    static constexpr uint8_t max_data_len = (1U << 6) - 1;
    static constexpr size_t max_disk_size =
        256 * 1024 * 1024; // 256mb, same as storage chunk size
    static constexpr unsigned disk_size_bytes = sizeof(uint32_t);
    static constexpr size_t max_size =
        max_disk_size + max_number_of_children * KECCAK256_SIZE;
    using BytesAllocator = allocators::malloc_free_allocator<std::byte>;

    static allocators::detail::type_raw_alloc_pair<
        std::allocator<Node>, BytesAllocator>
    pool();
    static size_t get_deallocate_count(Node *);
    using UniquePtr = std::unique_ptr<
        Node, allocators::unique_ptr_aliasing_allocator_deleter<
                  std::allocator<Node>, BytesAllocator, &Node::pool,
                  &Node::get_deallocate_count>>;

public:
    Node *prev{nullptr};
    Node *after{nullptr};
    LruList *list{nullptr};
    void *addr_to_reset{nullptr};
    bool temp_not_deallocate{false};
    unsigned char pad[7];

    /* Here starts on disk storage*/
    /* 16-bit mask for children */
    uint16_t mask{0};

    struct bitpacked_storage_t
    {
        /* does node have a value, value_len is not necessarily positive */
        bool has_value : 1 {false};
        bool path_nibble_index_start : 1 {false};
        /* size (in byte) of intermediate cache for branch hash */
        uint8_t data_len : 6 {0};
    } bitpacked{0};

    static_assert(sizeof(bitpacked) == 1);

    uint8_t path_nibble_index_end{0};
    /* size (in byte) of user-passed leaf data */
    uint32_t value_len{0};
    int64_t version{0};

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
    unsigned char fnext_data[0];
#pragma GCC diagnostic pop

    /* Member funcs and data layout that exceeds node struct size is organized
    as below:
    * `number_of_children()` is the number of children the node has and equals
    std::popcount(mask)
    * `fnext` array: size-n array storing children's on-disk offsets
    * `data_offset` array: size-n array each stores a specific child data's
    starting offset
    * `path`: a few bytes for relative path, size depends on
    path_nibble_index_start, path_nibble_index_end
    * `value`: user-passed leaf data of value_len bytes
    * `data`: intermediate hash cached for a implicit branch node, which
    exists in leaf nodes that have child.
    * `data_arr`: concatenated data bytes for all children
    * `next` array: size-n array storing children's mem pointers
    */

    // TODO:
    // 1. get rid of data_off_arr, and store child data bytes as rlp encoded
    // bytes
    // 2. children data_arr can be stored out of line or we reuse nodes instead
    // of allocating new ones when node size remains the same, as most of the
    // time only one child out of multiple is updated, and other children's data
    // remains unchanged, storing inline requires copying them all, storing data
    // out of line allows us to transfer ownership of data array from old node
    // to new one, also help to keep allocated size as small as possible.

    template <class... Args>
    static UniquePtr make(size_t bytes, Args &&...args)
    {
        MONAD_DEBUG_ASSERT(bytes <= Node::max_size);
#ifdef MONAD_MPT_NODE_COUNTER
        bytes_allocated += bytes;
        ++num_nodes;
#endif
        return allocators::allocate_aliasing_unique<
            std::allocator<Node>,
            BytesAllocator,
            &pool,
            &get_deallocate_count>(
            bytes,
            prevent_public_construction_tag{},
            std::forward<Args>(args)...);
    }

    Node(prevent_public_construction_tag);
    // or use std::optional<LruList>
    Node(
        prevent_public_construction_tag, LruList *list, uint16_t mask,
        std::optional<byte_string_view> value, size_t data_size,
        NibblesView path, int64_t version);
    Node(Node const &) = delete;
    Node(Node &&) = default;
    ~Node();

    unsigned to_child_index(unsigned branch) const noexcept;

    unsigned number_of_children() const noexcept;

    //! fnext array that stores physical chunk offset of each child
    chunk_offset_t const fnext(unsigned index) const noexcept;
    void set_fnext(unsigned index, chunk_offset_t) noexcept;

    //! fastlist min_offset array
    unsigned char *child_min_offset_fast_data() noexcept;
    unsigned char const *child_min_offset_fast_data() const noexcept;
    compact_virtual_chunk_offset_t min_offset_fast(unsigned index) noexcept;
    void set_min_offset_fast(
        unsigned index, compact_virtual_chunk_offset_t) noexcept;
    //! slowlist min_offset array
    unsigned char *child_min_offset_slow_data() noexcept;
    unsigned char const *child_min_offset_slow_data() const noexcept;
    compact_virtual_chunk_offset_t min_offset_slow(unsigned index) noexcept;
    void set_min_offset_slow(
        unsigned index, compact_virtual_chunk_offset_t) noexcept;

    //! data_offset array
    unsigned char *child_off_data() noexcept;
    unsigned char const *child_off_data() const noexcept;
    uint16_t child_data_offset(unsigned index) const noexcept;

    unsigned child_data_len(unsigned index);
    unsigned child_data_len();

    //! path
    unsigned char *path_data() noexcept;
    unsigned char const *path_data() const noexcept;
    unsigned path_nibbles_len() const noexcept;
    bool has_path() const noexcept;
    unsigned path_bytes() const noexcept;
    NibblesView path_nibble_view() const noexcept;
    unsigned path_start_nibble() const noexcept;

    //! value
    unsigned char *value_data() noexcept;
    unsigned char const *value_data() const noexcept;
    bool has_value() const noexcept;
    byte_string_view value() const noexcept;
    std::optional<byte_string_view> opt_value() const noexcept;

    //! data
    unsigned char *data_data() noexcept;
    unsigned char const *data_data() const noexcept;
    byte_string_view data() const noexcept;

    //! child data
    unsigned char *child_data() noexcept;
    unsigned char const *child_data() const noexcept;
    byte_string_view child_data_view(unsigned index) noexcept;
    unsigned char *child_data(unsigned index) noexcept;
    void set_child_data(unsigned index, byte_string_view data) noexcept;

    //! next pointers
    unsigned char *next_data() noexcept;
    unsigned char const *next_data() const noexcept;
    Node *next(size_t index) noexcept;
    Node const *next(size_t index) const noexcept;
    void set_next(unsigned index, Node *) noexcept;
    UniquePtr next_ptr(unsigned index) noexcept;

    //! node size in memory
    unsigned get_mem_size() const noexcept;
    uint32_t get_disk_size() const noexcept;

    bool is_in_list() const
    {
        return prev != nullptr || after != nullptr;
    }
};

static_assert(std::is_standard_layout_v<Node>, "required by offsetof");
static_assert(sizeof(Node) == 56);
static_assert(alignof(Node) == 8);

#ifdef MONAD_MPT_NODE_COUNTER
inline uint64_t Node::num_nodes = 0;
inline uint64_t Node::bytes_allocated = 0;
#endif

constexpr unsigned node_disk_mem_size_diff(unsigned number_of_children)
{
    return offsetof(Node, mask) +
           (unsigned)sizeof(Node *) * number_of_children -
           Node::disk_size_bytes;
}

constexpr unsigned node_disk_storage_offset()
{
    return offsetof(Node, mask);
}

// ChildData is for temporarily holding a child's info, including child ptr,
// file offset and hash data, in the update recursion.
struct ChildData
{
    Node *ptr{nullptr};
    chunk_offset_t offset{INVALID_OFFSET}; // physical offsets
    unsigned char data[32] = {0};
    compact_virtual_chunk_offset_t min_offset_fast{
        INVALID_COMPACT_VIRTUAL_OFFSET};
    compact_virtual_chunk_offset_t min_offset_slow{
        INVALID_COMPACT_VIRTUAL_OFFSET};

    uint8_t branch{INVALID_BRANCH};
    uint8_t len{0};
    bool cache_node{true};

    bool is_valid() const;
    void erase();
    void finalize(Node &, Compute &, bool cache);
    void copy_old_child(Node *old, unsigned i);
};

static_assert(sizeof(ChildData) == 64);
static_assert(alignof(ChildData) == 8);

constexpr size_t calculate_node_size(
    size_t const number_of_children, size_t const total_child_data_size,
    size_t const value_size, size_t const path_size,
    size_t const data_size) noexcept
{
    MONAD_DEBUG_ASSERT(number_of_children || total_child_data_size == 0);
    return sizeof(Node) +
           (sizeof(uint16_t) // child data offset
            + sizeof(compact_virtual_chunk_offset_t) * 2 // min truncated offset
            + sizeof(chunk_offset_t) + sizeof(Node *)) *
               number_of_children +
           total_child_data_size + value_size + path_size + data_size;
}

Node::UniquePtr make_node(
    Node &from, NibblesView path, std::optional<byte_string_view> value,
    int64_t version, bool cached_by_state_machine = true);

Node::UniquePtr make_node(
    uint16_t mask, std::span<ChildData>, NibblesView path,
    std::optional<byte_string_view> value, size_t data_size, int64_t version,
    LruList *lru_list = nullptr, bool cached_by_state_machine = true);

Node::UniquePtr make_node(
    uint16_t mask, std::span<ChildData>, NibblesView path,
    std::optional<byte_string_view> value, byte_string_view data,
    int64_t version, LruList *lru_list = nullptr,
    bool cached_by_state_machine = true);

// create node: either branch/extension, with or without leaf
Node *create_node_with_children(
    Compute &, uint16_t mask, std::span<ChildData> children, NibblesView path,
    std::optional<byte_string_view> value, int64_t version,
    LruList *lru_list = nullptr, bool cached_by_state_machine = true);

void serialize_node_to_buffer(
    unsigned char *write_pos, unsigned bytes_to_write, Node const &,
    uint32_t disk_size, unsigned offset = 0);

Node::UniquePtr
deserialize_node_from_buffer(unsigned char const *read_pos, size_t max_bytes);

//! input argument is node's physical offset
//! chunk_offset_t spare bits store the num page to read
Node *read_node_blocking(
    MONAD_ASYNC_NAMESPACE::storage_pool &, chunk_offset_t node_offset);

class LruList
{
    size_t max_size_{1000000};
    size_t size_{0};
    Node::UniquePtr head_{};
    Node::UniquePtr tail_{};

    void move_to_front(Node *node)
    {
        MONAD_DEBUG_ASSERT(node->is_in_list());
        unlink(node);
        push_front(node);
    }

    void push_front(Node *node)
    {
        Node *const head = head_->after;
        node->prev = head_.get();
        node->after = head;
        head->prev = node;
        head_->after = node;
        ++size_;
    }

public:
    static constexpr uintptr_t INVALID_RESET_ADDR = 0xffffffffffffffff;

    LruList(size_t const max_size)
        : max_size_{max_size}
        , head_{make_node(0, {}, {}, std::nullopt, {}, 0)}
        , tail_{make_node(0, {}, {}, std::nullopt, {}, 0)}
    {
        head_->after = tail_.get();
        tail_->prev = head_.get();
    }

    ~LruList()
    {
        // trie nodes should all be freed before destructing LRU list.
        MONAD_ASSERT(size_ == 0);
        head_->after = nullptr;
        tail_->prev = nullptr;
    }

    void evict()
    {
        MONAD_DEBUG_ASSERT(size_ == max_size_);
        Node *const target = tail_->prev;
        remove(target);
        if (!target->temp_not_deallocate) {
            Node::UniquePtr{target}.reset();
        }
    }

    void remove(Node *const target)
    {
        MONAD_DEBUG_ASSERT(target != head_.get());
        if (target->addr_to_reset) {
            MONAD_DEBUG_ASSERT(
                (uintptr_t)target->addr_to_reset != INVALID_RESET_ADDR);
            memset(target->addr_to_reset, 0, sizeof(Node *));
            target->addr_to_reset = nullptr;
        }
        unlink(target);
    }

    void unlink(Node *node)
    {
        MONAD_ASSERT(size_ > 0);
        Node *const prev = node->prev;
        Node *const next = node->after;
        prev->after = next;
        next->prev = prev;
        node->prev = nullptr;
        node->after = nullptr;
        --size_;
    }

    // call update() everytime we access an in memory node or create a trie node
    void update(Node *node)
    {
        if (node->is_in_list()) {
            move_to_front(node);
            return;
        }
        if (size_ >= max_size_) {
            evict();
        }
        push_front(node);
    }
};

MONAD_MPT_NAMESPACE_END
