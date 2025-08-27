#include <category/execution/ethereum/core/fmt/block_fmt.hpp>
#include <category/execution/ethereum/core/fmt/receipt_fmt.hpp>
#include <category/execution/ethereum/core/fmt/transaction_fmt.hpp>
#include <category/execution/ethereum/core/rlp/block_rlp.hpp>
#include <category/execution/ethereum/core/rlp/int_rlp.hpp>
#include <category/execution/ethereum/core/rlp/receipt_rlp.hpp>
#include <category/execution/ethereum/db/util.hpp>
#include <category/execution/ethereum/rlp/encode2.hpp>
#include <category/execution/ethereum/trace/call_frame.hpp>
#include <category/execution/ethereum/trace/rlp/call_frame_rlp.hpp>
#include <category/mpt/db.hpp>
#include <category/mpt/ondisk_db_config.hpp>
#include <category/mpt/traverse_util.hpp>
#include <category/mpt/util.hpp>

#include <CLI/CLI.hpp>
#include <quill/Quill.h>
#include <quill/bundled/fmt/core.h>
#include <quill/bundled/fmt/format.h>

#include <cstdint>
#include <filesystem>
#include <optional>
#include <vector>

using namespace monad;
using namespace monad::mpt;

std::vector<CallFrame> read_call_frame(
    mpt::Db &db, uint64_t const block_number, uint64_t const txn_idx)
{
    using namespace mpt;

    using KeyedChunk = std::pair<Nibbles, byte_string>;

    Nibbles const min = mpt::concat(
        FINALIZED_NIBBLE,
        CALL_FRAME_NIBBLE,
        NibblesView{serialize_as_big_endian<sizeof(uint32_t)>(txn_idx)});
    Nibbles const max = mpt::concat(
        FINALIZED_NIBBLE,
        CALL_FRAME_NIBBLE,
        NibblesView{serialize_as_big_endian<sizeof(uint32_t)>(txn_idx + 1)});

    std::vector<KeyedChunk> chunks;
    RangedGetMachine machine{
        min,
        max,
        [&chunks](NibblesView const path, byte_string_view const value) {
            chunks.emplace_back(path, value);
        }};
    db.traverse(db.root(), machine, block_number);
    MONAD_ASSERT(!chunks.empty());

    std::sort(
        chunks.begin(),
        chunks.end(),
        [](KeyedChunk const &c, KeyedChunk const &c2) {
            return c.first < NibblesView{c2.first};
        });

    byte_string const call_frames_encoded = std::accumulate(
        std::make_move_iterator(chunks.begin()),
        std::make_move_iterator(chunks.end()),
        byte_string{},
        [](byte_string const acc, KeyedChunk const chunk) {
            return std::move(acc) + std::move(chunk.second);
        });

    byte_string_view view{call_frames_encoded};
    fmt::println(
        "CallFrame ID {}: RLP encoded call trace bytes: {}",
        txn_idx,
        view.size());
    auto const call_frame = rlp::decode_call_frames(view);
    MONAD_ASSERT(!call_frame.has_error());
    MONAD_ASSERT(view.empty());
    return call_frame.value();
}

int main(int argc, char *argv[])
{
    std::vector<std::filesystem::path> dbname_paths;
    auto log_level = quill::LogLevel::Info;
    std::optional<uint64_t> tx_id = std::nullopt;
    uint64_t block_number = INVALID_BLOCK_NUM;

    CLI::App cli{"monad_cli"};
    cli.add_option(
           "--db",
           dbname_paths,
           "A comma-separated list of previously created database paths")
        ->required();
    cli.add_option("--block", block_number, "Block number to inspect")
        ->required();

    cli.add_option("--tx-id", tx_id, "Transaction ID to inspect");

    try {
        cli.parse(argc, argv);
    }
    catch (CLI::CallForHelp const &e) {
        return cli.exit(e);
    }
    catch (CLI::RequiredError const &e) {
        return cli.exit(e);
    }
    auto stdout_handler = quill::stdout_handler();
    stdout_handler->set_pattern(
        "%(time) [%(thread_id)] %(file_name):%(line_number) LOG_%(log_level)\t"
        "%(message)",
        "%Y-%m-%d %H:%M:%S.%Qns",
        quill::Timezone::GmtTime);
    quill::Config cfg;
    cfg.default_handlers.emplace_back(stdout_handler);
    quill::configure(cfg);
    quill::start(true);
    quill::get_root_logger()->set_log_level(log_level);
    quill::flush();

    fmt::println("Opening read only database {}.", dbname_paths);
    ReadOnlyOnDiskDbConfig const ro_config{.dbname_paths = dbname_paths};
    AsyncIOContext io_ctx{ro_config};
    Db ro_db{io_ctx};
    fmt::println(
        "db summary: earliest_block_id={} latest_block_id={} "
        "latest_finalized_block_id={} last_verified_block_id={} "
        "history_length={}\n",
        ro_db.get_earliest_version(),
        ro_db.get_latest_version(),
        ro_db.get_latest_finalized_version(),
        ro_db.get_latest_verified_version(),
        ro_db.get_history_length());

    if (block_number >= ro_db.get_earliest_version() &&
        block_number <= ro_db.get_latest_version()) {
        fmt::println("Inspecting block {}\n", block_number);
        auto const block_header_res = ro_db.get(
            concat(finalized_nibbles, BLOCKHEADER_NIBBLE), block_number);
        if (!block_header_res) {
            LOG_ERROR(
                "Error: block header not found for block {} -- {}",
                block_number,
                block_header_res.error().message().c_str());
        }
        else {
            auto encoded_block_header = block_header_res.value();
            auto const block_header_res =
                rlp::decode_block_header(encoded_block_header);
            if (!block_header_res) {
                LOG_ERROR(
                    "Error: failed to decode block header for block {} -- {}",
                    block_number,
                    block_header_res.error().message().c_str());
            }
            else {
                auto const &block_header = block_header_res.value();
                fmt::println("Block Header: {}\n", block_header);
            }
        }
    }
    else {
        LOG_ERROR(
            "Error: block number {} is out of range [{}, {}]",
            block_number,
            ro_db.get_earliest_version(),
            ro_db.get_latest_version());
        return 1;
    }

    if (tx_id) {
        auto const tx_res = ro_db.get(
            concat(
                finalized_nibbles,
                TRANSACTION_NIBBLE,
                mpt::NibblesView{rlp::encode_unsigned(tx_id.value())}),
            block_number);
        if (!tx_res) {
            LOG_ERROR(
                "Error: transaction id {} not found in block {} -- {}",
                tx_id.value(),
                block_number,
                tx_res.error().message().c_str());
        }
        else {
            auto encoded_tx = tx_res.value();
            auto const decode_res = decode_transaction_db(encoded_tx);
            if (!decode_res) {
                LOG_ERROR(
                    "Error: failed to decode transaction id {} in block {} -- "
                    "{}",
                    tx_id.value(),
                    block_number,
                    decode_res.error().message().c_str());
            }
            else {
                auto const &[tx, sender] = decode_res.value();
                fmt::println("Transaction ID {}: {}", tx_id.value(), tx);
                fmt::println("Sender: {}\n", sender);
            }
        }

        auto const receipt_res = ro_db.get(
            concat(
                finalized_nibbles,
                RECEIPT_NIBBLE,
                mpt::NibblesView{rlp::encode_unsigned(tx_id.value())}),
            block_number);
        if (!receipt_res) {
            LOG_ERROR(
                "Error: receipt id {} not found in block {} -- {}",
                tx_id.value(),
                block_number,
                receipt_res.error().message().c_str());
        }
        else {
            auto encoded_receipt = receipt_res.value();
            auto const decode_res = decode_receipt_db(encoded_receipt);
            if (!decode_res) {
                LOG_ERROR(
                    "Error: failed to decode receipt id {} in block {} -- {}",
                    tx_id.value(),
                    block_number,
                    decode_res.error().message().c_str());
            }
            else {
                auto const &[receipt, log_index_begin] = decode_res.value();
                fmt::println("Receipt ID {}: {}", tx_id.value(), receipt);
                fmt::println("Log index begin: {}\n", log_index_begin);
            }
        }
        auto const callframe_res =
            read_call_frame(ro_db, block_number, tx_id.value());
        if (callframe_res.empty()) {
            LOG_ERROR(
                "Error: call frame for tx id {} not found in block {}",
                tx_id.value(),
                block_number);
        }
        else {
            fmt::println(
                "CallFrame ID {} has {} frames",
                tx_id.value(),
                callframe_res.size());
            fmt::println("Call frame formatter is not supported yet.");
        }
    }
}
