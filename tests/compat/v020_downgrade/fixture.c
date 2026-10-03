#include <turboraft/raft_wal_storage.h>

#include <salts_error.h>

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Private test-only symbol from the current WalStorage archive. */
int tr_raft_wal_storage_rebase_segment_sequence_for_test(
    tr_raft_wal_storage_t *storage, uint64_t sequence);

static int commit_entry(tr_raft_wal_storage_t *storage)
{
    tr_raft_storage_t adapter;
    tr_raft_entry_t entry;

    memset(&adapter, 0, sizeof(adapter));
    memset(&entry, 0, sizeof(entry));
    entry.index = 1U;
    entry.term = 1U;
    entry.command_id = 1U;
    entry.data_length = 4U;
    memcpy(entry.data, "data", 4U);

    if (tr_raft_wal_storage_bind(storage, &adapter) != SALTS_OK) {
        return SALTS_EIO;
    }
    if (adapter.begin(adapter.context) != SALTS_OK ||
        adapter.write_hard_state(adapter.context, 1U, 1U) != SALTS_OK ||
        adapter.append_log(adapter.context, &entry, 1U) != SALTS_OK ||
        adapter.write_commit_index(adapter.context, 1U) != SALTS_OK) {
        return SALTS_EIO;
    }
    return adapter.commit(adapter.context);
}

int main(int argc, char **argv)
{
    tr_raft_wal_storage_config_t config;
    tr_raft_wal_storage_t *storage = NULL;
    int result;

    if (argc != 3 ||
        (strcmp(argv[2], "low") != 0 &&
         strcmp(argv[2], "entry") != 0 &&
         strcmp(argv[2], "seq2") != 0 &&
         strcmp(argv[2], "high") != 0)) {
        fprintf(stderr, "usage: %s PATH_PREFIX low|entry|seq2|high\n", argv[0]);
        return 2;
    }

    memset(&config, 0, sizeof(config));
    config.path_prefix = argv[1];
    config.segment_bytes = TR_RAFT_WAL_MIN_SEGMENT_BYTES;
    config.max_transaction_bytes = 16U * 1024U;
    config.max_live_segments = 4U;
    config.max_log_entries = 16U;
    config.max_snapshot_bytes = 1024U;
    config.create_if_missing = true;

    result = tr_raft_wal_storage_open(&config, &storage);
    if (result != SALTS_OK) {
        fprintf(stderr, "open failed: %d\n", result);
        return 1;
    }

    if (strcmp(argv[2], "entry") == 0) {
        result = commit_entry(storage);
    } else if (strcmp(argv[2], "seq2") == 0) {
        result = tr_raft_wal_storage_rebase_segment_sequence_for_test(
            storage, UINT64_C(2));
    } else if (strcmp(argv[2], "high") == 0) {
        result = tr_raft_wal_storage_rebase_segment_sequence_for_test(
            storage, UINT64_C(65536));
    }

    {
        int close_result = tr_raft_wal_storage_close(storage);
        if (result == SALTS_OK) {
            result = close_result;
        }
    }
    if (result != SALTS_OK) {
        fprintf(stderr, "fixture failed: %d\n", result);
        return 1;
    }
    return 0;
}
