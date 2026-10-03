#include <turboraft/raft_wal_storage.h>

#include <salts_error.h>

#include <stdint.h>
#include <stdio.h>
#include <string.h>

static tr_raft_entry_t make_entry(tr_raft_index_t index,
                                  tr_raft_term_t term,
                                  uint64_t command_id,
                                  const char *text)
{
    tr_raft_entry_t entry;
    size_t size = strlen(text);

    memset(&entry, 0, sizeof(entry));
    entry.index = index;
    entry.term = term;
    entry.command_id = command_id;
    entry.data_length = size;
    memcpy(entry.data, text, size);
    return entry;
}

static int fail(const char *stage, int result)
{
    fprintf(stderr, "v0.2.0 fixture producer failed at %s: %d\n",
            stage, result);
    return 1;
}

int main(int argc, char **argv)
{
    static const uint8_t snapshot[] = "v0.2.0-snapshot";
    const tr_raft_conf_t configuration = {
        TR_RAFT_CONF_FINAL, 7U, 1U,
        {{1U, TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER}}
    };
    tr_raft_wal_storage_config_t config;
    tr_raft_wal_storage_t *storage = NULL;
    tr_raft_storage_t adapter;
    tr_raft_entry_t entries[3];
    int result;

    if (argc != 2 || argv[1][0] == '\0') {
        fprintf(stderr, "usage: %s <wal-prefix>\n", argv[0]);
        return 2;
    }

    memset(&config, 0, sizeof(config));
    config.path_prefix = argv[1];
    config.segment_bytes = TR_RAFT_WAL_MIN_SEGMENT_BYTES;
    config.max_transaction_bytes = 16U * 1024U;
    config.max_segments = 4U;
    config.max_log_entries = 16U;
    config.max_snapshot_bytes = 1024U;
    config.create_if_missing = true;

    entries[0] = make_entry(1U, 1U, 1U, "one");
    entries[1] = make_entry(2U, 2U, 2U, "two");
    entries[2] = make_entry(3U, 2U, 3U, "three");

    result = tr_raft_wal_storage_open(&config, &storage);
    if (result != SALTS_OK) return fail("open", result);
    result = tr_raft_wal_storage_bind(storage, &adapter);
    if (result != SALTS_OK) return fail("bind", result);
    result = adapter.begin(adapter.context);
    if (result != SALTS_OK) return fail("begin", result);
    result = adapter.write_hard_state(adapter.context, 2U, 1U);
    if (result == SALTS_OK) {
        result = adapter.append_log(adapter.context, entries, 3U);
    }
    if (result == SALTS_OK) {
        result = adapter.write_commit_index(adapter.context, 3U);
    }
    if (result == SALTS_OK) {
        result = adapter.commit(adapter.context);
    }
    if (result != SALTS_OK) return fail("commit", result);

    result = tr_raft_wal_storage_store_snapshot(
        storage, 2U, 2U, &configuration, snapshot, sizeof(snapshot) - 1U);
    if (result != SALTS_OK) return fail("snapshot", result);

    result = tr_raft_wal_storage_close(storage);
    if (result != SALTS_OK) return fail("close", result);

    printf("producer=v0.2.0 term=2 voted_for=1 commit=3 snapshot=2/2 "
           "transition=7 suffix_index=3\n");
    return 0;
}
