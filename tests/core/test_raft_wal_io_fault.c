#include <turboraft/raft_wal_storage.h>

#include "raft_wal_storage_internal.h"

#include <cmeta_error.h>
#include <cmeta_fs.h>

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#if defined(_WIN32)
#include <windows.h>
#else
#include <unistd.h>
#endif

enum { FAULT_TEST_MAX_SEGMENTS = 4U, FAULT_TEST_MAX_RULES = 4U };

static unsigned fault_temp_sequence = 0U;

static char *make_temp_prefix(const char *name)
{
    char *prefix;
    int written;

    if (name == NULL || name[0] == '\0') {
        return NULL;
    }
    prefix = (char *)malloc(SALTS_FS_MAX_PATH);
    if (prefix == NULL) {
        return NULL;
    }
#if defined(_WIN32)
    {
        char directory[SALTS_FS_MAX_PATH];
        DWORD length = GetTempPathA((DWORD)sizeof(directory), directory);

        if (length == 0U || length >= sizeof(directory)) {
            free(prefix);
            return NULL;
        }
        written = snprintf(
            prefix, SALTS_FS_MAX_PATH, "%s%s-%lu-%u",
            directory, name, (unsigned long)GetCurrentProcessId(),
            ++fault_temp_sequence);
    }
#else
    {
        const char *directory = getenv("TMPDIR");

        if (directory == NULL || directory[0] == '\0') {
            directory = "/tmp";
        }
        written = snprintf(
            prefix, SALTS_FS_MAX_PATH, "%s/%s-%ld-%u",
            directory, name, (long)getpid(), ++fault_temp_sequence);
    }
#endif
    if (written < 0 || (size_t)written >= SALTS_FS_MAX_PATH) {
        free(prefix);
        return NULL;
    }
    return prefix;
}

typedef struct fault_rule {
    tr_raft_wal_io_phase_t phase;
    uint64_t ordinal;
    int status;
    size_t write_limit;
} fault_rule_t;

typedef struct fault_plan {
    fault_rule_t rules[FAULT_TEST_MAX_RULES];
    size_t count;
} fault_plan_t;

static int expect(int condition, const char *message)
{
    if (condition) {
        return 0;
    }
    fprintf(stderr, "FAIL: %s\n", message);
    return 1;
}

static int fault_before_io(
    void *context,
    tr_raft_wal_io_phase_t phase,
    uint64_t ordinal,
    size_t *inout_size)
{
    fault_plan_t *plan = (fault_plan_t *)context;
    size_t index;

    if (plan == NULL || inout_size == NULL) {
        return SALTS_EINVAL;
    }
    for (index = 0U; index < plan->count; ++index) {
        const fault_rule_t *rule = &plan->rules[index];

        if (rule->phase != phase || rule->ordinal != ordinal) {
            continue;
        }
        if (rule->write_limit != 0U &&
            *inout_size > rule->write_limit) {
            *inout_size = rule->write_limit;
        }
        return rule->status;
    }
    return SALTS_OK;
}

static tr_raft_wal_io_fault_provider_t provider_for(fault_plan_t *plan)
{
    tr_raft_wal_io_fault_provider_t provider;

    provider.before_io = fault_before_io;
    provider.context = plan;
    return provider;
}

static tr_raft_wal_storage_config_t fault_config(
    const char *prefix, int create)
{
    tr_raft_wal_storage_config_t config;

    memset(&config, 0, sizeof(config));
    config.path_prefix = prefix;
    config.segment_bytes = TR_RAFT_WAL_MIN_SEGMENT_BYTES;
    config.max_transaction_bytes = 8U * 1024U;
    config.max_segments = FAULT_TEST_MAX_SEGMENTS;
    config.max_log_entries = 16U;
    config.max_snapshot_bytes = 1024U;
    config.create_if_missing = create != 0;
    return config;
}

static tr_raft_entry_t fault_entry(tr_raft_index_t index, const char *value)
{
    tr_raft_entry_t entry;
    size_t size = strlen(value) + 1U;

    memset(&entry, 0, sizeof(entry));
    entry.index = index;
    entry.term = 1U;
    entry.command_id = index;
    entry.data_length = size;
    memcpy(entry.data, value, size);
    return entry;
}

static int commit_entry(
    tr_raft_wal_storage_t *storage,
    tr_raft_index_t index,
    const char *value)
{
    tr_raft_storage_t adapter;
    tr_raft_entry_t entry = fault_entry(index, value);
    int result;

    memset(&adapter, 0, sizeof(adapter));
    result = tr_raft_wal_storage_bind(storage, &adapter);
    if (result != SALTS_OK) return result;
    result = adapter.begin(adapter.context);
    if (result == SALTS_OK) {
        result = adapter.write_hard_state(adapter.context, 1U, 1U);
    }
    if (result == SALTS_OK) {
        result = adapter.append_log(adapter.context, &entry, 1U);
    }
    if (result == SALTS_OK) {
        result = adapter.write_commit_index(adapter.context, index);
    }
    if (result == SALTS_OK) {
        return adapter.commit(adapter.context);
    }
    (void)adapter.rollback(adapter.context);
    return result;
}

static void unlink_if_exists(const char *path)
{
    if (cmeta_fs_access(path, SALTS_FS_ACCESS_EXISTS) == SALTS_OK) {
        (void)cmeta_fs_unlink(path);
    }
}

static void cleanup_prefix(char *prefix)
{
    char path[SALTS_FS_MAX_PATH];
    size_t sequence;

    if (prefix == NULL) return;
    for (sequence = 1U; sequence <= FAULT_TEST_MAX_SEGMENTS; ++sequence) {
        (void)snprintf(path, sizeof(path), "%s.%08zu.wal",
                       prefix, sequence);
        unlink_if_exists(path);
        (void)snprintf(path, sizeof(path), "%s.%08zu.wal.tmp",
                       prefix, sequence);
        unlink_if_exists(path);
    }
    for (sequence = 0U; sequence < 2U; ++sequence) {
        const char *suffix = sequence == 0U ? "" : ".tmp";
        (void)snprintf(path, sizeof(path), "%s.snapshot.1.1%s",
                       prefix, suffix);
        unlink_if_exists(path);
    }
    (void)snprintf(path, sizeof(path), "%s.lock", prefix);
    unlink_if_exists(path);
    free(prefix);
}

static int recover_expect(
    const char *prefix,
    tr_raft_index_t commit_index,
    size_t entry_count,
    tr_raft_index_t snapshot_index)
{
    tr_raft_wal_storage_config_t config = fault_config(prefix, 0);
    tr_raft_wal_storage_t *storage = NULL;
    tr_raft_wal_recovery_t recovery;
    int failed = 0;

    memset(&recovery, 0, sizeof(recovery));
    failed |= expect(
        tr_raft_wal_storage_open(&config, &storage) == SALTS_OK,
        "normal reopen must succeed");
    if (storage == NULL) return 1;
    failed |= expect(
        tr_raft_wal_storage_load(storage, &recovery) == SALTS_OK,
        "normal recovery must succeed");
    failed |= expect(recovery.commit_index == commit_index,
                     "recovered commit index");
    failed |= expect(recovery.entry_count == entry_count,
                     "recovered entry count");
    failed |= expect(recovery.snapshot_index == snapshot_index,
                     "recovered snapshot index");
    tr_raft_wal_recovery_destroy(&recovery);
    failed |= expect(
        tr_raft_wal_storage_close(storage) == SALTS_OK,
        "normal reopen close");
    return failed;
}

static int test_short_write_disk_full(void)
{
    char *prefix = make_temp_prefix("turboraft-wal-short-write");
    tr_raft_wal_storage_config_t config = fault_config(prefix, 1);
    tr_raft_wal_storage_t *storage = NULL;
    fault_plan_t plan;
    tr_raft_wal_io_fault_provider_t provider;
    int failed = 0;

    memset(&plan, 0, sizeof(plan));
    plan.rules[0] = (fault_rule_t){
        TR_RAFT_WAL_IO_TRANSACTION_WRITE, 1U, SALTS_OK, 8U};
    plan.rules[1] = (fault_rule_t){
        TR_RAFT_WAL_IO_TRANSACTION_WRITE, 2U, SALTS_ENOSPC, 0U};
    plan.count = 2U;
    provider = provider_for(&plan);

    failed |= expect(prefix != NULL, "create short-write prefix");
    if (prefix == NULL) return 1;
    failed |= expect(
        tr_raft_wal_storage_open(&config, &storage) == SALTS_OK,
        "open short-write storage");
    if (storage == NULL) {
        cleanup_prefix(prefix);
        return 1;
    }
    failed |= expect(commit_entry(storage, 1U, "one") == SALTS_OK,
                     "commit durable baseline");
    failed |= expect(
        tr_raft_wal_storage_set_io_fault_provider_for_test(
            storage, &provider) == SALTS_OK,
        "install short-write provider");
    failed |= expect(
        commit_entry(storage, 2U, "two") == SALTS_ENOSPC,
        "short-write followed by ENOSPC must fail commit");
    failed |= expect(
        tr_raft_wal_storage_close(storage) == SALTS_OK,
        "close faulted short-write storage");
    storage = NULL;
    failed |= recover_expect(prefix, 1U, 1U, 0U);
    cleanup_prefix(prefix);
    return failed;
}

static int test_fsync_failure(void)
{
    char *prefix = make_temp_prefix("turboraft-wal-fsync");
    tr_raft_wal_storage_config_t config = fault_config(prefix, 1);
    tr_raft_wal_storage_t *storage = NULL;
    fault_plan_t plan;
    tr_raft_wal_io_fault_provider_t provider;
    int failed = 0;

    memset(&plan, 0, sizeof(plan));
    plan.rules[0] = (fault_rule_t){
        TR_RAFT_WAL_IO_TRANSACTION_FSYNC, 1U, SALTS_EIO, 0U};
    plan.count = 1U;
    provider = provider_for(&plan);

    failed |= expect(prefix != NULL, "create fsync prefix");
    if (prefix == NULL) return 1;
    failed |= expect(
        tr_raft_wal_storage_open(&config, &storage) == SALTS_OK,
        "open fsync storage");
    if (storage == NULL) {
        cleanup_prefix(prefix);
        return 1;
    }
    failed |= expect(commit_entry(storage, 1U, "one") == SALTS_OK,
                     "commit fsync baseline");
    failed |= expect(
        tr_raft_wal_storage_set_io_fault_provider_for_test(
            storage, &provider) == SALTS_OK,
        "install fsync provider");
    failed |= expect(
        commit_entry(storage, 2U, "two") == SALTS_EIO,
        "fsync failure must fail commit");
    failed |= expect(
        commit_entry(storage, 3U, "three") == SALTS_EIO,
        "faulted live owner must reject the next mutation");
    failed |= expect(
        tr_raft_wal_storage_close(storage) == SALTS_OK,
        "close fsync-faulted storage");
    storage = NULL;

    /*
     * The complete frame reached the file before fsync failed. Reopen may
     * therefore observe the fully written new transaction, but never a hybrid.
     */
    failed |= recover_expect(prefix, 2U, 2U, 0U);
    cleanup_prefix(prefix);
    return failed;
}

static int test_snapshot_write_failure(void)
{
    static const tr_raft_conf_t configuration = {
        TR_RAFT_CONF_FINAL,
        1U,
        1U,
        {{1U, TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER}}
    };
    static const uint8_t snapshot[] = {0x10U, 0x20U, 0x30U};
    char *prefix = make_temp_prefix("turboraft-wal-snapshot-write");
    tr_raft_wal_storage_config_t config = fault_config(prefix, 1);
    tr_raft_wal_storage_t *storage = NULL;
    tr_raft_wal_recovery_t recovery;
    fault_plan_t plan;
    tr_raft_wal_io_fault_provider_t provider;
    int failed = 0;

    memset(&plan, 0, sizeof(plan));
    plan.rules[0] = (fault_rule_t){
        TR_RAFT_WAL_IO_SNAPSHOT_WRITE, 1U, SALTS_EIO, 0U};
    plan.count = 1U;
    provider = provider_for(&plan);

    failed |= expect(prefix != NULL, "create snapshot-write prefix");
    if (prefix == NULL) return 1;
    failed |= expect(
        tr_raft_wal_storage_open(&config, &storage) == SALTS_OK,
        "open snapshot-write storage");
    if (storage == NULL) {
        cleanup_prefix(prefix);
        return 1;
    }
    failed |= expect(commit_entry(storage, 1U, "one") == SALTS_OK,
                     "commit snapshot baseline");
    failed |= expect(
        tr_raft_wal_storage_set_io_fault_provider_for_test(
            storage, &provider) == SALTS_OK,
        "install snapshot-write provider");
    failed |= expect(
        tr_raft_wal_storage_store_snapshot(
            storage, 1U, 1U, &configuration,
            snapshot, sizeof(snapshot)) == SALTS_EIO,
        "snapshot temp write fault must surface");

    memset(&recovery, 0, sizeof(recovery));
    failed |= expect(
        tr_raft_wal_storage_load(storage, &recovery) == SALTS_OK,
        "snapshot write failure keeps live WAL usable");
    failed |= expect(recovery.snapshot_index == 0U &&
                         recovery.commit_index == 1U,
                     "failed snapshot cannot become authoritative");
    tr_raft_wal_recovery_destroy(&recovery);

    failed |= expect(
        tr_raft_wal_storage_set_io_fault_provider_for_test(
            storage, NULL) == SALTS_OK,
        "clear snapshot-write provider");
    failed |= expect(
        tr_raft_wal_storage_store_snapshot(
            storage, 1U, 1U, &configuration,
            snapshot, sizeof(snapshot)) == SALTS_OK,
        "snapshot retry after recoverable write fault");
    failed |= expect(
        tr_raft_wal_storage_close(storage) == SALTS_OK,
        "close snapshot-write storage");
    storage = NULL;
    failed |= recover_expect(prefix, 1U, 0U, 1U);
    cleanup_prefix(prefix);
    return failed;
}

static int test_snapshot_header_rewrite_short_write(void)
{
    static const tr_raft_conf_t configuration = {
        TR_RAFT_CONF_FINAL,
        1U,
        1U,
        {{1U, TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER}}
    };
    static const uint8_t snapshot[] = {0x10U, 0x20U, 0x30U};
    char *prefix = make_temp_prefix("turboraft-wal-snapshot-header");
    tr_raft_wal_storage_config_t config = fault_config(prefix, 1);
    tr_raft_wal_storage_t *storage = NULL;
    tr_raft_wal_recovery_t recovery;
    fault_plan_t plan;
    tr_raft_wal_io_fault_provider_t provider;
    char snapshot_path[SALTS_FS_MAX_PATH];
    char staging_path[SALTS_FS_MAX_PATH];
    int failed = 0;

    memset(&plan, 0, sizeof(plan));
    plan.rules[0] = (fault_rule_t){
        TR_RAFT_WAL_IO_SNAPSHOT_HEADER_REWRITE, 1U, SALTS_OK, 8U};
    plan.count = 1U;
    provider = provider_for(&plan);

    failed |= expect(prefix != NULL, "create snapshot-header prefix");
    if (prefix == NULL) return 1;
    (void)snprintf(snapshot_path, sizeof(snapshot_path),
                   "%s.snapshot.1.1", prefix);
    (void)snprintf(staging_path, sizeof(staging_path),
                   "%s.snapshot.1.1.tmp", prefix);

    failed |= expect(
        tr_raft_wal_storage_open(&config, &storage) == SALTS_OK,
        "open snapshot-header storage");
    if (storage == NULL) {
        cleanup_prefix(prefix);
        return 1;
    }
    failed |= expect(commit_entry(storage, 1U, "one") == SALTS_OK,
                     "commit snapshot-header baseline");
    failed |= expect(
        tr_raft_wal_storage_set_io_fault_provider_for_test(
            storage, &provider) == SALTS_OK,
        "install snapshot-header provider");
    failed |= expect(
        tr_raft_wal_storage_store_snapshot(
            storage, 1U, 1U, &configuration,
            snapshot, sizeof(snapshot)) == SALTS_EIO,
        "short snapshot header rewrite must fail publication");
    failed |= expect(
        cmeta_fs_access(snapshot_path, SALTS_FS_ACCESS_EXISTS) != SALTS_OK,
        "failed header rewrite must not publish snapshot");
    failed |= expect(
        cmeta_fs_access(staging_path, SALTS_FS_ACCESS_EXISTS) != SALTS_OK,
        "failed header rewrite must release staging file");

    memset(&recovery, 0, sizeof(recovery));
    failed |= expect(
        tr_raft_wal_storage_load(storage, &recovery) == SALTS_OK,
        "header rewrite failure keeps live WAL usable");
    failed |= expect(recovery.snapshot_index == 0U &&
                         recovery.commit_index == 1U &&
                         recovery.entry_count == 1U,
                     "header rewrite fault leaves previous WAL authoritative");
    tr_raft_wal_recovery_destroy(&recovery);

    failed |= expect(
        tr_raft_wal_storage_set_io_fault_provider_for_test(
            storage, NULL) == SALTS_OK,
        "clear snapshot-header provider");
    failed |= expect(
        tr_raft_wal_storage_store_snapshot(
            storage, 1U, 1U, &configuration,
            snapshot, sizeof(snapshot)) == SALTS_OK,
        "snapshot retry after header rewrite fault");
    failed |= expect(
        tr_raft_wal_storage_close(storage) == SALTS_OK,
        "close snapshot-header storage");
    storage = NULL;
    failed |= recover_expect(prefix, 1U, 0U, 1U);
    cleanup_prefix(prefix);
    return failed;
}

static int test_reopen_open_failure(void)
{
    char *prefix = make_temp_prefix("turboraft-wal-reopen-open");
    tr_raft_wal_storage_config_t config = fault_config(prefix, 1);
    tr_raft_wal_storage_t *storage = NULL;
    fault_plan_t plan;
    tr_raft_wal_io_fault_provider_t provider;
    int failed = 0;

    memset(&plan, 0, sizeof(plan));
    plan.rules[0] = (fault_rule_t){
        TR_RAFT_WAL_IO_REOPEN_OPEN, 1U, SALTS_EIO, 0U};
    plan.count = 1U;
    provider = provider_for(&plan);

    failed |= expect(prefix != NULL, "create reopen-open prefix");
    if (prefix == NULL) return 1;
    failed |= expect(
        tr_raft_wal_storage_open(&config, &storage) == SALTS_OK,
        "create reopen-open storage");
    if (storage == NULL) {
        cleanup_prefix(prefix);
        return 1;
    }
    failed |= expect(commit_entry(storage, 1U, "one") == SALTS_OK,
                     "commit reopen-open baseline");
    failed |= expect(
        tr_raft_wal_storage_close(storage) == SALTS_OK,
        "close reopen-open baseline");
    storage = NULL;

    config.create_if_missing = false;
    failed |= expect(
        tr_raft_wal_storage_open_with_io_fault_provider_for_test(
            &config, &provider, &storage) == SALTS_EIO,
        "injected writable reopen failure");
    failed |= expect(storage == NULL, "failed reopen does not return owner");
    failed |= recover_expect(prefix, 1U, 1U, 0U);
    cleanup_prefix(prefix);
    return failed;
}

static int test_reopen_truncate_failure(void)
{
    char *prefix = make_temp_prefix("turboraft-wal-reopen-truncate");
    tr_raft_wal_storage_config_t config = fault_config(prefix, 1);
    tr_raft_wal_storage_t *storage = NULL;
    fault_plan_t write_plan;
    fault_plan_t reopen_plan;
    tr_raft_wal_io_fault_provider_t write_provider;
    tr_raft_wal_io_fault_provider_t reopen_provider;
    int failed = 0;

    memset(&write_plan, 0, sizeof(write_plan));
    write_plan.rules[0] = (fault_rule_t){
        TR_RAFT_WAL_IO_TRANSACTION_WRITE, 1U, SALTS_OK, 8U};
    write_plan.rules[1] = (fault_rule_t){
        TR_RAFT_WAL_IO_TRANSACTION_WRITE, 2U, SALTS_ENOSPC, 0U};
    write_plan.count = 2U;
    write_provider = provider_for(&write_plan);

    memset(&reopen_plan, 0, sizeof(reopen_plan));
    reopen_plan.rules[0] = (fault_rule_t){
        TR_RAFT_WAL_IO_REOPEN_TRUNCATE, 1U, SALTS_EIO, 0U};
    reopen_plan.count = 1U;
    reopen_provider = provider_for(&reopen_plan);

    failed |= expect(prefix != NULL, "create reopen-truncate prefix");
    if (prefix == NULL) return 1;
    failed |= expect(
        tr_raft_wal_storage_open(&config, &storage) == SALTS_OK,
        "create reopen-truncate storage");
    if (storage == NULL) {
        cleanup_prefix(prefix);
        return 1;
    }
    failed |= expect(commit_entry(storage, 1U, "one") == SALTS_OK,
                     "commit reopen-truncate baseline");
    failed |= expect(
        tr_raft_wal_storage_set_io_fault_provider_for_test(
            storage, &write_provider) == SALTS_OK,
        "install partial-tail provider");
    failed |= expect(
        commit_entry(storage, 2U, "two") == SALTS_ENOSPC,
        "create deterministic partial transaction tail");
    failed |= expect(
        tr_raft_wal_storage_close(storage) == SALTS_OK,
        "close partial-tail storage");
    storage = NULL;

    config.create_if_missing = false;
    failed |= expect(
        tr_raft_wal_storage_open_with_io_fault_provider_for_test(
            &config, &reopen_provider, &storage) == SALTS_EIO,
        "injected recovery truncate failure");
    failed |= expect(storage == NULL,
                     "truncate failure does not retain writer lock");
    failed |= recover_expect(prefix, 1U, 1U, 0U);
    cleanup_prefix(prefix);
    return failed;
}

int main(void)
{
    int failed = 0;

    failed |= test_short_write_disk_full();
    failed |= test_fsync_failure();
    failed |= test_snapshot_write_failure();
    failed |= test_snapshot_header_rewrite_short_write();
    failed |= test_reopen_open_failure();
    failed |= test_reopen_truncate_failure();

    if (failed != 0) {
        return 1;
    }
    puts("PASS: deterministic WAL I/O fault provider");
    return 0;
}
