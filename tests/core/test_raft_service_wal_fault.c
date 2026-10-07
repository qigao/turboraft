#include <turboraft/raft_service.h>
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

enum { SERVICE_WAL_MAX_SEGMENTS = 4U, SERVICE_WAL_MAX_RULES = 2U };

typedef struct fault_rule {
    tr_raft_wal_io_phase_t phase;
    uint64_t ordinal;
    int status;
    size_t write_limit;
} fault_rule_t;

typedef struct fault_plan {
    fault_rule_t rules[SERVICE_WAL_MAX_RULES];
    size_t count;
} fault_plan_t;

typedef struct apply_state {
    size_t apply_count;
} apply_state_t;

static unsigned temp_sequence = 0U;

static int expect(int condition, const char *message)
{
    if (condition) {
        return 0;
    }
    fprintf(stderr, "FAIL: %s\n", message);
    return 1;
}

static char *make_temp_prefix(const char *name)
{
    char *prefix = (char *)malloc(SALTS_FS_MAX_PATH);
    int written;

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
            ++temp_sequence);
    }
#else
    {
        const char *directory = getenv("TMPDIR");

        if (directory == NULL || directory[0] == '\0') {
            directory = "/tmp";
        }
        written = snprintf(
            prefix, SALTS_FS_MAX_PATH, "%s/%s-%ld-%u",
            directory, name, (long)getpid(), ++temp_sequence);
    }
#endif
    if (written < 0 || (size_t)written >= SALTS_FS_MAX_PATH) {
        free(prefix);
        return NULL;
    }
    return prefix;
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

    if (prefix == NULL) {
        return;
    }
    for (sequence = 1U; sequence <= SERVICE_WAL_MAX_SEGMENTS; ++sequence) {
        (void)snprintf(path, sizeof(path), "%s.%08zu.wal",
                       prefix, sequence);
        unlink_if_exists(path);
        (void)snprintf(path, sizeof(path), "%s.%08zu.wal.tmp",
                       prefix, sequence);
        unlink_if_exists(path);
    }
    (void)snprintf(path, sizeof(path), "%s.lock", prefix);
    unlink_if_exists(path);
    free(prefix);
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

static int apply_batch(
    void *context,
    const tr_raft_entry_t *entries,
    size_t entry_count)
{
    apply_state_t *state = (apply_state_t *)context;

    if (state == NULL || entries == NULL || entry_count == 0U) {
        return SALTS_EINVAL;
    }
    state->apply_count += entry_count;
    return SALTS_OK;
}

static tr_raft_wal_storage_config_t wal_config(
    const char *prefix, int create_if_missing)
{
    tr_raft_wal_storage_config_t config;

    memset(&config, 0, sizeof(config));
    config.path_prefix = prefix;
    config.segment_bytes = TR_RAFT_WAL_MIN_SEGMENT_BYTES;
    config.max_transaction_bytes = 8U * 1024U;
    config.max_segments = SERVICE_WAL_MAX_SEGMENTS;
    config.max_log_entries = 16U;
    config.max_snapshot_bytes = 1024U;
    config.create_if_missing = create_if_missing != 0;
    return config;
}

static void service_config(
    tr_raft_service_config_t *config,
    const tr_raft_storage_t *storage,
    apply_state_t *apply)
{
    static const tr_raft_node_id_t voters[] = {1U};

    memset(config, 0, sizeof(*config));
    config->core.self_id = 1U;
    config->core.voters = voters;
    config->core.voter_count = 1U;
    config->core.heartbeat_ticks = 1U;
    config->core.election_min_ticks = 3U;
    config->core.election_max_ticks = 5U;
    config->core.initial_election_timeout_ticks = 3U;
    config->core.max_log_entries = 16U;
    config->storage = *storage;
    config->state_machine.context = apply;
    config->state_machine.apply_batch = apply_batch;
}

static int recover_expect(
    const char *prefix,
    tr_raft_index_t commit_index,
    size_t entry_count)
{
    tr_raft_wal_storage_config_t config = wal_config(prefix, 0);
    tr_raft_wal_storage_t *storage = NULL;
    tr_raft_wal_recovery_t recovery;
    int failed = 0;

    memset(&recovery, 0, sizeof(recovery));
    failed |= expect(
        tr_raft_wal_storage_open(&config, &storage) == SALTS_OK,
        "normal reopen after Service fault");
    if (storage == NULL) {
        return 1;
    }
    failed |= expect(
        tr_raft_wal_storage_load(storage, &recovery) == SALTS_OK,
        "normal recovery after Service fault");
    failed |= expect(recovery.term == 1U,
                     "durable election hard state survives");
    failed |= expect(recovery.commit_index == commit_index,
                     "recovered commit index matches durable prefix");
    failed |= expect(recovery.entry_count == entry_count,
                     "recovered log count matches durable prefix");
    tr_raft_wal_recovery_destroy(&recovery);
    failed |= expect(
        tr_raft_wal_storage_close(storage) == SALTS_OK,
        "close recovered storage");
    return failed;
}

static int run_service_fault(
    const char *name,
    fault_plan_t *plan,
    int expected_error,
    tr_raft_index_t recovered_commit,
    size_t recovered_entries)
{
    char *prefix = make_temp_prefix(name);
    tr_raft_wal_storage_config_t config = wal_config(prefix, 1);
    tr_raft_wal_storage_t *storage = NULL;
    tr_raft_storage_t adapter;
    tr_raft_service_config_t service_configuration;
    tr_raft_service_t *service = NULL;
    tr_raft_service_status_t status;
    tr_raft_wal_io_fault_provider_t provider;
    tr_raft_tick_t tick = {3U, 4U};
    tr_raft_proposal_t proposal;
    apply_state_t apply;
    int failed = 0;
    int result;

    memset(&adapter, 0, sizeof(adapter));
    memset(&apply, 0, sizeof(apply));
    provider.before_io = fault_before_io;
    provider.context = plan;

    failed |= expect(prefix != NULL, "create Service/WAL temp prefix");
    if (prefix == NULL) {
        return 1;
    }
    failed |= expect(
        tr_raft_wal_storage_open(&config, &storage) == SALTS_OK,
        "open Service/WAL storage");
    if (storage == NULL) {
        cleanup_prefix(prefix);
        return 1;
    }
    failed |= expect(
        tr_raft_wal_storage_bind(storage, &adapter) == SALTS_OK,
        "bind Service/WAL adapter");

    service_config(&service_configuration, &adapter, &apply);
    failed |= expect(
        tr_raft_service_create(&service_configuration, &service) == SALTS_OK,
        "create Service over real WAL");
    if (service == NULL) {
        (void)tr_raft_wal_storage_close(storage);
        cleanup_prefix(prefix);
        return 1;
    }
    failed |= expect(
        tr_raft_service_tick(service, &tick) == SALTS_OK,
        "persist single-node election before injection");
    failed |= expect(
        tr_raft_service_status(service, &status) == SALTS_OK &&
            status.core.role == TR_RAFT_LEADER &&
            status.core.commit_index == 0U &&
            status.core.applied_index == 0U &&
            !status.core.ready_outstanding,
        "baseline leader is fully durable and quiescent");

    failed |= expect(
        tr_raft_wal_storage_set_io_fault_provider_for_test(
            storage, &provider) == SALTS_OK,
        "install Service/WAL fault provider");

    memset(&proposal, 0, sizeof(proposal));
    proposal.command_id = 1U;
    proposal.data = "x";
    proposal.data_length = 1U;
    result = tr_raft_service_propose(service, &proposal);
    failed |= expect(result == expected_error,
                     "Service proposal surfaces storage durability failure");

    failed |= expect(
        tr_raft_service_status(service, &status) == SALTS_OK,
        "read Service status after durability failure");
    failed |= expect(status.faulted && status.cause == expected_error,
                     "Service faults on storage durability failure");
    failed |= expect(
        status.runtime.stage == TR_RAFT_RUNTIME_STORAGE_COMMIT &&
            status.runtime.cause == expected_error &&
            !status.runtime.durable,
        "Runtime reports failed, non-durable storage commit");
    failed |= expect(status.core.ready_outstanding,
                     "failed durability keeps Ready outstanding");
    failed |= expect(status.core.commit_index == 1U,
                     "Core logical commit remains provisional behind Ready");
    failed |= expect(status.core.applied_index == 0U,
                     "Core applied index never crosses failed durability");
    failed |= expect(apply.apply_count == 0U,
                     "FSM is never invoked before durable storage commit");

    tr_raft_service_destroy(service);
    service = NULL;
    failed |= expect(
        tr_raft_wal_storage_close(storage) == SALTS_OK,
        "close faulted Service/WAL storage");
    storage = NULL;

    failed |= recover_expect(
        prefix, recovered_commit, recovered_entries);
    cleanup_prefix(prefix);
    return failed;
}

static int test_partial_write_enospc(void)
{
    fault_plan_t plan;

    memset(&plan, 0, sizeof(plan));
    plan.rules[0] = (fault_rule_t){
        TR_RAFT_WAL_IO_TRANSACTION_WRITE, 1U, SALTS_OK, 8U};
    plan.rules[1] = (fault_rule_t){
        TR_RAFT_WAL_IO_TRANSACTION_WRITE, 2U, SALTS_ENOSPC, 0U};
    plan.count = 2U;
    return run_service_fault(
        "turboraft-service-wal-short",
        &plan, SALTS_ENOSPC, 0U, 0U);
}

static int test_fsync_failure(void)
{
    fault_plan_t plan;

    memset(&plan, 0, sizeof(plan));
    plan.rules[0] = (fault_rule_t){
        TR_RAFT_WAL_IO_TRANSACTION_FSYNC, 1U, SALTS_EIO, 0U};
    plan.count = 1U;
    return run_service_fault(
        "turboraft-service-wal-fsync",
        &plan, SALTS_EIO, 1U, 1U);
}

int main(void)
{
    int failed = 0;

    failed |= test_partial_write_enospc();
    failed |= test_fsync_failure();
    if (failed != 0) {
        return 1;
    }
    puts("PASS: Service never applies or acknowledges failed WAL durability");
    return 0;
}
