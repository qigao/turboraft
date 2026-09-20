#include <turboraft/raft_service.h>
#include <turboraft/raft_wal_storage.h>

#include <salts_error.h>
#include <salts_fs.h>

#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

enum {
    BENCH_PROPOSALS = 512U,
    BENCH_PAYLOAD_BYTES = 64U
};

typedef struct bench_memory_storage {
    size_t begin_count;
    size_t commit_count;
} bench_memory_storage_t;

static uint64_t bench_now_ns(void)
{
    struct timespec ts;

    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        return 0U;
    }
    return (uint64_t)ts.tv_sec * UINT64_C(1000000000) +
           (uint64_t)ts.tv_nsec;
}

static int bench_storage_begin(void *context)
{
    bench_memory_storage_t *storage =
        (bench_memory_storage_t *)context;

    ++storage->begin_count;
    return SALTS_OK;
}

static int bench_storage_hard(void *context,
                              tr_raft_term_t term,
                              tr_raft_node_id_t vote)
{
    (void)context;
    (void)term;
    (void)vote;
    return SALTS_OK;
}

static int bench_storage_truncate(void *context,
                                  tr_raft_index_t from_index)
{
    (void)context;
    (void)from_index;
    return SALTS_OK;
}

static int bench_storage_append(void *context,
                                const tr_raft_entry_t *entries,
                                size_t count)
{
    (void)context;
    (void)entries;
    (void)count;
    return SALTS_OK;
}

static int bench_storage_commit_index(void *context,
                                      tr_raft_index_t commit_index)
{
    (void)context;
    (void)commit_index;
    return SALTS_OK;
}

static int bench_storage_commit(void *context)
{
    bench_memory_storage_t *storage =
        (bench_memory_storage_t *)context;

    ++storage->commit_count;
    return SALTS_OK;
}

static int bench_storage_rollback(void *context)
{
    (void)context;
    return SALTS_OK;
}

static int bench_apply(void *context,
                       const tr_raft_entry_t *entries,
                       size_t count)
{
    (void)context;
    (void)entries;
    (void)count;
    return SALTS_OK;
}

static void bench_core_config(tr_raft_core_config_t *config)
{
    static const tr_raft_node_id_t voters[] = {1U};

    memset(config, 0, sizeof(*config));
    config->self_id = 1U;
    config->voters = voters;
    config->voter_count = 1U;
    config->heartbeat_ticks = 2U;
    config->election_min_ticks = 5U;
    config->election_max_ticks = 10U;
    config->initial_election_timeout_ticks = 5U;
    config->max_log_entries = BENCH_PROPOSALS + 16U;
}

static int bench_elect_core(tr_raft_core_t *core)
{
    tr_raft_message_t messages[2];
    tr_raft_ready_t ready;
    tr_raft_tick_t tick = {5U, 7U};
    int result;

    memset(&ready, 0, sizeof(ready));
    ready.messages = messages;
    ready.message_capacity = 2U;
    result = tr_raft_core_tick(core, &tick, &ready);
    if (result != SALTS_OK) {
        return result;
    }
    return tr_raft_core_advance(core);
}

static int bench_core_path(double *out_ns_per_op)
{
    tr_raft_core_config_t config;
    tr_raft_core_t *core = NULL;
    tr_raft_message_t messages[2];
    uint8_t payload[BENCH_PAYLOAD_BYTES];
    size_t index;
    uint64_t start;
    uint64_t end;
    int result;

    if (out_ns_per_op == NULL) {
        return SALTS_EINVAL;
    }
    memset(payload, 0x5a, sizeof(payload));
    bench_core_config(&config);
    result = tr_raft_core_create(&config, &core);
    if (result != SALTS_OK) {
        return result;
    }
    result = bench_elect_core(core);
    if (result != SALTS_OK) {
        tr_raft_core_destroy(core);
        return result;
    }

    start = bench_now_ns();
    for (index = 0U; index < BENCH_PROPOSALS; ++index) {
        tr_raft_ready_t ready;
        tr_raft_proposal_t proposal;

        memset(&ready, 0, sizeof(ready));
        ready.messages = messages;
        ready.message_capacity = 2U;
        memset(&proposal, 0, sizeof(proposal));
        proposal.command_id = index + 1U;
        proposal.data = payload;
        proposal.data_length = sizeof(payload);
        result = tr_raft_core_propose(core, &proposal, &ready);
        if (result != SALTS_OK) {
            break;
        }
        result = tr_raft_core_advance(core);
        if (result != SALTS_OK) {
            break;
        }
    }
    end = bench_now_ns();

    if (result == SALTS_OK) {
        *out_ns_per_op =
            (double)(end - start) / (double)BENCH_PROPOSALS;
    }
    tr_raft_core_destroy(core);
    return result;
}

static int bench_service_create(
    const tr_raft_storage_t *storage,
    tr_raft_service_t **out_service)
{
    tr_raft_service_config_t config;

    if (storage == NULL || out_service == NULL) {
        return SALTS_EINVAL;
    }
    memset(&config, 0, sizeof(config));
    bench_core_config(&config.core);
    config.storage = *storage;
    config.state_machine.apply_batch = bench_apply;
    return tr_raft_service_create(&config, out_service);
}

static int bench_elect_service(tr_raft_service_t *service)
{
    tr_raft_tick_t tick = {5U, 7U};

    return tr_raft_service_tick(service, &tick);
}

static int bench_service_path(
    const tr_raft_storage_t *storage,
    double *out_ns_per_op)
{
    tr_raft_service_t *service = NULL;
    uint8_t payload[BENCH_PAYLOAD_BYTES];
    size_t index;
    uint64_t start;
    uint64_t end;
    int result;

    if (out_ns_per_op == NULL) {
        return SALTS_EINVAL;
    }
    memset(payload, 0x6b, sizeof(payload));
    result = bench_service_create(storage, &service);
    if (result != SALTS_OK) {
        return result;
    }
    result = bench_elect_service(service);
    if (result != SALTS_OK) {
        tr_raft_service_destroy(service);
        return result;
    }

    start = bench_now_ns();
    for (index = 0U; index < BENCH_PROPOSALS; ++index) {
        tr_raft_proposal_t proposal;

        memset(&proposal, 0, sizeof(proposal));
        proposal.command_id = index + 1U;
        proposal.data = payload;
        proposal.data_length = sizeof(payload);
        result = tr_raft_service_propose(service, &proposal);
        if (result != SALTS_OK) {
            break;
        }
    }
    end = bench_now_ns();

    if (result == SALTS_OK) {
        *out_ns_per_op =
            (double)(end - start) / (double)BENCH_PROPOSALS;
    }
    tr_raft_service_destroy(service);
    return result;
}

static void bench_memory_storage_adapter(
    bench_memory_storage_t *memory,
    tr_raft_storage_t *storage)
{
    memset(memory, 0, sizeof(*memory));
    memset(storage, 0, sizeof(*storage));
    storage->context = memory;
    storage->begin = bench_storage_begin;
    storage->write_hard_state = bench_storage_hard;
    storage->truncate_log = bench_storage_truncate;
    storage->append_log = bench_storage_append;
    storage->write_commit_index = bench_storage_commit_index;
    storage->commit = bench_storage_commit;
    storage->rollback = bench_storage_rollback;
}

static int bench_wal_batch_path(size_t batch_size,
                                double *out_ns_per_op)
{
    enum { MAX_BATCH = 16U };
    char prefix[256];
    char wal_path[320];
    char lock_path[320];
    tr_raft_wal_storage_config_t config;
    tr_raft_wal_storage_t *wal = NULL;
    tr_raft_storage_t storage;
    tr_raft_entry_t entries[MAX_BATCH];
    tr_raft_index_t next_index = 1U;
    size_t batch;
    uint64_t start;
    uint64_t end;
    int result = SALTS_OK;

    if (out_ns_per_op == NULL || batch_size == 0U ||
        batch_size > MAX_BATCH || BENCH_PROPOSALS % batch_size != 0U) {
        return SALTS_EINVAL;
    }

    snprintf(prefix, sizeof(prefix),
             "/tmp/turboraft-groupcommit-bench-%ld-%zu",
             (long)getpid(), batch_size);
    snprintf(wal_path, sizeof(wal_path), "%s.00000001.wal", prefix);
    snprintf(lock_path, sizeof(lock_path), "%s.lock", prefix);
    (void)salts_fs_unlink(wal_path);
    (void)salts_fs_unlink(lock_path);

    memset(&config, 0, sizeof(config));
    config.path_prefix = prefix;
    config.segment_bytes = TR_RAFT_WAL_DEFAULT_SEGMENT_BYTES;
    config.max_transaction_bytes =
        TR_RAFT_WAL_DEFAULT_TRANSACTION_BYTES;
    config.max_segments = 2U;
    config.max_log_entries = BENCH_PROPOSALS + 16U;
    config.max_snapshot_bytes = 1024U;
    config.create_if_missing = true;

    result = tr_raft_wal_storage_open(&config, &wal);
    if (result == SALTS_OK) {
        result = tr_raft_wal_storage_bind(wal, &storage);
    }

    start = bench_now_ns();
    for (batch = 0U;
         result == SALTS_OK && batch < BENCH_PROPOSALS / batch_size;
         ++batch) {
        size_t index;
        tr_raft_index_t last_index;

        for (index = 0U; index < batch_size; ++index) {
            uint8_t payload[BENCH_PAYLOAD_BYTES];

            memset(payload, 0x7c, sizeof(payload));
            memset(&entries[index], 0, sizeof(entries[index]));
            entries[index].index = next_index++;
            entries[index].term = 1U;
            entries[index].command_id = entries[index].index;
            entries[index].data_length = sizeof(payload);
            memcpy(entries[index].data, payload, sizeof(payload));
        }
        last_index = entries[batch_size - 1U].index;

        result = storage.begin(storage.context);
        if (result == SALTS_OK) {
            result = storage.write_hard_state(storage.context, 1U, 1U);
        }
        if (result == SALTS_OK) {
            result = storage.append_log(
                storage.context, entries, batch_size);
        }
        if (result == SALTS_OK) {
            result = storage.write_commit_index(
                storage.context, last_index);
        }
        if (result == SALTS_OK) {
            result = storage.commit(storage.context);
        } else {
            (void)storage.rollback(storage.context);
        }
    }
    end = bench_now_ns();

    if (result == SALTS_OK) {
        *out_ns_per_op =
            (double)(end - start) / (double)BENCH_PROPOSALS;
    }
    if (wal != NULL) {
        int close_result = tr_raft_wal_storage_close(wal);
        if (result == SALTS_OK) {
            result = close_result;
        }
    }

    (void)salts_fs_unlink(wal_path);
    (void)salts_fs_unlink(lock_path);
    return result;
}

static int bench_wal_path(double *out_ns_per_op)
{
    char prefix[256];
    char wal_path[320];
    char lock_path[320];
    tr_raft_wal_storage_config_t config;
    tr_raft_wal_storage_t *wal = NULL;
    tr_raft_storage_t storage;
    int result;

    if (out_ns_per_op == NULL) {
        return SALTS_EINVAL;
    }
    snprintf(prefix, sizeof(prefix),
             "/tmp/turboraft-proposal-bench-%ld",
             (long)getpid());
    snprintf(wal_path, sizeof(wal_path), "%s.00000001.wal", prefix);
    snprintf(lock_path, sizeof(lock_path), "%s.lock", prefix);
    (void)salts_fs_unlink(wal_path);
    (void)salts_fs_unlink(lock_path);

    memset(&config, 0, sizeof(config));
    config.path_prefix = prefix;
    config.segment_bytes = TR_RAFT_WAL_DEFAULT_SEGMENT_BYTES;
    config.max_transaction_bytes =
        TR_RAFT_WAL_DEFAULT_TRANSACTION_BYTES;
    config.max_segments = 2U;
    config.max_log_entries = BENCH_PROPOSALS + 16U;
    config.max_snapshot_bytes = 1024U;
    config.create_if_missing = true;

    result = tr_raft_wal_storage_open(&config, &wal);
    if (result == SALTS_OK) {
        result = tr_raft_wal_storage_bind(wal, &storage);
    }
    if (result == SALTS_OK) {
        result = bench_service_path(&storage, out_ns_per_op);
    }
    if (wal != NULL) {
        int close_result = tr_raft_wal_storage_close(wal);
        if (result == SALTS_OK) {
            result = close_result;
        }
    }

    (void)salts_fs_unlink(wal_path);
    (void)salts_fs_unlink(lock_path);
    return result;
}

int main(void)
{
    bench_memory_storage_t memory;
    tr_raft_storage_t storage;
    double core_ns = 0.0;
    double service_ns = 0.0;
    double wal_ns = 0.0;
    double wal_batch4_ns = 0.0;
    double wal_batch8_ns = 0.0;
    double wal_batch16_ns = 0.0;
    int result;

    result = bench_core_path(&core_ns);
    if (result != SALTS_OK) {
        fprintf(stderr, "core benchmark failed: %d\n", result);
        return 10;
    }

    bench_memory_storage_adapter(&memory, &storage);
    result = bench_service_path(&storage, &service_ns);
    if (result != SALTS_OK) {
        fprintf(stderr, "service benchmark failed: %d\n", result);
        return 20;
    }

    result = bench_wal_path(&wal_ns);
    if (result != SALTS_OK) {
        fprintf(stderr, "wal benchmark failed: %d\n", result);
        return 30;
    }
    result = bench_wal_batch_path(4U, &wal_batch4_ns);
    if (result != SALTS_OK) {
        fprintf(stderr, "wal batch4 benchmark failed: %d\n", result);
        return 31;
    }
    result = bench_wal_batch_path(8U, &wal_batch8_ns);
    if (result != SALTS_OK) {
        fprintf(stderr, "wal batch8 benchmark failed: %d\n", result);
        return 32;
    }
    result = bench_wal_batch_path(16U, &wal_batch16_ns);
    if (result != SALTS_OK) {
        fprintf(stderr, "wal batch16 benchmark failed: %d\n", result);
        return 33;
    }

    printf("boundary,operations,ns_per_op,ops_per_sec\n");
    printf("core_propose_advance,%u,%.2f,%.2f\n",
           BENCH_PROPOSALS, core_ns,
           core_ns != 0.0 ? 1000000000.0 / core_ns : 0.0);
    printf("service_memory,%u,%.2f,%.2f\n",
           BENCH_PROPOSALS, service_ns,
           service_ns != 0.0 ? 1000000000.0 / service_ns : 0.0);
    printf("service_wal_fsync,%u,%.2f,%.2f\n",
           BENCH_PROPOSALS, wal_ns,
           wal_ns != 0.0 ? 1000000000.0 / wal_ns : 0.0);
    printf("wal_vs_memory_ratio,1,%.2f,0\n",
           service_ns != 0.0 ? wal_ns / service_ns : 0.0);
    printf("wal_batch4,%u,%.2f,%.2f\n",
           BENCH_PROPOSALS, wal_batch4_ns,
           wal_batch4_ns != 0.0 ? 1000000000.0 / wal_batch4_ns : 0.0);
    printf("wal_batch8,%u,%.2f,%.2f\n",
           BENCH_PROPOSALS, wal_batch8_ns,
           wal_batch8_ns != 0.0 ? 1000000000.0 / wal_batch8_ns : 0.0);
    printf("wal_batch16,%u,%.2f,%.2f\n",
           BENCH_PROPOSALS, wal_batch16_ns,
           wal_batch16_ns != 0.0 ? 1000000000.0 / wal_batch16_ns : 0.0);
    printf("wal_batch4_speedup,1,%.2f,0\n",
           wal_batch4_ns != 0.0 ? wal_ns / wal_batch4_ns : 0.0);
    printf("wal_batch8_speedup,1,%.2f,0\n",
           wal_batch8_ns != 0.0 ? wal_ns / wal_batch8_ns : 0.0);
    printf("wal_batch16_speedup,1,%.2f,0\n",
           wal_batch16_ns != 0.0 ? wal_ns / wal_batch16_ns : 0.0);
    return 0;
}
