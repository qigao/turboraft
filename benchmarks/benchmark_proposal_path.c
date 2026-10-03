#include <turboraft/raft_service.h>
#include <turboraft/raft_wal_storage.h>

#include <salts_error.h>
#include <salts_fs.h>

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

enum {
    BENCH_PROPOSALS = 512U,
    BENCH_PAYLOAD_BYTES = 64U
};

typedef struct bench_latency_distribution {
    size_t batch_size;
    size_t sample_count;
    uint64_t samples_ns[BENCH_PROPOSALS];
    uint64_t p50_ns;
    uint64_t p95_ns;
    uint64_t p99_ns;
    uint64_t max_ns;
    double ns_per_op;
    double ops_per_sec;
} bench_latency_distribution_t;

static int bench_u64_compare(const void *left, const void *right)
{
    const uint64_t a = *(const uint64_t *)left;
    const uint64_t b = *(const uint64_t *)right;

    return a < b ? -1 : a > b ? 1 : 0;
}

static uint64_t bench_nearest_rank(
    const uint64_t *sorted,
    size_t count,
    size_t percentile)
{
    size_t rank;

    if (sorted == NULL || count == 0U || percentile == 0U ||
        percentile > 100U) {
        return 0U;
    }
    rank = (percentile * count + 99U) / 100U;
    if (rank == 0U) {
        rank = 1U;
    } else if (rank > count) {
        rank = count;
    }
    return sorted[rank - 1U];
}

static int bench_finalize_distribution(
    bench_latency_distribution_t *distribution)
{
    uint64_t sorted[BENCH_PROPOSALS];
    long double total_ns = 0.0L;
    size_t index;
    size_t operations;

    if (distribution == NULL || distribution->batch_size == 0U ||
        distribution->sample_count == 0U ||
        distribution->sample_count > BENCH_PROPOSALS) {
        return SALTS_EINVAL;
    }

    memcpy(sorted, distribution->samples_ns,
           distribution->sample_count * sizeof(sorted[0]));
    qsort(sorted, distribution->sample_count, sizeof(sorted[0]),
          bench_u64_compare);
    for (index = 0U; index < distribution->sample_count; ++index) {
        total_ns += (long double)distribution->samples_ns[index];
    }
    operations = distribution->sample_count * distribution->batch_size;
    distribution->p50_ns =
        bench_nearest_rank(sorted, distribution->sample_count, 50U);
    distribution->p95_ns =
        bench_nearest_rank(sorted, distribution->sample_count, 95U);
    distribution->p99_ns =
        bench_nearest_rank(sorted, distribution->sample_count, 99U);
    distribution->max_ns = sorted[distribution->sample_count - 1U];
    distribution->ns_per_op =
        (double)(total_ns / (long double)operations);
    distribution->ops_per_sec =
        distribution->ns_per_op > 0.0
            ? 1000000000.0 / distribution->ns_per_op
            : 0.0;
    return SALTS_OK;
}

static int bench_write_raw_samples(
    const char *path,
    const bench_latency_distribution_t *distributions,
    size_t distribution_count)
{
    FILE *output;
    size_t distribution_index;

    if (path == NULL || path[0] == '\0') {
        return SALTS_OK;
    }
    if (distributions == NULL || distribution_count == 0U) {
        return SALTS_EINVAL;
    }
    output = fopen(path, "w");
    if (output == NULL) {
        return SALTS_EIO;
    }
    fprintf(output, "boundary,batch_size,sample_index,sample_ns\n");
    for (distribution_index = 0U;
         distribution_index < distribution_count;
         ++distribution_index) {
        const bench_latency_distribution_t *distribution =
            &distributions[distribution_index];
        size_t sample_index;

        for (sample_index = 0U;
             sample_index < distribution->sample_count;
             ++sample_index) {
            fprintf(output, "wal_fsync,%zu,%zu,%" PRIu64 "\n",
                    distribution->batch_size, sample_index,
                    distribution->samples_ns[sample_index]);
        }
    }
    if (fclose(output) != 0) {
        return SALTS_EIO;
    }
    return SALTS_OK;
}

typedef struct bench_memory_storage {
    size_t begin_count;
    size_t commit_count;
} bench_memory_storage_t;

static void bench_unlink_if_exists(const char *path)
{
    if (path != NULL &&
        salts_fs_access(path, SALTS_FS_ACCESS_EXISTS) == SALTS_OK) {
        (void)salts_fs_unlink(path);
    }
}

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

static int bench_service_batch_path(
    const tr_raft_storage_t *storage,
    size_t batch_size,
    double *out_ns_per_op)
{
    enum { MAX_BATCH = TR_RAFT_MAX_PROPOSAL_BATCH };
    tr_raft_service_t *service = NULL;
    tr_raft_proposal_t proposals[MAX_BATCH];
    uint8_t payloads[MAX_BATCH][BENCH_PAYLOAD_BYTES];
    uint64_t command_id = 1U;
    size_t batch;
    uint64_t start;
    uint64_t end;
    int result;

    if (storage == NULL || out_ns_per_op == NULL ||
        batch_size == 0U || batch_size > MAX_BATCH ||
        BENCH_PROPOSALS % batch_size != 0U) {
        return SALTS_EINVAL;
    }

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
    for (batch = 0U;
         result == SALTS_OK && batch < BENCH_PROPOSALS / batch_size;
         ++batch) {
        size_t index;

        memset(proposals, 0, sizeof(proposals));
        for (index = 0U; index < batch_size; ++index) {
            memset(payloads[index], 0x4d, sizeof(payloads[index]));
            proposals[index].command_id = command_id++;
            proposals[index].data = payloads[index];
            proposals[index].data_length = sizeof(payloads[index]);
        }
        result = tr_raft_service_propose_batch(
            service, proposals, batch_size);
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

static int bench_wal_batch_path(
    size_t batch_size,
    bench_latency_distribution_t *out_distribution)
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
    int result = SALTS_OK;

    if (out_distribution == NULL || batch_size == 0U ||
        batch_size > MAX_BATCH || BENCH_PROPOSALS % batch_size != 0U) {
        return SALTS_EINVAL;
    }
    memset(out_distribution, 0, sizeof(*out_distribution));
    out_distribution->batch_size = batch_size;

    snprintf(prefix, sizeof(prefix),
             "/tmp/turboraft-groupcommit-bench-%ld-%zu",
             (long)getpid(), batch_size);
    snprintf(wal_path, sizeof(wal_path), "%s.00000001.wal", prefix);
    snprintf(lock_path, sizeof(lock_path), "%s.lock", prefix);
    bench_unlink_if_exists(wal_path);
    bench_unlink_if_exists(lock_path);

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

    for (batch = 0U;
         result == SALTS_OK && batch < BENCH_PROPOSALS / batch_size;
         ++batch) {
        size_t index;
        tr_raft_index_t last_index;
        uint64_t sample_start;
        uint64_t sample_end;

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

        sample_start = bench_now_ns();
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
        sample_end = bench_now_ns();
        if (result == SALTS_OK) {
            if (out_distribution->sample_count >= BENCH_PROPOSALS) {
                result = SALTS_ENOSPC;
            } else {
                out_distribution->samples_ns[
                    out_distribution->sample_count++] =
                    sample_end - sample_start;
            }
        }
    }

    if (result == SALTS_OK) {
        result = bench_finalize_distribution(out_distribution);
    }
    if (wal != NULL) {
        int close_result = tr_raft_wal_storage_close(wal);
        if (result == SALTS_OK) {
            result = close_result;
        }
    }

    bench_unlink_if_exists(wal_path);
    bench_unlink_if_exists(lock_path);
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
    bench_unlink_if_exists(wal_path);
    bench_unlink_if_exists(lock_path);

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

    bench_unlink_if_exists(wal_path);
    bench_unlink_if_exists(lock_path);
    return result;
}

static int bench_wal_service_batch_path(
    size_t batch_size,
    double *out_ns_per_op)
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
             "/tmp/turboraft-service-batch-bench-%ld-%zu",
             (long)getpid(), batch_size);
    snprintf(wal_path, sizeof(wal_path), "%s.00000001.wal", prefix);
    snprintf(lock_path, sizeof(lock_path), "%s.lock", prefix);
    bench_unlink_if_exists(wal_path);
    bench_unlink_if_exists(lock_path);

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
        result = bench_service_batch_path(
            &storage, batch_size, out_ns_per_op);
    }
    if (wal != NULL) {
        int close_result = tr_raft_wal_storage_close(wal);
        if (result == SALTS_OK) {
            result = close_result;
        }
    }

    bench_unlink_if_exists(wal_path);
    bench_unlink_if_exists(lock_path);
    return result;
}

int main(void)
{
    bench_memory_storage_t memory;
    tr_raft_storage_t storage;
    double core_ns = 0.0;
    double service_ns = 0.0;
    double wal_ns = 0.0;
    bench_latency_distribution_t wal_distributions[4];
    double service_batch4_ns = 0.0;
    double service_batch8_ns = 0.0;
    double service_batch16_ns = 0.0;
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
    result = bench_wal_batch_path(1U, &wal_distributions[0]);
    if (result != SALTS_OK) {
        fprintf(stderr, "wal batch1 benchmark failed: %d\n", result);
        return 31;
    }
    result = bench_wal_batch_path(4U, &wal_distributions[1]);
    if (result != SALTS_OK) {
        fprintf(stderr, "wal batch4 benchmark failed: %d\n", result);
        return 32;
    }
    result = bench_wal_batch_path(8U, &wal_distributions[2]);
    if (result != SALTS_OK) {
        fprintf(stderr, "wal batch8 benchmark failed: %d\n", result);
        return 33;
    }
    result = bench_wal_batch_path(16U, &wal_distributions[3]);
    if (result != SALTS_OK) {
        fprintf(stderr, "wal batch16 benchmark failed: %d\n", result);
        return 34;
    }
    result = bench_wal_service_batch_path(4U, &service_batch4_ns);
    if (result != SALTS_OK) {
        fprintf(stderr, "service batch4 benchmark failed: %d\n", result);
        return 35;
    }
    result = bench_wal_service_batch_path(8U, &service_batch8_ns);
    if (result != SALTS_OK) {
        fprintf(stderr, "service batch8 benchmark failed: %d\n", result);
        return 36;
    }
    result = bench_wal_service_batch_path(16U, &service_batch16_ns);
    if (result != SALTS_OK) {
        fprintf(stderr, "service batch16 benchmark failed: %d\n", result);
        return 37;
    }

    printf("boundary,batch_size,samples,operations,ns_per_op,ops_per_sec,"
           "p50_sample_ns,p95_sample_ns,p99_sample_ns,max_sample_ns\n");
    printf("core_propose_advance,1,0,%u,%.2f,%.2f,0,0,0,0\n",
           BENCH_PROPOSALS, core_ns,
           core_ns != 0.0 ? 1000000000.0 / core_ns : 0.0);
    printf("service_memory,1,0,%u,%.2f,%.2f,0,0,0,0\n",
           BENCH_PROPOSALS, service_ns,
           service_ns != 0.0 ? 1000000000.0 / service_ns : 0.0);
    printf("service_wal_fsync,1,0,%u,%.2f,%.2f,0,0,0,0\n",
           BENCH_PROPOSALS, wal_ns,
           wal_ns != 0.0 ? 1000000000.0 / wal_ns : 0.0);
    printf("wal_vs_memory_ratio,1,0,1,%.2f,0,0,0,0,0\n",
           service_ns != 0.0 ? wal_ns / service_ns : 0.0);
    {
        size_t distribution_index;

        for (distribution_index = 0U;
             distribution_index < sizeof(wal_distributions) /
                                      sizeof(wal_distributions[0]);
             ++distribution_index) {
            const bench_latency_distribution_t *distribution =
                &wal_distributions[distribution_index];
            printf("wal_fsync,%zu,%zu,%zu,%.2f,%.2f,%" PRIu64
                   ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 "\n",
                   distribution->batch_size,
                   distribution->sample_count,
                   distribution->sample_count * distribution->batch_size,
                   distribution->ns_per_op,
                   distribution->ops_per_sec,
                   distribution->p50_ns,
                   distribution->p95_ns,
                   distribution->p99_ns,
                   distribution->max_ns);
        }
    }
    printf("wal_batch4_speedup,4,0,1,%.2f,0,0,0,0,0\n",
           wal_distributions[1].ns_per_op != 0.0
               ? wal_distributions[0].ns_per_op /
                     wal_distributions[1].ns_per_op
               : 0.0);
    printf("wal_batch8_speedup,8,0,1,%.2f,0,0,0,0,0\n",
           wal_distributions[2].ns_per_op != 0.0
               ? wal_distributions[0].ns_per_op /
                     wal_distributions[2].ns_per_op
               : 0.0);
    printf("wal_batch16_speedup,16,0,1,%.2f,0,0,0,0,0\n",
           wal_distributions[3].ns_per_op != 0.0
               ? wal_distributions[0].ns_per_op /
                     wal_distributions[3].ns_per_op
               : 0.0);
    printf("service_wal_batch4,4,0,%u,%.2f,%.2f,0,0,0,0\n",
           BENCH_PROPOSALS, service_batch4_ns,
           service_batch4_ns != 0.0
               ? 1000000000.0 / service_batch4_ns
               : 0.0);
    printf("service_wal_batch8,8,0,%u,%.2f,%.2f,0,0,0,0\n",
           BENCH_PROPOSALS, service_batch8_ns,
           service_batch8_ns != 0.0
               ? 1000000000.0 / service_batch8_ns
               : 0.0);
    printf("service_wal_batch16,16,0,%u,%.2f,%.2f,0,0,0,0\n",
           BENCH_PROPOSALS, service_batch16_ns,
           service_batch16_ns != 0.0
               ? 1000000000.0 / service_batch16_ns
               : 0.0);
    printf("service_batch4_speedup,4,0,1,%.2f,0,0,0,0,0\n",
           service_batch4_ns != 0.0 ? wal_ns / service_batch4_ns : 0.0);
    printf("service_batch8_speedup,8,0,1,%.2f,0,0,0,0,0\n",
           service_batch8_ns != 0.0 ? wal_ns / service_batch8_ns : 0.0);
    printf("service_batch16_speedup,16,0,1,%.2f,0,0,0,0,0\n",
           service_batch16_ns != 0.0 ? wal_ns / service_batch16_ns : 0.0);
    {
        const char *raw_samples_path =
            getenv("TURBORAFT_BENCHMARK_RAW_SAMPLES");

        result = bench_write_raw_samples(
            raw_samples_path, wal_distributions,
            sizeof(wal_distributions) / sizeof(wal_distributions[0]));
        if (result != SALTS_OK) {
            fprintf(stderr, "cannot write raw WAL samples: %d\n", result);
            return 38;
        }
    }

    return 0;
}
