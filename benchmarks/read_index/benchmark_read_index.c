#include <turboraft/raft_service.h>
#include <turboraft/raft_wal_storage.h>

#include <cmeta_error.h>

#include <glob.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

enum {
    BENCH_READ_SAMPLES = 256U,
    BENCH_WARMUP_SAMPLES = 8U,
    BENCH_PAYLOAD_BYTES = 64U
};

typedef struct bench_distribution {
    const char *phase;
    size_t sample_count;
    uint64_t samples_ns[BENCH_READ_SAMPLES];
    uint64_t p50_ns;
    uint64_t p95_ns;
    uint64_t p99_ns;
    uint64_t max_ns;
    double mean_ns;
} bench_distribution_t;

typedef struct bench_fixture {
    char prefix[256];
    tr_raft_wal_storage_t *wal;
    tr_raft_storage_t storage;
    tr_raft_service_t *service;
    tr_raft_term_t term;
    tr_raft_index_t next_index;
} bench_fixture_t;

static const tr_raft_node_id_t bench_voters[] = {1U, 2U, 3U};

static uint64_t bench_now_ns(void)
{
    struct timespec ts;

    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        return 0U;
    }
    return (uint64_t)ts.tv_sec * UINT64_C(1000000000) +
           (uint64_t)ts.tv_nsec;
}

static int bench_u64_compare(const void *left, const void *right)
{
    const uint64_t a = *(const uint64_t *)left;
    const uint64_t b = *(const uint64_t *)right;

    return a < b ? -1 : a > b ? 1 : 0;
}

static uint64_t bench_nearest_rank(const uint64_t *sorted,
                                   size_t count,
                                   size_t percentile)
{
    size_t rank;

    if (sorted == NULL || count == 0U ||
        percentile == 0U || percentile > 100U) {
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

static int bench_finalize_distribution(bench_distribution_t *distribution)
{
    uint64_t sorted[BENCH_READ_SAMPLES];
    long double total = 0.0L;
    size_t index;

    if (distribution == NULL ||
        distribution->sample_count != BENCH_READ_SAMPLES) {
        return SALTS_EINVAL;
    }
    memcpy(sorted, distribution->samples_ns, sizeof(sorted));
    qsort(sorted, BENCH_READ_SAMPLES, sizeof(sorted[0]),
          bench_u64_compare);
    for (index = 0U; index < BENCH_READ_SAMPLES; ++index) {
        total += (long double)distribution->samples_ns[index];
    }
    distribution->p50_ns =
        bench_nearest_rank(sorted, BENCH_READ_SAMPLES, 50U);
    distribution->p95_ns =
        bench_nearest_rank(sorted, BENCH_READ_SAMPLES, 95U);
    distribution->p99_ns =
        bench_nearest_rank(sorted, BENCH_READ_SAMPLES, 99U);
    distribution->max_ns = sorted[BENCH_READ_SAMPLES - 1U];
    distribution->mean_ns =
        (double)(total / (long double)BENCH_READ_SAMPLES);
    return SALTS_OK;
}

static void bench_cleanup_prefix(const char *prefix)
{
    char pattern[320];
    glob_t matches;
    size_t index;
    int written;

    if (prefix == NULL) {
        return;
    }
    written = snprintf(pattern, sizeof(pattern), "%s*", prefix);
    if (written < 0 || (size_t)written >= sizeof(pattern)) {
        return;
    }
    memset(&matches, 0, sizeof(matches));
    if (glob(pattern, 0, NULL, &matches) != 0) {
        return;
    }
    for (index = 0U; index < matches.gl_pathc; ++index) {
        (void)unlink(matches.gl_pathv[index]);
    }
    globfree(&matches);
}

static int bench_transport_enqueue(void *context,
                                   const tr_raft_message_t *message)
{
    (void)context;
    return message == NULL ? SALTS_EINVAL : SALTS_OK;
}

static int bench_apply(void *context,
                       const tr_raft_entry_t *entries,
                       size_t count)
{
    (void)context;
    return entries != NULL && count != 0U ? SALTS_OK : SALTS_EINVAL;
}

static void bench_core_config(tr_raft_core_config_t *config)
{
    memset(config, 0, sizeof(*config));
    config->self_id = 1U;
    config->voters = bench_voters;
    config->voter_count = 3U;
    config->heartbeat_ticks = 2U;
    config->election_min_ticks = 5U;
    config->election_max_ticks = 10U;
    config->initial_election_timeout_ticks = 5U;
    config->max_log_entries =
        2U * BENCH_READ_SAMPLES + 2U * BENCH_WARMUP_SAMPLES + 64U;
    config->max_pending_reads = 8U;
}

static int bench_elect(bench_fixture_t *fixture)
{
    tr_raft_tick_t tick = {5U, 7U};
    tr_raft_message_t response;
    tr_raft_service_status_t status;
    int result;

    result = tr_raft_service_tick(fixture->service, &tick);
    if (result != SALTS_OK) {
        return result;
    }

    memset(&response, 0, sizeof(response));
    response.type = TR_RAFT_MSG_PRE_VOTE_RESPONSE;
    response.from = 2U;
    response.to = 1U;
    response.campaign_term = 1U;
    response.granted = true;
    result = tr_raft_service_step(fixture->service, &response);
    if (result != SALTS_OK) {
        return result;
    }

    response.type = TR_RAFT_MSG_VOTE_RESPONSE;
    response.term = 1U;
    result = tr_raft_service_step(fixture->service, &response);
    if (result != SALTS_OK) {
        return result;
    }
    result = tr_raft_service_status(fixture->service, &status);
    if (result != SALTS_OK || status.core.role != TR_RAFT_LEADER) {
        return result == SALTS_OK ? SALTS_EPROTO : result;
    }
    fixture->term = status.core.term;
    fixture->next_index = status.core.last_log_index + 1U;
    return SALTS_OK;
}

static int bench_commit_write(bench_fixture_t *fixture)
{
    uint8_t payload[BENCH_PAYLOAD_BYTES];
    tr_raft_proposal_t proposal;
    tr_raft_message_t response;
    tr_raft_index_t index = fixture->next_index;
    int result;

    memset(payload, (int)(index & 0xffU), sizeof(payload));
    memset(&proposal, 0, sizeof(proposal));
    proposal.command_id = index;
    proposal.data = payload;
    proposal.data_length = sizeof(payload);

    result = tr_raft_service_propose(fixture->service, &proposal);
    if (result != SALTS_OK) {
        return result;
    }

    memset(&response, 0, sizeof(response));
    response.type = TR_RAFT_MSG_APPEND_RESPONSE;
    response.from = 2U;
    response.to = 1U;
    response.term = fixture->term;
    response.granted = true;
    response.previous_log_index = index - 1U;
    response.match_index = index;
    result = tr_raft_service_step(fixture->service, &response);
    if (result == SALTS_OK) {
        fixture->next_index = index + 1U;
    }
    return result;
}

static int bench_fixture_open(const char *label,
                              bench_fixture_t *fixture)
{
    tr_raft_wal_storage_config_t wal_config;
    tr_raft_service_config_t service_config;
    int result;

    if (label == NULL || fixture == NULL) {
        return SALTS_EINVAL;
    }
    memset(fixture, 0, sizeof(*fixture));
    (void)snprintf(fixture->prefix, sizeof(fixture->prefix),
                   "/tmp/turboraft-read-index-%s-%ld",
                   label, (long)getpid());
    bench_cleanup_prefix(fixture->prefix);

    memset(&wal_config, 0, sizeof(wal_config));
    wal_config.path_prefix = fixture->prefix;
    wal_config.segment_bytes = TR_RAFT_WAL_MIN_SEGMENT_BYTES;
    wal_config.max_transaction_bytes = 32U * 1024U;
    wal_config.max_live_segments = 16U;
    wal_config.max_log_entries =
        2U * BENCH_READ_SAMPLES + 2U * BENCH_WARMUP_SAMPLES + 64U;
    wal_config.max_snapshot_bytes = 1024U;
    wal_config.create_if_missing = true;

    result = tr_raft_wal_storage_open(&wal_config, &fixture->wal);
    if (result == SALTS_OK) {
        result = tr_raft_wal_storage_bind(
            fixture->wal, &fixture->storage);
    }
    if (result != SALTS_OK) {
        bench_cleanup_prefix(fixture->prefix);
        return result;
    }

    memset(&service_config, 0, sizeof(service_config));
    bench_core_config(&service_config.core);
    service_config.storage = fixture->storage;
    service_config.transport.enqueue = bench_transport_enqueue;
    service_config.state_machine.apply_batch = bench_apply;
    result = tr_raft_service_create(&service_config, &fixture->service);
    if (result == SALTS_OK) {
        result = bench_elect(fixture);
    }
    if (result == SALTS_OK) {
        result = bench_commit_write(fixture);
    }
    if (result != SALTS_OK) {
        if (fixture->service != NULL) {
            tr_raft_service_destroy(fixture->service);
            fixture->service = NULL;
        }
        if (fixture->wal != NULL) {
            (void)tr_raft_wal_storage_close(fixture->wal);
            fixture->wal = NULL;
        }
        bench_cleanup_prefix(fixture->prefix);
    }
    return result;
}

static int bench_fixture_close(bench_fixture_t *fixture)
{
    int result = SALTS_OK;

    if (fixture == NULL) {
        return SALTS_EINVAL;
    }
    if (fixture->service != NULL) {
        tr_raft_service_destroy(fixture->service);
        fixture->service = NULL;
    }
    if (fixture->wal != NULL) {
        result = tr_raft_wal_storage_close(fixture->wal);
        fixture->wal = NULL;
    }
    bench_cleanup_prefix(fixture->prefix);
    return result;
}

static int bench_complete_read(bench_fixture_t *fixture,
                               uint64_t context_id)
{
    tr_raft_message_t response;
    tr_raft_read_state_t read_state;
    int result;

    memset(&response, 0, sizeof(response));
    response.type = TR_RAFT_MSG_READ_INDEX_RESPONSE;
    response.from = 2U;
    response.to = 1U;
    response.term = fixture->term;
    response.context_id = context_id;
    result = tr_raft_service_step(fixture->service, &response);
    if (result != SALTS_OK) {
        return result;
    }
    result = tr_raft_service_take_read_state(
        fixture->service, &read_state);
    if (result != SALTS_OK || read_state.context_id != context_id) {
        return result == SALTS_OK ? SALTS_EPROTO : result;
    }
    return SALTS_OK;
}

static int bench_one_read(bench_fixture_t *fixture,
                          uint64_t context_id,
                          int interleave_write,
                          uint64_t *out_latency_ns)
{
    uint64_t start;
    uint64_t end;
    int result;

    if (fixture == NULL || out_latency_ns == NULL) {
        return SALTS_EINVAL;
    }
    start = bench_now_ns();
    result = tr_raft_service_read_index(
        fixture->service, context_id);
    if (result == SALTS_OK && interleave_write) {
        result = bench_commit_write(fixture);
    }
    if (result == SALTS_OK) {
        result = bench_complete_read(fixture, context_id);
    }
    end = bench_now_ns();
    if (result == SALTS_OK) {
        *out_latency_ns = end - start;
    }
    return result;
}

static int bench_read_phase(const char *label,
                            const char *phase,
                            int interleave_write,
                            bench_distribution_t *distribution)
{
    bench_fixture_t fixture;
    size_t index;
    int result;

    if (label == NULL || phase == NULL || distribution == NULL) {
        return SALTS_EINVAL;
    }
    memset(distribution, 0, sizeof(*distribution));
    distribution->phase = phase;

    result = bench_fixture_open(label, &fixture);
    for (index = 0U;
         result == SALTS_OK && index < BENCH_WARMUP_SAMPLES;
         ++index) {
        uint64_t ignored = 0U;

        result = bench_one_read(
            &fixture, UINT64_C(100000) + index,
            interleave_write, &ignored);
    }
    for (index = 0U;
         result == SALTS_OK && index < BENCH_READ_SAMPLES;
         ++index) {
        result = bench_one_read(
            &fixture, UINT64_C(200000) + index,
            interleave_write,
            &distribution->samples_ns[index]);
        if (result == SALTS_OK) {
            ++distribution->sample_count;
        }
    }
    if (result == SALTS_OK) {
        result = bench_finalize_distribution(distribution);
    }
    {
        int close_result = bench_fixture_close(&fixture);
        if (result == SALTS_OK) {
            result = close_result;
        }
    }
    return result;
}

static int bench_write_raw_samples(
    const char *path,
    const bench_distribution_t *distributions,
    size_t count)
{
    FILE *output;
    size_t phase_index;

    if (path == NULL || path[0] == '\0') {
        return SALTS_OK;
    }
    output = fopen(path, "w");
    if (output == NULL) {
        return SALTS_EIO;
    }
    fprintf(output, "phase,sample_index,sample_ns\n");
    for (phase_index = 0U; phase_index < count; ++phase_index) {
        size_t sample_index;
        const bench_distribution_t *distribution =
            &distributions[phase_index];

        for (sample_index = 0U;
             sample_index < distribution->sample_count;
             ++sample_index) {
            fprintf(output, "%s,%zu,%" PRIu64 "\n",
                    distribution->phase, sample_index,
                    distribution->samples_ns[sample_index]);
        }
    }
    if (fclose(output) != 0) {
        return SALTS_EIO;
    }
    return SALTS_OK;
}

int main(void)
{
    bench_distribution_t distributions[2];
    const char *raw_path =
        getenv("TURBORAFT_READ_INDEX_RAW_SAMPLES");
    int result;

    result = bench_read_phase(
        "idle", "read_index_idle", 0, &distributions[0]);
    if (result != SALTS_OK) {
        fprintf(stderr, "idle ReadIndex benchmark failed: %d\n", result);
        return 10;
    }
    result = bench_read_phase(
        "write", "read_index_write_load", 1, &distributions[1]);
    if (result != SALTS_OK) {
        fprintf(stderr, "write-load ReadIndex benchmark failed: %d\n",
                result);
        return 11;
    }

    printf("phase,samples,p50_ns,p95_ns,p99_ns,max_ns,mean_ns\n");
    for (size_t index = 0U; index < 2U; ++index) {
        const bench_distribution_t *distribution =
            &distributions[index];
        printf("%s,%zu,%" PRIu64 ",%" PRIu64 ",%" PRIu64
               ",%" PRIu64 ",%.2f\n",
               distribution->phase,
               distribution->sample_count,
               distribution->p50_ns,
               distribution->p95_ns,
               distribution->p99_ns,
               distribution->max_ns,
               distribution->mean_ns);
    }

    result = bench_write_raw_samples(raw_path, distributions, 2U);
    if (result != SALTS_OK) {
        fprintf(stderr, "cannot write raw ReadIndex samples: %d\n",
                result);
        return 12;
    }
    return 0;
}
