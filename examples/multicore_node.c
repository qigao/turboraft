#include "multicore_node.h"
#include <turboraft/raft_wal_storage.h>
#include <salts/thread.h>
#include <salts/clock.h>
#include <cmeta_error.h>
#include <fmt.h>
#include <tlog.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

typedef struct counter_group {
    tr_raft_wal_storage_t *wal;
    uint64_t count;
} counter_group_t;
typedef struct counter_node {
    const tr_raft_node_settings_t *settings;
    counter_group_t *groups;
    cmeta_mutex_t mutex;
    int close_error;
} counter_node_t;

static size_t counter_index(counter_node_t *node, uint64_t id)
{
    size_t i;
    for (i = 0; i < node->settings->runtime.group_count; ++i)
        if (node->settings->groups[i].group_id == id) break;
    return i;
}

static int counter_apply(void *context, const tr_raft_entry_t *entries, size_t count)
{
    counter_group_t *g = context;
    size_t i;
    if (UINT64_MAX - g->count < count) return SALTS_ERANGE;
    for (i = 0; i < count; ++i)
        if (entries[i].data_length != 1U || entries[i].data[0] != 1U) return SALTS_EPROTO;
    /* This demo rebuilds the in-memory counter from the committed WAL at each
     * startup. No separately persisted applied index can get ahead of data. */
    g->count += count;
    return SALTS_OK;
}

static int counter_send(void *context, const tr_raft_message_t *message)
{
    (void)context; (void)message;
    /* A single-voter Core must never emit remote messages. */
    return SALTS_EPROTO;
}

static int counter_open(void *context, tr_raft_owner_t *owner, uint64_t id,
                         tr_raft_service_t **out)
{
    counter_node_t *node = context;
    const size_t index = counter_index(node, id);
    counter_group_t *g = &node->groups[index];
    const tr_raft_node_group_config_t *spec = &node->settings->groups[index];
    tr_raft_wal_storage_config_t wal = {0};
    tr_raft_wal_recovery_t recovered = {0};
    tr_raft_service_config_t config = {0};
    int result;
    (void)owner;
    *out = NULL;
    wal.path_prefix = spec->storage_path;
    wal.segment_bytes = TR_RAFT_WAL_DEFAULT_SEGMENT_BYTES;
    wal.max_transaction_bytes = TR_RAFT_WAL_DEFAULT_TRANSACTION_BYTES;
    wal.max_live_segments = 4U;
    wal.max_log_entries = spec->core.max_log_entries;
    wal.max_snapshot_bytes = TR_RAFT_WAL_DEFAULT_MAX_SNAPSHOT_BYTES;
    wal.create_if_missing = true;
    result = tr_raft_wal_storage_open(&wal, &g->wal);
    if (result == SALTS_OK) result = tr_raft_wal_storage_load(g->wal, &recovered);
    if (result == SALTS_OK && recovered.snapshot_index != 0U) result = SALTS_ENOTSUP;
    if (result == SALTS_OK) {
        config.core = spec->core;
        config.core.initial_term = recovered.term;
        config.core.initial_vote = recovered.voted_for;
        config.core.initial_log_entries = recovered.entry_count != 0U ? recovered.entries : NULL;
        config.core.initial_log_entry_count = recovered.entry_count;
        config.core.initial_commit_index = recovered.commit_index;
        config.core.initial_applied_index = 0U;
        result = tr_raft_wal_storage_bind(g->wal, &config.storage);
    }
    if (result == SALTS_OK) {
        config.transport.enqueue = counter_send;
        config.state_machine.context = g;
        config.state_machine.apply_batch = counter_apply;
        result = tr_raft_service_create(&config, out);
        if (result == SALTS_OK) result = tr_raft_service_poll(*out);
    }
    tr_raft_wal_recovery_destroy(&recovered);
    if (result != SALTS_OK) {
        tr_raft_service_destroy(*out);
        *out = NULL;
        if (g->wal != NULL) (void)tr_raft_wal_storage_close(g->wal);
        g->wal = NULL;
    }
    return result;
}

static void counter_close(void *context, tr_raft_owner_t *owner, uint64_t id)
{
    counter_node_t *node = context;
    counter_group_t *g = &node->groups[counter_index(node, id)];
    int result = tr_raft_wal_storage_close(g->wal);
    (void)owner;
    g->wal = NULL;
    cmeta_mutex_lock(&node->mutex);
    if (result != SALTS_OK) node->close_error = result;
    cmeta_mutex_unlock(&node->mutex);
}

static int counter_take(tr_raft_multicore_t *runtime, uint64_t id,
                         tr_raft_multicore_completion_t *completion)
{
    const uint64_t deadline = cmeta_monotonic_ms() + 30000U;
    int result;
    do {
        result = tr_raft_multicore_take(runtime, id, completion);
        if (result != SALTS_ENOENT) return result;
        cmeta_sleep_ms(1U);
    } while (cmeta_monotonic_ms() < deadline);
    return SALTS_ETIMEDOUT;
}

int multicore_node_run(const tr_raft_node_settings_t *settings, uint64_t *counts, size_t count)
{
    counter_node_t node = {0};
    tr_raft_multicore_t *runtime = NULL;
    tr_raft_multicore_factory_t factory = {0};
    tr_raft_multicore_request_t request = {0};
    tr_raft_multicore_completion_t completion = {0};
    int result = tr_raft_node_settings_validate(settings);
    size_t i;
    if (result != SALTS_OK) return result;
    if (counts == NULL || count != settings->runtime.group_count) return SALTS_EINVAL;
    if (settings->network_enabled) return SALTS_ENOTSUP;
    node.settings = settings;
    node.groups = calloc(count, sizeof(*node.groups));
    cmeta_mutex_init(&node.mutex);
    if (node.groups == NULL || node.mutex == NULL) { result = SALTS_ENOMEM; goto done; }
    factory.context = &node;
    factory.group_open = counter_open;
    factory.group_close = counter_close;
    result = tr_raft_multicore_create(&settings->runtime, &factory, &runtime);
    for (i = 0; result == SALTS_OK && i < count; ++i) {
        const uint64_t id = settings->groups[i].group_id;
        const uint64_t deadline = cmeta_monotonic_ms() + 30000U;
        bool leader = false;
        request.operation = TR_RAFT_MULTICORE_STATUS;
        while (result == SALTS_OK && !leader) {
            result = tr_raft_multicore_submit(runtime, id, &request);
            if (result == SALTS_OK) result = counter_take(runtime, id, &completion);
            if (result == SALTS_OK) result = completion.result;
            if (result != SALTS_OK) break;
            leader = completion.value.status.core.role == TR_RAFT_LEADER;
            if (cmeta_monotonic_ms() >= deadline) result = SALTS_ETIMEDOUT;
            if (!leader) cmeta_sleep_ms(1U);
        }
        if (result == SALTS_OK) {
            if (completion.value.status.core.last_log_index == UINT64_MAX) { result = SALTS_ERANGE; break; }
            request.value.proposal.command_id = completion.value.status.core.last_log_index + 1U;
            request.operation = TR_RAFT_MULTICORE_PROPOSE;
            request.value.proposal.data[0] = 1U;
            request.value.proposal.size = 1U;
            result = tr_raft_multicore_submit(runtime, id, &request);
        }
    }
    /* All groups can execute their proposals concurrently. */
    for (i = 0; result == SALTS_OK && i < count; ++i) {
        result = counter_take(runtime, settings->groups[i].group_id, &completion);
        if (result == SALTS_OK) result = completion.result;
    }
    if (runtime != NULL) {
        int stopped = tr_raft_multicore_stop(runtime);
        if (result == SALTS_OK) result = stopped;
    }
    if (result == SALTS_OK) result = node.close_error;
    if (result == SALTS_OK)
        for (i = 0; i < count; ++i) counts[i] = node.groups[i].count;
done:
    tr_raft_multicore_destroy(runtime);
    cmeta_mutex_destroy(&node.mutex);
    free(node.groups);
    return result;
}

#ifndef TURBORAFT_MULTICORE_DEMO_NO_MAIN
int main(int argc, char **argv)
{
    tr_raft_node_config_t *document = NULL;
    const tr_raft_node_settings_t *settings;
    DataBindError error = DATA_BIND_ERROR_INIT;
    uint64_t *counts = NULL;
    int result;
    if (argc != 2) { puts("Usage: turboraft_multicore_node <node.json>"); return 2; }
    result = tr_raft_node_config_load(argv[1], &document, &error);
    if (result == SALTS_OK) {
        settings = tr_raft_node_config_settings(document);
        counts = calloc(settings->runtime.group_count, sizeof(*counts));
        result = counts == NULL ? SALTS_ENOMEM : multicore_node_run(settings, counts, settings->runtime.group_count);
        if (result == SALTS_OK) {
            for (size_t i = 0; i < settings->runtime.group_count; ++i) {
                tstr line = tstr_format("group={} owner={} durable_count={}", settings->groups[i].group_id,
                                       settings->runtime.groups[i].owner_index, counts[i]);
                if (line == NULL) { result = SALTS_ENOMEM; break; }
                puts(line);
                tstr_free(line);
            }
        }
    }
    if (result != SALTS_OK) {
        TLOG_ERRORF("multicore counter startup/run failed: status={}; check node configuration and WAL paths", result);
    }
    free(counts);
    tr_raft_node_config_destroy(document);
    return result == SALTS_OK ? 0 : 1;
}
#endif
