#include "raft_service_wal_reload.h"

#include <salts_error.h>

#include <stdlib.h>
#include <string.h>

struct tr_raft_service_wal_reload {
    tr_raft_service_t *service;
    tr_raft_wal_storage_t *storage;
    tr_raft_node_id_t self_id;
    tr_raft_node_id_t voters[TR_RAFT_MAX_VOTERS];
    size_t voter_count;
    tr_raft_node_id_t learners[TR_RAFT_MAX_MEMBERS];
    size_t learner_count;
    uint32_t heartbeat_ticks;
    uint32_t election_min_ticks;
    uint32_t election_max_ticks;
    uint32_t initial_election_timeout_ticks;
    size_t max_log_entries;
    size_t max_inflight_append_requests;
};

int tr_raft_service_wal_reload_create(
    const tr_raft_service_wal_reload_config_t *config,
    tr_raft_service_wal_reload_t **out_reload)
{
    tr_raft_service_wal_reload_t *reload;
    size_t index;

    if (config == NULL || out_reload == NULL || config->service == NULL ||
        config->storage == NULL || config->self_id == 0U ||
        config->voters == NULL || config->voter_count == 0U ||
        config->voter_count > TR_RAFT_MAX_VOTERS ||
        config->learner_count > TR_RAFT_MAX_MEMBERS ||
        config->voter_count + config->learner_count > TR_RAFT_MAX_MEMBERS ||
        (config->learner_count != 0U && config->learners == NULL) ||
        (config->learner_count == 0U && config->learners != NULL) ||
        config->heartbeat_ticks == 0U ||
        config->election_min_ticks <= config->heartbeat_ticks ||
        config->election_min_ticks > config->election_max_ticks ||
        config->initial_election_timeout_ticks < config->election_min_ticks ||
        config->initial_election_timeout_ticks > config->election_max_ticks ||
        config->max_log_entries == 0U ||
        config->max_inflight_append_requests >
            TR_RAFT_MAX_INFLIGHT_APPEND_REQUESTS) {
        return SALTS_EINVAL;
    }
    for (index = 0U; index < config->voter_count; ++index) {
        if (config->voters[index] == 0U ||
            (index > 0U && config->voters[index - 1U] >=
                               config->voters[index])) {
            return SALTS_EINVAL;
        }
    }
    for (index = 0U; index < config->learner_count; ++index) {
        size_t voter_index;

        if (config->learners[index] == 0U ||
            (index > 0U && config->learners[index - 1U] >=
                              config->learners[index])) {
            return SALTS_EINVAL;
        }
        for (voter_index = 0U; voter_index < config->voter_count;
             ++voter_index) {
            if (config->learners[index] == config->voters[voter_index]) {
                return SALTS_EINVAL;
            }
        }
    }

    *out_reload = NULL;
    reload = (tr_raft_service_wal_reload_t *) calloc(
        1U, sizeof(*reload));
    if (reload == NULL) {
        return SALTS_ENOMEM;
    }
    reload->service = config->service;
    reload->storage = config->storage;
    reload->self_id = config->self_id;
    reload->voter_count = config->voter_count;
    memcpy(reload->voters, config->voters,
           config->voter_count * sizeof(reload->voters[0]));
    if (config->learner_count != 0U) {
        memcpy(reload->learners, config->learners,
               config->learner_count * sizeof(reload->learners[0]));
    }
    reload->learner_count = config->learner_count;
    reload->heartbeat_ticks = config->heartbeat_ticks;
    reload->election_min_ticks = config->election_min_ticks;
    reload->election_max_ticks = config->election_max_ticks;
    reload->initial_election_timeout_ticks =
        config->initial_election_timeout_ticks;
    reload->max_log_entries = config->max_log_entries;
    reload->max_inflight_append_requests =
        config->max_inflight_append_requests;
    *out_reload = reload;
    return SALTS_OK;
}

void tr_raft_service_wal_reload_destroy(tr_raft_service_wal_reload_t *reload)
{
    free(reload);
}

int tr_raft_service_wal_reload_runtime(
    void *context,
    tr_raft_index_t snapshot_index,
    tr_raft_term_t snapshot_term)
{
    tr_raft_service_wal_reload_t *reload =
        (tr_raft_service_wal_reload_t *) context;
    tr_raft_wal_recovery_t recovery;
    tr_raft_core_config_t core_config;
    int result;

    if (reload == NULL || snapshot_index == 0U || snapshot_term == 0U) {
        return SALTS_EINVAL;
    }
    memset(&recovery, 0, sizeof(recovery));
    result = tr_raft_wal_storage_load(reload->storage, &recovery);
    if (result != SALTS_OK) {
        return result;
    }
    if (recovery.snapshot_index != snapshot_index ||
        recovery.snapshot_term != snapshot_term ||
        recovery.commit_index < snapshot_index) {
        tr_raft_wal_recovery_destroy(&recovery);
        return SALTS_EPROTO;
    }

    memset(&core_config, 0, sizeof(core_config));
    core_config.self_id = reload->self_id;
    core_config.voters = reload->voters;
    core_config.voter_count = reload->voter_count;
    core_config.learners = reload->learner_count == 0U
                               ? NULL
                               : reload->learners;
    core_config.learner_count = reload->learner_count;
    core_config.initial_configuration =
        recovery.has_snapshot_configuration
            ? &recovery.snapshot_configuration
            : NULL;
    core_config.heartbeat_ticks = reload->heartbeat_ticks;
    core_config.election_min_ticks = reload->election_min_ticks;
    core_config.election_max_ticks = reload->election_max_ticks;
    core_config.initial_election_timeout_ticks =
        reload->initial_election_timeout_ticks;
    core_config.initial_term = recovery.term;
    core_config.initial_vote = recovery.voted_for;
    core_config.initial_last_log_index = recovery.snapshot_index;
    core_config.initial_last_log_term = recovery.snapshot_term;
    core_config.initial_log_entries = recovery.entry_count == 0U
                                          ? NULL
                                          : recovery.entries;
    core_config.initial_log_entry_count = recovery.entry_count;
    core_config.initial_commit_index = recovery.commit_index;
    core_config.initial_applied_index = recovery.snapshot_index;
    core_config.max_log_entries = reload->max_log_entries;
    core_config.max_inflight_append_requests =
        reload->max_inflight_append_requests;
    result = tr_raft_service_reload(reload->service, &core_config);
    tr_raft_wal_recovery_destroy(&recovery);
    return result;
}
