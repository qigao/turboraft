#include "raft_snapshot_coordinator.h"

#include <turbo_error.h>

#include <stdlib.h>

struct tr_raft_snapshot_coordinator {
    tr_raft_snapshot_sender_t *sender;
    tr_raft_snapshot_emit_fn emit;
    void *emit_context;
};

static int tr_snapshot_coordinator_emit_current(
    tr_raft_snapshot_coordinator_t *coordinator)
{
    tr_raft_snapshot_chunk_t chunk;
    int result;

    result = tr_raft_snapshot_sender_next_chunk(coordinator->sender, &chunk);
    if (result != TURBO_OK) {
        return result;
    }
    return coordinator->emit(coordinator->emit_context, &chunk);
}

int tr_raft_snapshot_coordinator_create(
    const tr_raft_snapshot_coordinator_config_t *config,
    tr_raft_snapshot_coordinator_t **out_coordinator)
{
    tr_raft_snapshot_sender_config_t sender_config;
    tr_raft_snapshot_coordinator_t *coordinator;
    int result;

    if (config == NULL || out_coordinator == NULL || config->emit == NULL) {
        return TURBO_EINVAL;
    }
    *out_coordinator = NULL;
    coordinator = (tr_raft_snapshot_coordinator_t *) calloc(
        1U, sizeof(*coordinator));
    if (coordinator == NULL) {
        return TURBO_ENOMEM;
    }

    sender_config.self_id = config->self_id;
    sender_config.peer_id = config->peer_id;
    sender_config.max_snapshot_bytes = config->max_snapshot_bytes;
    result = tr_raft_snapshot_sender_create(&sender_config,
                                            &coordinator->sender);
    if (result != TURBO_OK) {
        free(coordinator);
        return result;
    }
    coordinator->emit = config->emit;
    coordinator->emit_context = config->emit_context;
    *out_coordinator = coordinator;
    return TURBO_OK;
}

void tr_raft_snapshot_coordinator_destroy(
    tr_raft_snapshot_coordinator_t *coordinator)
{
    if (coordinator == NULL) {
        return;
    }
    tr_raft_snapshot_sender_destroy(coordinator->sender);
    free(coordinator);
}

int tr_raft_snapshot_coordinator_begin(
    tr_raft_snapshot_coordinator_t *coordinator,
    tr_raft_term_t leader_term,
    tr_raft_index_t snapshot_index,
    tr_raft_term_t snapshot_term,
    const tr_raft_conf_t *configuration,
    const uint8_t *data,
    size_t size)
{
    int result;

    if (coordinator == NULL) {
        return TURBO_EINVAL;
    }
    result = tr_raft_snapshot_sender_begin(
        coordinator->sender, leader_term, snapshot_index, snapshot_term,
        configuration, data, size);
    if (result != TURBO_OK) {
        return result;
    }
    return tr_snapshot_coordinator_emit_current(coordinator);
}

int tr_raft_snapshot_coordinator_handle_ack(
    tr_raft_snapshot_coordinator_t *coordinator,
    const tr_raft_snapshot_ack_t *ack)
{
    tr_raft_snapshot_sender_status_t status;
    int result;

    if (coordinator == NULL || ack == NULL) {
        return TURBO_EINVAL;
    }
    result = tr_raft_snapshot_sender_acknowledge(coordinator->sender, ack);
    if (result != TURBO_OK) {
        return result;
    }
    result = tr_raft_snapshot_sender_get_status(coordinator->sender, &status);
    if (result != TURBO_OK || status.complete) {
        return result;
    }
    return tr_snapshot_coordinator_emit_current(coordinator);
}

int tr_raft_snapshot_coordinator_resume(
    tr_raft_snapshot_coordinator_t *coordinator)
{
    if (coordinator == NULL) {
        return TURBO_EINVAL;
    }
    return tr_snapshot_coordinator_emit_current(coordinator);
}

int tr_raft_snapshot_coordinator_get_status(
    const tr_raft_snapshot_coordinator_t *coordinator,
    tr_raft_snapshot_sender_status_t *out_status)
{
    if (coordinator == NULL) {
        return TURBO_EINVAL;
    }
    return tr_raft_snapshot_sender_get_status(coordinator->sender,
                                              out_status);
}
