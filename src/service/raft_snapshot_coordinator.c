#include "raft_snapshot_coordinator.h"

#include <salts_error.h>

#include <stdlib.h>
#include <string.h>

struct tr_raft_snapshot_coordinator {
    tr_raft_snapshot_sender_t *sender;
    tr_raft_snapshot_emit_fn emit;
    void *emit_context;
};

static int tr_snapshot_coordinator_fill_window(
    tr_raft_snapshot_coordinator_t *coordinator)
{
    for (;;) {
        tr_raft_snapshot_chunk_t chunk;
        int result;

        result = tr_raft_snapshot_sender_next_chunk(coordinator->sender,
                                                    &chunk);
        if (result == SALTS_EBUSY) {
            return SALTS_OK;
        }
        if (result != SALTS_OK) {
            return result;
        }
        result = coordinator->emit(coordinator->emit_context, &chunk);
        if (result != SALTS_OK) {
            int cancel_result = tr_raft_snapshot_sender_cancel_chunk(
                coordinator->sender, chunk.snapshot_offset);

            return cancel_result == SALTS_OK ? result : cancel_result;
        }
        {
            tr_raft_snapshot_sender_status_t status;

            result = tr_raft_snapshot_sender_get_status(
                coordinator->sender, &status);
            if (result != SALTS_OK ||
                status.inflight_chunks >= status.max_inflight_chunks) {
                return result;
            }
        }
    }
}

int tr_raft_snapshot_coordinator_create(
    const tr_raft_snapshot_coordinator_config_t *config,
    tr_raft_snapshot_coordinator_t **out_coordinator)
{
    tr_raft_snapshot_sender_config_t sender_config;
    tr_raft_snapshot_coordinator_t *coordinator;
    int result;

    if (config == NULL || out_coordinator == NULL || config->emit == NULL) {
        return SALTS_EINVAL;
    }
    *out_coordinator = NULL;
    memset(&sender_config, 0, sizeof(sender_config));
    coordinator = (tr_raft_snapshot_coordinator_t *) calloc(
        1U, sizeof(*coordinator));
    if (coordinator == NULL) {
        return SALTS_ENOMEM;
    }

    sender_config.self_id = config->self_id;
    sender_config.peer_id = config->peer_id;
    sender_config.max_snapshot_bytes = config->max_snapshot_bytes;
    sender_config.chunk_size = config->chunk_size;
    sender_config.max_inflight_chunks = config->max_inflight_chunks;
    result = tr_raft_snapshot_sender_create(&sender_config,
                                            &coordinator->sender);
    if (result != SALTS_OK) {
        free(coordinator);
        return result;
    }
    coordinator->emit = config->emit;
    coordinator->emit_context = config->emit_context;
    *out_coordinator = coordinator;
    return SALTS_OK;
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
        return SALTS_EINVAL;
    }
    result = tr_raft_snapshot_sender_begin(
        coordinator->sender, leader_term, snapshot_index, snapshot_term,
        configuration, data, size);
    if (result != SALTS_OK) {
        return result;
    }
    return tr_snapshot_coordinator_fill_window(coordinator);
}

int tr_raft_snapshot_coordinator_begin_source(
    tr_raft_snapshot_coordinator_t *coordinator,
    tr_raft_term_t leader_term,
    tr_raft_index_t snapshot_index,
    tr_raft_term_t snapshot_term,
    const tr_raft_conf_t *configuration,
    const tr_raft_snapshot_source_t *source)
{
    int result;

    if (coordinator == NULL || source == NULL) {
        return SALTS_EINVAL;
    }
    result = tr_raft_snapshot_sender_begin_source(
        coordinator->sender, leader_term, snapshot_index, snapshot_term,
        configuration, source);
    if (result != SALTS_OK) {
        if (source->release != NULL) {
            source->release(source->context);
        }
        return result;
    }
    return tr_snapshot_coordinator_fill_window(coordinator);
}

int tr_raft_snapshot_coordinator_handle_ack(
    tr_raft_snapshot_coordinator_t *coordinator,
    const tr_raft_snapshot_ack_t *ack)
{
    tr_raft_snapshot_sender_status_t status;
    int result;

    if (coordinator == NULL || ack == NULL) {
        return SALTS_EINVAL;
    }
    result = tr_raft_snapshot_sender_acknowledge(coordinator->sender, ack);
    if (result != SALTS_OK) {
        return result;
    }
    result = tr_raft_snapshot_sender_get_status(coordinator->sender, &status);
    if (result != SALTS_OK || status.complete) {
        return result;
    }
    return tr_snapshot_coordinator_fill_window(coordinator);
}

int tr_raft_snapshot_coordinator_resume(
    tr_raft_snapshot_coordinator_t *coordinator)
{
    if (coordinator == NULL) {
        return SALTS_EINVAL;
    }
    {
        int result = tr_raft_snapshot_sender_prepare_resume(
            coordinator->sender);

        return result == SALTS_OK
                   ? tr_snapshot_coordinator_fill_window(coordinator)
                   : result;
    }
}

int tr_raft_snapshot_coordinator_get_status(
    const tr_raft_snapshot_coordinator_t *coordinator,
    tr_raft_snapshot_sender_status_t *out_status)
{
    if (coordinator == NULL) {
        return SALTS_EINVAL;
    }
    return tr_raft_snapshot_sender_get_status(coordinator->sender,
                                              out_status);
}
