#include <turboraft/raft_snapshot_sender.h>

#include <openssl/sha.h>
#include <turbo_error.h>

#include <stdlib.h>
#include <string.h>

struct tr_raft_snapshot_sender {
    tr_raft_snapshot_sender_config_t config;
    uint8_t *data;
    size_t size;
    uint8_t digest[TR_RAFT_WIRE_SNAPSHOT_DIGEST_SIZE];
    tr_raft_term_t leader_term;
    tr_raft_index_t snapshot_index;
    tr_raft_term_t snapshot_term;
    tr_raft_conf_t configuration;
    uint64_t acknowledged_offset;
    bool active;
    bool complete;
    bool chunk_issued;
};

static void tr_raft_snapshot_sender_clear_transfer(tr_raft_snapshot_sender_t *sender)
{
    free(sender->data);
    sender->data = NULL;
    sender->size = 0U;
    memset(sender->digest, 0, sizeof(sender->digest));
    sender->leader_term = 0U;
    sender->snapshot_index = 0U;
    sender->snapshot_term = 0U;
    memset(&sender->configuration, 0, sizeof(sender->configuration));
    sender->acknowledged_offset = 0U;
    sender->active = false;
    sender->complete = false;
    sender->chunk_issued = false;
}

int tr_raft_snapshot_sender_create(
    const tr_raft_snapshot_sender_config_t *config,
    tr_raft_snapshot_sender_t **out_sender)
{
    tr_raft_snapshot_sender_t *sender;

    if (config == NULL || out_sender == NULL || config->self_id == 0U ||
        config->peer_id == 0U || config->self_id == config->peer_id ||
        config->max_snapshot_bytes == 0U ||
        config->max_snapshot_bytes > TR_RAFT_WIRE_MAX_SNAPSHOT_BYTES) {
        return TURBO_EINVAL;
    }

    sender = (tr_raft_snapshot_sender_t *)calloc(1U, sizeof(*sender));
    if (sender == NULL) {
        return TURBO_ENOMEM;
    }
    sender->config = *config;
    *out_sender = sender;
    return TURBO_OK;
}

void tr_raft_snapshot_sender_destroy(tr_raft_snapshot_sender_t *sender)
{
    if (sender == NULL) {
        return;
    }
    tr_raft_snapshot_sender_clear_transfer(sender);
    free(sender);
}

void tr_raft_snapshot_sender_reset(tr_raft_snapshot_sender_t *sender)
{
    if (sender != NULL) {
        tr_raft_snapshot_sender_clear_transfer(sender);
    }
}

int tr_raft_snapshot_sender_begin(
    tr_raft_snapshot_sender_t *sender,
    tr_raft_term_t leader_term,
    tr_raft_index_t snapshot_index,
    tr_raft_term_t snapshot_term,
    const tr_raft_conf_t *configuration,
    const uint8_t *data,
    size_t size)
{
    uint8_t *copy = NULL;

    if (sender == NULL || configuration == NULL ||
        tr_raft_conf_validate(configuration) != TURBO_OK ||
        leader_term == 0U || snapshot_index == 0U ||
        snapshot_term == 0U || snapshot_term > leader_term ||
        size > sender->config.max_snapshot_bytes || (size > 0U && data == NULL)) {
        return TURBO_EINVAL;
    }
    if (sender->active && !sender->complete) {
        return TURBO_EBUSY;
    }

    if (size > 0U) {
        copy = (uint8_t *)malloc(size);
        if (copy == NULL) {
            return TURBO_ENOMEM;
        }
        memcpy(copy, data, size);
    }

    tr_raft_snapshot_sender_clear_transfer(sender);
    sender->data = copy;
    sender->size = size;
    sender->leader_term = leader_term;
    sender->snapshot_index = snapshot_index;
    sender->snapshot_term = snapshot_term;
    sender->configuration = *configuration;
    SHA256(size > 0U ? sender->data : (const uint8_t *)"", size, sender->digest);
    sender->active = true;
    return TURBO_OK;
}

int tr_raft_snapshot_sender_next_chunk(
    tr_raft_snapshot_sender_t *sender,
    tr_raft_snapshot_chunk_t *out_chunk)
{
    size_t remaining;
    size_t chunk_size;

    if (sender == NULL || out_chunk == NULL) {
        return TURBO_EINVAL;
    }
    if (!sender->active || sender->complete) {
        return TURBO_EBUSY;
    }

    remaining = sender->size - (size_t)sender->acknowledged_offset;
    chunk_size = remaining < TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES
                     ? remaining
                     : TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES;

    memset(out_chunk, 0, sizeof(*out_chunk));
    out_chunk->from = sender->config.self_id;
    out_chunk->to = sender->config.peer_id;
    out_chunk->term = sender->leader_term;
    out_chunk->snapshot_index = sender->snapshot_index;
    out_chunk->snapshot_term = sender->snapshot_term;
    out_chunk->snapshot_size = sender->size;
    out_chunk->snapshot_offset = sender->acknowledged_offset;
    if (sender->acknowledged_offset == 0U) {
        out_chunk->has_configuration = true;
        out_chunk->configuration = sender->configuration;
    }
    out_chunk->data_length = chunk_size;
    out_chunk->done = sender->acknowledged_offset + chunk_size == sender->size;
    memcpy(out_chunk->snapshot_digest, sender->digest, sizeof(sender->digest));
    if (chunk_size > 0U) {
        memcpy(out_chunk->data, sender->data + sender->acknowledged_offset,
               chunk_size);
    }
    sender->chunk_issued = true;
    return TURBO_OK;
}

int tr_raft_snapshot_sender_acknowledge(
    tr_raft_snapshot_sender_t *sender,
    const tr_raft_snapshot_ack_t *ack)
{
    uint64_t remaining;
    uint64_t chunk_size;
    uint64_t expected_next;

    if (sender == NULL || ack == NULL) {
        return TURBO_EINVAL;
    }
    if (!sender->active) {
        return TURBO_EBUSY;
    }
    if (ack->from != sender->config.peer_id || ack->to != sender->config.self_id ||
        ack->term != sender->leader_term ||
        ack->snapshot_index != sender->snapshot_index ||
        ack->snapshot_size != sender->size ||
        memcmp(ack->snapshot_digest, sender->digest, sizeof(sender->digest)) != 0) {
        return TURBO_EPROTO;
    }
    if (sender->complete) {
        return ack->accepted && ack->next_offset == sender->size
                   ? TURBO_OK
                   : TURBO_EPROTO;
    }
    if (!sender->chunk_issued) {
        return ack->accepted &&
                       ack->next_offset == sender->acknowledged_offset
                   ? TURBO_OK
                   : TURBO_EPROTO;
    }

    remaining = sender->size - sender->acknowledged_offset;
    chunk_size = remaining < TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES
                     ? remaining
                     : TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES;
    expected_next = sender->acknowledged_offset + chunk_size;

    if (ack->accepted) {
        if (ack->next_offset != sender->acknowledged_offset &&
            ack->next_offset != expected_next) {
            return TURBO_EPROTO;
        }
        if (ack->next_offset == expected_next) {
            sender->acknowledged_offset = expected_next;
            sender->chunk_issued = false;
            sender->complete = expected_next == sender->size;
        }
        return TURBO_OK;
    }

    if (ack->next_offset > sender->acknowledged_offset ||
        ack->next_offset % TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES != 0U) {
        return TURBO_EPROTO;
    }
    sender->acknowledged_offset = ack->next_offset;
    sender->chunk_issued = false;
    return TURBO_OK;
}

int tr_raft_snapshot_sender_get_status(
    const tr_raft_snapshot_sender_t *sender,
    tr_raft_snapshot_sender_status_t *out_status)
{
    if (sender == NULL || out_status == NULL) {
        return TURBO_EINVAL;
    }

    memset(out_status, 0, sizeof(*out_status));
    out_status->active = sender->active;
    out_status->complete = sender->complete;
    out_status->leader_term = sender->leader_term;
    out_status->snapshot_index = sender->snapshot_index;
    out_status->snapshot_term = sender->snapshot_term;
    out_status->snapshot_size = sender->size;
    out_status->acknowledged_offset = sender->acknowledged_offset;
    return TURBO_OK;
}
