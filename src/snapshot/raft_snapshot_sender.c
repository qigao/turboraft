#include <turboraft/raft_snapshot_sender.h>

#include <openssl/sha.h>
#include <salts_error.h>

#include <stdlib.h>
#include <string.h>

typedef struct tr_snapshot_claim {
    uint64_t offset;
    size_t length;
} tr_snapshot_claim_t;

struct tr_raft_snapshot_sender {
    tr_raft_snapshot_sender_config_t config;
    uint8_t *data;
    size_t size;
    size_t chunk_size;
    size_t max_inflight_chunks;
    uint8_t digest[TR_RAFT_WIRE_SNAPSHOT_DIGEST_SIZE];
    tr_raft_term_t leader_term;
    tr_raft_index_t snapshot_index;
    tr_raft_term_t snapshot_term;
    tr_raft_conf_t configuration;
    uint64_t acknowledged_offset;
    uint64_t next_offset;
    tr_snapshot_claim_t claims[TR_RAFT_SNAPSHOT_MAX_INFLIGHT_CHUNKS];
    size_t claim_count;
    bool active;
    bool complete;
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
    sender->next_offset = 0U;
    memset(sender->claims, 0, sizeof(sender->claims));
    sender->claim_count = 0U;
    sender->active = false;
    sender->complete = false;
}

int tr_raft_snapshot_sender_create(
    const tr_raft_snapshot_sender_config_t *config,
    tr_raft_snapshot_sender_t **out_sender)
{
    tr_raft_snapshot_sender_t *sender;
    size_t chunk_size;
    size_t inflight;

    if (config == NULL || out_sender == NULL || config->self_id == 0U ||
        config->peer_id == 0U || config->self_id == config->peer_id ||
        config->max_snapshot_bytes == 0U ||
        config->max_snapshot_bytes > TR_RAFT_WIRE_MAX_SNAPSHOT_BYTES) {
        return SALTS_EINVAL;
    }
    chunk_size = config->chunk_size;
    inflight = config->max_inflight_chunks;
    if (chunk_size == 0U ||
        chunk_size > TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES ||
        chunk_size % TR_RAFT_WIRE_LEGACY_SNAPSHOT_CHUNK_BYTES != 0U ||
        inflight == 0U ||
        inflight > TR_RAFT_SNAPSHOT_MAX_INFLIGHT_CHUNKS ||
        chunk_size > SIZE_MAX / inflight ||
        chunk_size * inflight >
            TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES *
                TR_RAFT_SNAPSHOT_MAX_INFLIGHT_CHUNKS) {
        return SALTS_EINVAL;
    }

    sender = (tr_raft_snapshot_sender_t *)calloc(1U, sizeof(*sender));
    if (sender == NULL) {
        return SALTS_ENOMEM;
    }
    sender->config = *config;
    sender->chunk_size = chunk_size;
    sender->max_inflight_chunks = inflight;
    *out_sender = sender;
    return SALTS_OK;
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
        tr_raft_conf_validate(configuration) != SALTS_OK ||
        leader_term == 0U || snapshot_index == 0U ||
        snapshot_term == 0U || snapshot_term > leader_term ||
        size > sender->config.max_snapshot_bytes || (size > 0U && data == NULL)) {
        return SALTS_EINVAL;
    }
    if (sender->active && !sender->complete) {
        return SALTS_EBUSY;
    }
    if (size > 0U) {
        copy = (uint8_t *)malloc(size);
        if (copy == NULL) {
            return SALTS_ENOMEM;
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
    return SALTS_OK;
}

int tr_raft_snapshot_sender_next_chunk(
    tr_raft_snapshot_sender_t *sender,
    tr_raft_snapshot_chunk_t *out_chunk)
{
    size_t remaining;
    size_t length;
    tr_snapshot_claim_t *claim;

    if (sender == NULL || out_chunk == NULL) {
        return SALTS_EINVAL;
    }
    if (!sender->active || sender->complete) {
        return SALTS_EBUSY;
    }
    if (sender->claim_count >= sender->max_inflight_chunks) {
        if (sender->max_inflight_chunks != 1U) {
            return SALTS_EBUSY;
        }
        sender->next_offset = sender->claims[0].offset;
        sender->claim_count = 0U;
    }
    if (sender->next_offset == sender->size && sender->claim_count != 0U) {
        return SALTS_EBUSY;
    }

    remaining = sender->size - (size_t)sender->next_offset;
    length = remaining < sender->chunk_size ? remaining : sender->chunk_size;
    memset(out_chunk, 0, sizeof(*out_chunk));
    out_chunk->from = sender->config.self_id;
    out_chunk->to = sender->config.peer_id;
    out_chunk->term = sender->leader_term;
    out_chunk->snapshot_index = sender->snapshot_index;
    out_chunk->snapshot_term = sender->snapshot_term;
    out_chunk->snapshot_size = sender->size;
    out_chunk->snapshot_offset = sender->next_offset;
    if (sender->next_offset == 0U) {
        out_chunk->has_configuration = true;
        out_chunk->configuration = sender->configuration;
    }
    out_chunk->data_length = length;
    out_chunk->data = length == 0U ? NULL : sender->data + sender->next_offset;
    out_chunk->done = sender->next_offset + length == sender->size;
    memcpy(out_chunk->snapshot_digest, sender->digest, sizeof(sender->digest));

    claim = &sender->claims[sender->claim_count++];
    claim->offset = sender->next_offset;
    claim->length = length;
    sender->next_offset += length;
    return SALTS_OK;
}

int tr_raft_snapshot_sender_cancel_chunk(
    tr_raft_snapshot_sender_t *sender,
    uint64_t snapshot_offset)
{
    tr_snapshot_claim_t *claim;

    if (sender == NULL || sender->claim_count == 0U) {
        return SALTS_EINVAL;
    }
    claim = &sender->claims[sender->claim_count - 1U];
    if (claim->offset != snapshot_offset) {
        return SALTS_EPROTO;
    }
    sender->next_offset = claim->offset;
    memset(claim, 0, sizeof(*claim));
    --sender->claim_count;
    return SALTS_OK;
}

int tr_raft_snapshot_sender_prepare_resume(tr_raft_snapshot_sender_t *sender)
{
    if (sender == NULL) {
        return SALTS_EINVAL;
    }
    if (!sender->active || sender->complete) {
        return SALTS_EBUSY;
    }
    sender->next_offset = sender->acknowledged_offset;
    sender->claim_count = 0U;
    memset(sender->claims, 0, sizeof(sender->claims));
    return SALTS_OK;
}

int tr_raft_snapshot_sender_acknowledge(
    tr_raft_snapshot_sender_t *sender,
    const tr_raft_snapshot_ack_t *ack)
{
    size_t released = 0U;
    bool boundary = false;

    if (sender == NULL || ack == NULL) {
        return SALTS_EINVAL;
    }
    if (!sender->active) {
        return SALTS_EBUSY;
    }
    if (ack->from != sender->config.peer_id || ack->to != sender->config.self_id ||
        ack->term != sender->leader_term ||
        ack->snapshot_index != sender->snapshot_index ||
        ack->snapshot_size != sender->size ||
        memcmp(ack->snapshot_digest, sender->digest, sizeof(sender->digest)) != 0) {
        return SALTS_EPROTO;
    }
    if (sender->complete) {
        return ack->accepted && ack->next_offset == sender->size
                   ? SALTS_OK
                   : SALTS_EPROTO;
    }
    if (ack->next_offset > sender->next_offset ||
        (ack->next_offset != sender->size &&
         ack->next_offset % sender->chunk_size != 0U)) {
        return SALTS_EPROTO;
    }
    if (!ack->accepted) {
        if (ack->next_offset > sender->acknowledged_offset) {
            return SALTS_EPROTO;
        }
        sender->acknowledged_offset = ack->next_offset;
        sender->next_offset = ack->next_offset;
        sender->claim_count = 0U;
        memset(sender->claims, 0, sizeof(sender->claims));
        return SALTS_OK;
    }
    if (ack->next_offset < sender->acknowledged_offset) {
        return SALTS_EPROTO;
    }
    boundary = ack->next_offset == sender->acknowledged_offset;
    for (released = 0U; released < sender->claim_count; ++released) {
        if (sender->claims[released].offset +
                sender->claims[released].length == ack->next_offset) {
            boundary = true;
            break;
        }
    }
    if (!boundary) {
        return SALTS_EPROTO;
    }
    sender->acknowledged_offset = ack->next_offset;
    released = 0U;
    while (released < sender->claim_count &&
           sender->claims[released].offset + sender->claims[released].length <=
               ack->next_offset) {
        ++released;
    }
    if (released != 0U) {
        memmove(sender->claims, sender->claims + released,
                (sender->claim_count - released) * sizeof(sender->claims[0]));
        sender->claim_count -= released;
        memset(sender->claims + sender->claim_count, 0,
               released * sizeof(sender->claims[0]));
    }
    sender->complete = ack->next_offset == sender->size;
    return SALTS_OK;
}

int tr_raft_snapshot_sender_get_status(
    const tr_raft_snapshot_sender_t *sender,
    tr_raft_snapshot_sender_status_t *out_status)
{
    if (sender == NULL || out_status == NULL) {
        return SALTS_EINVAL;
    }
    memset(out_status, 0, sizeof(*out_status));
    out_status->active = sender->active;
    out_status->complete = sender->complete;
    out_status->leader_term = sender->leader_term;
    out_status->snapshot_index = sender->snapshot_index;
    out_status->snapshot_term = sender->snapshot_term;
    out_status->snapshot_size = sender->size;
    out_status->acknowledged_offset = sender->acknowledged_offset;
    out_status->next_offset = sender->next_offset;
    out_status->inflight_chunks = sender->claim_count;
    out_status->max_inflight_chunks = sender->max_inflight_chunks;
    return SALTS_OK;
}
