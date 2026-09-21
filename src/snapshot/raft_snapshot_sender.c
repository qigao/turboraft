#include <turboraft/raft_snapshot_sender.h>

#include <openssl/sha.h>
#include <salts_error.h>

#include <stdlib.h>
#include <string.h>

typedef struct tr_snapshot_claim {
    uint64_t offset;
    size_t length;
    size_t slot;
} tr_snapshot_claim_t;

typedef struct tr_snapshot_memory_source {
    uint8_t *data;
    size_t size;
} tr_snapshot_memory_source_t;

struct tr_raft_snapshot_sender {
    tr_raft_snapshot_sender_config_t config;
    tr_raft_snapshot_source_t source;
    uint8_t *chunk_storage;
    uint64_t size;
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
    bool source_owned;
    bool active;
    bool complete;
};

static int tr_snapshot_memory_read_at(
    void *context,
    uint64_t offset,
    uint8_t *buffer,
    size_t capacity,
    size_t *out_size)
{
    tr_snapshot_memory_source_t *source =
        (tr_snapshot_memory_source_t *)context;

    if (source == NULL || out_size == NULL ||
        offset > source->size ||
        capacity > source->size - (size_t)offset ||
        (capacity != 0U && buffer == NULL)) {
        return SALTS_EINVAL;
    }
    if (capacity != 0U) {
        memcpy(buffer, source->data + (size_t)offset, capacity);
    }
    *out_size = capacity;
    return SALTS_OK;
}

static void tr_snapshot_memory_release(void *context)
{
    tr_snapshot_memory_source_t *source =
        (tr_snapshot_memory_source_t *)context;

    if (source == NULL) {
        return;
    }
    free(source->data);
    free(source);
}

static void tr_raft_snapshot_sender_release_source(
    tr_raft_snapshot_sender_t *sender)
{
    if (sender->source_owned && sender->source.release != NULL) {
        sender->source.release(sender->source.context);
    }
    memset(&sender->source, 0, sizeof(sender->source));
    sender->source_owned = false;
}

static void tr_raft_snapshot_sender_clear_transfer(
    tr_raft_snapshot_sender_t *sender)
{
    tr_raft_snapshot_sender_release_source(sender);
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

static bool tr_raft_snapshot_sender_slot_in_use(
    const tr_raft_snapshot_sender_t *sender,
    size_t slot)
{
    size_t index;

    for (index = 0U; index < sender->claim_count; ++index) {
        if (sender->claims[index].slot == slot) {
            return true;
        }
    }
    return false;
}

static int tr_raft_snapshot_sender_find_free_slot(
    const tr_raft_snapshot_sender_t *sender,
    size_t *out_slot)
{
    size_t slot;

    if (out_slot == NULL) {
        return SALTS_EINVAL;
    }
    for (slot = 0U; slot < sender->max_inflight_chunks; ++slot) {
        if (!tr_raft_snapshot_sender_slot_in_use(sender, slot)) {
            *out_slot = slot;
            return SALTS_OK;
        }
    }
    return SALTS_EBUSY;
}

static void tr_raft_snapshot_sender_fill_chunk(
    const tr_raft_snapshot_sender_t *sender,
    const tr_snapshot_claim_t *claim,
    tr_raft_snapshot_chunk_t *out_chunk)
{
    memset(out_chunk, 0, sizeof(*out_chunk));
    out_chunk->from = sender->config.self_id;
    out_chunk->to = sender->config.peer_id;
    out_chunk->term = sender->leader_term;
    out_chunk->snapshot_index = sender->snapshot_index;
    out_chunk->snapshot_term = sender->snapshot_term;
    out_chunk->snapshot_size = sender->size;
    out_chunk->snapshot_offset = claim->offset;
    if (claim->offset == 0U) {
        out_chunk->has_configuration = true;
        out_chunk->configuration = sender->configuration;
    }
    out_chunk->data_length = claim->length;
    out_chunk->data =
        claim->length == 0U
            ? NULL
            : sender->chunk_storage + claim->slot * sender->chunk_size;
    out_chunk->done = claim->offset + claim->length == sender->size;
    memcpy(out_chunk->snapshot_digest, sender->digest,
           sizeof(sender->digest));
}

int tr_raft_snapshot_sender_create(
    const tr_raft_snapshot_sender_config_t *config,
    tr_raft_snapshot_sender_t **out_sender)
{
    tr_raft_snapshot_sender_t *sender;
    size_t chunk_size;
    size_t inflight;
    size_t storage_size;

    if (config == NULL || out_sender == NULL || config->self_id == 0U ||
        config->peer_id == 0U || config->self_id == config->peer_id ||
        config->max_snapshot_bytes == 0U) {
        return SALTS_EINVAL;
    }
    chunk_size = config->chunk_size;
    inflight = config->max_inflight_chunks;
    if (chunk_size == 0U ||
        chunk_size > TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES ||
        inflight == 0U ||
        inflight > TR_RAFT_SNAPSHOT_MAX_INFLIGHT_CHUNKS ||
        chunk_size > SIZE_MAX / inflight) {
        return SALTS_EINVAL;
    }
    storage_size = chunk_size * inflight;

    sender = (tr_raft_snapshot_sender_t *)calloc(1U, sizeof(*sender));
    if (sender == NULL) {
        return SALTS_ENOMEM;
    }
    sender->chunk_storage = (uint8_t *)malloc(storage_size);
    if (sender->chunk_storage == NULL) {
        free(sender);
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
    free(sender->chunk_storage);
    free(sender);
}

void tr_raft_snapshot_sender_reset(tr_raft_snapshot_sender_t *sender)
{
    if (sender != NULL) {
        tr_raft_snapshot_sender_clear_transfer(sender);
    }
}

int tr_raft_snapshot_sender_begin_source(
    tr_raft_snapshot_sender_t *sender,
    tr_raft_term_t leader_term,
    tr_raft_index_t snapshot_index,
    tr_raft_term_t snapshot_term,
    const tr_raft_conf_t *configuration,
    const tr_raft_snapshot_source_t *source)
{
    if (sender == NULL || configuration == NULL || source == NULL ||
        source->read_at == NULL ||
        tr_raft_conf_validate(configuration) != SALTS_OK ||
        leader_term == 0U || snapshot_index == 0U ||
        snapshot_term == 0U || snapshot_term > leader_term ||
        source->size > (uint64_t)sender->config.max_snapshot_bytes) {
        return SALTS_EINVAL;
    }
    if (sender->active && !sender->complete) {
        return SALTS_EBUSY;
    }

    tr_raft_snapshot_sender_clear_transfer(sender);
    sender->source = *source;
    sender->source_owned = true;
    sender->size = source->size;
    memcpy(sender->digest, source->digest, sizeof(sender->digest));
    sender->leader_term = leader_term;
    sender->snapshot_index = snapshot_index;
    sender->snapshot_term = snapshot_term;
    sender->configuration = *configuration;
    sender->active = true;
    return SALTS_OK;
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
    tr_snapshot_memory_source_t *memory = NULL;
    tr_raft_snapshot_source_t source;
    int result;

    if (sender == NULL || configuration == NULL ||
        size > sender->config.max_snapshot_bytes ||
        (size > 0U && data == NULL)) {
        return SALTS_EINVAL;
    }

    memory = (tr_snapshot_memory_source_t *)calloc(1U, sizeof(*memory));
    if (memory == NULL) {
        return SALTS_ENOMEM;
    }
    if (size != 0U) {
        memory->data = (uint8_t *)malloc(size);
        if (memory->data == NULL) {
            free(memory);
            return SALTS_ENOMEM;
        }
        memcpy(memory->data, data, size);
    }
    memory->size = size;

    memset(&source, 0, sizeof(source));
    source.context = memory;
    source.size = size;
    SHA256(size != 0U ? memory->data : (const uint8_t *)"",
           size, source.digest);
    source.read_at = tr_snapshot_memory_read_at;
    source.release = tr_snapshot_memory_release;

    result = tr_raft_snapshot_sender_begin_source(
        sender, leader_term, snapshot_index, snapshot_term,
        configuration, &source);
    if (result != SALTS_OK) {
        tr_snapshot_memory_release(memory);
    }
    return result;
}

int tr_raft_snapshot_sender_next_chunk(
    tr_raft_snapshot_sender_t *sender,
    tr_raft_snapshot_chunk_t *out_chunk)
{
    uint64_t remaining;
    size_t length;
    size_t slot;
    size_t read_size = 0U;
    uint8_t *buffer;
    tr_snapshot_claim_t *claim;
    int result;

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
        tr_raft_snapshot_sender_fill_chunk(
            sender, &sender->claims[0], out_chunk);
        return SALTS_OK;
    }
    if (sender->next_offset == sender->size &&
        sender->claim_count != 0U) {
        return SALTS_EBUSY;
    }

    remaining = sender->size - sender->next_offset;
    length = remaining < sender->chunk_size
                 ? (size_t)remaining
                 : sender->chunk_size;
    result = tr_raft_snapshot_sender_find_free_slot(sender, &slot);
    if (result != SALTS_OK) {
        return result;
    }
    buffer = sender->chunk_storage + slot * sender->chunk_size;
    if (length != 0U) {
        result = sender->source.read_at(
            sender->source.context, sender->next_offset,
            buffer, length, &read_size);
        if (result != SALTS_OK) {
            return result;
        }
        if (read_size != length) {
            return SALTS_EPROTO;
        }
    }

    claim = &sender->claims[sender->claim_count++];
    claim->offset = sender->next_offset;
    claim->length = length;
    claim->slot = slot;
    sender->next_offset += length;
    tr_raft_snapshot_sender_fill_chunk(sender, claim, out_chunk);
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
    if (ack->from != sender->config.peer_id ||
        ack->to != sender->config.self_id ||
        ack->term != sender->leader_term ||
        ack->snapshot_index != sender->snapshot_index ||
        ack->snapshot_size != sender->size ||
        memcmp(ack->snapshot_digest, sender->digest,
               sizeof(sender->digest)) != 0) {
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
           sender->claims[released].offset +
                   sender->claims[released].length <=
               ack->next_offset) {
        ++released;
    }
    if (released != 0U) {
        memmove(sender->claims, sender->claims + released,
                (sender->claim_count - released) *
                    sizeof(sender->claims[0]));
        sender->claim_count -= released;
        memset(sender->claims + sender->claim_count, 0,
               released * sizeof(sender->claims[0]));
    }
    sender->complete = ack->next_offset == sender->size;
    if (sender->complete) {
        tr_raft_snapshot_sender_release_source(sender);
    }
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
