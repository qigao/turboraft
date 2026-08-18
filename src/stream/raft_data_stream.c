#include <turboraft/raft_data_stream.h>

#include <openssl/sha.h>
#include <turbo_error.h>

#include <stdlib.h>
#include <string.h>

static const uint8_t TR_DATA_DESCRIPTOR_MAGIC[4] = {'T', 'R', 'D', 'S'};

static void tr_data_put_u16(uint8_t *output, uint16_t value)
{
    output[0] = (uint8_t)(value >> 8U);
    output[1] = (uint8_t)value;
}

static void tr_data_put_u64(uint8_t *output, uint64_t value)
{
    size_t index;
    for (index = 0U; index < 8U; ++index)
        output[index] = (uint8_t)(value >> (56U - index * 8U));
}

static uint16_t tr_data_get_u16(const uint8_t *input)
{
    return (uint16_t)(((uint16_t)input[0] << 8U) | input[1]);
}

static uint64_t tr_data_get_u64(const uint8_t *input)
{
    uint64_t value = 0U;
    size_t index;
    for (index = 0U; index < 8U; ++index) value = (value << 8U) | input[index];
    return value;
}

int tr_raft_data_descriptor_encode(
    const tr_raft_data_descriptor_t *descriptor,
    uint8_t *output,
    size_t output_capacity,
    size_t *output_size)
{
    if (output_size != NULL) *output_size = TR_RAFT_DATA_DESCRIPTOR_ENCODED_SIZE;
    if (descriptor == NULL || output == NULL || output_size == NULL ||
        descriptor->stream_id == 0U ||
        descriptor->stream_size > TR_RAFT_WIRE_MAX_DATA_STREAM_BYTES) {
        return TURBO_EINVAL;
    }
    if (output_capacity < TR_RAFT_DATA_DESCRIPTOR_ENCODED_SIZE)
        return TURBO_ENOSPC;
    memcpy(output, TR_DATA_DESCRIPTOR_MAGIC, sizeof(TR_DATA_DESCRIPTOR_MAGIC));
    tr_data_put_u16(output + 4U, TR_RAFT_DATA_DESCRIPTOR_VERSION);
    tr_data_put_u16(output + 6U, 0U);
    tr_data_put_u64(output + 8U, descriptor->stream_id);
    tr_data_put_u64(output + 16U, descriptor->stream_size);
    memcpy(output + 24U, descriptor->stream_digest,
           sizeof(descriptor->stream_digest));
    return TURBO_OK;
}

int tr_raft_data_descriptor_decode(
    const uint8_t *input,
    size_t input_size,
    tr_raft_data_descriptor_t *descriptor)
{
    if (input == NULL || descriptor == NULL) return TURBO_EINVAL;
    if (input_size != TR_RAFT_DATA_DESCRIPTOR_ENCODED_SIZE ||
        memcmp(input, TR_DATA_DESCRIPTOR_MAGIC,
               sizeof(TR_DATA_DESCRIPTOR_MAGIC)) != 0 ||
        tr_data_get_u16(input + 4U) != TR_RAFT_DATA_DESCRIPTOR_VERSION ||
        tr_data_get_u16(input + 6U) != 0U) {
        return TURBO_EPROTO;
    }
    memset(descriptor, 0, sizeof(*descriptor));
    descriptor->stream_id = tr_data_get_u64(input + 8U);
    descriptor->stream_size = tr_data_get_u64(input + 16U);
    memcpy(descriptor->stream_digest, input + 24U,
           sizeof(descriptor->stream_digest));
    return descriptor->stream_id != 0U &&
                   descriptor->stream_size <=
                       TR_RAFT_WIRE_MAX_DATA_STREAM_BYTES
               ? TURBO_OK
               : TURBO_EPROTO;
}

typedef struct tr_data_claim {
    uint64_t offset;
    size_t length;
} tr_data_claim_t;

struct tr_raft_data_stream_sender {
    tr_raft_data_stream_sender_config_t config;
    const uint8_t *data;
    size_t size;
    size_t chunk_size;
    size_t max_inflight_chunks;
    tr_raft_term_t term;
    uint64_t stream_id;
    uint64_t acknowledged_offset;
    uint64_t next_offset;
    uint8_t digest[TR_RAFT_WIRE_DATA_DIGEST_SIZE];
    tr_data_claim_t claims[TR_RAFT_DATA_STREAM_MAX_INFLIGHT_CHUNKS];
    size_t claim_count;
    bool active;
    bool complete;
};

struct tr_raft_data_stream_receiver {
    tr_raft_data_stream_receiver_config_t config;
    tr_raft_node_id_t leader_id;
    tr_raft_term_t term;
    uint64_t stream_id;
    uint64_t stream_size;
    uint64_t next_offset;
    uint8_t digest[TR_RAFT_WIRE_DATA_DIGEST_SIZE];
    SHA256_CTX sha256;
    bool active;
    bool committed;
};

struct tr_raft_data_quorum {
    tr_raft_data_quorum_config_t config;
    bool durable[TR_RAFT_MAX_MEMBERS];
};

static size_t tr_data_majority(size_t voters)
{
    return voters / 2U + 1U;
}

static size_t tr_data_member_index(
    const tr_raft_conf_t *configuration, tr_raft_node_id_t node_id)
{
    size_t index;
    for (index = 0U; index < configuration->member_count; ++index) {
        if (configuration->members[index].node_id == node_id) return index;
    }
    return SIZE_MAX;
}

int tr_raft_data_quorum_create(
    const tr_raft_data_quorum_config_t *config,
    tr_raft_data_quorum_t **out_quorum)
{
    tr_raft_data_quorum_t *quorum;
    size_t self_index;
    if (out_quorum == NULL) return TURBO_EINVAL;
    *out_quorum = NULL;
    if (config == NULL || config->self_id == 0U || config->term == 0U ||
        config->descriptor.stream_id == 0U ||
        config->descriptor.stream_size > TR_RAFT_WIRE_MAX_DATA_STREAM_BYTES ||
        tr_raft_conf_validate(&config->configuration) != TURBO_OK) {
        return TURBO_EINVAL;
    }
    self_index = tr_data_member_index(&config->configuration, config->self_id);
    if (self_index == SIZE_MAX ||
        (config->configuration.members[self_index].roles &
         (TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER)) == 0U) {
        return TURBO_EINVAL;
    }
    quorum = (tr_raft_data_quorum_t *)calloc(1U, sizeof(*quorum));
    if (quorum == NULL) return TURBO_ENOMEM;
    quorum->config = *config;
    *out_quorum = quorum;
    return TURBO_OK;
}

void tr_raft_data_quorum_destroy(tr_raft_data_quorum_t *quorum)
{
    free(quorum);
}

int tr_raft_data_quorum_mark_local_durable(tr_raft_data_quorum_t *quorum)
{
    size_t index;
    if (quorum == NULL) return TURBO_EINVAL;
    index = tr_data_member_index(&quorum->config.configuration,
                                 quorum->config.self_id);
    if (index == SIZE_MAX) return TURBO_EPROTO;
    quorum->durable[index] = true;
    return TURBO_OK;
}

int tr_raft_data_quorum_acknowledge(
    tr_raft_data_quorum_t *quorum, const tr_raft_data_ack_t *ack)
{
    size_t index;
    if (quorum == NULL || ack == NULL) return TURBO_EINVAL;
    if (ack->to != quorum->config.self_id ||
        ack->term != quorum->config.term ||
        ack->stream_id != quorum->config.descriptor.stream_id ||
        ack->stream_size != quorum->config.descriptor.stream_size ||
        ack->next_offset != ack->stream_size || !ack->accepted ||
        !ack->durable ||
        memcmp(ack->stream_digest, quorum->config.descriptor.stream_digest,
               sizeof(ack->stream_digest)) != 0) {
        return TURBO_EPROTO;
    }
    index = tr_data_member_index(&quorum->config.configuration, ack->from);
    if (index == SIZE_MAX) return TURBO_EPROTO;
    quorum->durable[index] = true;
    return TURBO_OK;
}

bool tr_raft_data_quorum_ready(const tr_raft_data_quorum_t *quorum)
{
    size_t old_voters = 0U;
    size_t new_voters = 0U;
    size_t old_durable = 0U;
    size_t new_durable = 0U;
    size_t index;
    if (quorum == NULL) return false;
    for (index = 0U; index < quorum->config.configuration.member_count; ++index) {
        uint8_t roles = quorum->config.configuration.members[index].roles;
        if ((roles & TR_RAFT_CONF_OLD_VOTER) != 0U) {
            ++old_voters;
            old_durable += quorum->durable[index] ? 1U : 0U;
        }
        if ((roles & TR_RAFT_CONF_NEW_VOTER) != 0U) {
            ++new_voters;
            new_durable += quorum->durable[index] ? 1U : 0U;
        }
    }
    if (old_voters == 0U || new_voters == 0U ||
        old_durable < tr_data_majority(old_voters) ||
        new_durable < tr_data_majority(new_voters)) {
        return false;
    }
    /* Every replication target must stage before it can receive and apply the
     * descriptor. A future per-peer AppendEntries gate may safely relax this. */
    for (index = 0U; index < quorum->config.configuration.member_count; ++index) {
        if (!quorum->durable[index]) return false;
    }
    return true;
}

int tr_raft_data_quorum_make_proposal(
    const tr_raft_data_quorum_t *quorum,
    uint64_t command_id,
    uint8_t descriptor_storage[TR_RAFT_DATA_DESCRIPTOR_ENCODED_SIZE],
    tr_raft_proposal_t *out_proposal)
{
    size_t encoded_size = 0U;
    int rc;
    if (quorum == NULL || command_id == 0U || descriptor_storage == NULL ||
        out_proposal == NULL) return TURBO_EINVAL;
    if (!tr_raft_data_quorum_ready(quorum)) return TURBO_EBUSY;
    rc = tr_raft_data_descriptor_encode(
        &quorum->config.descriptor, descriptor_storage,
        TR_RAFT_DATA_DESCRIPTOR_ENCODED_SIZE, &encoded_size);
    if (rc != TURBO_OK) return rc;
    memset(out_proposal, 0, sizeof(*out_proposal));
    out_proposal->command_id = command_id;
    out_proposal->data = descriptor_storage;
    out_proposal->data_length = encoded_size;
    return TURBO_OK;
}

static void tr_data_sender_clear(tr_raft_data_stream_sender_t *sender)
{
    sender->data = NULL;
    sender->size = 0U;
    sender->term = 0U;
    sender->stream_id = 0U;
    sender->acknowledged_offset = 0U;
    sender->next_offset = 0U;
    sender->claim_count = 0U;
    sender->active = false;
    sender->complete = false;
    memset(sender->digest, 0, sizeof(sender->digest));
    memset(sender->claims, 0, sizeof(sender->claims));
}

int tr_raft_data_stream_sender_create(
    const tr_raft_data_stream_sender_config_t *config,
    tr_raft_data_stream_sender_t **out_sender)
{
    tr_raft_data_stream_sender_t *sender;
    size_t chunk_size;
    size_t inflight;

    if (out_sender == NULL) return TURBO_EINVAL;
    *out_sender = NULL;
    if (config == NULL || config->self_id == 0U || config->peer_id == 0U ||
        config->self_id == config->peer_id || config->max_stream_bytes == 0U ||
        config->max_stream_bytes > TR_RAFT_WIRE_MAX_DATA_STREAM_BYTES) {
        return TURBO_EINVAL;
    }
    chunk_size = config->chunk_size == 0U
                     ? TR_RAFT_WIRE_MAX_DATA_CHUNK_BYTES
                     : config->chunk_size;
    inflight = config->max_inflight_chunks == 0U
                   ? TR_RAFT_DATA_STREAM_DEFAULT_INFLIGHT_CHUNKS
                   : config->max_inflight_chunks;
    if (chunk_size == 0U ||
        chunk_size > TR_RAFT_WIRE_MAX_DATA_CHUNK_BYTES ||
        inflight == 0U ||
        inflight > TR_RAFT_DATA_STREAM_MAX_INFLIGHT_CHUNKS ||
        chunk_size > SIZE_MAX / inflight) {
        return TURBO_EINVAL;
    }
    sender = (tr_raft_data_stream_sender_t *)calloc(1U, sizeof(*sender));
    if (sender == NULL) return TURBO_ENOMEM;
    sender->config = *config;
    sender->chunk_size = chunk_size;
    sender->max_inflight_chunks = inflight;
    *out_sender = sender;
    return TURBO_OK;
}

void tr_raft_data_stream_sender_destroy(tr_raft_data_stream_sender_t *sender)
{
    if (sender != NULL) {
        tr_data_sender_clear(sender);
        free(sender);
    }
}

void tr_raft_data_stream_sender_reset(tr_raft_data_stream_sender_t *sender)
{
    if (sender != NULL) tr_data_sender_clear(sender);
}

int tr_raft_data_stream_sender_begin(
    tr_raft_data_stream_sender_t *sender,
    tr_raft_term_t term,
    uint64_t stream_id,
    const uint8_t *data,
    size_t size)
{
    if (sender == NULL || term == 0U || stream_id == 0U ||
        size > sender->config.max_stream_bytes || (size != 0U && data == NULL)) {
        return TURBO_EINVAL;
    }
    if (sender->active && !sender->complete) return TURBO_EBUSY;
    tr_data_sender_clear(sender);
    sender->data = data;
    sender->size = size;
    sender->term = term;
    sender->stream_id = stream_id;
    SHA256(size == 0U ? (const uint8_t *)"" : data, size, sender->digest);
    sender->active = true;
    return TURBO_OK;
}

int tr_raft_data_stream_sender_next(
    tr_raft_data_stream_sender_t *sender,
    tr_raft_data_chunk_t *out_chunk)
{
    size_t remaining;
    size_t length;
    tr_data_claim_t *claim;

    if (sender == NULL || out_chunk == NULL) return TURBO_EINVAL;
    if (!sender->active || sender->complete ||
        sender->claim_count >= sender->max_inflight_chunks ||
        (sender->next_offset == sender->size && sender->claim_count != 0U)) {
        return TURBO_EBUSY;
    }
    remaining = sender->size - (size_t)sender->next_offset;
    length = remaining < sender->chunk_size ? remaining : sender->chunk_size;
    memset(out_chunk, 0, sizeof(*out_chunk));
    out_chunk->from = sender->config.self_id;
    out_chunk->to = sender->config.peer_id;
    out_chunk->term = sender->term;
    out_chunk->stream_id = sender->stream_id;
    out_chunk->stream_offset = sender->next_offset;
    out_chunk->stream_size = sender->size;
    memcpy(out_chunk->stream_digest, sender->digest, sizeof(sender->digest));
    out_chunk->data = length == 0U ? NULL : sender->data + sender->next_offset;
    out_chunk->data_length = length;
    out_chunk->done = sender->next_offset + length == sender->size;
    claim = &sender->claims[sender->claim_count++];
    claim->offset = sender->next_offset;
    claim->length = length;
    sender->next_offset += length;
    return TURBO_OK;
}

int tr_raft_data_stream_sender_cancel(
    tr_raft_data_stream_sender_t *sender, uint64_t stream_offset)
{
    tr_data_claim_t *claim;
    if (sender == NULL || sender->claim_count == 0U) return TURBO_EINVAL;
    claim = &sender->claims[sender->claim_count - 1U];
    if (claim->offset != stream_offset) return TURBO_EPROTO;
    sender->next_offset = claim->offset;
    memset(claim, 0, sizeof(*claim));
    --sender->claim_count;
    return TURBO_OK;
}

int tr_raft_data_stream_sender_resume(tr_raft_data_stream_sender_t *sender)
{
    if (sender == NULL) return TURBO_EINVAL;
    if (!sender->active || sender->complete) return TURBO_EBUSY;
    sender->next_offset = sender->acknowledged_offset;
    sender->claim_count = 0U;
    memset(sender->claims, 0, sizeof(sender->claims));
    return TURBO_OK;
}

int tr_raft_data_stream_sender_acknowledge(
    tr_raft_data_stream_sender_t *sender, const tr_raft_data_ack_t *ack)
{
    size_t released = 0U;
    bool boundary = false;

    if (sender == NULL || ack == NULL) return TURBO_EINVAL;
    if (!sender->active) return TURBO_EBUSY;
    if (ack->from != sender->config.peer_id ||
        ack->to != sender->config.self_id || ack->term != sender->term ||
        ack->stream_id != sender->stream_id || ack->stream_size != sender->size ||
        memcmp(ack->stream_digest, sender->digest, sizeof(sender->digest)) != 0 ||
        ack->next_offset > sender->next_offset) {
        return TURBO_EPROTO;
    }
    if (!ack->accepted) {
        if (ack->durable || ack->next_offset > sender->acknowledged_offset)
            return TURBO_EPROTO;
        sender->acknowledged_offset = ack->next_offset;
        return tr_raft_data_stream_sender_resume(sender);
    }
    if (ack->next_offset < sender->acknowledged_offset) return TURBO_EPROTO;
    boundary = ack->next_offset == sender->acknowledged_offset;
    for (released = 0U; released < sender->claim_count; ++released) {
        if (sender->claims[released].offset + sender->claims[released].length ==
            ack->next_offset) {
            boundary = true;
            break;
        }
    }
    if (!boundary) return TURBO_EPROTO;
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
    if (ack->next_offset == sender->size) {
        if (!ack->durable) return TURBO_EPROTO;
        sender->complete = true;
    } else if (ack->durable) {
        return TURBO_EPROTO;
    }
    return TURBO_OK;
}

int tr_raft_data_stream_sender_get_status(
    const tr_raft_data_stream_sender_t *sender,
    tr_raft_data_stream_sender_status_t *out_status)
{
    if (sender == NULL || out_status == NULL) return TURBO_EINVAL;
    memset(out_status, 0, sizeof(*out_status));
    out_status->active = sender->active;
    out_status->complete = sender->complete;
    out_status->stream_id = sender->stream_id;
    out_status->stream_size = sender->size;
    out_status->acknowledged_offset = sender->acknowledged_offset;
    out_status->next_offset = sender->next_offset;
    out_status->inflight_chunks = sender->claim_count;
    return TURBO_OK;
}

static void tr_data_receiver_clear(tr_raft_data_stream_receiver_t *receiver)
{
    if (receiver->active && !receiver->committed)
        receiver->config.sink.abort(receiver->config.sink.context);
    receiver->leader_id = 0U;
    receiver->term = 0U;
    receiver->stream_id = 0U;
    receiver->stream_size = 0U;
    receiver->next_offset = 0U;
    receiver->active = false;
    receiver->committed = false;
    memset(receiver->digest, 0, sizeof(receiver->digest));
}

int tr_raft_data_stream_receiver_create(
    const tr_raft_data_stream_receiver_config_t *config,
    tr_raft_data_stream_receiver_t **out_receiver)
{
    tr_raft_data_stream_receiver_t *receiver;
    if (out_receiver == NULL) return TURBO_EINVAL;
    *out_receiver = NULL;
    if (config == NULL || config->self_id == 0U ||
        config->max_stream_bytes == 0U ||
        config->max_stream_bytes > TR_RAFT_WIRE_MAX_DATA_STREAM_BYTES ||
        config->sink.begin == NULL || config->sink.write == NULL ||
        config->sink.commit == NULL || config->sink.abort == NULL) {
        return TURBO_EINVAL;
    }
    receiver = (tr_raft_data_stream_receiver_t *)calloc(1U, sizeof(*receiver));
    if (receiver == NULL) return TURBO_ENOMEM;
    receiver->config = *config;
    *out_receiver = receiver;
    return TURBO_OK;
}

void tr_raft_data_stream_receiver_destroy(
    tr_raft_data_stream_receiver_t *receiver)
{
    if (receiver != NULL) {
        tr_data_receiver_clear(receiver);
        free(receiver);
    }
}

void tr_raft_data_stream_receiver_reset(
    tr_raft_data_stream_receiver_t *receiver)
{
    if (receiver != NULL) tr_data_receiver_clear(receiver);
}

int tr_raft_data_stream_receiver_handle(
    tr_raft_data_stream_receiver_t *receiver,
    const tr_raft_data_chunk_t *chunk,
    tr_raft_data_stream_receive_result_t *out_result)
{
    uint8_t digest[SHA256_DIGEST_LENGTH];
    int rc;

    if (receiver == NULL || chunk == NULL || out_result == NULL)
        return TURBO_EINVAL;
    memset(out_result, 0, sizeof(*out_result));
    out_result->ack.from = receiver->config.self_id;
    out_result->ack.to = chunk->from;
    out_result->ack.term = chunk->term;
    out_result->ack.stream_id = chunk->stream_id;
    out_result->ack.stream_size = chunk->stream_size;
    memcpy(out_result->ack.stream_digest, chunk->stream_digest,
           sizeof(out_result->ack.stream_digest));
    if (chunk->from == 0U || chunk->to != receiver->config.self_id ||
        chunk->term == 0U || chunk->stream_id == 0U ||
        chunk->stream_size > receiver->config.max_stream_bytes ||
        chunk->stream_offset > chunk->stream_size ||
        chunk->data_length > TR_RAFT_WIRE_MAX_DATA_CHUNK_BYTES ||
        (chunk->data_length != 0U && chunk->data == NULL) ||
        chunk->data_length > chunk->stream_size - chunk->stream_offset ||
        chunk->done !=
            (chunk->stream_offset + chunk->data_length == chunk->stream_size)) {
        return TURBO_EPROTO;
    }
    if (!receiver->active) {
        if (chunk->stream_offset != 0U) return TURBO_EPROTO;
        receiver->leader_id = chunk->from;
        receiver->term = chunk->term;
        receiver->stream_id = chunk->stream_id;
        receiver->stream_size = chunk->stream_size;
        memcpy(receiver->digest, chunk->stream_digest, sizeof(receiver->digest));
        if (SHA256_Init(&receiver->sha256) != 1) return TURBO_EPROTO;
        rc = receiver->config.sink.begin(
            receiver->config.sink.context, chunk->from, chunk->term,
            chunk->stream_id, chunk->stream_size, chunk->stream_digest);
        if (rc != TURBO_OK) {
            tr_data_receiver_clear(receiver);
            return rc;
        }
        receiver->active = true;
    } else if (receiver->leader_id != chunk->from ||
               receiver->term != chunk->term ||
               receiver->stream_id != chunk->stream_id ||
               receiver->stream_size != chunk->stream_size ||
               memcmp(receiver->digest, chunk->stream_digest,
                      sizeof(receiver->digest)) != 0) {
        return TURBO_EPROTO;
    }
    if (receiver->committed) {
        out_result->ack.next_offset = receiver->next_offset;
        out_result->ack.accepted = true;
        out_result->ack.durable = true;
        return TURBO_OK;
    }
    if (chunk->stream_offset < receiver->next_offset) {
        out_result->ack.next_offset = receiver->next_offset;
        out_result->ack.accepted = true;
        return TURBO_OK;
    }
    if (chunk->stream_offset != receiver->next_offset) {
        out_result->ack.next_offset = receiver->next_offset;
        return TURBO_EPROTO;
    }
    if (chunk->data_length != 0U) {
        if (SHA256_Update(&receiver->sha256, chunk->data,
                          chunk->data_length) != 1) {
            tr_data_receiver_clear(receiver);
            return TURBO_EPROTO;
        }
        rc = receiver->config.sink.write(receiver->config.sink.context,
                                         receiver->next_offset, chunk->data,
                                         chunk->data_length);
        if (rc != TURBO_OK) {
            tr_data_receiver_clear(receiver);
            return rc;
        }
    }
    receiver->next_offset += chunk->data_length;
    out_result->ack.next_offset = receiver->next_offset;
    out_result->ack.accepted = true;
    if (!chunk->done) return TURBO_OK;
    if (SHA256_Final(digest, &receiver->sha256) != 1 ||
        memcmp(digest, receiver->digest, sizeof(digest)) != 0) {
        out_result->ack.accepted = false;
        out_result->ack.next_offset = 0U;
        tr_data_receiver_clear(receiver);
        return TURBO_EPROTO;
    }
    rc = receiver->config.sink.commit(receiver->config.sink.context);
    if (rc != TURBO_OK) {
        out_result->ack.accepted = false;
        out_result->ack.next_offset = 0U;
        tr_data_receiver_clear(receiver);
        return rc;
    }
    receiver->committed = true;
    out_result->committed = true;
    out_result->ack.durable = true;
    return TURBO_OK;
}
