#include <turboraft/raft_transport.h>

#include <salts_error.h>

#include <limits.h>
#include <stdlib.h>
#include <string.h>

struct tr_raft_transport_session {
    int callback_active;
    int destroy_pending;
    tr_raft_wire_codec_t *codec;
    tr_raft_cluster_id_t cluster_id;
    tr_raft_node_id_t local_node_id;
    tr_raft_node_id_t peer_node_id;
    uint32_t snapshot_chunk_size;
    uint32_t max_frame_size;
    uint64_t next_outbound_message_id;
    uint64_t last_inbound_message_id;
    int outbound_ids_exhausted;
    tr_raft_transport_payload_handler_fn on_payload;
    void *payload_context;
    tr_raft_transport_status_t status;
    uint8_t length_prefix[TR_RAFT_TRANSPORT_LENGTH_PREFIX_SIZE];
    size_t length_prefix_used;
    uint8_t frame[TR_RAFT_WIRE_MAX_FRAME_SIZE];
    size_t expected_frame_size;
    size_t frame_used;
};

static void tr_raft_transport_session_finalize(
    tr_raft_transport_session_t *session)
{
    tr_raft_wire_codec_destroy(session->codec);
    free(session);
}

static uint32_t tr_raft_transport_read_u32_be(const uint8_t *input)
{
    return ((uint32_t) input[0] << 24U) |
           ((uint32_t) input[1] << 16U) |
           ((uint32_t) input[2] << 8U) |
           (uint32_t) input[3];
}

static void tr_raft_transport_write_u32_be(uint8_t *output, uint32_t value)
{
    output[0] = (uint8_t) (value >> 24U);
    output[1] = (uint8_t) (value >> 16U);
    output[2] = (uint8_t) (value >> 8U);
    output[3] = (uint8_t) value;
}

static int tr_raft_transport_is_active(
    const tr_raft_transport_session_t *session)
{
    return session->status.state == TR_RAFT_TRANSPORT_STATE_READY;
}

static int tr_raft_transport_fault(tr_raft_transport_session_t *session,
                                   int error)
{
    session->status.state = TR_RAFT_TRANSPORT_STATE_FAULTED;
    return error;
}

static int tr_raft_transport_group_routing_rejection(int error)
{
    return error == SALTS_ENOENT || error == SALTS_ESHUTDOWN;
}

static int tr_raft_transport_cluster_id_is_valid(
    const tr_raft_cluster_id_t *cluster_id)
{
    size_t index;

    for (index = 0U; index < sizeof(cluster_id->bytes); ++index) {
        if (cluster_id->bytes[index] != 0U) {
            return 1;
        }
    }
    return 0;
}

static int tr_raft_transport_dispatch_frame(
    tr_raft_transport_session_t *session)
{
    tr_raft_wire_metadata_t metadata;
    tr_raft_transport_payload_t payload;
    tr_raft_wire_payload_kind_t kind;
    tr_raft_node_id_t from;
    tr_raft_node_id_t to;
    int result;

    memset(&metadata, 0, sizeof(metadata));
    memset(&payload, 0, sizeof(payload));

    result = tr_raft_wire_peek_payload_kind(
        session->frame, session->expected_frame_size, &kind);
    if (result != SALTS_OK) {
        return tr_raft_transport_fault(session, result);
    }

    payload.kind = kind;
    switch (kind) {
    case TR_RAFT_WIRE_PAYLOAD_RAFT:
        result = tr_raft_wire_decode(
            session->codec, session->frame, session->expected_frame_size,
            &metadata, &payload.data.raft);
        from = payload.data.raft.from;
        to = payload.data.raft.to;
        break;
    case TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_CHUNK:
        result = tr_raft_wire_decode_snapshot_chunk(
            session->codec, session->frame, session->expected_frame_size,
            &metadata, &payload.data.snapshot_chunk);
        from = payload.data.snapshot_chunk.from;
        to = payload.data.snapshot_chunk.to;
        if (result == SALTS_OK &&
            payload.data.snapshot_chunk.data_length >
                session->snapshot_chunk_size) {
            result = SALTS_EPROTO;
        }
        break;
    case TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_ACK:
        result = tr_raft_wire_decode_snapshot_ack(
            session->codec, session->frame, session->expected_frame_size,
            &metadata, &payload.data.snapshot_ack);
        from = payload.data.snapshot_ack.from;
        to = payload.data.snapshot_ack.to;
        break;
    case TR_RAFT_WIRE_PAYLOAD_DATA_CHUNK:
        result = tr_raft_wire_decode_data_chunk(
            session->codec, session->frame, session->expected_frame_size,
            &metadata, &payload.data.data_chunk);
        from = payload.data.data_chunk.from;
        to = payload.data.data_chunk.to;
        break;
    case TR_RAFT_WIRE_PAYLOAD_DATA_ACK:
        result = tr_raft_wire_decode_data_ack(
            session->codec, session->frame, session->expected_frame_size,
            &metadata, &payload.data.data_ack);
        from = payload.data.data_ack.from;
        to = payload.data.data_ack.to;
        break;
    default:
        return tr_raft_transport_fault(session, SALTS_EPROTO);
    }
    if (result != SALTS_OK) {
        return tr_raft_transport_fault(session, result);
    }

    payload.group_id = metadata.group_id;
    if (memcmp(metadata.cluster_id.bytes, session->cluster_id.bytes,
               sizeof(metadata.cluster_id.bytes)) != 0 ||
        metadata.group_id == 0U || metadata.message_id == 0U ||
        metadata.message_id <= session->last_inbound_message_id ||
        from != session->peer_node_id || to != session->local_node_id) {
        return tr_raft_transport_fault(session, SALTS_EPROTO);
    }

    session->last_inbound_message_id = metadata.message_id;
    session->status.last_inbound_message_id = metadata.message_id;
    session->status.frames_decoded++;
    session->callback_active = 1;
    result = session->on_payload(session->payload_context, &payload);
    session->callback_active = 0;

    if (session->destroy_pending) {
        tr_raft_transport_session_finalize(session);
        return SALTS_ECANCELED;
    }
    if (tr_raft_transport_group_routing_rejection(result)) {
        ++session->status.group_routing_rejections;
        session->status.last_rejected_group_id = metadata.group_id;
        session->status.last_group_routing_error = result;
        return SALTS_OK;
    }
    if (result != SALTS_OK) {
        return tr_raft_transport_fault(session, result);
    }
    return SALTS_OK;
}

int tr_raft_transport_session_create(
    const tr_raft_transport_session_config_t *config,
    tr_raft_transport_session_t **out_session)
{
    tr_raft_transport_session_t *session;
    int result;

    if (config == NULL || out_session == NULL ||
        config->handshake == NULL || config->on_payload == NULL ||
        !tr_raft_transport_cluster_id_is_valid(&config->cluster_id) ||
        config->local_node_id == 0U || config->peer_node_id == 0U ||
        config->local_node_id == config->peer_node_id ||
        config->first_outbound_message_id == 0U) {
        return SALTS_EINVAL;
    }

    result = tr_raft_handshake_result_validate(
        config->handshake, &config->cluster_id, config->local_node_id,
        config->peer_node_id);
    if (result != SALTS_OK) {
        return result;
    }
    if (config->handshake->feature_bits != 0U ||
        config->handshake->wire_major != TR_RAFT_HANDSHAKE_WIRE_MAJOR ||
        config->handshake->wire_minor != TR_RAFT_HANDSHAKE_WIRE_MINOR ||
        config->handshake->max_frame_size < TR_RAFT_WIRE_MAX_FRAME_SIZE ||
        config->handshake->max_snapshot_chunk_size <
            TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES) {
        return SALTS_EPROTONOSUPPORT;
    }

    session = (tr_raft_transport_session_t *) calloc(1U, sizeof(*session));
    if (session == NULL) {
        return SALTS_ENOMEM;
    }
    result = tr_raft_wire_codec_create(&session->codec);
    if (result != SALTS_OK) {
        free(session);
        return result;
    }

    session->cluster_id = config->cluster_id;
    session->local_node_id = config->local_node_id;
    session->peer_node_id = config->peer_node_id;
    session->snapshot_chunk_size = TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES;
    session->max_frame_size = config->handshake->max_frame_size;
    session->next_outbound_message_id = config->first_outbound_message_id;
    session->on_payload = config->on_payload;
    session->payload_context = config->payload_context;
    session->status.state = TR_RAFT_TRANSPORT_STATE_READY;
    *out_session = session;
    return SALTS_OK;
}

int tr_raft_transport_session_destroy(tr_raft_transport_session_t *session)
{
    if (session == NULL) {
        return SALTS_OK;
    }
    if (session->callback_active) {
        session->destroy_pending = 1;
        session->status.state = TR_RAFT_TRANSPORT_STATE_FAULTED;
        return SALTS_OK;
    }
    tr_raft_transport_session_finalize(session);
    return SALTS_OK;
}

static int tr_raft_transport_payload_nodes(
    const tr_raft_transport_payload_t *payload,
    tr_raft_node_id_t *out_from,
    tr_raft_node_id_t *out_to)
{
    if (payload == NULL || out_from == NULL || out_to == NULL) {
        return SALTS_EINVAL;
    }
    switch (payload->kind) {
    case TR_RAFT_WIRE_PAYLOAD_RAFT:
        *out_from = payload->data.raft.from;
        *out_to = payload->data.raft.to;
        return SALTS_OK;
    case TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_CHUNK:
        *out_from = payload->data.snapshot_chunk.from;
        *out_to = payload->data.snapshot_chunk.to;
        return SALTS_OK;
    case TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_ACK:
        *out_from = payload->data.snapshot_ack.from;
        *out_to = payload->data.snapshot_ack.to;
        return SALTS_OK;
    case TR_RAFT_WIRE_PAYLOAD_DATA_CHUNK:
        *out_from = payload->data.data_chunk.from;
        *out_to = payload->data.data_chunk.to;
        return SALTS_OK;
    case TR_RAFT_WIRE_PAYLOAD_DATA_ACK:
        *out_from = payload->data.data_ack.from;
        *out_to = payload->data.data_ack.to;
        return SALTS_OK;
    default:
        return SALTS_EPROTO;
    }
}

int tr_raft_transport_encode_payload(
    tr_raft_transport_session_t *session,
    const tr_raft_transport_payload_t *payload,
    uint8_t *output,
    size_t output_capacity,
    size_t *output_size)
{
    tr_raft_wire_metadata_t metadata;
    tr_raft_node_id_t from;
    tr_raft_node_id_t to;
    size_t frame_size = 0U;
    int result;

    if (session == NULL || payload == NULL || payload->group_id == 0U ||
        output == NULL || output_size == NULL ||
        output_capacity < TR_RAFT_TRANSPORT_LENGTH_PREFIX_SIZE) {
        return SALTS_EINVAL;
    }
    result = tr_raft_transport_payload_nodes(payload, &from, &to);
    if (result != SALTS_OK) {
        return result;
    }
    if (!tr_raft_transport_is_active(session) ||
        session->outbound_ids_exhausted ||
        from != session->local_node_id || to != session->peer_node_id) {
        return SALTS_EPROTO;
    }

    memset(&metadata, 0, sizeof(metadata));
    metadata.cluster_id = session->cluster_id;
    metadata.group_id = payload->group_id;
    metadata.message_id = session->next_outbound_message_id;

    switch (payload->kind) {
    case TR_RAFT_WIRE_PAYLOAD_RAFT:
        result = tr_raft_wire_encode(
            session->codec, &metadata, &payload->data.raft,
            output + TR_RAFT_TRANSPORT_LENGTH_PREFIX_SIZE,
            output_capacity - TR_RAFT_TRANSPORT_LENGTH_PREFIX_SIZE,
            &frame_size);
        break;
    case TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_CHUNK:
        if (payload->data.snapshot_chunk.data_length >
            session->snapshot_chunk_size) {
            return SALTS_EPROTONOSUPPORT;
        }
        result = tr_raft_wire_encode_snapshot_chunk(
            session->codec, &metadata, &payload->data.snapshot_chunk,
            output + TR_RAFT_TRANSPORT_LENGTH_PREFIX_SIZE,
            output_capacity - TR_RAFT_TRANSPORT_LENGTH_PREFIX_SIZE,
            &frame_size);
        break;
    case TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_ACK:
        result = tr_raft_wire_encode_snapshot_ack(
            session->codec, &metadata, &payload->data.snapshot_ack,
            output + TR_RAFT_TRANSPORT_LENGTH_PREFIX_SIZE,
            output_capacity - TR_RAFT_TRANSPORT_LENGTH_PREFIX_SIZE,
            &frame_size);
        break;
    case TR_RAFT_WIRE_PAYLOAD_DATA_CHUNK:
        result = tr_raft_wire_encode_data_chunk(
            session->codec, &metadata, &payload->data.data_chunk,
            output + TR_RAFT_TRANSPORT_LENGTH_PREFIX_SIZE,
            output_capacity - TR_RAFT_TRANSPORT_LENGTH_PREFIX_SIZE,
            &frame_size);
        break;
    case TR_RAFT_WIRE_PAYLOAD_DATA_ACK:
        result = tr_raft_wire_encode_data_ack(
            session->codec, &metadata, &payload->data.data_ack,
            output + TR_RAFT_TRANSPORT_LENGTH_PREFIX_SIZE,
            output_capacity - TR_RAFT_TRANSPORT_LENGTH_PREFIX_SIZE,
            &frame_size);
        break;
    default:
        return SALTS_EPROTO;
    }
    if (result != SALTS_OK) {
        return result;
    }
    if (frame_size > UINT32_MAX) {
        return SALTS_EPROTO;
    }

    tr_raft_transport_write_u32_be(output, (uint32_t) frame_size);
    *output_size = frame_size + TR_RAFT_TRANSPORT_LENGTH_PREFIX_SIZE;
    session->status.last_outbound_message_id =
        session->next_outbound_message_id;
    if (session->next_outbound_message_id == UINT64_MAX) {
        session->outbound_ids_exhausted = 1;
    } else {
        session->next_outbound_message_id++;
    }
    session->status.frames_encoded++;
    session->status.bytes_encoded += *output_size;
    return SALTS_OK;
}

int tr_raft_transport_encode(
    tr_raft_transport_session_t *session,
    tr_raft_group_id_t group_id,
    const tr_raft_message_t *message,
    uint8_t *output,
    size_t output_capacity,
    size_t *output_size)
{
    tr_raft_transport_payload_t payload;

    if (message == NULL || group_id == 0U) {
        return SALTS_EINVAL;
    }
    memset(&payload, 0, sizeof(payload));
    payload.group_id = group_id;
    payload.kind = TR_RAFT_WIRE_PAYLOAD_RAFT;
    payload.data.raft = *message;
    return tr_raft_transport_encode_payload(
        session, &payload, output, output_capacity, output_size);
}

int tr_raft_transport_feed(tr_raft_transport_session_t *session,
                           const uint8_t *data,
                           size_t size)
{
    size_t offset = 0U;

    if (session == NULL || (data == NULL && size != 0U)) {
        return SALTS_EINVAL;
    }
    if (!tr_raft_transport_is_active(session)) {
        return SALTS_EPROTO;
    }
    session->status.bytes_decoded += size;

    while (offset < size) {
        size_t available;
        size_t required;
        size_t copied;
        int result;

        if (session->length_prefix_used <
            TR_RAFT_TRANSPORT_LENGTH_PREFIX_SIZE) {
            required = TR_RAFT_TRANSPORT_LENGTH_PREFIX_SIZE -
                       session->length_prefix_used;
            available = size - offset;
            copied = required < available ? required : available;
            memcpy(session->length_prefix + session->length_prefix_used,
                   data + offset, copied);
            session->length_prefix_used += copied;
            offset += copied;
            if (session->length_prefix_used <
                TR_RAFT_TRANSPORT_LENGTH_PREFIX_SIZE) {
                continue;
            }
            session->expected_frame_size =
                (size_t) tr_raft_transport_read_u32_be(session->length_prefix);
            if (session->expected_frame_size < TR_RAFT_WIRE_HEADER_SIZE ||
                session->expected_frame_size > session->max_frame_size ||
                session->expected_frame_size > TR_RAFT_WIRE_MAX_FRAME_SIZE) {
                return tr_raft_transport_fault(session, SALTS_EPROTO);
            }
        }

        required = session->expected_frame_size - session->frame_used;
        available = size - offset;
        copied = required < available ? required : available;
        memcpy(session->frame + session->frame_used, data + offset, copied);
        session->frame_used += copied;
        offset += copied;
        if (session->frame_used < session->expected_frame_size) {
            continue;
        }

        result = tr_raft_transport_dispatch_frame(session);
        if (result != SALTS_OK) {
            return result;
        }
        session->length_prefix_used = 0U;
        session->expected_frame_size = 0U;
        session->frame_used = 0U;
    }
    return SALTS_OK;
}

int tr_raft_transport_get_status(const tr_raft_transport_session_t *session,
                                 tr_raft_transport_status_t *out_status)
{
    if (session == NULL || out_status == NULL) {
        return SALTS_EINVAL;
    }
    *out_status = session->status;
    return SALTS_OK;
}
