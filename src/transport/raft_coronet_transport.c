#include <turboraft/raft_coronet_transport.h>

#include "raft_coronet_transport_internal.h"

#include <turbo_error.h>

#include <limits.h>
#include <stdlib.h>
#include <string.h>

struct tr_raft_coronet_session {
    coro_socket_t *socket;
    int owns_socket;
    int callback_active;
    int destroy_pending;
    tr_raft_coronet_peer_manager_t *owner_manager;
    tr_raft_wire_codec_t *codec;
    tr_raft_cluster_id_t cluster_id;
    tr_raft_node_id_t local_node_id;
    tr_raft_node_id_t peer_node_id;
    uint16_t raft_wire_version;
    uint16_t snapshot_wire_version;
    uint32_t snapshot_chunk_size;
    uint32_t max_frame_size;
    uint64_t next_outbound_message_id;
    uint64_t last_inbound_message_id;
    int outbound_ids_exhausted;
    tr_raft_coronet_message_handler_fn on_message;
    void *message_context;
    tr_raft_coronet_snapshot_handler_fn on_snapshot;
    void *snapshot_context;
    tr_raft_coronet_status_t status;
    uint8_t length_prefix[TR_RAFT_CORONET_LENGTH_PREFIX_SIZE];
    size_t length_prefix_used;
    uint8_t frame[TR_RAFT_WIRE_MAX_FRAME_SIZE];
    size_t expected_frame_size;
    size_t frame_used;
    uint8_t outbound_packet[TR_RAFT_CORONET_MAX_PACKET_SIZE];
};

struct tr_raft_coronet_peer_manager {
    tr_raft_cluster_id_t cluster_id;
    tr_raft_node_id_t local_node_id;
    size_t peer_count;
    tr_raft_node_id_t peer_node_ids[TR_RAFT_CORONET_MAX_PEERS];
    tr_raft_coronet_session_t *sessions[TR_RAFT_CORONET_MAX_PEERS];
    size_t callback_depth;
};

static void tr_raft_coronet_session_finalize(
    tr_raft_coronet_session_t *session)
{
    if (session->owns_socket && session->socket != NULL) {
        coro_socket_destroy(session->socket);
        session->socket = NULL;
    }
    tr_raft_wire_codec_destroy(session->codec);
    free(session);
}

static void tr_raft_coronet_peer_manager_unlink_session(
    tr_raft_coronet_session_t *session)
{
    tr_raft_coronet_peer_manager_t *manager = session->owner_manager;
    size_t index;

    if (manager == NULL) {
        return;
    }
    for (index = 0U; index < manager->peer_count; ++index) {
        if (manager->sessions[index] == session) {
            manager->sessions[index] = NULL;
            break;
        }
    }
    session->owner_manager = NULL;
}

static uint32_t tr_raft_coronet_read_u32_be(const uint8_t *input)
{
    return ((uint32_t) input[0] << 24U) |
           ((uint32_t) input[1] << 16U) |
           ((uint32_t) input[2] << 8U) |
           (uint32_t) input[3];
}

static void tr_raft_coronet_write_u32_be(uint8_t *output, uint32_t value)
{
    output[0] = (uint8_t) (value >> 24U);
    output[1] = (uint8_t) (value >> 16U);
    output[2] = (uint8_t) (value >> 8U);
    output[3] = (uint8_t) value;
}

static int tr_raft_coronet_is_active(const tr_raft_coronet_session_t *session)
{
    return session->status.state == TR_RAFT_CORONET_STATE_DETACHED ||
           session->status.state == TR_RAFT_CORONET_STATE_CONNECTED;
}

static int tr_raft_coronet_fault(tr_raft_coronet_session_t *session, int error)
{
    session->status.state = TR_RAFT_CORONET_STATE_FAULTED;
    return error;
}

static int tr_raft_coronet_cluster_id_is_valid(
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

static size_t tr_raft_coronet_peer_manager_find(
    const tr_raft_coronet_peer_manager_t *manager,
    tr_raft_node_id_t peer_node_id)
{
    size_t first = 0U;
    size_t count = manager->peer_count;

    while (count != 0U) {
        size_t step = count / 2U;
        size_t index = first + step;

        if (manager->peer_node_ids[index] < peer_node_id) {
            first = index + 1U;
            count -= step + 1U;
        } else {
            count = step;
        }
    }
    if (first < manager->peer_count &&
        manager->peer_node_ids[first] == peer_node_id) {
        return first;
    }
    return SIZE_MAX;
}

static int tr_raft_coronet_dispatch_frame(tr_raft_coronet_session_t *session)
{
    tr_raft_coronet_peer_manager_t *manager;
    tr_raft_wire_metadata_t metadata;
    tr_raft_coronet_payload_t payload;
    tr_raft_wire_payload_kind_t kind;
    uint16_t wire_version;
    tr_raft_node_id_t from;
    tr_raft_node_id_t to;
    int result;

    memset(&metadata, 0, sizeof(metadata));
    memset(&payload, 0, sizeof(payload));
    result = tr_raft_wire_peek_payload_kind(
        session->frame, session->expected_frame_size, &kind);
    if (result != TURBO_OK) {
        return tr_raft_coronet_fault(session, result);
    }
    result = tr_raft_wire_peek_version(
        session->frame, session->expected_frame_size, &wire_version);
    if (result != TURBO_OK ||
        (kind == TR_RAFT_WIRE_PAYLOAD_RAFT &&
         wire_version != session->raft_wire_version) ||
        (kind != TR_RAFT_WIRE_PAYLOAD_RAFT &&
         wire_version != session->snapshot_wire_version)) {
        return tr_raft_coronet_fault(session, TURBO_EPROTO);
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
        if (result == TURBO_OK &&
            payload.data.snapshot_chunk.data_length >
                session->snapshot_chunk_size) {
            result = TURBO_EPROTO;
        }
        break;
    case TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_ACK:
        result = tr_raft_wire_decode_snapshot_ack(
            session->codec, session->frame, session->expected_frame_size,
            &metadata, &payload.data.snapshot_ack);
        from = payload.data.snapshot_ack.from;
        to = payload.data.snapshot_ack.to;
        break;
    default:
        return tr_raft_coronet_fault(session, TURBO_EPROTO);
    }
    if (result != TURBO_OK) {
        return tr_raft_coronet_fault(session, result);
    }
    if (memcmp(metadata.cluster_id.bytes, session->cluster_id.bytes,
               sizeof(metadata.cluster_id.bytes)) != 0 ||
        metadata.message_id == 0U ||
        metadata.message_id <= session->last_inbound_message_id ||
        from != session->peer_node_id || to != session->local_node_id ||
        (kind != TR_RAFT_WIRE_PAYLOAD_RAFT &&
         session->on_snapshot == NULL)) {
        return tr_raft_coronet_fault(session, TURBO_EPROTO);
    }

    session->last_inbound_message_id = metadata.message_id;
    session->status.last_inbound_message_id = metadata.message_id;
    session->status.frames_received++;
    manager = session->owner_manager;
    if (manager != NULL) {
        manager->callback_depth++;
    }
    session->callback_active = 1;
    result = kind == TR_RAFT_WIRE_PAYLOAD_RAFT
                 ? session->on_message(session->message_context,
                                       &payload.data.raft)
                 : session->on_snapshot(session->snapshot_context, &payload);
    session->callback_active = 0;
    if (manager != NULL) {
        manager->callback_depth--;
    }
    if (session->destroy_pending) {
        tr_raft_coronet_peer_manager_unlink_session(session);
        tr_raft_coronet_session_finalize(session);
        return TURBO_ECANCELED;
    }
    if (result != TURBO_OK) {
        return tr_raft_coronet_fault(session, result);
    }
    return TURBO_OK;
}

int tr_raft_coronet_session_create(
    const tr_raft_coronet_session_config_t *config,
    tr_raft_coronet_session_t **out_session)
{
    tr_raft_coronet_session_t *session;
    int result;

    if (config == NULL || out_session == NULL || config->on_message == NULL ||
        !tr_raft_coronet_cluster_id_is_valid(&config->cluster_id) ||
        config->local_node_id == 0U || config->peer_node_id == 0U ||
        config->local_node_id == config->peer_node_id ||
        config->first_outbound_message_id == 0U) {
        return TURBO_EINVAL;
    }
    if (config->owns_socket && config->socket == NULL) {
        return TURBO_EINVAL;
    }
    if (config->socket != NULL && config->handshake == NULL) {
        return TURBO_EPROTO;
    }
    if (config->handshake != NULL &&
        tr_raft_handshake_result_validate(
            config->handshake, &config->cluster_id, config->local_node_id,
            config->peer_node_id) != TURBO_OK) {
        return TURBO_EPROTO;
    }

    session = (tr_raft_coronet_session_t *) calloc(1U, sizeof(*session));
    if (session == NULL) {
        return TURBO_ENOMEM;
    }
    result = tr_raft_wire_codec_create(&session->codec);
    if (result != TURBO_OK) {
        free(session);
        return result;
    }

    session->socket = config->socket;
    session->owns_socket = config->owns_socket != 0;
    session->cluster_id = config->cluster_id;
    session->local_node_id = config->local_node_id;
    session->peer_node_id = config->peer_node_id;
    session->raft_wire_version = TR_RAFT_WIRE_VERSION;
    session->snapshot_wire_version = TR_RAFT_WIRE_SNAPSHOT_VERSION;
    session->snapshot_chunk_size = TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES;
    session->max_frame_size = TR_RAFT_WIRE_MAX_FRAME_SIZE;
    if (config->handshake != NULL) {
        result = tr_raft_handshake_select_raft_wire_version(
            config->handshake, 0U, &session->raft_wire_version);
        if (result != TURBO_OK) {
            tr_raft_wire_codec_destroy(session->codec);
            free(session);
            return result;
        }
        result = tr_raft_handshake_select_snapshot_wire_version(
            config->handshake, &session->snapshot_wire_version,
            &session->snapshot_chunk_size);
        if (result == TURBO_EPROTONOSUPPORT) {
            session->snapshot_wire_version = 0U;
            session->snapshot_chunk_size = 0U;
        } else if (result != TURBO_OK) {
            tr_raft_wire_codec_destroy(session->codec);
            free(session);
            return result;
        }
        session->max_frame_size = config->handshake->max_frame_size;
    }
    session->next_outbound_message_id = config->first_outbound_message_id;
    session->on_message = config->on_message;
    session->message_context = config->message_context;
    session->on_snapshot = config->on_snapshot;
    session->snapshot_context = config->snapshot_context;
    session->status.state = config->socket == NULL
                                ? TR_RAFT_CORONET_STATE_DETACHED
                                : TR_RAFT_CORONET_STATE_CONNECTED;
    *out_session = session;
    return TURBO_OK;
}

int tr_raft_coronet_session_destroy(tr_raft_coronet_session_t *session)
{
    if (session == NULL) {
        return TURBO_OK;
    }
    if (session->callback_active) {
        session->destroy_pending = 1;
        session->status.state = TR_RAFT_CORONET_STATE_FAULTED;
        return TURBO_OK;
    }
    if (session->owner_manager != NULL &&
        session->owner_manager->callback_depth != 0U) {
        return TURBO_EBUSY;
    }
    tr_raft_coronet_peer_manager_unlink_session(session);
    tr_raft_coronet_session_finalize(session);
    return TURBO_OK;
}

static int tr_raft_coronet_payload_nodes(
    const tr_raft_coronet_payload_t *payload,
    tr_raft_node_id_t *out_from,
    tr_raft_node_id_t *out_to)
{
    if (payload == NULL || out_from == NULL || out_to == NULL) {
        return TURBO_EINVAL;
    }
    switch (payload->kind) {
    case TR_RAFT_WIRE_PAYLOAD_RAFT:
        *out_from = payload->data.raft.from;
        *out_to = payload->data.raft.to;
        return TURBO_OK;
    case TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_CHUNK:
        *out_from = payload->data.snapshot_chunk.from;
        *out_to = payload->data.snapshot_chunk.to;
        return TURBO_OK;
    case TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_ACK:
        *out_from = payload->data.snapshot_ack.from;
        *out_to = payload->data.snapshot_ack.to;
        return TURBO_OK;
    default:
        return TURBO_EPROTO;
    }
}

int tr_raft_coronet_encode_payload_packet(
    tr_raft_coronet_session_t *session,
    const tr_raft_coronet_payload_t *payload,
    uint8_t *output,
    size_t output_capacity,
    size_t *output_size)
{
    tr_raft_wire_metadata_t metadata;
    tr_raft_node_id_t from;
    tr_raft_node_id_t to;
    size_t frame_size = 0U;
    int result;

    if (session == NULL || payload == NULL || output == NULL ||
        output_size == NULL ||
        output_capacity < TR_RAFT_CORONET_LENGTH_PREFIX_SIZE) {
        return TURBO_EINVAL;
    }
    result = tr_raft_coronet_payload_nodes(payload, &from, &to);
    if (result != TURBO_OK) {
        return result;
    }
    if (!tr_raft_coronet_is_active(session) ||
        session->outbound_ids_exhausted ||
        from != session->local_node_id || to != session->peer_node_id) {
        return TURBO_EPROTO;
    }

    memset(&metadata, 0, sizeof(metadata));
    metadata.cluster_id = session->cluster_id;
    metadata.message_id = session->next_outbound_message_id;
    switch (payload->kind) {
    case TR_RAFT_WIRE_PAYLOAD_RAFT:
        if (session->raft_wire_version == TR_RAFT_WIRE_MIN_VERSION &&
            payload->data.raft.entry_count > 1U) {
            return TURBO_EPROTONOSUPPORT;
        }
        result = tr_raft_wire_encode_version(
            session->codec, session->raft_wire_version, &metadata,
            &payload->data.raft,
            output + TR_RAFT_CORONET_LENGTH_PREFIX_SIZE,
            output_capacity - TR_RAFT_CORONET_LENGTH_PREFIX_SIZE,
            &frame_size);
        break;
    case TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_CHUNK:
        if (session->snapshot_wire_version == 0U ||
            payload->data.snapshot_chunk.data_length >
            session->snapshot_chunk_size) {
            return TURBO_EPROTONOSUPPORT;
        }
        result = tr_raft_wire_encode_snapshot_chunk_version(
            session->codec, session->snapshot_wire_version, &metadata,
            &payload->data.snapshot_chunk,
            output + TR_RAFT_CORONET_LENGTH_PREFIX_SIZE,
            output_capacity - TR_RAFT_CORONET_LENGTH_PREFIX_SIZE,
            &frame_size);
        break;
    case TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_ACK:
        if (session->snapshot_wire_version == 0U) {
            return TURBO_EPROTONOSUPPORT;
        }
        result = tr_raft_wire_encode_snapshot_ack_version(
            session->codec, session->snapshot_wire_version, &metadata,
            &payload->data.snapshot_ack,
            output + TR_RAFT_CORONET_LENGTH_PREFIX_SIZE,
            output_capacity - TR_RAFT_CORONET_LENGTH_PREFIX_SIZE,
            &frame_size);
        break;
    default:
        return TURBO_EPROTO;
    }
    if (result != TURBO_OK) {
        return result;
    }
    if (frame_size > UINT32_MAX) {
        return TURBO_EPROTO;
    }

    tr_raft_coronet_write_u32_be(output, (uint32_t) frame_size);
    *output_size = frame_size + TR_RAFT_CORONET_LENGTH_PREFIX_SIZE;
    session->status.last_outbound_message_id =
        session->next_outbound_message_id;
    if (session->next_outbound_message_id == UINT64_MAX) {
        session->outbound_ids_exhausted = 1;
    } else {
        session->next_outbound_message_id++;
    }
    return TURBO_OK;
}

int tr_raft_coronet_encode_packet(tr_raft_coronet_session_t *session,
                                  const tr_raft_message_t *message,
                                  uint8_t *output,
                                  size_t output_capacity,
                                  size_t *output_size)
{
    tr_raft_coronet_payload_t payload;

    if (message == NULL) {
        return TURBO_EINVAL;
    }
    memset(&payload, 0, sizeof(payload));
    payload.kind = TR_RAFT_WIRE_PAYLOAD_RAFT;
    payload.data.raft = *message;
    return tr_raft_coronet_encode_payload_packet(
        session, &payload, output, output_capacity, output_size);
}

int tr_raft_coronet_v2_append_part(
    const tr_raft_message_t *message,
    size_t entry_index,
    tr_raft_message_t *out_message)
{
    if (message == NULL || out_message == NULL ||
        message->type != TR_RAFT_MSG_APPEND_REQUEST ||
        message->entry_count <= 1U ||
        message->entry_count > TR_RAFT_MAX_APPEND_ENTRIES ||
        entry_index >= message->entry_count) {
        return TURBO_EINVAL;
    }
    *out_message = *message;
    out_message->entry_count = 1U;
    out_message->entries[0] = message->entries[entry_index];
    if (entry_index != 0U) {
        out_message->previous_log_index =
            message->entries[entry_index - 1U].index;
        out_message->previous_log_term =
            message->entries[entry_index - 1U].term;
    }
    return TURBO_OK;
}

static int tr_raft_coronet_send_payload_once(
    tr_raft_coronet_session_t *session,
    const tr_raft_coronet_payload_t *payload)
{
    size_t packet_size = 0U;
    int result;

    if (session == NULL || payload == NULL) {
        return TURBO_EINVAL;
    }
    if (session->status.state != TR_RAFT_CORONET_STATE_CONNECTED ||
        session->socket == NULL) {
        return TURBO_EPROTO;
    }
    result = tr_raft_coronet_encode_payload_packet(
        session, payload, session->outbound_packet,
        sizeof(session->outbound_packet), &packet_size);
    if (result != TURBO_OK) {
        return tr_raft_coronet_fault(session, result);
    }
    result = coro_socket_send(session->socket,
                              (const char *) session->outbound_packet,
                              packet_size);
    if (result != TURBO_OK) {
        return tr_raft_coronet_fault(session, result);
    }
    session->status.frames_sent++;
    session->status.bytes_sent += packet_size;
    return TURBO_OK;
}

int tr_raft_coronet_enqueue_payload(
    tr_raft_coronet_session_t *session,
    const tr_raft_coronet_payload_t *payload)
{
    size_t index;

    if (session == NULL || payload == NULL) {
        return TURBO_EINVAL;
    }
    if (session->status.state != TR_RAFT_CORONET_STATE_CONNECTED ||
        session->socket == NULL) {
        return TURBO_EPROTO;
    }
    if (payload->kind == TR_RAFT_WIRE_PAYLOAD_RAFT &&
        session->raft_wire_version == TR_RAFT_WIRE_MIN_VERSION &&
        payload->data.raft.type == TR_RAFT_MSG_APPEND_REQUEST &&
        payload->data.raft.entry_count > 1U) {
        for (index = 0U; index < payload->data.raft.entry_count; ++index) {
            tr_raft_coronet_payload_t part = *payload;
            int result = tr_raft_coronet_v2_append_part(
                &payload->data.raft, index, &part.data.raft);

            if (result != TURBO_OK) {
                return tr_raft_coronet_fault(session, result);
            }
            result = tr_raft_coronet_send_payload_once(session, &part);
            if (result != TURBO_OK) {
                return result;
            }
        }
        return TURBO_OK;
    }
    return tr_raft_coronet_send_payload_once(session, payload);
}

int tr_raft_coronet_enqueue(void *context, const tr_raft_message_t *message)
{
    tr_raft_coronet_payload_t payload;

    if (message == NULL) {
        return TURBO_EINVAL;
    }
    memset(&payload, 0, sizeof(payload));
    payload.kind = TR_RAFT_WIRE_PAYLOAD_RAFT;
    payload.data.raft = *message;
    return tr_raft_coronet_enqueue_payload(
        (tr_raft_coronet_session_t *) context, &payload);
}

int tr_raft_coronet_feed(tr_raft_coronet_session_t *session,
                         const uint8_t *data,
                         size_t size)
{
    size_t offset = 0U;

    if (session == NULL || (data == NULL && size != 0U)) {
        return TURBO_EINVAL;
    }
    if (!tr_raft_coronet_is_active(session)) {
        return TURBO_EPROTO;
    }
    session->status.bytes_received += size;

    while (offset < size) {
        size_t available;
        size_t required;
        size_t copied;
        int result;

        if (session->length_prefix_used <
            TR_RAFT_CORONET_LENGTH_PREFIX_SIZE) {
            required = TR_RAFT_CORONET_LENGTH_PREFIX_SIZE -
                       session->length_prefix_used;
            available = size - offset;
            copied = required < available ? required : available;
            memcpy(session->length_prefix + session->length_prefix_used,
                   data + offset, copied);
            session->length_prefix_used += copied;
            offset += copied;
            if (session->length_prefix_used <
                TR_RAFT_CORONET_LENGTH_PREFIX_SIZE) {
                continue;
            }
            session->expected_frame_size =
                (size_t) tr_raft_coronet_read_u32_be(session->length_prefix);
            if (session->expected_frame_size < TR_RAFT_WIRE_HEADER_SIZE ||
                session->expected_frame_size > session->max_frame_size ||
                session->expected_frame_size > TR_RAFT_WIRE_MAX_FRAME_SIZE) {
                return tr_raft_coronet_fault(session, TURBO_EPROTO);
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

        result = tr_raft_coronet_dispatch_frame(session);
        if (result != TURBO_OK) {
            return result;
        }
        session->length_prefix_used = 0U;
        session->expected_frame_size = 0U;
        session->frame_used = 0U;
    }
    return TURBO_OK;
}

int tr_raft_coronet_receive_once(tr_raft_coronet_session_t *session)
{
    char *data = NULL;
    size_t size = 0U;
    int result;

    if (session == NULL) {
        return TURBO_EINVAL;
    }
    if (session->status.state != TR_RAFT_CORONET_STATE_CONNECTED ||
        session->socket == NULL) {
        return TURBO_EPROTO;
    }
    result = coro_socket_recv(session->socket, &data, &size);
    if (result != TURBO_OK) {
        return tr_raft_coronet_fault(session, result);
    }
    if (data == NULL || size == 0U) {
        coro_socket_free_recv(data);
        return tr_raft_coronet_fault(session, TURBO_EIO);
    }
    result = tr_raft_coronet_feed(session, (const uint8_t *) data, size);
    coro_socket_free_recv(data);
    return result;
}

int tr_raft_coronet_get_status(const tr_raft_coronet_session_t *session,
                               tr_raft_coronet_status_t *out_status)
{
    if (session == NULL || out_status == NULL) {
        return TURBO_EINVAL;
    }
    *out_status = session->status;
    return TURBO_OK;
}

int tr_raft_coronet_peer_manager_create(
    const tr_raft_coronet_peer_manager_config_t *config,
    tr_raft_coronet_peer_manager_t **out_manager)
{
    tr_raft_coronet_peer_manager_t *manager;
    size_t index;

    if (config == NULL || out_manager == NULL ||
        !tr_raft_coronet_cluster_id_is_valid(&config->cluster_id) ||
        config->local_node_id == 0U ||
        config->peer_count > TR_RAFT_CORONET_MAX_PEERS ||
        (config->peer_count != 0U && config->peer_node_ids == NULL)) {
        return TURBO_EINVAL;
    }
    for (index = 0U; index < config->peer_count; ++index) {
        if (config->peer_node_ids[index] == 0U ||
            config->peer_node_ids[index] == config->local_node_id ||
            (index != 0U &&
             config->peer_node_ids[index - 1U] >=
                 config->peer_node_ids[index])) {
            return TURBO_EINVAL;
        }
    }

    manager = (tr_raft_coronet_peer_manager_t *) calloc(1U, sizeof(*manager));
    if (manager == NULL) {
        return TURBO_ENOMEM;
    }
    manager->cluster_id = config->cluster_id;
    manager->local_node_id = config->local_node_id;
    manager->peer_count = config->peer_count;
    if (config->peer_count != 0U) {
        memcpy(manager->peer_node_ids, config->peer_node_ids,
               config->peer_count * sizeof(config->peer_node_ids[0]));
    }
    *out_manager = manager;
    return TURBO_OK;
}

int tr_raft_coronet_peer_manager_destroy(
    tr_raft_coronet_peer_manager_t *manager)
{
    size_t index;

    if (manager == NULL) {
        return TURBO_OK;
    }
    if (manager->callback_depth != 0U) {
        return TURBO_EBUSY;
    }
    for (index = 0U; index < manager->peer_count; ++index) {
        if (manager->sessions[index] != NULL) {
            manager->sessions[index]->owner_manager = NULL;
            tr_raft_coronet_session_finalize(manager->sessions[index]);
        }
    }
    free(manager);
    return TURBO_OK;
}

int tr_raft_coronet_peer_manager_attach(
    tr_raft_coronet_peer_manager_t *manager,
    tr_raft_coronet_session_t *session)
{
    size_t index;

    if (manager == NULL || session == NULL) {
        return TURBO_EINVAL;
    }
    if (manager->callback_depth != 0U) {
        return TURBO_EBUSY;
    }
    if (!tr_raft_coronet_is_active(session) ||
        session->local_node_id != manager->local_node_id ||
        memcmp(session->cluster_id.bytes, manager->cluster_id.bytes,
               sizeof(manager->cluster_id.bytes)) != 0) {
        return TURBO_EPROTO;
    }
    index = tr_raft_coronet_peer_manager_find(manager, session->peer_node_id);
    if (index == SIZE_MAX || manager->sessions[index] != NULL) {
        return TURBO_EPROTO;
    }
    manager->sessions[index] = session;
    session->owner_manager = manager;
    return TURBO_OK;
}

int tr_raft_coronet_expected_direction(
    tr_raft_node_id_t local_node_id,
    tr_raft_node_id_t peer_node_id,
    tr_raft_coronet_connection_direction_t *out_direction)
{
    if (local_node_id == 0U || peer_node_id == 0U ||
        local_node_id == peer_node_id || out_direction == NULL) {
        return TURBO_EINVAL;
    }
    *out_direction = local_node_id < peer_node_id
                         ? TR_RAFT_CORONET_CONNECTION_OUTBOUND
                         : TR_RAFT_CORONET_CONNECTION_INBOUND;
    return TURBO_OK;
}

int tr_raft_coronet_peer_manager_admit(
    tr_raft_coronet_peer_manager_t *manager,
    tr_raft_coronet_connection_direction_t direction,
    tr_raft_coronet_session_t *session)
{
    tr_raft_coronet_connection_direction_t expected_direction;
    int result;

    if (manager == NULL || session == NULL ||
        (direction != TR_RAFT_CORONET_CONNECTION_INBOUND &&
         direction != TR_RAFT_CORONET_CONNECTION_OUTBOUND)) {
        return TURBO_EINVAL;
    }
    if (session->status.state != TR_RAFT_CORONET_STATE_CONNECTED) {
        return TURBO_EPROTO;
    }
    result = tr_raft_coronet_expected_direction(
        manager->local_node_id, session->peer_node_id, &expected_direction);
    if (result != TURBO_OK) {
        return result;
    }
    if (direction != expected_direction) {
        return TURBO_EPROTO;
    }
    return tr_raft_coronet_peer_manager_attach(manager, session);
}

int tr_raft_coronet_peer_manager_admit_owned_socket(
    tr_raft_coronet_peer_manager_t *manager,
    coro_socket_t *socket,
    const tr_raft_coronet_owned_socket_admission_config_t *config,
    tr_raft_node_id_t *out_peer_node_id)
{
    tr_raft_handshake_result_t handshake_result;
    tr_raft_coronet_receive_remainder_t remainder;
    tr_raft_coronet_session_config_t session_config;
    tr_raft_coronet_session_t *session = NULL;
    tr_raft_coronet_session_t *detached = NULL;
    tr_raft_node_id_t peer_node_id = 0U;
    int result;

    if (out_peer_node_id != NULL) {
        *out_peer_node_id = 0U;
    }
    if (socket == NULL) {
        return TURBO_EINVAL;
    }
    if (manager == NULL || config == NULL || out_peer_node_id == NULL ||
        config->handshake.handshake == NULL ||
        config->first_outbound_message_id == 0U ||
        config->peer_idle_timeout_ms == 0U ||
        config->on_message == NULL ||
        (config->direction != TR_RAFT_CORONET_CONNECTION_INBOUND &&
         config->direction != TR_RAFT_CORONET_CONNECTION_OUTBOUND)) {
        coro_socket_destroy(socket);
        return TURBO_EINVAL;
    }

    memset(&handshake_result, 0, sizeof(handshake_result));
    memset(&remainder, 0, sizeof(remainder));
    result = tr_raft_coronet_handshake_exchange(
        socket, &config->handshake, &handshake_result, &remainder);
    if (result != TURBO_OK) {
        coro_socket_destroy(socket);
        return result;
    }
    peer_node_id = handshake_result.peer_node_id;
    if (config->expected_peer_node_id != 0U &&
        peer_node_id != config->expected_peer_node_id) {
        tr_raft_coronet_receive_remainder_release(&remainder);
        coro_socket_destroy(socket);
        return TURBO_EPROTO;
    }
    coro_socket_set_timeout(socket, config->peer_idle_timeout_ms);

    memset(&session_config, 0, sizeof(session_config));
    session_config.socket = socket;
    session_config.owns_socket = 1;
    session_config.cluster_id = handshake_result.cluster_id;
    session_config.local_node_id = handshake_result.local_node_id;
    session_config.peer_node_id = peer_node_id;
    session_config.first_outbound_message_id =
        config->first_outbound_message_id;
    session_config.handshake = &handshake_result;
    session_config.on_message = config->on_message;
    session_config.message_context = config->message_context;
    session_config.on_snapshot = config->on_snapshot;
    session_config.snapshot_context = config->snapshot_context;
    result = tr_raft_coronet_session_create(&session_config, &session);
    if (result != TURBO_OK) {
        tr_raft_coronet_receive_remainder_release(&remainder);
        coro_socket_destroy(socket);
        return result;
    }

    result = tr_raft_coronet_peer_manager_admit(
        manager, config->direction, session);
    if (result != TURBO_OK) {
        tr_raft_coronet_receive_remainder_release(&remainder);
        tr_raft_coronet_session_destroy(session);
        return result;
    }
    if (remainder.receive_buffer != NULL) {
        result = tr_raft_coronet_receive_remainder_apply(session, &remainder);
        if (result != TURBO_OK) {
            if (tr_raft_coronet_peer_manager_detach(
                    manager, peer_node_id, &detached) == TURBO_OK) {
                tr_raft_coronet_session_destroy(detached);
            }
            return result;
        }
    }

    *out_peer_node_id = peer_node_id;
    return TURBO_OK;
}

int tr_raft_coronet_peer_manager_detach(
    tr_raft_coronet_peer_manager_t *manager,
    tr_raft_node_id_t peer_node_id,
    tr_raft_coronet_session_t **out_session)
{
    size_t index;

    if (manager == NULL || peer_node_id == 0U || out_session == NULL) {
        return TURBO_EINVAL;
    }
    if (manager->callback_depth != 0U) {
        return TURBO_EBUSY;
    }
    index = tr_raft_coronet_peer_manager_find(manager, peer_node_id);
    if (index == SIZE_MAX || manager->sessions[index] == NULL) {
        return TURBO_EPROTO;
    }
    *out_session = manager->sessions[index];
    manager->sessions[index] = NULL;
    (*out_session)->owner_manager = NULL;
    return TURBO_OK;
}

int tr_raft_coronet_peer_manager_enqueue(
    void *context,
    const tr_raft_message_t *message)
{
    tr_raft_coronet_payload_t payload;

    if (message == NULL) {
        return TURBO_EINVAL;
    }
    memset(&payload, 0, sizeof(payload));
    payload.kind = TR_RAFT_WIRE_PAYLOAD_RAFT;
    payload.data.raft = *message;
    return tr_raft_coronet_peer_manager_enqueue_payload(
        (tr_raft_coronet_peer_manager_t *) context, &payload);
}

int tr_raft_coronet_peer_manager_enqueue_payload(
    tr_raft_coronet_peer_manager_t *manager,
    const tr_raft_coronet_payload_t *payload)
{
    tr_raft_node_id_t from;
    tr_raft_node_id_t to;
    size_t index;
    int result;

    if (manager == NULL || payload == NULL) {
        return TURBO_EINVAL;
    }
    result = tr_raft_coronet_payload_nodes(payload, &from, &to);
    if (result != TURBO_OK || from != manager->local_node_id) {
        return result != TURBO_OK ? result : TURBO_EPROTO;
    }
    index = tr_raft_coronet_peer_manager_find(manager, to);
    if (index == SIZE_MAX || manager->sessions[index] == NULL) {
        return TURBO_EPROTO;
    }
    return tr_raft_coronet_enqueue_payload(manager->sessions[index], payload);
}

int tr_raft_coronet_peer_manager_receive_peer(
    tr_raft_coronet_peer_manager_t *manager,
    tr_raft_node_id_t peer_node_id)
{
    size_t index;

    if (manager == NULL || peer_node_id == 0U) {
        return TURBO_EINVAL;
    }
    index = tr_raft_coronet_peer_manager_find(manager, peer_node_id);
    if (index == SIZE_MAX || manager->sessions[index] == NULL) {
        return TURBO_ENOTCONN;
    }
    return tr_raft_coronet_receive_once(manager->sessions[index]);
}

int tr_raft_coronet_peer_manager_release_peer(
    tr_raft_coronet_peer_manager_t *manager,
    tr_raft_node_id_t peer_node_id)
{
    tr_raft_coronet_session_t *session;
    size_t index;

    if (manager == NULL || peer_node_id == 0U) {
        return TURBO_EINVAL;
    }
    if (manager->callback_depth != 0U) {
        return TURBO_EBUSY;
    }
    index = tr_raft_coronet_peer_manager_find(manager, peer_node_id);
    if (index == SIZE_MAX || manager->sessions[index] == NULL) {
        return TURBO_ENOTCONN;
    }
    session = manager->sessions[index];
    manager->sessions[index] = NULL;
    session->owner_manager = NULL;
    return tr_raft_coronet_session_destroy(session);
}

int tr_raft_coronet_peer_manager_close_peer(
    tr_raft_coronet_peer_manager_t *manager,
    tr_raft_node_id_t peer_node_id)
{
    tr_raft_coronet_session_t *session;
    size_t index;

    if (manager == NULL || peer_node_id == 0U) {
        return TURBO_EINVAL;
    }
    if (manager->callback_depth != 0U) {
        return TURBO_EBUSY;
    }
    index = tr_raft_coronet_peer_manager_find(manager, peer_node_id);
    if (index == SIZE_MAX || manager->sessions[index] == NULL) {
        return TURBO_ENOTCONN;
    }
    session = manager->sessions[index];
    if (session->socket == NULL) {
        return TURBO_ENOTCONN;
    }
    coro_socket_destroy(session->socket);
    session->socket = NULL;
    session->status.state = TR_RAFT_CORONET_STATE_FAULTED;
    return TURBO_OK;
}

int tr_raft_coronet_peer_manager_close_all(
    tr_raft_coronet_peer_manager_t *manager)
{
    size_t index;

    if (manager == NULL) {
        return TURBO_EINVAL;
    }
    if (manager->callback_depth != 0U) {
        return TURBO_EBUSY;
    }
    for (index = 0U; index < manager->peer_count; ++index) {
        tr_raft_coronet_session_t *session = manager->sessions[index];
        if (session != NULL && session->socket != NULL) {
            coro_socket_destroy(session->socket);
            session->socket = NULL;
            session->status.state = TR_RAFT_CORONET_STATE_FAULTED;
        }
    }
    return TURBO_OK;
}

int tr_raft_coronet_peer_manager_get_status(
    const tr_raft_coronet_peer_manager_t *manager,
    tr_raft_coronet_peer_manager_status_t *out_status)
{
    size_t index;

    if (manager == NULL || out_status == NULL) {
        return TURBO_EINVAL;
    }
    memset(out_status, 0, sizeof(*out_status));
    out_status->configured_count = manager->peer_count;
    for (index = 0U; index < manager->peer_count; ++index) {
        const tr_raft_coronet_session_t *session = manager->sessions[index];

        if (session == NULL) {
            continue;
        }
        out_status->attached_count++;
        if (session->status.state == TR_RAFT_CORONET_STATE_DETACHED) {
            out_status->detached_count++;
        } else if (session->status.state == TR_RAFT_CORONET_STATE_CONNECTED) {
            out_status->connected_count++;
        } else {
            out_status->faulted_count++;
        }
    }
    return TURBO_OK;
}

int tr_raft_coronet_peer_manager_get_peer_status(
    const tr_raft_coronet_peer_manager_t *manager,
    tr_raft_node_id_t peer_node_id,
    tr_raft_coronet_peer_status_t *out_status)
{
    size_t index;

    if (manager == NULL || peer_node_id == 0U || out_status == NULL) {
        return TURBO_EINVAL;
    }
    index = tr_raft_coronet_peer_manager_find(manager, peer_node_id);
    if (index == SIZE_MAX) {
        return TURBO_EPROTO;
    }
    memset(out_status, 0, sizeof(*out_status));
    out_status->peer_node_id = peer_node_id;
    if (manager->sessions[index] != NULL) {
        out_status->attached = 1;
        out_status->session = manager->sessions[index]->status;
    }
    return TURBO_OK;
}
