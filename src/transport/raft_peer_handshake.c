#include <turboraft/raft_peer_handshake.h>

#include <turbo_error.h>

#include <stdlib.h>
#include <string.h>

static const uint8_t tr_raft_handshake_magic[4] = {'T', 'R', 'H', 'S'};

struct tr_raft_handshake_exchange {
    tr_raft_handshake_config_t local;
    tr_raft_node_id_t authenticated_peer_node_id;
    tr_raft_handshake_exchange_state_t state;
    tr_raft_handshake_result_t result;
    uint8_t packet[TR_RAFT_HANDSHAKE_PACKET_SIZE];
    size_t packet_used;
};

static uint16_t tr_raft_handshake_read_u16(const uint8_t *input)
{
    return (uint16_t) (((uint16_t) input[0] << 8U) | input[1]);
}

static uint32_t tr_raft_handshake_read_u32(const uint8_t *input)
{
    return ((uint32_t) input[0] << 24U) |
           ((uint32_t) input[1] << 16U) |
           ((uint32_t) input[2] << 8U) |
           (uint32_t) input[3];
}

static uint64_t tr_raft_handshake_read_u64(const uint8_t *input)
{
    uint64_t value = 0U;
    size_t index;

    for (index = 0U; index < 8U; ++index) {
        value = (value << 8U) | input[index];
    }
    return value;
}

static void tr_raft_handshake_write_u16(uint8_t *output, uint16_t value)
{
    output[0] = (uint8_t) (value >> 8U);
    output[1] = (uint8_t) value;
}

static void tr_raft_handshake_write_u32(uint8_t *output, uint32_t value)
{
    output[0] = (uint8_t) (value >> 24U);
    output[1] = (uint8_t) (value >> 16U);
    output[2] = (uint8_t) (value >> 8U);
    output[3] = (uint8_t) value;
}

static void tr_raft_handshake_write_u64(uint8_t *output, uint64_t value)
{
    size_t index;

    for (index = 0U; index < 8U; ++index) {
        output[7U - index] = (uint8_t) value;
        value >>= 8U;
    }
}

static int tr_raft_handshake_bytes_nonzero(const uint8_t *bytes, size_t size)
{
    size_t index;

    for (index = 0U; index < size; ++index) {
        if (bytes[index] != 0U) {
            return 1;
        }
    }
    return 0;
}

static int tr_raft_handshake_message_validate(
    const tr_raft_handshake_message_t *message)
{
    if (message == NULL ||
        (message->type != TR_RAFT_HANDSHAKE_HELLO &&
         message->type != TR_RAFT_HANDSHAKE_HELLO_ACK) ||
        message->wire_major_min == 0U ||
        message->wire_major_min > message->wire_major_max ||
        message->wire_minor_min > message->wire_minor_max ||
        (message->feature_bits &
         TR_RAFT_HANDSHAKE_FEATURE_SNAPSHOT_CONF_STATE) == 0U ||
        !tr_raft_handshake_bytes_nonzero(message->cluster_id.bytes,
                                         sizeof(message->cluster_id.bytes)) ||
        message->node_id == 0U ||
        !tr_raft_handshake_bytes_nonzero(
            message->process_incarnation.bytes,
            sizeof(message->process_incarnation.bytes)) ||
        message->max_frame_size < TR_RAFT_HANDSHAKE_MIN_FRAME_SIZE ||
        message->max_snapshot_chunk_size <
            TR_RAFT_HANDSHAKE_MIN_SNAPSHOT_CHUNK_SIZE) {
        return TURBO_EPROTO;
    }
    if (message->type == TR_RAFT_HANDSHAKE_HELLO_ACK &&
        (message->wire_major_min != message->wire_major_max ||
         message->wire_minor_min != message->wire_minor_max)) {
        return TURBO_EPROTO;
    }
    return TURBO_OK;
}

static int tr_raft_handshake_config_validate(
    const tr_raft_handshake_config_t *config)
{
    tr_raft_handshake_message_t message;

    if (config == NULL) {
        return TURBO_EINVAL;
    }
    memset(&message, 0, sizeof(message));
    message.type = TR_RAFT_HANDSHAKE_HELLO;
    message.wire_major_min = config->wire_major_min;
    message.wire_major_max = config->wire_major_max;
    message.wire_minor_min = config->wire_minor_min;
    message.wire_minor_max = config->wire_minor_max;
    message.feature_bits = config->feature_bits;
    message.cluster_id = config->cluster_id;
    message.node_id = config->local_node_id;
    message.process_incarnation = config->process_incarnation;
    message.config_epoch = config->config_epoch;
    message.max_frame_size = config->max_frame_size;
    message.max_snapshot_chunk_size = config->max_snapshot_chunk_size;
    return tr_raft_handshake_message_validate(&message);
}

int tr_raft_handshake_make_hello(
    const tr_raft_handshake_config_t *config,
    tr_raft_handshake_message_t *out_hello)
{
    int result;

    if (out_hello == NULL) {
        return TURBO_EINVAL;
    }
    result = tr_raft_handshake_config_validate(config);
    if (result != TURBO_OK) {
        return result;
    }
    memset(out_hello, 0, sizeof(*out_hello));
    out_hello->type = TR_RAFT_HANDSHAKE_HELLO;
    out_hello->wire_major_min = config->wire_major_min;
    out_hello->wire_major_max = config->wire_major_max;
    out_hello->wire_minor_min = config->wire_minor_min;
    out_hello->wire_minor_max = config->wire_minor_max;
    out_hello->feature_bits = config->feature_bits;
    out_hello->cluster_id = config->cluster_id;
    out_hello->node_id = config->local_node_id;
    out_hello->process_incarnation = config->process_incarnation;
    out_hello->config_epoch = config->config_epoch;
    out_hello->max_frame_size = config->max_frame_size;
    out_hello->max_snapshot_chunk_size = config->max_snapshot_chunk_size;
    return TURBO_OK;
}

int tr_raft_handshake_encode(const tr_raft_handshake_message_t *message,
                             uint8_t *output,
                             size_t output_capacity,
                             size_t *output_size)
{
    uint8_t *record;
    int result;

    if (output == NULL || output_size == NULL ||
        output_capacity < TR_RAFT_HANDSHAKE_PACKET_SIZE) {
        return TURBO_EINVAL;
    }
    result = tr_raft_handshake_message_validate(message);
    if (result != TURBO_OK) {
        return result;
    }

    memset(output, 0, TR_RAFT_HANDSHAKE_PACKET_SIZE);
    tr_raft_handshake_write_u32(output, TR_RAFT_HANDSHAKE_RECORD_SIZE);
    record = output + TR_RAFT_HANDSHAKE_LENGTH_PREFIX_SIZE;
    memcpy(record, tr_raft_handshake_magic, sizeof(tr_raft_handshake_magic));
    tr_raft_handshake_write_u16(record + 4U,
                                TR_RAFT_HANDSHAKE_FORMAT_VERSION);
    tr_raft_handshake_write_u16(record + 6U, (uint16_t) message->type);
    tr_raft_handshake_write_u16(record + 8U, message->wire_major_min);
    tr_raft_handshake_write_u16(record + 10U, message->wire_major_max);
    tr_raft_handshake_write_u16(record + 12U, message->wire_minor_min);
    tr_raft_handshake_write_u16(record + 14U, message->wire_minor_max);
    tr_raft_handshake_write_u64(record + 16U, message->feature_bits);
    memcpy(record + 24U, message->cluster_id.bytes,
           sizeof(message->cluster_id.bytes));
    tr_raft_handshake_write_u64(record + 40U, message->node_id);
    memcpy(record + 48U, message->process_incarnation.bytes,
           sizeof(message->process_incarnation.bytes));
    tr_raft_handshake_write_u64(record + 64U, message->config_epoch);
    tr_raft_handshake_write_u32(record + 72U, message->max_frame_size);
    tr_raft_handshake_write_u32(record + 76U,
                                message->max_snapshot_chunk_size);
    *output_size = TR_RAFT_HANDSHAKE_PACKET_SIZE;
    return TURBO_OK;
}

int tr_raft_handshake_decode(const uint8_t *packet,
                             size_t packet_size,
                             tr_raft_handshake_message_t *out_message)
{
    const uint8_t *record;

    if (packet == NULL || out_message == NULL) {
        return TURBO_EINVAL;
    }
    if (packet_size != TR_RAFT_HANDSHAKE_PACKET_SIZE ||
        tr_raft_handshake_read_u32(packet) != TR_RAFT_HANDSHAKE_RECORD_SIZE) {
        return TURBO_EPROTO;
    }
    record = packet + TR_RAFT_HANDSHAKE_LENGTH_PREFIX_SIZE;
    if (memcmp(record, tr_raft_handshake_magic,
               sizeof(tr_raft_handshake_magic)) != 0 ||
        tr_raft_handshake_read_u16(record + 4U) !=
            TR_RAFT_HANDSHAKE_FORMAT_VERSION ||
        tr_raft_handshake_read_u32(record + 80U) != 0U) {
        return TURBO_EPROTO;
    }

    memset(out_message, 0, sizeof(*out_message));
    out_message->type = (tr_raft_handshake_message_type_t)
        tr_raft_handshake_read_u16(record + 6U);
    out_message->wire_major_min = tr_raft_handshake_read_u16(record + 8U);
    out_message->wire_major_max = tr_raft_handshake_read_u16(record + 10U);
    out_message->wire_minor_min = tr_raft_handshake_read_u16(record + 12U);
    out_message->wire_minor_max = tr_raft_handshake_read_u16(record + 14U);
    out_message->feature_bits = tr_raft_handshake_read_u64(record + 16U);
    memcpy(out_message->cluster_id.bytes, record + 24U,
           sizeof(out_message->cluster_id.bytes));
    out_message->node_id = tr_raft_handshake_read_u64(record + 40U);
    memcpy(out_message->process_incarnation.bytes, record + 48U,
           sizeof(out_message->process_incarnation.bytes));
    out_message->config_epoch = tr_raft_handshake_read_u64(record + 64U);
    out_message->max_frame_size = tr_raft_handshake_read_u32(record + 72U);
    out_message->max_snapshot_chunk_size =
        tr_raft_handshake_read_u32(record + 76U);
    return tr_raft_handshake_message_validate(out_message);
}

int tr_raft_handshake_negotiate(
    const tr_raft_handshake_config_t *local,
    tr_raft_node_id_t authenticated_peer_node_id,
    const tr_raft_handshake_message_t *remote_hello,
    tr_raft_handshake_message_t *out_local_ack,
    tr_raft_handshake_result_t *out_result)
{
    uint16_t major_min;
    uint16_t major_max;
    uint16_t minor_min;
    uint16_t minor_max;
    int result;

    if (authenticated_peer_node_id == 0U || remote_hello == NULL ||
        out_local_ack == NULL || out_result == NULL) {
        return TURBO_EINVAL;
    }
    result = tr_raft_handshake_config_validate(local);
    if (result != TURBO_OK) {
        return result;
    }
    result = tr_raft_handshake_message_validate(remote_hello);
    if (result != TURBO_OK || remote_hello->type != TR_RAFT_HANDSHAKE_HELLO ||
        remote_hello->node_id != authenticated_peer_node_id ||
        remote_hello->node_id == local->local_node_id ||
        memcmp(remote_hello->cluster_id.bytes, local->cluster_id.bytes,
               sizeof(local->cluster_id.bytes)) != 0) {
        return TURBO_EPROTO;
    }

    major_min = local->wire_major_min > remote_hello->wire_major_min
                    ? local->wire_major_min
                    : remote_hello->wire_major_min;
    major_max = local->wire_major_max < remote_hello->wire_major_max
                    ? local->wire_major_max
                    : remote_hello->wire_major_max;
    minor_min = local->wire_minor_min > remote_hello->wire_minor_min
                    ? local->wire_minor_min
                    : remote_hello->wire_minor_min;
    minor_max = local->wire_minor_max < remote_hello->wire_minor_max
                    ? local->wire_minor_max
                    : remote_hello->wire_minor_max;
    if (major_min > major_max || minor_min > minor_max) {
        return TURBO_EPROTONOSUPPORT;
    }

    memset(out_result, 0, sizeof(*out_result));
    out_result->cluster_id = local->cluster_id;
    out_result->local_node_id = local->local_node_id;
    out_result->peer_node_id = remote_hello->node_id;
    out_result->peer_process_incarnation =
        remote_hello->process_incarnation;
    out_result->peer_config_epoch = remote_hello->config_epoch;
    out_result->feature_bits = local->feature_bits & remote_hello->feature_bits;
    out_result->wire_major = major_max;
    out_result->wire_minor = minor_max;
    out_result->max_frame_size =
        local->max_frame_size < remote_hello->max_frame_size
            ? local->max_frame_size
            : remote_hello->max_frame_size;
    out_result->max_snapshot_chunk_size =
        local->max_snapshot_chunk_size <
                remote_hello->max_snapshot_chunk_size
            ? local->max_snapshot_chunk_size
            : remote_hello->max_snapshot_chunk_size;

    memset(out_local_ack, 0, sizeof(*out_local_ack));
    out_local_ack->type = TR_RAFT_HANDSHAKE_HELLO_ACK;
    out_local_ack->wire_major_min = out_result->wire_major;
    out_local_ack->wire_major_max = out_result->wire_major;
    out_local_ack->wire_minor_min = out_result->wire_minor;
    out_local_ack->wire_minor_max = out_result->wire_minor;
    out_local_ack->feature_bits = out_result->feature_bits;
    out_local_ack->cluster_id = local->cluster_id;
    out_local_ack->node_id = local->local_node_id;
    out_local_ack->process_incarnation = local->process_incarnation;
    out_local_ack->config_epoch = local->config_epoch;
    out_local_ack->max_frame_size = out_result->max_frame_size;
    out_local_ack->max_snapshot_chunk_size =
        out_result->max_snapshot_chunk_size;
    return TURBO_OK;
}

int tr_raft_handshake_validate_ack(
    tr_raft_handshake_result_t *result,
    const tr_raft_handshake_message_t *remote_ack)
{
    int validation;

    if (result == NULL || remote_ack == NULL || result->complete) {
        return TURBO_EINVAL;
    }
    validation = tr_raft_handshake_message_validate(remote_ack);
    if (validation != TURBO_OK ||
        remote_ack->type != TR_RAFT_HANDSHAKE_HELLO_ACK ||
        remote_ack->node_id != result->peer_node_id ||
        memcmp(remote_ack->cluster_id.bytes, result->cluster_id.bytes,
               sizeof(result->cluster_id.bytes)) != 0 ||
        memcmp(remote_ack->process_incarnation.bytes,
               result->peer_process_incarnation.bytes,
               sizeof(result->peer_process_incarnation.bytes)) != 0 ||
        remote_ack->config_epoch != result->peer_config_epoch ||
        remote_ack->wire_major_min != result->wire_major ||
        remote_ack->wire_minor_min != result->wire_minor ||
        remote_ack->feature_bits != result->feature_bits ||
        remote_ack->max_frame_size != result->max_frame_size ||
        remote_ack->max_snapshot_chunk_size !=
            result->max_snapshot_chunk_size) {
        return TURBO_EPROTO;
    }
    result->complete = 1;
    return TURBO_OK;
}

int tr_raft_handshake_result_validate(
    const tr_raft_handshake_result_t *result,
    const tr_raft_cluster_id_t *cluster_id,
    tr_raft_node_id_t local_node_id,
    tr_raft_node_id_t peer_node_id)
{
    if (result == NULL || cluster_id == NULL || !result->complete ||
        (result->feature_bits &
         TR_RAFT_HANDSHAKE_FEATURE_SNAPSHOT_CONF_STATE) == 0U ||
        result->local_node_id != local_node_id ||
        result->peer_node_id != peer_node_id ||
        result->wire_major == 0U ||
        result->max_frame_size < TR_RAFT_HANDSHAKE_MIN_FRAME_SIZE ||
        result->max_snapshot_chunk_size <
            TR_RAFT_HANDSHAKE_MIN_SNAPSHOT_CHUNK_SIZE ||
        memcmp(result->cluster_id.bytes, cluster_id->bytes,
               sizeof(cluster_id->bytes)) != 0 ||
        !tr_raft_handshake_bytes_nonzero(
            result->peer_process_incarnation.bytes,
            sizeof(result->peer_process_incarnation.bytes))) {
        return TURBO_EPROTO;
    }
    return TURBO_OK;
}

int tr_raft_handshake_select_raft_wire_version(
    const tr_raft_handshake_result_t *result,
    size_t entry_count,
    uint16_t *out_wire_version)
{
    if (result == NULL || out_wire_version == NULL || !result->complete ||
        (result->feature_bits &
         TR_RAFT_HANDSHAKE_FEATURE_SNAPSHOT_CONF_STATE) == 0U) {
        return TURBO_EINVAL;
    }
    if ((result->feature_bits &
         TR_RAFT_HANDSHAKE_FEATURE_RAFT_BATCH_V3) != 0U) {
        *out_wire_version = TR_RAFT_WIRE_VERSION;
        return TURBO_OK;
    }
    if (entry_count > 1U) {
        return TURBO_EPROTONOSUPPORT;
    }
    *out_wire_version = TR_RAFT_WIRE_MIN_VERSION;
    return TURBO_OK;
}

int tr_raft_handshake_require_snapshot_v4(
    const tr_raft_handshake_result_t *result)
{
    if (result == NULL || !result->complete ||
        (result->feature_bits &
         TR_RAFT_HANDSHAKE_FEATURE_SNAPSHOT_CONF_STATE) == 0U) {
        return TURBO_EINVAL;
    }
    return (result->feature_bits &
            TR_RAFT_HANDSHAKE_FEATURE_SNAPSHOT_V4) != 0U
               ? TURBO_OK
               : TURBO_EPROTONOSUPPORT;
}

int tr_raft_handshake_select_snapshot_wire_version(
    const tr_raft_handshake_result_t *result,
    uint16_t *out_wire_version,
    uint32_t *out_chunk_size)
{
    uint32_t chunk_size;

    if (result == NULL || out_wire_version == NULL ||
        out_chunk_size == NULL || !result->complete ||
        (result->feature_bits &
         TR_RAFT_HANDSHAKE_FEATURE_SNAPSHOT_CONF_STATE) == 0U) {
        return TURBO_EINVAL;
    }
    if ((result->feature_bits & TR_RAFT_HANDSHAKE_FEATURE_SNAPSHOT_V5) != 0U &&
        result->max_frame_size >= TR_RAFT_WIRE_MAX_FRAME_SIZE &&
        result->max_snapshot_chunk_size >=
            TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES) {
        chunk_size = TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES;
        *out_wire_version = TR_RAFT_WIRE_SNAPSHOT_VERSION;
        *out_chunk_size = chunk_size;
        return TURBO_OK;
    }
    if ((result->feature_bits & TR_RAFT_HANDSHAKE_FEATURE_SNAPSHOT_V4) != 0U) {
        *out_wire_version = TR_RAFT_WIRE_SNAPSHOT_LEGACY_VERSION;
        *out_chunk_size = TR_RAFT_WIRE_LEGACY_SNAPSHOT_CHUNK_BYTES;
        return TURBO_OK;
    }
    return TURBO_EPROTONOSUPPORT;
}

static int tr_raft_handshake_exchange_fault(
    tr_raft_handshake_exchange_t *exchange,
    int error)
{
    exchange->state = TR_RAFT_HANDSHAKE_EXCHANGE_FAULTED;
    return error;
}

int tr_raft_handshake_exchange_create(
    const tr_raft_handshake_config_t *local,
    tr_raft_node_id_t authenticated_peer_node_id,
    tr_raft_handshake_exchange_t **out_exchange)
{
    tr_raft_handshake_exchange_t *exchange;
    int result;

    if (authenticated_peer_node_id == 0U || out_exchange == NULL) {
        return TURBO_EINVAL;
    }
    result = tr_raft_handshake_config_validate(local);
    if (result != TURBO_OK) {
        return result;
    }
    if (authenticated_peer_node_id == local->local_node_id) {
        return TURBO_EPROTO;
    }
    exchange =
        (tr_raft_handshake_exchange_t *) calloc(1U, sizeof(*exchange));
    if (exchange == NULL) {
        return TURBO_ENOMEM;
    }
    exchange->local = *local;
    exchange->authenticated_peer_node_id = authenticated_peer_node_id;
    exchange->state = TR_RAFT_HANDSHAKE_EXCHANGE_NEW;
    *out_exchange = exchange;
    return TURBO_OK;
}

void tr_raft_handshake_exchange_destroy(
    tr_raft_handshake_exchange_t *exchange)
{
    free(exchange);
}

int tr_raft_handshake_exchange_start(
    tr_raft_handshake_exchange_t *exchange,
    uint8_t *output,
    size_t output_capacity,
    size_t *output_size)
{
    tr_raft_handshake_message_t hello;
    int result;

    if (exchange == NULL || output == NULL || output_size == NULL) {
        return TURBO_EINVAL;
    }
    if (exchange->state != TR_RAFT_HANDSHAKE_EXCHANGE_NEW) {
        return TURBO_EPROTO;
    }
    result = tr_raft_handshake_make_hello(&exchange->local, &hello);
    if (result == TURBO_OK) {
        result = tr_raft_handshake_encode(&hello, output, output_capacity,
                                          output_size);
    }
    if (result != TURBO_OK) {
        return tr_raft_handshake_exchange_fault(exchange, result);
    }
    exchange->state = TR_RAFT_HANDSHAKE_EXCHANGE_WAIT_HELLO;
    return TURBO_OK;
}

int tr_raft_handshake_exchange_feed(
    tr_raft_handshake_exchange_t *exchange,
    const uint8_t *data,
    size_t size,
    size_t *consumed_size,
    uint8_t *output,
    size_t output_capacity,
    size_t *output_size)
{
    size_t offset = 0U;

    if (exchange == NULL || (data == NULL && size != 0U) ||
        consumed_size == NULL || output == NULL || output_size == NULL ||
        output_capacity < TR_RAFT_HANDSHAKE_PACKET_SIZE) {
        return TURBO_EINVAL;
    }
    *consumed_size = 0U;
    *output_size = 0U;
    if (exchange->state != TR_RAFT_HANDSHAKE_EXCHANGE_WAIT_HELLO &&
        exchange->state != TR_RAFT_HANDSHAKE_EXCHANGE_WAIT_ACK) {
        return TURBO_EPROTO;
    }

    while (offset < size &&
           exchange->state != TR_RAFT_HANDSHAKE_EXCHANGE_COMPLETE) {
        size_t required = TR_RAFT_HANDSHAKE_PACKET_SIZE -
                          exchange->packet_used;
        size_t available = size - offset;
        size_t copied = required < available ? required : available;
        int result;

        memcpy(exchange->packet + exchange->packet_used, data + offset,
               copied);
        exchange->packet_used += copied;
        offset += copied;
        *consumed_size = offset;

        if (exchange->packet_used >= TR_RAFT_HANDSHAKE_LENGTH_PREFIX_SIZE &&
            tr_raft_handshake_read_u32(exchange->packet) !=
                TR_RAFT_HANDSHAKE_RECORD_SIZE) {
            return tr_raft_handshake_exchange_fault(exchange, TURBO_EPROTO);
        }
        if (exchange->packet_used != TR_RAFT_HANDSHAKE_PACKET_SIZE) {
            continue;
        }

        {
            tr_raft_handshake_message_t message;

            result = tr_raft_handshake_decode(
                exchange->packet, exchange->packet_used, &message);
            if (result != TURBO_OK) {
                return tr_raft_handshake_exchange_fault(exchange, result);
            }
            exchange->packet_used = 0U;
            if (exchange->state == TR_RAFT_HANDSHAKE_EXCHANGE_WAIT_HELLO) {
                tr_raft_handshake_message_t ack;

                result = tr_raft_handshake_negotiate(
                    &exchange->local,
                    exchange->authenticated_peer_node_id,
                    &message,
                    &ack,
                    &exchange->result);
                if (result == TURBO_OK) {
                    result = tr_raft_handshake_encode(
                        &ack, output, output_capacity, output_size);
                }
                if (result != TURBO_OK) {
                    return tr_raft_handshake_exchange_fault(exchange,
                                                             result);
                }
                exchange->state = TR_RAFT_HANDSHAKE_EXCHANGE_WAIT_ACK;
            } else {
                result = tr_raft_handshake_validate_ack(&exchange->result,
                                                        &message);
                if (result != TURBO_OK) {
                    return tr_raft_handshake_exchange_fault(exchange,
                                                             result);
                }
                exchange->state = TR_RAFT_HANDSHAKE_EXCHANGE_COMPLETE;
            }
        }
    }
    return TURBO_OK;
}

int tr_raft_handshake_exchange_get_state(
    const tr_raft_handshake_exchange_t *exchange,
    tr_raft_handshake_exchange_state_t *out_state)
{
    if (exchange == NULL || out_state == NULL) {
        return TURBO_EINVAL;
    }
    *out_state = exchange->state;
    return TURBO_OK;
}

int tr_raft_handshake_exchange_get_result(
    const tr_raft_handshake_exchange_t *exchange,
    tr_raft_handshake_result_t *out_result)
{
    if (exchange == NULL || out_result == NULL) {
        return TURBO_EINVAL;
    }
    if (exchange->state != TR_RAFT_HANDSHAKE_EXCHANGE_COMPLETE ||
        !exchange->result.complete) {
        return TURBO_EPROTO;
    }
    *out_result = exchange->result;
    return TURBO_OK;
}
