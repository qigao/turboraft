#include <turboraft/raft_wire_codec.h>

#include "raft_configuration.h"

#include "turboraft_wire_tbe.h"
#undef SCHEMA_GENERATED_H
#include "turboraft_wire_v3_tbe.h"

#include <turbo_error.h>
#include <turbo_vec.h>

#include <stdlib.h>
#include <string.h>

static const uint8_t TR_RAFT_WIRE_MAGIC[4] = {'T', 'R', 'F', 'T'};

struct tr_raft_wire_codec {
    DataBind *binding_v2;
    DataBind *binding_v3;
};

static void tr_put_u16(uint8_t *output, uint16_t value)
{
    output[0] = (uint8_t) (value >> 8U);
    output[1] = (uint8_t) value;
}

static void tr_put_u32(uint8_t *output, uint32_t value)
{
    output[0] = (uint8_t) (value >> 24U);
    output[1] = (uint8_t) (value >> 16U);
    output[2] = (uint8_t) (value >> 8U);
    output[3] = (uint8_t) value;
}

static void tr_put_u64(uint8_t *output, uint64_t value)
{
    size_t index;

    for (index = 0U; index < 8U; ++index) {
        output[index] = (uint8_t) (value >> (56U - 8U * index));
    }
}

static uint16_t tr_get_u16(const uint8_t *input)
{
    return (uint16_t) (((uint16_t) input[0] << 8U) | input[1]);
}

static uint32_t tr_get_u32(const uint8_t *input)
{
    return ((uint32_t) input[0] << 24U) |
           ((uint32_t) input[1] << 16U) |
           ((uint32_t) input[2] << 8U) | (uint32_t) input[3];
}

static uint64_t tr_get_u64(const uint8_t *input)
{
    uint64_t value = 0U;
    size_t index;

    for (index = 0U; index < 8U; ++index) {
        value = (value << 8U) | input[index];
    }
    return value;
}

static bool tr_message_valid(const tr_raft_message_t *message)
{
    bool read_message;

    if (message == NULL || message->type > TR_RAFT_MSG_READ_INDEX_RESPONSE ||
        message->from == 0U || message->to == 0U ||
        message->entry_count > TR_RAFT_MAX_APPEND_ENTRIES) {
        return false;
    }
    read_message = message->type == TR_RAFT_MSG_READ_INDEX_REQUEST ||
                   message->type == TR_RAFT_MSG_READ_INDEX_RESPONSE;
    if (read_message) {
        return message->context_id != 0U && message->term != 0U &&
               message->entry_count == 0U &&
               message->entries[0].data_length == 0U;
    }
    if (message->context_id != 0U) {
        return false;
    }
    if (message->entry_count == 0U) {
        return message->entries[0].data_length == 0U;
    }
    if (message->type != TR_RAFT_MSG_APPEND_REQUEST) {
        return false;
    }
    for (size_t index = 0U; index < message->entry_count; ++index) {
        const tr_raft_entry_t *entry = &message->entries[index];

        if (entry->index == 0U || entry->term == 0U ||
            entry->data_length > TR_RAFT_MAX_ENTRY_BYTES ||
            entry->index != message->previous_log_index + index + 1U) {
            return false;
        }
        if (tr_raft_conf_entry_is_configuration(entry) &&
            tr_raft_conf_entry_decode(entry, &(tr_raft_conf_t){0}) !=
                TURBO_OK) {
            return false;
        }
    }
    return true;
}

static bool tr_snapshot_chunk_valid(const tr_raft_snapshot_chunk_t *chunk)
{
    uint64_t remaining;
    size_t expected_length;

    if (chunk == NULL || chunk->from == 0U || chunk->to == 0U ||
        chunk->term == 0U || chunk->snapshot_index == 0U ||
        chunk->snapshot_term == 0U ||
        chunk->snapshot_size > TR_RAFT_WIRE_MAX_SNAPSHOT_BYTES ||
        chunk->snapshot_offset > chunk->snapshot_size ||
        chunk->data_length > TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES) {
        return false;
    }
    remaining = chunk->snapshot_size - chunk->snapshot_offset;
    expected_length = remaining > TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES
                          ? TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES
                          : (size_t) remaining;
    if ((chunk->snapshot_offset == 0U &&
         (!chunk->has_configuration ||
          tr_raft_conf_validate(&chunk->configuration) != TURBO_OK)) ||
        (chunk->snapshot_offset != 0U && chunk->has_configuration)) {
        return false;
    }
    return chunk->data_length == expected_length &&
           chunk->done ==
               (remaining <= TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES);
}

static bool tr_snapshot_ack_valid(const tr_raft_snapshot_ack_t *ack)
{
    return ack != NULL && ack->from != 0U && ack->to != 0U &&
           ack->term != 0U && ack->snapshot_index != 0U &&
           ack->snapshot_size <= TR_RAFT_WIRE_MAX_SNAPSHOT_BYTES &&
           ack->next_offset <= ack->snapshot_size;
}

static void tr_wire_write_envelope(
    uint8_t *output,
    uint16_t wire_version,
    uint32_t payload_kind,
    size_t payload_length,
    const tr_raft_wire_metadata_t *metadata)
{
    memcpy(output, TR_RAFT_WIRE_MAGIC, sizeof(TR_RAFT_WIRE_MAGIC));
    tr_put_u16(output + 4U, wire_version);
    tr_put_u16(output + 6U, TR_RAFT_WIRE_HEADER_SIZE);
    tr_put_u32(output + 8U, (uint32_t) payload_length);
    tr_put_u32(output + 12U, payload_kind);
    memcpy(output + 16U, metadata->cluster_id.bytes,
           sizeof(metadata->cluster_id.bytes));
    tr_put_u64(output + 32U, metadata->message_id);
}

static int tr_wire_read_envelope(
    const uint8_t *frame,
    size_t frame_length,
    uint32_t expected_payload_kind,
    tr_raft_wire_metadata_t *metadata,
    uint32_t *out_payload_length,
    uint16_t *out_wire_version)
{
    uint32_t payload_length;
    uint16_t wire_version = tr_get_u16(frame + 4U);

    if (memcmp(frame, TR_RAFT_WIRE_MAGIC, sizeof(TR_RAFT_WIRE_MAGIC)) != 0 ||
        wire_version < TR_RAFT_WIRE_MIN_VERSION ||
        wire_version > TR_RAFT_WIRE_MAX_VERSION ||
        tr_get_u16(frame + 6U) != TR_RAFT_WIRE_HEADER_SIZE ||
        tr_get_u32(frame + 12U) != expected_payload_kind) {
        return TURBO_EPROTO;
    }
    payload_length = tr_get_u32(frame + 8U);
    if (payload_length > TR_RAFT_WIRE_MAX_PAYLOAD_SIZE ||
        frame_length != TR_RAFT_WIRE_HEADER_SIZE + payload_length) {
        return TURBO_EPROTO;
    }
    memset(metadata, 0, sizeof(*metadata));
    memcpy(metadata->cluster_id.bytes, frame + 16U,
           sizeof(metadata->cluster_id.bytes));
    metadata->message_id = tr_get_u64(frame + 32U);
    *out_payload_length = payload_length;
    if (out_wire_version != NULL) {
        *out_wire_version = wire_version;
    }
    return TURBO_OK;
}

static int tr_wire_v3_from_message(RaftWireMessageV3_t *wire,
                                   const tr_raft_message_t *message)
{
    uint64_t *entry_indices[TR_RAFT_MAX_APPEND_ENTRIES] = {
        &wire->entry1_index, &wire->entry2_index, &wire->entry3_index,
        &wire->entry4_index, &wire->entry5_index, &wire->entry6_index,
        &wire->entry7_index, &wire->entry8_index};
    uint64_t *entry_terms[TR_RAFT_MAX_APPEND_ENTRIES] = {
        &wire->entry1_term, &wire->entry2_term, &wire->entry3_term,
        &wire->entry4_term, &wire->entry5_term, &wire->entry6_term,
        &wire->entry7_term, &wire->entry8_term};
    uint64_t *entry_commands[TR_RAFT_MAX_APPEND_ENTRIES] = {
        &wire->entry1_command_id, &wire->entry2_command_id,
        &wire->entry3_command_id, &wire->entry4_command_id,
        &wire->entry5_command_id, &wire->entry6_command_id,
        &wire->entry7_command_id, &wire->entry8_command_id};
    tbe_bytes_t *entry_data[TR_RAFT_MAX_APPEND_ENTRIES] = {
        &wire->entry1_data, &wire->entry2_data, &wire->entry3_data,
        &wire->entry4_data, &wire->entry5_data, &wire->entry6_data,
        &wire->entry7_data, &wire->entry8_data};
    size_t index;

    wire->message_type = (uint8_t) message->type;
    wire->granted = message->granted ? 1U : 0U;
    wire->entry_count = (uint32_t) message->entry_count;
    wire->from_node = message->from;
    wire->to_node = message->to;
    wire->term = message->term;
    wire->campaign_term =
        message->type == TR_RAFT_MSG_READ_INDEX_REQUEST ||
                message->type == TR_RAFT_MSG_READ_INDEX_RESPONSE
            ? message->context_id
            : message->campaign_term;
    wire->last_log_index = message->last_log_index;
    wire->last_log_term = message->last_log_term;
    wire->leader_commit = message->leader_commit;
    wire->previous_log_index = message->previous_log_index;
    wire->previous_log_term = message->previous_log_term;
    wire->match_index = message->match_index;
    wire->reject_hint = message->reject_hint;
    for (index = 0U; index < message->entry_count; ++index) {
        int result;

        *entry_indices[index] = message->entries[index].index;
        *entry_terms[index] = message->entries[index].term;
        *entry_commands[index] = message->entries[index].command_id;
        result = turbo_vec_resize(&entry_data[index]->raw,
                                  message->entries[index].data_length);
        if (result != TURBO_OK) {
            return result;
        }
        if (message->entries[index].data_length != 0U) {
            memcpy(tbe_bytes_t_data(entry_data[index]),
                   message->entries[index].data,
                   message->entries[index].data_length);
        }
    }
    return TURBO_OK;
}

static int tr_raft_wire_encode_v3(tr_raft_wire_codec_t *codec,
                                  const tr_raft_wire_metadata_t *metadata,
                                  const tr_raft_message_t *message,
                                  uint8_t *output,
                                  size_t output_capacity,
                                  size_t *output_length)
{
    RaftWireMessageV3_t wire;
    DataBindError error = DATA_BIND_ERROR_INIT;
    DataBindStatus status;
    size_t payload_length = 0U;
    int result;

    RaftWireMessageV3_init(&wire);
    result = tr_wire_v3_from_message(&wire, message);
    if (result != TURBO_OK) {
        RaftWireMessageV3_clear(&wire);
        return result;
    }
    status = RaftWireMessageV3_to_bin_into(
        &wire, output + TR_RAFT_WIRE_HEADER_SIZE,
        output_capacity - TR_RAFT_WIRE_HEADER_SIZE, &payload_length, &error);
    RaftWireMessageV3_clear(&wire);
    *output_length = TR_RAFT_WIRE_HEADER_SIZE + payload_length;
    if (status != DATA_BIND_OK) {
        return *output_length > output_capacity ? TURBO_ENOSPC : TURBO_EPROTO;
    }
    if (payload_length > TR_RAFT_WIRE_MAX_PAYLOAD_SIZE) {
        return TURBO_EPROTO;
    }
    tr_wire_write_envelope(output, TR_RAFT_WIRE_VERSION,
                           TR_RAFT_WIRE_PAYLOAD_RAFT, payload_length,
                           metadata);
    return TURBO_OK;
}

static void tr_wire_from_message(RaftWireMessage_t *wire,
                                 const tr_raft_message_t *message)
{
    wire->message_type = (uint8_t) message->type;
    wire->granted = message->granted ? 1U : 0U;
    wire->entry_count = (uint32_t) message->entry_count;
    wire->from_node = message->from;
    wire->to_node = message->to;
    wire->term = message->term;
    wire->campaign_term =
        message->type == TR_RAFT_MSG_READ_INDEX_REQUEST ||
                message->type == TR_RAFT_MSG_READ_INDEX_RESPONSE
            ? message->context_id
            : message->campaign_term;
    wire->last_log_index = message->last_log_index;
    wire->last_log_term = message->last_log_term;
    wire->leader_commit = message->leader_commit;
    wire->previous_log_index = message->previous_log_index;
    wire->previous_log_term = message->previous_log_term;
    wire->match_index = message->match_index;
    wire->reject_hint = message->reject_hint;
    if (message->entry_count != 0U) {
        wire->entry_index = message->entry.index;
        wire->entry_term = message->entry.term;
        wire->entry_command_id = message->entry.command_id;
    }
}

int tr_raft_wire_codec_create(tr_raft_wire_codec_t **out_codec)
{
    tr_raft_wire_codec_t *codec;
    DataBindError error = DATA_BIND_ERROR_INIT;
    DataBindStatus status;

    if (out_codec == NULL) {
        return TURBO_EINVAL;
    }
    *out_codec = NULL;
    codec = (tr_raft_wire_codec_t *) calloc(1U, sizeof(*codec));
    if (codec == NULL) {
        return TURBO_ENOMEM;
    }
    status = TurboRaftWire_codec_create(&codec->binding_v2, &error);
    if (status != DATA_BIND_OK) {
        free(codec);
        return TURBO_EPROTO;
    }
    status = TurboRaftWireV3_codec_create(&codec->binding_v3, &error);
    if (status != DATA_BIND_OK) {
        data_bind_free(codec->binding_v2);
        free(codec);
        return TURBO_EPROTO;
    }
    *out_codec = codec;
    return TURBO_OK;
}

void tr_raft_wire_codec_destroy(tr_raft_wire_codec_t *codec)
{
    if (codec == NULL) {
        return;
    }
    data_bind_free(codec->binding_v3);
    data_bind_free(codec->binding_v2);
    free(codec);
}

int tr_raft_wire_encode_version(tr_raft_wire_codec_t *codec,
                                uint16_t wire_version,
                                const tr_raft_wire_metadata_t *metadata,
                                const tr_raft_message_t *message,
                                uint8_t *output,
                                size_t output_capacity,
                                size_t *output_length)
{
    RaftWireMessage_t wire;
    DataBindError error = DATA_BIND_ERROR_INIT;
    DataBindStatus status;
    size_t payload_length = 0U;
    int resize_result;

    if (output_length != NULL) {
        *output_length = 0U;
    }
    if (codec == NULL || codec->binding_v2 == NULL ||
        codec->binding_v3 == NULL || metadata == NULL ||
        !tr_message_valid(message) || output == NULL || output_length == NULL) {
        return TURBO_EINVAL;
    }
    if (wire_version < TR_RAFT_WIRE_MIN_VERSION ||
        wire_version > TR_RAFT_WIRE_VERSION ||
        (wire_version == 2U && message->entry_count > 1U)) {
        return TURBO_EPROTO;
    }
    if (output_capacity < TR_RAFT_WIRE_HEADER_SIZE) {
        *output_length = TR_RAFT_WIRE_HEADER_SIZE;
        return TURBO_ENOSPC;
    }

    if (wire_version == TR_RAFT_WIRE_VERSION) {
        return tr_raft_wire_encode_v3(codec, metadata, message, output,
                                      output_capacity, output_length);
    }
    RaftWireMessage_init(&wire);
    tr_wire_from_message(&wire, message);
    if (message->entry_count != 0U && message->entry.data_length != 0U) {
        resize_result = turbo_vec_resize(&wire.entry_data.raw,
                                         message->entry.data_length);
        if (resize_result != TURBO_OK) {
            RaftWireMessage_clear(&wire);
            return resize_result;
        }
        memcpy(tbe_bytes_t_data(&wire.entry_data), message->entry.data,
               message->entry.data_length);
    }

    status = RaftWireMessage_to_bin_into(
        &wire, output + TR_RAFT_WIRE_HEADER_SIZE,
        output_capacity - TR_RAFT_WIRE_HEADER_SIZE, &payload_length, &error);
    RaftWireMessage_clear(&wire);
    *output_length = TR_RAFT_WIRE_HEADER_SIZE + payload_length;
    if (status != DATA_BIND_OK) {
        return *output_length > output_capacity ? TURBO_ENOSPC : TURBO_EPROTO;
    }
    if (payload_length > TR_RAFT_WIRE_MAX_PAYLOAD_SIZE) {
        return TURBO_EPROTO;
    }

    tr_wire_write_envelope(output, wire_version, TR_RAFT_WIRE_PAYLOAD_RAFT,
                           payload_length, metadata);
    return TURBO_OK;
}

int tr_raft_wire_encode(tr_raft_wire_codec_t *codec,
                        const tr_raft_wire_metadata_t *metadata,
                        const tr_raft_message_t *message,
                        uint8_t *output,
                        size_t output_capacity,
                        size_t *output_length)
{
    return tr_raft_wire_encode_version(codec, TR_RAFT_WIRE_VERSION, metadata,
                                       message, output, output_capacity,
                                       output_length);
}

static int tr_raft_wire_decode_v3(tr_raft_wire_codec_t *codec,
                                  const uint8_t *payload,
                                  size_t payload_length,
                                  tr_raft_message_t *message)
{
    RaftWireMessageV3_t wire;
    DataBindError error = DATA_BIND_ERROR_INIT;
    DataBindStatus status;
    uint64_t *entry_indices[TR_RAFT_MAX_APPEND_ENTRIES];
    uint64_t *entry_terms[TR_RAFT_MAX_APPEND_ENTRIES];
    uint64_t *entry_commands[TR_RAFT_MAX_APPEND_ENTRIES];
    tbe_bytes_t *entry_data[TR_RAFT_MAX_APPEND_ENTRIES];
    size_t index;

    RaftWireMessageV3_init(&wire);
    status = RaftWireMessageV3_from_bin(codec->binding_v3, &wire, payload,
                                        payload_length, &error);
    if (status != DATA_BIND_OK) {
        RaftWireMessageV3_clear(&wire);
        return TURBO_EPROTO;
    }
    {
        uint64_t *indices[] = {
            &wire.entry1_index, &wire.entry2_index, &wire.entry3_index,
            &wire.entry4_index, &wire.entry5_index, &wire.entry6_index,
            &wire.entry7_index, &wire.entry8_index};
        uint64_t *terms[] = {
            &wire.entry1_term, &wire.entry2_term, &wire.entry3_term,
            &wire.entry4_term, &wire.entry5_term, &wire.entry6_term,
            &wire.entry7_term, &wire.entry8_term};
        uint64_t *commands[] = {
            &wire.entry1_command_id, &wire.entry2_command_id,
            &wire.entry3_command_id, &wire.entry4_command_id,
            &wire.entry5_command_id, &wire.entry6_command_id,
            &wire.entry7_command_id, &wire.entry8_command_id};
        tbe_bytes_t *data[] = {
            &wire.entry1_data, &wire.entry2_data, &wire.entry3_data,
            &wire.entry4_data, &wire.entry5_data, &wire.entry6_data,
            &wire.entry7_data, &wire.entry8_data};

        memcpy(entry_indices, indices, sizeof(indices));
        memcpy(entry_terms, terms, sizeof(terms));
        memcpy(entry_commands, commands, sizeof(commands));
        memcpy(entry_data, data, sizeof(data));
    }
    if (wire.reserved != 0U || wire.granted > 1U ||
        wire.message_type > TR_RAFT_MSG_READ_INDEX_RESPONSE ||
        wire.from_node == 0U || wire.to_node == 0U ||
        wire.entry_count > TR_RAFT_MAX_APPEND_ENTRIES) {
        RaftWireMessageV3_clear(&wire);
        return TURBO_EPROTO;
    }
    for (index = 0U; index < TR_RAFT_MAX_APPEND_ENTRIES; ++index) {
        size_t data_length = tbe_bytes_t_size(entry_data[index]);
        bool active = index < wire.entry_count;

        if (data_length > TR_RAFT_MAX_ENTRY_BYTES ||
            (active && (*entry_indices[index] == 0U ||
                        *entry_terms[index] == 0U)) ||
            (!active && (*entry_indices[index] != 0U ||
                         *entry_terms[index] != 0U ||
                         *entry_commands[index] != 0U || data_length != 0U))) {
            RaftWireMessageV3_clear(&wire);
            return TURBO_EPROTO;
        }
    }

    memset(message, 0, sizeof(*message));
    message->type = (tr_raft_message_type_t) wire.message_type;
    message->granted = wire.granted != 0U;
    message->entry_count = wire.entry_count;
    message->from = wire.from_node;
    message->to = wire.to_node;
    message->term = wire.term;
    if (message->type == TR_RAFT_MSG_READ_INDEX_REQUEST ||
        message->type == TR_RAFT_MSG_READ_INDEX_RESPONSE) {
        message->context_id = wire.campaign_term;
    } else {
        message->campaign_term = wire.campaign_term;
    }
    message->last_log_index = wire.last_log_index;
    message->last_log_term = wire.last_log_term;
    message->leader_commit = wire.leader_commit;
    message->previous_log_index = wire.previous_log_index;
    message->previous_log_term = wire.previous_log_term;
    message->match_index = wire.match_index;
    message->reject_hint = wire.reject_hint;
    for (index = 0U; index < message->entry_count; ++index) {
        size_t data_length = tbe_bytes_t_size(entry_data[index]);

        message->entries[index].index = *entry_indices[index];
        message->entries[index].term = *entry_terms[index];
        message->entries[index].command_id = *entry_commands[index];
        message->entries[index].data_length = data_length;
        if (data_length != 0U) {
            memcpy(message->entries[index].data,
                   tbe_bytes_t_data(entry_data[index]), data_length);
        }
    }
    RaftWireMessageV3_clear(&wire);
    return tr_message_valid(message) ? TURBO_OK : TURBO_EPROTO;
}

int tr_raft_wire_decode(tr_raft_wire_codec_t *codec,
                        const uint8_t *frame,
                        size_t frame_length,
                        tr_raft_wire_metadata_t *metadata,
                        tr_raft_message_t *message)
{
    RaftWireMessage_t wire;
    DataBindError error = DATA_BIND_ERROR_INIT;
    DataBindStatus status;
    uint32_t payload_length;
    size_t entry_data_length;
    uint16_t wire_version;

    if (codec == NULL || codec->binding_v2 == NULL ||
        codec->binding_v3 == NULL || frame == NULL ||
        metadata == NULL || message == NULL ||
        frame_length < TR_RAFT_WIRE_HEADER_SIZE) {
        return TURBO_EINVAL;
    }
    if (tr_wire_read_envelope(frame, frame_length,
                              TR_RAFT_WIRE_PAYLOAD_RAFT, metadata,
                              &payload_length, &wire_version) != TURBO_OK) {
        return TURBO_EPROTO;
    }
    if (wire_version == TR_RAFT_WIRE_VERSION) {
        return tr_raft_wire_decode_v3(
            codec, frame + TR_RAFT_WIRE_HEADER_SIZE, payload_length, message);
    }
    if (wire_version != TR_RAFT_WIRE_MIN_VERSION) {
        return TURBO_EPROTO;
    }

    RaftWireMessage_init(&wire);
    status = RaftWireMessage_from_bin(
        codec->binding_v2, &wire, frame + TR_RAFT_WIRE_HEADER_SIZE,
        payload_length, &error);
    if (status != DATA_BIND_OK) {
        RaftWireMessage_clear(&wire);
        return TURBO_EPROTO;
    }
    entry_data_length = tbe_bytes_t_size(&wire.entry_data);
    if (wire.reserved != 0U || wire.granted > 1U ||
        wire.message_type > TR_RAFT_MSG_READ_INDEX_RESPONSE ||
        wire.from_node == 0U || wire.to_node == 0U || wire.entry_count > 1U ||
        entry_data_length > TR_RAFT_MAX_ENTRY_BYTES ||
        (wire.entry_count == 0U &&
         (wire.entry_index != 0U || wire.entry_term != 0U ||
          wire.entry_command_id != 0U || entry_data_length != 0U)) ||
        (wire.entry_count == 1U &&
         (wire.message_type != TR_RAFT_MSG_APPEND_REQUEST ||
          wire.entry_index == 0U || wire.entry_term == 0U)) ||
        ((wire.message_type == TR_RAFT_MSG_READ_INDEX_REQUEST ||
          wire.message_type == TR_RAFT_MSG_READ_INDEX_RESPONSE) &&
         (wire.campaign_term == 0U || wire.term == 0U))) {
        RaftWireMessage_clear(&wire);
        return TURBO_EPROTO;
    }

    memset(message, 0, sizeof(*message));
    message->type = (tr_raft_message_type_t) wire.message_type;
    message->granted = wire.granted != 0U;
    message->entry_count = wire.entry_count;
    message->from = wire.from_node;
    message->to = wire.to_node;
    message->term = wire.term;
    if (message->type == TR_RAFT_MSG_READ_INDEX_REQUEST ||
        message->type == TR_RAFT_MSG_READ_INDEX_RESPONSE) {
        message->context_id = wire.campaign_term;
    } else {
        message->campaign_term = wire.campaign_term;
    }
    message->last_log_index = wire.last_log_index;
    message->last_log_term = wire.last_log_term;
    message->leader_commit = wire.leader_commit;
    message->previous_log_index = wire.previous_log_index;
    message->previous_log_term = wire.previous_log_term;
    message->match_index = wire.match_index;
    message->reject_hint = wire.reject_hint;
    if (wire.entry_count != 0U) {
        message->entry.index = wire.entry_index;
        message->entry.term = wire.entry_term;
        message->entry.command_id = wire.entry_command_id;
        message->entry.data_length = entry_data_length;
        if (entry_data_length != 0U) {
            memcpy(message->entry.data, tbe_bytes_t_data(&wire.entry_data),
                   entry_data_length);
        }
    }
    RaftWireMessage_clear(&wire);
    return tr_message_valid(message) ? TURBO_OK : TURBO_EPROTO;
}

int tr_raft_wire_encode_snapshot_chunk(
    tr_raft_wire_codec_t *codec,
    const tr_raft_wire_metadata_t *metadata,
    const tr_raft_snapshot_chunk_t *chunk,
    uint8_t *output,
    size_t output_capacity,
    size_t *output_length)
{
    InstallSnapshotChunk_t wire;
    DataBindError error = DATA_BIND_ERROR_INIT;
    DataBindStatus status;
    size_t payload_length = 0U;
    uint8_t encoded_configuration[TR_RAFT_CONF_MAX_ENCODED_SIZE];
    size_t encoded_configuration_size = 0U;
    int result;

    if (output_length != NULL) {
        *output_length = 0U;
    }
    if (codec == NULL || codec->binding_v2 == NULL || metadata == NULL ||
        !tr_snapshot_chunk_valid(chunk) || output == NULL ||
        output_length == NULL) {
        return TURBO_EINVAL;
    }
    if (output_capacity < TR_RAFT_WIRE_HEADER_SIZE) {
        *output_length = TR_RAFT_WIRE_HEADER_SIZE;
        return TURBO_ENOSPC;
    }
    InstallSnapshotChunk_init(&wire);
    wire.from_node = chunk->from;
    wire.to_node = chunk->to;
    wire.term = chunk->term;
    wire.snapshot_index = chunk->snapshot_index;
    wire.snapshot_term = chunk->snapshot_term;
    wire.snapshot_offset = chunk->snapshot_offset;
    wire.snapshot_size = chunk->snapshot_size;
    wire.done = chunk->done ? 1U : 0U;
    result = chunk->has_configuration
                 ? tr_raft_conf_encode(
                       &chunk->configuration, encoded_configuration,
                       sizeof(encoded_configuration),
                       &encoded_configuration_size)
                 : TURBO_OK;
    if (result == TURBO_OK) {
        result = turbo_vec_resize(&wire.snapshot_configuration.raw,
                                  encoded_configuration_size);
    }
    if (result == TURBO_OK && encoded_configuration_size != 0U) {
        memcpy(tbe_bytes_t_data(&wire.snapshot_configuration),
               encoded_configuration, encoded_configuration_size);
    }
    if (result == TURBO_OK) {
        result = turbo_vec_resize(&wire.snapshot_digest.raw,
                                  TR_RAFT_WIRE_SNAPSHOT_DIGEST_SIZE);
    }
    if (result == TURBO_OK) {
        memcpy(tbe_bytes_t_data(&wire.snapshot_digest), chunk->snapshot_digest,
               TR_RAFT_WIRE_SNAPSHOT_DIGEST_SIZE);
        result = turbo_vec_resize(&wire.chunk_data.raw, chunk->data_length);
    }
    if (result == TURBO_OK && chunk->data_length != 0U) {
        memcpy(tbe_bytes_t_data(&wire.chunk_data), chunk->data,
               chunk->data_length);
    }
    if (result != TURBO_OK) {
        InstallSnapshotChunk_clear(&wire);
        return result;
    }
    status = InstallSnapshotChunk_to_bin_into(
        &wire, output + TR_RAFT_WIRE_HEADER_SIZE,
        output_capacity - TR_RAFT_WIRE_HEADER_SIZE, &payload_length, &error);
    InstallSnapshotChunk_clear(&wire);
    *output_length = TR_RAFT_WIRE_HEADER_SIZE + payload_length;
    if (status != DATA_BIND_OK) {
        return *output_length > output_capacity ? TURBO_ENOSPC : TURBO_EPROTO;
    }
    if (payload_length > TR_RAFT_WIRE_MAX_PAYLOAD_SIZE) {
        return TURBO_EPROTO;
    }
    tr_wire_write_envelope(output, TR_RAFT_WIRE_SNAPSHOT_VERSION,
                           TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_CHUNK,
                           payload_length, metadata);
    return TURBO_OK;
}

int tr_raft_wire_decode_snapshot_chunk(
    tr_raft_wire_codec_t *codec,
    const uint8_t *frame,
    size_t frame_length,
    tr_raft_wire_metadata_t *metadata,
    tr_raft_snapshot_chunk_t *chunk)
{
    InstallSnapshotChunk_t wire;
    DataBindError error = DATA_BIND_ERROR_INIT;
    uint32_t payload_length;
    size_t digest_length;
    size_t configuration_length;
    size_t data_length;
    uint16_t wire_version;
    DataBindStatus status;

    if (codec == NULL || codec->binding_v2 == NULL || frame == NULL ||
        metadata == NULL || chunk == NULL ||
        frame_length < TR_RAFT_WIRE_HEADER_SIZE) {
        return TURBO_EINVAL;
    }
    if (tr_wire_read_envelope(frame, frame_length,
                              TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_CHUNK, metadata,
                              &payload_length, &wire_version) != TURBO_OK ||
        wire_version != TR_RAFT_WIRE_SNAPSHOT_VERSION) {
        return TURBO_EPROTO;
    }
    InstallSnapshotChunk_init(&wire);
    status = InstallSnapshotChunk_from_bin(
        codec->binding_v2, &wire, frame + TR_RAFT_WIRE_HEADER_SIZE,
        payload_length, &error);
    if (status != DATA_BIND_OK) {
        InstallSnapshotChunk_clear(&wire);
        return TURBO_EPROTO;
    }
    digest_length = tbe_bytes_t_size(&wire.snapshot_digest);
    configuration_length =
        tbe_bytes_t_size(&wire.snapshot_configuration);
    data_length = tbe_bytes_t_size(&wire.chunk_data);
    if (wire.done > 1U ||
        digest_length != TR_RAFT_WIRE_SNAPSHOT_DIGEST_SIZE ||
        configuration_length > TR_RAFT_CONF_MAX_ENCODED_SIZE ||
        data_length > TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES) {
        InstallSnapshotChunk_clear(&wire);
        return TURBO_EPROTO;
    }
    memset(chunk, 0, sizeof(*chunk));
    chunk->from = wire.from_node;
    chunk->to = wire.to_node;
    chunk->term = wire.term;
    chunk->snapshot_index = wire.snapshot_index;
    chunk->snapshot_term = wire.snapshot_term;
    chunk->snapshot_offset = wire.snapshot_offset;
    chunk->snapshot_size = wire.snapshot_size;
    chunk->done = wire.done != 0U;
    chunk->data_length = data_length;
    if (configuration_length != 0U) {
        if (tr_raft_conf_decode(
                tbe_bytes_t_data(&wire.snapshot_configuration),
                configuration_length, &chunk->configuration) != TURBO_OK) {
            InstallSnapshotChunk_clear(&wire);
            return TURBO_EPROTO;
        }
        chunk->has_configuration = true;
    }
    memcpy(chunk->snapshot_digest, tbe_bytes_t_data(&wire.snapshot_digest),
           digest_length);
    if (data_length != 0U) {
        memcpy(chunk->data, tbe_bytes_t_data(&wire.chunk_data), data_length);
    }
    InstallSnapshotChunk_clear(&wire);
    return tr_snapshot_chunk_valid(chunk) ? TURBO_OK : TURBO_EPROTO;
}

int tr_raft_wire_encode_snapshot_ack(
    tr_raft_wire_codec_t *codec,
    const tr_raft_wire_metadata_t *metadata,
    const tr_raft_snapshot_ack_t *ack,
    uint8_t *output,
    size_t output_capacity,
    size_t *output_length)
{
    InstallSnapshotAck_t wire;
    DataBindError error = DATA_BIND_ERROR_INIT;
    DataBindStatus status;
    size_t payload_length = 0U;
    int result;

    if (output_length != NULL) {
        *output_length = 0U;
    }
    if (codec == NULL || codec->binding_v2 == NULL || metadata == NULL ||
        !tr_snapshot_ack_valid(ack) || output == NULL ||
        output_length == NULL) {
        return TURBO_EINVAL;
    }
    if (output_capacity < TR_RAFT_WIRE_HEADER_SIZE) {
        *output_length = TR_RAFT_WIRE_HEADER_SIZE;
        return TURBO_ENOSPC;
    }
    InstallSnapshotAck_init(&wire);
    wire.from_node = ack->from;
    wire.to_node = ack->to;
    wire.term = ack->term;
    wire.snapshot_index = ack->snapshot_index;
    wire.snapshot_size = ack->snapshot_size;
    wire.next_offset = ack->next_offset;
    wire.accepted = ack->accepted ? 1U : 0U;
    result = turbo_vec_resize(&wire.snapshot_digest.raw,
                              TR_RAFT_WIRE_SNAPSHOT_DIGEST_SIZE);
    if (result == TURBO_OK) {
        memcpy(tbe_bytes_t_data(&wire.snapshot_digest), ack->snapshot_digest,
               TR_RAFT_WIRE_SNAPSHOT_DIGEST_SIZE);
    }
    if (result != TURBO_OK) {
        InstallSnapshotAck_clear(&wire);
        return result;
    }
    status = InstallSnapshotAck_to_bin_into(
        &wire, output + TR_RAFT_WIRE_HEADER_SIZE,
        output_capacity - TR_RAFT_WIRE_HEADER_SIZE, &payload_length, &error);
    InstallSnapshotAck_clear(&wire);
    *output_length = TR_RAFT_WIRE_HEADER_SIZE + payload_length;
    if (status != DATA_BIND_OK) {
        return *output_length > output_capacity ? TURBO_ENOSPC : TURBO_EPROTO;
    }
    if (payload_length > TR_RAFT_WIRE_MAX_PAYLOAD_SIZE) {
        return TURBO_EPROTO;
    }
    tr_wire_write_envelope(output, TR_RAFT_WIRE_SNAPSHOT_VERSION,
                           TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_ACK,
                           payload_length, metadata);
    return TURBO_OK;
}

int tr_raft_wire_decode_snapshot_ack(
    tr_raft_wire_codec_t *codec,
    const uint8_t *frame,
    size_t frame_length,
    tr_raft_wire_metadata_t *metadata,
    tr_raft_snapshot_ack_t *ack)
{
    InstallSnapshotAck_t wire;
    DataBindError error = DATA_BIND_ERROR_INIT;
    uint32_t payload_length;
    uint16_t wire_version;
    size_t digest_length;
    DataBindStatus status;

    if (codec == NULL || codec->binding_v2 == NULL || frame == NULL ||
        metadata == NULL || ack == NULL ||
        frame_length < TR_RAFT_WIRE_HEADER_SIZE) {
        return TURBO_EINVAL;
    }
    if (tr_wire_read_envelope(frame, frame_length,
                              TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_ACK, metadata,
                              &payload_length, &wire_version) != TURBO_OK ||
        wire_version != TR_RAFT_WIRE_SNAPSHOT_VERSION) {
        return TURBO_EPROTO;
    }
    InstallSnapshotAck_init(&wire);
    status = InstallSnapshotAck_from_bin(
        codec->binding_v2, &wire, frame + TR_RAFT_WIRE_HEADER_SIZE,
        payload_length, &error);
    if (status != DATA_BIND_OK) {
        InstallSnapshotAck_clear(&wire);
        return TURBO_EPROTO;
    }
    digest_length = tbe_bytes_t_size(&wire.snapshot_digest);
    if (wire.accepted > 1U ||
        digest_length != TR_RAFT_WIRE_SNAPSHOT_DIGEST_SIZE) {
        InstallSnapshotAck_clear(&wire);
        return TURBO_EPROTO;
    }
    memset(ack, 0, sizeof(*ack));
    ack->from = wire.from_node;
    ack->to = wire.to_node;
    ack->term = wire.term;
    ack->snapshot_index = wire.snapshot_index;
    ack->snapshot_size = wire.snapshot_size;
    ack->next_offset = wire.next_offset;
    ack->accepted = wire.accepted != 0U;
    memcpy(ack->snapshot_digest, tbe_bytes_t_data(&wire.snapshot_digest),
           digest_length);
    InstallSnapshotAck_clear(&wire);
    return tr_snapshot_ack_valid(ack) ? TURBO_OK : TURBO_EPROTO;
}

int tr_raft_wire_peek_version(
    const uint8_t *frame,
    size_t frame_length,
    uint16_t *out_version)
{
    uint32_t payload_length;
    uint16_t wire_version;

    if (frame == NULL || out_version == NULL ||
        frame_length < TR_RAFT_WIRE_HEADER_SIZE) {
        return TURBO_EINVAL;
    }
    wire_version = tr_get_u16(frame + 4U);
    if (memcmp(frame, TR_RAFT_WIRE_MAGIC, sizeof(TR_RAFT_WIRE_MAGIC)) != 0 ||
        wire_version < TR_RAFT_WIRE_MIN_VERSION ||
        wire_version > TR_RAFT_WIRE_MAX_VERSION ||
        tr_get_u16(frame + 6U) != TR_RAFT_WIRE_HEADER_SIZE) {
        return TURBO_EPROTO;
    }
    payload_length = tr_get_u32(frame + 8U);
    if (payload_length > TR_RAFT_WIRE_MAX_PAYLOAD_SIZE ||
        frame_length != TR_RAFT_WIRE_HEADER_SIZE + payload_length) {
        return TURBO_EPROTO;
    }
    *out_version = wire_version;
    return TURBO_OK;
}

int tr_raft_wire_peek_payload_kind(
    const uint8_t *frame,
    size_t frame_length,
    tr_raft_wire_payload_kind_t *out_kind)
{
    uint32_t payload_length;
    uint32_t payload_kind;
    uint16_t wire_version;

    if (out_kind == NULL) {
        return TURBO_EINVAL;
    }
    if (tr_raft_wire_peek_version(frame, frame_length, &wire_version) !=
        TURBO_OK) {
        return frame == NULL || frame_length < TR_RAFT_WIRE_HEADER_SIZE
                   ? TURBO_EINVAL
                   : TURBO_EPROTO;
    }
    payload_length = tr_get_u32(frame + 8U);
    payload_kind = tr_get_u32(frame + 12U);
    if (payload_kind < TR_RAFT_WIRE_PAYLOAD_RAFT ||
        payload_kind > TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_ACK) {
        return TURBO_EPROTO;
    }
    *out_kind = (tr_raft_wire_payload_kind_t) payload_kind;
    return TURBO_OK;
}
