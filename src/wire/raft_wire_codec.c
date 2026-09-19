#include <turboraft/raft_wire_codec.h>

#include "raft_configuration.h"
#include "../turboraft_stl_status.h"

#include "turboraft_wire_tbe.h"
#undef SCHEMA_GENERATED_H
#include "turboraft_wire_v3_tbe.h"

#include <salts_error.h>
#include <cstl/vec.h>

#include <stdlib.h>
#include <string.h>

static const uint8_t TR_RAFT_WIRE_MAGIC[4] = {'T', 'R', 'F', 'T'};

struct tr_raft_wire_codec {
    DataBind *binding;
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

static bool tr_wire_metadata_valid(
    const tr_raft_wire_metadata_t *metadata)
{
    return metadata != NULL && metadata->group_id != 0U;
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
                SALTS_OK) {
            return false;
        }
    }
    return true;
}

static bool tr_snapshot_chunk_valid(const tr_raft_snapshot_chunk_t *chunk)
{
    if (chunk == NULL || chunk->from == 0U || chunk->to == 0U ||
        chunk->term == 0U || chunk->snapshot_index == 0U ||
        chunk->snapshot_term == 0U || chunk->snapshot_term > chunk->term ||
        chunk->snapshot_size > TR_RAFT_WIRE_MAX_SNAPSHOT_BYTES ||
        chunk->snapshot_offset > chunk->snapshot_size ||
        chunk->data_length > TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES ||
        (chunk->data_length != 0U && chunk->data == NULL) ||
        chunk->data_length > chunk->snapshot_size - chunk->snapshot_offset) {
        return false;
    }
    if ((chunk->snapshot_offset == 0U &&
         (!chunk->has_configuration ||
          tr_raft_conf_validate(&chunk->configuration) != SALTS_OK)) ||
        (chunk->snapshot_offset != 0U && chunk->has_configuration)) {
        return false;
    }
    return chunk->done ==
               (chunk->snapshot_offset + chunk->data_length ==
                chunk->snapshot_size) &&
           (chunk->done || chunk->data_length != 0U);
}

static size_t tr_wire_payload_limit(uint32_t payload_kind)
{
    switch (payload_kind) {
    case TR_RAFT_WIRE_PAYLOAD_RAFT:
        return TR_RAFT_WIRE_MAX_RAFT_PAYLOAD_SIZE;
    case TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_CHUNK:
    case TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_ACK:
        return TR_RAFT_WIRE_MAX_SNAPSHOT_PAYLOAD_SIZE;
    case TR_RAFT_WIRE_PAYLOAD_DATA_CHUNK:
    case TR_RAFT_WIRE_PAYLOAD_DATA_ACK:
        return TR_RAFT_WIRE_MAX_DATA_PAYLOAD_SIZE;
    default:
        return 0U;
    }
}

static bool tr_data_chunk_valid(const tr_raft_data_chunk_t *chunk)
{
    return chunk != NULL && chunk->from != 0U && chunk->to != 0U &&
           chunk->term != 0U && chunk->stream_id != 0U &&
           chunk->stream_size <= TR_RAFT_WIRE_MAX_DATA_STREAM_BYTES &&
           chunk->stream_offset <= chunk->stream_size &&
           chunk->data_length <= TR_RAFT_WIRE_MAX_DATA_CHUNK_BYTES &&
           (chunk->data_length == 0U || chunk->data != NULL) &&
           chunk->data_length <= chunk->stream_size - chunk->stream_offset &&
           chunk->done ==
               (chunk->stream_offset + chunk->data_length ==
                chunk->stream_size) &&
           (chunk->done || chunk->data_length != 0U);
}

static bool tr_data_ack_valid(const tr_raft_data_ack_t *ack)
{
    return ack != NULL && ack->from != 0U && ack->to != 0U &&
           ack->term != 0U && ack->stream_id != 0U &&
           ack->stream_size <= TR_RAFT_WIRE_MAX_DATA_STREAM_BYTES &&
           ack->next_offset <= ack->stream_size &&
           (!ack->durable ||
            (ack->accepted && ack->next_offset == ack->stream_size));
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
    uint32_t payload_kind,
    size_t payload_length,
    const tr_raft_wire_metadata_t *metadata)
{
    memcpy(output, TR_RAFT_WIRE_MAGIC, sizeof(TR_RAFT_WIRE_MAGIC));
    tr_put_u16(output + 4U, TR_RAFT_WIRE_VERSION);
    tr_put_u16(output + 6U, TR_RAFT_WIRE_HEADER_SIZE);
    tr_put_u32(output + 8U, (uint32_t) payload_length);
    tr_put_u32(output + 12U, payload_kind);
    memcpy(output + 16U, metadata->cluster_id.bytes,
           sizeof(metadata->cluster_id.bytes));
    tr_put_u64(output + 32U, metadata->group_id);
    tr_put_u64(output + 40U, metadata->message_id);
}

static int tr_wire_read_envelope(
    const uint8_t *frame,
    size_t frame_length,
    uint32_t expected_payload_kind,
    tr_raft_wire_metadata_t *metadata,
    uint32_t *out_payload_length)
{
    uint32_t payload_length;

    if (frame == NULL || metadata == NULL || out_payload_length == NULL ||
        frame_length < TR_RAFT_WIRE_HEADER_SIZE) {
        return SALTS_EPROTO;
    }
    if (memcmp(frame, TR_RAFT_WIRE_MAGIC, sizeof(TR_RAFT_WIRE_MAGIC)) != 0 ||
        tr_get_u16(frame + 4U) != TR_RAFT_WIRE_VERSION ||
        tr_get_u16(frame + 6U) != TR_RAFT_WIRE_HEADER_SIZE ||
        tr_get_u32(frame + 12U) != expected_payload_kind) {
        return SALTS_EPROTO;
    }
    payload_length = tr_get_u32(frame + 8U);
    if (payload_length > tr_wire_payload_limit(expected_payload_kind) ||
        frame_length != TR_RAFT_WIRE_HEADER_SIZE + payload_length) {
        return SALTS_EPROTO;
    }

    memset(metadata, 0, sizeof(*metadata));
    memcpy(metadata->cluster_id.bytes, frame + 16U,
           sizeof(metadata->cluster_id.bytes));
    metadata->group_id = tr_get_u64(frame + 32U);
    metadata->message_id = tr_get_u64(frame + 40U);
    if (metadata->group_id == 0U) {
        return SALTS_EPROTO;
    }
    *out_payload_length = payload_length;
    return SALTS_OK;
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
        result = tr_raft_stl_status_to_error(vec_resize(
            &entry_data[index]->raw, message->entries[index].data_length));
        if (result != SALTS_OK) {
            return result;
        }
        if (message->entries[index].data_length != 0U) {
            memcpy(tbe_bytes_t_data(entry_data[index]),
                   message->entries[index].data,
                   message->entries[index].data_length);
        }
    }
    return SALTS_OK;
}

static int tr_raft_wire_encode_current(
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
    if (result != SALTS_OK) {
        RaftWireMessageV3_clear(&wire);
        return result;
    }
    status = RaftWireMessageV3_to_bin_into(
        &wire, output + TR_RAFT_WIRE_HEADER_SIZE,
        output_capacity - TR_RAFT_WIRE_HEADER_SIZE,
        &payload_length, &error);
    RaftWireMessageV3_clear(&wire);
    *output_length = TR_RAFT_WIRE_HEADER_SIZE + payload_length;
    if (status != DATA_BIND_OK) {
        return *output_length > output_capacity ? SALTS_ENOSPC : SALTS_EPROTO;
    }
    if (payload_length > TR_RAFT_WIRE_MAX_RAFT_PAYLOAD_SIZE) {
        return SALTS_EPROTO;
    }
    tr_wire_write_envelope(output, TR_RAFT_WIRE_PAYLOAD_RAFT,
                           payload_length, metadata);
    return SALTS_OK;
}

int tr_raft_wire_codec_create(tr_raft_wire_codec_t **out_codec)
{
    tr_raft_wire_codec_t *codec;
    DataBindError error = DATA_BIND_ERROR_INIT;
    DataBindStatus status;

    if (out_codec == NULL) {
        return SALTS_EINVAL;
    }
    *out_codec = NULL;
    codec = (tr_raft_wire_codec_t *) calloc(1U, sizeof(*codec));
    if (codec == NULL) {
        return SALTS_ENOMEM;
    }
    status = TurboRaftWire_codec_create(&codec->binding, &error);
    if (status != DATA_BIND_OK) {
        free(codec);
        return SALTS_EPROTO;
    }
    *out_codec = codec;
    return SALTS_OK;
}

void tr_raft_wire_codec_destroy(tr_raft_wire_codec_t *codec)
{
    if (codec == NULL) {
        return;
    }
    data_bind_free(codec->binding);
    free(codec);
}

int tr_raft_wire_encode(tr_raft_wire_codec_t *codec,
                        const tr_raft_wire_metadata_t *metadata,
                        const tr_raft_message_t *message,
                        uint8_t *output,
                        size_t output_capacity,
                        size_t *output_length)
{
    if (output_length != NULL) {
        *output_length = 0U;
    }
    if (codec == NULL || codec->binding == NULL ||
        !tr_wire_metadata_valid(metadata) || !tr_message_valid(message) ||
        output == NULL || output_length == NULL) {
        return SALTS_EINVAL;
    }
    if (output_capacity < TR_RAFT_WIRE_HEADER_SIZE) {
        *output_length = TR_RAFT_WIRE_HEADER_SIZE;
        return SALTS_ENOSPC;
    }
    return tr_raft_wire_encode_current(metadata, message, output,
                                       output_capacity, output_length);
}

typedef struct tr_wire_v3_fields {
    uint8_t message_type;
    uint8_t granted;
    uint16_t reserved;
    uint32_t entry_count;
    uint64_t from_node;
    uint64_t to_node;
    uint64_t term;
    uint64_t campaign_term;
    uint64_t last_log_index;
    uint64_t last_log_term;
    uint64_t leader_commit;
    uint64_t previous_log_index;
    uint64_t previous_log_term;
    uint64_t match_index;
    uint64_t reject_hint;
    uint64_t entry_indices[TR_RAFT_MAX_APPEND_ENTRIES];
    uint64_t entry_terms[TR_RAFT_MAX_APPEND_ENTRIES];
    uint64_t entry_commands[TR_RAFT_MAX_APPEND_ENTRIES];
    /* Borrowed from the immutable frame and consumed before decode returns. */
    tbe_var_data_t entry_data[TR_RAFT_MAX_APPEND_ENTRIES];
} tr_wire_v3_fields_t;

static bool tr_wire_v3_read_fields(const uint8_t *payload,
                                   size_t payload_length,
                                   tr_wire_v3_fields_t *fields)
{
    RaftWireMessageV3_view_t view;

    if (fields == NULL ||
        !RaftWireMessageV3_view_bind(&view, payload, payload_length)) {
        return false;
    }
    memset(fields, 0, sizeof(*fields));
    fields->message_type = RaftWireMessageV3_message_type_get(&view);
    fields->granted = RaftWireMessageV3_granted_get(&view);
    fields->reserved = RaftWireMessageV3_reserved_get(&view);
    fields->entry_count = RaftWireMessageV3_entry_count_get(&view);
    fields->from_node = RaftWireMessageV3_from_node_get(&view);
    fields->to_node = RaftWireMessageV3_to_node_get(&view);
    fields->term = RaftWireMessageV3_term_get(&view);
    fields->campaign_term = RaftWireMessageV3_campaign_term_get(&view);
    fields->last_log_index = RaftWireMessageV3_last_log_index_get(&view);
    fields->last_log_term = RaftWireMessageV3_last_log_term_get(&view);
    fields->leader_commit = RaftWireMessageV3_leader_commit_get(&view);
    fields->previous_log_index =
        RaftWireMessageV3_previous_log_index_get(&view);
    fields->previous_log_term =
        RaftWireMessageV3_previous_log_term_get(&view);
    fields->match_index = RaftWireMessageV3_match_index_get(&view);
    fields->reject_hint = RaftWireMessageV3_reject_hint_get(&view);

    fields->entry_indices[0] = RaftWireMessageV3_entry1_index_get(&view);
    fields->entry_indices[1] = RaftWireMessageV3_entry2_index_get(&view);
    fields->entry_indices[2] = RaftWireMessageV3_entry3_index_get(&view);
    fields->entry_indices[3] = RaftWireMessageV3_entry4_index_get(&view);
    fields->entry_indices[4] = RaftWireMessageV3_entry5_index_get(&view);
    fields->entry_indices[5] = RaftWireMessageV3_entry6_index_get(&view);
    fields->entry_indices[6] = RaftWireMessageV3_entry7_index_get(&view);
    fields->entry_indices[7] = RaftWireMessageV3_entry8_index_get(&view);
    fields->entry_terms[0] = RaftWireMessageV3_entry1_term_get(&view);
    fields->entry_terms[1] = RaftWireMessageV3_entry2_term_get(&view);
    fields->entry_terms[2] = RaftWireMessageV3_entry3_term_get(&view);
    fields->entry_terms[3] = RaftWireMessageV3_entry4_term_get(&view);
    fields->entry_terms[4] = RaftWireMessageV3_entry5_term_get(&view);
    fields->entry_terms[5] = RaftWireMessageV3_entry6_term_get(&view);
    fields->entry_terms[6] = RaftWireMessageV3_entry7_term_get(&view);
    fields->entry_terms[7] = RaftWireMessageV3_entry8_term_get(&view);
    fields->entry_commands[0] =
        RaftWireMessageV3_entry1_command_id_get(&view);
    fields->entry_commands[1] =
        RaftWireMessageV3_entry2_command_id_get(&view);
    fields->entry_commands[2] =
        RaftWireMessageV3_entry3_command_id_get(&view);
    fields->entry_commands[3] =
        RaftWireMessageV3_entry4_command_id_get(&view);
    fields->entry_commands[4] =
        RaftWireMessageV3_entry5_command_id_get(&view);
    fields->entry_commands[5] =
        RaftWireMessageV3_entry6_command_id_get(&view);
    fields->entry_commands[6] =
        RaftWireMessageV3_entry7_command_id_get(&view);
    fields->entry_commands[7] =
        RaftWireMessageV3_entry8_command_id_get(&view);

    if (!RaftWireMessageV3_entry1_data(&view, &fields->entry_data[0]) ||
        !RaftWireMessageV3_entry2_data(&view, &fields->entry_data[1]) ||
        !RaftWireMessageV3_entry3_data(&view, &fields->entry_data[2]) ||
        !RaftWireMessageV3_entry4_data(&view, &fields->entry_data[3]) ||
        !RaftWireMessageV3_entry5_data(&view, &fields->entry_data[4]) ||
        !RaftWireMessageV3_entry6_data(&view, &fields->entry_data[5]) ||
        !RaftWireMessageV3_entry7_data(&view, &fields->entry_data[6]) ||
        !RaftWireMessageV3_entry8_data(&view, &fields->entry_data[7])) {
        return false;
    }
    return tbe_wire_var_data_end(&fields->entry_data[7]) ==
           payload + payload_length;
}

static bool tr_wire_v3_fields_valid(const tr_wire_v3_fields_t *fields)
{
    bool read_message;
    size_t index;

    if (fields->reserved != 0U || fields->granted > 1U ||
        fields->message_type > TR_RAFT_MSG_READ_INDEX_RESPONSE ||
        fields->from_node == 0U || fields->to_node == 0U ||
        fields->entry_count > TR_RAFT_MAX_APPEND_ENTRIES) {
        return false;
    }
    read_message =
        fields->message_type == TR_RAFT_MSG_READ_INDEX_REQUEST ||
        fields->message_type == TR_RAFT_MSG_READ_INDEX_RESPONSE;
    if (read_message &&
        (fields->campaign_term == 0U || fields->term == 0U ||
         fields->entry_count != 0U)) {
        return false;
    }
    if (fields->entry_count != 0U &&
        fields->message_type != TR_RAFT_MSG_APPEND_REQUEST) {
        return false;
    }
    for (index = 0U; index < TR_RAFT_MAX_APPEND_ENTRIES; ++index) {
        bool active = index < fields->entry_count;

        if (fields->entry_data[index].size > TR_RAFT_MAX_ENTRY_BYTES ||
            (active &&
             (fields->entry_indices[index] == 0U ||
              fields->entry_terms[index] == 0U ||
              fields->entry_indices[index] !=
                  fields->previous_log_index + index + 1U)) ||
            (!active &&
             (fields->entry_indices[index] != 0U ||
              fields->entry_terms[index] != 0U ||
              fields->entry_commands[index] != 0U ||
              fields->entry_data[index].size != 0U))) {
            return false;
        }
        if (active && fields->entry_commands[index] == 0U) {
            tr_raft_conf_t configuration;

            if (tr_raft_conf_decode(fields->entry_data[index].data,
                                    fields->entry_data[index].size,
                                    &configuration) != SALTS_OK) {
                return false;
            }
        }
    }
    return true;
}

static int tr_raft_wire_decode_v3(const uint8_t *payload,
                                  size_t payload_length,
                                  tr_raft_message_t *message)
{
    tr_wire_v3_fields_t fields;
    size_t index;

    if (!tr_wire_v3_read_fields(payload, payload_length, &fields) ||
        !tr_wire_v3_fields_valid(&fields)) {
        return SALTS_EPROTO;
    }

    memset(message, 0, sizeof(*message));
    message->type = (tr_raft_message_type_t) fields.message_type;
    message->granted = fields.granted != 0U;
    message->entry_count = fields.entry_count;
    message->from = fields.from_node;
    message->to = fields.to_node;
    message->term = fields.term;
    if (message->type == TR_RAFT_MSG_READ_INDEX_REQUEST ||
        message->type == TR_RAFT_MSG_READ_INDEX_RESPONSE) {
        message->context_id = fields.campaign_term;
    } else {
        message->campaign_term = fields.campaign_term;
    }
    message->last_log_index = fields.last_log_index;
    message->last_log_term = fields.last_log_term;
    message->leader_commit = fields.leader_commit;
    message->previous_log_index = fields.previous_log_index;
    message->previous_log_term = fields.previous_log_term;
    message->match_index = fields.match_index;
    message->reject_hint = fields.reject_hint;
    for (index = 0U; index < message->entry_count; ++index) {
        tr_raft_entry_t *entry = &message->entries[index];

        entry->index = fields.entry_indices[index];
        entry->term = fields.entry_terms[index];
        entry->command_id = fields.entry_commands[index];
        entry->data_length = fields.entry_data[index].size;
        if (entry->data_length != 0U) {
            memcpy(entry->data, fields.entry_data[index].data,
                   entry->data_length);
        }
    }
    return SALTS_OK;
}

int tr_raft_wire_decode(tr_raft_wire_codec_t *codec,
                        const uint8_t *frame,
                        size_t frame_length,
                        tr_raft_wire_metadata_t *metadata,
                        tr_raft_message_t *message)
{
    uint32_t payload_length;

    if (codec == NULL || codec->binding == NULL || frame == NULL ||
        metadata == NULL || message == NULL ||
        frame_length < TR_RAFT_WIRE_HEADER_SIZE) {
        return SALTS_EINVAL;
    }
    if (tr_wire_read_envelope(frame, frame_length,
                              TR_RAFT_WIRE_PAYLOAD_RAFT, metadata,
                              &payload_length) != SALTS_OK) {
        return SALTS_EPROTO;
    }
    return tr_raft_wire_decode_v3(
        frame + TR_RAFT_WIRE_HEADER_SIZE, payload_length, message);
}

int tr_raft_wire_encode_snapshot_chunk(
    tr_raft_wire_codec_t *codec,
    const tr_raft_wire_metadata_t *metadata,
    const tr_raft_snapshot_chunk_t *chunk,
    uint8_t *output,
    size_t output_capacity,
    size_t *output_length)
{
    InstallSnapshotChunk_builder_t builder;
    InstallSnapshotChunk_view_t view;
    tbe_var_data_t encoded_data;
    size_t payload_length = 0U;
    uint8_t encoded_configuration[TR_RAFT_CONF_MAX_ENCODED_SIZE];
    size_t encoded_configuration_size = 0U;
    int result;

    if (output_length != NULL) {
        *output_length = 0U;
    }
    if (codec == NULL || codec->binding == NULL ||
        !tr_wire_metadata_valid(metadata) ||
        !tr_snapshot_chunk_valid(chunk) || output == NULL ||
        output_length == NULL) {
        return SALTS_EINVAL;
    }
    if (output_capacity < TR_RAFT_WIRE_HEADER_SIZE) {
        *output_length = TR_RAFT_WIRE_HEADER_SIZE;
        return SALTS_ENOSPC;
    }

    result = chunk->has_configuration
                 ? tr_raft_conf_encode(
                       &chunk->configuration, encoded_configuration,
                       sizeof(encoded_configuration),
                       &encoded_configuration_size)
                 : SALTS_OK;
    if (result != SALTS_OK) {
        return result;
    }

    if (!InstallSnapshotChunk_builder_bind(
            &builder, output + TR_RAFT_WIRE_HEADER_SIZE,
            output_capacity - TR_RAFT_WIRE_HEADER_SIZE) ||
        !InstallSnapshotChunk_from_node_set(&builder, chunk->from) ||
        !InstallSnapshotChunk_to_node_set(&builder, chunk->to) ||
        !InstallSnapshotChunk_term_set(&builder, chunk->term) ||
        !InstallSnapshotChunk_snapshot_index_set(&builder,
                                                 chunk->snapshot_index) ||
        !InstallSnapshotChunk_snapshot_term_set(&builder,
                                                chunk->snapshot_term) ||
        !InstallSnapshotChunk_snapshot_offset_set(&builder,
                                                  chunk->snapshot_offset) ||
        !InstallSnapshotChunk_snapshot_size_set(&builder,
                                                chunk->snapshot_size) ||
        !InstallSnapshotChunk_done_set(&builder, chunk->done ? 1U : 0U) ||
        !InstallSnapshotChunk_snapshot_configuration_set(
            &builder, encoded_configuration, encoded_configuration_size) ||
        !InstallSnapshotChunk_snapshot_digest_set(
            &builder, chunk->snapshot_digest,
            TR_RAFT_WIRE_SNAPSHOT_DIGEST_SIZE) ||
        !InstallSnapshotChunk_chunk_data_set(
            &builder, chunk->data, chunk->data_length)) {
        *output_length = TR_RAFT_WIRE_HEADER_SIZE +
                         InstallSnapshotChunk_BLOCK_LENGTH + 12U +
                         encoded_configuration_size +
                         TR_RAFT_WIRE_SNAPSHOT_DIGEST_SIZE +
                         chunk->data_length;
        return SALTS_ENOSPC;
    }
    if (!InstallSnapshotChunk_view_bind(
            &view, output + TR_RAFT_WIRE_HEADER_SIZE,
            output_capacity - TR_RAFT_WIRE_HEADER_SIZE) ||
        !InstallSnapshotChunk_chunk_data(&view, &encoded_data)) {
        return SALTS_EPROTO;
    }

    payload_length = (size_t)(tbe_wire_var_data_end(&encoded_data) -
                              (output + TR_RAFT_WIRE_HEADER_SIZE));
    if (payload_length >
        tr_wire_payload_limit(TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_CHUNK)) {
        return SALTS_EPROTO;
    }
    *output_length = TR_RAFT_WIRE_HEADER_SIZE + payload_length;
    tr_wire_write_envelope(output, TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_CHUNK,
                           payload_length, metadata);
    return SALTS_OK;
}

int tr_raft_wire_decode_snapshot_chunk(
    tr_raft_wire_codec_t *codec,
    const uint8_t *frame,
    size_t frame_length,
    tr_raft_wire_metadata_t *metadata,
    tr_raft_snapshot_chunk_t *chunk)
{
    InstallSnapshotChunk_view_t wire;
    tbe_var_data_t digest;
    tbe_var_data_t configuration;
    tbe_var_data_t data;
    uint32_t payload_length;

    if (codec == NULL || codec->binding == NULL || frame == NULL ||
        metadata == NULL || chunk == NULL ||
        frame_length < TR_RAFT_WIRE_HEADER_SIZE) {
        return SALTS_EINVAL;
    }
    if (tr_wire_read_envelope(
            frame, frame_length, TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_CHUNK,
            metadata, &payload_length) != SALTS_OK) {
        return SALTS_EPROTO;
    }
    if (!InstallSnapshotChunk_view_bind(
            &wire, frame + TR_RAFT_WIRE_HEADER_SIZE, payload_length) ||
        !InstallSnapshotChunk_snapshot_configuration(&wire, &configuration) ||
        !InstallSnapshotChunk_snapshot_digest(&wire, &digest) ||
        !InstallSnapshotChunk_chunk_data(&wire, &data) ||
        tbe_wire_var_data_end(&data) != frame + frame_length) {
        return SALTS_EPROTO;
    }
    if (InstallSnapshotChunk_done_get(&wire) > 1U ||
        digest.size != TR_RAFT_WIRE_SNAPSHOT_DIGEST_SIZE ||
        configuration.size > TR_RAFT_CONF_MAX_ENCODED_SIZE ||
        data.size > TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES ||
        InstallSnapshotChunk_snapshot_offset_get(&wire) >
            InstallSnapshotChunk_snapshot_size_get(&wire)) {
        return SALTS_EPROTO;
    }

    memset(chunk, 0, sizeof(*chunk));
    chunk->from = InstallSnapshotChunk_from_node_get(&wire);
    chunk->to = InstallSnapshotChunk_to_node_get(&wire);
    chunk->term = InstallSnapshotChunk_term_get(&wire);
    chunk->snapshot_index = InstallSnapshotChunk_snapshot_index_get(&wire);
    chunk->snapshot_term = InstallSnapshotChunk_snapshot_term_get(&wire);
    chunk->snapshot_offset = InstallSnapshotChunk_snapshot_offset_get(&wire);
    chunk->snapshot_size = InstallSnapshotChunk_snapshot_size_get(&wire);
    chunk->done = InstallSnapshotChunk_done_get(&wire) != 0U;
    chunk->data_length = data.size;
    chunk->data = data.data;
    if (configuration.size != 0U) {
        if (tr_raft_conf_decode(configuration.data, configuration.size,
                                &chunk->configuration) != SALTS_OK) {
            return SALTS_EPROTO;
        }
        chunk->has_configuration = true;
    }
    memcpy(chunk->snapshot_digest, digest.data, digest.size);
    return tr_snapshot_chunk_valid(chunk) ? SALTS_OK : SALTS_EPROTO;
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
    if (codec == NULL || codec->binding == NULL ||
        !tr_wire_metadata_valid(metadata) ||
        !tr_snapshot_ack_valid(ack) || output == NULL ||
        output_length == NULL) {
        return SALTS_EINVAL;
    }
    if (output_capacity < TR_RAFT_WIRE_HEADER_SIZE) {
        *output_length = TR_RAFT_WIRE_HEADER_SIZE;
        return SALTS_ENOSPC;
    }

    InstallSnapshotAck_init(&wire);
    wire.from_node = ack->from;
    wire.to_node = ack->to;
    wire.term = ack->term;
    wire.snapshot_index = ack->snapshot_index;
    wire.snapshot_size = ack->snapshot_size;
    wire.next_offset = ack->next_offset;
    wire.accepted = ack->accepted ? 1U : 0U;
    result = tr_raft_stl_status_to_error(vec_resize(
        &wire.snapshot_digest.raw, TR_RAFT_WIRE_SNAPSHOT_DIGEST_SIZE));
    if (result == SALTS_OK) {
        memcpy(tbe_bytes_t_data(&wire.snapshot_digest), ack->snapshot_digest,
               TR_RAFT_WIRE_SNAPSHOT_DIGEST_SIZE);
    }
    if (result != SALTS_OK) {
        InstallSnapshotAck_clear(&wire);
        return result;
    }

    status = InstallSnapshotAck_to_bin_into(
        &wire, output + TR_RAFT_WIRE_HEADER_SIZE,
        output_capacity - TR_RAFT_WIRE_HEADER_SIZE,
        &payload_length, &error);
    InstallSnapshotAck_clear(&wire);
    *output_length = TR_RAFT_WIRE_HEADER_SIZE + payload_length;
    if (status != DATA_BIND_OK) {
        return *output_length > output_capacity ? SALTS_ENOSPC : SALTS_EPROTO;
    }
    if (payload_length >
        tr_wire_payload_limit(TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_ACK)) {
        return SALTS_EPROTO;
    }

    tr_wire_write_envelope(output, TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_ACK,
                           payload_length, metadata);
    return SALTS_OK;
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
    size_t digest_length;
    DataBindStatus status;

    if (codec == NULL || codec->binding == NULL || frame == NULL ||
        metadata == NULL || ack == NULL ||
        frame_length < TR_RAFT_WIRE_HEADER_SIZE) {
        return SALTS_EINVAL;
    }
    if (tr_wire_read_envelope(
            frame, frame_length, TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_ACK,
            metadata, &payload_length) != SALTS_OK) {
        return SALTS_EPROTO;
    }

    InstallSnapshotAck_init(&wire);
    status = InstallSnapshotAck_from_bin(
        codec->binding, &wire, frame + TR_RAFT_WIRE_HEADER_SIZE,
        payload_length, &error);
    if (status != DATA_BIND_OK) {
        InstallSnapshotAck_clear(&wire);
        return SALTS_EPROTO;
    }
    digest_length = tbe_bytes_t_size(&wire.snapshot_digest);
    if (wire.accepted > 1U ||
        digest_length != TR_RAFT_WIRE_SNAPSHOT_DIGEST_SIZE) {
        InstallSnapshotAck_clear(&wire);
        return SALTS_EPROTO;
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
    return tr_snapshot_ack_valid(ack) ? SALTS_OK : SALTS_EPROTO;
}

#define TR_DATA_CHUNK_FIXED_SIZE 85U
#define TR_DATA_ACK_FIXED_SIZE 82U

int tr_raft_wire_encode_data_chunk(
    tr_raft_wire_codec_t *codec,
    const tr_raft_wire_metadata_t *metadata,
    const tr_raft_data_chunk_t *chunk,
    uint8_t *output,
    size_t output_capacity,
    size_t *output_length)
{
    size_t payload_length;
    uint8_t *payload;

    if (output_length != NULL) {
        *output_length = 0U;
    }
    if (codec == NULL || codec->binding == NULL ||
        !tr_wire_metadata_valid(metadata) ||
        !tr_data_chunk_valid(chunk) || output == NULL ||
        output_length == NULL) {
        return SALTS_EINVAL;
    }

    payload_length = TR_DATA_CHUNK_FIXED_SIZE + chunk->data_length;
    if (output_capacity < TR_RAFT_WIRE_HEADER_SIZE + payload_length) {
        *output_length = TR_RAFT_WIRE_HEADER_SIZE + payload_length;
        return SALTS_ENOSPC;
    }

    payload = output + TR_RAFT_WIRE_HEADER_SIZE;
    tr_put_u64(payload, chunk->from);
    tr_put_u64(payload + 8U, chunk->to);
    tr_put_u64(payload + 16U, chunk->term);
    tr_put_u64(payload + 24U, chunk->stream_id);
    tr_put_u64(payload + 32U, chunk->stream_offset);
    tr_put_u64(payload + 40U, chunk->stream_size);
    memcpy(payload + 48U, chunk->stream_digest,
           TR_RAFT_WIRE_DATA_DIGEST_SIZE);
    payload[80U] = chunk->done ? 1U : 0U;
    tr_put_u32(payload + 81U, (uint32_t)chunk->data_length);
    if (chunk->data_length != 0U) {
        memcpy(payload + TR_DATA_CHUNK_FIXED_SIZE, chunk->data,
               chunk->data_length);
    }

    *output_length = TR_RAFT_WIRE_HEADER_SIZE + payload_length;
    tr_wire_write_envelope(output, TR_RAFT_WIRE_PAYLOAD_DATA_CHUNK,
                           payload_length, metadata);
    return SALTS_OK;
}

int tr_raft_wire_decode_data_chunk(
    tr_raft_wire_codec_t *codec,
    const uint8_t *frame,
    size_t frame_length,
    tr_raft_wire_metadata_t *metadata,
    tr_raft_data_chunk_t *chunk)
{
    const uint8_t *payload;
    uint32_t payload_length;
    uint32_t data_length;

    if (codec == NULL || codec->binding == NULL || frame == NULL ||
        metadata == NULL || chunk == NULL ||
        frame_length < TR_RAFT_WIRE_HEADER_SIZE) {
        return SALTS_EINVAL;
    }
    if (tr_wire_read_envelope(
            frame, frame_length, TR_RAFT_WIRE_PAYLOAD_DATA_CHUNK,
            metadata, &payload_length) != SALTS_OK ||
        payload_length < TR_DATA_CHUNK_FIXED_SIZE) {
        return SALTS_EPROTO;
    }

    payload = frame + TR_RAFT_WIRE_HEADER_SIZE;
    data_length = tr_get_u32(payload + 81U);
    if ((size_t)data_length != payload_length - TR_DATA_CHUNK_FIXED_SIZE) {
        return SALTS_EPROTO;
    }

    memset(chunk, 0, sizeof(*chunk));
    chunk->from = tr_get_u64(payload);
    chunk->to = tr_get_u64(payload + 8U);
    chunk->term = tr_get_u64(payload + 16U);
    chunk->stream_id = tr_get_u64(payload + 24U);
    chunk->stream_offset = tr_get_u64(payload + 32U);
    chunk->stream_size = tr_get_u64(payload + 40U);
    memcpy(chunk->stream_digest, payload + 48U,
           TR_RAFT_WIRE_DATA_DIGEST_SIZE);
    if (payload[80U] > 1U) {
        return SALTS_EPROTO;
    }
    chunk->done = payload[80U] != 0U;
    chunk->data_length = data_length;
    chunk->data = data_length == 0U
                      ? NULL
                      : payload + TR_DATA_CHUNK_FIXED_SIZE;
    return tr_data_chunk_valid(chunk) ? SALTS_OK : SALTS_EPROTO;
}

int tr_raft_wire_encode_data_ack(
    tr_raft_wire_codec_t *codec,
    const tr_raft_wire_metadata_t *metadata,
    const tr_raft_data_ack_t *ack,
    uint8_t *output,
    size_t output_capacity,
    size_t *output_length)
{
    uint8_t *payload;

    if (output_length != NULL) {
        *output_length = 0U;
    }
    if (codec == NULL || codec->binding == NULL ||
        !tr_wire_metadata_valid(metadata) ||
        !tr_data_ack_valid(ack) || output == NULL || output_length == NULL) {
        return SALTS_EINVAL;
    }
    if (output_capacity <
        TR_RAFT_WIRE_HEADER_SIZE + TR_DATA_ACK_FIXED_SIZE) {
        *output_length = TR_RAFT_WIRE_HEADER_SIZE + TR_DATA_ACK_FIXED_SIZE;
        return SALTS_ENOSPC;
    }

    payload = output + TR_RAFT_WIRE_HEADER_SIZE;
    tr_put_u64(payload, ack->from);
    tr_put_u64(payload + 8U, ack->to);
    tr_put_u64(payload + 16U, ack->term);
    tr_put_u64(payload + 24U, ack->stream_id);
    tr_put_u64(payload + 32U, ack->stream_size);
    tr_put_u64(payload + 40U, ack->next_offset);
    memcpy(payload + 48U, ack->stream_digest,
           TR_RAFT_WIRE_DATA_DIGEST_SIZE);
    payload[80U] = ack->accepted ? 1U : 0U;
    payload[81U] = ack->durable ? 1U : 0U;

    *output_length = TR_RAFT_WIRE_HEADER_SIZE + TR_DATA_ACK_FIXED_SIZE;
    tr_wire_write_envelope(output, TR_RAFT_WIRE_PAYLOAD_DATA_ACK,
                           TR_DATA_ACK_FIXED_SIZE, metadata);
    return SALTS_OK;
}

int tr_raft_wire_decode_data_ack(
    tr_raft_wire_codec_t *codec,
    const uint8_t *frame,
    size_t frame_length,
    tr_raft_wire_metadata_t *metadata,
    tr_raft_data_ack_t *ack)
{
    const uint8_t *payload;
    uint32_t payload_length;

    if (codec == NULL || codec->binding == NULL || frame == NULL ||
        metadata == NULL || ack == NULL ||
        frame_length < TR_RAFT_WIRE_HEADER_SIZE) {
        return SALTS_EINVAL;
    }
    if (tr_wire_read_envelope(
            frame, frame_length, TR_RAFT_WIRE_PAYLOAD_DATA_ACK,
            metadata, &payload_length) != SALTS_OK ||
        payload_length != TR_DATA_ACK_FIXED_SIZE) {
        return SALTS_EPROTO;
    }

    payload = frame + TR_RAFT_WIRE_HEADER_SIZE;
    if (payload[80U] > 1U || payload[81U] > 1U) {
        return SALTS_EPROTO;
    }

    memset(ack, 0, sizeof(*ack));
    ack->from = tr_get_u64(payload);
    ack->to = tr_get_u64(payload + 8U);
    ack->term = tr_get_u64(payload + 16U);
    ack->stream_id = tr_get_u64(payload + 24U);
    ack->stream_size = tr_get_u64(payload + 32U);
    ack->next_offset = tr_get_u64(payload + 40U);
    memcpy(ack->stream_digest, payload + 48U,
           TR_RAFT_WIRE_DATA_DIGEST_SIZE);
    ack->accepted = payload[80U] != 0U;
    ack->durable = payload[81U] != 0U;
    return tr_data_ack_valid(ack) ? SALTS_OK : SALTS_EPROTO;
}

int tr_raft_wire_peek_version(
    const uint8_t *frame,
    size_t frame_length,
    uint16_t *out_version)
{
    uint32_t payload_length;
    uint32_t payload_kind;

    if (frame == NULL || out_version == NULL ||
        frame_length < TR_RAFT_WIRE_HEADER_SIZE) {
        return SALTS_EINVAL;
    }
    if (memcmp(frame, TR_RAFT_WIRE_MAGIC, sizeof(TR_RAFT_WIRE_MAGIC)) != 0 ||
        tr_get_u16(frame + 4U) != TR_RAFT_WIRE_VERSION ||
        tr_get_u16(frame + 6U) != TR_RAFT_WIRE_HEADER_SIZE) {
        return SALTS_EPROTO;
    }
    payload_length = tr_get_u32(frame + 8U);
    payload_kind = tr_get_u32(frame + 12U);
    if (payload_length > tr_wire_payload_limit(payload_kind) ||
        frame_length != TR_RAFT_WIRE_HEADER_SIZE + payload_length) {
        return SALTS_EPROTO;
    }
    *out_version = TR_RAFT_WIRE_VERSION;
    return SALTS_OK;
}

int tr_raft_wire_peek_payload_kind(
    const uint8_t *frame,
    size_t frame_length,
    tr_raft_wire_payload_kind_t *out_kind)
{
    uint32_t payload_kind;
    uint16_t wire_version;

    if (out_kind == NULL) {
        return SALTS_EINVAL;
    }
    if (tr_raft_wire_peek_version(frame, frame_length, &wire_version) !=
        SALTS_OK) {
        return frame == NULL || frame_length < TR_RAFT_WIRE_HEADER_SIZE
                   ? SALTS_EINVAL
                   : SALTS_EPROTO;
    }
    payload_kind = tr_get_u32(frame + 12U);
    if (payload_kind < TR_RAFT_WIRE_PAYLOAD_RAFT ||
        payload_kind > TR_RAFT_WIRE_PAYLOAD_DATA_ACK) {
        return SALTS_EPROTO;
    }
    *out_kind = (tr_raft_wire_payload_kind_t) payload_kind;
    return SALTS_OK;
}
