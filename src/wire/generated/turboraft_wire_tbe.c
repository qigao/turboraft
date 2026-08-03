#include "turboraft_wire_tbe.h"

#include <stddef.h>
#include <stdio.h>
#include <string.h>

static const char TurboRaftWire_SCHEMA_TEXT[] = "schema TurboRaftWire [id(8101), version(2), byte_order(little)];\n\nmessage RaftWireMessage {\n    uint8 message_type;\n    uint8 granted;\n    uint16 reserved;\n    uint32 entry_count;\n    uint64 from_node;\n    uint64 to_node;\n    uint64 term;\n    uint64 campaign_term;\n    uint64 last_log_index;\n    uint64 last_log_term;\n    uint64 leader_commit;\n    uint64 previous_log_index;\n    uint64 previous_log_term;\n    uint64 match_index;\n    uint64 reject_hint;\n    uint64 entry_index;\n    uint64 entry_term;\n    uint64 entry_command_id;\n    bytes entry_data;\n}\n\nmessage InstallSnapshotChunk {\n    uint64 from_node;\n    uint64 to_node;\n    uint64 term;\n    uint64 snapshot_index;\n    uint64 snapshot_term;\n    uint64 snapshot_offset;\n    uint64 snapshot_size;\n    uint8 done;\n    bytes snapshot_configuration;\n    bytes snapshot_digest;\n    bytes chunk_data;\n}\n\nmessage InstallSnapshotAck {\n    uint64 from_node;\n    uint64 to_node;\n    uint64 term;\n    uint64 snapshot_index;\n    uint64 snapshot_size;\n    uint64 next_offset;\n    uint8 accepted;\n    bytes snapshot_digest;\n}\n";

const char *TurboRaftWire_schema_text(void) {
    return TurboRaftWire_SCHEMA_TEXT;
}

DataBindStatus TurboRaftWire_codec_create(DataBind **out_codec, DataBindError *error) {
    return data_bind_create_from_text(TurboRaftWire_SCHEMA_TEXT,
                                      sizeof(TurboRaftWire_SCHEMA_TEXT) - 1,
                                      out_codec, error);
}

static TbeTypedType RaftWireMessage_TYPED_TYPE;
static TbeTypedType InstallSnapshotChunk_TYPED_TYPE;
static TbeTypedType InstallSnapshotAck_TYPED_TYPE;



static const TbeTypedField RaftWireMessage_TYPED_FIELDS[] = {
    { .name = "message_type", .kind = TBE_TYPED_U8,
      .wire_kind = TBE_TYPED_U8, .offset = offsetof(RaftWireMessage_t, message_type),
      .element_kind = TBE_TYPED_BOOL,
      .element_wire_kind = TBE_TYPED_BOOL,
      .element_size = 0,
      .fixed_count = 0,
      .object_type = NULL,
      .map_entry_size = 0,
      .map_key_offset = 0,
      .map_value_offset = 0,
      .map_value_kind = TBE_TYPED_BOOL,
      .map_value_wire_kind = TBE_TYPED_BOOL,
      .map_value_type = NULL,
      .wire_offset = 0,
      .wire_size = 1,
      .optional_bit = 0,
      .flags = 0 | TBE_TYPED_FIELD_WIRE_OFFSET },
    { .name = "granted", .kind = TBE_TYPED_U8,
      .wire_kind = TBE_TYPED_U8, .offset = offsetof(RaftWireMessage_t, granted),
      .element_kind = TBE_TYPED_BOOL,
      .element_wire_kind = TBE_TYPED_BOOL,
      .element_size = 0,
      .fixed_count = 0,
      .object_type = NULL,
      .map_entry_size = 0,
      .map_key_offset = 0,
      .map_value_offset = 0,
      .map_value_kind = TBE_TYPED_BOOL,
      .map_value_wire_kind = TBE_TYPED_BOOL,
      .map_value_type = NULL,
      .wire_offset = 1,
      .wire_size = 1,
      .optional_bit = 0,
      .flags = 0 | TBE_TYPED_FIELD_WIRE_OFFSET },
    { .name = "reserved", .kind = TBE_TYPED_U16,
      .wire_kind = TBE_TYPED_U16, .offset = offsetof(RaftWireMessage_t, reserved),
      .element_kind = TBE_TYPED_BOOL,
      .element_wire_kind = TBE_TYPED_BOOL,
      .element_size = 0,
      .fixed_count = 0,
      .object_type = NULL,
      .map_entry_size = 0,
      .map_key_offset = 0,
      .map_value_offset = 0,
      .map_value_kind = TBE_TYPED_BOOL,
      .map_value_wire_kind = TBE_TYPED_BOOL,
      .map_value_type = NULL,
      .wire_offset = 2,
      .wire_size = 2,
      .optional_bit = 0,
      .flags = 0 | TBE_TYPED_FIELD_WIRE_OFFSET },
    { .name = "entry_count", .kind = TBE_TYPED_U32,
      .wire_kind = TBE_TYPED_U32, .offset = offsetof(RaftWireMessage_t, entry_count),
      .element_kind = TBE_TYPED_BOOL,
      .element_wire_kind = TBE_TYPED_BOOL,
      .element_size = 0,
      .fixed_count = 0,
      .object_type = NULL,
      .map_entry_size = 0,
      .map_key_offset = 0,
      .map_value_offset = 0,
      .map_value_kind = TBE_TYPED_BOOL,
      .map_value_wire_kind = TBE_TYPED_BOOL,
      .map_value_type = NULL,
      .wire_offset = 4,
      .wire_size = 4,
      .optional_bit = 0,
      .flags = 0 | TBE_TYPED_FIELD_WIRE_OFFSET },
    { .name = "from_node", .kind = TBE_TYPED_U64,
      .wire_kind = TBE_TYPED_U64, .offset = offsetof(RaftWireMessage_t, from_node),
      .element_kind = TBE_TYPED_BOOL,
      .element_wire_kind = TBE_TYPED_BOOL,
      .element_size = 0,
      .fixed_count = 0,
      .object_type = NULL,
      .map_entry_size = 0,
      .map_key_offset = 0,
      .map_value_offset = 0,
      .map_value_kind = TBE_TYPED_BOOL,
      .map_value_wire_kind = TBE_TYPED_BOOL,
      .map_value_type = NULL,
      .wire_offset = 8,
      .wire_size = 8,
      .optional_bit = 0,
      .flags = 0 | TBE_TYPED_FIELD_WIRE_OFFSET },
    { .name = "to_node", .kind = TBE_TYPED_U64,
      .wire_kind = TBE_TYPED_U64, .offset = offsetof(RaftWireMessage_t, to_node),
      .element_kind = TBE_TYPED_BOOL,
      .element_wire_kind = TBE_TYPED_BOOL,
      .element_size = 0,
      .fixed_count = 0,
      .object_type = NULL,
      .map_entry_size = 0,
      .map_key_offset = 0,
      .map_value_offset = 0,
      .map_value_kind = TBE_TYPED_BOOL,
      .map_value_wire_kind = TBE_TYPED_BOOL,
      .map_value_type = NULL,
      .wire_offset = 16,
      .wire_size = 8,
      .optional_bit = 0,
      .flags = 0 | TBE_TYPED_FIELD_WIRE_OFFSET },
    { .name = "term", .kind = TBE_TYPED_U64,
      .wire_kind = TBE_TYPED_U64, .offset = offsetof(RaftWireMessage_t, term),
      .element_kind = TBE_TYPED_BOOL,
      .element_wire_kind = TBE_TYPED_BOOL,
      .element_size = 0,
      .fixed_count = 0,
      .object_type = NULL,
      .map_entry_size = 0,
      .map_key_offset = 0,
      .map_value_offset = 0,
      .map_value_kind = TBE_TYPED_BOOL,
      .map_value_wire_kind = TBE_TYPED_BOOL,
      .map_value_type = NULL,
      .wire_offset = 24,
      .wire_size = 8,
      .optional_bit = 0,
      .flags = 0 | TBE_TYPED_FIELD_WIRE_OFFSET },
    { .name = "campaign_term", .kind = TBE_TYPED_U64,
      .wire_kind = TBE_TYPED_U64, .offset = offsetof(RaftWireMessage_t, campaign_term),
      .element_kind = TBE_TYPED_BOOL,
      .element_wire_kind = TBE_TYPED_BOOL,
      .element_size = 0,
      .fixed_count = 0,
      .object_type = NULL,
      .map_entry_size = 0,
      .map_key_offset = 0,
      .map_value_offset = 0,
      .map_value_kind = TBE_TYPED_BOOL,
      .map_value_wire_kind = TBE_TYPED_BOOL,
      .map_value_type = NULL,
      .wire_offset = 32,
      .wire_size = 8,
      .optional_bit = 0,
      .flags = 0 | TBE_TYPED_FIELD_WIRE_OFFSET },
    { .name = "last_log_index", .kind = TBE_TYPED_U64,
      .wire_kind = TBE_TYPED_U64, .offset = offsetof(RaftWireMessage_t, last_log_index),
      .element_kind = TBE_TYPED_BOOL,
      .element_wire_kind = TBE_TYPED_BOOL,
      .element_size = 0,
      .fixed_count = 0,
      .object_type = NULL,
      .map_entry_size = 0,
      .map_key_offset = 0,
      .map_value_offset = 0,
      .map_value_kind = TBE_TYPED_BOOL,
      .map_value_wire_kind = TBE_TYPED_BOOL,
      .map_value_type = NULL,
      .wire_offset = 40,
      .wire_size = 8,
      .optional_bit = 0,
      .flags = 0 | TBE_TYPED_FIELD_WIRE_OFFSET },
    { .name = "last_log_term", .kind = TBE_TYPED_U64,
      .wire_kind = TBE_TYPED_U64, .offset = offsetof(RaftWireMessage_t, last_log_term),
      .element_kind = TBE_TYPED_BOOL,
      .element_wire_kind = TBE_TYPED_BOOL,
      .element_size = 0,
      .fixed_count = 0,
      .object_type = NULL,
      .map_entry_size = 0,
      .map_key_offset = 0,
      .map_value_offset = 0,
      .map_value_kind = TBE_TYPED_BOOL,
      .map_value_wire_kind = TBE_TYPED_BOOL,
      .map_value_type = NULL,
      .wire_offset = 48,
      .wire_size = 8,
      .optional_bit = 0,
      .flags = 0 | TBE_TYPED_FIELD_WIRE_OFFSET },
    { .name = "leader_commit", .kind = TBE_TYPED_U64,
      .wire_kind = TBE_TYPED_U64, .offset = offsetof(RaftWireMessage_t, leader_commit),
      .element_kind = TBE_TYPED_BOOL,
      .element_wire_kind = TBE_TYPED_BOOL,
      .element_size = 0,
      .fixed_count = 0,
      .object_type = NULL,
      .map_entry_size = 0,
      .map_key_offset = 0,
      .map_value_offset = 0,
      .map_value_kind = TBE_TYPED_BOOL,
      .map_value_wire_kind = TBE_TYPED_BOOL,
      .map_value_type = NULL,
      .wire_offset = 56,
      .wire_size = 8,
      .optional_bit = 0,
      .flags = 0 | TBE_TYPED_FIELD_WIRE_OFFSET },
    { .name = "previous_log_index", .kind = TBE_TYPED_U64,
      .wire_kind = TBE_TYPED_U64, .offset = offsetof(RaftWireMessage_t, previous_log_index),
      .element_kind = TBE_TYPED_BOOL,
      .element_wire_kind = TBE_TYPED_BOOL,
      .element_size = 0,
      .fixed_count = 0,
      .object_type = NULL,
      .map_entry_size = 0,
      .map_key_offset = 0,
      .map_value_offset = 0,
      .map_value_kind = TBE_TYPED_BOOL,
      .map_value_wire_kind = TBE_TYPED_BOOL,
      .map_value_type = NULL,
      .wire_offset = 64,
      .wire_size = 8,
      .optional_bit = 0,
      .flags = 0 | TBE_TYPED_FIELD_WIRE_OFFSET },
    { .name = "previous_log_term", .kind = TBE_TYPED_U64,
      .wire_kind = TBE_TYPED_U64, .offset = offsetof(RaftWireMessage_t, previous_log_term),
      .element_kind = TBE_TYPED_BOOL,
      .element_wire_kind = TBE_TYPED_BOOL,
      .element_size = 0,
      .fixed_count = 0,
      .object_type = NULL,
      .map_entry_size = 0,
      .map_key_offset = 0,
      .map_value_offset = 0,
      .map_value_kind = TBE_TYPED_BOOL,
      .map_value_wire_kind = TBE_TYPED_BOOL,
      .map_value_type = NULL,
      .wire_offset = 72,
      .wire_size = 8,
      .optional_bit = 0,
      .flags = 0 | TBE_TYPED_FIELD_WIRE_OFFSET },
    { .name = "match_index", .kind = TBE_TYPED_U64,
      .wire_kind = TBE_TYPED_U64, .offset = offsetof(RaftWireMessage_t, match_index),
      .element_kind = TBE_TYPED_BOOL,
      .element_wire_kind = TBE_TYPED_BOOL,
      .element_size = 0,
      .fixed_count = 0,
      .object_type = NULL,
      .map_entry_size = 0,
      .map_key_offset = 0,
      .map_value_offset = 0,
      .map_value_kind = TBE_TYPED_BOOL,
      .map_value_wire_kind = TBE_TYPED_BOOL,
      .map_value_type = NULL,
      .wire_offset = 80,
      .wire_size = 8,
      .optional_bit = 0,
      .flags = 0 | TBE_TYPED_FIELD_WIRE_OFFSET },
    { .name = "reject_hint", .kind = TBE_TYPED_U64,
      .wire_kind = TBE_TYPED_U64, .offset = offsetof(RaftWireMessage_t, reject_hint),
      .element_kind = TBE_TYPED_BOOL,
      .element_wire_kind = TBE_TYPED_BOOL,
      .element_size = 0,
      .fixed_count = 0,
      .object_type = NULL,
      .map_entry_size = 0,
      .map_key_offset = 0,
      .map_value_offset = 0,
      .map_value_kind = TBE_TYPED_BOOL,
      .map_value_wire_kind = TBE_TYPED_BOOL,
      .map_value_type = NULL,
      .wire_offset = 88,
      .wire_size = 8,
      .optional_bit = 0,
      .flags = 0 | TBE_TYPED_FIELD_WIRE_OFFSET },
    { .name = "entry_index", .kind = TBE_TYPED_U64,
      .wire_kind = TBE_TYPED_U64, .offset = offsetof(RaftWireMessage_t, entry_index),
      .element_kind = TBE_TYPED_BOOL,
      .element_wire_kind = TBE_TYPED_BOOL,
      .element_size = 0,
      .fixed_count = 0,
      .object_type = NULL,
      .map_entry_size = 0,
      .map_key_offset = 0,
      .map_value_offset = 0,
      .map_value_kind = TBE_TYPED_BOOL,
      .map_value_wire_kind = TBE_TYPED_BOOL,
      .map_value_type = NULL,
      .wire_offset = 96,
      .wire_size = 8,
      .optional_bit = 0,
      .flags = 0 | TBE_TYPED_FIELD_WIRE_OFFSET },
    { .name = "entry_term", .kind = TBE_TYPED_U64,
      .wire_kind = TBE_TYPED_U64, .offset = offsetof(RaftWireMessage_t, entry_term),
      .element_kind = TBE_TYPED_BOOL,
      .element_wire_kind = TBE_TYPED_BOOL,
      .element_size = 0,
      .fixed_count = 0,
      .object_type = NULL,
      .map_entry_size = 0,
      .map_key_offset = 0,
      .map_value_offset = 0,
      .map_value_kind = TBE_TYPED_BOOL,
      .map_value_wire_kind = TBE_TYPED_BOOL,
      .map_value_type = NULL,
      .wire_offset = 104,
      .wire_size = 8,
      .optional_bit = 0,
      .flags = 0 | TBE_TYPED_FIELD_WIRE_OFFSET },
    { .name = "entry_command_id", .kind = TBE_TYPED_U64,
      .wire_kind = TBE_TYPED_U64, .offset = offsetof(RaftWireMessage_t, entry_command_id),
      .element_kind = TBE_TYPED_BOOL,
      .element_wire_kind = TBE_TYPED_BOOL,
      .element_size = 0,
      .fixed_count = 0,
      .object_type = NULL,
      .map_entry_size = 0,
      .map_key_offset = 0,
      .map_value_offset = 0,
      .map_value_kind = TBE_TYPED_BOOL,
      .map_value_wire_kind = TBE_TYPED_BOOL,
      .map_value_type = NULL,
      .wire_offset = 112,
      .wire_size = 8,
      .optional_bit = 0,
      .flags = 0 | TBE_TYPED_FIELD_WIRE_OFFSET },
    { .name = "entry_data", .kind = TBE_TYPED_BYTES,
      .wire_kind = TBE_TYPED_BYTES, .offset = offsetof(RaftWireMessage_t, entry_data),
      .element_kind = TBE_TYPED_BOOL,
      .element_wire_kind = TBE_TYPED_BOOL,
      .element_size = 0,
      .fixed_count = 0,
      .object_type = NULL,
      .map_entry_size = 0,
      .map_key_offset = 0,
      .map_value_offset = 0,
      .map_value_kind = TBE_TYPED_BOOL,
      .map_value_wire_kind = TBE_TYPED_BOOL,
      .map_value_type = NULL,
      .wire_offset = 0,
      .wire_size = 0,
      .optional_bit = 0,
      .flags = 0 | TBE_TYPED_FIELD_VAR_DATA },
};
static TbeTypedType RaftWireMessage_TYPED_TYPE = {
    .name = "RaftWireMessage", .size = sizeof(RaftWireMessage_t), .fields = RaftWireMessage_TYPED_FIELDS,
    .field_count = sizeof(RaftWireMessage_TYPED_FIELDS) / sizeof(RaftWireMessage_TYPED_FIELDS[0]),
    .fixed_block_size = 120,
    .presence_offset = 0,
    .presence_size = 0,
    .wire_big_endian = 0
};
static const TbeTypedField InstallSnapshotChunk_TYPED_FIELDS[] = {
    { .name = "from_node", .kind = TBE_TYPED_U64,
      .wire_kind = TBE_TYPED_U64, .offset = offsetof(InstallSnapshotChunk_t, from_node),
      .element_kind = TBE_TYPED_BOOL,
      .element_wire_kind = TBE_TYPED_BOOL,
      .element_size = 0,
      .fixed_count = 0,
      .object_type = NULL,
      .map_entry_size = 0,
      .map_key_offset = 0,
      .map_value_offset = 0,
      .map_value_kind = TBE_TYPED_BOOL,
      .map_value_wire_kind = TBE_TYPED_BOOL,
      .map_value_type = NULL,
      .wire_offset = 0,
      .wire_size = 8,
      .optional_bit = 0,
      .flags = 0 | TBE_TYPED_FIELD_WIRE_OFFSET },
    { .name = "to_node", .kind = TBE_TYPED_U64,
      .wire_kind = TBE_TYPED_U64, .offset = offsetof(InstallSnapshotChunk_t, to_node),
      .element_kind = TBE_TYPED_BOOL,
      .element_wire_kind = TBE_TYPED_BOOL,
      .element_size = 0,
      .fixed_count = 0,
      .object_type = NULL,
      .map_entry_size = 0,
      .map_key_offset = 0,
      .map_value_offset = 0,
      .map_value_kind = TBE_TYPED_BOOL,
      .map_value_wire_kind = TBE_TYPED_BOOL,
      .map_value_type = NULL,
      .wire_offset = 8,
      .wire_size = 8,
      .optional_bit = 0,
      .flags = 0 | TBE_TYPED_FIELD_WIRE_OFFSET },
    { .name = "term", .kind = TBE_TYPED_U64,
      .wire_kind = TBE_TYPED_U64, .offset = offsetof(InstallSnapshotChunk_t, term),
      .element_kind = TBE_TYPED_BOOL,
      .element_wire_kind = TBE_TYPED_BOOL,
      .element_size = 0,
      .fixed_count = 0,
      .object_type = NULL,
      .map_entry_size = 0,
      .map_key_offset = 0,
      .map_value_offset = 0,
      .map_value_kind = TBE_TYPED_BOOL,
      .map_value_wire_kind = TBE_TYPED_BOOL,
      .map_value_type = NULL,
      .wire_offset = 16,
      .wire_size = 8,
      .optional_bit = 0,
      .flags = 0 | TBE_TYPED_FIELD_WIRE_OFFSET },
    { .name = "snapshot_index", .kind = TBE_TYPED_U64,
      .wire_kind = TBE_TYPED_U64, .offset = offsetof(InstallSnapshotChunk_t, snapshot_index),
      .element_kind = TBE_TYPED_BOOL,
      .element_wire_kind = TBE_TYPED_BOOL,
      .element_size = 0,
      .fixed_count = 0,
      .object_type = NULL,
      .map_entry_size = 0,
      .map_key_offset = 0,
      .map_value_offset = 0,
      .map_value_kind = TBE_TYPED_BOOL,
      .map_value_wire_kind = TBE_TYPED_BOOL,
      .map_value_type = NULL,
      .wire_offset = 24,
      .wire_size = 8,
      .optional_bit = 0,
      .flags = 0 | TBE_TYPED_FIELD_WIRE_OFFSET },
    { .name = "snapshot_term", .kind = TBE_TYPED_U64,
      .wire_kind = TBE_TYPED_U64, .offset = offsetof(InstallSnapshotChunk_t, snapshot_term),
      .element_kind = TBE_TYPED_BOOL,
      .element_wire_kind = TBE_TYPED_BOOL,
      .element_size = 0,
      .fixed_count = 0,
      .object_type = NULL,
      .map_entry_size = 0,
      .map_key_offset = 0,
      .map_value_offset = 0,
      .map_value_kind = TBE_TYPED_BOOL,
      .map_value_wire_kind = TBE_TYPED_BOOL,
      .map_value_type = NULL,
      .wire_offset = 32,
      .wire_size = 8,
      .optional_bit = 0,
      .flags = 0 | TBE_TYPED_FIELD_WIRE_OFFSET },
    { .name = "snapshot_offset", .kind = TBE_TYPED_U64,
      .wire_kind = TBE_TYPED_U64, .offset = offsetof(InstallSnapshotChunk_t, snapshot_offset),
      .element_kind = TBE_TYPED_BOOL,
      .element_wire_kind = TBE_TYPED_BOOL,
      .element_size = 0,
      .fixed_count = 0,
      .object_type = NULL,
      .map_entry_size = 0,
      .map_key_offset = 0,
      .map_value_offset = 0,
      .map_value_kind = TBE_TYPED_BOOL,
      .map_value_wire_kind = TBE_TYPED_BOOL,
      .map_value_type = NULL,
      .wire_offset = 40,
      .wire_size = 8,
      .optional_bit = 0,
      .flags = 0 | TBE_TYPED_FIELD_WIRE_OFFSET },
    { .name = "snapshot_size", .kind = TBE_TYPED_U64,
      .wire_kind = TBE_TYPED_U64, .offset = offsetof(InstallSnapshotChunk_t, snapshot_size),
      .element_kind = TBE_TYPED_BOOL,
      .element_wire_kind = TBE_TYPED_BOOL,
      .element_size = 0,
      .fixed_count = 0,
      .object_type = NULL,
      .map_entry_size = 0,
      .map_key_offset = 0,
      .map_value_offset = 0,
      .map_value_kind = TBE_TYPED_BOOL,
      .map_value_wire_kind = TBE_TYPED_BOOL,
      .map_value_type = NULL,
      .wire_offset = 48,
      .wire_size = 8,
      .optional_bit = 0,
      .flags = 0 | TBE_TYPED_FIELD_WIRE_OFFSET },
    { .name = "done", .kind = TBE_TYPED_U8,
      .wire_kind = TBE_TYPED_U8, .offset = offsetof(InstallSnapshotChunk_t, done),
      .element_kind = TBE_TYPED_BOOL,
      .element_wire_kind = TBE_TYPED_BOOL,
      .element_size = 0,
      .fixed_count = 0,
      .object_type = NULL,
      .map_entry_size = 0,
      .map_key_offset = 0,
      .map_value_offset = 0,
      .map_value_kind = TBE_TYPED_BOOL,
      .map_value_wire_kind = TBE_TYPED_BOOL,
      .map_value_type = NULL,
      .wire_offset = 56,
      .wire_size = 1,
      .optional_bit = 0,
      .flags = 0 | TBE_TYPED_FIELD_WIRE_OFFSET },
    { .name = "snapshot_configuration", .kind = TBE_TYPED_BYTES,
      .wire_kind = TBE_TYPED_BYTES, .offset = offsetof(InstallSnapshotChunk_t, snapshot_configuration),
      .element_kind = TBE_TYPED_BOOL,
      .element_wire_kind = TBE_TYPED_BOOL,
      .element_size = 0,
      .fixed_count = 0,
      .object_type = NULL,
      .map_entry_size = 0,
      .map_key_offset = 0,
      .map_value_offset = 0,
      .map_value_kind = TBE_TYPED_BOOL,
      .map_value_wire_kind = TBE_TYPED_BOOL,
      .map_value_type = NULL,
      .wire_offset = 0,
      .wire_size = 0,
      .optional_bit = 0,
      .flags = 0 | TBE_TYPED_FIELD_VAR_DATA },
    { .name = "snapshot_digest", .kind = TBE_TYPED_BYTES,
      .wire_kind = TBE_TYPED_BYTES, .offset = offsetof(InstallSnapshotChunk_t, snapshot_digest),
      .element_kind = TBE_TYPED_BOOL,
      .element_wire_kind = TBE_TYPED_BOOL,
      .element_size = 0,
      .fixed_count = 0,
      .object_type = NULL,
      .map_entry_size = 0,
      .map_key_offset = 0,
      .map_value_offset = 0,
      .map_value_kind = TBE_TYPED_BOOL,
      .map_value_wire_kind = TBE_TYPED_BOOL,
      .map_value_type = NULL,
      .wire_offset = 0,
      .wire_size = 0,
      .optional_bit = 0,
      .flags = 0 | TBE_TYPED_FIELD_VAR_DATA },
    { .name = "chunk_data", .kind = TBE_TYPED_BYTES,
      .wire_kind = TBE_TYPED_BYTES, .offset = offsetof(InstallSnapshotChunk_t, chunk_data),
      .element_kind = TBE_TYPED_BOOL,
      .element_wire_kind = TBE_TYPED_BOOL,
      .element_size = 0,
      .fixed_count = 0,
      .object_type = NULL,
      .map_entry_size = 0,
      .map_key_offset = 0,
      .map_value_offset = 0,
      .map_value_kind = TBE_TYPED_BOOL,
      .map_value_wire_kind = TBE_TYPED_BOOL,
      .map_value_type = NULL,
      .wire_offset = 0,
      .wire_size = 0,
      .optional_bit = 0,
      .flags = 0 | TBE_TYPED_FIELD_VAR_DATA },
};
static TbeTypedType InstallSnapshotChunk_TYPED_TYPE = {
    .name = "InstallSnapshotChunk", .size = sizeof(InstallSnapshotChunk_t), .fields = InstallSnapshotChunk_TYPED_FIELDS,
    .field_count = sizeof(InstallSnapshotChunk_TYPED_FIELDS) / sizeof(InstallSnapshotChunk_TYPED_FIELDS[0]),
    .fixed_block_size = 57,
    .presence_offset = 0,
    .presence_size = 0,
    .wire_big_endian = 0
};
static const TbeTypedField InstallSnapshotAck_TYPED_FIELDS[] = {
    { .name = "from_node", .kind = TBE_TYPED_U64,
      .wire_kind = TBE_TYPED_U64, .offset = offsetof(InstallSnapshotAck_t, from_node),
      .element_kind = TBE_TYPED_BOOL,
      .element_wire_kind = TBE_TYPED_BOOL,
      .element_size = 0,
      .fixed_count = 0,
      .object_type = NULL,
      .map_entry_size = 0,
      .map_key_offset = 0,
      .map_value_offset = 0,
      .map_value_kind = TBE_TYPED_BOOL,
      .map_value_wire_kind = TBE_TYPED_BOOL,
      .map_value_type = NULL,
      .wire_offset = 0,
      .wire_size = 8,
      .optional_bit = 0,
      .flags = 0 | TBE_TYPED_FIELD_WIRE_OFFSET },
    { .name = "to_node", .kind = TBE_TYPED_U64,
      .wire_kind = TBE_TYPED_U64, .offset = offsetof(InstallSnapshotAck_t, to_node),
      .element_kind = TBE_TYPED_BOOL,
      .element_wire_kind = TBE_TYPED_BOOL,
      .element_size = 0,
      .fixed_count = 0,
      .object_type = NULL,
      .map_entry_size = 0,
      .map_key_offset = 0,
      .map_value_offset = 0,
      .map_value_kind = TBE_TYPED_BOOL,
      .map_value_wire_kind = TBE_TYPED_BOOL,
      .map_value_type = NULL,
      .wire_offset = 8,
      .wire_size = 8,
      .optional_bit = 0,
      .flags = 0 | TBE_TYPED_FIELD_WIRE_OFFSET },
    { .name = "term", .kind = TBE_TYPED_U64,
      .wire_kind = TBE_TYPED_U64, .offset = offsetof(InstallSnapshotAck_t, term),
      .element_kind = TBE_TYPED_BOOL,
      .element_wire_kind = TBE_TYPED_BOOL,
      .element_size = 0,
      .fixed_count = 0,
      .object_type = NULL,
      .map_entry_size = 0,
      .map_key_offset = 0,
      .map_value_offset = 0,
      .map_value_kind = TBE_TYPED_BOOL,
      .map_value_wire_kind = TBE_TYPED_BOOL,
      .map_value_type = NULL,
      .wire_offset = 16,
      .wire_size = 8,
      .optional_bit = 0,
      .flags = 0 | TBE_TYPED_FIELD_WIRE_OFFSET },
    { .name = "snapshot_index", .kind = TBE_TYPED_U64,
      .wire_kind = TBE_TYPED_U64, .offset = offsetof(InstallSnapshotAck_t, snapshot_index),
      .element_kind = TBE_TYPED_BOOL,
      .element_wire_kind = TBE_TYPED_BOOL,
      .element_size = 0,
      .fixed_count = 0,
      .object_type = NULL,
      .map_entry_size = 0,
      .map_key_offset = 0,
      .map_value_offset = 0,
      .map_value_kind = TBE_TYPED_BOOL,
      .map_value_wire_kind = TBE_TYPED_BOOL,
      .map_value_type = NULL,
      .wire_offset = 24,
      .wire_size = 8,
      .optional_bit = 0,
      .flags = 0 | TBE_TYPED_FIELD_WIRE_OFFSET },
    { .name = "snapshot_size", .kind = TBE_TYPED_U64,
      .wire_kind = TBE_TYPED_U64, .offset = offsetof(InstallSnapshotAck_t, snapshot_size),
      .element_kind = TBE_TYPED_BOOL,
      .element_wire_kind = TBE_TYPED_BOOL,
      .element_size = 0,
      .fixed_count = 0,
      .object_type = NULL,
      .map_entry_size = 0,
      .map_key_offset = 0,
      .map_value_offset = 0,
      .map_value_kind = TBE_TYPED_BOOL,
      .map_value_wire_kind = TBE_TYPED_BOOL,
      .map_value_type = NULL,
      .wire_offset = 32,
      .wire_size = 8,
      .optional_bit = 0,
      .flags = 0 | TBE_TYPED_FIELD_WIRE_OFFSET },
    { .name = "next_offset", .kind = TBE_TYPED_U64,
      .wire_kind = TBE_TYPED_U64, .offset = offsetof(InstallSnapshotAck_t, next_offset),
      .element_kind = TBE_TYPED_BOOL,
      .element_wire_kind = TBE_TYPED_BOOL,
      .element_size = 0,
      .fixed_count = 0,
      .object_type = NULL,
      .map_entry_size = 0,
      .map_key_offset = 0,
      .map_value_offset = 0,
      .map_value_kind = TBE_TYPED_BOOL,
      .map_value_wire_kind = TBE_TYPED_BOOL,
      .map_value_type = NULL,
      .wire_offset = 40,
      .wire_size = 8,
      .optional_bit = 0,
      .flags = 0 | TBE_TYPED_FIELD_WIRE_OFFSET },
    { .name = "accepted", .kind = TBE_TYPED_U8,
      .wire_kind = TBE_TYPED_U8, .offset = offsetof(InstallSnapshotAck_t, accepted),
      .element_kind = TBE_TYPED_BOOL,
      .element_wire_kind = TBE_TYPED_BOOL,
      .element_size = 0,
      .fixed_count = 0,
      .object_type = NULL,
      .map_entry_size = 0,
      .map_key_offset = 0,
      .map_value_offset = 0,
      .map_value_kind = TBE_TYPED_BOOL,
      .map_value_wire_kind = TBE_TYPED_BOOL,
      .map_value_type = NULL,
      .wire_offset = 48,
      .wire_size = 1,
      .optional_bit = 0,
      .flags = 0 | TBE_TYPED_FIELD_WIRE_OFFSET },
    { .name = "snapshot_digest", .kind = TBE_TYPED_BYTES,
      .wire_kind = TBE_TYPED_BYTES, .offset = offsetof(InstallSnapshotAck_t, snapshot_digest),
      .element_kind = TBE_TYPED_BOOL,
      .element_wire_kind = TBE_TYPED_BOOL,
      .element_size = 0,
      .fixed_count = 0,
      .object_type = NULL,
      .map_entry_size = 0,
      .map_key_offset = 0,
      .map_value_offset = 0,
      .map_value_kind = TBE_TYPED_BOOL,
      .map_value_wire_kind = TBE_TYPED_BOOL,
      .map_value_type = NULL,
      .wire_offset = 0,
      .wire_size = 0,
      .optional_bit = 0,
      .flags = 0 | TBE_TYPED_FIELD_VAR_DATA },
};
static TbeTypedType InstallSnapshotAck_TYPED_TYPE = {
    .name = "InstallSnapshotAck", .size = sizeof(InstallSnapshotAck_t), .fields = InstallSnapshotAck_TYPED_FIELDS,
    .field_count = sizeof(InstallSnapshotAck_TYPED_FIELDS) / sizeof(InstallSnapshotAck_TYPED_FIELDS[0]),
    .fixed_block_size = 49,
    .presence_offset = 0,
    .presence_size = 0,
    .wire_big_endian = 0
};

#define TBE_TYPED_DEFINE_RECORD(name) \
    void name##_init(name##_t *object) { \
        if (object) (void)tbe_typed_init(&name##_TYPED_TYPE, object, NULL); \
    } \
    void name##_clear(name##_t *object) { tbe_typed_clear(&name##_TYPED_TYPE, object); } \
    DataBindStatus name##_from_bin(DataBind *codec, name##_t *object, const void *data, size_t len, DataBindError *error) { \
        return tbe_typed_parse(codec, #name, &name##_TYPED_TYPE, "bin", data, len, 0, object, error); \
    } \
    DataBindStatus name##_from_json(DataBind *codec, name##_t *object, const char *data, size_t len, DataBindError *error) { \
        return tbe_typed_parse(codec, #name, &name##_TYPED_TYPE, "json", data, len, 0, object, error); \
    } \
    DataBindStatus name##_from_yaml(DataBind *codec, name##_t *object, const char *data, size_t len, DataBindError *error) { \
        return tbe_typed_parse(codec, #name, &name##_TYPED_TYPE, "yaml", data, len, 0, object, error); \
    } \
    DataBindStatus name##_from_csv(DataBind *codec, name##_t *object, const char *data, size_t len, size_t row, DataBindError *error) { \
        return tbe_typed_parse(codec, #name, &name##_TYPED_TYPE, "csv", data, len, row, object, error); \
    } \
    DataBindStatus name##_from_xml(DataBind *codec, name##_t *object, const char *data, size_t len, DataBindError *error) { \
        return tbe_typed_parse(codec, #name, &name##_TYPED_TYPE, "xml", data, len, 0, object, error); \
    } \
    DataBindStatus name##_to_bin(const name##_t *object, uint8_t **out, size_t *out_len, DataBindError *error) { \
        return tbe_typed_serialize_binary(&name##_TYPED_TYPE, object, out, out_len, error); \
    } \
    DataBindStatus name##_to_bin_into(const name##_t *object, uint8_t *output, size_t output_capacity, size_t *out_len, DataBindError *error) { \
        return tbe_typed_serialize_binary_into(&name##_TYPED_TYPE, object, output, output_capacity, out_len, error); \
    } \
    DataBindStatus name##_to_json(DataBind *codec, const name##_t *object, char **out, size_t *out_len, DataBindError *error) { \
        return tbe_typed_serialize(codec, #name, &name##_TYPED_TYPE, object, "json", out, out_len, error); \
    } \
    DataBindStatus name##_to_yaml(DataBind *codec, const name##_t *object, char **out, size_t *out_len, DataBindError *error) { \
        return tbe_typed_serialize(codec, #name, &name##_TYPED_TYPE, object, "yaml", out, out_len, error); \
    } \
    DataBindStatus name##_to_csv(DataBind *codec, const name##_t *object, char **out, size_t *out_len, DataBindError *error) { \
        return tbe_typed_serialize(codec, #name, &name##_TYPED_TYPE, object, "csv", out, out_len, error); \
    } \
    DataBindStatus name##_to_xml(DataBind *codec, const name##_t *object, char **out, size_t *out_len, DataBindError *error) { \
        return tbe_typed_serialize(codec, #name, &name##_TYPED_TYPE, object, "xml", out, out_len, error); \
    }

TBE_TYPED_DEFINE_RECORD(RaftWireMessage)
TBE_TYPED_DEFINE_RECORD(InstallSnapshotChunk)
TBE_TYPED_DEFINE_RECORD(InstallSnapshotAck)

static DataBindStatus TurboRaftWire_schema_codec_error(DataBindError *error,
                                                                DataBindStatus status,
                                                                const char *path,
                                                                const char *message) {
    if (error != NULL && error->size >= sizeof(*error)) {
        error->code = status;
        error->line = -1;
        error->column = -1;
        snprintf(error->path, sizeof(error->path), "%s", path != NULL ? path : "");
        snprintf(error->message, sizeof(error->message), "%s", message != NULL ? message : "");
    }
    return status;
}

static DataBindStatus TurboRaftWire_text_to_binary_into(
    DataBind *codec, const char *type_name, uint32_t format, const void *input,
    size_t input_len, size_t csv_row, uint8_t *output, size_t output_capacity,
    size_t *out_len, DataBindError *error) {
    if (out_len != NULL) *out_len = 0;
    if (codec == NULL || type_name == NULL || input == NULL || output == NULL || out_len == NULL)
        return TurboRaftWire_schema_codec_error(
            error, DATA_BIND_ERR_INVALID_ARG, type_name,
            "Invalid schema codec text-to-binary arguments");

#define TBE_SCHEMA_TEXT_TO_BINARY_CASE(name) \
    if (strcmp(type_name, #name) == 0) { \
        name##_t object; \
        DataBindStatus status; \
        name##_init(&object); \
        switch (format) { \
        case TBE_SCHEMA_FORMAT_JSON: \
            status = name##_from_json(codec, &object, (const char *)input, input_len, error); \
            break; \
        case TBE_SCHEMA_FORMAT_YAML: \
            status = name##_from_yaml(codec, &object, (const char *)input, input_len, error); \
            break; \
        case TBE_SCHEMA_FORMAT_CSV: \
            status = name##_from_csv(codec, &object, (const char *)input, input_len, csv_row, error); \
            break; \
        case TBE_SCHEMA_FORMAT_XML: \
            status = name##_from_xml(codec, &object, (const char *)input, input_len, error); \
            break; \
        default: \
            status = TurboRaftWire_schema_codec_error( \
                error, DATA_BIND_ERR_INVALID_ARG, type_name, "Unknown schema text format"); \
            break; \
        } \
        if (status == DATA_BIND_OK) status = name##_to_bin_into(&object, output, output_capacity, out_len, error); \
        name##_clear(&object); \
        return status; \
    }

    TBE_SCHEMA_TEXT_TO_BINARY_CASE(RaftWireMessage)
    TBE_SCHEMA_TEXT_TO_BINARY_CASE(InstallSnapshotChunk)
    TBE_SCHEMA_TEXT_TO_BINARY_CASE(InstallSnapshotAck)
#undef TBE_SCHEMA_TEXT_TO_BINARY_CASE

    return TurboRaftWire_schema_codec_error(
        error, DATA_BIND_ERR_TYPE_NOT_FOUND, type_name, "Schema codec type not found");
}

static DataBindStatus TurboRaftWire_binary_to_text(
    DataBind *codec, const char *type_name, uint32_t format, const void *input,
    size_t input_len, char **out, size_t *out_len, DataBindError *error) {
    if (out != NULL) *out = NULL;
    if (out_len != NULL) *out_len = 0;
    if (codec == NULL || type_name == NULL || input == NULL || input_len == 0 ||
        out == NULL || out_len == NULL)
        return TurboRaftWire_schema_codec_error(
            error, DATA_BIND_ERR_INVALID_ARG, type_name,
            "Invalid schema codec binary-to-text arguments");

#define TBE_SCHEMA_BINARY_TO_TEXT_CASE(name) \
    if (strcmp(type_name, #name) == 0) { \
        name##_t object; \
        DataBindStatus status; \
        name##_init(&object); \
        status = name##_from_bin(codec, &object, input, input_len, error); \
        if (status == DATA_BIND_OK) { \
            switch (format) { \
            case TBE_SCHEMA_FORMAT_JSON: \
                status = name##_to_json(codec, &object, out, out_len, error); \
                break; \
            case TBE_SCHEMA_FORMAT_YAML: \
                status = name##_to_yaml(codec, &object, out, out_len, error); \
                break; \
            case TBE_SCHEMA_FORMAT_CSV: \
                status = name##_to_csv(codec, &object, out, out_len, error); \
                break; \
            case TBE_SCHEMA_FORMAT_XML: \
                status = name##_to_xml(codec, &object, out, out_len, error); \
                break; \
            default: \
                status = TurboRaftWire_schema_codec_error( \
                    error, DATA_BIND_ERR_INVALID_ARG, type_name, "Unknown schema text format"); \
                break; \
            } \
        } \
        name##_clear(&object); \
        return status; \
    }

    TBE_SCHEMA_BINARY_TO_TEXT_CASE(RaftWireMessage)
    TBE_SCHEMA_BINARY_TO_TEXT_CASE(InstallSnapshotChunk)
    TBE_SCHEMA_BINARY_TO_TEXT_CASE(InstallSnapshotAck)
#undef TBE_SCHEMA_BINARY_TO_TEXT_CASE

    return TurboRaftWire_schema_codec_error(
        error, DATA_BIND_ERR_TYPE_NOT_FOUND, type_name, "Schema codec type not found");
}

static void TurboRaftWire_schema_codec_free_output(void *output) {
    tbe_typed_serialized_free(output);
}

static const tbe_schema_codec_v1_t TurboRaftWire_SCHEMA_CODEC = {
    .struct_size = sizeof(tbe_schema_codec_v1_t),
    .abi_version = TBE_SCHEMA_CODEC_ABI_VERSION,
    .schema_id = "TurboRaftWire",
    .create = TurboRaftWire_codec_create,
    .text_to_binary_into = TurboRaftWire_text_to_binary_into,
    .binary_to_text = TurboRaftWire_binary_to_text,
    .free_output = TurboRaftWire_schema_codec_free_output
};

const tbe_schema_codec_v1_t *TurboRaftWire_schema_codec(void) {
    return &TurboRaftWire_SCHEMA_CODEC;
}
