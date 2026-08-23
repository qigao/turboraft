#include <turboraft/text_protocol_executor.h>

#include <stdbool.h>
#include <string.h>

typedef struct {
  const char *name;
  size_t length;
  tr_raft_message_type_t type;
} tr_text_protocol_message_name_t;

static const tr_text_protocol_message_name_t
    tr_text_protocol_message_names[] = {
        {"pre_vote_request", sizeof("pre_vote_request") - 1u,
         TR_RAFT_MSG_PRE_VOTE_REQUEST},
        {"pre_vote_response", sizeof("pre_vote_response") - 1u,
         TR_RAFT_MSG_PRE_VOTE_RESPONSE},
        {"vote_request", sizeof("vote_request") - 1u,
         TR_RAFT_MSG_VOTE_REQUEST},
        {"vote_response", sizeof("vote_response") - 1u,
         TR_RAFT_MSG_VOTE_RESPONSE},
        {"heartbeat_request", sizeof("heartbeat_request") - 1u,
         TR_RAFT_MSG_HEARTBEAT_REQUEST},
        {"heartbeat_response", sizeof("heartbeat_response") - 1u,
         TR_RAFT_MSG_HEARTBEAT_RESPONSE},
        {"append_request", sizeof("append_request") - 1u,
         TR_RAFT_MSG_APPEND_REQUEST},
        {"append_response", sizeof("append_response") - 1u,
         TR_RAFT_MSG_APPEND_RESPONSE},
        {"timeout_now", sizeof("timeout_now") - 1u,
         TR_RAFT_MSG_TIMEOUT_NOW},
        {"read_index_request", sizeof("read_index_request") - 1u,
         TR_RAFT_MSG_READ_INDEX_REQUEST},
        {"read_index_response", sizeof("read_index_response") - 1u,
         TR_RAFT_MSG_READ_INDEX_RESPONSE}};

static bool tr_text_protocol_text_equals(vstr value, const char *literal)
{
  size_t literal_length;

  if (value.data == NULL || literal == NULL) {
    return false;
  }
  literal_length = strlen(literal);
  return value.len == literal_length &&
         memcmp(value.data, literal, literal_length) == 0;
}

static int tr_text_protocol_hex_value(char value)
{
  if (value >= '0' && value <= '9') {
    return (int)(value - '0');
  }
  if (value >= 'a' && value <= 'f') {
    return (int)(value - 'a') + 10;
  }
  if (value >= 'A' && value <= 'F') {
    return (int)(value - 'A') + 10;
  }
  return -1;
}

static int tr_text_protocol_decode_hex(
    vstr input,
    uint8_t *output,
    size_t output_capacity,
    size_t *output_length)
{
  size_t hex_digits;
  size_t decoded_length;
  size_t index;

  if (input.data == NULL || output == NULL || output_length == NULL ||
      input.len < 3u || input.data[0] != '0' ||
      (input.data[1] != 'x' && input.data[1] != 'X')) {
    return TURBO_EPROTO;
  }
  hex_digits = input.len - 2u;
  if ((hex_digits & 1u) != 0u) {
    return TURBO_EPROTO;
  }
  decoded_length = hex_digits / 2u;
  if (decoded_length > output_capacity) {
    return TURBO_ENOSPC;
  }
  for (index = 0u; index < decoded_length; ++index) {
    int high = tr_text_protocol_hex_value(input.data[2u + index * 2u]);
    int low = tr_text_protocol_hex_value(input.data[3u + index * 2u]);

    if (high < 0 || low < 0) {
      return TURBO_EPROTO;
    }
    output[index] = (uint8_t)((high << 4) | low);
  }
  *output_length = decoded_length;
  return TURBO_OK;
}

static bool tr_text_protocol_find_message(
    vstr name,
    tr_raft_message_type_t *out_type)
{
  size_t index;

  for (index = 0u;
       index < sizeof(tr_text_protocol_message_names) /
                   sizeof(tr_text_protocol_message_names[0]);
       ++index) {
    const tr_text_protocol_message_name_t *entry =
        &tr_text_protocol_message_names[index];

    if (name.data != NULL && name.len == entry->length &&
        memcmp(name.data, entry->name, entry->length) == 0) {
      if (out_type != NULL) {
        *out_type = entry->type;
      }
      return true;
    }
  }
  return false;
}

static int tr_text_protocol_expected_kind(
    vstr name,
    tr_raft_wire_payload_kind_t *out_kind)
{
  if (tr_text_protocol_text_equals(name, "raft")) {
    *out_kind = TR_RAFT_WIRE_PAYLOAD_RAFT;
    return TURBO_OK;
  }
  if (tr_text_protocol_text_equals(name, "snapshot_chunk")) {
    *out_kind = TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_CHUNK;
    return TURBO_OK;
  }
  if (tr_text_protocol_text_equals(name, "snapshot_ack")) {
    *out_kind = TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_ACK;
    return TURBO_OK;
  }
  return TURBO_ENOTSUP;
}

static int tr_text_protocol_validate_common(
    const tr_text_protocol_frame_t *frame,
    uint16_t version,
    tr_raft_node_id_t from,
    tr_raft_node_id_t to,
    tr_raft_term_t term)
{
  if (frame->version != (uint64_t)version || frame->from != from ||
      frame->to != to || frame->term != term) {
    return TURBO_EPROTO;
  }
  return TURBO_OK;
}

static int tr_text_protocol_decode_frame(
    tr_raft_wire_codec_t *codec,
    const tr_text_protocol_frame_t *frame,
    tr_text_protocol_debug_decoded_frame_t *decoded)
{
  uint8_t wire_frame[TR_RAFT_WIRE_MAX_FRAME_SIZE];
  size_t wire_frame_length;
  uint16_t version;
  tr_raft_wire_payload_kind_t payload_kind;
  tr_raft_wire_payload_kind_t expected_kind;
  tr_raft_message_type_t expected_message;
  int result;

  if (frame->payload_hex.len == 0u) {
    return TURBO_EINVAL;
  }
  result = tr_text_protocol_expected_kind(frame->kind, &expected_kind);
  if (result != TURBO_OK) {
    return result;
  }
  result = tr_text_protocol_decode_hex(
      frame->payload_hex, wire_frame, sizeof(wire_frame), &wire_frame_length);
  if (result != TURBO_OK) {
    return result;
  }
  result = tr_raft_wire_peek_version(
      wire_frame, wire_frame_length, &version);
  if (result != TURBO_OK) {
    return result;
  }
  result = tr_raft_wire_peek_payload_kind(
      wire_frame, wire_frame_length, &payload_kind);
  if (result != TURBO_OK) {
    return result;
  }
  if (payload_kind != expected_kind ||
      frame->version != (uint64_t)version) {
    return TURBO_EPROTO;
  }

  memset(decoded, 0, sizeof(*decoded));
  decoded->version = version;
  decoded->payload_kind = payload_kind;

  switch (payload_kind) {
    case TR_RAFT_WIRE_PAYLOAD_RAFT:
      if (!tr_text_protocol_find_message(frame->message, &expected_message)) {
        return TURBO_ENOTSUP;
      }
      result = tr_raft_wire_decode(
          codec, wire_frame, wire_frame_length, &decoded->metadata,
          &decoded->payload.raft);
      if (result != TURBO_OK) {
        return result;
      }
      if (decoded->payload.raft.type != expected_message) {
        return TURBO_EPROTO;
      }
      return tr_text_protocol_validate_common(
          frame, version, decoded->payload.raft.from,
          decoded->payload.raft.to, decoded->payload.raft.term);

    case TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_CHUNK:
      if (!tr_text_protocol_text_equals(frame->message, "snapshot_chunk")) {
        return TURBO_EPROTO;
      }
      result = tr_raft_wire_decode_snapshot_chunk(
          codec, wire_frame, wire_frame_length, &decoded->metadata,
          &decoded->payload.snapshot_chunk);
      if (result != TURBO_OK) {
        return result;
      }
      return tr_text_protocol_validate_common(
          frame, version, decoded->payload.snapshot_chunk.from,
          decoded->payload.snapshot_chunk.to,
          decoded->payload.snapshot_chunk.term);

    case TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_ACK:
      if (!tr_text_protocol_text_equals(frame->message, "snapshot_ack")) {
        return TURBO_EPROTO;
      }
      result = tr_raft_wire_decode_snapshot_ack(
          codec, wire_frame, wire_frame_length, &decoded->metadata,
          &decoded->payload.snapshot_ack);
      if (result != TURBO_OK) {
        return result;
      }
      return tr_text_protocol_validate_common(
          frame, version, decoded->payload.snapshot_ack.from,
          decoded->payload.snapshot_ack.to,
          decoded->payload.snapshot_ack.term);

    default:
      return TURBO_ENOTSUP;
  }
}

int tr_text_protocol_debug_execute(
    const tr_text_protocol_debug_plan_t *plan,
    const tr_text_protocol_debug_executor_ops_t *ops,
    void *context)
{
  tr_raft_wire_codec_t *codec = NULL;
  size_t frame_index;
  int result;

  if (plan == NULL || ops == NULL ||
      plan->frame_count > TR_TEXT_MAX_STATEMENTS) {
    return TURBO_EINVAL;
  }
  if (ops->frame == NULL) {
    return TURBO_ENOTSUP;
  }
  result = tr_raft_wire_codec_create(&codec);
  if (result != TURBO_OK) {
    return result;
  }

  result = TURBO_OK;
  for (frame_index = 0u; frame_index < plan->frame_count; ++frame_index) {
    tr_text_protocol_debug_decoded_frame_t decoded;
    const tr_text_protocol_frame_t *frame = &plan->frames[frame_index];

    result = tr_text_protocol_decode_frame(codec, frame, &decoded);
    if (result != TURBO_OK) {
      break;
    }
    result = ops->frame(context, frame, &decoded);
    if (result != TURBO_OK) {
      break;
    }
  }
  tr_raft_wire_codec_destroy(codec);
  return result;
}
