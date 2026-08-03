#include <tinytest.h>

#include <turboraft/raft_wire_codec.h>
#include <turboraft/text_protocol_executor.h>

#include <stdio.h>
#include <string.h>

typedef struct {
  int calls;
  int result;
  tr_raft_wire_payload_kind_t payload_kind;
  tr_raft_message_type_t message_type;
  uint64_t from;
  uint64_t to;
  uint64_t term;
  uint64_t snapshot_index;
} tr_text_protocol_executor_test_state_t;

static int tr_text_protocol_executor_test_frame(
    void *context,
    const tr_text_protocol_frame_t *frame,
    const tr_text_protocol_debug_decoded_frame_t *decoded)
{
  tr_text_protocol_executor_test_state_t *state =
      (tr_text_protocol_executor_test_state_t *)context;

  state->calls++;
  state->payload_kind = decoded->payload_kind;
  if (decoded->payload_kind == TR_RAFT_WIRE_PAYLOAD_RAFT) {
    state->message_type = decoded->payload.raft.type;
    state->from = decoded->payload.raft.from;
    state->to = decoded->payload.raft.to;
    state->term = decoded->payload.raft.term;
  } else if (decoded->payload_kind ==
             TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_CHUNK) {
    state->from = decoded->payload.snapshot_chunk.from;
    state->to = decoded->payload.snapshot_chunk.to;
    state->term = decoded->payload.snapshot_chunk.term;
    state->snapshot_index = decoded->payload.snapshot_chunk.snapshot_index;
  } else {
    state->from = decoded->payload.snapshot_ack.from;
    state->to = decoded->payload.snapshot_ack.to;
    state->term = decoded->payload.snapshot_ack.term;
    state->snapshot_index = decoded->payload.snapshot_ack.snapshot_index;
  }
  check(frame != NULL);
  return state->result;
}

static const tr_text_protocol_debug_executor_ops_t
    tr_text_protocol_executor_test_ops = {
        .frame = tr_text_protocol_executor_test_frame};

static int tr_text_protocol_executor_hex_encode(
    const uint8_t *input,
    size_t input_length,
    char *output,
    size_t output_capacity)
{
  static const char digits[] = "0123456789abcdef";
  size_t index;

  if (output == NULL ||
      output_capacity < input_length * 2u + 3u) {
    return TURBO_ENOSPC;
  }
  output[0] = '0';
  output[1] = 'x';
  for (index = 0u; index < input_length; ++index) {
    output[2u + index * 2u] = digits[input[index] >> 4u];
    output[3u + index * 2u] = digits[input[index] & 0x0fu];
  }
  output[input_length * 2u + 2u] = '\0';
  return TURBO_OK;
}

static int tr_text_protocol_executor_make_raft_frame(
    uint8_t *output,
    size_t output_capacity,
    size_t *output_length)
{
  tr_raft_wire_codec_t *codec = NULL;
  tr_raft_wire_metadata_t metadata;
  tr_raft_message_t message;
  int result;

  memset(&metadata, 0, sizeof(metadata));
  metadata.message_id = 11u;
  memset(&message, 0, sizeof(message));
  message.type = TR_RAFT_MSG_TIMEOUT_NOW;
  message.from = 1u;
  message.to = 2u;
  message.term = 9u;

  result = tr_raft_wire_codec_create(&codec);
  if (result != TURBO_OK) {
    return result;
  }
  result = tr_raft_wire_encode(
      codec, &metadata, &message, output, output_capacity, output_length);
  tr_raft_wire_codec_destroy(codec);
  return result;
}

static int tr_text_protocol_executor_make_snapshot_frames(
    uint8_t *chunk_output,
    size_t chunk_capacity,
    size_t *chunk_length,
    uint8_t *ack_output,
    size_t ack_capacity,
    size_t *ack_length)
{
  tr_raft_wire_codec_t *codec = NULL;
  tr_raft_wire_metadata_t metadata;
  tr_raft_snapshot_chunk_t chunk;
  tr_raft_snapshot_ack_t ack;
  size_t index;
  int result;

  memset(&metadata, 0, sizeof(metadata));
  metadata.message_id = 12u;
  memset(&chunk, 0, sizeof(chunk));
  chunk.from = 1u;
  chunk.to = 2u;
  chunk.term = 7u;
  chunk.snapshot_index = 50u;
  chunk.snapshot_term = 6u;
  chunk.snapshot_size = 2u;
  chunk.data_length = 2u;
  chunk.data[0] = 0xaau;
  chunk.data[1] = 0xbbu;
  chunk.done = true;
  chunk.has_configuration = true;
  chunk.configuration.phase = TR_RAFT_CONF_FINAL;
  chunk.configuration.member_count = 1u;
  chunk.configuration.members[0].node_id = 2u;
  chunk.configuration.members[0].roles =
      TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER;
  for (index = 0u; index < sizeof(chunk.snapshot_digest); ++index) {
    chunk.snapshot_digest[index] = (uint8_t)(index + 1u);
  }
  ack.from = 2u;
  ack.to = 1u;
  ack.term = 7u;
  ack.snapshot_index = 50u;
  ack.snapshot_size = 2u;
  ack.next_offset = 2u;
  ack.accepted = true;
  memcpy(ack.snapshot_digest, chunk.snapshot_digest,
         sizeof(ack.snapshot_digest));

  result = tr_raft_wire_codec_create(&codec);
  if (result != TURBO_OK) {
    return result;
  }
  result = tr_raft_wire_encode_snapshot_chunk(
      codec, &metadata, &chunk, chunk_output, chunk_capacity, chunk_length);
  if (result == TURBO_OK) {
    result = tr_raft_wire_encode_snapshot_ack(
        codec, &metadata, &ack, ack_output, ack_capacity, ack_length);
  }
  tr_raft_wire_codec_destroy(codec);
  return result;
}

static void tr_text_protocol_executor_set_frame(
    tr_text_protocol_frame_t *frame,
    uint64_t version,
    const char *kind,
    const char *message,
    const char *payload)
{
  frame->version = version;
  frame->kind.data = kind;
  frame->kind.len = strlen(kind);
  frame->from = 1u;
  frame->to = 2u;
  frame->term = 9u;
  frame->message.data = message;
  frame->message.len = strlen(message);
  frame->payload_hex.data = payload;
  frame->payload_hex.len = strlen(payload);
}

spec("TurboRaft text protocol executor") {
  it("parses, decodes, and validates a raft frame") {
    uint8_t wire_frame[TR_RAFT_WIRE_MAX_FRAME_SIZE];
    char payload[TR_RAFT_WIRE_MAX_FRAME_SIZE * 2u + 3u];
    char input[TR_RAFT_WIRE_MAX_FRAME_SIZE * 2u + 256u];
    tr_text_protocol_debug_plan_t plan;
    tr_text_protocol_executor_test_state_t state = {0};
    tr_text_diagnostic_t diagnostic;
    size_t frame_length = 0u;
    int input_length;

    check_int_eq(tr_text_protocol_executor_make_raft_frame(
                     wire_frame, sizeof(wire_frame), &frame_length),
                 TURBO_OK);
    check_int_eq(tr_text_protocol_executor_hex_encode(
                     wire_frame, frame_length, payload, sizeof(payload)),
                 TURBO_OK);
    input_length = snprintf(
        input, sizeof(input),
        "frame version 3 kind raft { from = 1; to = 2; term = 9; "
        "message = timeout_now; payload = %s; }",
        payload);
    check(input_length > 0);
    check((size_t)input_length < sizeof(input));
    check_int_eq(tr_text_protocol_debug_parse(
                     input, (size_t)input_length, NULL, &plan, &diagnostic),
                 TURBO_OK);
    check_int_eq(tr_text_protocol_debug_execute(
                     &plan, &tr_text_protocol_executor_test_ops, &state),
                 TURBO_OK);
    check_int_eq(state.calls, 1);
    check_int_eq(state.payload_kind, TR_RAFT_WIRE_PAYLOAD_RAFT);
    check_int_eq(state.message_type, TR_RAFT_MSG_TIMEOUT_NOW);
    check_long_eq(state.from, 1u);
    check_long_eq(state.to, 2u);
    check_long_eq(state.term, 9u);
  }

  it("decodes snapshot chunks and acknowledgements") {
    uint8_t chunk_frame[TR_RAFT_WIRE_MAX_FRAME_SIZE];
    uint8_t ack_frame[TR_RAFT_WIRE_MAX_FRAME_SIZE];
    char chunk_payload[TR_RAFT_WIRE_MAX_FRAME_SIZE * 2u + 3u];
    char ack_payload[TR_RAFT_WIRE_MAX_FRAME_SIZE * 2u + 3u];
    tr_text_protocol_debug_plan_t plan = {0};
    tr_text_protocol_executor_test_state_t state = {0};
    size_t chunk_length = 0u;
    size_t ack_length = 0u;

    check_int_eq(tr_text_protocol_executor_make_snapshot_frames(
                     chunk_frame, sizeof(chunk_frame), &chunk_length,
                     ack_frame, sizeof(ack_frame), &ack_length),
                 TURBO_OK);
    check_int_eq(tr_text_protocol_executor_hex_encode(
                     chunk_frame, chunk_length, chunk_payload,
                     sizeof(chunk_payload)),
                 TURBO_OK);
    check_int_eq(tr_text_protocol_executor_hex_encode(
                     ack_frame, ack_length, ack_payload, sizeof(ack_payload)),
                 TURBO_OK);
    tr_text_protocol_executor_set_frame(
        &plan.frames[0], 4u, "snapshot_chunk", "snapshot_chunk",
        chunk_payload);
    tr_text_protocol_executor_set_frame(
        &plan.frames[1], 4u, "snapshot_ack", "snapshot_ack", ack_payload);
    plan.frames[0].term = 7u;
    plan.frames[1].from = 2u;
    plan.frames[1].to = 1u;
    plan.frames[1].term = 7u;
    plan.frame_count = 2u;

    check_int_eq(tr_text_protocol_debug_execute(
                     &plan, &tr_text_protocol_executor_test_ops, &state),
                 TURBO_OK);
    check_int_eq(state.calls, 2);
    check_int_eq(state.payload_kind, TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_ACK);
    check_long_eq(state.from, 2u);
    check_long_eq(state.to, 1u);
    check_long_eq(state.term, 7u);
    check_long_eq(state.snapshot_index, 50u);
  }

  it("rejects missing payload before invoking the callback") {
    tr_text_protocol_debug_plan_t plan = {0};
    tr_text_protocol_executor_test_state_t state = {0};

    tr_text_protocol_executor_set_frame(
        &plan.frames[0], 3u, "raft", "timeout_now", "");
    plan.frame_count = 1u;

    check_int_eq(tr_text_protocol_debug_execute(
                     &plan, &tr_text_protocol_executor_test_ops, &state),
                 TURBO_EINVAL);
    check_int_eq(state.calls, 0);
  }

  it("rejects a payload larger than the bounded frame buffer") {
    enum {
      OVERSIZE_HEX_LENGTH =
          2u + (TR_RAFT_WIRE_MAX_FRAME_SIZE + 1u) * 2u
    };
    char payload[OVERSIZE_HEX_LENGTH + 1u];
    tr_text_protocol_debug_plan_t plan = {0};
    tr_text_protocol_executor_test_state_t state = {0};
    size_t index;

    payload[0] = '0';
    payload[1] = 'x';
    for (index = 2u; index < sizeof(payload) - 1u; index += 2u) {
      payload[index] = '0';
      payload[index + 1u] = '0';
    }
    payload[sizeof(payload) - 1u] = '\0';
    tr_text_protocol_executor_set_frame(
        &plan.frames[0], 3u, "raft", "timeout_now", payload);
    plan.frame_count = 1u;

    check_int_eq(tr_text_protocol_debug_execute(
                     &plan, &tr_text_protocol_executor_test_ops, &state),
                 TURBO_ENOSPC);
    check_int_eq(state.calls, 0);
  }

  it("rejects a message name that does not match the decoded frame") {
    uint8_t wire_frame[TR_RAFT_WIRE_MAX_FRAME_SIZE];
    char payload[TR_RAFT_WIRE_MAX_FRAME_SIZE * 2u + 3u];
    tr_text_protocol_debug_plan_t plan = {0};
    tr_text_protocol_executor_test_state_t state = {0};
    size_t frame_length = 0u;

    check_int_eq(tr_text_protocol_executor_make_raft_frame(
                     wire_frame, sizeof(wire_frame), &frame_length),
                 TURBO_OK);
    check_int_eq(tr_text_protocol_executor_hex_encode(
                     wire_frame, frame_length, payload, sizeof(payload)),
                 TURBO_OK);
    tr_text_protocol_executor_set_frame(
        &plan.frames[0], 3u, "raft", "append_request", payload);
    plan.frame_count = 1u;

    check_int_eq(tr_text_protocol_debug_execute(
                     &plan, &tr_text_protocol_executor_test_ops, &state),
                 TURBO_EPROTO);
    check_int_eq(state.calls, 0);
  }

  it("returns callback errors without continuing") {
    uint8_t wire_frame[TR_RAFT_WIRE_MAX_FRAME_SIZE];
    char payload[TR_RAFT_WIRE_MAX_FRAME_SIZE * 2u + 3u];
    tr_text_protocol_debug_plan_t plan = {0};
    tr_text_protocol_executor_test_state_t state = {
        .result = TURBO_EBUSY};
    size_t frame_length = 0u;

    check_int_eq(tr_text_protocol_executor_make_raft_frame(
                     wire_frame, sizeof(wire_frame), &frame_length),
                 TURBO_OK);
    check_int_eq(tr_text_protocol_executor_hex_encode(
                     wire_frame, frame_length, payload, sizeof(payload)),
                 TURBO_OK);
    tr_text_protocol_executor_set_frame(
        &plan.frames[0], 3u, "raft", "timeout_now", payload);
    plan.frame_count = 1u;

    check_int_eq(tr_text_protocol_debug_execute(
                     &plan, &tr_text_protocol_executor_test_ops, &state),
                 TURBO_EBUSY);
    check_int_eq(state.calls, 1);
  }
}
