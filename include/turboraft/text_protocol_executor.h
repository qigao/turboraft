#ifndef TURBORAFT_TEXT_PROTOCOL_EXECUTOR_H
#define TURBORAFT_TEXT_PROTOCOL_EXECUTOR_H

#include <turboraft/raft_wire_codec.h>
#include <turboraft/text_syntax.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  uint16_t version;
  tr_raft_wire_payload_kind_t payload_kind;
  tr_raft_wire_metadata_t metadata;
  union {
    tr_raft_message_t raft;
    tr_raft_snapshot_chunk_t snapshot_chunk;
    tr_raft_snapshot_ack_t snapshot_ack;
  } payload;
} tr_text_protocol_debug_decoded_frame_t;

/*
 * The plan frame and decoded frame are borrowed for the duration of the
 * callback. The callback must copy any data it needs after returning.
 */
typedef int (*tr_text_protocol_debug_frame_fn)(
    void *context,
    const tr_text_protocol_frame_t *frame,
    const tr_text_protocol_debug_decoded_frame_t *decoded);

typedef struct {
  tr_text_protocol_debug_frame_fn frame;
} tr_text_protocol_debug_executor_ops_t;

/*
 * Decodes and validates each protocol-debug frame in plan order.
 *
 * The payload field must contain one complete wire frame encoded as an even
 * hexadecimal literal. The executor validates the envelope, requested kind,
 * version, endpoints, term, and message name before invoking the callback.
 * The callback receives a typed view valid only until it returns. The first
 * callback error is returned unchanged and stops execution.
 */
int tr_text_protocol_debug_execute(
    const tr_text_protocol_debug_plan_t *plan,
    const tr_text_protocol_debug_executor_ops_t *ops,
    void *context);

#ifdef __cplusplus
}
#endif

#endif
