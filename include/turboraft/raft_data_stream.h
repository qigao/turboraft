#ifndef TURBORAFT_RAFT_DATA_STREAM_H
#define TURBORAFT_RAFT_DATA_STREAM_H

#include <turboraft/raft_wire_codec.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TR_RAFT_DATA_STREAM_RECOMMENDED_INFLIGHT_CHUNKS 4U
#define TR_RAFT_DATA_STREAM_MAX_INFLIGHT_CHUNKS 16U
#define TR_RAFT_DATA_DESCRIPTOR_VERSION 1U
#define TR_RAFT_DATA_DESCRIPTOR_ENCODED_SIZE 56U

typedef struct tr_raft_data_descriptor {
    uint64_t stream_id;
    uint64_t stream_size;
    uint8_t stream_digest[TR_RAFT_WIRE_DATA_DIGEST_SIZE];
} tr_raft_data_descriptor_t;

int tr_raft_data_descriptor_encode(
    const tr_raft_data_descriptor_t *descriptor,
    uint8_t *output,
    size_t output_capacity,
    size_t *output_size);
int tr_raft_data_descriptor_decode(
    const uint8_t *input,
    size_t input_size,
    tr_raft_data_descriptor_t *descriptor);

typedef struct tr_raft_data_quorum tr_raft_data_quorum_t;

typedef struct tr_raft_data_quorum_config {
    tr_raft_node_id_t self_id;
    tr_raft_term_t term;
    tr_raft_conf_t configuration;
    tr_raft_data_descriptor_t descriptor;
} tr_raft_data_quorum_config_t;

int tr_raft_data_quorum_create(
    const tr_raft_data_quorum_config_t *config,
    tr_raft_data_quorum_t **out_quorum);
void tr_raft_data_quorum_destroy(tr_raft_data_quorum_t *quorum);
/** Marks the local staging object durable after its sink commit succeeds. */
int tr_raft_data_quorum_mark_local_durable(tr_raft_data_quorum_t *quorum);
int tr_raft_data_quorum_acknowledge(
    tr_raft_data_quorum_t *quorum,
    const tr_raft_data_ack_t *ack);
bool tr_raft_data_quorum_ready(const tr_raft_data_quorum_t *quorum);
/** Builds a proposal whose data view borrows descriptor_storage. */
int tr_raft_data_quorum_make_proposal(
    const tr_raft_data_quorum_t *quorum,
    uint64_t command_id,
    uint8_t descriptor_storage[TR_RAFT_DATA_DESCRIPTOR_ENCODED_SIZE],
    tr_raft_proposal_t *out_proposal);

typedef struct tr_raft_data_stream_sender tr_raft_data_stream_sender_t;
typedef struct tr_raft_data_stream_receiver tr_raft_data_stream_receiver_t;

typedef int (*tr_raft_data_stream_source_read_at_fn)(
    void *context,
    uint64_t offset,
    uint8_t *buffer,
    size_t capacity,
    size_t *out_size);
typedef void (*tr_raft_data_stream_source_release_fn)(void *context);

typedef struct tr_raft_data_stream_source {
    void *context;
    uint64_t size;
    uint8_t digest[TR_RAFT_WIRE_DATA_DIGEST_SIZE];
    tr_raft_data_stream_source_read_at_fn read_at;
    tr_raft_data_stream_source_release_fn release;
} tr_raft_data_stream_source_t;

typedef struct tr_raft_data_stream_sender_config {
    tr_raft_node_id_t self_id;
    tr_raft_node_id_t peer_id;
    uint64_t max_stream_bytes;
    size_t chunk_size;
    size_t max_inflight_chunks;
} tr_raft_data_stream_sender_config_t;

typedef struct tr_raft_data_stream_sender_status {
    bool active;
    bool complete;
    uint64_t stream_id;
    uint64_t stream_size;
    uint64_t acknowledged_offset;
    uint64_t next_offset;
    size_t inflight_chunks;
} tr_raft_data_stream_sender_status_t;

int tr_raft_data_stream_sender_create(
    const tr_raft_data_stream_sender_config_t *config,
    tr_raft_data_stream_sender_t **out_sender);
void tr_raft_data_stream_sender_destroy(tr_raft_data_stream_sender_t *sender);
void tr_raft_data_stream_sender_reset(tr_raft_data_stream_sender_t *sender);

/** Small-object compatibility helper; copies the complete payload. */
int tr_raft_data_stream_sender_begin(
    tr_raft_data_stream_sender_t *sender,
    tr_raft_term_t term,
    uint64_t stream_id,
    const uint8_t *data,
    size_t size);
/**
 * Starts a database-scale transfer and takes ownership of source on success.
 * read_at is called only with bounded chunk-sized buffers.
 */
int tr_raft_data_stream_sender_begin_source(
    tr_raft_data_stream_sender_t *sender,
    tr_raft_term_t term,
    uint64_t stream_id,
    const tr_raft_data_stream_source_t *source);
int tr_raft_data_stream_sender_next(
    tr_raft_data_stream_sender_t *sender,
    tr_raft_data_chunk_t *out_chunk);
int tr_raft_data_stream_sender_cancel(
    tr_raft_data_stream_sender_t *sender,
    uint64_t stream_offset);
int tr_raft_data_stream_sender_resume(tr_raft_data_stream_sender_t *sender);
int tr_raft_data_stream_sender_acknowledge(
    tr_raft_data_stream_sender_t *sender,
    const tr_raft_data_ack_t *ack);
int tr_raft_data_stream_sender_get_status(
    const tr_raft_data_stream_sender_t *sender,
    tr_raft_data_stream_sender_status_t *out_status);

typedef int (*tr_raft_data_stream_begin_fn)(
    void *context,
    tr_raft_node_id_t leader_id,
    tr_raft_term_t term,
    uint64_t stream_id,
    uint64_t stream_size,
    const uint8_t digest[TR_RAFT_WIRE_DATA_DIGEST_SIZE]);
typedef int (*tr_raft_data_stream_write_fn)(
    void *context, uint64_t offset, const uint8_t *data, size_t size);
typedef int (*tr_raft_data_stream_commit_fn)(void *context);
typedef void (*tr_raft_data_stream_abort_fn)(void *context);

typedef struct tr_raft_data_stream_sink {
    tr_raft_data_stream_begin_fn begin;
    tr_raft_data_stream_write_fn write;
    tr_raft_data_stream_commit_fn commit;
    tr_raft_data_stream_abort_fn abort;
    void *context;
} tr_raft_data_stream_sink_t;

typedef struct tr_raft_data_stream_receiver_config {
    tr_raft_node_id_t self_id;
    uint64_t max_stream_bytes;
    tr_raft_data_stream_sink_t sink;
} tr_raft_data_stream_receiver_config_t;

typedef struct tr_raft_data_stream_receive_result {
    tr_raft_data_ack_t ack;
    bool committed;
} tr_raft_data_stream_receive_result_t;

int tr_raft_data_stream_receiver_create(
    const tr_raft_data_stream_receiver_config_t *config,
    tr_raft_data_stream_receiver_t **out_receiver);
void tr_raft_data_stream_receiver_destroy(
    tr_raft_data_stream_receiver_t *receiver);
void tr_raft_data_stream_receiver_reset(
    tr_raft_data_stream_receiver_t *receiver);
int tr_raft_data_stream_receiver_handle(
    tr_raft_data_stream_receiver_t *receiver,
    const tr_raft_data_chunk_t *chunk,
    tr_raft_data_stream_receive_result_t *out_result);

#ifdef __cplusplus
}
#endif

#endif
