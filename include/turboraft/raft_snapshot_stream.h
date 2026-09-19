#ifndef TURBORAFT_RAFT_SNAPSHOT_STREAM_H
#define TURBORAFT_RAFT_SNAPSHOT_STREAM_H

#include <turboraft/raft_wire_codec.h>

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef int (*tr_raft_snapshot_source_read_at_fn)(
    void *context,
    uint64_t offset,
    uint8_t *buffer,
    size_t capacity,
    size_t *out_size);

typedef void (*tr_raft_snapshot_source_release_fn)(void *context);

typedef struct tr_raft_snapshot_source {
    void *context;
    uint64_t size;
    uint8_t digest[TR_RAFT_WIRE_SNAPSHOT_DIGEST_SIZE];
    tr_raft_snapshot_source_read_at_fn read_at;
    tr_raft_snapshot_source_release_fn release;
} tr_raft_snapshot_source_t;

typedef int (*tr_raft_snapshot_stream_begin_fn)(
    void *context,
    tr_raft_term_t leader_term,
    tr_raft_index_t snapshot_index,
    tr_raft_term_t snapshot_term,
    const tr_raft_conf_t *configuration,
    uint64_t snapshot_size);

typedef int (*tr_raft_snapshot_stream_write_fn)(
    void *context,
    uint64_t offset,
    const uint8_t *data,
    size_t size);

typedef int (*tr_raft_snapshot_stream_finish_fn)(void *context);
typedef void (*tr_raft_snapshot_stream_abort_fn)(void *context);

typedef struct tr_raft_snapshot_stream_sink {
    tr_raft_snapshot_stream_begin_fn begin;
    tr_raft_snapshot_stream_write_fn write;
    /* Atomically makes the staged snapshot visible after digest validation. */
    tr_raft_snapshot_stream_finish_fn commit;
    tr_raft_snapshot_stream_abort_fn abort;
    void *context;
} tr_raft_snapshot_stream_sink_t;

#ifdef __cplusplus
}
#endif

#endif
