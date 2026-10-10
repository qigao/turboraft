#ifndef TURBORAFT_RAFT_MULTICORE_CHUNK_RECEIVERS_H
#define TURBORAFT_RAFT_MULTICORE_CHUNK_RECEIVERS_H

#include <turboraft/raft_multicore.h>
#include <turboraft/raft_snapshot_receiver.h>
#include <turboraft/raft_data_stream.h>

#ifdef __cplusplus
extern "C" {
#endif

/* An owner-local composition of the EXISTING verified DATA/SNAPSHOT
 * receivers. The host selects the exact Group inside its Multicore
 * factory.receive_chunk callback; this helper never chooses another Owner,
 * retains runtime state, opens a socket, queues data or retries.
 *
 * Pointers are borrowed for this call and must be owner-thread confined.
 * The SnapshotReceiver or DataStreamReceiver owns its own transfer state.
 */
typedef struct tr_raft_multicore_chunk_receivers {
    tr_raft_snapshot_receiver_t *snapshot;
    tr_raft_data_stream_receiver_t *data;
} tr_raft_multicore_chunk_receivers_t;

/* Output is reset before use. On SALTS_OK an ACK is valid only when the
 * receiver accepted the chunk; a final positive durable_or_installed bit
 * comes solely from the receiver's actual completed install/commit.
 * An error never manufactures a success ACK. This helper does not send the
 * ACK: the host must correlate the Group completion to an authenticated,
 * still-live CNet generation and independently enqueue it.
 */
int tr_raft_multicore_chunk_receivers_handle(
    const tr_raft_multicore_chunk_receivers_t *receivers,
    const tr_raft_transport_payload_t *payload,
    tr_raft_multicore_chunk_result_t *out_result);

#ifdef __cplusplus
}
#endif
#endif /* TURBORAFT_RAFT_MULTICORE_CHUNK_RECEIVERS_H */
