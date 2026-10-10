#ifndef TURBORAFT_RAFT_MULTICORE_INGRESS_H
#define TURBORAFT_RAFT_MULTICORE_INGRESS_H

#include <turboraft/raft_multicore.h>
#include <turboraft/raft_transport.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Transport-neutral ACE Half-Sync / Active Object ingress to the EXISTING
 * Multicore bounded owner mailbox. No CNet dependency, second queue,
 * executor, thread, retry engine or transport registry is introduced.
 *
 * The caller first authenticates the peer and Group on its CNet progress
 * Owner (e.g. tr_raft_cnet_peer_directory_receive), then installs
 * tr_raft_multicore_ingress_receive as that directory's on_payload.
 * This adapter is NOT an authentication gateway by itself.
 *
 * Each accepted Raft frame copies its entire inline tr_raft_message_t
 * into exactly one TR_RAFT_MULTICORE_STEP request, addressed by Group.
 * Multicore owns the copy and reserves one completion slot BEFORE return.
 * The host MUST drain tr_raft_multicore_take() for every accepted request,
 * including ECANCELED completions after stop. Successful local admission
 * does NOT imply transport delivery, Raft commit, WAL fsync or apply.
 *
 * SNAPSHOT/DATA chunk payloads require an explicit per-Group byte budget
 * and receive_chunk Owner callback configured at Multicore creation.
 * Without both they return ENOTSUP (never shallow-copy borrowed pointers).
 * The existing Multicore ring reserves item AND byte credits before
 * materializing the borrowed chunk into one canonical Salts Core buffer.
 * CNet may release/reuse the original view immediately after acceptance.
 * The exact Group Owner invokes receive_chunk, then returns byte credits
 * and releases the lease BEFORE publishing one completion. Completion slot
 * remains outstanding until host take(); stop produces ECANCELED without
 * invoking the receiver callback, releasing the buffer exactly once.
 *
 * An optional authenticated reply origin includes a host-issued, nonzero
 * module-generation epoch. The host MUST issue it outside any reloadable
 * provider and never recycle it while previous in-memory completions can
 * exist. The Group Owner only copies the opaque value; the final CNet owner
 * independently checks that exact epoch + Channel instance + connection
 * token before a manual ACK send. A missing epoch fails closed.
 *
 * The callback may populate a typed SNAPSHOT_ACK or DATA_ACK completion
 * only when its receiver has actually established those semantics.
 * In particular a transport copy or local callback return is NOT fsync,
 * durable data settlement, remote delivery, or a network ACK. The host
 * explicitly sends any verified ACK on its owning CNet connection.
 *
 * Request IDs are monotonically unique within ONE ingress; the host must
 * allocate non-overlapping ID namespaces if other producers submit to the
 * same Multicore Runtime. A rejected request has no completion and transfers
 * no ownership. ENOSPC is an explicit full ring (no implicit retry).
 *
 * One address-stable ingress may be called by multiple producer threads.
 * It borrows Runtime and must outlive all callback invocations, then be
 * destroyed only AFTER CNet producers are quiesced and BEFORE Runtime
 * is destroyed. Destroy never implicitly stops/drains the Runtime.
 *
 * REQUIRED terminal host protocol (not an ingress-owned synchronization
 * mechanism):
 *  1. Prevent any new Channel/Directory callback from borrowing context.
 *  2. Drive each CNet Channel through its terminal owner callback and
 *     complete/join the CNet progress Owner(s). A mere stop request or
 *     socket close is not proof that on_payload has returned.
 *  3. Only after every in-flight on_payload has returned, request Multicore
 *     stop; settle/take every already accepted Group completion and join the
 *     Raft Group Owners.
 *  4. Destroy ingress before destroying the borrowed Multicore Runtime.
 * Callback context, directory and TLS identities must remain alive until
 * the end of step 2. Concurrent destroy/receive is undefined and must NOT
 * be tested by invoking use-after-free; establish a producer join barrier.
 * Future generation fencing/terminal ack must be a separately verified
 * host-level protocol, not a second ingress scheduler or queue.
 */
typedef struct tr_raft_multicore_ingress tr_raft_multicore_ingress_t;

/* first_request_id must be nonzero. No native socket or scheduler is opened. */
int tr_raft_multicore_ingress_create(
    tr_raft_multicore_t *runtime, uint64_t first_request_id,
    tr_raft_multicore_ingress_t **out_ingress);

/* Thread-safe from authenticated CNet producers; *out_request_id=0 on
 * every failure. No fallback to borrowed pointer storage or retry.
 * Completion (including any explicit receiver-generated ACK) is retrieved
 * only from the EXISTING Multicore group ring.
 */
int tr_raft_multicore_ingress_submit(
    tr_raft_multicore_ingress_t *ingress,
    const tr_raft_transport_payload_t *payload,
    uint64_t *out_request_id);

/* Explicit authenticated reply-origin variant for DATA/SNAPSHOT only.
 * The CNet final Owner must first verify the channel's TLS+HELLO identity,
 * capture the exact immutable Channel generation and pass this ticket from
 * its owner thread. The ticket is carried by VALUE through the same Group
 * request/completion ring; no CNet pointer, raw native handle or additional
 * routing queue is retained. A rejected request returns ID zero.
 *
 * A future ACK can be sent ONLY if the CNet owner checks this saved ticket
 * against the same still-active Channel. Zero/ticketless completions must
 * never automatically route a network ACK. This API does not send ACKs.
 */
int tr_raft_multicore_ingress_submit_with_origin(
    tr_raft_multicore_ingress_t *ingress,
    const tr_raft_transport_payload_t *payload,
    const tr_raft_transport_reply_origin_t *origin,
    uint64_t *out_request_id);

/* Compatible with tr_raft_transport_payload_handler_fn and CNet Directory's
 * borrowed on_payload/context pair. Rejects exactly as submit() does.
 */
int tr_raft_multicore_ingress_receive(
    void *ingress_context, const tr_raft_transport_payload_t *payload);

/* No concurrent callbacks, host has stopped borrowing ingress. */
int tr_raft_multicore_ingress_destroy(
    tr_raft_multicore_ingress_t *ingress);

#ifdef __cplusplus
}
#endif

#endif /* TURBORAFT_RAFT_MULTICORE_INGRESS_H */
