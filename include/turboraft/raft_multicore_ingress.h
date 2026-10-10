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
 * SNAPSHOT/DATA payload kinds (including borrowed chunk pointers) are
 * explicitly ENOTSUP: they require a distinct bounded ownership/SG lease
 * protocol, never a shallow copy into the Raft request ring.
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

/* Thread-safe from CNet producers; *out_request_id=0 on every failure.
 * Completion is retrieved only from the EXISTING Multicore group ring.
 */
int tr_raft_multicore_ingress_submit(
    tr_raft_multicore_ingress_t *ingress,
    const tr_raft_transport_payload_t *payload,
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
