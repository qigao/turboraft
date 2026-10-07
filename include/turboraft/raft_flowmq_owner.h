#ifndef TURBORAFT_RAFT_FLOWMQ_OWNER_H
#define TURBORAFT_RAFT_FLOWMQ_OWNER_H

#include <turboraft/raft_multicore.h>
#include <turboraft/raft_flowmq_peer_service.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct tr_raft_flowmq_owner tr_raft_flowmq_owner_t;

/**
 * Owner-callback-only adapter for one independent FlowMQ peer-service.
 * Copies group IDs and binds/starts the existing peer-service. Peer handshake,
 * TLS and identity validation are unchanged. config.on_payload receives only
 * non-Raft payloads, with its original context and borrowed-payload lifetime.
 * Unknown/misassigned groups return ENOENT before reaching a Service.
 * There is no implicit handshake, network worker or FlowMQ owner-lane bind.
 */
int tr_raft_flowmq_owner_create(tr_raft_owner_t *owner,
                                const tr_raft_flowmq_peer_service_config_t *config,
                                const uint64_t *group_ids, size_t group_count,
                                tr_raft_flowmq_owner_t **out_link);
/* Installs enqueue/context only; preserves the host's snapshot adapter. */
int tr_raft_flowmq_owner_bind(tr_raft_flowmq_owner_t *link, uint64_t group_id,
                              tr_raft_transport_t *transport);
int tr_raft_flowmq_owner_poll(tr_raft_flowmq_owner_t *link);
/* Call in owner_close, AFTER all Services and group resources are destroyed. */
int tr_raft_flowmq_owner_destroy(tr_raft_flowmq_owner_t *link);
/* Borrowed owner-thread-only peer-service for snapshot/data and diagnostics. */
tr_raft_flowmq_peer_service_t *tr_raft_flowmq_owner_peer_service(tr_raft_flowmq_owner_t *link);

#ifdef __cplusplus
}
#endif
#endif
