#ifndef TURBORAFT_RAFT_COMPONENT_H
#define TURBORAFT_RAFT_COMPONENT_H

#include <turboraft/raft_runtime.h>
#include <cmeta/interface.h>

/*
 * Declarative, setup-only CMeta interfaces over existing Raft callback tables.
 * snapshot() copies an adapter record, NOT its borrowed callback contexts.
 * All three contexts must outlive the Component domain and its Runtime.
 * Once bound, Raft's hot path calls the original callbacks directly.
 */
#define TR_RAFT_STORAGE_SOURCE_METHODS(X, I) \
    X(I, R1, int, snapshot, tr_raft_storage_t *, out)
#define TR_RAFT_TRANSPORT_SOURCE_METHODS(X, I) \
    X(I, R1, int, snapshot, tr_raft_transport_t *, out)
#define TR_RAFT_STATE_MACHINE_SOURCE_METHODS(X, I) \
    X(I, R1, int, snapshot, tr_raft_state_machine_t *, out)

CMETA_INTERFACE(tr_raft_storage_source, TR_RAFT_STORAGE_SOURCE_METHODS);
CMETA_INTERFACE(tr_raft_transport_source, TR_RAFT_TRANSPORT_SOURCE_METHODS);
CMETA_INTERFACE(tr_raft_state_machine_source, TR_RAFT_STATE_MACHINE_SOURCE_METHODS);

#ifdef __cplusplus
extern "C" {
#endif

typedef struct tr_raft_component_domain tr_raft_component_domain_t;

/*
 * Create and activate one bounded four-Component dependency graph:
 *
 * Storage ---+
 * Transport -+--> Runtime (borrows one external Core)
 * Apply -----+
 *
 * Salts::Component, not TurboRaft, owns graph ordering and ObjectRef
 * rollback. Domain owns the Runtime only; Core and callback contexts
 * remain owned by their caller. No network threads, WAL writer, sockets,
 * scheduler or dynamic Plugin loader are created here.
 *
 * The domain is single-owner and address-stable. On error, *out_domain is
 * NULL and no borrowed callback survives. Invalid/incomplete callbacks are
 * rejected before Component startup.
 */
int tr_raft_component_domain_create(
    const tr_raft_runtime_config_t *config,
    tr_raft_component_domain_t **out_domain);

/* Borrowed only while the domain remains active and on its owner thread. */
tr_raft_runtime_t *tr_raft_component_domain_runtime(
    tr_raft_component_domain_t *domain);

/* Explicit, dependency-ordered stop destroys Runtime before its adapters. */
int tr_raft_component_domain_stop(tr_raft_component_domain_t *domain);

/* Requires successful stop; active domains return SALTS_EBUSY unchanged. */
int tr_raft_component_domain_destroy(tr_raft_component_domain_t *domain);

#ifdef __cplusplus
}
#endif

#endif /* TURBORAFT_RAFT_COMPONENT_H */
