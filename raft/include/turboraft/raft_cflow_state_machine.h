#ifndef TURBORAFT_RAFT_CFLOW_STATE_MACHINE_H
#define TURBORAFT_RAFT_CFLOW_STATE_MACHINE_H

#include <turboraft/raft_apply_runtime.h>

#include <cflow/statechart_instance.h>

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TR_RAFT_CFLOW_STATE_MACHINE_CONFIG_ABI_V1 1U

typedef struct tr_raft_cflow_state_machine tr_raft_cflow_state_machine_t;

/**
 * Decodes one Runtime-owned committed entry into a bounded trivial CFlow Event.
 *
 * entry is borrowed only for this callback. out_event->payload may reference
 * context-owned scratch, but that storage must remain valid until the enclosing
 * try_apply call returns. On successful mailbox admission CFlow copies exactly
 * payload_type->size bytes before try_apply returns; neither the adapter nor
 * CFlow retains the entry or decoder scratch afterwards.
 *
 * Codecs representing variable-length commands must use a self-contained
 * bounded trivial Event representation (for example length + inline bytes) and
 * reject input that exceeds that declared bound.
 */
typedef int (*tr_raft_cflow_decode_entry_fn)(
    void *context,
    const tr_raft_entry_t *entry,
    cflow_event_view *out_event);

typedef struct tr_raft_cflow_state_machine_config_v1 {
    uint32_t abi_version;
    size_t struct_size;
    tr_raft_cflow_decode_entry_fn decode_entry;
    void *decode_context;
    /** Optional host transaction forwarded with host_context as its user. */
    cflow_statechart_host_transaction_fn host_transaction;
    void *host_context;
} tr_raft_cflow_state_machine_config_v1_t;

/** Creates a one-entry bounded settlement bridge. */
int tr_raft_cflow_state_machine_create(
    const tr_raft_cflow_state_machine_config_v1_t *config,
    tr_raft_cflow_state_machine_t **out_state_machine);

/**
 * Produces the V5 hooks and user pointer required by Statechart init.
 *
 * The adapter and callback user must outlive the bound instance. The instance
 * must reserve all nonzero tagged external Events for this Raft adapter; token
 * zero remains available for unobserved application traffic.
 */
int tr_raft_cflow_state_machine_hooks(
    tr_raft_cflow_state_machine_t *state_machine,
    cflow_statechart_instance_hooks *out_hooks,
    void **out_hook_user);

/** Binds one initialized, dedicated Statechart instance. */
int tr_raft_cflow_state_machine_bind(
    tr_raft_cflow_state_machine_t *state_machine,
    cflow_statechart_instance *instance);

/** Copies the asynchronous Raft entry SPI after a successful bind. */
int tr_raft_cflow_state_machine_get_spi(
    tr_raft_cflow_state_machine_t *state_machine,
    tr_raft_entry_state_machine_v1_t *out_spi);

/**
 * Clears the binding after cflow_statechart_instance_destroy() has succeeded.
 * Pending or undelivered settlement returns SALTS_EBUSY.
 */
int tr_raft_cflow_state_machine_unbind(
    tr_raft_cflow_state_machine_t *state_machine,
    const cflow_statechart_instance *instance);

/** Destroys an unbound adapter; a live binding returns SALTS_EBUSY. */
int tr_raft_cflow_state_machine_destroy(
    tr_raft_cflow_state_machine_t *state_machine);

#ifdef __cplusplus
}
#endif

#endif
