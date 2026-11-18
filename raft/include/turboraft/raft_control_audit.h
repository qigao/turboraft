#ifndef TURBORAFT_RAFT_CONTROL_AUDIT_H
#define TURBORAFT_RAFT_CONTROL_AUDIT_H

#include <turboraft/raft_core.h>

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TR_RAFT_CONTROL_AUDIT_VERSION 1U
#define TR_RAFT_CONTROL_PRINCIPAL_FINGERPRINT_BYTES 32U

typedef enum tr_raft_control_audit_method {
    TR_RAFT_CONTROL_AUDIT_TICK = 1,
    TR_RAFT_CONTROL_AUDIT_PROPOSE = 2,
    TR_RAFT_CONTROL_AUDIT_READ_INDEX = 3,
    TR_RAFT_CONTROL_AUDIT_LEADER_TRANSFER = 4,
    TR_RAFT_CONTROL_AUDIT_CHANGE_MEMBERSHIP = 5,
    TR_RAFT_CONTROL_AUDIT_ADD_LEARNER = 6,
    TR_RAFT_CONTROL_AUDIT_PROMOTE = 7,
    TR_RAFT_CONTROL_AUDIT_REMOVE = 8,
    TR_RAFT_CONTROL_AUDIT_SNAPSHOT = 9,
    TR_RAFT_CONTROL_AUDIT_OPERATION_STATUS = 10
} tr_raft_control_audit_method_t;

typedef enum tr_raft_control_audit_phase {
    TR_RAFT_CONTROL_AUDIT_AUTHORIZATION = 1,
    TR_RAFT_CONTROL_AUDIT_ACCEPTANCE = 2,
    TR_RAFT_CONTROL_AUDIT_COMPLETION = 3
} tr_raft_control_audit_phase_t;

typedef struct tr_raft_control_audit_event {
    uint32_t version;
    uint32_t size;
    uint64_t sequence;
    uint32_t method;
    uint32_t phase;
    int32_t outcome;
    tr_raft_node_id_t local_node_id;
    tr_raft_node_id_t target_node_id;
    tr_raft_term_t term;
    tr_raft_index_t index;
    uint8_t principal_fingerprint[
        TR_RAFT_CONTROL_PRINCIPAL_FINGERPRINT_BYTES];
} tr_raft_control_audit_event_t;

typedef int (*tr_raft_control_audit_sink_fn)(
    void *context,
    const tr_raft_control_audit_event_t *event);

typedef struct tr_raft_control_audit_config {
    uint32_t version;
    uint32_t size;
    tr_raft_control_audit_sink_fn sink;
    void *context;
    uint8_t required;
    uint8_t reserved[7];
} tr_raft_control_audit_config_t;

#define TR_RAFT_CONTROL_AUDIT_CONFIG_INIT                                \
    {                                                                    \
        TR_RAFT_CONTROL_AUDIT_VERSION,                                   \
        (uint32_t)sizeof(tr_raft_control_audit_config_t), NULL, NULL, 0U, \
        {0U}                                                             \
    }

typedef struct tr_raft_control_audit tr_raft_control_audit_t;

int tr_raft_control_audit_create(
    const tr_raft_control_audit_config_t *config,
    tr_raft_control_audit_t **out_audit);

void tr_raft_control_audit_destroy(tr_raft_control_audit_t *audit);

int tr_raft_control_audit_emit(
    tr_raft_control_audit_t *audit,
    tr_raft_control_audit_event_t *event);

uint64_t tr_raft_control_audit_dropped(
    const tr_raft_control_audit_t *audit);

#ifdef __cplusplus
}
#endif

#endif
