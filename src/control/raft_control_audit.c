#include <turboraft/raft_control_audit.h>

#include <salts_error.h>

#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

struct tr_raft_control_audit {
    tr_raft_control_audit_sink_fn sink;
    void *context;
    bool required;
    atomic_uint_fast64_t next_sequence;
    atomic_uint_fast64_t dropped_events;
};

static bool tr_control_audit_method_valid(
    uint32_t method)
{
    return method >= TR_RAFT_CONTROL_AUDIT_TICK &&
           method <= TR_RAFT_CONTROL_AUDIT_OPERATION_STATUS;
}

static bool tr_control_audit_phase_valid(
    uint32_t phase)
{
    return phase >= TR_RAFT_CONTROL_AUDIT_AUTHORIZATION &&
           phase <= TR_RAFT_CONTROL_AUDIT_COMPLETION;
}

int tr_raft_control_audit_create(
    const tr_raft_control_audit_config_t *config,
    tr_raft_control_audit_t **out_audit)
{
    static const uint8_t zero_reserved[7] = {0U};
    tr_raft_control_audit_t *audit;

    if (config == NULL || out_audit == NULL ||
        config->version != TR_RAFT_CONTROL_AUDIT_VERSION ||
        config->size < sizeof(*config) || config->required > 1U ||
        memcmp(config->reserved, zero_reserved,
               sizeof(config->reserved)) != 0 ||
        (config->required && config->sink == NULL)) {
        return SALTS_EINVAL;
    }

    *out_audit = NULL;
    audit = (tr_raft_control_audit_t *)calloc(1U, sizeof(*audit));
    if (audit == NULL) {
        return SALTS_ENOMEM;
    }
    audit->sink = config->sink;
    audit->context = config->context;
    audit->required = config->required != 0U;
    atomic_init(&audit->next_sequence, 1U);
    atomic_init(&audit->dropped_events, 0U);
    *out_audit = audit;
    return SALTS_OK;
}

void tr_raft_control_audit_destroy(tr_raft_control_audit_t *audit)
{
    free(audit);
}

int tr_raft_control_audit_emit(
    tr_raft_control_audit_t *audit,
    tr_raft_control_audit_event_t *event)
{
    int result;

    if (audit == NULL || event == NULL ||
        !tr_control_audit_method_valid(event->method) ||
        !tr_control_audit_phase_valid(event->phase)) {
        return SALTS_EINVAL;
    }

    event->version = TR_RAFT_CONTROL_AUDIT_VERSION;
    event->size = (uint32_t)sizeof(*event);
    event->sequence = atomic_fetch_add_explicit(
        &audit->next_sequence, 1U, memory_order_relaxed);
    if (audit->sink == NULL) {
        atomic_fetch_add_explicit(&audit->dropped_events, 1U,
                                  memory_order_relaxed);
        return SALTS_OK;
    }

    result = audit->sink(audit->context, event);
    if (result == SALTS_OK || audit->required) {
        return result;
    }
    atomic_fetch_add_explicit(&audit->dropped_events, 1U,
                              memory_order_relaxed);
    return SALTS_OK;
}

uint64_t tr_raft_control_audit_dropped(
    const tr_raft_control_audit_t *audit)
{
    if (audit == NULL) {
        return 0U;
    }
    return (uint64_t)atomic_load_explicit(&audit->dropped_events,
                                          memory_order_relaxed);
}
