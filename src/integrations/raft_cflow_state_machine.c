#include <turboraft/raft_cflow_state_machine.h>

#include <salts/thread.h>
#include <salts_error.h>

#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

struct tr_raft_cflow_state_machine {
    tr_raft_cflow_decode_entry_fn decode_entry;
    void *decode_context;
    cflow_statechart_host_transaction_fn host_transaction;
    void *host_context;
    cflow_statechart_instance *instance;
    salts_mutex_t mutex;
    tr_raft_apply_settlement_t settlement;
    uint64_t expected_token;
    bool in_flight;
    bool settlement_ready;
    bool protocol_fault;
};

static cflow_statechart_host_result tr_raft_cflow_on_host_transaction(
    void *user,
    cflow_statechart_host_context *context,
    const char **out_error)
{
    tr_raft_cflow_state_machine_t *state_machine =
        (tr_raft_cflow_state_machine_t *) user;

    if (state_machine == NULL || state_machine->host_transaction == NULL) {
        if (out_error != NULL) {
            *out_error = "missing CFlow host transaction";
        }
        return CFLOW_STATECHART_HOST_FATAL;
    }
    return state_machine->host_transaction(state_machine->host_context,
                                           context, out_error);
}

static void tr_raft_cflow_on_settlement(
    void *user,
    const cflow_statechart_external_settlement *settlement)
{
    tr_raft_cflow_state_machine_t *state_machine =
        (tr_raft_cflow_state_machine_t *) user;

    if (state_machine == NULL || settlement == NULL) {
        return;
    }
    salts_mutex_lock(&state_machine->mutex);
    if (!state_machine->in_flight || state_machine->settlement_ready ||
        settlement->origin_token != state_machine->expected_token) {
        state_machine->protocol_fault = true;
        salts_mutex_unlock(&state_machine->mutex);
        return;
    }
    state_machine->settlement.token = settlement->origin_token;
    if (settlement->kind == CFLOW_STATECHART_EXTERNAL_SETTLED_COMPLETED) {
        state_machine->settlement.outcome = TR_RAFT_APPLY_OUTCOME_APPLIED;
        state_machine->settlement.cause = SALTS_OK;
    } else if (settlement->kind ==
               CFLOW_STATECHART_EXTERNAL_SETTLED_DROPPED) {
        state_machine->settlement.outcome = TR_RAFT_APPLY_OUTCOME_PENDING;
        state_machine->settlement.cause = SALTS_OK;
    } else if (settlement->kind ==
               CFLOW_STATECHART_EXTERNAL_SETTLED_CANCELLED) {
        state_machine->settlement.outcome = TR_RAFT_APPLY_OUTCOME_UNKNOWN;
        state_machine->settlement.cause = SALTS_ECANCELED;
    } else if (settlement->kind ==
               CFLOW_STATECHART_EXTERNAL_SETTLED_FAILED) {
        state_machine->settlement.outcome = TR_RAFT_APPLY_OUTCOME_UNKNOWN;
        state_machine->settlement.cause = SALTS_EIO;
    } else {
        state_machine->settlement.outcome = TR_RAFT_APPLY_OUTCOME_UNKNOWN;
        state_machine->settlement.cause = SALTS_EPROTO;
    }
    state_machine->settlement_ready = true;
    salts_mutex_unlock(&state_machine->mutex);
}

static tr_raft_apply_admission_t tr_raft_cflow_try_apply(
    void *context,
    const tr_raft_entry_t *entry,
    uint64_t token,
    int *out_cause)
{
    tr_raft_cflow_state_machine_t *state_machine =
        (tr_raft_cflow_state_machine_t *) context;
    cflow_statechart_instance *instance;
    cflow_event_view event;
    cflow_mailbox_status status;
    int decode_result;

    if (state_machine == NULL || entry == NULL || token == 0U ||
        out_cause == NULL) {
        if (out_cause != NULL) {
            *out_cause = SALTS_EINVAL;
        }
        return TR_RAFT_APPLY_ADMISSION_FAILED;
    }
    *out_cause = SALTS_OK;
    salts_mutex_lock(&state_machine->mutex);
    if (state_machine->instance == NULL ||
        state_machine->instance->impl == NULL) {
        salts_mutex_unlock(&state_machine->mutex);
        *out_cause = SALTS_ESHUTDOWN;
        return TR_RAFT_APPLY_ADMISSION_CLOSED;
    }
    if (state_machine->protocol_fault || state_machine->in_flight ||
        state_machine->settlement_ready) {
        salts_mutex_unlock(&state_machine->mutex);
        *out_cause = SALTS_EPROTO;
        return TR_RAFT_APPLY_ADMISSION_FAILED;
    }
    salts_mutex_unlock(&state_machine->mutex);

    memset(&event, 0, sizeof(event));
    decode_result = state_machine->decode_entry(
        state_machine->decode_context, entry, &event);
    if (decode_result != SALTS_OK || event.id == 0U ||
        event.payload_type == NULL || event.payload == NULL) {
        *out_cause = decode_result == SALTS_OK ? SALTS_EINVAL
                                               : decode_result;
        return TR_RAFT_APPLY_ADMISSION_FAILED;
    }

    salts_mutex_lock(&state_machine->mutex);
    if (state_machine->instance == NULL ||
        state_machine->instance->impl == NULL) {
        salts_mutex_unlock(&state_machine->mutex);
        *out_cause = SALTS_ESHUTDOWN;
        return TR_RAFT_APPLY_ADMISSION_CLOSED;
    }
    if (state_machine->protocol_fault || state_machine->in_flight ||
        state_machine->settlement_ready) {
        salts_mutex_unlock(&state_machine->mutex);
        *out_cause = SALTS_EPROTO;
        return TR_RAFT_APPLY_ADMISSION_FAILED;
    }
    state_machine->expected_token = token;
    state_machine->in_flight = true;
    instance = state_machine->instance;
    salts_mutex_unlock(&state_machine->mutex);

    status = cflow_statechart_instance_try_send_tagged(instance, &event,
                                                       token);
    if (status == CFLOW_MAILBOX_OK) {
        return TR_RAFT_APPLY_ADMISSION_ACCEPTED;
    }

    salts_mutex_lock(&state_machine->mutex);
    if (state_machine->settlement_ready) {
        state_machine->protocol_fault = true;
    } else {
        state_machine->expected_token = 0U;
        state_machine->in_flight = false;
    }
    salts_mutex_unlock(&state_machine->mutex);
    if (status == CFLOW_MAILBOX_FULL) {
        *out_cause = SALTS_ENOBUFS;
        return TR_RAFT_APPLY_ADMISSION_FULL;
    }
    if (status == CFLOW_MAILBOX_CLOSED) {
        *out_cause = SALTS_ESHUTDOWN;
        return TR_RAFT_APPLY_ADMISSION_CLOSED;
    }
    if (status == CFLOW_MAILBOX_CANCELLED) {
        *out_cause = SALTS_ECANCELED;
        return TR_RAFT_APPLY_ADMISSION_CLOSED;
    }
    *out_cause = status == CFLOW_MAILBOX_ALLOCATION_FAILED
                     ? SALTS_ENOMEM
                     : SALTS_EINVAL;
    return TR_RAFT_APPLY_ADMISSION_FAILED;
}

static int tr_raft_cflow_poll_settlement(
    void *context,
    tr_raft_apply_settlement_t *out_settlement,
    bool *out_ready)
{
    tr_raft_cflow_state_machine_t *state_machine =
        (tr_raft_cflow_state_machine_t *) context;

    if (state_machine == NULL || out_settlement == NULL ||
        out_ready == NULL) {
        return SALTS_EINVAL;
    }
    *out_ready = false;
    salts_mutex_lock(&state_machine->mutex);
    if (state_machine->protocol_fault) {
        salts_mutex_unlock(&state_machine->mutex);
        return SALTS_EPROTO;
    }
    if (state_machine->settlement_ready) {
        *out_settlement = state_machine->settlement;
        state_machine->settlement_ready = false;
        state_machine->in_flight = false;
        state_machine->expected_token = 0U;
        memset(&state_machine->settlement, 0,
               sizeof(state_machine->settlement));
        *out_ready = true;
    }
    salts_mutex_unlock(&state_machine->mutex);
    return SALTS_OK;
}

int tr_raft_cflow_state_machine_create(
    const tr_raft_cflow_state_machine_config_v1_t *config,
    tr_raft_cflow_state_machine_t **out_state_machine)
{
    tr_raft_cflow_state_machine_t *state_machine;

    if (config == NULL || out_state_machine == NULL) {
        return SALTS_EINVAL;
    }
    *out_state_machine = NULL;
    if (config->abi_version !=
            TR_RAFT_CFLOW_STATE_MACHINE_CONFIG_ABI_V1 ||
        config->struct_size < sizeof(*config) ||
        config->decode_entry == NULL) {
        return SALTS_EINVAL;
    }
    state_machine = (tr_raft_cflow_state_machine_t *) calloc(
        1U, sizeof(*state_machine));
    if (state_machine == NULL) {
        return SALTS_ENOMEM;
    }
    salts_mutex_init(&state_machine->mutex);
    state_machine->decode_entry = config->decode_entry;
    state_machine->decode_context = config->decode_context;
    state_machine->host_transaction = config->host_transaction;
    state_machine->host_context = config->host_context;
    *out_state_machine = state_machine;
    return SALTS_OK;
}

int tr_raft_cflow_state_machine_hooks(
    tr_raft_cflow_state_machine_t *state_machine,
    cflow_statechart_instance_hooks *out_hooks,
    void **out_hook_user)
{
    if (state_machine == NULL || out_hooks == NULL ||
        out_hook_user == NULL) {
        return SALTS_EINVAL;
    }
    memset(out_hooks, 0, sizeof(*out_hooks));
    out_hooks->abi_version = CFLOW_STATECHART_INSTANCE_HOOKS_ABI_V5;
    out_hooks->struct_size = sizeof(*out_hooks);
    if (state_machine->host_transaction != NULL) {
        out_hooks->on_host_transaction =
            tr_raft_cflow_on_host_transaction;
    }
    out_hooks->on_external_settlement = tr_raft_cflow_on_settlement;
    *out_hook_user = state_machine;
    return SALTS_OK;
}

int tr_raft_cflow_state_machine_bind(
    tr_raft_cflow_state_machine_t *state_machine,
    cflow_statechart_instance *instance)
{
    int result = SALTS_OK;

    if (state_machine == NULL || instance == NULL || instance->impl == NULL) {
        return SALTS_EINVAL;
    }
    salts_mutex_lock(&state_machine->mutex);
    if (state_machine->instance != NULL) {
        result = SALTS_EALREADY;
    } else if (state_machine->in_flight ||
               state_machine->settlement_ready ||
               state_machine->protocol_fault) {
        result = SALTS_EPROTO;
    } else {
        state_machine->instance = instance;
    }
    salts_mutex_unlock(&state_machine->mutex);
    return result;
}

int tr_raft_cflow_state_machine_get_spi(
    tr_raft_cflow_state_machine_t *state_machine,
    tr_raft_entry_state_machine_v1_t *out_spi)
{
    if (state_machine == NULL || out_spi == NULL) {
        return SALTS_EINVAL;
    }
    salts_mutex_lock(&state_machine->mutex);
    if (state_machine->instance == NULL ||
        state_machine->instance->impl == NULL) {
        salts_mutex_unlock(&state_machine->mutex);
        return SALTS_EPROTO;
    }
    memset(out_spi, 0, sizeof(*out_spi));
    out_spi->abi_version = TR_RAFT_ENTRY_STATE_MACHINE_ABI_V1;
    out_spi->struct_size = sizeof(*out_spi);
    out_spi->context = state_machine;
    out_spi->try_apply = tr_raft_cflow_try_apply;
    out_spi->poll_settlement = tr_raft_cflow_poll_settlement;
    salts_mutex_unlock(&state_machine->mutex);
    return SALTS_OK;
}

int tr_raft_cflow_state_machine_unbind(
    tr_raft_cflow_state_machine_t *state_machine,
    const cflow_statechart_instance *instance)
{
    int result = SALTS_OK;

    if (state_machine == NULL || instance == NULL) {
        return SALTS_EINVAL;
    }
    salts_mutex_lock(&state_machine->mutex);
    if (state_machine->instance != instance) {
        result = SALTS_EINVAL;
    } else if (instance->impl != NULL || state_machine->in_flight ||
               state_machine->settlement_ready) {
        result = SALTS_EBUSY;
    } else {
        state_machine->instance = NULL;
    }
    salts_mutex_unlock(&state_machine->mutex);
    return result;
}

int tr_raft_cflow_state_machine_destroy(
    tr_raft_cflow_state_machine_t *state_machine)
{
    if (state_machine == NULL) {
        return SALTS_OK;
    }
    salts_mutex_lock(&state_machine->mutex);
    if (state_machine->instance != NULL || state_machine->in_flight ||
        state_machine->settlement_ready) {
        salts_mutex_unlock(&state_machine->mutex);
        return SALTS_EBUSY;
    }
    salts_mutex_unlock(&state_machine->mutex);
    salts_mutex_destroy(&state_machine->mutex);
    free(state_machine);
    return SALTS_OK;
}
