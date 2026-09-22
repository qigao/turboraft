#include <turboraft/raft_apply_runtime.h>

#include "raft_configuration.h"

#include <salts_error.h>

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

struct tr_raft_apply_runtime {
    tr_raft_core_t *core;
    tr_raft_storage_t storage;
    tr_raft_transport_t transport;
    tr_raft_entry_state_machine_v1_t state_machine;
    tr_raft_entry_t *entries;
    size_t entry_capacity;
    size_t entry_count;
    size_t entry_cursor;
    tr_raft_index_t admitted_through;
    tr_raft_apply_runtime_state_t state;
    tr_raft_apply_runtime_result_t progress;
    bool active;
    bool faulted;
    bool reconciliation_pending;
};

static bool tr_apply_ready_needs_storage(const tr_raft_ready_t *ready)
{
    return ready->hard_state_changed || ready->log_changed ||
           ready->commit_changed;
}

static bool tr_apply_storage_complete(const tr_raft_storage_t *storage)
{
    return storage->begin != NULL && storage->write_hard_state != NULL &&
           storage->truncate_log != NULL && storage->append_log != NULL &&
           storage->write_commit_index != NULL && storage->commit != NULL &&
           storage->rollback != NULL;
}

static void tr_apply_copy_result(const tr_raft_apply_runtime_t *runtime,
                                 tr_raft_apply_runtime_result_t *result)
{
    *result = runtime->progress;
    result->state = runtime->state;
    result->in_flight_token =
        runtime->state == TR_RAFT_APPLY_RUNTIME_WAITING_SETTLEMENT
            ? runtime->entries[runtime->entry_cursor].index
            : runtime->reconciliation_pending
                  ? runtime->entries[runtime->entry_cursor].index
                  : 0U;
}

static void tr_apply_compact_entries(tr_raft_apply_runtime_t *runtime)
{
    size_t remaining;

    if (runtime->entry_cursor == 0U) {
        return;
    }
    remaining = runtime->entry_count - runtime->entry_cursor;
    if (remaining != 0U) {
        memmove(runtime->entries,
                runtime->entries + runtime->entry_cursor,
                remaining * sizeof(*runtime->entries));
    }
    if (remaining < runtime->entry_count) {
        memset(runtime->entries + remaining, 0,
               (runtime->entry_count - remaining) *
                   sizeof(*runtime->entries));
    }
    runtime->entry_count = remaining;
    runtime->entry_cursor = 0U;
}

static int tr_apply_set_fault(tr_raft_apply_runtime_t *runtime,
                              tr_raft_runtime_stage_t stage,
                              int cause,
                              bool reconciliation_pending,
                              tr_raft_apply_runtime_result_t *result)
{
    runtime->faulted = true;
    runtime->reconciliation_pending = reconciliation_pending;
    runtime->state = TR_RAFT_APPLY_RUNTIME_FAULTED;
    runtime->progress.stage = stage;
    runtime->progress.cause = cause == SALTS_OK ? SALTS_EPROTO : cause;
    tr_apply_copy_result(runtime, result);
    return runtime->progress.cause;
}

static int tr_apply_fail(tr_raft_apply_runtime_t *runtime,
                         tr_raft_runtime_stage_t stage,
                         int cause,
                         tr_raft_apply_runtime_result_t *result)
{
    return tr_apply_set_fault(runtime, stage, cause, false, result);
}

static int tr_apply_fail_recoverable(
    tr_raft_apply_runtime_t *runtime,
    tr_raft_runtime_stage_t stage,
    int cause,
    tr_raft_apply_runtime_result_t *result)
{
    return tr_apply_set_fault(runtime, stage, cause, true, result);
}

static bool tr_apply_entry_equal(const tr_raft_entry_t *left,
                                 const tr_raft_entry_t *right)
{
    return left->index == right->index && left->term == right->term &&
           left->command_id == right->command_id &&
           left->data_length == right->data_length &&
           left->data_length <= TR_RAFT_MAX_ENTRY_BYTES &&
           memcmp(left->data, right->data, left->data_length) == 0;
}

static int tr_apply_rollback(tr_raft_apply_runtime_t *runtime,
                             tr_raft_runtime_stage_t stage,
                             int cause,
                             tr_raft_apply_runtime_result_t *result)
{
    runtime->progress.rollback_error =
        runtime->storage.rollback(runtime->storage.context);
    return tr_apply_fail(runtime, stage, cause, result);
}

static int tr_apply_persist(tr_raft_apply_runtime_t *runtime,
                            const tr_raft_ready_t *ready,
                            tr_raft_apply_runtime_result_t *result)
{
    int callback_result;

    runtime->progress.stage = TR_RAFT_RUNTIME_STORAGE_BEGIN;
    callback_result = runtime->storage.begin(runtime->storage.context);
    if (callback_result != SALTS_OK) {
        return tr_apply_fail(runtime, runtime->progress.stage,
                             callback_result, result);
    }
    if (ready->hard_state_changed) {
        runtime->progress.stage = TR_RAFT_RUNTIME_STORAGE_HARD_STATE;
        callback_result = runtime->storage.write_hard_state(
            runtime->storage.context, ready->term, ready->voted_for);
        if (callback_result != SALTS_OK) {
            return tr_apply_rollback(runtime, runtime->progress.stage,
                                     callback_result, result);
        }
    }
    if (ready->log_changed && ready->log_truncate_from != 0U) {
        runtime->progress.stage = TR_RAFT_RUNTIME_STORAGE_TRUNCATE;
        callback_result = runtime->storage.truncate_log(
            runtime->storage.context, ready->log_truncate_from);
        if (callback_result != SALTS_OK) {
            return tr_apply_rollback(runtime, runtime->progress.stage,
                                     callback_result, result);
        }
    }
    if (ready->log_changed && ready->log_entry_count != 0U) {
        runtime->progress.stage = TR_RAFT_RUNTIME_STORAGE_APPEND;
        callback_result = runtime->storage.append_log(
            runtime->storage.context, ready->log_entries,
            ready->log_entry_count);
        if (callback_result != SALTS_OK) {
            return tr_apply_rollback(runtime, runtime->progress.stage,
                                     callback_result, result);
        }
    }
    if (ready->commit_changed) {
        runtime->progress.stage = TR_RAFT_RUNTIME_STORAGE_COMMIT_INDEX;
        callback_result = runtime->storage.write_commit_index(
            runtime->storage.context, ready->commit_index);
        if (callback_result != SALTS_OK) {
            return tr_apply_rollback(runtime, runtime->progress.stage,
                                     callback_result, result);
        }
    }
    runtime->progress.stage = TR_RAFT_RUNTIME_STORAGE_COMMIT;
    callback_result = runtime->storage.commit(runtime->storage.context);
    if (callback_result != SALTS_OK) {
        return tr_apply_rollback(runtime, runtime->progress.stage,
                                 callback_result, result);
    }
    runtime->progress.durable = true;
    return SALTS_OK;
}

static int tr_apply_send(tr_raft_apply_runtime_t *runtime,
                         const tr_raft_ready_t *ready,
                         tr_raft_apply_runtime_result_t *result)
{
    size_t index;
    int callback_result;

    runtime->progress.stage = TR_RAFT_RUNTIME_TRANSPORT_SEND;
    if (ready->message_count != 0U && runtime->transport.enqueue == NULL) {
        return tr_apply_fail(runtime, runtime->progress.stage, SALTS_EINVAL,
                             result);
    }
    for (index = 0U; index < ready->message_count; ++index) {
        callback_result = runtime->transport.enqueue(
            runtime->transport.context, &ready->messages[index]);
        if (callback_result != SALTS_OK) {
            return tr_apply_fail(runtime, runtime->progress.stage,
                                 callback_result, result);
        }
        ++runtime->progress.messages_enqueued;
    }
    for (index = 0U; index < ready->snapshot_request_count; ++index) {
        if (runtime->transport.enqueue_snapshot == NULL) {
            return tr_apply_fail(runtime, runtime->progress.stage,
                                 SALTS_EINVAL, result);
        }
        callback_result = runtime->transport.enqueue_snapshot(
            runtime->transport.snapshot_context,
            &ready->snapshot_requests[index]);
        if (callback_result != SALTS_OK) {
            return tr_apply_fail(runtime, runtime->progress.stage,
                                 callback_result, result);
        }
        ++runtime->progress.snapshots_requested;
    }
    return SALTS_OK;
}

static int tr_apply_drive_admission(
    tr_raft_apply_runtime_t *runtime,
    tr_raft_apply_runtime_result_t *result)
{
    while (runtime->entry_cursor < runtime->entry_count) {
        const tr_raft_entry_t *entry =
            &runtime->entries[runtime->entry_cursor];
        tr_raft_apply_admission_t admission;
        int cause = SALTS_OK;
        int core_result;

        if (tr_raft_conf_entry_is_configuration(entry)) {
            core_result = tr_raft_core_ack_applied(
                runtime->core, entry->index);
            if (core_result != SALTS_OK) {
                return tr_apply_fail(runtime, TR_RAFT_RUNTIME_CORE_ADVANCE,
                                     core_result, result);
            }
            runtime->progress.applied_through = entry->index;
            ++runtime->entry_cursor;
            continue;
        }

        runtime->state = TR_RAFT_APPLY_RUNTIME_WAITING_ADMISSION;
        runtime->progress.stage = TR_RAFT_RUNTIME_STATE_MACHINE_APPLY;
        admission = runtime->state_machine.try_apply(
            runtime->state_machine.context, entry, entry->index, &cause);
        if (admission == TR_RAFT_APPLY_ADMISSION_ACCEPTED) {
            runtime->state = TR_RAFT_APPLY_RUNTIME_WAITING_SETTLEMENT;
            runtime->progress.cause = SALTS_OK;
            tr_apply_copy_result(runtime, result);
            return SALTS_OK;
        }
        if (admission == TR_RAFT_APPLY_ADMISSION_FULL) {
            runtime->progress.cause = SALTS_ENOBUFS;
            tr_apply_copy_result(runtime, result);
            return SALTS_OK;
        }
        if (admission == TR_RAFT_APPLY_ADMISSION_CLOSED) {
            return tr_apply_fail(runtime, runtime->progress.stage,
                                 cause == SALTS_OK ? SALTS_ESHUTDOWN : cause,
                                 result);
        }
        return tr_apply_fail(runtime, runtime->progress.stage,
                             cause == SALTS_OK ? SALTS_EPROTO : cause,
                             result);
    }

    runtime->active = false;
    runtime->entry_count = 0U;
    runtime->entry_cursor = 0U;
    runtime->state = TR_RAFT_APPLY_RUNTIME_COMPLETE;
    runtime->progress.stage = TR_RAFT_RUNTIME_COMPLETE;
    runtime->progress.cause = SALTS_OK;
    tr_apply_copy_result(runtime, result);
    return SALTS_OK;
}

int tr_raft_apply_runtime_create(
    const tr_raft_apply_runtime_config_v1_t *config,
    tr_raft_apply_runtime_t **out_runtime)
{
    tr_raft_apply_runtime_t *runtime;
    tr_raft_status_t status;

    if (config == NULL || out_runtime == NULL) {
        return SALTS_EINVAL;
    }
    *out_runtime = NULL;
    if (config->abi_version != TR_RAFT_APPLY_RUNTIME_CONFIG_ABI_V1 ||
        config->struct_size < sizeof(*config) || config->core == NULL ||
        config->max_pending_entries == 0U ||
        config->max_pending_entries > SIZE_MAX / sizeof(tr_raft_entry_t) ||
        config->state_machine.abi_version !=
            TR_RAFT_ENTRY_STATE_MACHINE_ABI_V1 ||
        config->state_machine.struct_size < sizeof(config->state_machine) ||
        config->state_machine.try_apply == NULL ||
        config->state_machine.poll_settlement == NULL) {
        return SALTS_EINVAL;
    }
    runtime = (tr_raft_apply_runtime_t *) calloc(1U, sizeof(*runtime));
    if (runtime == NULL) {
        return SALTS_ENOMEM;
    }
    runtime->entries = (tr_raft_entry_t *) calloc(
        config->max_pending_entries, sizeof(*runtime->entries));
    if (runtime->entries == NULL) {
        free(runtime);
        return SALTS_ENOMEM;
    }
    runtime->core = config->core;
    runtime->storage = config->storage;
    runtime->transport = config->transport;
    runtime->state_machine = config->state_machine;
    runtime->entry_capacity = config->max_pending_entries;
    if (tr_raft_core_status(runtime->core, &status) != SALTS_OK ||
        status.applied_index > status.commit_index) {
        free(runtime->entries);
        free(runtime);
        return SALTS_EPROTO;
    }
    runtime->admitted_through = status.applied_index;
    runtime->progress.applied_through = status.applied_index;
    runtime->state = TR_RAFT_APPLY_RUNTIME_IDLE;
    *out_runtime = runtime;
    return SALTS_OK;
}

int tr_raft_apply_runtime_start(tr_raft_apply_runtime_t *runtime,
                                const tr_raft_ready_t *ready,
                                tr_raft_apply_runtime_result_t *result)
{
    tr_raft_status_t status;
    tr_raft_apply_runtime_state_t prior_state;
    tr_raft_index_t committed_delta;
    size_t append_offset;
    size_t index;
    int prior_cause;
    int callback_result;
    bool was_active;

    if (runtime == NULL || ready == NULL || result == NULL) {
        return SALTS_EINVAL;
    }
    memset(result, 0, sizeof(*result));
    if (runtime->faulted) {
        runtime->state = TR_RAFT_APPLY_RUNTIME_FAULTED;
        tr_apply_copy_result(runtime, result);
        return runtime->progress.cause == SALTS_OK ? SALTS_EPROTO
                                                   : runtime->progress.cause;
    }
    if ((ready->message_count != 0U && ready->messages == NULL) ||
        ready->message_count > ready->message_capacity ||
        (ready->log_entry_count != 0U && ready->log_entries == NULL) ||
        (ready->committed_entry_count != 0U &&
         ready->committed_entries == NULL) ||
        ready->snapshot_request_count > TR_RAFT_MAX_MEMBERS) {
        result->cause = SALTS_EINVAL;
        return SALTS_EINVAL;
    }
    if (tr_raft_core_status(runtime->core, &status) != SALTS_OK ||
        !status.ready_outstanding ||
        status.applied_index > status.commit_index ||
        runtime->admitted_through > status.commit_index) {
        result->cause = SALTS_EPROTO;
        return SALTS_EPROTO;
    }

    committed_delta = status.commit_index - runtime->admitted_through;
    if (committed_delta > SIZE_MAX ||
        ready->committed_entry_count != (size_t) committed_delta) {
        result->cause = SALTS_EPROTO;
        return SALTS_EPROTO;
    }

    tr_apply_compact_entries(runtime);
    if (ready->committed_entry_count >
        runtime->entry_capacity - runtime->entry_count) {
        result->state = runtime->active ? runtime->state
                                        : TR_RAFT_APPLY_RUNTIME_IDLE;
        result->cause = SALTS_ENOBUFS;
        return SALTS_ENOBUFS;
    }
    for (index = 0U; index < ready->committed_entry_count; ++index) {
        if (ready->committed_entries[index].data_length >
                TR_RAFT_MAX_ENTRY_BYTES ||
            runtime->admitted_through == UINT64_MAX ||
            ready->committed_entries[index].index !=
                runtime->admitted_through +
                    (tr_raft_index_t) index + 1U) {
            result->cause = SALTS_EPROTO;
            return SALTS_EPROTO;
        }
    }

    was_active = runtime->active;
    prior_state = runtime->state;
    prior_cause = runtime->progress.cause;
    append_offset = runtime->entry_count;
    if (ready->committed_entry_count != 0U) {
        memcpy(runtime->entries + append_offset, ready->committed_entries,
               ready->committed_entry_count * sizeof(*runtime->entries));
        runtime->entry_count += ready->committed_entry_count;
    }
    runtime->active = true;
    runtime->progress.rollback_error = SALTS_OK;
    runtime->progress.messages_enqueued = 0U;
    runtime->progress.snapshots_requested = 0U;
    runtime->progress.durable = false;
    runtime->progress.read_state_ready = ready->read_state_ready;
    runtime->progress.read_state = ready->read_state;
    runtime->progress.applied_through = status.applied_index;

    if (tr_apply_ready_needs_storage(ready)) {
        if (!tr_apply_storage_complete(&runtime->storage)) {
            return tr_apply_fail(runtime, TR_RAFT_RUNTIME_STORAGE_BEGIN,
                                 SALTS_EINVAL, result);
        }
        callback_result = tr_apply_persist(runtime, ready, result);
        if (callback_result != SALTS_OK) {
            return callback_result;
        }
    }
    callback_result = tr_apply_send(runtime, ready, result);
    if (callback_result != SALTS_OK) {
        return callback_result;
    }

    runtime->progress.stage = TR_RAFT_RUNTIME_CORE_ADVANCE;
    callback_result = tr_raft_core_ack_ready(
        runtime->core, ready->committed_entry_count);
    if (callback_result != SALTS_OK) {
        return tr_apply_fail(runtime, runtime->progress.stage,
                             callback_result, result);
    }
    runtime->admitted_through +=
        (tr_raft_index_t) ready->committed_entry_count;

    if (was_active) {
        runtime->state = prior_state;
        runtime->progress.cause = prior_cause;
        runtime->progress.stage = TR_RAFT_RUNTIME_STATE_MACHINE_APPLY;
        tr_apply_copy_result(runtime, result);
        return SALTS_OK;
    }

    runtime->state = TR_RAFT_APPLY_RUNTIME_WAITING_ADMISSION;
    runtime->progress.cause = SALTS_OK;
    return tr_apply_drive_admission(runtime, result);
}

int tr_raft_apply_runtime_poll(tr_raft_apply_runtime_t *runtime,
                               tr_raft_apply_runtime_result_t *result)
{
    tr_raft_apply_settlement_t settlement;
    bool ready = false;
    int callback_result;

    if (runtime == NULL || result == NULL) {
        return SALTS_EINVAL;
    }
    memset(result, 0, sizeof(*result));
    if (runtime->faulted) {
        runtime->state = TR_RAFT_APPLY_RUNTIME_FAULTED;
        tr_apply_copy_result(runtime, result);
        return runtime->progress.cause == SALTS_OK ? SALTS_EPROTO
                                                   : runtime->progress.cause;
    }
    if (!runtime->active) {
        result->state = TR_RAFT_APPLY_RUNTIME_IDLE;
        result->cause = SALTS_EPROTO;
        return SALTS_EPROTO;
    }
    if (runtime->state == TR_RAFT_APPLY_RUNTIME_WAITING_ADMISSION) {
        return tr_apply_drive_admission(runtime, result);
    }
    if (runtime->state != TR_RAFT_APPLY_RUNTIME_WAITING_SETTLEMENT ||
        runtime->entry_cursor >= runtime->entry_count) {
        return tr_apply_fail(runtime, TR_RAFT_RUNTIME_STATE_MACHINE_APPLY,
                             SALTS_EPROTO, result);
    }

    memset(&settlement, 0, sizeof(settlement));
    callback_result = runtime->state_machine.poll_settlement(
        runtime->state_machine.context, &settlement, &ready);
    if (callback_result != SALTS_OK) {
        return tr_apply_fail(runtime, TR_RAFT_RUNTIME_STATE_MACHINE_APPLY,
                             callback_result, result);
    }
    if (!ready) {
        tr_apply_copy_result(runtime, result);
        return SALTS_OK;
    }
    if (settlement.token != runtime->entries[runtime->entry_cursor].index) {
        return tr_apply_fail(runtime, TR_RAFT_RUNTIME_STATE_MACHINE_APPLY,
                             SALTS_EPROTO, result);
    }
    if (settlement.outcome == TR_RAFT_APPLY_OUTCOME_PENDING) {
        runtime->state = TR_RAFT_APPLY_RUNTIME_WAITING_ADMISSION;
        runtime->progress.cause = settlement.cause;
        return tr_apply_drive_admission(runtime, result);
    }
    if (settlement.outcome != TR_RAFT_APPLY_OUTCOME_APPLIED) {
        if (settlement.outcome == TR_RAFT_APPLY_OUTCOME_GAP ||
            settlement.outcome == TR_RAFT_APPLY_OUTCOME_CONFLICT ||
            settlement.outcome == TR_RAFT_APPLY_OUTCOME_UNKNOWN) {
            return tr_apply_fail_recoverable(
                runtime, TR_RAFT_RUNTIME_STATE_MACHINE_APPLY,
                settlement.cause == SALTS_OK ? SALTS_EPROTO
                                             : settlement.cause,
                result);
        }
        return tr_apply_fail(runtime, TR_RAFT_RUNTIME_STATE_MACHINE_APPLY,
                             SALTS_EPROTO, result);
    }
    if (settlement.cause != SALTS_OK) {
        return tr_apply_fail(runtime, TR_RAFT_RUNTIME_STATE_MACHINE_APPLY,
                             SALTS_EPROTO, result);
    }
    callback_result = tr_raft_core_ack_applied(
        runtime->core, settlement.token);
    if (callback_result != SALTS_OK) {
        return tr_apply_fail(runtime, TR_RAFT_RUNTIME_CORE_ADVANCE,
                             callback_result, result);
    }
    runtime->progress.applied_through = settlement.token;
    ++runtime->entry_cursor;
    return tr_apply_drive_admission(runtime, result);
}

int tr_raft_apply_runtime_reconcile(
    tr_raft_apply_runtime_t *runtime,
    const tr_raft_entry_t *entry,
    tr_raft_apply_outcome_t outcome,
    tr_raft_apply_runtime_result_t *result)
{
    const tr_raft_entry_t *current;
    int callback_result;

    if (runtime == NULL || entry == NULL || result == NULL) {
        return SALTS_EINVAL;
    }
    memset(result, 0, sizeof(*result));
    if (outcome != TR_RAFT_APPLY_OUTCOME_APPLIED &&
        outcome != TR_RAFT_APPLY_OUTCOME_PENDING) {
        tr_apply_copy_result(runtime, result);
        return SALTS_EINVAL;
    }
    if (!runtime->faulted || !runtime->reconciliation_pending ||
        !runtime->active ||
        runtime->state != TR_RAFT_APPLY_RUNTIME_FAULTED ||
        runtime->entry_cursor >= runtime->entry_count) {
        tr_apply_copy_result(runtime, result);
        return SALTS_EPROTO;
    }
    current = &runtime->entries[runtime->entry_cursor];
    if (!tr_apply_entry_equal(current, entry)) {
        tr_apply_copy_result(runtime, result);
        return SALTS_EPROTO;
    }

    runtime->faulted = false;
    runtime->reconciliation_pending = false;
    runtime->progress.cause = SALTS_OK;
    if (outcome == TR_RAFT_APPLY_OUTCOME_PENDING) {
        runtime->state = TR_RAFT_APPLY_RUNTIME_WAITING_ADMISSION;
        return tr_apply_drive_admission(runtime, result);
    }

    callback_result = tr_raft_core_ack_applied(
        runtime->core, current->index);
    if (callback_result != SALTS_OK) {
        return tr_apply_fail(runtime, TR_RAFT_RUNTIME_CORE_ADVANCE,
                             callback_result, result);
    }
    runtime->progress.applied_through = current->index;
    ++runtime->entry_cursor;
    return tr_apply_drive_admission(runtime, result);
}

bool tr_raft_apply_runtime_is_faulted(
    const tr_raft_apply_runtime_t *runtime)
{
    return runtime == NULL || runtime->faulted;
}

int tr_raft_apply_runtime_destroy(tr_raft_apply_runtime_t *runtime)
{
    if (runtime == NULL) {
        return SALTS_OK;
    }
    if (runtime->active && !runtime->faulted) {
        return SALTS_EBUSY;
    }
    free(runtime->entries);
    free(runtime);
    return SALTS_OK;
}
