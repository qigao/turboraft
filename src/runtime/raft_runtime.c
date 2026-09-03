#include <turboraft/raft_runtime.h>

#include "raft_configuration.h"

#include <salts_error.h>

#include <string.h>

static bool tr_ready_needs_storage(const tr_raft_ready_t *ready)
{
    return ready->hard_state_changed || ready->log_changed ||
           ready->commit_changed;
}

static bool tr_storage_complete(const tr_raft_storage_t *storage)
{
    return storage->begin != NULL && storage->write_hard_state != NULL &&
           storage->truncate_log != NULL && storage->append_log != NULL &&
           storage->write_commit_index != NULL && storage->commit != NULL &&
           storage->rollback != NULL;
}

static int tr_runtime_fail(tr_raft_runtime_t *runtime,
                           tr_raft_runtime_result_t *result,
                           tr_raft_runtime_stage_t stage,
                           int cause)
{
    runtime->faulted = true;
    result->stage = stage;
    result->cause = cause;
    return cause;
}

static int tr_runtime_rollback(tr_raft_runtime_t *runtime,
                               tr_raft_runtime_result_t *result,
                               tr_raft_runtime_stage_t stage,
                               int cause)
{
    result->rollback_error =
        runtime->storage.rollback(runtime->storage.context);
    return tr_runtime_fail(runtime, result, stage, cause);
}

static int tr_runtime_persist(tr_raft_runtime_t *runtime,
                              const tr_raft_ready_t *ready,
                              tr_raft_runtime_result_t *result)
{
    int callback_result;

    result->stage = TR_RAFT_RUNTIME_STORAGE_BEGIN;
    callback_result = runtime->storage.begin(runtime->storage.context);
    if (callback_result != SALTS_OK) {
        return tr_runtime_fail(runtime, result, result->stage, callback_result);
    }
    if (ready->hard_state_changed) {
        result->stage = TR_RAFT_RUNTIME_STORAGE_HARD_STATE;
        callback_result = runtime->storage.write_hard_state(
            runtime->storage.context, ready->term, ready->voted_for);
        if (callback_result != SALTS_OK) {
            return tr_runtime_rollback(runtime, result, result->stage,
                                       callback_result);
        }
    }
    if (ready->log_changed && ready->log_truncate_from != 0U) {
        result->stage = TR_RAFT_RUNTIME_STORAGE_TRUNCATE;
        callback_result = runtime->storage.truncate_log(
            runtime->storage.context, ready->log_truncate_from);
        if (callback_result != SALTS_OK) {
            return tr_runtime_rollback(runtime, result, result->stage,
                                       callback_result);
        }
    }
    if (ready->log_changed && ready->log_entry_count != 0U) {
        result->stage = TR_RAFT_RUNTIME_STORAGE_APPEND;
        callback_result = runtime->storage.append_log(
            runtime->storage.context, ready->log_entries,
            ready->log_entry_count);
        if (callback_result != SALTS_OK) {
            return tr_runtime_rollback(runtime, result, result->stage,
                                       callback_result);
        }
    }
    if (ready->commit_changed) {
        result->stage = TR_RAFT_RUNTIME_STORAGE_COMMIT_INDEX;
        callback_result = runtime->storage.write_commit_index(
            runtime->storage.context, ready->commit_index);
        if (callback_result != SALTS_OK) {
            return tr_runtime_rollback(runtime, result, result->stage,
                                       callback_result);
        }
    }
    result->stage = TR_RAFT_RUNTIME_STORAGE_COMMIT;
    callback_result = runtime->storage.commit(runtime->storage.context);
    if (callback_result != SALTS_OK) {
        return tr_runtime_rollback(runtime, result, result->stage,
                                   callback_result);
    }
    result->durable = true;
    return SALTS_OK;
}

int tr_raft_runtime_init(tr_raft_runtime_t *runtime,
                         const tr_raft_runtime_config_t *config)
{
    if (runtime == NULL || config == NULL || config->core == NULL) {
        return SALTS_EINVAL;
    }
    memset(runtime, 0, sizeof(*runtime));
    runtime->core = config->core;
    runtime->storage = config->storage;
    runtime->transport = config->transport;
    runtime->state_machine = config->state_machine;
    return SALTS_OK;
}

int tr_raft_runtime_process(tr_raft_runtime_t *runtime,
                            const tr_raft_ready_t *ready,
                            tr_raft_runtime_result_t *result)
{
    size_t index;
    int callback_result;

    if (runtime == NULL || ready == NULL || result == NULL ||
        runtime->core == NULL) {
        return SALTS_EINVAL;
    }
    memset(result, 0, sizeof(*result));
    if (runtime->faulted) {
        return tr_runtime_fail(runtime, result, TR_RAFT_RUNTIME_IDLE,
                               SALTS_EPROTO);
    }
    if ((ready->message_count != 0U && ready->messages == NULL) ||
        (ready->log_entry_count != 0U && ready->log_entries == NULL) ||
        (ready->committed_entry_count != 0U &&
         ready->committed_entries == NULL)) {
        return tr_runtime_fail(runtime, result, TR_RAFT_RUNTIME_IDLE,
                               SALTS_EINVAL);
    }
    result->read_state_ready = ready->read_state_ready;
    result->read_state = ready->read_state;
    if (tr_ready_needs_storage(ready)) {
        if (!tr_storage_complete(&runtime->storage)) {
            return tr_runtime_fail(runtime, result,
                                   TR_RAFT_RUNTIME_STORAGE_BEGIN,
                                   SALTS_EINVAL);
        }
        callback_result = tr_runtime_persist(runtime, ready, result);
        if (callback_result != SALTS_OK) {
            return callback_result;
        }
    }
    if (ready->message_count != 0U && runtime->transport.enqueue == NULL) {
        return tr_runtime_fail(runtime, result,
                               TR_RAFT_RUNTIME_TRANSPORT_SEND,
                               SALTS_EINVAL);
    }
    result->stage = TR_RAFT_RUNTIME_TRANSPORT_SEND;
    for (index = 0U; index < ready->message_count; ++index) {
        callback_result = runtime->transport.enqueue(
            runtime->transport.context, &ready->messages[index]);
        if (callback_result != SALTS_OK) {
            return tr_runtime_fail(runtime, result, result->stage,
                                   callback_result);
        }
        ++result->messages_enqueued;
    }
    for (index = 0U; index < ready->snapshot_request_count; ++index) {
        if (runtime->transport.enqueue_snapshot == NULL) {
            return tr_runtime_fail(runtime, result, result->stage,
                                   SALTS_EINVAL);
        }
        callback_result = runtime->transport.enqueue_snapshot(
            runtime->transport.snapshot_context,
            &ready->snapshot_requests[index]);
        if (callback_result != SALTS_OK) {
            return tr_runtime_fail(runtime, result, result->stage,
                                   callback_result);
        }
        ++result->snapshots_requested;
    }
    if (ready->committed_entry_count != 0U) {
        size_t begin = 0U;

        result->stage = TR_RAFT_RUNTIME_STATE_MACHINE_APPLY;
        while (begin < ready->committed_entry_count) {
            size_t end = begin;

            if (tr_raft_conf_entry_is_configuration(
                    &ready->committed_entries[begin])) {
                ++begin;
                continue;
            }
            while (end < ready->committed_entry_count &&
                   !tr_raft_conf_entry_is_configuration(
                       &ready->committed_entries[end])) {
                ++end;
            }
            if (runtime->state_machine.apply_batch == NULL) {
                return tr_runtime_fail(runtime, result, result->stage,
                                       SALTS_EINVAL);
            }
            callback_result = runtime->state_machine.apply_batch(
                runtime->state_machine.context,
                ready->committed_entries + begin, end - begin);
            if (callback_result != SALTS_OK) {
                return tr_runtime_fail(runtime, result, result->stage,
                                       callback_result);
            }
            begin = end;
        }
        result->applied_through =
            ready->committed_entries[ready->committed_entry_count - 1U].index;
    }
    result->stage = TR_RAFT_RUNTIME_CORE_ADVANCE;
    callback_result = tr_raft_core_advance(runtime->core);
    if (callback_result != SALTS_OK) {
        return tr_runtime_fail(runtime, result, result->stage,
                               callback_result);
    }
    result->stage = TR_RAFT_RUNTIME_COMPLETE;
    return SALTS_OK;
}

bool tr_raft_runtime_is_faulted(const tr_raft_runtime_t *runtime)
{
    return runtime == NULL || runtime->faulted;
}
