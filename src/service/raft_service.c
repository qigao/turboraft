#include <turboraft/raft_service.h>

#include <salts_error.h>

#include <stdlib.h>
#include <string.h>

typedef struct tr_raft_service_peer_delivery {
    tr_raft_node_id_t node_id;
    bool paused;
    tr_raft_message_t *messages;
    size_t message_head;
    size_t message_count;
    bool snapshot_pending;
    tr_raft_snapshot_request_t snapshot;
    uint64_t capacity_rejection_count;
    uint64_t paused_ticks;
    int last_error;
} tr_raft_service_peer_delivery_t;

struct tr_raft_service {
    tr_raft_core_t *core;
    tr_raft_runtime_t *runtime;
    tr_raft_storage_t storage;
    tr_raft_transport_t transport;
    tr_raft_state_machine_t state_machine;
    tr_raft_snapshot_policy_t snapshot_policy;
    uint8_t *snapshot_buffer;
    tr_raft_snapshot_point_t pending_snapshot;
    bool journal_compaction_pending;
    bool backup_prepared;
    tr_raft_message_t messages[TR_RAFT_MAX_VOTERS];
    tr_raft_service_peer_delivery_t deliveries[TR_RAFT_MAX_MEMBERS];
    tr_raft_runtime_result_t last_runtime_result;
    tr_raft_read_state_t read_states[TR_RAFT_MAX_PENDING_READS];
    size_t read_state_head;
    size_t read_state_count;
    size_t max_completed_reads;
    bool faulted;
    int cause;
};

static void tr_service_prepare_ready(
    tr_raft_service_t *service,
    tr_raft_ready_t *ready)
{
    memset(ready, 0, sizeof(*ready));
    ready->messages = service->messages;
    ready->message_capacity = sizeof(service->messages) /
                              sizeof(service->messages[0]);
}

static bool tr_service_ready_has_effects(const tr_raft_ready_t *ready)
{
    return ready->message_count != 0U || ready->hard_state_changed ||
           ready->role_changed || ready->log_changed ||
           ready->commit_changed || ready->committed_entry_count != 0U ||
           ready->read_state_count != 0U ||
           ready->snapshot_request_count != 0U;
}

static int tr_service_fault(tr_raft_service_t *service, int cause)
{
    service->faulted = true;
    service->cause = cause;
    return cause;
}

static bool tr_service_delivery_empty(
    const tr_raft_service_peer_delivery_t *delivery)
{
    return delivery->message_count == 0U && !delivery->snapshot_pending;
}

static tr_raft_service_peer_delivery_t *tr_service_find_delivery(
    tr_raft_service_t *service,
    tr_raft_node_id_t peer_id,
    bool create)
{
    tr_raft_service_peer_delivery_t *free_slot = NULL;
    tr_raft_service_peer_delivery_t *reusable = NULL;
    size_t index;

    for (index = 0U; index < TR_RAFT_MAX_MEMBERS; ++index) {
        tr_raft_service_peer_delivery_t *delivery =
            &service->deliveries[index];

        if (delivery->node_id == peer_id) {
            return delivery;
        }
        if (delivery->node_id == 0U && free_slot == NULL) {
            free_slot = delivery;
        } else if (create && reusable == NULL &&
                   !delivery->paused &&
                   tr_service_delivery_empty(delivery)) {
            bool ignored = false;
            if (tr_raft_core_peer_paused(
                    service->core, delivery->node_id, &ignored) ==
                SALTS_ENOENT) {
                reusable = delivery;
            }
        }
    }
    if (!create) {
        return NULL;
    }
    if (free_slot == NULL) {
        free_slot = reusable;
    }
    if (free_slot == NULL) {
        return NULL;
    }
    free(free_slot->messages);
    memset(free_slot, 0, sizeof(*free_slot));
    free_slot->node_id = peer_id;
    free_slot->last_error = SALTS_OK;
    return free_slot;
}

static const tr_raft_service_peer_delivery_t *tr_service_find_delivery_const(
    const tr_raft_service_t *service,
    tr_raft_node_id_t peer_id)
{
    size_t index;

    for (index = 0U; index < TR_RAFT_MAX_MEMBERS; ++index) {
        if (service->deliveries[index].node_id == peer_id) {
            return &service->deliveries[index];
        }
    }
    return NULL;
}

static bool tr_service_has_pending_transport(
    const tr_raft_service_t *service)
{
    size_t index;

    for (index = 0U; index < TR_RAFT_MAX_MEMBERS; ++index) {
        const tr_raft_service_peer_delivery_t *delivery =
            &service->deliveries[index];

        if (delivery->paused || !tr_service_delivery_empty(delivery)) {
            return true;
        }
    }
    return false;
}

static void tr_service_clear_pending_transport(tr_raft_service_t *service)
{
    size_t index;

    for (index = 0U; index < TR_RAFT_MAX_MEMBERS; ++index) {
        free(service->deliveries[index].messages);
    }
    memset(service->deliveries, 0, sizeof(service->deliveries));
}

static int tr_service_pause_delivery(
    tr_raft_service_t *service,
    tr_raft_service_peer_delivery_t *delivery)
{
    int result;

    if (delivery->paused) {
        ++delivery->capacity_rejection_count;
        delivery->last_error = SALTS_ENOSPC;
        return SALTS_OK;
    }
    result = tr_raft_core_set_peer_paused(
        service->core, delivery->node_id, true);
    if (result != SALTS_OK) {
        return result;
    }
    delivery->paused = true;
    delivery->paused_ticks = 0U;
    ++delivery->capacity_rejection_count;
    delivery->last_error = SALTS_ENOSPC;
    return SALTS_OK;
}

static int tr_service_stage_message(
    tr_raft_service_peer_delivery_t *delivery,
    const tr_raft_message_t *message)
{
    size_t slot;

    if (delivery->messages == NULL) {
        delivery->messages = (tr_raft_message_t *)calloc(
            TR_RAFT_MAX_VOTERS, sizeof(*delivery->messages));
        if (delivery->messages == NULL) {
            return SALTS_ENOMEM;
        }
    }
    if (delivery->message_count == TR_RAFT_MAX_VOTERS) {
        return SALTS_EPROTO;
    }
    slot = (delivery->message_head + delivery->message_count) %
           TR_RAFT_MAX_VOTERS;
    delivery->messages[slot] = *message;
    ++delivery->message_count;
    return SALTS_OK;
}

static int tr_service_stage_snapshot(
    tr_raft_service_peer_delivery_t *delivery,
    const tr_raft_snapshot_request_t *request)
{
    if (delivery->snapshot_pending) {
        return SALTS_EPROTO;
    }
    delivery->snapshot = *request;
    delivery->snapshot_pending = true;
    return SALTS_OK;
}

static int tr_service_enqueue_message(
    void *context,
    const tr_raft_message_t *message)
{
    tr_raft_service_t *service = (tr_raft_service_t *)context;
    tr_raft_service_peer_delivery_t *delivery;
    int result;

    if (service == NULL || message == NULL ||
        service->transport.enqueue == NULL) {
        return SALTS_EINVAL;
    }
    delivery = tr_service_find_delivery(service, message->to, false);
    if (delivery != NULL && delivery->paused) {
        return tr_service_stage_message(delivery, message);
    }

    result = service->transport.enqueue(
        service->transport.context, message);
    if (result != SALTS_ENOSPC) {
        if (delivery != NULL) {
            delivery->last_error = result;
        }
        return result;
    }

    delivery = tr_service_find_delivery(service, message->to, true);
    if (delivery == NULL) {
        return SALTS_ENOSPC;
    }
    result = tr_service_pause_delivery(service, delivery);
    if (result != SALTS_OK) {
        return result;
    }
    return tr_service_stage_message(delivery, message);
}

static int tr_service_enqueue_snapshot(
    void *context,
    const tr_raft_snapshot_request_t *request)
{
    tr_raft_service_t *service = (tr_raft_service_t *)context;
    tr_raft_service_peer_delivery_t *delivery;
    int result;

    if (service == NULL || request == NULL ||
        service->transport.enqueue_snapshot == NULL) {
        return SALTS_EINVAL;
    }
    delivery = tr_service_find_delivery(service, request->peer_id, false);
    if (delivery != NULL && delivery->paused) {
        return tr_service_stage_snapshot(delivery, request);
    }

    result = service->transport.enqueue_snapshot(
        service->transport.snapshot_context, request);
    if (result != SALTS_ENOSPC) {
        if (delivery != NULL) {
            delivery->last_error = result;
        }
        return result;
    }

    delivery = tr_service_find_delivery(service, request->peer_id, true);
    if (delivery == NULL) {
        return SALTS_ENOSPC;
    }
    result = tr_service_pause_delivery(service, delivery);
    if (result != SALTS_OK) {
        return result;
    }
    return tr_service_stage_snapshot(delivery, request);
}

static int tr_service_drain_delivery(
    tr_raft_service_t *service,
    tr_raft_service_peer_delivery_t *delivery)
{
    bool core_paused = false;
    int result;

    result = tr_raft_core_peer_paused(
        service->core, delivery->node_id, &core_paused);
    if (result == SALTS_ENOENT) {
        free(delivery->messages);
        memset(delivery, 0, sizeof(*delivery));
        return SALTS_OK;
    }
    if (result != SALTS_OK) {
        return tr_service_fault(service, result);
    }

    while (delivery->message_count != 0U) {
        tr_raft_message_t *message =
            &delivery->messages[delivery->message_head];

        result = service->transport.enqueue(
            service->transport.context, message);
        if (result == SALTS_ENOSPC) {
            ++delivery->capacity_rejection_count;
            delivery->last_error = result;
            return SALTS_OK;
        }
        if (result != SALTS_OK) {
            delivery->last_error = result;
            return tr_service_fault(service, result);
        }
        delivery->message_head =
            (delivery->message_head + 1U) % TR_RAFT_MAX_VOTERS;
        --delivery->message_count;
    }
    delivery->message_head = 0U;

    if (delivery->snapshot_pending) {
        result = service->transport.enqueue_snapshot(
            service->transport.snapshot_context, &delivery->snapshot);
        if (result == SALTS_ENOSPC) {
            ++delivery->capacity_rejection_count;
            delivery->last_error = result;
            return SALTS_OK;
        }
        if (result != SALTS_OK) {
            delivery->last_error = result;
            return tr_service_fault(service, result);
        }
        memset(&delivery->snapshot, 0, sizeof(delivery->snapshot));
        delivery->snapshot_pending = false;
    }

    if (delivery->paused && tr_service_delivery_empty(delivery)) {
        result = tr_raft_core_set_peer_paused(
            service->core, delivery->node_id, false);
        if (result != SALTS_OK && result != SALTS_ENOENT) {
            return tr_service_fault(service, result);
        }
        delivery->paused = false;
        delivery->last_error = SALTS_OK;
        free(delivery->messages);
        delivery->messages = NULL;
    }
    return SALTS_OK;
}

static int tr_service_drain_transport(tr_raft_service_t *service)
{
    size_t index;

    for (index = 0U; index < TR_RAFT_MAX_MEMBERS; ++index) {
        tr_raft_service_peer_delivery_t *delivery =
            &service->deliveries[index];
        int result;

        if (delivery->node_id == 0U ||
            (!delivery->paused && tr_service_delivery_empty(delivery))) {
            continue;
        }
        result = tr_service_drain_delivery(service, delivery);
        if (result != SALTS_OK) {
            return result;
        }
    }
    return SALTS_OK;
}

static void tr_service_add_paused_ticks(
    tr_raft_service_t *service,
    uint32_t elapsed_ticks)
{
    size_t index;

    for (index = 0U; index < TR_RAFT_MAX_MEMBERS; ++index) {
        tr_raft_service_peer_delivery_t *delivery =
            &service->deliveries[index];

        if (!delivery->paused) {
            continue;
        }
        if (UINT64_MAX - delivery->paused_ticks < elapsed_ticks) {
            delivery->paused_ticks = UINT64_MAX;
        } else {
            delivery->paused_ticks += elapsed_ticks;
        }
    }
}

static int tr_service_mutation_guard(tr_raft_service_t *service)
{
    int result;

    if (service->faulted) {
        return SALTS_EPROTO;
    }
    if (service->backup_prepared) {
        return SALTS_EBUSY;
    }
    if (tr_service_has_pending_transport(service)) {
        result = tr_service_drain_transport(service);
        if (result != SALTS_OK) {
            return result;
        }
    }
    return tr_raft_runtime_apply_blocked(service->runtime)
               ? SALTS_EBUSY
               : SALTS_OK;
}

static bool tr_service_storage_complete(const tr_raft_storage_t *storage)
{
    return storage->begin != NULL && storage->write_hard_state != NULL &&
           storage->truncate_log != NULL && storage->append_log != NULL &&
           storage->write_commit_index != NULL && storage->commit != NULL &&
           storage->rollback != NULL;
}

static bool tr_service_snapshot_policy_disabled(
    const tr_raft_snapshot_policy_t *policy)
{
    return policy->applied_entry_threshold == 0U &&
           policy->max_snapshot_bytes == 0U &&
           policy->max_buffered_snapshot_bytes == 0U &&
           policy->source_create == NULL &&
           policy->source_create_context == NULL &&
           policy->source_store == NULL &&
           policy->source_store_context == NULL &&
           policy->create == NULL &&
           policy->create_context == NULL &&
           policy->store == NULL &&
           policy->store_context == NULL &&
           policy->journal_compact == NULL &&
           policy->journal_compact_context == NULL;
}

static bool tr_service_snapshot_policy_streaming(
    const tr_raft_snapshot_policy_t *policy)
{
    return policy->source_create != NULL || policy->source_store != NULL ||
           policy->source_create_context != NULL ||
           policy->source_store_context != NULL;
}

static bool tr_service_snapshot_policy_buffered(
    const tr_raft_snapshot_policy_t *policy)
{
    return policy->create != NULL || policy->store != NULL ||
           policy->create_context != NULL || policy->store_context != NULL;
}

static void tr_service_snapshot_source_release(
    tr_raft_snapshot_source_t *source)
{
    if (source != NULL && source->release != NULL) {
        source->release(source->context);
    }
    if (source != NULL) {
        memset(source, 0, sizeof(*source));
    }
}

static int tr_service_snapshot_policy_validate(
    const tr_raft_service_config_t *config)
{
    const tr_raft_snapshot_policy_t *policy = &config->snapshot_policy;
    size_t max_log_entries = config->core.max_log_entries == 0U
                                 ? TR_RAFT_DEFAULT_MAX_LOG_ENTRIES
                                 : config->core.max_log_entries;

    if (tr_service_snapshot_policy_disabled(policy)) {
        return SALTS_OK;
    }
    if (policy->applied_entry_threshold == 0U ||
        policy->applied_entry_threshold > max_log_entries ||
        policy->max_snapshot_bytes == 0U ||
        (policy->journal_compact == NULL &&
         policy->journal_compact_context != NULL)) {
        return SALTS_EINVAL;
    }

    if (tr_service_snapshot_policy_streaming(policy)) {
        if (tr_service_snapshot_policy_buffered(policy) ||
            policy->source_create == NULL || policy->source_store == NULL ||
            policy->max_buffered_snapshot_bytes != 0U) {
            return SALTS_EINVAL;
        }
        return SALTS_OK;
    }

    if (!tr_service_snapshot_policy_buffered(policy) ||
        policy->create == NULL || policy->store == NULL ||
        policy->max_buffered_snapshot_bytes == 0U ||
        policy->max_buffered_snapshot_bytes > policy->max_snapshot_bytes ||
        policy->max_buffered_snapshot_bytes > (uint64_t)SIZE_MAX) {
        return SALTS_EINVAL;
    }
    return SALTS_OK;
}

static int tr_service_finish_journal_compaction(
    tr_raft_service_t *service)
{
    int result;

    result = service->snapshot_policy.journal_compact(
        service->snapshot_policy.journal_compact_context,
        service->pending_snapshot.index, service->pending_snapshot.term);
    if (result != SALTS_OK) {
        return result == SALTS_EIO ? result : tr_service_fault(service, result);
    }
    result = tr_raft_core_compact(service->core, &service->pending_snapshot);
    if (result != SALTS_OK) {
        return tr_service_fault(service, result);
    }
    memset(&service->pending_snapshot, 0, sizeof(service->pending_snapshot));
    service->journal_compaction_pending = false;
    return SALTS_OK;
}

static int tr_service_snapshot(tr_raft_service_t *service, bool force)
{
    tr_raft_status_t status;
    tr_raft_snapshot_point_t point;
    size_t snapshot_size = 0U;
    int result;

    if (service->snapshot_policy.applied_entry_threshold == 0U) {
        return force ? SALTS_EPROTONOSUPPORT : SALTS_OK;
    }
    if (service->journal_compaction_pending) {
        return tr_service_finish_journal_compaction(service);
    }
    result = tr_raft_core_status(service->core, &status);
    if (result != SALTS_OK) {
        return tr_service_fault(service, result);
    }
    if (status.applied_index == status.log_base_index) {
        return force ? SALTS_ENOENT : SALTS_OK;
    }
    if (!force && status.applied_index - status.log_base_index <
        service->snapshot_policy.applied_entry_threshold) {
        return SALTS_OK;
    }
    result = tr_raft_core_snapshot_point(service->core, &point);
    if (result != SALTS_OK) {
        return tr_service_fault(service, result);
    }
    if (service->snapshot_policy.source_create != NULL) {
        tr_raft_snapshot_source_t source;

        memset(&source, 0, sizeof(source));
        result = service->snapshot_policy.source_create(
            service->snapshot_policy.source_create_context,
            point.index, &source);
        if (result != SALTS_OK) {
            return tr_service_fault(service, result);
        }
        if (source.read_at == NULL ||
            source.size > service->snapshot_policy.max_snapshot_bytes) {
            tr_service_snapshot_source_release(&source);
            return tr_service_fault(service, SALTS_EPROTO);
        }

        result = service->snapshot_policy.source_store(
            service->snapshot_policy.source_store_context,
            point.index, point.term, &point.configuration, &source);
        tr_service_snapshot_source_release(&source);
        if (result != SALTS_OK) {
            return tr_service_fault(service, result);
        }
    } else {
        result = service->snapshot_policy.create(
            service->snapshot_policy.create_context, point.index,
            service->snapshot_buffer,
            (size_t)service->snapshot_policy.max_buffered_snapshot_bytes,
            &snapshot_size);
        if (result != SALTS_OK) {
            return tr_service_fault(service, result);
        }
        if (snapshot_size >
            (size_t)service->snapshot_policy.max_buffered_snapshot_bytes) {
            return tr_service_fault(service, SALTS_ENOSPC);
        }
        result = service->snapshot_policy.store(
            service->snapshot_policy.store_context, point.index, point.term,
            &point.configuration, service->snapshot_buffer, snapshot_size);
        if (result != SALTS_OK) {
            return tr_service_fault(service, result);
        }
    }
    if (service->snapshot_policy.journal_compact != NULL) {
        service->pending_snapshot = point;
        service->journal_compaction_pending = true;
        return tr_service_finish_journal_compaction(service);
    }
    result = tr_raft_core_compact(service->core, &point);
    if (result != SALTS_OK) {
        return tr_service_fault(service, result);
    }
    return SALTS_OK;
}

static bool tr_service_read_queue_empty(
    const tr_raft_service_t *service)
{
    return service->read_state_count == 0U;
}

static bool tr_service_completed_read_context_exists(
    const tr_raft_service_t *service,
    uint64_t context_id)
{
    size_t index;

    for (index = 0U; index < service->read_state_count; ++index) {
        size_t slot =
            (service->read_state_head + index) %
            service->max_completed_reads;

        if (service->read_states[slot].context_id == context_id) {
            return true;
        }
    }
    return false;
}

static int tr_service_enqueue_read_states(
    tr_raft_service_t *service,
    const tr_raft_ready_t *ready)
{
    size_t index;

    if (ready->read_state_count == 0U) {
        return SALTS_OK;
    }
    if (ready->read_state_count >
        service->max_completed_reads - service->read_state_count) {
        return SALTS_EPROTO;
    }
    for (index = 0U; index < ready->read_state_count; ++index) {
        size_t slot =
            (service->read_state_head + service->read_state_count) %
            service->max_completed_reads;

        service->read_states[slot] = ready->read_states[index];
        ++service->read_state_count;
    }
    return SALTS_OK;
}

static int tr_service_process_ready(
    tr_raft_service_t *service,
    const tr_raft_ready_t *ready)
{
    int result;

    if (!tr_service_ready_has_effects(ready)) {
        return service->journal_compaction_pending
                   ? tr_service_snapshot(service, false)
                   : SALTS_OK;
    }
    result = tr_raft_runtime_process(
        service->runtime, ready, &service->last_runtime_result);
    if (result == SALTS_EBUSY &&
        tr_raft_runtime_apply_blocked(service->runtime)) {
        result = tr_service_enqueue_read_states(service, ready);
        return result == SALTS_OK ? SALTS_EBUSY
                                  : tr_service_fault(service, result);
    }
    if (result != SALTS_OK) {
        tr_service_clear_pending_transport(service);
        return tr_service_fault(service, result);
    }
    result = tr_service_enqueue_read_states(service, ready);
    if (result != SALTS_OK) {
        return tr_service_fault(service, result);
    }
    return tr_service_snapshot(service, false);
}

static int tr_service_runtime_create(
    tr_raft_service_t *service,
    tr_raft_core_t *core,
    const tr_raft_storage_t *storage,
    tr_raft_runtime_t **out_runtime)
{
    tr_raft_runtime_config_t config;

    memset(&config, 0, sizeof(config));
    config.core = core;
    config.storage = *storage;
    config.transport.context = service;
    config.transport.enqueue = tr_service_enqueue_message;
    config.transport.snapshot_context = service;
    config.transport.enqueue_snapshot = tr_service_enqueue_snapshot;
    config.state_machine = service->state_machine;
    return tr_raft_runtime_create(&config, out_runtime);
}

int tr_raft_service_create(
    const tr_raft_service_config_t *config,
    tr_raft_service_t **out_service)
{
    tr_raft_service_t *service;
    int result;

    if (config == NULL || out_service == NULL) {
        return SALTS_EINVAL;
    }
    result = tr_service_snapshot_policy_validate(config);
    if (result != SALTS_OK) {
        return result;
    }
    *out_service = NULL;
    service = (tr_raft_service_t *) calloc(1U, sizeof(*service));
    if (service == NULL) {
        return SALTS_ENOMEM;
    }
    service->storage = config->storage;
    service->transport = config->transport;
    service->state_machine = config->state_machine;
    service->snapshot_policy = config->snapshot_policy;
    service->max_completed_reads =
        config->core.max_pending_reads == 0U
            ? TR_RAFT_DEFAULT_MAX_PENDING_READS
            : config->core.max_pending_reads;
    if (service->snapshot_policy.applied_entry_threshold != 0U &&
        service->snapshot_policy.create != NULL) {
        service->snapshot_buffer = (uint8_t *)malloc(
            (size_t)service->snapshot_policy.max_buffered_snapshot_bytes);
        if (service->snapshot_buffer == NULL) {
            free(service);
            return SALTS_ENOMEM;
        }
    }
    result = tr_raft_core_create(&config->core, &service->core);
    if (result != SALTS_OK) {
        free(service->snapshot_buffer);
        free(service);
        return result;
    }
    result = tr_service_runtime_create(service, service->core,
                                       &service->storage, &service->runtime);
    if (result != SALTS_OK) {
        tr_raft_core_destroy(service->core);
        free(service->snapshot_buffer);
        free(service);
        return result;
    }
    *out_service = service;
    return SALTS_OK;
}

void tr_raft_service_destroy(tr_raft_service_t *service)
{
    if (service == NULL) {
        return;
    }
    tr_service_clear_pending_transport(service);
    (void)tr_raft_runtime_destroy(service->runtime);
    tr_raft_core_destroy(service->core);
    free(service->snapshot_buffer);
    free(service);
}

int tr_raft_service_prepare_backup(tr_raft_service_t *service)
{
    tr_raft_status_t status;
    int result;

    if (service == NULL) {
        return SALTS_EINVAL;
    }
    result = tr_service_mutation_guard(service);
    if (result != SALTS_OK) {
        return result;
    }
    if (service->journal_compaction_pending ||
        !tr_service_read_queue_empty(service) ||
        tr_service_has_pending_transport(service)) {
        return SALTS_EBUSY;
    }
    result = tr_raft_core_status(service->core, &status);
    if (result != SALTS_OK) {
        return tr_service_fault(service, result);
    }
    if (status.ready_outstanding) {
        return SALTS_EBUSY;
    }
    service->backup_prepared = true;
    return SALTS_OK;
}

int tr_raft_service_resume_backup(tr_raft_service_t *service,
                                  const tr_raft_storage_t *storage)
{
    tr_raft_runtime_t *replacement_runtime = NULL;
    tr_raft_runtime_t *previous_runtime;
    int result;

    if (service == NULL || storage == NULL) {
        return SALTS_EINVAL;
    }
    if (!service->backup_prepared || !tr_service_storage_complete(storage)) {
        return SALTS_EINVAL;
    }
    result = tr_service_runtime_create(
        service, service->core, storage, &replacement_runtime);
    if (result != SALTS_OK) {
        return result;
    }
    previous_runtime = service->runtime;
    service->storage = *storage;
    service->runtime = replacement_runtime;
    service->backup_prepared = false;
    (void)tr_raft_runtime_destroy(previous_runtime);
    return SALTS_OK;
}

int tr_raft_service_tick(
    tr_raft_service_t *service,
    const tr_raft_tick_t *tick)
{
    tr_raft_ready_t ready;
    int result;

    if (service == NULL || tick == NULL) {
        return SALTS_EINVAL;
    }
    result = tr_service_mutation_guard(service);
    if (result != SALTS_OK) {
        return result;
    }
    tr_service_add_paused_ticks(service, tick->elapsed_ticks);
    tr_service_prepare_ready(service, &ready);
    result = tr_raft_core_tick(service->core, tick, &ready);
    if (result != SALTS_OK) {
        return result;
    }
    return tr_service_process_ready(service, &ready);
}

int tr_raft_service_step(
    tr_raft_service_t *service,
    const tr_raft_message_t *message)
{
    tr_raft_ready_t ready;
    int result;

    if (service == NULL || message == NULL) {
        return SALTS_EINVAL;
    }
    result = tr_service_mutation_guard(service);
    if (result != SALTS_OK) {
        return result;
    }
    tr_service_prepare_ready(service, &ready);
    result = tr_raft_core_step(service->core, message, &ready);
    if (result != SALTS_OK) {
        return result;
    }
    return tr_service_process_ready(service, &ready);
}

int tr_raft_service_propose_batch(
    tr_raft_service_t *service,
    const tr_raft_proposal_t *proposals,
    size_t proposal_count)
{
    tr_raft_ready_t ready;
    int result;

    if (service == NULL || proposals == NULL || proposal_count == 0U ||
        proposal_count > TR_RAFT_MAX_PROPOSAL_BATCH) {
        return SALTS_EINVAL;
    }
    result = tr_service_mutation_guard(service);
    if (result != SALTS_OK) {
        return result;
    }
    tr_service_prepare_ready(service, &ready);
    result = tr_raft_core_propose_batch(
        service->core, proposals, proposal_count, &ready);
    if (result != SALTS_OK) {
        return result;
    }
    return tr_service_process_ready(service, &ready);
}

int tr_raft_service_propose(
    tr_raft_service_t *service,
    const tr_raft_proposal_t *proposal)
{
    if (proposal == NULL) {
        return SALTS_EINVAL;
    }
    return tr_raft_service_propose_batch(service, proposal, 1U);
}

int tr_raft_service_propose_batch_with_receipts(
    tr_raft_service_t *service,
    const tr_raft_proposal_t *proposals,
    size_t proposal_count,
    tr_raft_operation_status_t *out_receipts,
    size_t receipt_capacity)
{
    tr_raft_status_t status;
    tr_raft_index_t first_index;
    size_t index;
    int result;

    if (service == NULL || proposals == NULL || proposal_count == 0U ||
        proposal_count > TR_RAFT_MAX_PROPOSAL_BATCH ||
        out_receipts == NULL) {
        return SALTS_EINVAL;
    }
    if (receipt_capacity < proposal_count) {
        return SALTS_ENOSPC;
    }

    result = tr_raft_service_propose_batch(
        service, proposals, proposal_count);
    if (result != SALTS_OK) {
        return result;
    }
    result = tr_raft_core_status(service->core, &status);
    if (result != SALTS_OK) {
        return result;
    }
    if (status.last_log_index < proposal_count) {
        return tr_service_fault(service, SALTS_EPROTO);
    }
    first_index = status.last_log_index - proposal_count + 1U;
    for (index = 0U; index < proposal_count; ++index) {
        result = tr_raft_core_operation_status(
            service->core, status.term, first_index + index,
            &out_receipts[index]);
        if (result != SALTS_OK) {
            return result;
        }
    }
    return SALTS_OK;
}

int tr_raft_service_propose_with_receipt(
    tr_raft_service_t *service,
    const tr_raft_proposal_t *proposal,
    tr_raft_operation_status_t *out_receipt)
{
    if (proposal == NULL || out_receipt == NULL) {
        return SALTS_EINVAL;
    }
    return tr_raft_service_propose_batch_with_receipts(
        service, proposal, 1U, out_receipt, 1U);
}

int tr_raft_service_transfer_leadership(
    tr_raft_service_t *service,
    tr_raft_node_id_t transferee_id)
{
    tr_raft_ready_t ready;
    int result;

    if (service == NULL || transferee_id == 0U) {
        return SALTS_EINVAL;
    }
    result = tr_service_mutation_guard(service);
    if (result != SALTS_OK) {
        return result;
    }
    tr_service_prepare_ready(service, &ready);
    result = tr_raft_core_transfer_leadership(
        service->core, transferee_id, &ready);
    if (result != SALTS_OK) {
        return result;
    }
    return tr_service_process_ready(service, &ready);
}

int tr_raft_service_change_membership(
    tr_raft_service_t *service,
    const tr_raft_membership_change_t *change)
{
    tr_raft_ready_t ready;
    int result;

    if (service == NULL || change == NULL) {
        return SALTS_EINVAL;
    }
    result = tr_service_mutation_guard(service);
    if (result != SALTS_OK) {
        return result;
    }
    tr_service_prepare_ready(service, &ready);
    result = tr_raft_core_change_membership(service->core, change, &ready);
    if (result != SALTS_OK) {
        return result;
    }
    return tr_service_process_ready(service, &ready);
}

int tr_raft_service_change_membership_with_receipt(
    tr_raft_service_t *service,
    const tr_raft_membership_change_t *change,
    tr_raft_operation_status_t *out_receipt)
{
    tr_raft_status_t status;
    int result;

    if (service == NULL || change == NULL || out_receipt == NULL) {
        return SALTS_EINVAL;
    }
    result = tr_raft_service_change_membership(service, change);
    if (result != SALTS_OK) {
        return result;
    }
    result = tr_raft_core_status(service->core, &status);
    if (result != SALTS_OK) {
        return result;
    }
    return tr_raft_core_operation_status(service->core, status.term,
                                         status.last_log_index,
                                         out_receipt);
}

int tr_raft_service_read_index(tr_raft_service_t *service,
                               uint64_t context_id)
{
    tr_raft_ready_t ready;
    tr_raft_status_t status;
    int result;

    if (service == NULL || context_id == 0U) {
        return SALTS_EINVAL;
    }
    result = tr_service_mutation_guard(service);
    if (result != SALTS_OK) {
        return result;
    }
    if (tr_service_completed_read_context_exists(service, context_id)) {
        return SALTS_EALREADY;
    }
    result = tr_raft_core_status(service->core, &status);
    if (result != SALTS_OK) {
        return tr_service_fault(service, result);
    }
    if (service->read_state_count + status.pending_read_count >=
        service->max_completed_reads) {
        return SALTS_ENOSPC;
    }

    tr_service_prepare_ready(service, &ready);
    result = tr_raft_core_read_index(service->core, context_id, &ready);
    if (result != SALTS_OK) {
        return result;
    }
    return tr_service_process_ready(service, &ready);
}

int tr_raft_service_take_read_state(tr_raft_service_t *service,
                                    tr_raft_read_state_t *out_read_state)
{
    tr_raft_status_t status;
    tr_raft_read_state_t *state;
    int result;

    if (service == NULL || out_read_state == NULL) {
        return SALTS_EINVAL;
    }
    result = tr_service_mutation_guard(service);
    if (result != SALTS_OK) {
        return result;
    }
    if (tr_service_read_queue_empty(service)) {
        return SALTS_ENOENT;
    }
    result = tr_raft_core_status(service->core, &status);
    if (result != SALTS_OK) {
        return tr_service_fault(service, result);
    }
    state = &service->read_states[service->read_state_head];
    if (status.applied_index < state->index) {
        return SALTS_EBUSY;
    }
    *out_read_state = *state;
    memset(state, 0, sizeof(*state));
    service->read_state_head =
        (service->read_state_head + 1U) % service->max_completed_reads;
    --service->read_state_count;
    if (service->read_state_count == 0U) {
        service->read_state_head = 0U;
    }
    return SALTS_OK;
}

int tr_raft_service_poll(tr_raft_service_t *service)
{
    tr_raft_ready_t ready;
    int result;

    if (service == NULL) {
        return SALTS_EINVAL;
    }
    if (service->faulted) {
        return SALTS_EPROTO;
    }
    if (service->backup_prepared) {
        return SALTS_EBUSY;
    }
    if (tr_service_has_pending_transport(service)) {
        result = tr_service_drain_transport(service);
        if (result != SALTS_OK) {
            return result;
        }
    }
    if (tr_raft_runtime_apply_blocked(service->runtime)) {
        result = tr_raft_runtime_retry_apply(
            service->runtime, &service->last_runtime_result);
        if (result == SALTS_EBUSY) {
            return result;
        }
        if (result != SALTS_OK) {
            return tr_service_fault(service, result);
        }
        return tr_service_snapshot(service, false);
    }

    tr_service_prepare_ready(service, &ready);
    result = tr_raft_core_poll(service->core, &ready);
    if (result != SALTS_OK) {
        return result;
    }
    return tr_service_process_ready(service, &ready);
}

int tr_raft_service_trigger_snapshot(tr_raft_service_t *service)
{
    int result;

    if (service == NULL) {
        return SALTS_EINVAL;
    }
    result = tr_service_mutation_guard(service);
    if (result != SALTS_OK) {
        return result;
    }
    return tr_service_snapshot(service, true);
}

int tr_raft_service_snapshot_completed(tr_raft_service_t *service,
                                       tr_raft_node_id_t peer_id,
                                       tr_raft_index_t snapshot_index)
{
    tr_raft_ready_t ready;
    int result;

    if (service == NULL || peer_id == 0U || snapshot_index == 0U) {
        return SALTS_EINVAL;
    }
    result = tr_service_mutation_guard(service);
    if (result != SALTS_OK) {
        return result;
    }
    tr_service_prepare_ready(service, &ready);
    result = tr_raft_core_snapshot_completed(service->core, peer_id,
                                             snapshot_index, &ready);
    if (result != SALTS_OK) {
        return result;
    }
    return tr_service_process_ready(service, &ready);
}

int tr_raft_service_snapshot_complete_callback(void *context,
                                               tr_raft_node_id_t peer_id,
                                               tr_raft_index_t snapshot_index)
{
    return tr_raft_service_snapshot_completed(
        (tr_raft_service_t *) context, peer_id, snapshot_index);
}

int tr_raft_service_reload(
    tr_raft_service_t *service,
    const tr_raft_core_config_t *recovery_config)
{
    tr_raft_core_t *replacement = NULL;
    tr_raft_core_t *previous_core;
    tr_raft_runtime_t *replacement_runtime = NULL;
    tr_raft_runtime_t *previous_runtime;
    tr_raft_status_t current;
    int result;

    if (service == NULL || recovery_config == NULL) {
        return SALTS_EINVAL;
    }
    if (service->backup_prepared) {
        return SALTS_EBUSY;
    }
    result = tr_raft_core_status(service->core, &current);
    if (result != SALTS_OK) {
        return tr_service_fault(service, result);
    }
    if (!service->faulted && current.ready_outstanding) {
        return SALTS_EBUSY;
    }
    result = tr_raft_core_create(recovery_config, &replacement);
    if (result != SALTS_OK) {
        return tr_service_fault(service, result);
    }
    result = tr_service_runtime_create(
        service, replacement, &service->storage, &replacement_runtime);
    if (result != SALTS_OK) {
        tr_raft_core_destroy(replacement);
        return tr_service_fault(service, result);
    }

    previous_core = service->core;
    previous_runtime = service->runtime;
    service->core = replacement;
    service->runtime = replacement_runtime;
    (void)tr_raft_runtime_destroy(previous_runtime);
    tr_raft_core_destroy(previous_core);
    memset(&service->last_runtime_result, 0,
           sizeof(service->last_runtime_result));
    memset(service->read_states, 0, sizeof(service->read_states));
    service->read_state_head = 0U;
    service->read_state_count = 0U;
    tr_service_clear_pending_transport(service);
    service->faulted = false;
    service->cause = SALTS_OK;
    return SALTS_OK;
}

int tr_raft_service_status(
    const tr_raft_service_t *service,
    tr_raft_service_status_t *out_status)
{
    int result;

    if (service == NULL || out_status == NULL) {
        return SALTS_EINVAL;
    }
    memset(out_status, 0, sizeof(*out_status));
    out_status->faulted = service->faulted;
    out_status->cause = service->cause;
    out_status->backup_prepared = service->backup_prepared;
    out_status->read_state_available = service->read_state_count != 0U;
    out_status->completed_read_count = service->read_state_count;
    out_status->max_completed_reads = service->max_completed_reads;
    out_status->journal_compaction_pending =
        service->journal_compaction_pending;
    out_status->runtime = service->last_runtime_result;
    result = tr_raft_core_status(service->core, &out_status->core);
    return result;
}

int tr_raft_service_get_peer_delivery_status(
    const tr_raft_service_t *service,
    tr_raft_node_id_t peer_id,
    tr_raft_service_peer_delivery_status_t *out_status)
{
    const tr_raft_service_peer_delivery_t *delivery;
    bool paused = false;
    int result;

    if (service == NULL || peer_id == 0U || out_status == NULL) {
        return SALTS_EINVAL;
    }
    result = tr_raft_core_peer_paused(
        service->core, peer_id, &paused);
    if (result != SALTS_OK) {
        return result;
    }

    memset(out_status, 0, sizeof(*out_status));
    out_status->peer_id = peer_id;
    out_status->paused = paused;
    out_status->last_error = SALTS_OK;
    delivery = tr_service_find_delivery_const(service, peer_id);
    if (delivery != NULL) {
        out_status->staged_message_count = delivery->message_count;
        out_status->staged_message_bytes =
            delivery->message_count * sizeof(tr_raft_message_t);
        out_status->snapshot_staged = delivery->snapshot_pending;
        out_status->capacity_rejection_count =
            delivery->capacity_rejection_count;
        out_status->paused_ticks = delivery->paused_ticks;
        out_status->last_error = delivery->last_error;
    }
    return SALTS_OK;
}

int tr_raft_service_configuration(
    const tr_raft_service_t *service,
    tr_raft_conf_t *out_configuration)
{
    return service == NULL || out_configuration == NULL
               ? SALTS_EINVAL
               : tr_raft_core_configuration(service->core,
                                            out_configuration);
}

int tr_raft_service_progress(
    const tr_raft_service_t *service,
    tr_raft_progress_view_t *out_progress)
{
    return service == NULL || out_progress == NULL
               ? SALTS_EINVAL
               : tr_raft_core_progress(service->core, out_progress);
}

int tr_raft_service_operation_status(
    const tr_raft_service_t *service,
    tr_raft_term_t term,
    tr_raft_index_t index,
    tr_raft_operation_status_t *out_status)
{
    if (service == NULL) {
        return SALTS_EINVAL;
    }
    return tr_raft_core_operation_status(service->core, term, index,
                                         out_status);
}
