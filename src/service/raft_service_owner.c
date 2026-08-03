#include <turboraft/raft_service_owner.h>

#include <turbo_error.h>

#include <stdatomic.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

typedef struct tr_raft_service_owner_pending {
    tr_raft_service_owner_t *owner;
    tr_raft_service_owner_command_fn command;
    tr_raft_service_owner_complete_fn completion;
    void *completion_context;
    size_t payload_size;
    unsigned char payload[];
} tr_raft_service_owner_pending_t;

struct tr_raft_service_owner {
    tr_raft_service_owner_config_t config;
    atomic_bool started;
    atomic_bool running;
    atomic_bool stopping;
    atomic_bool faulted;
    atomic_int cause;
    atomic_size_t pending_commands;
    atomic_uint_fast64_t completed_commands;
    atomic_uint_fast64_t tick_count;
};

static void tr_raft_service_owner_fault(tr_raft_service_owner_t *owner,
                                        int cause)
{
    int expected = TURBO_OK;

    atomic_compare_exchange_strong_explicit(&owner->cause, &expected, cause,
                                            memory_order_acq_rel,
                                            memory_order_acquire);
    atomic_store_explicit(&owner->faulted, true, memory_order_release);
    atomic_store_explicit(&owner->stopping, true, memory_order_release);
}

static int tr_raft_service_owner_reserve(tr_raft_service_owner_t *owner)
{
    size_t pending = atomic_load_explicit(&owner->pending_commands,
                                          memory_order_acquire);

    for (;;) {
        if (pending >= owner->config.max_pending_commands) {
            return TURBO_EBUSY;
        }
        if (atomic_compare_exchange_weak_explicit(
                &owner->pending_commands, &pending, pending + 1U,
                memory_order_acq_rel, memory_order_acquire)) {
            return TURBO_OK;
        }
    }
}

static void tr_raft_service_owner_execute(void *arg1, void *arg2)
{
    tr_raft_service_owner_pending_t *pending =
        (tr_raft_service_owner_pending_t *)arg1;
    tr_raft_service_owner_t *owner = pending->owner;
    int result;

    (void)arg2;
    result = pending->command(owner->config.service, pending->payload,
                              pending->payload_size);
    if (pending->completion != NULL) {
        pending->completion(pending->completion_context, result);
    }
    atomic_fetch_add_explicit(&owner->completed_commands, 1U,
                              memory_order_release);
    atomic_fetch_sub_explicit(&owner->pending_commands, 1U,
                              memory_order_release);
    free(pending);
}

static void tr_raft_service_owner_tick(coro_t *coroutine, void *context)
{
    tr_raft_service_owner_t *owner = (tr_raft_service_owner_t *)context;

    (void)coroutine;
    while (!atomic_load_explicit(&owner->stopping, memory_order_acquire)) {
        tr_raft_tick_t tick;
        uint32_t election_timeout = 0U;
        int result;

        coro_sleep(owner->config.context, owner->config.tick_interval_ms);
        if (atomic_load_explicit(&owner->stopping, memory_order_acquire)) {
            break;
        }
        result = owner->config.next_election_timeout(
            owner->config.timeout_context, &election_timeout);
        if (result != TURBO_OK || election_timeout == 0U) {
            tr_raft_service_owner_fault(
                owner, result == TURBO_OK ? TURBO_EINVAL : result);
            break;
        }
        tick = (tr_raft_tick_t){owner->config.elapsed_ticks,
                                election_timeout};
        result = tr_raft_service_tick(owner->config.service, &tick);
        if (result != TURBO_OK) {
            tr_raft_service_owner_fault(owner, result);
            break;
        }
        atomic_fetch_add_explicit(&owner->tick_count, 1U,
                                  memory_order_release);
    }
    atomic_store_explicit(&owner->running, false, memory_order_release);
}

int tr_raft_service_owner_create(const tr_raft_service_owner_config_t *config,
                                 tr_raft_service_owner_t **owner)
{
    tr_raft_service_owner_t *created;

    if (config == NULL || owner == NULL || config->service == NULL ||
        config->context == NULL || config->tick_interval_ms == 0U ||
        config->elapsed_ticks == 0U ||
        config->next_election_timeout == NULL) {
        return TURBO_EINVAL;
    }
    if (config->max_pending_commands > TR_RAFT_SERVICE_OWNER_MAX_PENDING ||
        config->max_command_bytes > TR_RAFT_SERVICE_OWNER_MAX_COMMAND_BYTES) {
        return TURBO_EINVAL;
    }
    created = (tr_raft_service_owner_t *)calloc(1U, sizeof(*created));
    if (created == NULL) {
        return TURBO_ENOMEM;
    }
    created->config = *config;
    if (created->config.max_pending_commands == 0U) {
        created->config.max_pending_commands =
            TR_RAFT_SERVICE_OWNER_DEFAULT_MAX_PENDING;
    }
    if (created->config.max_command_bytes == 0U) {
        created->config.max_command_bytes =
            TR_RAFT_SERVICE_OWNER_DEFAULT_MAX_COMMAND_BYTES;
    }
    atomic_init(&created->started, false);
    atomic_init(&created->running, false);
    atomic_init(&created->stopping, false);
    atomic_init(&created->faulted, false);
    atomic_init(&created->cause, TURBO_OK);
    atomic_init(&created->pending_commands, 0U);
    atomic_init(&created->completed_commands, 0U);
    atomic_init(&created->tick_count, 0U);
    *owner = created;
    return TURBO_OK;
}

int tr_raft_service_owner_start(tr_raft_service_owner_t *owner)
{
    bool expected = false;
    int result;

    if (owner == NULL) {
        return TURBO_EINVAL;
    }
    if (!atomic_compare_exchange_strong_explicit(
            &owner->started, &expected, true, memory_order_acq_rel,
            memory_order_acquire)) {
        return TURBO_EALREADY;
    }
    atomic_store_explicit(&owner->running, true, memory_order_release);
    result = coro_context_spawn(owner->config.context,
                                tr_raft_service_owner_tick, owner);
    if (result != TURBO_OK) {
        atomic_store_explicit(&owner->running, false, memory_order_release);
        atomic_store_explicit(&owner->started, false, memory_order_release);
        return result;
    }
    return TURBO_OK;
}

int tr_raft_service_owner_submit(
    tr_raft_service_owner_t *owner,
    tr_raft_service_owner_command_fn command,
    const void *payload,
    size_t payload_size,
    tr_raft_service_owner_complete_fn completion,
    void *completion_context)
{
    tr_raft_service_owner_pending_t *pending;
    int result;

    if (owner == NULL || command == NULL ||
        (payload_size != 0U && payload == NULL)) {
        return TURBO_EINVAL;
    }
    if (!atomic_load_explicit(&owner->started, memory_order_acquire) ||
        atomic_load_explicit(&owner->stopping, memory_order_acquire) ||
        atomic_load_explicit(&owner->faulted, memory_order_acquire)) {
        return TURBO_ECANCELED;
    }
    if (payload_size > owner->config.max_command_bytes ||
        payload_size > SIZE_MAX - sizeof(*pending)) {
        return TURBO_EINVAL;
    }
    pending = (tr_raft_service_owner_pending_t *)malloc(sizeof(*pending) +
                                                        payload_size);
    if (pending == NULL) {
        return TURBO_ENOMEM;
    }
    result = tr_raft_service_owner_reserve(owner);
    if (result != TURBO_OK) {
        free(pending);
        return result;
    }
    if (atomic_load_explicit(&owner->stopping, memory_order_acquire)) {
        atomic_fetch_sub_explicit(&owner->pending_commands, 1U,
                                  memory_order_release);
        free(pending);
        return TURBO_ECANCELED;
    }
    pending->owner = owner;
    pending->command = command;
    pending->completion = completion;
    pending->completion_context = completion_context;
    pending->payload_size = payload_size;
    if (payload_size != 0U) {
        memcpy(pending->payload, payload, payload_size);
    }
    result = coro_post(owner->config.context, tr_raft_service_owner_execute,
                       pending, NULL);
    if (result != TURBO_OK) {
        atomic_fetch_sub_explicit(&owner->pending_commands, 1U,
                                  memory_order_release);
        free(pending);
        return result;
    }
    return TURBO_OK;
}

tr_raft_service_t *tr_raft_service_owner_service(
    const tr_raft_service_owner_t *owner)
{
    return owner == NULL ? NULL : owner->config.service;
}

coro_context_t *tr_raft_service_owner_context(
    const tr_raft_service_owner_t *owner)
{
    return owner == NULL ? NULL : owner->config.context;
}

int tr_raft_service_owner_execute_current(
    tr_raft_service_owner_t *owner,
    tr_raft_service_owner_command_fn command,
    const void *payload,
    size_t payload_size)
{
    int result;

    if (owner == NULL || command == NULL ||
        (payload_size != 0U && payload == NULL) ||
        payload_size > owner->config.max_command_bytes) {
        return TURBO_EINVAL;
    }
    if (coro_context_current() != owner->config.context) {
        return TURBO_EPROTO;
    }
    if (!atomic_load_explicit(&owner->started, memory_order_acquire) ||
        atomic_load_explicit(&owner->stopping, memory_order_acquire) ||
        atomic_load_explicit(&owner->faulted, memory_order_acquire)) {
        return TURBO_ECANCELED;
    }
    result = command(owner->config.service, payload, payload_size);
    atomic_fetch_add_explicit(&owner->completed_commands, 1U,
                              memory_order_release);
    return result;
}

int tr_raft_service_owner_stop(tr_raft_service_owner_t *owner)
{
    if (owner == NULL) {
        return TURBO_EINVAL;
    }
    atomic_store_explicit(&owner->stopping, true, memory_order_release);
    return TURBO_OK;
}

int tr_raft_service_owner_status(const tr_raft_service_owner_t *owner,
                                 tr_raft_service_owner_status_t *status)
{
    if (owner == NULL || status == NULL) {
        return TURBO_EINVAL;
    }
    status->started = atomic_load_explicit(&owner->started,
                                           memory_order_acquire);
    status->running = atomic_load_explicit(&owner->running,
                                           memory_order_acquire);
    status->stopping = atomic_load_explicit(&owner->stopping,
                                            memory_order_acquire);
    status->faulted = atomic_load_explicit(&owner->faulted,
                                           memory_order_acquire);
    status->cause = atomic_load_explicit(&owner->cause, memory_order_acquire);
    status->pending_commands = atomic_load_explicit(&owner->pending_commands,
                                                    memory_order_acquire);
    status->completed_commands = atomic_load_explicit(
        &owner->completed_commands, memory_order_acquire);
    status->tick_count = atomic_load_explicit(&owner->tick_count,
                                              memory_order_acquire);
    return TURBO_OK;
}

int tr_raft_service_owner_close(tr_raft_service_owner_t *owner)
{
    if (owner == NULL) {
        return TURBO_OK;
    }
    if (atomic_load_explicit(&owner->running, memory_order_acquire) ||
        atomic_load_explicit(&owner->pending_commands, memory_order_acquire) !=
            0U) {
        return TURBO_EBUSY;
    }
    free(owner);
    return TURBO_OK;
}
