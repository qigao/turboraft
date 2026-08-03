#include "raft_control_owner_bridge.h"

#if defined(TURBORAFT_HAS_SERVICE_OWNER)

#include <CoroNet/turbo_coro_context.h>
#include <turbo_error.h>

#include <stdatomic.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

enum {
    TR_CONTROL_BRIDGE_QUEUED = 0,
    TR_CONTROL_BRIDGE_RUNNING = 1,
    TR_CONTROL_BRIDGE_COMPLETE = 2,
    TR_CONTROL_BRIDGE_CANCELED = 3,
    TR_CONTROL_BRIDGE_POLL_MS = 1
};

typedef struct tr_control_owner_bridge_call {
    atomic_uint references;
    atomic_int state;
    coro_wait_t *wait;
    tr_raft_service_owner_command_fn command;
    size_t payload_size;
    int result;
    unsigned char payload[];
} tr_control_owner_bridge_call_t;

static void tr_control_owner_bridge_release(
    tr_control_owner_bridge_call_t *call)
{
    if (atomic_fetch_sub_explicit(&call->references, 1U,
                                  memory_order_acq_rel) == 1U) {
        (void)coro_wait_destroy(call->wait);
        free(call);
    }
}

static int tr_control_owner_bridge_invoke(tr_raft_service_t *service,
                                          const void *payload,
                                          size_t payload_size)
{
    tr_control_owner_bridge_call_t *call;
    int expected = TR_CONTROL_BRIDGE_QUEUED;
    int result;

    if (payload == NULL || payload_size != sizeof(call)) {
        return TURBO_EINVAL;
    }
    memcpy(&call, payload, sizeof(call));
    if (call == NULL || !atomic_compare_exchange_strong_explicit(
                            &call->state, &expected,
                            TR_CONTROL_BRIDGE_RUNNING,
                            memory_order_acq_rel, memory_order_acquire)) {
        return expected == TR_CONTROL_BRIDGE_CANCELED ? TURBO_ECANCELED
                                                       : TURBO_EPROTO;
    }
    result = call->command(service, call->payload, call->payload_size);
    call->result = result;
    atomic_store_explicit(&call->state, TR_CONTROL_BRIDGE_COMPLETE,
                          memory_order_release);
    return result;
}

static void tr_control_owner_bridge_complete(void *context, int result)
{
    tr_control_owner_bridge_call_t *call =
        (tr_control_owner_bridge_call_t *)context;

    (void)result;
    if (atomic_load_explicit(&call->state, memory_order_acquire) ==
        TR_CONTROL_BRIDGE_COMPLETE) {
        (void)coro_wait_interrupt(call->wait, TURBO_ECANCELED);
    }
    tr_control_owner_bridge_release(call);
}

static int tr_control_owner_bridge_wait(
    tr_control_owner_bridge_call_t *call,
    coro_context_t *context,
    uint64_t queue_timeout_ms)
{
    uint64_t started_at = coro_context_now(context);

    for (;;) {
        int state = atomic_load_explicit(&call->state,
                                         memory_order_acquire);

        if (state == TR_CONTROL_BRIDGE_COMPLETE) {
            return call->result;
        }
        if (state == TR_CONTROL_BRIDGE_CANCELED) {
            return TURBO_ETIMEDOUT;
        }
        if (state == TR_CONTROL_BRIDGE_QUEUED) {
            uint64_t now = coro_context_now(context);
            uint64_t elapsed = now >= started_at ? now - started_at
                                                  : UINT64_MAX;

            if (elapsed >= queue_timeout_ms) {
                int expected = TR_CONTROL_BRIDGE_QUEUED;

                if (atomic_compare_exchange_strong_explicit(
                        &call->state, &expected,
                        TR_CONTROL_BRIDGE_CANCELED,
                        memory_order_acq_rel, memory_order_acquire)) {
                    return TURBO_ETIMEDOUT;
                }
                continue;
            }
        }
        (void)coro_wait_for(call->wait, TR_CONTROL_BRIDGE_POLL_MS);
    }
}

int tr_control_owner_bridge_execute(
    tr_raft_service_owner_t *owner,
    tr_raft_service_owner_command_fn command,
    const void *payload,
    size_t payload_size,
    uint64_t queue_timeout_ms)
{
    tr_control_owner_bridge_call_t *call;
    tr_control_owner_bridge_call_t *submitted;
    coro_context_t *context;
    int result;

    if (owner == NULL || command == NULL || queue_timeout_ms == 0U ||
        (payload_size != 0U && payload == NULL) ||
        payload_size > SIZE_MAX - sizeof(*call)) {
        return TURBO_EINVAL;
    }
    if (coro_context_current() == tr_raft_service_owner_context(owner)) {
        return tr_raft_service_owner_execute_current(
            owner, command, payload, payload_size);
    }
    context = coro_context_current();
    if (context == NULL) {
        return TURBO_EPROTO;
    }
    call = (tr_control_owner_bridge_call_t *)calloc(
        1U, sizeof(*call) + payload_size);
    if (call == NULL) {
        return TURBO_ENOMEM;
    }
    call->wait = coro_wait_create(context);
    if (call->wait == NULL) {
        free(call);
        return TURBO_ENOMEM;
    }
    atomic_init(&call->references, 2U);
    atomic_init(&call->state, TR_CONTROL_BRIDGE_QUEUED);
    call->command = command;
    call->payload_size = payload_size;
    if (payload_size != 0U) {
        memcpy(call->payload, payload, payload_size);
    }
    submitted = call;
    result = tr_raft_service_owner_submit(
        owner, tr_control_owner_bridge_invoke, &submitted,
        sizeof(submitted), tr_control_owner_bridge_complete, call);
    if (result != TURBO_OK) {
        tr_control_owner_bridge_release(call);
        tr_control_owner_bridge_release(call);
        return result;
    }
    result = tr_control_owner_bridge_wait(call, context, queue_timeout_ms);
    tr_control_owner_bridge_release(call);
    return result;
}

#endif
