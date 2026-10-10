#include <turboraft/raft_multicore.h>

#include "raft_multicore_chunk_internal.h"
#include "../transport/raft_transport_payload_storage.h"

#include <ring_buffer.h>
#include <salts/thread.h>
#include <salts/clock.h>
#include <salts/random.h>
#include <cmeta_error.h>
#include <stdlib.h>
#include <string.h>

typedef struct tr_multicore_group {
    tr_raft_group_assignment_t assignment;
    tr_raft_service_t *service;
    ring_data_type requests;
    ring_data_type completions;
    tr_raft_multicore_group_status_t status;
    uint64_t last_tick_ms;
} tr_multicore_group_t;

struct tr_raft_owner {
    tr_raft_multicore_t *runtime;
    uint32_t index;
    cmeta_thread_t thread;
    cmeta_mutex_t mutex;
    cmeta_cond_t wake;
    const void *thread_token;
    bool stopping;
    bool opened;
};

struct tr_raft_multicore {
    tr_raft_multicore_config_t config;
    tr_raft_multicore_factory_t factory;
    tr_raft_owner_t *owners;
    tr_multicore_group_t *groups;
    cmeta_mutex_t mutex;
    cmeta_cond_t changed;
    size_t ready_count;
    size_t exited_count;
    int error;
    bool released;
    bool joining;
    bool joined;
};

/* Unlike native thread tokens, this cannot match a recycled, exited thread. */
static SALTS_THREAD_LOCAL tr_raft_multicore_t *tr_active_multicore;

int tr_raft_multicore_config_validate(const tr_raft_multicore_config_t *c)
{
    bool used[TR_RAFT_MULTICORE_MAX_OWNERS] = {false};
    size_t i, j;
    if (c == NULL || c->version != TR_RAFT_MULTICORE_VERSION ||
        c->owner_count == 0U || c->owner_count > TR_RAFT_MULTICORE_MAX_OWNERS ||
        c->capacity == 0U || c->capacity > TR_RAFT_MULTICORE_MAX_CAPACITY ||
        c->work_budget == 0U || c->work_budget > c->capacity ||
        c->tick_ms == 0U || c->tick_ms > 60000U ||
        c->idle_ms == 0U || c->idle_ms > c->tick_ms ||
        c->groups == NULL || c->group_count == 0U ||
        c->group_count > TR_RAFT_MULTICORE_MAX_GROUPS)
        return SALTS_EINVAL;
    for (i = 0; i < c->group_count; ++i) {
        const tr_raft_group_assignment_t *g = &c->groups[i];
        if (g->group_id == 0U || g->owner_index >= c->owner_count ||
            g->election_min_ticks == 0U ||
            g->election_max_ticks < g->election_min_ticks)
            return SALTS_EINVAL;
        used[g->owner_index] = true;
        for (j = 0; j < i; ++j)
            if (g->group_id == c->groups[j].group_id) return SALTS_EINVAL;
    }
    for (i = 0; i < c->owner_count; ++i)
        if (!used[i]) return SALTS_EINVAL;
    /* Preflight all queue arithmetic, including the ring's spare element. */
    if ((size_t)c->capacity + 1U > SIZE_MAX / sizeof(tr_raft_multicore_request_t) ||
        (size_t)c->capacity + 1U > SIZE_MAX / sizeof(tr_raft_multicore_completion_t))
        return SALTS_ERANGE;
    if (((uint64_t)c->capacity + 1U) * c->group_count *
        (sizeof(tr_raft_multicore_request_t) + sizeof(tr_raft_multicore_completion_t)) >
        TR_RAFT_MULTICORE_MAX_QUEUE_BYTES) return SALTS_ERANGE;
    /* These are admission-time bytes, not a preallocated second queue.
     * Enforce a hard process-independent upper bound even for 1024 Groups. */
    if ((uint64_t)c->owned_chunk_bytes_per_group >
            TR_RAFT_MULTICORE_MAX_CHUNK_LEASE_BYTES ||
        (c->owned_chunk_bytes_per_group != 0U &&
         c->group_count >
             TR_RAFT_MULTICORE_MAX_CHUNK_LEASE_BYTES /
                 c->owned_chunk_bytes_per_group))
        return SALTS_ERANGE;
    return SALTS_OK;
}

static tr_multicore_group_t *tr_multicore_find(tr_raft_multicore_t *r, uint64_t id)
{
    size_t i;
    if (r != NULL)
        for (i = 0; i < r->config.group_count; ++i)
            if (r->groups[i].assignment.group_id == id) return &r->groups[i];
    return NULL;
}

uint32_t tr_raft_owner_index(const tr_raft_owner_t *owner)
{
    return owner != NULL ? owner->index : UINT32_MAX;
}

tr_raft_service_t *tr_raft_owner_service(tr_raft_owner_t *owner, uint64_t id)
{
    tr_multicore_group_t *g;
    if (owner == NULL || owner->thread_token != cmeta_thread_current_token())
        return NULL;
    g = tr_multicore_find(owner->runtime, id);
    return g != NULL && g->assignment.owner_index == owner->index ? g->service : NULL;
}

bool tr_raft_owner_contains(tr_raft_owner_t *owner, uint64_t id)
{
    tr_multicore_group_t *g;
    if (owner == NULL || owner->thread_token != cmeta_thread_current_token()) return false;
    g = tr_multicore_find(owner->runtime, id);
    return g != NULL && g->assignment.owner_index == owner->index;
}

static int tr_multicore_execute(tr_raft_service_t *s,
                                const tr_raft_multicore_request_t *q,
                                tr_raft_multicore_completion_t *c)
{
    switch (q->operation) {
    case TR_RAFT_MULTICORE_PROPOSE: {
        tr_raft_proposal_t p = {q->value.proposal.command_id,
                                q->value.proposal.data, q->value.proposal.size};
        return tr_raft_service_propose_with_receipt(s, &p, &c->value.receipt);
    }
    case TR_RAFT_MULTICORE_STEP:
        return tr_raft_service_step(s, &q->value.message);
    case TR_RAFT_MULTICORE_STATUS:
        return tr_raft_service_status(s, &c->value.status);
    case TR_RAFT_MULTICORE_READ_INDEX:
        return tr_raft_service_read_index(s, q->value.context_id);
    case TR_RAFT_MULTICORE_TAKE_READ:
        return tr_raft_service_take_read_state(s, &c->value.read);
    case TR_RAFT_MULTICORE_TRANSFER:
        return tr_raft_service_transfer_leadership(s, q->value.transferee_id);
    case TR_RAFT_MULTICORE_MEMBERSHIP: {
        tr_raft_membership_change_t m = {
            q->value.membership.transition_id,
            q->value.membership.voters, q->value.membership.voter_count,
            q->value.membership.learners, q->value.membership.learner_count};
        return tr_raft_service_change_membership_with_receipt(s, &m, &c->value.receipt);
    }
    case TR_RAFT_MULTICORE_SNAPSHOT:
        return tr_raft_service_trigger_snapshot(s);
    case TR_RAFT_MULTICORE_OPERATION_STATUS:
        return tr_raft_service_operation_status(s, q->value.operation_status.term,
                 q->value.operation_status.index, &c->value.receipt);
    default: return SALTS_EINVAL;
    }
}

/* Both queues are serialized by owner->mutex. No allocations or host calls
 * happen under this lock. Credits reserve completion space before execution. */
static bool tr_multicore_work(tr_raft_owner_t *o, tr_multicore_group_t *g)
{
    tr_raft_multicore_request_t q;
    tr_raft_multicore_completion_t c = {0};
    uint8_t *slot;
    size_t available;
    bool stopping;
    int background_error;
    cmeta_mutex_lock(&o->mutex);
    slot = ring_read_acquire(&g->requests, &available);
    if (available == 0U) {
        cmeta_mutex_unlock(&o->mutex);
        return false;
    }
    memcpy(&q, slot, sizeof(q));
    ring_read_release(&g->requests, sizeof(q));
    --g->status.queued;
    stopping = o->stopping;
    background_error = g->status.background_error;
    cmeta_mutex_unlock(&o->mutex);
    c.request_id = q.request_id;
    c.operation = q.operation;
    c.reply_origin = q.reply_origin;
    if (q.operation == TR_RAFT_MULTICORE_RECEIVE_CHUNK) {
        tr_raft_owned_transport_payload_t *owned =
            (tr_raft_owned_transport_payload_t *)q.value.chunk.internal_owned_chunk;
        size_t bytes;
        if (owned == NULL) abort(); /* only internal admission can enqueue */
        bytes = owned->payload.kind == TR_RAFT_WIRE_PAYLOAD_DATA_CHUNK
            ? owned->payload.data.data_chunk.data_length
            : owned->payload.data.snapshot_chunk.data_length;
        c.value.chunk.kind = owned->payload.kind;
        if (stopping) c.result = SALTS_ECANCELED;
        else if (background_error != SALTS_OK) c.result = background_error;
        else c.result = o->runtime->factory.receive_chunk(
                o->runtime->factory.context, o, g->assignment.group_id,
                &owned->payload, &c.value.chunk);
        /* Callback has returned on the one assigned Group Owner, so its
         * borrowed view is no longer live. The exact retained Salts Core
         * buffer is released before publishing the completion. */
        tr_raft_owned_transport_payload_release(owned);
        free(owned);
        cmeta_mutex_lock(&o->mutex);
        if (g->status.owned_chunk_bytes < bytes) abort();
        g->status.owned_chunk_bytes -= bytes;
    } else {
        c.result = stopping ? SALTS_ECANCELED :
            background_error != SALTS_OK && q.operation != TR_RAFT_MULTICORE_STATUS ?
            background_error : tr_multicore_execute(g->service, &q, &c);
        cmeta_mutex_lock(&o->mutex);
    }
    slot = ring_write_acquire(&g->completions, sizeof(c));
    /* A missing reserved slot is an internal invariant violation. */
    if (slot == NULL) abort();
    memcpy(slot, &c, sizeof(c));
    ring_write_release(&g->completions, sizeof(c));
    ++g->status.completed;
    cmeta_mutex_unlock(&o->mutex);
    return true;
}

static void tr_multicore_record_error(tr_raft_owner_t *o,
                                      tr_multicore_group_t *g, int error)
{
    if (error == SALTS_OK) return;
    cmeta_mutex_lock(&o->mutex);
    if (g != NULL && g->status.background_error == SALTS_OK)
        g->status.background_error = error;
    cmeta_mutex_unlock(&o->mutex);
    cmeta_mutex_lock(&o->runtime->mutex);
    if (o->runtime->error == SALTS_OK) o->runtime->error = error;
    cmeta_mutex_unlock(&o->runtime->mutex);
}

static void tr_multicore_tick(tr_raft_owner_t *o, tr_multicore_group_t *g)
{
    const uint64_t now = cmeta_monotonic_ms();
    uint64_t elapsed = (now - g->last_tick_ms) / o->runtime->config.tick_ms;
    uint32_t random;
    tr_raft_tick_t tick;
    int result;
    if (elapsed == 0U) return;
    result = cmeta_platform_secure_random(&random, sizeof(random));
    if (result == SALTS_OK) {
        /* Split a very long pause over multiple rounds without losing time. */
        tick.elapsed_ticks = elapsed > UINT32_MAX ? UINT32_MAX : (uint32_t)elapsed;
        tick.next_election_timeout_ticks = g->assignment.election_min_ticks +
            (uint32_t)((uint64_t)random %
            ((uint64_t)g->assignment.election_max_ticks - g->assignment.election_min_ticks + 1U));
        result = tr_raft_service_tick(g->service, &tick);
        g->last_tick_ms += (uint64_t)tick.elapsed_ticks * o->runtime->config.tick_ms;
    }
    tr_multicore_record_error(o, g, result);
}

static void tr_multicore_owner_main(void *context)
{
    tr_raft_owner_t *o = context;
    tr_raft_multicore_t *r = o->runtime;
    size_t i, n;
    int result = SALTS_OK;
    o->thread_token = cmeta_thread_current_token();
    tr_active_multicore = r;
    if (r->factory.owner_open != NULL)
        result = r->factory.owner_open(r->factory.context, o);
    o->opened = result == SALTS_OK;
    for (i = 0; result == SALTS_OK && i < r->config.group_count; ++i) {
        tr_multicore_group_t *g = &r->groups[i];
        if (g->assignment.owner_index != o->index) continue;
        result = r->factory.group_open(r->factory.context, o,
                                       g->assignment.group_id, &g->service);
        if (result == SALTS_OK && g->service == NULL) result = SALTS_EINVAL;
        g->last_tick_ms = cmeta_monotonic_ms();
    }
    cmeta_mutex_lock(&r->mutex);
    if (r->error == SALTS_OK) r->error = result;
    ++r->ready_count;
    cmeta_cond_broadcast(&r->changed);
    while (!r->released) cmeta_cond_wait(&r->changed, &r->mutex);
    cmeta_mutex_unlock(&r->mutex);

    for (;;) {
        bool stopping, work = false;
        cmeta_mutex_lock(&o->mutex);
        stopping = o->stopping;
        cmeta_mutex_unlock(&o->mutex);
        if (!stopping && r->factory.owner_poll != NULL) {
            result = r->factory.owner_poll(r->factory.context, o);
            if (result != SALTS_OK) {
                tr_multicore_record_error(o, NULL, result);
                tr_raft_multicore_request_stop(r);
                stopping = true;
            }
        }
        for (i = 0; i < r->config.group_count; ++i) {
            tr_multicore_group_t *g = &r->groups[i];
            int error;
            if (g->assignment.owner_index != o->index) continue;
            cmeta_mutex_lock(&o->mutex);
            error = g->status.background_error;
            stopping = o->stopping;
            cmeta_mutex_unlock(&o->mutex);
            if (!stopping && error == SALTS_OK) tr_multicore_tick(o, g);
            for (n = 0; n < r->config.work_budget; ++n) {
                if (!tr_multicore_work(o, g)) break;
                work = true;
            }
        }
        if (stopping && !work) break;
        if (!work) {
            /* Inspect queues under the same lock as submit + wake, avoiding a
             * lost wake between the last group scan and condition wait. */
            cmeta_mutex_lock(&o->mutex);
            for (i = 0; i < r->config.group_count; ++i)
                if (r->groups[i].assignment.owner_index == o->index &&
                    r->groups[i].status.queued != 0U) break;
            if (!o->stopping && i == r->config.group_count)
                (void)cmeta_cond_timedwait(&o->wake, &o->mutex,
                    (uint64_t)r->config.idle_ms * UINT64_C(1000000));
            cmeta_mutex_unlock(&o->mutex);
        }
    }
    for (i = r->config.group_count; i > 0U; --i) {
        tr_multicore_group_t *g = &r->groups[i - 1U];
        if (g->assignment.owner_index != o->index) continue;
        if (g->service != NULL) {
            tr_raft_service_destroy(g->service);
            g->service = NULL;
            r->factory.group_close(r->factory.context, o, g->assignment.group_id);
        }
        cmeta_mutex_lock(&o->mutex);
        g->status.stopped = true;
        cmeta_mutex_unlock(&o->mutex);
    }
    if (o->opened && r->factory.owner_close != NULL)
        r->factory.owner_close(r->factory.context, o);
    cmeta_mutex_lock(&r->mutex);
    ++r->exited_count;
    cmeta_cond_broadcast(&r->changed);
    cmeta_mutex_unlock(&r->mutex);
    tr_active_multicore = NULL;
}

int tr_raft_multicore_submit(tr_raft_multicore_t *r, uint64_t id,
                            const tr_raft_multicore_request_t *q)
{
    tr_multicore_group_t *g;
    tr_raft_owner_t *o;
    uint8_t *slot;
    int result = SALTS_OK;
    if (r == NULL || q == NULL || q->operation < TR_RAFT_MULTICORE_PROPOSE ||
        q->operation > TR_RAFT_MULTICORE_OPERATION_STATUS ||
        (q->operation == TR_RAFT_MULTICORE_PROPOSE && q->value.proposal.size > TR_RAFT_MAX_ENTRY_BYTES) ||
        (q->operation == TR_RAFT_MULTICORE_MEMBERSHIP &&
         (q->value.membership.voter_count > TR_RAFT_MAX_MEMBERS ||
          q->value.membership.learner_count > TR_RAFT_MAX_MEMBERS)))
        return SALTS_EINVAL;
    g = tr_multicore_find(r, id);
    if (g == NULL) return SALTS_ENOENT;
    o = &r->owners[g->assignment.owner_index];
    cmeta_mutex_lock(&o->mutex);
    if (o->stopping) result = SALTS_ECANCELED;
    else if (g->status.outstanding == r->config.capacity) {
        result = SALTS_ENOSPC;
        if (g->status.rejected != UINT64_MAX) ++g->status.rejected;
    } else {
        slot = ring_write_acquire(&g->requests, sizeof(*q));
        if (slot == NULL) abort();
        memcpy(slot, q, sizeof(*q));
        ring_write_release(&g->requests, sizeof(*q));
        ++g->status.outstanding;
        ++g->status.queued;
        cmeta_cond_signal(&o->wake);
    }
    cmeta_mutex_unlock(&o->mutex);
    return result;
}

/* Private path: the existing per-Group request+completion ring is the only
 * queue. Reserve item AND bytes before copying any borrowed CNet data.
 * Holding the existing owner mutex across one bounded (<=64KiB) materialize
 * guarantees stop/submit races cannot leave an untracked in-flight lease. */
int tr_raft_multicore_submit_owned_chunk(
    tr_raft_multicore_t *r,
    const tr_raft_transport_payload_t *payload,
    uint64_t request_id,
    const tr_raft_transport_reply_origin_t *reply_origin)
{
    tr_multicore_group_t *g;
    tr_raft_owner_t *o;
    tr_raft_owned_transport_payload_t *owned = NULL;
    tr_raft_multicore_request_t q = {0};
    uint8_t *slot;
    size_t bytes;
    int result = SALTS_OK;

    if (r == NULL || payload == NULL || request_id == 0U ||
        payload->group_id == 0U)
        return SALTS_EINVAL;
    if (payload->kind == TR_RAFT_WIRE_PAYLOAD_DATA_CHUNK) {
        const tr_raft_data_chunk_t *chunk = &payload->data.data_chunk;
        bytes = chunk->data_length;
        if (chunk->from == 0U || chunk->to == 0U ||
            chunk->from == chunk->to || chunk->term == 0U ||
            bytes > TR_RAFT_WIRE_MAX_DATA_CHUNK_BYTES ||
            (bytes != 0U && chunk->data == NULL))
            return SALTS_EPROTO;
    } else if (payload->kind == TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_CHUNK) {
        const tr_raft_snapshot_chunk_t *chunk =
            &payload->data.snapshot_chunk;
        bytes = chunk->data_length;
        if (chunk->from == 0U || chunk->to == 0U ||
            chunk->from == chunk->to || chunk->term == 0U ||
            bytes > TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES ||
            (bytes != 0U && chunk->data == NULL))
            return SALTS_EPROTO;
    } else return SALTS_ENOTSUP;

    if (reply_origin != NULL &&
        (reply_origin->channel_instance == 0U ||
         reply_origin->authenticated_peer_node_id == 0U ||
         reply_origin->group_id != payload->group_id ||
         reply_origin->connection_token == 0U ||
         reply_origin->authenticated_peer_node_id !=
             (payload->kind == TR_RAFT_WIRE_PAYLOAD_DATA_CHUNK
                  ? payload->data.data_chunk.from
                  : payload->data.snapshot_chunk.from)))
        return SALTS_EPROTO;
    if (r->factory.receive_chunk == NULL ||
        r->config.owned_chunk_bytes_per_group == 0U)
        return SALTS_ENOTSUP;
    g = tr_multicore_find(r, payload->group_id);
    if (g == NULL) return SALTS_ENOENT;
    o = &r->owners[g->assignment.owner_index];
    cmeta_mutex_lock(&o->mutex);
    if (o->stopping) result = SALTS_ECANCELED;
    else if (g->status.outstanding >= r->config.capacity ||
             bytes > r->config.owned_chunk_bytes_per_group -
                         g->status.owned_chunk_bytes) {
        result = SALTS_ENOSPC;
        if (g->status.rejected != UINT64_MAX) ++g->status.rejected;
    } else {
        owned = (tr_raft_owned_transport_payload_t *)calloc(1U, sizeof(*owned));
        if (owned == NULL) result = SALTS_ENOMEM;
        else result = tr_raft_owned_transport_payload_copy(owned, payload);
        if (result == SALTS_OK) {
            q.operation = TR_RAFT_MULTICORE_RECEIVE_CHUNK;
            q.request_id = request_id;
            if (reply_origin != NULL) q.reply_origin = *reply_origin;
            q.value.chunk.internal_owned_chunk = owned;
            slot = ring_write_acquire(&g->requests, sizeof(q));
            if (slot == NULL) abort(); /* item credit already reserved */
            memcpy(slot, &q, sizeof(q));
            ring_write_release(&g->requests, sizeof(q));
            ++g->status.outstanding;
            ++g->status.queued;
            g->status.owned_chunk_bytes += bytes;
            cmeta_cond_signal(&o->wake);
            owned = NULL; /* ring owns exactly one lease now */
        }
    }
    cmeta_mutex_unlock(&o->mutex);
    if (owned != NULL) {
        tr_raft_owned_transport_payload_release(owned);
        free(owned);
    }
    return result;
}

int tr_raft_multicore_take(tr_raft_multicore_t *r, uint64_t id,
                          tr_raft_multicore_completion_t *c)
{
    tr_multicore_group_t *g;
    tr_raft_owner_t *o;
    uint8_t *slot;
    size_t available;
    int result = SALTS_ENOENT;
    if (r == NULL || c == NULL) return SALTS_EINVAL;
    g = tr_multicore_find(r, id);
    if (g == NULL) return SALTS_ENOENT;
    o = &r->owners[g->assignment.owner_index];
    cmeta_mutex_lock(&o->mutex);
    slot = ring_read_acquire(&g->completions, &available);
    if (available != 0U) {
        memcpy(c, slot, sizeof(*c));
        ring_read_release(&g->completions, sizeof(*c));
        --g->status.completed;
        --g->status.outstanding;
        result = SALTS_OK;
    }
    cmeta_mutex_unlock(&o->mutex);
    return result;
}

int tr_raft_multicore_group_status(tr_raft_multicore_t *r, uint64_t id,
                                  tr_raft_multicore_group_status_t *status)
{
    tr_multicore_group_t *g;
    tr_raft_owner_t *o;
    if (r == NULL || status == NULL) return SALTS_EINVAL;
    g = tr_multicore_find(r, id);
    if (g == NULL) return SALTS_ENOENT;
    o = &r->owners[g->assignment.owner_index];
    cmeta_mutex_lock(&o->mutex);
    *status = g->status;
    cmeta_mutex_unlock(&o->mutex);
    return SALTS_OK;
}

void tr_raft_multicore_request_stop(tr_raft_multicore_t *r)
{
    size_t i;
    if (r == NULL) return;
    for (i = 0; i < r->config.owner_count; ++i) {
        cmeta_mutex_lock(&r->owners[i].mutex);
        r->owners[i].stopping = true;
        cmeta_cond_broadcast(&r->owners[i].wake);
        cmeta_mutex_unlock(&r->owners[i].mutex);
    }
}

int tr_raft_multicore_wait_stopped(tr_raft_multicore_t *r, uint32_t timeout_ms)
{
    const uint64_t began = cmeta_monotonic_ms();
    int result;
    if (r == NULL) return SALTS_EINVAL;
    if (tr_active_multicore == r) return SALTS_EBUSY;
    cmeta_mutex_lock(&r->mutex);
    while (r->exited_count != r->config.owner_count) {
        uint64_t elapsed = cmeta_monotonic_ms() - began;
        if (elapsed >= timeout_ms) {
            cmeta_mutex_unlock(&r->mutex);
            return SALTS_ETIMEDOUT;
        }
        (void)cmeta_cond_timedwait(&r->changed, &r->mutex,
            ((uint64_t)timeout_ms - elapsed) * UINT64_C(1000000));
    }
    result = r->error;
    cmeta_mutex_unlock(&r->mutex);
    return result;
}

int tr_raft_multicore_stop(tr_raft_multicore_t *r)
{
    size_t i;
    int result;
    if (r == NULL) return SALTS_EINVAL;
    if (tr_active_multicore == r) return SALTS_EBUSY;
    cmeta_mutex_lock(&r->mutex);
    while (r->joining) cmeta_cond_wait(&r->changed, &r->mutex);
    if (r->joined) {
        result = r->error;
        cmeta_mutex_unlock(&r->mutex);
        return result;
    }
    r->joining = true;
    cmeta_mutex_unlock(&r->mutex);
    tr_raft_multicore_request_stop(r);
    for (i = 0; i < r->config.owner_count; ++i) {
        if (r->owners[i].thread != NULL && cmeta_thread_join(&r->owners[i].thread) != 0)
            abort(); /* Never free resources while an owner might still run. */
    }
    cmeta_mutex_lock(&r->mutex);
    r->joining = false;
    r->joined = true;
    result = r->error;
    cmeta_cond_broadcast(&r->changed);
    cmeta_mutex_unlock(&r->mutex);
    return result;
}

static void tr_multicore_free(tr_raft_multicore_t *r)
{
    size_t i;
    if (r->owners != NULL)
        for (i = 0; i < r->config.owner_count; ++i) {
            cmeta_cond_destroy(&r->owners[i].wake);
            cmeta_mutex_destroy(&r->owners[i].mutex);
        }
    if (r->groups != NULL)
        for (i = 0; i < r->config.group_count; ++i) {
            free(r->groups[i].requests.data);
            free(r->groups[i].completions.data);
        }
    cmeta_cond_destroy(&r->changed);
    cmeta_mutex_destroy(&r->mutex);
    free(r->owners);
    free(r->groups);
    free(r);
}

void tr_raft_multicore_destroy(tr_raft_multicore_t *r)
{
    if (r == NULL) return;
    if (tr_active_multicore == r) return;
    (void)tr_raft_multicore_stop(r);
    tr_multicore_free(r);
}

int tr_raft_multicore_create(const tr_raft_multicore_config_t *c,
                            const tr_raft_multicore_factory_t *f,
                            tr_raft_multicore_t **out)
{
    tr_raft_multicore_t *r;
    size_t i, threads = 0;
    int result;
    if (out == NULL) return SALTS_EINVAL;
    *out = NULL;
    result = tr_raft_multicore_config_validate(c);
    if (result != SALTS_OK) return result;
    if (f == NULL || f->group_open == NULL || f->group_close == NULL ||
        ((f->owner_open != NULL || f->owner_poll != NULL || f->owner_close != NULL) &&
         (f->owner_open == NULL || f->owner_poll == NULL || f->owner_close == NULL)) ||
        ((c->owned_chunk_bytes_per_group != 0U) !=
         (f->receive_chunk != NULL)))
        return SALTS_EINVAL;
    r = calloc(1U, sizeof(*r));
    if (r == NULL) return SALTS_ENOMEM;
    r->config = *c;
    r->config.groups = NULL; /* assignments below are the runtime's only copy */
    r->factory = *f;
    r->owners = calloc(c->owner_count, sizeof(*r->owners));
    r->groups = calloc(c->group_count, sizeof(*r->groups));
    cmeta_mutex_init(&r->mutex);
    cmeta_cond_init(&r->changed);
    if (r->owners == NULL || r->groups == NULL || r->mutex == NULL || r->changed == NULL) {
        tr_multicore_free(r);
        return SALTS_ENOMEM;
    }
    for (i = 0; i < c->owner_count; ++i) {
        r->owners[i].runtime = r;
        r->owners[i].index = (uint32_t)i;
        cmeta_mutex_init(&r->owners[i].mutex);
        cmeta_cond_init(&r->owners[i].wake);
        if (r->owners[i].mutex == NULL || r->owners[i].wake == NULL) {
            tr_multicore_free(r);
            return SALTS_ENOMEM;
        }
    }
    for (i = 0; i < c->group_count; ++i) {
        const size_t request_bytes = ((size_t)c->capacity + 1U) * sizeof(tr_raft_multicore_request_t);
        const size_t completion_bytes = ((size_t)c->capacity + 1U) * sizeof(tr_raft_multicore_completion_t);
        tr_multicore_group_t *g = &r->groups[i];
        uint8_t *requests = malloc(request_bytes);
        uint8_t *completions = malloc(completion_bytes);
        if (requests == NULL || completions == NULL) {
            free(requests);
            free(completions);
            tr_multicore_free(r);
            return SALTS_ENOMEM;
        }
        g->assignment = c->groups[i];
        ring_init(&g->requests, requests, request_bytes);
        ring_init(&g->completions, completions, completion_bytes);
    }
    for (i = 0; i < c->owner_count; ++i) {
        if (cmeta_thread_create(&r->owners[i].thread, tr_multicore_owner_main, &r->owners[i]) != 0) {
            result = SALTS_EIO;
            break;
        }
        ++threads;
    }
    cmeta_mutex_lock(&r->mutex);
    while (r->ready_count != threads) cmeta_cond_wait(&r->changed, &r->mutex);
    if (result == SALTS_OK) result = r->error;
    if (result != SALTS_OK) tr_raft_multicore_request_stop(r);
    r->released = true;
    cmeta_cond_broadcast(&r->changed);
    cmeta_mutex_unlock(&r->mutex);
    if (result != SALTS_OK) {
        (void)tr_raft_multicore_stop(r);
        tr_multicore_free(r);
        return result;
    }
    *out = r;
    return SALTS_OK;
}
