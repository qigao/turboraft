#include <turboraft/raft_flowmq_owner.h>
#include <salts/thread.h>
#include <cmeta_error.h>
#include <stdlib.h>

typedef struct tr_flowmq_group_binding {
    struct tr_raft_flowmq_owner *link;
    uint64_t group_id;
} tr_flowmq_group_binding_t;

struct tr_raft_flowmq_owner {
    tr_raft_owner_t *owner;
    tr_raft_flowmq_peer_service_t *service;
    tr_flowmq_group_binding_t *groups;
    size_t group_count;
    tr_raft_transport_payload_handler_fn on_payload;
    void *payload_context;
    const void *thread;
};

static tr_flowmq_group_binding_t *tr_flowmq_binding(tr_raft_flowmq_owner_t *link, uint64_t id)
{
    size_t i;
    if (link == NULL || link->thread != cmeta_thread_current_token()) return NULL;
    for (i = 0; i < link->group_count; ++i)
        if (link->groups[i].group_id == id) return &link->groups[i];
    return NULL;
}

static int tr_flowmq_owner_receive(void *context, const tr_raft_transport_payload_t *payload)
{
    tr_raft_flowmq_owner_t *link = context;
    tr_raft_service_t *service;
    if (tr_flowmq_binding(link, payload->group_id) == NULL) return SALTS_ENOENT;
    service = tr_raft_owner_service(link->owner, payload->group_id);
    if (service == NULL) return SALTS_ENOENT;
    if (payload->kind == TR_RAFT_WIRE_PAYLOAD_RAFT)
        return tr_raft_service_step(service, &payload->data.raft);
    return link->on_payload != NULL ? link->on_payload(link->payload_context, payload) : SALTS_ENOTSUP;
}

static int tr_flowmq_owner_send(void *context, const tr_raft_message_t *message)
{
    tr_flowmq_group_binding_t *g = context;
    if (g->link->thread != cmeta_thread_current_token()) return SALTS_EINVAL;
    return tr_raft_flowmq_peer_service_enqueue_group(g->link->service, g->group_id, message);
}

int tr_raft_flowmq_owner_create(tr_raft_owner_t *owner,
                                const tr_raft_flowmq_peer_service_config_t *config,
                                const uint64_t *ids, size_t count,
                                tr_raft_flowmq_owner_t **out)
{
    tr_raft_flowmq_owner_t *link;
    tr_raft_flowmq_peer_service_config_t peer;
    size_t i, j;
    int result;
    if (out == NULL) return SALTS_EINVAL;
    *out = NULL;
    if (owner == NULL || config == NULL || ids == NULL || count == 0U ||
        count > TR_RAFT_MULTICORE_MAX_GROUPS || count > config->outbound_limits.max_active_groups)
        return SALTS_EINVAL;
    for (i = 0; i < count; ++i) {
        if (!tr_raft_owner_contains(owner, ids[i])) return SALTS_EINVAL;
        for (j = 0; j < i; ++j) if (ids[i] == ids[j]) return SALTS_EINVAL;
    }
    link = calloc(1U, sizeof(*link));
    if (link == NULL) return SALTS_ENOMEM;
    link->groups = calloc(count, sizeof(*link->groups));
    if (link->groups == NULL) { free(link); return SALTS_ENOMEM; }
    link->owner = owner;
    link->thread = cmeta_thread_current_token();
    link->group_count = count;
    link->on_payload = config->on_payload;
    link->payload_context = config->payload_context;
    for (i = 0; i < count; ++i) {
        link->groups[i].link = link;
        link->groups[i].group_id = ids[i];
    }
    peer = *config;
    peer.on_payload = tr_flowmq_owner_receive;
    peer.payload_context = link;
    result = tr_raft_flowmq_peer_service_create(&peer, &link->service);
    if (result == SALTS_OK) result = tr_raft_flowmq_peer_service_start(link->service);
    if (result != SALTS_OK) {
        (void)tr_raft_flowmq_peer_service_stop(link->service);
        (void)tr_raft_flowmq_peer_service_destroy(link->service);
        free(link->groups);
        free(link);
        return result;
    }
    *out = link;
    return SALTS_OK;
}

int tr_raft_flowmq_owner_bind(tr_raft_flowmq_owner_t *link, uint64_t id, tr_raft_transport_t *transport)
{
    tr_flowmq_group_binding_t *g = tr_flowmq_binding(link, id);
    if (transport == NULL) return SALTS_EINVAL;
    if (g == NULL) return SALTS_ENOENT;
    transport->context = g;
    transport->enqueue = tr_flowmq_owner_send;
    return SALTS_OK;
}

int tr_raft_flowmq_owner_poll(tr_raft_flowmq_owner_t *link)
{
    tr_raft_flowmq_peer_service_step_result_t step;
    if (link == NULL || link->thread != cmeta_thread_current_token()) return SALTS_EINVAL;
    return tr_raft_flowmq_peer_service_step(link->service, &step);
}

int tr_raft_flowmq_owner_destroy(tr_raft_flowmq_owner_t *link)
{
    int result;
    if (link == NULL) return SALTS_OK;
    if (link->thread != cmeta_thread_current_token()) return SALTS_EINVAL;
    result = tr_raft_flowmq_peer_service_stop(link->service);
    if (result != SALTS_OK) return result;
    result = tr_raft_flowmq_peer_service_destroy(link->service);
    if (result != SALTS_OK) return result;
    free(link->groups);
    free(link);
    return SALTS_OK;
}

tr_raft_flowmq_peer_service_t *tr_raft_flowmq_owner_peer_service(tr_raft_flowmq_owner_t *link)
{
    return link != NULL && link->thread == cmeta_thread_current_token() ? link->service : NULL;
}
