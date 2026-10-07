#ifndef TURBORAFT_MULTICORE_FIXTURE_H
#define TURBORAFT_MULTICORE_FIXTURE_H
#include <turboraft/raft_multicore.h>
#include <turboraft/raft_flowmq_owner.h>
#include <salts/thread.h>
#include <salts/clock.h>
#include <cmeta_error.h>
#include <tinytest.h>
#include <string.h>

#define TEST_GROUPS 8U
#define TEST_TIMEOUT_MS 10000U

typedef struct multicore_test_group {
    tr_raft_node_id_t self;
    const void *thread;
    uint64_t last_command;
    size_t applied;
    bool opened, closed, wrong_thread, wrong_order;
} multicore_test_group_t;

typedef struct multicore_test {
    multicore_test_group_t groups[TEST_GROUPS];
    cmeta_mutex_t mutex;
    cmeta_cond_t changed;
    bool block, entered, release;
    uint64_t fail_group;
    size_t owner_opened[4], owner_closed[4];
    tr_raft_multicore_t *runtime;
    tr_raft_group_assignment_t assignments[TEST_GROUPS];
    tr_raft_multicore_config_t config;
    tr_raft_node_id_t node;
    tr_raft_node_id_t voters[2];
    bool network;
    tr_raft_flowmq_peer_service_config_t network_config[4];
    tr_raft_flowmq_peer_config_t peer_config[4];
    tr_raft_handshake_result_t handshake[4];
    tr_raft_flowmq_owner_t *links[4];
    char endpoints[4][128];
    const char *fingerprint;
    int network_close_error;
    size_t max_log_entries;
} multicore_test_t;

static void test_thread(multicore_test_group_t *g)
{
    if (g->thread != cmeta_thread_current_token()) g->wrong_thread = true;
}
static int storage_begin(void *p) { test_thread(p); return SALTS_OK; }
static int storage_hard(void *p, tr_raft_term_t t, tr_raft_node_id_t n)
{ (void)t; (void)n; return storage_begin(p); }
static int storage_index(void *p, tr_raft_index_t i)
{ (void)i; return storage_begin(p); }
static int storage_entries(void *p, const tr_raft_entry_t *e, size_t n)
{ (void)e; (void)n; return storage_begin(p); }
static int transport_send(void *p, const tr_raft_message_t *m)
{ (void)m; return storage_begin(p); }
static int apply_entries(void *p, const tr_raft_entry_t *e, size_t n)
{
    multicore_test_group_t *g = p;
    size_t i;
    test_thread(g);
    for (i = 0; i < n; ++i) {
        if (e[i].command_id != g->last_command + 1U ||
            e[i].data_length != sizeof(uint64_t) ||
            memcmp(e[i].data, &e[i].command_id, sizeof(uint64_t)) != 0)
            g->wrong_order = true;
        g->last_command = e[i].command_id;
        ++g->applied;
    }
    return SALTS_OK;
}
static int owner_open(void *p, tr_raft_owner_t *o)
{
    multicore_test_t *f = p;
    const uint32_t index = tr_raft_owner_index(o);
    ++f->owner_opened[index];
    if (f->network) {
        uint64_t ids[TEST_GROUPS];
        size_t count = 0;
        for (size_t i = 0; i < TEST_GROUPS; ++i)
            if (f->assignments[i].owner_index == index) ids[count++] = i + 1U;
        return tr_raft_flowmq_owner_create(o, &f->network_config[index], ids, count, &f->links[index]);
    }
    return SALTS_OK;
}
static int owner_poll(void *p, tr_raft_owner_t *o)
{
    multicore_test_t *f = p;
    if (tr_raft_owner_index(o) == 0U) {
        cmeta_mutex_lock(&f->mutex);
        if (f->block && !f->release) {
            f->entered = true;
            cmeta_cond_broadcast(&f->changed);
            while (!f->release) cmeta_cond_wait(&f->changed, &f->mutex);
        }
        cmeta_mutex_unlock(&f->mutex);
    }
    return f->network ? tr_raft_flowmq_owner_poll(f->links[tr_raft_owner_index(o)]) : SALTS_OK;
}
static void owner_close(void *p, tr_raft_owner_t *o)
{
    multicore_test_t *f = p;
    ++f->owner_closed[tr_raft_owner_index(o)];
    if (f->network) {
        int result = tr_raft_flowmq_owner_destroy(f->links[tr_raft_owner_index(o)]);
        cmeta_mutex_lock(&f->mutex);
        if (result != SALTS_OK) f->network_close_error = result;
        cmeta_mutex_unlock(&f->mutex);
    }
}
static int group_open(void *p, tr_raft_owner_t *o, uint64_t id, tr_raft_service_t **out)
{
    multicore_test_t *f = p;
    multicore_test_group_t *g = &f->groups[id - 1U];
    tr_raft_service_config_t c = {0};
    tr_raft_tick_t tick = {3U, 3U};
    int result;
    (void)o;
    *out = NULL;
    if (id == f->fail_group) return SALTS_EIO;
    g->self = 1U;
    g->thread = cmeta_thread_current_token();
    c.core.self_id = f->node;
    c.core.voters = f->network ? f->voters : &g->self;
    c.core.voter_count = f->network ? 2U : 1U;
    c.core.heartbeat_ticks = 1U;
    c.core.election_min_ticks = 3U;
    c.core.election_max_ticks = 5U;
    c.core.initial_election_timeout_ticks = 3U;
    c.core.max_log_entries = f->max_log_entries;
    c.storage.context = g;
    c.storage.begin = storage_begin;
    c.storage.write_hard_state = storage_hard;
    c.storage.truncate_log = storage_index;
    c.storage.append_log = storage_entries;
    c.storage.write_commit_index = storage_index;
    c.storage.commit = storage_begin;
    c.storage.rollback = storage_begin;
    c.transport.context = g;
    c.transport.enqueue = transport_send;
    if (f->network) {
        result = tr_raft_flowmq_owner_bind(f->links[tr_raft_owner_index(o)], id, &c.transport);
        if (result != SALTS_OK) return result;
    }
    c.state_machine.context = g;
    c.state_machine.apply_batch = apply_entries;
    result = tr_raft_service_create(&c, out);
    if (result == SALTS_OK) result = tr_raft_service_tick(*out, &tick);
    if (result != SALTS_OK) {
        tr_raft_service_destroy(*out);
        *out = NULL;
    } else g->opened = true;
    return result;
}
static void group_close(void *p, tr_raft_owner_t *o, uint64_t id)
{
    multicore_test_t *f = p;
    multicore_test_group_t *g = &f->groups[id - 1U];
    test_thread(g);
    if (tr_raft_owner_service(o, id) != NULL) g->wrong_order = true;
    g->closed = true;
}
static void test_config(multicore_test_t *f, uint32_t owners)
{
    size_t i;
    memset(f, 0, sizeof(*f));
    f->node = 1U;
    f->max_log_entries = 1024U;
    f->voters[0] = 1U;
    f->voters[1] = 2U;
    cmeta_mutex_init(&f->mutex);
    cmeta_cond_init(&f->changed);
    for (i = 0; i < TEST_GROUPS; ++i) {
        f->assignments[i].group_id = i + 1U;
        f->assignments[i].owner_index = (uint32_t)i % owners;
        f->assignments[i].election_min_ticks = 3U;
        f->assignments[i].election_max_ticks = 5U;
    }
    f->config.version = TR_RAFT_MULTICORE_VERSION;
    f->config.owner_count = owners;
    f->config.capacity = 8U;
    f->config.work_budget = 2U;
    f->config.tick_ms = 10U;
    f->config.idle_ms = 1U;
    f->config.groups = f->assignments;
    f->config.group_count = TEST_GROUPS;
}
static int test_start(multicore_test_t *f)
{
    tr_raft_multicore_factory_t factory = {f, owner_open, owner_poll, owner_close,
                                          group_open, group_close};
    return tr_raft_multicore_create(&f->config, &factory, &f->runtime);
}
static void test_release(multicore_test_t *f)
{
    cmeta_mutex_lock(&f->mutex);
    f->release = true;
    cmeta_cond_broadcast(&f->changed);
    cmeta_mutex_unlock(&f->mutex);
}
static int test_take(multicore_test_t *f, uint64_t id, tr_raft_multicore_completion_t *c)
{
    uint64_t deadline = cmeta_monotonic_ms() + TEST_TIMEOUT_MS;
    int result;
    do {
        result = tr_raft_multicore_take(f->runtime, id, c);
        if (result != SALTS_ENOENT) return result;
        cmeta_sleep_ms(1U);
    } while (cmeta_monotonic_ms() < deadline);
    return SALTS_ETIMEDOUT;
}
static void test_stop(void *p) { (void)tr_raft_multicore_stop(p); }

typedef struct multicore_producer {
    tr_raft_multicore_t *runtime;
    uint64_t producer;
    int result;
} multicore_producer_t;

static void concurrent_producer(void *context)
{
    multicore_producer_t *producer = context;
    tr_raft_multicore_request_t request = {0};
    request.operation = TR_RAFT_MULTICORE_STATUS;
    for (uint64_t i = 0U; i < 32U; ++i) {
        request.request_id = producer->producer * 32U + i;
        producer->result = tr_raft_multicore_submit(producer->runtime, 1U, &request);
        if (producer->result != SALTS_OK) return;
    }
}

static int reserve_endpoint(char *out, size_t capacity)
{
    flowmq_ctx_t *ctx = flowmq_ctx_new();
    flowmq_socket_t *socket;
    size_t size = 0U;
    int result;
    if (ctx == NULL) return SALTS_ENOMEM;
    socket = flowmq_socket(ctx, FLOWMQ_PAIR);
    if (socket == NULL) { flowmq_ctx_term(ctx); return SALTS_ENOMEM; }
    result = flowmq_bind(socket, "tcp://127.0.0.1:0");
    if (result == SALTS_OK) result = flowmq_last_endpoint(socket, out, capacity, &size);
    (void)flowmq_close(socket);
    (void)flowmq_ctx_term(ctx);
    return result;
}

static int network_configure(multicore_test_t *a, multicore_test_t *b, bool tls)
{
    size_t i;
    b->node = 2U;
    a->network = b->network = true;
    b->fingerprint = "sha256:eb571a92b33237897216c79501066b3e77391047eaeb6be084526c4769657549";
    for (i = 0; i < a->config.owner_count; ++i) {
        int result = reserve_endpoint(a->endpoints[i], sizeof(a->endpoints[i]));
        if (result == SALTS_OK) result = reserve_endpoint(b->endpoints[i], sizeof(b->endpoints[i]));
        if (result != SALTS_OK) return result;
        if (tls) memcpy(b->endpoints[i], "tls", 3U);
        for (size_t n = 0; n < 2U; ++n) {
            multicore_test_t *local = n == 0U ? a : b;
            multicore_test_t *remote = n == 0U ? b : a;
            tr_raft_flowmq_peer_service_config_t *c = &local->network_config[i];
            tr_raft_flowmq_peer_config_t *p = &local->peer_config[i];
            tr_raft_handshake_result_t *h = &local->handshake[i];
            c->protocol.cluster_id.bytes[0] = 17U;
            c->protocol.local_node_id = local->node;
            c->protocol.process_incarnation.bytes[0] = (uint8_t)local->node;
            c->protocol.config_epoch = 1U;
            c->protocol.wire_major_min = c->protocol.wire_major_max = TR_RAFT_HANDSHAKE_WIRE_MAJOR;
            c->protocol.wire_minor_min = c->protocol.wire_minor_max = TR_RAFT_HANDSHAKE_WIRE_MINOR;
            c->protocol.max_frame_size = TR_RAFT_WIRE_MAX_FRAME_SIZE;
            c->protocol.max_snapshot_chunk_size = TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES;
            /* Test-only negotiated fixture; production must supply authenticated results. */
            h->complete = 1;
            h->cluster_id = c->protocol.cluster_id;
            h->local_node_id = local->node;
            h->peer_node_id = remote->node;
            h->peer_process_incarnation.bytes[0] = (uint8_t)remote->node;
            h->peer_config_epoch = 1U;
            h->wire_major = TR_RAFT_HANDSHAKE_WIRE_MAJOR;
            h->wire_minor = TR_RAFT_HANDSHAKE_WIRE_MINOR;
            h->max_frame_size = TR_RAFT_WIRE_MAX_FRAME_SIZE;
            h->max_snapshot_chunk_size = TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES;
            c->bind_endpoint = local->endpoints[i];
            c->local_identity = n == 0U ? "node-1" : "node-2";
            c->peers = p;
            c->peer_count = 1U;
            p->node_id = remote->node;
            p->identity = n == 0U ? "node-2" : "node-1";
            p->handshake = h;
            p->endpoint = remote->endpoints[i];
            c->outbound_limits.total_item_capacity = 256U;
            c->outbound_limits.total_data_bytes = 4U * 1024U * 1024U;
            c->outbound_limits.max_active_groups = TEST_GROUPS;
            c->outbound_limits.per_group_item_capacity = 32U;
            c->outbound_limits.per_group_data_bytes = 1024U * 1024U;
            c->max_send_batch_items = c->max_receive_batch_items = 16U;
            c->send_hwm_messages = c->receive_hwm_messages = 256U;
            c->send_hwm_bytes = c->receive_hwm_bytes = 4U * 1024U * 1024U;
            c->reconnect_initial_ms = 1U;
            c->reconnect_max_ms = 16U;
            c->heartbeat_interval_ms = 1000U;
            c->heartbeat_timeout_ms = 5000U;
            if (tls && n == 0U) {
                p->tls.ca_file = TURBORAFT_TEST_FIXTURE_DIR "/ca.pem";
                p->tls.cert_file = TURBORAFT_TEST_FIXTURE_DIR "/node1-cert.pem";
                p->tls.key_file = TURBORAFT_TEST_FIXTURE_DIR "/node1-key.pem";
                p->tls.server_name = "node-2.mesh";
            } else if (tls) {
                c->tls.ca_file = TURBORAFT_TEST_FIXTURE_DIR "/ca.pem";
                c->tls.cert_file = TURBORAFT_TEST_FIXTURE_DIR "/node2-cert.pem";
                c->tls.key_file = TURBORAFT_TEST_FIXTURE_DIR "/node2-key.pem";
                c->tls.require_client_certificate = 1;
                p->client_certificate_sha256 = &b->fingerprint;
                p->client_certificate_sha256_count = 1U;
            }
        }
    }
    return SALTS_OK;
}


#endif
