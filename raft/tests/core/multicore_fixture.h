#ifndef TURBORAFT_MULTICORE_FIXTURE_H
#define TURBORAFT_MULTICORE_FIXTURE_H
#include <turboraft/raft_multicore.h>
#include <threads/threads.h>
#include <core/clock.h>
#include <core/error.h>
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
    sync_mutex_t mutex;
    sync_cond_t changed;
    bool block, entered, release;
    uint64_t fail_group;
    size_t owner_opened[4], owner_closed[4];
    tr_raft_multicore_t *runtime;
    tr_raft_group_assignment_t assignments[TEST_GROUPS];
    tr_raft_multicore_config_t config;
    tr_raft_node_id_t node;
    size_t max_log_entries;
} multicore_test_t;

static void test_thread(multicore_test_group_t *g)
{
    if (g->thread != sync_thread_current_token()) g->wrong_thread = true;
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
    return SALTS_OK;
}
static int owner_poll(void *p, tr_raft_owner_t *o)
{
    multicore_test_t *f = p;
    if (tr_raft_owner_index(o) == 0U) {
        sync_mutex_lock(&f->mutex);
        if (f->block && !f->release) {
            f->entered = true;
            sync_cond_broadcast(&f->changed);
            while (!f->release) sync_cond_wait(&f->changed, &f->mutex);
        }
        sync_mutex_unlock(&f->mutex);
    }
    return SALTS_OK;
}
static void owner_close(void *p, tr_raft_owner_t *o)
{
    multicore_test_t *f = p;
    ++f->owner_closed[tr_raft_owner_index(o)];
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
    g->thread = sync_thread_current_token();
    c.core.self_id = f->node;
    c.core.voters = &g->self;
    c.core.voter_count = 1U;
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
    sync_mutex_init(&f->mutex);
    sync_cond_init(&f->changed);
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
    sync_mutex_lock(&f->mutex);
    f->release = true;
    sync_cond_broadcast(&f->changed);
    sync_mutex_unlock(&f->mutex);
}
static int test_take(multicore_test_t *f, uint64_t id, tr_raft_multicore_completion_t *c)
{
    uint64_t deadline = monotonic_ms() + TEST_TIMEOUT_MS;
    int result;
    do {
        result = tr_raft_multicore_take(f->runtime, id, c);
        if (result != SALTS_ENOENT) return result;
        sleep_ms(1U);
    } while (monotonic_ms() < deadline);
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

#endif
