#include <turboraft/raft_multicore_ingress.h>

#include <cmeta_error.h>
#include <salts/clock.h>
#include <salts/thread.h>
#include <tinytest.h>

#include <stdatomic.h>
#include <string.h>

enum { OWNER_COUNT = 2, GROUP_COUNT = 2, TEST_TIMEOUT_MS = 5000 };
static const uint64_t GROUP_IDS[GROUP_COUNT] = {101U, 103U};

typedef struct owner_group {
    const void *thread_token;
    uint64_t id;
    unsigned responses;
    bool wrong_owner;
    bool closed;
} owner_group;

typedef struct ingress_fixture {
    owner_group groups[GROUP_COUNT];
    tr_raft_group_assignment_t assignments[GROUP_COUNT];
    tr_raft_multicore_config_t config;
    tr_raft_multicore_t *runtime;
    tr_raft_multicore_ingress_t *ingress;
    tr_raft_node_id_t voters[3];
    atomic_bool release;
    atomic_bool entered[OWNER_COUNT];
    unsigned owner_opened[OWNER_COUNT];
    unsigned owner_closed[OWNER_COUNT];
} ingress_fixture;

static size_t group_slot(uint64_t id)
{
    return id == GROUP_IDS[0] ? 0U : 1U;
}

static int check_group_thread(void *context)
{
    owner_group *group = (owner_group *)context;
    if (group->thread_token != cmeta_thread_current_token())
        group->wrong_owner = true;
    return SALTS_OK;
}
static int mock_hard(void *context, tr_raft_term_t term, tr_raft_node_id_t vote)
{
    (void)term; (void)vote;
    return check_group_thread(context);
}
static int mock_truncate(void *context, tr_raft_index_t index)
{
    (void)index;
    return check_group_thread(context);
}
static int mock_append(void *context, const tr_raft_entry_t *entries, size_t count)
{
    (void)entries; (void)count;
    return check_group_thread(context);
}
static int mock_commit_index(void *context, tr_raft_index_t index)
{
    (void)index;
    return check_group_thread(context);
}
static int mock_send(void *context, const tr_raft_message_t *message)
{
    owner_group *group = (owner_group *)context;
    if (message == NULL) return SALTS_EINVAL;
    ++group->responses;
    return check_group_thread(context);
}
static int mock_apply(void *context, const tr_raft_entry_t *entries, size_t count)
{
    (void)entries; (void)count;
    return check_group_thread(context);
}

static int test_owner_open(void *context, tr_raft_owner_t *owner)
{
    ingress_fixture *fixture = (ingress_fixture *)context;
    const uint32_t index = tr_raft_owner_index(owner);
    if (index >= OWNER_COUNT) return SALTS_EINVAL;
    ++fixture->owner_opened[index];
    return SALTS_OK;
}

static int test_owner_poll(void *context, tr_raft_owner_t *owner)
{
    ingress_fixture *fixture = (ingress_fixture *)context;
    const uint32_t index = tr_raft_owner_index(owner);
    if (index >= OWNER_COUNT) return SALTS_EINVAL;

    atomic_store_explicit(&fixture->entered[index], true, memory_order_release);
    while (!atomic_load_explicit(&fixture->release, memory_order_acquire))
        cmeta_sleep_ms(1U);
    return SALTS_OK;
}

static void test_owner_close(void *context, tr_raft_owner_t *owner)
{
    ingress_fixture *fixture = (ingress_fixture *)context;
    ++fixture->owner_closed[tr_raft_owner_index(owner)];
}

static int test_group_open(void *context, tr_raft_owner_t *owner,
                           uint64_t group_id, tr_raft_service_t **out_service)
{
    ingress_fixture *fixture = (ingress_fixture *)context;
    const size_t index = group_slot(group_id);
    owner_group *group = &fixture->groups[index];
    tr_raft_service_config_t cfg = {0};

    *out_service = NULL;
    group->id = group_id;
    group->thread_token = cmeta_thread_current_token();
    if (!tr_raft_owner_contains(owner, group_id) ||
        tr_raft_owner_index(owner) != index)
        group->wrong_owner = true;

    cfg.core.self_id = 2U;
    cfg.core.voters = fixture->voters;
    cfg.core.voter_count = 3U;
    cfg.core.heartbeat_ticks = 2U;
    cfg.core.election_min_ticks = 5U;
    cfg.core.election_max_ticks = 9U;
    cfg.core.initial_election_timeout_ticks = 5U;
    cfg.core.max_log_entries = 32U;

    cfg.storage.context = group;
    cfg.storage.begin = check_group_thread;
    cfg.storage.write_hard_state = mock_hard;
    cfg.storage.truncate_log = mock_truncate;
    cfg.storage.append_log = mock_append;
    cfg.storage.write_commit_index = mock_commit_index;
    cfg.storage.commit = check_group_thread;
    cfg.storage.rollback = check_group_thread;
    cfg.transport.context = group;
    cfg.transport.enqueue = mock_send;
    cfg.state_machine.context = group;
    cfg.state_machine.apply_batch = mock_apply;
    return tr_raft_service_create(&cfg, out_service);
}

static void test_group_close(void *context, tr_raft_owner_t *owner,
                             uint64_t group_id)
{
    ingress_fixture *fixture = (ingress_fixture *)context;
    owner_group *group = &fixture->groups[group_slot(group_id)];
    if (group->thread_token != cmeta_thread_current_token() ||
        tr_raft_owner_service(owner, group_id) != NULL)
        group->wrong_owner = true;
    group->closed = true;
}

static int fixture_create(ingress_fixture *fixture)
{
    tr_raft_multicore_factory_t factory = {
        fixture, test_owner_open, test_owner_poll, test_owner_close,
        test_group_open, test_group_close
    };
    size_t i;
    int result;

    fixture->voters[0] = 1U;
    fixture->voters[1] = 2U;
    fixture->voters[2] = 3U;
    atomic_init(&fixture->release, false);
    for (i = 0U; i < OWNER_COUNT; ++i) {
        atomic_init(&fixture->entered[i], false);
        fixture->assignments[i].group_id = GROUP_IDS[i];
        fixture->assignments[i].owner_index = (uint32_t)i;
        fixture->assignments[i].election_min_ticks = 5U;
        fixture->assignments[i].election_max_ticks = 9U;
    }
    fixture->config.version = TR_RAFT_MULTICORE_VERSION;
    fixture->config.owner_count = OWNER_COUNT;
    fixture->config.capacity = 1U; /* includes unread completions */
    fixture->config.work_budget = 1U;
    fixture->config.tick_ms = 1000U;
    fixture->config.idle_ms = 1U;
    fixture->config.groups = fixture->assignments;
    fixture->config.group_count = GROUP_COUNT;
    result = tr_raft_multicore_create(
        &fixture->config, &factory, &fixture->runtime);
    if (result != SALTS_OK) return result;
    return tr_raft_multicore_ingress_create(
        fixture->runtime, 1001U, &fixture->ingress);
}

static int wait_for_owners(ingress_fixture *fixture)
{
    uint64_t deadline = cmeta_monotonic_ms() + TEST_TIMEOUT_MS;
    for (;;) {
        bool ready = true;
        size_t i;
        for (i = 0U; i < OWNER_COUNT; ++i)
            if (!atomic_load_explicit(
                    &fixture->entered[i], memory_order_acquire))
                ready = false;
        if (ready) return SALTS_OK;
        if (cmeta_monotonic_ms() >= deadline) return SALTS_ETIMEDOUT;
        cmeta_sleep_ms(1U);
    }
}

static int take_completion(ingress_fixture *fixture, size_t group,
                           tr_raft_multicore_completion_t *out)
{
    const uint64_t deadline = cmeta_monotonic_ms() + TEST_TIMEOUT_MS;
    int result;
    do {
        result = tr_raft_multicore_take(
            fixture->runtime, GROUP_IDS[group], out);
        if (result != SALTS_ENOENT) return result;
        cmeta_sleep_ms(1U);
    } while (cmeta_monotonic_ms() < deadline);
    return SALTS_ETIMEDOUT;
}

static tr_raft_transport_payload_t heartbeat(uint64_t group_id,
                                               tr_raft_node_id_t from)
{
    tr_raft_transport_payload_t packet = {0};
    packet.group_id = group_id;
    packet.kind = TR_RAFT_WIRE_PAYLOAD_RAFT;
    packet.data.raft.type = TR_RAFT_MSG_HEARTBEAT_REQUEST;
    packet.data.raft.from = from;
    packet.data.raft.to = 2U;
    packet.data.raft.term = 5U;
    return packet;
}

static int fixture_destroy(ingress_fixture *fixture)
{
    int result;
    int first = SALTS_OK;
    size_t i;

    atomic_store_explicit(&fixture->release, true, memory_order_release);
    if (fixture->runtime != NULL) {
        tr_raft_multicore_request_stop(fixture->runtime);
        result = tr_raft_multicore_stop(fixture->runtime);
        if (result != SALTS_OK && first == SALTS_OK) first = result;
    }
    result = tr_raft_multicore_ingress_destroy(fixture->ingress);
    if (result != SALTS_OK && first == SALTS_OK) first = result;
    fixture->ingress = NULL;
    tr_raft_multicore_destroy(fixture->runtime);
    fixture->runtime = NULL;

    for (i = 0U; i < GROUP_COUNT; ++i) {
        if (fixture->owner_opened[i] != 1U ||
            fixture->owner_closed[i] != 1U ||
            fixture->groups[i].wrong_owner ||
            !fixture->groups[i].closed)
            return SALTS_EPROTO;
    }
    return first;
}


/* Real OS threads race on one borrowed ingress while both Group owners are
 * parked. There are exactly two credits; every other result must fail closed. */
enum { PARALLEL_PRODUCERS = 8 };
typedef struct parallel_producer {
    tr_raft_multicore_ingress_t *ingress;
    atomic_bool *start;
    atomic_uint *ready;
    uint64_t group_id;
    tr_raft_node_id_t from;
    uint64_t request_id;
    int result;
} parallel_producer;

static void parallel_submit(void *arg)
{
    parallel_producer *p = (parallel_producer *)arg;
    tr_raft_transport_payload_t packet =
        heartbeat(p->group_id, p->from);
    atomic_fetch_add_explicit(p->ready, 1U, memory_order_release);
    while (!atomic_load_explicit(p->start, memory_order_acquire))
        cmeta_thread_yield();
    p->request_id = UINT64_C(99);
    p->result = tr_raft_multicore_ingress_submit(
        p->ingress, &packet, &p->request_id);
}

typedef struct parallel_stop {
    tr_raft_multicore_t *runtime;
    atomic_bool *start;
} parallel_stop;

static void parallel_request_stop(void *arg)
{
    parallel_stop *p = (parallel_stop *)arg;
    while (!atomic_load_explicit(p->start, memory_order_acquire))
        cmeta_thread_yield();
    tr_raft_multicore_request_stop(p->runtime);
}

spec("ACE 2.3 borrowed CNet Raft frame -> exact existing Multicore Owner")
{
    it("copies accepted messages and preserves independent Group owner credits")
    {
        ingress_fixture f = {0};
        tr_raft_transport_payload_t a = heartbeat(101U, 1U);
        tr_raft_transport_payload_t b = heartbeat(103U, 3U);
        tr_raft_multicore_completion_t ca = {0}, cb = {0};
        tr_raft_multicore_group_status_t status = {0};
        uint64_t a_id = 0U, b_id = 0U, refused = 88U;
        int result;

        check_equal(fixture_create(&f), SALTS_OK);
        check_equal(wait_for_owners(&f), SALTS_OK);
        check_equal(tr_raft_multicore_ingress_submit(
            f.ingress, &a, &a_id), SALTS_OK);
        check_equal(tr_raft_multicore_ingress_submit(
            f.ingress, &b, &b_id), SALTS_OK);
        check_equal(a_id, UINT64_C(1001));
        check_equal(b_id, UINT64_C(1002));

        /* Each group's completion credit is already reserved. No other
         * Raft Owner is blocked by a saturated Group. */
        check_equal(tr_raft_multicore_ingress_submit(
            f.ingress, &a, &refused), SALTS_ENOSPC);
        check_equal(refused, UINT64_C(0));
        check_equal(tr_raft_multicore_group_status(
            f.runtime, 101U, &status), SALTS_OK);
        check_equal(status.outstanding, (size_t)1U);
        check_equal(status.rejected, UINT64_C(1));

        /* Original CNet receive view is now free to be reused; the
         * fully inline message was copied into Multicore's owned ring. */
        memset(&a, 0xff, sizeof(a));
        memset(&b, 0xff, sizeof(b));
        atomic_store_explicit(&f.release, true, memory_order_release);
        check_equal(take_completion(&f, 0U, &ca), SALTS_OK);
        check_equal(take_completion(&f, 1U, &cb), SALTS_OK);
        check_equal(ca.operation, TR_RAFT_MULTICORE_STEP);
        check_equal(cb.operation, TR_RAFT_MULTICORE_STEP);
        check_equal(ca.request_id, a_id);
        check_equal(cb.request_id, b_id);
        check_equal(ca.result, SALTS_OK);
        check_equal(cb.result, SALTS_OK);
        check_equal(tr_raft_multicore_group_status(
            f.runtime, 101U, &status), SALTS_OK);
        check_equal(status.outstanding, (size_t)0U);

        result = fixture_destroy(&f);
        check_equal(result, SALTS_OK);
    }

    it("rejects borrowed SG chunks, unknown Groups and malformed sender before admission")
    {
        ingress_fixture f = {0};
        tr_raft_transport_payload_t a = heartbeat(999U, 1U);
        uint64_t id = 77U;
        int result;

        check_equal(fixture_create(&f), SALTS_OK);
        check_equal(wait_for_owners(&f), SALTS_OK);
        check_equal(tr_raft_multicore_ingress_submit(
            f.ingress, &a, &id), SALTS_ENOENT);
        check_equal(id, UINT64_C(0));
        a.group_id = 101U;
        a.kind = TR_RAFT_WIRE_PAYLOAD_DATA_CHUNK;
        a.data.data_chunk.data = (const uint8_t *)"borrowed";
        a.data.data_chunk.data_length = 8U;
        check_equal(tr_raft_multicore_ingress_submit(
            f.ingress, &a, &id), SALTS_ENOTSUP);
        check_equal(id, UINT64_C(0));
        a.kind = TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_CHUNK;
        check_equal(tr_raft_multicore_ingress_receive(
            f.ingress, &a), SALTS_ENOTSUP);
        a = heartbeat(101U, 0U);
        check_equal(tr_raft_multicore_ingress_receive(
            f.ingress, &a), SALTS_EPROTO);
        a = heartbeat(101U, 1U);
        a.data.raft.entry_count = TR_RAFT_MAX_APPEND_ENTRIES + 1U;
        check_equal(tr_raft_multicore_ingress_receive(
            f.ingress, &a), SALTS_EPROTO);
        check_equal(tr_raft_multicore_group_status(
            f.runtime, 101U, &(tr_raft_multicore_group_status_t){0}),
            SALTS_OK);
        result = fixture_destroy(&f);
        check_equal(result, SALTS_OK);
    }

    it("keeps one cancellation completion for every admitted message on stop")
    {
        ingress_fixture f = {0};
        tr_raft_transport_payload_t a = heartbeat(101U, 1U);
        tr_raft_transport_payload_t b = heartbeat(103U, 3U);
        tr_raft_multicore_completion_t ca = {0}, cb = {0};
        int result;

        check_equal(fixture_create(&f), SALTS_OK);
        check_equal(wait_for_owners(&f), SALTS_OK);
        check_equal(tr_raft_multicore_ingress_receive(
            f.ingress, &a), SALTS_OK);
        check_equal(tr_raft_multicore_ingress_receive(
            f.ingress, &b), SALTS_OK);
        tr_raft_multicore_request_stop(f.runtime);
        check_equal(tr_raft_multicore_ingress_receive(
            f.ingress, &a), SALTS_ECANCELED);
        atomic_store_explicit(&f.release, true, memory_order_release);
        check_equal(take_completion(&f, 0U, &ca), SALTS_OK);
        check_equal(take_completion(&f, 1U, &cb), SALTS_OK);
        check_equal(ca.result, SALTS_ECANCELED);
        check_equal(cb.result, SALTS_ECANCELED);
        check_true(ca.request_id != cb.request_id);
        result = fixture_destroy(&f);
        check_equal(result, SALTS_OK);
    }

    it("admits exactly bounded credits from eight simultaneous producers")
    {
        ingress_fixture f = {0};
        parallel_producer producers[PARALLEL_PRODUCERS] = {{0}};
        cmeta_thread_t threads[PARALLEL_PRODUCERS] = {0};
        atomic_bool start;
        atomic_uint ready;
        unsigned accepted[GROUP_COUNT] = {0};
        uint64_t accepted_ids[GROUP_COUNT] = {0};
        tr_raft_multicore_completion_t completion = {0};
        tr_raft_multicore_group_status_t status = {0};
        unsigned i, started = 0U;
        int result = SALTS_OK;
        const uint64_t deadline = cmeta_monotonic_ms() + TEST_TIMEOUT_MS;

        atomic_init(&start, false);
        atomic_init(&ready, 0U);
        check_equal(fixture_create(&f), SALTS_OK);
        check_equal(wait_for_owners(&f), SALTS_OK);
        for (i = 0U; i < PARALLEL_PRODUCERS; ++i) {
            producers[i].ingress = f.ingress;
            producers[i].start = &start;
            producers[i].ready = &ready;
            producers[i].group_id = GROUP_IDS[i % GROUP_COUNT];
            producers[i].from = (i % GROUP_COUNT) == 0U ? 1U : 3U;
            result = cmeta_thread_create(
                &threads[i], parallel_submit, &producers[i]);
            if (result != SALTS_OK) break;
            ++started;
        }
        while (atomic_load_explicit(&ready, memory_order_acquire) < started &&
               cmeta_monotonic_ms() < deadline)
            cmeta_sleep_ms(1U);
        /* Always release/join spawned producers before destroying ingress,
         * including on thread creation or barrier failure. */
        atomic_store_explicit(&start, true, memory_order_release);
        for (i = 0U; i < started; ++i) {
            int joined = cmeta_thread_join(&threads[i]);
            if (joined != SALTS_OK && result == SALTS_OK) result = joined;
            cmeta_thread_destroy(&threads[i]);
        }
        check_equal(result, SALTS_OK);
        check_equal(started, (unsigned)PARALLEL_PRODUCERS);
        for (i = 0U; i < started; ++i) {
            const unsigned g = i % GROUP_COUNT;
            if (producers[i].result == SALTS_OK) {
                ++accepted[g];
                accepted_ids[g] = producers[i].request_id;
                check_true(producers[i].request_id != UINT64_C(0));
            } else {
                check_equal(producers[i].result, SALTS_ENOSPC);
                check_equal(producers[i].request_id, UINT64_C(0));
            }
        }
        check_equal(accepted[0], 1U);
        check_equal(accepted[1], 1U);
        check_true(accepted_ids[0] != accepted_ids[1]);
        for (i = 0U; i < GROUP_COUNT; ++i) {
            check_equal(tr_raft_multicore_group_status(
                f.runtime, GROUP_IDS[i], &status), SALTS_OK);
            check_equal(status.outstanding, (size_t)1U);
            check_equal(status.rejected, UINT64_C(3));
        }
        tr_raft_multicore_request_stop(f.runtime);
        atomic_store_explicit(&f.release, true, memory_order_release);
        for (i = 0U; i < GROUP_COUNT; ++i) {
            check_equal(take_completion(&f, i, &completion), SALTS_OK);
            check_equal(completion.result, SALTS_ECANCELED);
            check_equal(completion.request_id, accepted_ids[i]);
            check_equal(tr_raft_multicore_group_status(
                f.runtime, GROUP_IDS[i], &status), SALTS_OK);
            check_equal(status.outstanding, (size_t)0U);
        }
        check_equal(fixture_destroy(&f), SALTS_OK);
    }


    it("races concurrent producer admission against runtime stop without losing completions")
    {
        ingress_fixture f = {0};
        parallel_producer producers[PARALLEL_PRODUCERS] = {{0}};
        cmeta_thread_t threads[PARALLEL_PRODUCERS] = {0};
        cmeta_thread_t stopper = NULL;
        parallel_stop stop_context = {0};
        atomic_bool start;
        atomic_uint ready;
        unsigned accepted[GROUP_COUNT] = {0};
        uint64_t accepted_ids[GROUP_COUNT] = {0};
        tr_raft_multicore_group_status_t status = {0};
        tr_raft_multicore_completion_t completion = {0};
        unsigned i, started = 0U;
        int result = SALTS_OK;
        const uint64_t deadline = cmeta_monotonic_ms() + TEST_TIMEOUT_MS;

        atomic_init(&start, false);
        atomic_init(&ready, 0U);
        check_equal(fixture_create(&f), SALTS_OK);
        check_equal(wait_for_owners(&f), SALTS_OK);
        stop_context.runtime = f.runtime;
        stop_context.start = &start;
        for (i = 0U; i < PARALLEL_PRODUCERS; ++i) {
            producers[i].ingress = f.ingress;
            producers[i].start = &start;
            producers[i].ready = &ready;
            producers[i].group_id = GROUP_IDS[i % GROUP_COUNT];
            producers[i].from = (i % GROUP_COUNT) == 0U ? 1U : 3U;
            result = cmeta_thread_create(
                &threads[i], parallel_submit, &producers[i]);
            if (result != SALTS_OK) break;
            ++started;
        }
        if (result == SALTS_OK)
            result = cmeta_thread_create(
                &stopper, parallel_request_stop, &stop_context);
        while (atomic_load_explicit(&ready, memory_order_acquire) < started &&
               cmeta_monotonic_ms() < deadline)
            cmeta_sleep_ms(1U);
        /* Always open the barrier and join before any borrowed ingress
         * or Multicore runtime can be freed, even on creation failure. */
        atomic_store_explicit(&start, true, memory_order_release);
        if (stopper != NULL) {
            int joined = cmeta_thread_join(&stopper);
            if (joined != SALTS_OK && result == SALTS_OK) result = joined;
            cmeta_thread_destroy(&stopper);
        } else {
            tr_raft_multicore_request_stop(f.runtime);
        }
        for (i = 0U; i < started; ++i) {
            int joined = cmeta_thread_join(&threads[i]);
            if (joined != SALTS_OK && result == SALTS_OK) result = joined;
            cmeta_thread_destroy(&threads[i]);
        }
        check_equal(result, SALTS_OK);
        check_equal(started, (unsigned)PARALLEL_PRODUCERS);
        for (i = 0U; i < started; ++i) {
            const unsigned g = i % GROUP_COUNT;
            if (producers[i].result == SALTS_OK) {
                ++accepted[g];
                accepted_ids[g] = producers[i].request_id;
                check_true(producers[i].request_id != UINT64_C(0));
            } else {
                check_true(producers[i].result == SALTS_ENOSPC ||
                           producers[i].result == SALTS_ECANCELED);
                check_equal(producers[i].request_id, UINT64_C(0));
            }
        }
        for (i = 0U; i < GROUP_COUNT; ++i) {
            check_true(accepted[i] <= 1U);
            check_equal(tr_raft_multicore_group_status(
                f.runtime, GROUP_IDS[i], &status), SALTS_OK);
            check_equal(status.outstanding, (size_t)accepted[i]);
        }
        if (accepted[0] != 0U && accepted[1] != 0U)
            check_true(accepted_ids[0] != accepted_ids[1]);
        atomic_store_explicit(&f.release, true, memory_order_release);
        for (i = 0U; i < GROUP_COUNT; ++i) {
            if (accepted[i] != 0U) {
                check_equal(take_completion(&f, i, &completion), SALTS_OK);
                check_equal(completion.result, SALTS_ECANCELED);
                check_equal(completion.request_id, accepted_ids[i]);
            }
            check_equal(tr_raft_multicore_group_status(
                f.runtime, GROUP_IDS[i], &status), SALTS_OK);
            check_equal(status.outstanding, (size_t)0U);
        }
        check_equal(fixture_destroy(&f), SALTS_OK);
    }


    it("fails closed at correlation ID exhaustion without reusing an accepted ID")
    {
        ingress_fixture f = {0};
        tr_raft_transport_payload_t packet = heartbeat(101U, 1U);
        tr_raft_multicore_completion_t completion = {0};
        tr_raft_multicore_group_status_t status = {0};
        uint64_t accepted_id = 0U, refused_id = UINT64_C(99);

        check_equal(fixture_create(&f), SALTS_OK);
        check_equal(wait_for_owners(&f), SALTS_OK);
        /* No callback has borrowed the original ingress yet. Replace it
         * exclusively to validate the terminal correlation-ID boundary. */
        check_equal(tr_raft_multicore_ingress_destroy(f.ingress), SALTS_OK);
        f.ingress = NULL;
        check_equal(tr_raft_multicore_ingress_create(
            f.runtime, UINT64_MAX - UINT64_C(1), &f.ingress), SALTS_OK);
        check_equal(tr_raft_multicore_ingress_submit(
            f.ingress, &packet, &accepted_id), SALTS_OK);
        check_equal(accepted_id, UINT64_MAX - UINT64_C(1));
        check_equal(tr_raft_multicore_ingress_submit(
            f.ingress, &packet, &refused_id), SALTS_ERANGE);
        check_equal(refused_id, UINT64_C(0));
        check_equal(tr_raft_multicore_group_status(
            f.runtime, 101U, &status), SALTS_OK);
        check_equal(status.outstanding, (size_t)1U);
        tr_raft_multicore_request_stop(f.runtime);
        atomic_store_explicit(&f.release, true, memory_order_release);
        check_equal(take_completion(&f, 0U, &completion), SALTS_OK);
        check_equal(completion.result, SALTS_ECANCELED);
        check_equal(completion.request_id, accepted_id);
        check_equal(tr_raft_multicore_group_status(
            f.runtime, 101U, &status), SALTS_OK);
        check_equal(status.outstanding, (size_t)0U);
        check_equal(fixture_destroy(&f), SALTS_OK);
    }

}
