#include <turboraft/raft_runtime.h>

#include <tinytest.h>
#include <turbo_error.h>

#include <string.h>

enum runtime_event {
    EVENT_BEGIN = 1,
    EVENT_HARD_STATE,
    EVENT_APPEND,
    EVENT_COMMIT_INDEX,
    EVENT_STORAGE_COMMIT,
    EVENT_APPLY
};

typedef struct runtime_fixture {
    int events[16];
    size_t event_count;
} runtime_fixture_t;

static int record_event(runtime_fixture_t *fixture, int event)
{
    fixture->events[fixture->event_count++] = event;
    return TURBO_OK;
}

static int storage_begin(void *ctx)
{
    return record_event((runtime_fixture_t *) ctx, EVENT_BEGIN);
}

static int storage_hard(void *ctx, tr_raft_term_t term,
                        tr_raft_node_id_t vote)
{
    (void) term;
    (void) vote;
    return record_event((runtime_fixture_t *) ctx, EVENT_HARD_STATE);
}

static int storage_truncate(void *ctx, tr_raft_index_t index)
{
    (void) ctx;
    (void) index;
    return TURBO_OK;
}

static int storage_append(void *ctx, const tr_raft_entry_t *entries,
                          size_t count)
{
    (void) entries;
    (void) count;
    return record_event((runtime_fixture_t *) ctx, EVENT_APPEND);
}

static int storage_commit_index(void *ctx, tr_raft_index_t index)
{
    (void) index;
    return record_event((runtime_fixture_t *) ctx, EVENT_COMMIT_INDEX);
}

static int storage_commit(void *ctx)
{
    return record_event((runtime_fixture_t *) ctx, EVENT_STORAGE_COMMIT);
}

static int storage_rollback(void *ctx)
{
    (void) ctx;
    return TURBO_OK;
}

static int state_apply(void *ctx, const tr_raft_entry_t *entries, size_t count)
{
    (void) entries;
    (void) count;
    return record_event((runtime_fixture_t *) ctx, EVENT_APPLY);
}

spec("raft runtime ordering")
{
    it("persists a single-node proposal before apply and advance")
    {
        static const tr_raft_node_id_t voters[] = {1U};
        tr_raft_core_config_t core_config;
        tr_raft_core_t *core = NULL;
        tr_raft_runtime_config_t runtime_config;
        tr_raft_runtime_t runtime;
        runtime_fixture_t fixture;
        tr_raft_tick_t tick = {5U, 6U};
        tr_raft_ready_t ready;
        tr_raft_runtime_result_t result;
        tr_raft_proposal_t proposal = {1U, "set", 3U};
        tr_raft_status_t status;

        memset(&core_config, 0, sizeof(core_config));
        core_config.self_id = 1U;
        core_config.voters = voters;
        core_config.voter_count = 1U;
        core_config.heartbeat_ticks = 2U;
        core_config.election_min_ticks = 5U;
        core_config.election_max_ticks = 10U;
        core_config.initial_election_timeout_ticks = 5U;
        core_config.max_log_entries = 4U;
        check_equal(tr_raft_core_create(&core_config, &core), TURBO_OK);

        memset(&fixture, 0, sizeof(fixture));
        memset(&runtime_config, 0, sizeof(runtime_config));
        runtime_config.core = core;
        runtime_config.storage.context = &fixture;
        runtime_config.storage.begin = storage_begin;
        runtime_config.storage.write_hard_state = storage_hard;
        runtime_config.storage.truncate_log = storage_truncate;
        runtime_config.storage.append_log = storage_append;
        runtime_config.storage.write_commit_index = storage_commit_index;
        runtime_config.storage.commit = storage_commit;
        runtime_config.storage.rollback = storage_rollback;
        runtime_config.state_machine.context = &fixture;
        runtime_config.state_machine.apply_batch = state_apply;
        check_equal(tr_raft_runtime_init(&runtime, &runtime_config), TURBO_OK);

        memset(&ready, 0, sizeof(ready));
        check_equal(tr_raft_core_tick(core, &tick, &ready), TURBO_OK);
        check_equal(tr_raft_runtime_process(&runtime, &ready, &result),
                     TURBO_OK);
        fixture.event_count = 0U;

        memset(&ready, 0, sizeof(ready));
        check_equal(tr_raft_core_propose(core, &proposal, &ready), TURBO_OK);
        check_equal(tr_raft_runtime_process(&runtime, &ready, &result),
                     TURBO_OK);
        check_true(result.durable);
        check_equal(result.stage, TR_RAFT_RUNTIME_COMPLETE);
        check_equal(fixture.event_count, 5U);
        check_equal(fixture.events[0], EVENT_BEGIN);
        check_equal(fixture.events[1], EVENT_APPEND);
        check_equal(fixture.events[2], EVENT_COMMIT_INDEX);
        check_equal(fixture.events[3], EVENT_STORAGE_COMMIT);
        check_equal(fixture.events[4], EVENT_APPLY);
        check_equal(tr_raft_core_status(core, &status), TURBO_OK);
        check_equal(status.applied_index, 1U);
        memset(&ready, 0, sizeof(ready));
        check_equal(tr_raft_core_read_index(core, 11U, &ready), TURBO_OK);
        check_equal(tr_raft_runtime_process(&runtime, &ready, &result),
                     TURBO_OK);
        check(result.read_state_ready);
        check_equal(result.read_state.context_id, 11U);
        check_equal(result.read_state.index, 1U);
        tr_raft_core_destroy(core);
    }
}
