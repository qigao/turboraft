#include <turboraft/raft_runtime.h>
#include <turboraft/raft_data_stream.h>

#include <tinytest.h>
#include <salts_error.h>

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
    int events[32];
    size_t event_count;
    size_t apply_attempts;
    size_t apply_busy_attempts;
    uint64_t apply_busy_command_id;
    int blob_durable;
    size_t descriptor_apply_count;
} runtime_fixture_t;

static int record_event(runtime_fixture_t *fixture, int event)
{
    fixture->events[fixture->event_count++] = event;
    return SALTS_OK;
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
    return SALTS_OK;
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
    return SALTS_OK;
}

static int state_apply(void *ctx, const tr_raft_entry_t *entries, size_t count)
{
    runtime_fixture_t *fixture = (runtime_fixture_t *)ctx;
    (void) entries;
    (void) count;
    ++fixture->apply_attempts;
    if (fixture->apply_busy_attempts != 0U &&
        (fixture->apply_busy_command_id == 0U ||
         (count != 0U &&
          entries[0].command_id == fixture->apply_busy_command_id))) {
        --fixture->apply_busy_attempts;
        return SALTS_EBUSY;
    }
    return record_event(fixture, EVENT_APPLY);
}

static int state_apply_descriptor(void *ctx,
                                  const tr_raft_entry_t *entries,
                                  size_t count)
{
    runtime_fixture_t *fixture = (runtime_fixture_t *)ctx;
    tr_raft_data_descriptor_t descriptor;

    if (fixture == NULL || entries == NULL || count != 1U) {
        return SALTS_EINVAL;
    }
    ++fixture->apply_attempts;
    memset(&descriptor, 0, sizeof(descriptor));
    if (tr_raft_data_descriptor_decode(
            entries[0].data, entries[0].data_length,
            &descriptor) != SALTS_OK ||
        descriptor.stream_id != 77U ||
        descriptor.stream_size != UINT64_C(5) * 1024U * 1024U * 1024U + 17U) {
        return SALTS_EPROTO;
    }
    if (!fixture->blob_durable) {
        return SALTS_EBUSY;
    }
    ++fixture->descriptor_apply_count;
    return record_event(fixture, EVENT_APPLY);
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
        check_equal(tr_raft_core_create(&core_config, &core), SALTS_OK);

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
        check_equal(tr_raft_runtime_init(&runtime, &runtime_config), SALTS_OK);

        memset(&ready, 0, sizeof(ready));
        check_equal(tr_raft_core_tick(core, &tick, &ready), SALTS_OK);
        check_equal(tr_raft_runtime_process(&runtime, &ready, &result),
                     SALTS_OK);
        fixture.event_count = 0U;

        memset(&ready, 0, sizeof(ready));
        check_equal(tr_raft_core_propose(core, &proposal, &ready), SALTS_OK);
        check_equal(tr_raft_runtime_process(&runtime, &ready, &result),
                     SALTS_OK);
        check_true(result.durable);
        check_equal(result.stage, TR_RAFT_RUNTIME_COMPLETE);
        check_equal(fixture.event_count, 5U);
        check_equal(fixture.events[0], EVENT_BEGIN);
        check_equal(fixture.events[1], EVENT_APPEND);
        check_equal(fixture.events[2], EVENT_COMMIT_INDEX);
        check_equal(fixture.events[3], EVENT_STORAGE_COMMIT);
        check_equal(fixture.events[4], EVENT_APPLY);
        check_equal(tr_raft_core_status(core, &status), SALTS_OK);
        check_equal(status.applied_index, 1U);
        memset(&ready, 0, sizeof(ready));
        check_equal(tr_raft_core_read_index(core, 11U, &ready), SALTS_OK);
        check_equal(tr_raft_runtime_process(&runtime, &ready, &result),
                     SALTS_OK);
        check(result.read_state_ready);
        check_equal(result.read_state.context_id, 11U);
        check_equal(result.read_state.index, 1U);
        tr_raft_core_destroy(core);
    }
    it("retries a blocked state-machine apply without replaying durability or transport")
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
        tr_raft_proposal_t proposal = {2U, "blob", 4U};
        tr_raft_status_t status;
        size_t durable_events;

        memset(&core_config, 0, sizeof(core_config));
        core_config.self_id = 1U;
        core_config.voters = voters;
        core_config.voter_count = 1U;
        core_config.heartbeat_ticks = 2U;
        core_config.election_min_ticks = 5U;
        core_config.election_max_ticks = 10U;
        core_config.initial_election_timeout_ticks = 5U;
        core_config.max_log_entries = 4U;
        check_equal(tr_raft_core_create(&core_config, &core), SALTS_OK);

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
        check_equal(tr_raft_runtime_init(&runtime, &runtime_config), SALTS_OK);

        memset(&ready, 0, sizeof(ready));
        check_equal(tr_raft_core_tick(core, &tick, &ready), SALTS_OK);
        check_equal(tr_raft_runtime_process(&runtime, &ready, &result),
                    SALTS_OK);

        fixture.event_count = 0U;
        fixture.apply_attempts = 0U;
        fixture.apply_busy_attempts = 1U;
        memset(&ready, 0, sizeof(ready));
        check_equal(tr_raft_core_propose(core, &proposal, &ready), SALTS_OK);
        check_equal(tr_raft_runtime_process(&runtime, &ready, &result),
                    SALTS_EBUSY);
        check_false(tr_raft_runtime_is_faulted(&runtime));
        check(tr_raft_runtime_apply_blocked(&runtime));
        check_equal(tr_raft_core_status(core, &status), SALTS_OK);
        check_equal(status.commit_index, 1U);
        check_equal(status.applied_index, 0U);
        check_equal(fixture.apply_attempts, 1U);
        durable_events = fixture.event_count;

        check_equal(tr_raft_runtime_retry_apply(&runtime, &result), SALTS_OK);
        check_false(tr_raft_runtime_apply_blocked(&runtime));
        check_equal(fixture.apply_attempts, 2U);
        check_equal(fixture.event_count, durable_events + 1U);
        check_equal(fixture.events[fixture.event_count - 1U], EVENT_APPLY);
        check_equal(tr_raft_core_status(core, &status), SALTS_OK);
        check_equal(status.applied_index, 1U);

        tr_raft_core_destroy(core);
    }

    it("advances only a proven prefix before retrying a blocked suffix")
    {
        static const tr_raft_node_id_t voters[] = {1U};
        tr_raft_core_config_t core_config;
        tr_raft_core_t *core = NULL;
        tr_raft_runtime_config_t runtime_config;
        tr_raft_runtime_t runtime;
        runtime_fixture_t fixture;
        tr_raft_entry_t entries[3];
        tr_raft_conf_t configuration;
        tr_raft_ready_t ready;
        tr_raft_runtime_result_t result;
        tr_raft_status_t status;
        size_t configuration_length = 0U;
        size_t successful_events;

        memset(entries, 0, sizeof(entries));
        entries[0].index = 1U;
        entries[0].term = 1U;
        entries[0].command_id = 101U;
        entries[0].data[0] = 'a';
        entries[0].data_length = 1U;

        memset(&configuration, 0, sizeof(configuration));
        configuration.phase = TR_RAFT_CONF_FINAL;
        configuration.member_count = 1U;
        configuration.members[0].node_id = 1U;
        configuration.members[0].roles =
            TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER;
        entries[1].index = 2U;
        entries[1].term = 1U;
        check_equal(tr_raft_conf_encode(
                        &configuration, entries[1].data,
                        sizeof(entries[1].data), &configuration_length),
                    SALTS_OK);
        entries[1].data_length = configuration_length;

        entries[2].index = 3U;
        entries[2].term = 1U;
        entries[2].command_id = 303U;
        entries[2].data[0] = 'c';
        entries[2].data_length = 1U;

        memset(&core_config, 0, sizeof(core_config));
        core_config.self_id = 1U;
        core_config.voters = voters;
        core_config.voter_count = 1U;
        core_config.heartbeat_ticks = 2U;
        core_config.election_min_ticks = 5U;
        core_config.election_max_ticks = 10U;
        core_config.initial_election_timeout_ticks = 5U;
        core_config.initial_term = 1U;
        core_config.initial_log_entries = entries;
        core_config.initial_log_entry_count = 3U;
        core_config.initial_commit_index = 3U;
        core_config.initial_applied_index = 0U;
        core_config.max_log_entries = 8U;
        check_equal(tr_raft_core_create(&core_config, &core), SALTS_OK);

        memset(&fixture, 0, sizeof(fixture));
        fixture.apply_busy_attempts = 1U;
        fixture.apply_busy_command_id = 303U;
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
        check_equal(tr_raft_runtime_init(&runtime, &runtime_config), SALTS_OK);

        memset(&ready, 0, sizeof(ready));
        check_equal(tr_raft_core_poll(core, &ready), SALTS_OK);
        check_equal(ready.committed_entry_count, 3U);
        check_equal(tr_raft_runtime_process(&runtime, &ready, &result),
                    SALTS_EBUSY);
        check_false(tr_raft_runtime_is_faulted(&runtime));
        check(tr_raft_runtime_apply_blocked(&runtime));
        check_equal(fixture.apply_attempts, 2U);
        check_equal(result.applied_through, 2U);
        check_equal(tr_raft_core_status(core, &status), SALTS_OK);
        check_false(status.ready_outstanding);
        check_equal(status.commit_index, 3U);
        check_equal(status.applied_index, 2U);
        successful_events = fixture.event_count;
        check_equal(successful_events, 1U);
        check_equal(fixture.events[0], EVENT_APPLY);

        check_equal(tr_raft_runtime_retry_apply(&runtime, &result), SALTS_OK);
        check_false(tr_raft_runtime_apply_blocked(&runtime));
        check_equal(result.stage, TR_RAFT_RUNTIME_COMPLETE);
        check_equal(result.applied_through, 3U);
        check_equal(fixture.apply_attempts, 3U);
        check_equal(fixture.event_count, successful_events + 1U);
        check_equal(fixture.events[fixture.event_count - 1U], EVENT_APPLY);
        check_equal(tr_raft_core_status(core, &status), SALTS_OK);
        check_equal(status.applied_index, 3U);

        tr_raft_core_destroy(core);
    }

    it("does not apply a committed data descriptor before local bytes are durable")
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
        tr_raft_data_descriptor_t descriptor;
        uint8_t descriptor_bytes[TR_RAFT_DATA_DESCRIPTOR_ENCODED_SIZE];
        size_t descriptor_size = 0U;
        tr_raft_proposal_t proposal;
        tr_raft_status_t status;
        size_t durable_events;

        memset(&core_config, 0, sizeof(core_config));
        core_config.self_id = 1U;
        core_config.voters = voters;
        core_config.voter_count = 1U;
        core_config.heartbeat_ticks = 2U;
        core_config.election_min_ticks = 5U;
        core_config.election_max_ticks = 10U;
        core_config.initial_election_timeout_ticks = 5U;
        core_config.max_log_entries = 4U;
        check_equal(tr_raft_core_create(&core_config, &core), SALTS_OK);

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
        runtime_config.state_machine.apply_batch = state_apply_descriptor;
        check_equal(tr_raft_runtime_init(&runtime, &runtime_config), SALTS_OK);

        memset(&ready, 0, sizeof(ready));
        check_equal(tr_raft_core_tick(core, &tick, &ready), SALTS_OK);
        check_equal(tr_raft_runtime_process(&runtime, &ready, &result),
                    SALTS_OK);

        memset(&descriptor, 0, sizeof(descriptor));
        descriptor.stream_id = 77U;
        descriptor.stream_size =
            UINT64_C(5) * 1024U * 1024U * 1024U + 17U;
        memset(descriptor.stream_digest, 0x5c,
               sizeof(descriptor.stream_digest));
        check_equal(tr_raft_data_descriptor_encode(
                        &descriptor, descriptor_bytes,
                        sizeof(descriptor_bytes), &descriptor_size),
                    SALTS_OK);
        memset(&proposal, 0, sizeof(proposal));
        proposal.command_id = 77U;
        proposal.data = descriptor_bytes;
        proposal.data_length = descriptor_size;

        fixture.event_count = 0U;
        memset(&ready, 0, sizeof(ready));
        check_equal(tr_raft_core_propose(core, &proposal, &ready), SALTS_OK);
        check_equal(tr_raft_runtime_process(&runtime, &ready, &result),
                    SALTS_EBUSY);
        check_true(result.durable);
        check(tr_raft_runtime_apply_blocked(&runtime));
        check_equal(fixture.apply_attempts, 1U);
        check_equal(fixture.descriptor_apply_count, 0U);
        check_equal(tr_raft_core_status(core, &status), SALTS_OK);
        check_equal(status.commit_index, 1U);
        check_equal(status.applied_index, 0U);
        durable_events = fixture.event_count;

        fixture.blob_durable = 1;
        check_equal(tr_raft_runtime_retry_apply(&runtime, &result), SALTS_OK);
        check_false(tr_raft_runtime_apply_blocked(&runtime));
        check_equal(fixture.apply_attempts, 2U);
        check_equal(fixture.descriptor_apply_count, 1U);
        check_equal(fixture.event_count, durable_events + 1U);
        check_equal(fixture.events[fixture.event_count - 1U], EVENT_APPLY);
        check_equal(tr_raft_core_status(core, &status), SALTS_OK);
        check_equal(status.applied_index, 1U);

        tr_raft_core_destroy(core);
    }

}
