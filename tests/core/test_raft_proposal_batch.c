#include <turboraft/raft_service.h>

#include <salts_error.h>
#include <tinytest.h>

#include <string.h>

typedef struct batch_fixture {
    size_t begin_count;
    size_t append_count;
    size_t appended_entries;
    size_t commit_count;
    size_t rollback_count;
    size_t apply_count;
    size_t applied_entries;
} batch_fixture_t;

static const tr_raft_node_id_t batch_voters[] = {1U};

static tr_raft_ready_t batch_ready(tr_raft_message_t *messages)
{
    tr_raft_ready_t ready;

    memset(&ready, 0, sizeof(ready));
    ready.messages = messages;
    ready.message_capacity = 4U;
    return ready;
}

static tr_raft_core_t *batch_core(size_t max_log_entries)
{
    tr_raft_core_config_t config;
    tr_raft_core_t *core = NULL;

    memset(&config, 0, sizeof(config));
    config.self_id = 1U;
    config.voters = batch_voters;
    config.voter_count = 1U;
    config.heartbeat_ticks = 2U;
    config.election_min_ticks = 5U;
    config.election_max_ticks = 10U;
    config.initial_election_timeout_ticks = 5U;
    config.max_log_entries = max_log_entries;
    check_equal(tr_raft_core_create(&config, &core), SALTS_OK);
    return core;
}

static void batch_elect_core(tr_raft_core_t *core)
{
    tr_raft_message_t messages[4];
    tr_raft_ready_t ready = batch_ready(messages);
    tr_raft_tick_t tick = {5U, 7U};

    check_equal(tr_raft_core_tick(core, &tick, &ready), SALTS_OK);
    check_equal(ready.role, TR_RAFT_LEADER);
    check_equal(tr_raft_core_advance(core), SALTS_OK);
}

static tr_raft_proposal_t proposal(uint64_t command_id, const char *value)
{
    tr_raft_proposal_t result;

    memset(&result, 0, sizeof(result));
    result.command_id = command_id;
    result.data = value;
    result.data_length = strlen(value);
    return result;
}

static int storage_begin(void *context)
{
    batch_fixture_t *fixture = (batch_fixture_t *)context;
    ++fixture->begin_count;
    return SALTS_OK;
}

static int storage_hard(void *context, tr_raft_term_t term,
                        tr_raft_node_id_t vote)
{
    (void)context;
    (void)term;
    (void)vote;
    return SALTS_OK;
}

static int storage_truncate(void *context, tr_raft_index_t index)
{
    (void)context;
    (void)index;
    return SALTS_OK;
}

static int storage_append(void *context,
                          const tr_raft_entry_t *entries,
                          size_t count)
{
    batch_fixture_t *fixture = (batch_fixture_t *)context;

    if (entries == NULL || count == 0U) {
        return SALTS_EINVAL;
    }
    ++fixture->append_count;
    fixture->appended_entries += count;
    return SALTS_OK;
}

static int storage_commit_index(void *context, tr_raft_index_t index)
{
    (void)context;
    (void)index;
    return SALTS_OK;
}

static int storage_commit(void *context)
{
    batch_fixture_t *fixture = (batch_fixture_t *)context;
    ++fixture->commit_count;
    return SALTS_OK;
}

static int storage_rollback(void *context)
{
    batch_fixture_t *fixture = (batch_fixture_t *)context;
    ++fixture->rollback_count;
    return SALTS_OK;
}

static int state_apply(void *context,
                       const tr_raft_entry_t *entries,
                       size_t count)
{
    batch_fixture_t *fixture = (batch_fixture_t *)context;

    if (entries == NULL || count == 0U) {
        return SALTS_EINVAL;
    }
    ++fixture->apply_count;
    fixture->applied_entries += count;
    return SALTS_OK;
}

static tr_raft_service_t *batch_service(batch_fixture_t *fixture)
{
    tr_raft_service_config_t config;
    tr_raft_service_t *service = NULL;
    tr_raft_tick_t tick = {5U, 7U};

    memset(&config, 0, sizeof(config));
    config.core.self_id = 1U;
    config.core.voters = batch_voters;
    config.core.voter_count = 1U;
    config.core.heartbeat_ticks = 2U;
    config.core.election_min_ticks = 5U;
    config.core.election_max_ticks = 10U;
    config.core.initial_election_timeout_ticks = 5U;
    config.core.max_log_entries = 32U;
    config.storage.context = fixture;
    config.storage.begin = storage_begin;
    config.storage.write_hard_state = storage_hard;
    config.storage.truncate_log = storage_truncate;
    config.storage.append_log = storage_append;
    config.storage.write_commit_index = storage_commit_index;
    config.storage.commit = storage_commit;
    config.storage.rollback = storage_rollback;
    config.state_machine.context = fixture;
    config.state_machine.apply_batch = state_apply;

    check_equal(tr_raft_service_create(&config, &service), SALTS_OK);
    check_equal(tr_raft_service_tick(service, &tick), SALTS_OK);
    memset(fixture, 0, sizeof(*fixture));
    return service;
}

spec("bounded proposal batching")
{
    it("appends an ordered batch through one Ready")
    {
        tr_raft_core_t *core = batch_core(16U);
        tr_raft_message_t messages[4];
        tr_raft_ready_t ready;
        tr_raft_proposal_t proposals[3];
        tr_raft_status_t status;

        batch_elect_core(core);
        proposals[0] = proposal(11U, "a");
        proposals[1] = proposal(12U, "bb");
        proposals[2] = proposal(13U, "ccc");
        ready = batch_ready(messages);

        check_equal(tr_raft_core_propose_batch(
                        core, proposals, 3U, &ready),
                    SALTS_OK);
        check(ready.log_changed);
        check_equal(ready.log_entry_count, 3U);
        check_not_null(ready.log_entries);
        check_equal(ready.log_entries[0].index, 1U);
        check_equal(ready.log_entries[1].index, 2U);
        check_equal(ready.log_entries[2].index, 3U);
        check_equal(ready.log_entries[0].command_id, 11U);
        check_equal(ready.log_entries[1].command_id, 12U);
        check_equal(ready.log_entries[2].command_id, 13U);
        check(ready.commit_changed);
        check_equal(ready.commit_index, 3U);
        check_equal(ready.committed_entry_count, 3U);
        check_equal(tr_raft_core_advance(core), SALTS_OK);
        check_equal(tr_raft_core_status(core, &status), SALTS_OK);
        check_equal(status.last_log_index, 3U);
        check_equal(status.applied_index, 3U);

        tr_raft_core_destroy(core);
    }

    it("rejects an invalid or oversized batch without partial log mutation")
    {
        tr_raft_core_t *core = batch_core(2U);
        tr_raft_message_t messages[4];
        tr_raft_ready_t ready;
        tr_raft_proposal_t proposals[3];
        tr_raft_status_t status;

        batch_elect_core(core);
        proposals[0] = proposal(21U, "one");
        proposals[1] = proposal(22U, "two");
        proposals[2] = proposal(23U, "three");
        ready = batch_ready(messages);
        check_equal(tr_raft_core_propose_batch(
                        core, proposals, 3U, &ready),
                    SALTS_ENOSPC);
        check_equal(tr_raft_core_status(core, &status), SALTS_OK);
        check_equal(status.last_log_index, 0U);

        proposals[1].command_id = 0U;
        ready = batch_ready(messages);
        check_equal(tr_raft_core_propose_batch(
                        core, proposals, 2U, &ready),
                    SALTS_EINVAL);
        check_equal(tr_raft_core_status(core, &status), SALTS_OK);
        check_equal(status.last_log_index, 0U);

        tr_raft_core_destroy(core);
    }

    it("uses one storage transaction and returns one receipt per proposal")
    {
        batch_fixture_t fixture;
        tr_raft_service_t *service;
        tr_raft_proposal_t proposals[3];
        tr_raft_operation_status_t receipts[3];

        memset(&fixture, 0, sizeof(fixture));
        memset(receipts, 0, sizeof(receipts));
        service = batch_service(&fixture);
        proposals[0] = proposal(31U, "x");
        proposals[1] = proposal(32U, "y");
        proposals[2] = proposal(33U, "z");

        check_equal(tr_raft_service_propose_batch_with_receipts(
                        service, proposals, 3U,
                        receipts, 3U),
                    SALTS_OK);
        check_equal(fixture.begin_count, 1U);
        check_equal(fixture.append_count, 1U);
        check_equal(fixture.appended_entries, 3U);
        check_equal(fixture.commit_count, 1U);
        check_equal(fixture.rollback_count, 0U);
        check_equal(fixture.apply_count, 1U);
        check_equal(fixture.applied_entries, 3U);

        check_equal(receipts[0].index, 1U);
        check_equal(receipts[1].index, 2U);
        check_equal(receipts[2].index, 3U);
        check_equal(receipts[0].state, TR_RAFT_OPERATION_APPLIED);
        check_equal(receipts[1].state, TR_RAFT_OPERATION_APPLIED);
        check_equal(receipts[2].state, TR_RAFT_OPERATION_APPLIED);

        tr_raft_service_destroy(service);
    }

    it("rejects insufficient receipt capacity before admitting the batch")
    {
        batch_fixture_t fixture;
        tr_raft_service_t *service;
        tr_raft_proposal_t proposals[3];
        tr_raft_operation_status_t receipts[2];
        tr_raft_service_status_t status;

        memset(&fixture, 0, sizeof(fixture));
        service = batch_service(&fixture);
        proposals[0] = proposal(41U, "x");
        proposals[1] = proposal(42U, "y");
        proposals[2] = proposal(43U, "z");

        check_equal(tr_raft_service_propose_batch_with_receipts(
                        service, proposals, 3U,
                        receipts, 2U),
                    SALTS_ENOSPC);
        check_equal(fixture.begin_count, 0U);
        check_equal(fixture.append_count, 0U);
        check_equal(tr_raft_service_status(service, &status), SALTS_OK);
        check_equal(status.core.last_log_index, 0U);

        tr_raft_service_destroy(service);
    }
}
