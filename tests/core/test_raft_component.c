#include <turboraft/raft_component.h>

#include <tinytest.h>
#include <cmeta_error.h>

#include <string.h>

typedef enum tr_component_event {
    TR_COMPONENT_BEGIN = 1,
    TR_COMPONENT_HARD_STATE,
    TR_COMPONENT_APPEND,
    TR_COMPONENT_COMMIT_INDEX,
    TR_COMPONENT_COMMIT,
    TR_COMPONENT_APPLY
} tr_component_event_t;

typedef struct tr_component_fixture {
    tr_component_event_t events[32];
    size_t event_count;
    size_t applied_count;
    size_t sent_count;
} tr_component_fixture_t;

static int tr_component_record(tr_component_fixture_t *fixture, tr_component_event_t event)
{
    if (fixture->event_count >= sizeof(fixture->events) / sizeof(fixture->events[0]))
        return SALTS_ENOBUFS;
    fixture->events[fixture->event_count++] = event;
    return SALTS_OK;
}

static int tr_component_storage_begin(void *ctx)
{
    return tr_component_record((tr_component_fixture_t *)ctx, TR_COMPONENT_BEGIN);
}

static int tr_component_storage_hard(void *ctx, tr_raft_term_t term,
                                      tr_raft_node_id_t vote)
{
    (void)term;
    (void)vote;
    return tr_component_record((tr_component_fixture_t *)ctx, TR_COMPONENT_HARD_STATE);
}

static int tr_component_storage_truncate(void *ctx, tr_raft_index_t from_index)
{
    (void)ctx;
    (void)from_index;
    return SALTS_OK;
}

static int tr_component_storage_append(void *ctx, const tr_raft_entry_t *entries,
                                        size_t count)
{
    (void)entries;
    (void)count;
    return tr_component_record((tr_component_fixture_t *)ctx, TR_COMPONENT_APPEND);
}

static int tr_component_storage_commit_index(void *ctx, tr_raft_index_t index)
{
    (void)index;
    return tr_component_record((tr_component_fixture_t *)ctx, TR_COMPONENT_COMMIT_INDEX);
}

static int tr_component_storage_commit(void *ctx)
{
    return tr_component_record((tr_component_fixture_t *)ctx, TR_COMPONENT_COMMIT);
}

static int tr_component_storage_rollback(void *ctx)
{
    (void)ctx;
    return SALTS_OK;
}

static int tr_component_send(void *ctx, const tr_raft_message_t *message)
{
    tr_component_fixture_t *fixture = (tr_component_fixture_t *)ctx;
    (void)message;
    ++fixture->sent_count;
    return SALTS_OK;
}

static int tr_component_apply(void *ctx, const tr_raft_entry_t *entries, size_t count)
{
    tr_component_fixture_t *fixture = (tr_component_fixture_t *)ctx;
    (void)entries;
    fixture->applied_count += count;
    return tr_component_record(fixture, TR_COMPONENT_APPLY);
}

static tr_raft_runtime_config_t tr_component_runtime_config(
    tr_raft_core_t *core, tr_component_fixture_t *fixture)
{
    tr_raft_runtime_config_t config;
    memset(&config, 0, sizeof(config));
    config.core = core;
    config.storage.context = fixture;
    config.storage.begin = tr_component_storage_begin;
    config.storage.write_hard_state = tr_component_storage_hard;
    config.storage.truncate_log = tr_component_storage_truncate;
    config.storage.append_log = tr_component_storage_append;
    config.storage.write_commit_index = tr_component_storage_commit_index;
    config.storage.commit = tr_component_storage_commit;
    config.storage.rollback = tr_component_storage_rollback;
    config.transport.context = fixture;
    config.transport.enqueue = tr_component_send;
    config.state_machine.context = fixture;
    config.state_machine.apply_batch = tr_component_apply;
    return config;
}

static int tr_component_test_core(tr_raft_core_t **out_core)
{
    static const tr_raft_node_id_t voters[] = {1u};
    tr_raft_core_config_t config;
    memset(&config, 0, sizeof(config));
    config.self_id = 1u;
    config.voters = voters;
    config.voter_count = 1u;
    config.heartbeat_ticks = 2u;
    config.election_min_ticks = 5u;
    config.election_max_ticks = 10u;
    config.initial_election_timeout_ticks = 5u;
    config.max_log_entries = 4u;
    return tr_raft_core_create(&config, out_core);
}

spec("Raft CMeta/ACE static Component runtime")
{
    it("exports four immutable canonical Component descriptors")
    {
        const cmeta_component_desc *storage = tr_raft_component_descriptor(
            TR_RAFT_COMPONENT_STORAGE);
        const cmeta_component_desc *transport = tr_raft_component_descriptor(
            TR_RAFT_COMPONENT_TRANSPORT);
        const cmeta_component_desc *machine = tr_raft_component_descriptor(
            TR_RAFT_COMPONENT_STATE_MACHINE);
        const cmeta_component_desc *runtime = tr_raft_component_descriptor(
            TR_RAFT_COMPONENT_RUNTIME);

        check_not_null(storage);
        check_not_null(transport);
        check_not_null(machine);
        check_not_null(runtime);
        check_true(cmeta_component_desc_valid(storage));
        check_true(cmeta_component_desc_valid(transport));
        check_true(cmeta_component_desc_valid(machine));
        check_true(cmeta_component_desc_valid(runtime));
        check_equal(storage->capability_count, (size_t)1u);
        check_equal(transport->capability_count, (size_t)1u);
        check_equal(machine->capability_count, (size_t)1u);
        check_equal(runtime->capability_count, (size_t)3u);
        check_equal(storage->capabilities[0].role, CMETA_COMPONENT_PROVIDES);
        check_equal(runtime->capabilities[0].role, CMETA_COMPONENT_REQUIRES);
        check_null(tr_raft_component_descriptor(
            (tr_raft_component_kind_t)0));
    }


    it("builds a typed 3-provider graph and keeps WAL before apply")
    {
        tr_raft_core_t *core = NULL;
        tr_raft_component_domain_t *domain = NULL;
        tr_component_fixture_t fixture = {0};
        tr_raft_runtime_config_t config;
        tr_raft_tick_t tick = {5u, 6u};
        tr_raft_ready_t ready;
        tr_raft_runtime_result_t result;
        tr_raft_proposal_t proposal = {1u, "set", 3u};
        size_t i;
        size_t commit_index = SIZE_MAX;
        size_t apply_index = SIZE_MAX;

        check_equal(tr_component_test_core(&core), SALTS_OK);
        config = tr_component_runtime_config(core, &fixture);
        check_equal(tr_raft_component_domain_create(&config, &domain), SALTS_OK);
        check_not_null(domain);
        check_not_null(tr_raft_component_domain_runtime(domain));

        memset(&ready, 0, sizeof(ready));
        check_equal(tr_raft_core_tick(core, &tick, &ready), SALTS_OK);
        check_equal(tr_raft_runtime_process(
            tr_raft_component_domain_runtime(domain), &ready, &result), SALTS_OK);
        fixture.event_count = 0u;

        memset(&ready, 0, sizeof(ready));
        check_equal(tr_raft_core_propose(core, &proposal, &ready), SALTS_OK);
        check_equal(tr_raft_runtime_process(
            tr_raft_component_domain_runtime(domain), &ready, &result), SALTS_OK);
        check_true(result.durable);
        check_equal(fixture.applied_count, (size_t)1u);
        for (i = 0u; i < fixture.event_count; ++i) {
            if (fixture.events[i] == TR_COMPONENT_COMMIT)
                commit_index = i;
            if (fixture.events[i] == TR_COMPONENT_APPLY)
                apply_index = i;
        }
        check_true(commit_index != SIZE_MAX);
        check_true(apply_index != SIZE_MAX);
        check_true(commit_index < apply_index);

        /* Domain stop owns Runtime teardown; the caller still owns Core. */
        check_equal(tr_raft_component_domain_destroy(domain), SALTS_EBUSY);
        check_equal(tr_raft_component_domain_stop(domain), SALTS_OK);
        check_null(tr_raft_component_domain_runtime(domain));
        check_equal(tr_raft_component_domain_destroy(domain), SALTS_OK);
        tr_raft_core_destroy(core);
    }

    it("fails before ownership transfer for missing real adapter callbacks")
    {
        tr_raft_core_t *core = NULL;
        tr_component_fixture_t fixture = {0};
        tr_raft_runtime_config_t config;
        tr_raft_component_domain_t *domain =
            (tr_raft_component_domain_t *)(uintptr_t)1u;

        check_equal(tr_component_test_core(&core), SALTS_OK);
        config = tr_component_runtime_config(core, &fixture);
        config.storage.commit = NULL;
        check_equal(tr_raft_component_domain_create(&config, &domain), SALTS_EINVAL);
        check_null(domain);

        config = tr_component_runtime_config(core, &fixture);
        config.transport.enqueue = NULL;
        check_equal(tr_raft_component_domain_create(&config, &domain), SALTS_EINVAL);
        check_null(domain);

        config = tr_component_runtime_config(core, &fixture);
        config.state_machine.apply_batch = NULL;
        check_equal(tr_raft_component_domain_create(&config, &domain), SALTS_EINVAL);
        check_null(domain);
        check_equal(fixture.event_count, (size_t)0u);
        tr_raft_core_destroy(core);
    }

    it("requires explicit stop and never destroys a borrowed Core")
    {
        tr_raft_core_t *core = NULL;
        tr_component_fixture_t fixture = {0};
        tr_raft_runtime_config_t config;
        tr_raft_component_domain_t *domain = NULL;
        tr_raft_status_t status;

        check_equal(tr_component_test_core(&core), SALTS_OK);
        config = tr_component_runtime_config(core, &fixture);
        check_equal(tr_raft_component_domain_create(&config, &domain), SALTS_OK);
        check_equal(tr_raft_component_domain_destroy(domain), SALTS_EBUSY);
        check_equal(tr_raft_component_domain_stop(domain), SALTS_OK);
        check_equal(tr_raft_component_domain_stop(domain), SALTS_EBUSY);
        check_equal(tr_raft_component_domain_destroy(domain), SALTS_OK);
        check_equal(tr_raft_core_status(core, &status), SALTS_OK);
        tr_raft_core_destroy(core);
    }
}
