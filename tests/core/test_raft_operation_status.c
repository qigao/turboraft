#include <turboraft/raft_core.h>

#include <tinytest.h>
#include <turbo_error.h>

#include <string.h>

static tr_raft_core_config_t operation_test_config(
    const tr_raft_node_id_t *voters,
    const tr_raft_entry_t *entries)
{
    tr_raft_core_config_t config;

    memset(&config, 0, sizeof(config));
    config.self_id = voters[0];
    config.voters = voters;
    config.voter_count = 1U;
    config.heartbeat_ticks = 1U;
    config.election_min_ticks = 2U;
    config.election_max_ticks = 3U;
    config.initial_election_timeout_ticks = 2U;
    config.initial_term = 3U;
    config.initial_last_log_index = 5U;
    config.initial_last_log_term = 2U;
    config.initial_log_entries = entries;
    config.initial_log_entry_count = 2U;
    config.initial_commit_index = 6U;
    config.initial_applied_index = 5U;
    return config;
}

spec("raft operation receipt status")
{
    it("derives progress from the authoritative log indexes")
    {
        const tr_raft_node_id_t voters[] = {1U};
        tr_raft_entry_t entries[2];
        tr_raft_core_config_t config;
        tr_raft_core_t *core = NULL;
        tr_raft_operation_status_t status;

        memset(entries, 0, sizeof(entries));
        entries[0].index = 6U;
        entries[0].term = 3U;
        entries[0].command_id = 10U;
        entries[1].index = 7U;
        entries[1].term = 3U;
        entries[1].command_id = 11U;
        config = operation_test_config(voters, entries);

        check_equal(tr_raft_core_create(&config, &core), TURBO_OK);

        check_equal(tr_raft_core_operation_status(core, 3U, 7U, &status),
                     TURBO_OK);
        check_equal(status.state, TR_RAFT_OPERATION_PENDING);
        check_equal(status.commit_index, 6U);
        check_equal(status.applied_index, 5U);

        check_equal(tr_raft_core_operation_status(core, 3U, 6U, &status),
                     TURBO_OK);
        check_equal(status.state, TR_RAFT_OPERATION_COMMITTED);

        check_equal(tr_raft_core_operation_status(core, 2U, 5U, &status),
                     TURBO_OK);
        check_equal(status.state, TR_RAFT_OPERATION_APPLIED);

        check_equal(tr_raft_core_operation_status(core, 1U, 4U, &status),
                     TURBO_OK);
        check_equal(status.state, TR_RAFT_OPERATION_EXPIRED);

        check_equal(tr_raft_core_operation_status(core, 2U, 6U, &status),
                     TURBO_OK);
        check_equal(status.state, TR_RAFT_OPERATION_LOST);

        check_equal(tr_raft_core_operation_status(core, 3U, 8U, &status),
                     TURBO_OK);
        check_equal(status.state, TR_RAFT_OPERATION_LOST);

        check_equal(tr_raft_core_operation_status(core, 4U, 8U, &status),
                     TURBO_EINVAL);
        check_equal(tr_raft_core_operation_status(core, 0U, 1U, &status),
                     TURBO_EINVAL);
        check_equal(tr_raft_core_operation_status(core, 1U, 0U, &status),
                     TURBO_EINVAL);

        tr_raft_core_destroy(core);
    }
}
