#include <turboraft/raft_core.h>

#include <tinytest.h>
#include <salts_error.h>

#include <string.h>

static tr_raft_core_config_t single_node_config(void)
{
    static const tr_raft_node_id_t voters[] = {1U};
    tr_raft_core_config_t config;

    memset(&config, 0, sizeof(config));
    config.self_id = 1U;
    config.voters = voters;
    config.voter_count = 1U;
    config.heartbeat_ticks = 2U;
    config.election_min_ticks = 5U;
    config.election_max_ticks = 10U;
    config.initial_election_timeout_ticks = 5U;
    config.max_log_entries = 16U;
    return config;
}

static int elect_single_node(tr_raft_core_t *core)
{
    tr_raft_tick_t tick = {5U, 6U};
    tr_raft_ready_t ready;

    memset(&ready, 0, sizeof(ready));
    if (tr_raft_core_tick(core, &tick, &ready) != SALTS_OK) {
        return SALTS_EPROTO;
    }
    return tr_raft_core_advance(core);
}

static void make_recovery_entries(tr_raft_entry_t entries[3])
{
    size_t index;

    memset(entries, 0, sizeof(tr_raft_entry_t) * 3U);
    for (index = 0U; index < 3U; ++index) {
        entries[index].index = (tr_raft_index_t) index + 1U;
        entries[index].term = 1U;
        entries[index].command_id = 301U + index;
        entries[index].data[0] = (uint8_t) ('a' + (int) index);
        entries[index].data_length = 1U;
    }
}

static tr_raft_core_config_t recovery_config(
    const tr_raft_entry_t entries[3],
    tr_raft_index_t applied_index)
{
    tr_raft_core_config_t config = single_node_config();

    config.initial_term = 1U;
    config.initial_log_entries = entries;
    config.initial_log_entry_count = 3U;
    config.initial_commit_index = 3U;
    config.initial_applied_index = applied_index;
    return config;
}

spec("raft split Ready and apply acknowledgement")
{
    it("re-emits only an unadmitted committed suffix")
    {
        tr_raft_core_config_t config = single_node_config();
        tr_raft_core_t *core = NULL;
        tr_raft_proposal_t proposals[3] = {
            {101U, "a", 1U},
            {102U, "b", 1U},
            {103U, "c", 1U}
        };
        tr_raft_ready_t ready;
        tr_raft_status_t status;

        check_equal(tr_raft_core_create(&config, &core), SALTS_OK);
        check_equal(elect_single_node(core), SALTS_OK);

        memset(&ready, 0, sizeof(ready));
        check_equal(tr_raft_core_propose_batch(core, proposals, 3U, &ready),
                    SALTS_OK);
        check_equal(ready.committed_entry_count, 3U);
        check_equal(ready.committed_entries[0].index, 1U);
        check_equal(ready.committed_entries[2].index, 3U);

        check_equal(tr_raft_core_ack_ready(core, 1U), SALTS_OK);
        check_equal(tr_raft_core_status(core, &status), SALTS_OK);
        check_false(status.ready_outstanding);
        check_equal(status.commit_index, 3U);
        check_equal(status.applied_index, 0U);

        memset(&ready, 0, sizeof(ready));
        check_equal(tr_raft_core_poll(core, &ready), SALTS_OK);
        check_false(ready.log_changed);
        check_false(ready.commit_changed);
        check_equal(ready.committed_entry_count, 2U);
        check_equal(ready.committed_entries[0].index, 2U);
        check_equal(ready.committed_entries[1].index, 3U);

        check_equal(tr_raft_core_ack_ready(core, 2U), SALTS_OK);

        memset(&ready, 0, sizeof(ready));
        check_equal(tr_raft_core_poll(core, &ready), SALTS_OK);
        check_equal(ready.committed_entry_count, 0U);

        check_equal(tr_raft_core_ack_applied(core, 2U), SALTS_EPROTO);
        check_equal(tr_raft_core_ack_applied(core, 1U), SALTS_OK);
        check_equal(tr_raft_core_ack_applied(core, 2U), SALTS_OK);
        check_equal(tr_raft_core_ack_applied(core, 3U), SALTS_OK);
        check_equal(tr_raft_core_status(core, &status), SALTS_OK);
        check_equal(status.applied_index, 3U);

        tr_raft_core_destroy(core);
    }

    it("rejects application acknowledgement before admission")
    {
        tr_raft_core_config_t config = single_node_config();
        tr_raft_core_t *core = NULL;
        tr_raft_proposal_t proposals[2] = {
            {201U, "x", 1U},
            {202U, "y", 1U}
        };
        tr_raft_ready_t ready;

        check_equal(tr_raft_core_create(&config, &core), SALTS_OK);
        check_equal(elect_single_node(core), SALTS_OK);

        memset(&ready, 0, sizeof(ready));
        check_equal(tr_raft_core_propose_batch(core, proposals, 2U, &ready),
                    SALTS_OK);
        check_equal(ready.committed_entry_count, 2U);
        check_equal(tr_raft_core_ack_ready(core, 1U), SALTS_OK);

        check_equal(tr_raft_core_ack_applied(core, 1U), SALTS_OK);
        check_equal(tr_raft_core_ack_applied(core, 1U), SALTS_EALREADY);
        check_equal(tr_raft_core_ack_applied(core, 2U), SALTS_EBUSY);

        memset(&ready, 0, sizeof(ready));
        check_equal(tr_raft_core_poll(core, &ready), SALTS_OK);
        check_equal(ready.committed_entry_count, 1U);
        check_equal(ready.committed_entries[0].index, 2U);
        check_equal(tr_raft_core_ack_ready(core, 1U), SALTS_OK);
        check_equal(tr_raft_core_ack_applied(core, 2U), SALTS_OK);

        tr_raft_core_destroy(core);
    }

    it("reconstructs admission from the durable applied boundary")
    {
        tr_raft_core_config_t config = single_node_config();
        tr_raft_core_t *core = NULL;
        tr_raft_entry_t entries[3];
        tr_raft_ready_t ready;
        tr_raft_status_t status;
        make_recovery_entries(entries);
        config = recovery_config(entries, 1U);

        check_equal(tr_raft_core_create(&config, &core), SALTS_OK);

        memset(&ready, 0, sizeof(ready));
        check_equal(tr_raft_core_poll(core, &ready), SALTS_OK);
        check_equal(ready.committed_entry_count, 2U);
        check_equal(ready.committed_entries[0].index, 2U);
        check_equal(ready.committed_entries[1].index, 3U);
        check_equal(tr_raft_core_status(core, &status), SALTS_OK);
        check_equal(status.applied_index, 1U);

        check_equal(tr_raft_core_ack_ready(core, 2U), SALTS_OK);
        check_equal(tr_raft_core_ack_applied(core, 2U), SALTS_OK);
        check_equal(tr_raft_core_ack_applied(core, 3U), SALTS_OK);

        tr_raft_core_destroy(core);
    }

    it("replays an admitted but unapplied suffix after restart")
    {
        tr_raft_entry_t entries[3];
        tr_raft_core_config_t config;
        tr_raft_core_t *core = NULL;
        tr_raft_ready_t ready;

        make_recovery_entries(entries);
        config = recovery_config(entries, 0U);
        check_equal(tr_raft_core_create(&config, &core), SALTS_OK);

        memset(&ready, 0, sizeof(ready));
        check_equal(tr_raft_core_poll(core, &ready), SALTS_OK);
        check_equal(ready.committed_entry_count, 3U);
        check_equal(tr_raft_core_ack_ready(core, 3U), SALTS_OK);

        memset(&ready, 0, sizeof(ready));
        check_equal(tr_raft_core_poll(core, &ready), SALTS_OK);
        check_equal(ready.committed_entry_count, 0U);
        tr_raft_core_destroy(core);
        core = NULL;

        config = recovery_config(entries, 0U);
        check_equal(tr_raft_core_create(&config, &core), SALTS_OK);
        memset(&ready, 0, sizeof(ready));
        check_equal(tr_raft_core_poll(core, &ready), SALTS_OK);
        check_equal(ready.committed_entry_count, 3U);
        check_equal(ready.committed_entries[0].index, 1U);
        check_equal(ready.committed_entries[2].index, 3U);

        tr_raft_core_destroy(core);
    }

    it("restarts from a durable application marker before Core apply ack")
    {
        tr_raft_entry_t entries[3];
        tr_raft_core_config_t config;
        tr_raft_core_t *core = NULL;
        tr_raft_ready_t ready;

        make_recovery_entries(entries);
        config = recovery_config(entries, 0U);
        check_equal(tr_raft_core_create(&config, &core), SALTS_OK);

        memset(&ready, 0, sizeof(ready));
        check_equal(tr_raft_core_poll(core, &ready), SALTS_OK);
        check_equal(tr_raft_core_ack_ready(core, 3U), SALTS_OK);
        tr_raft_core_destroy(core);
        core = NULL;

        /* The application proved index 1 durable before Core ack won. */
        config = recovery_config(entries, 1U);
        check_equal(tr_raft_core_create(&config, &core), SALTS_OK);
        memset(&ready, 0, sizeof(ready));
        check_equal(tr_raft_core_poll(core, &ready), SALTS_OK);
        check_equal(ready.committed_entry_count, 2U);
        check_equal(ready.committed_entries[0].index, 2U);
        check_equal(ready.committed_entries[1].index, 3U);

        tr_raft_core_destroy(core);
    }

    it("restarts after an exact Core applied acknowledgement")
    {
        tr_raft_entry_t entries[3];
        tr_raft_core_config_t config;
        tr_raft_core_t *core = NULL;
        tr_raft_ready_t ready;
        tr_raft_status_t status;

        make_recovery_entries(entries);
        config = recovery_config(entries, 0U);
        check_equal(tr_raft_core_create(&config, &core), SALTS_OK);

        memset(&ready, 0, sizeof(ready));
        check_equal(tr_raft_core_poll(core, &ready), SALTS_OK);
        check_equal(tr_raft_core_ack_ready(core, 3U), SALTS_OK);
        check_equal(tr_raft_core_ack_applied(core, 1U), SALTS_OK);
        check_equal(tr_raft_core_status(core, &status), SALTS_OK);
        check_equal(status.applied_index, 1U);
        tr_raft_core_destroy(core);
        core = NULL;

        config = recovery_config(entries, 1U);
        check_equal(tr_raft_core_create(&config, &core), SALTS_OK);
        memset(&ready, 0, sizeof(ready));
        check_equal(tr_raft_core_poll(core, &ready), SALTS_OK);
        check_equal(ready.committed_entry_count, 2U);
        check_equal(ready.committed_entries[0].index, 2U);
        check_equal(ready.committed_entries[1].index, 3U);

        tr_raft_core_destroy(core);
    }
}
