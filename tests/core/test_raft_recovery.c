#include <turboraft/raft_core.h>

#include <tinytest.h>
#include <turbo_error.h>

#include <string.h>

static const tr_raft_node_id_t recovery_voter[] = {1U};

static tr_raft_entry_t recovery_entry(tr_raft_index_t index,
                                      tr_raft_term_t term,
                                      uint64_t command_id,
                                      const char *payload)
{
    tr_raft_entry_t entry;

    memset(&entry, 0, sizeof(entry));
    entry.index = index;
    entry.term = term;
    entry.command_id = command_id;
    entry.data_length = strlen(payload);
    memcpy(entry.data, payload, entry.data_length);
    return entry;
}

static tr_raft_core_config_t recovery_config(const tr_raft_entry_t *entries,
                                             size_t entry_count)
{
    tr_raft_core_config_t config;

    memset(&config, 0, sizeof(config));
    config.self_id = 1U;
    config.voters = recovery_voter;
    config.voter_count = 1U;
    config.heartbeat_ticks = 2U;
    config.election_min_ticks = 5U;
    config.election_max_ticks = 10U;
    config.initial_election_timeout_ticks = 5U;
    config.initial_term = 2U;
    config.initial_log_entries = entries;
    config.initial_log_entry_count = entry_count;
    config.max_log_entries = 8U;
    return config;
}

spec("raft startup recovery and apply")
{
    it("polls committed but unapplied restored entries")
    {
        tr_raft_entry_t entries[2];
        tr_raft_core_config_t config;
        tr_raft_core_t *core = NULL;
        tr_raft_ready_t ready;
        tr_raft_status_t status;

        entries[0] = recovery_entry(1U, 1U, 11U, "one");
        entries[1] = recovery_entry(2U, 2U, 12U, "two");
        config = recovery_config(entries, 2U);
        config.initial_commit_index = 2U;
        config.initial_applied_index = 1U;
        check_equal(tr_raft_core_create(&config, &core), TURBO_OK);

        memset(&ready, 0, sizeof(ready));
        check_equal(tr_raft_core_poll(core, &ready), TURBO_OK);
        check_equal(ready.committed_entry_count, 1U);
        check_not_null(ready.committed_entries);
        check_equal(ready.committed_entries[0].index, 2U);
        check_equal(ready.committed_entries[0].command_id, 12U);
        check_equal(tr_raft_core_status(core, &status), TURBO_OK);
        check_equal(status.applied_index, 1U);

        check_equal(tr_raft_core_advance(core), TURBO_OK);
        check_equal(tr_raft_core_status(core, &status), TURBO_OK);
        check_equal(status.applied_index, 2U);
        tr_raft_core_destroy(core);
    }

    it("rejects a restored commit index beyond the durable log")
    {
        tr_raft_entry_t entry = recovery_entry(1U, 1U, 11U, "one");
        tr_raft_core_config_t config = recovery_config(&entry, 1U);
        tr_raft_core_t *core = NULL;

        config.initial_commit_index = 2U;
        config.initial_applied_index = 1U;
        check_equal(tr_raft_core_create(&config, &core), TURBO_EINVAL);
        check_null(core);
    }

    it("blocks new input until restored application is advanced")
    {
        tr_raft_entry_t entry = recovery_entry(1U, 2U, 11U, "one");
        tr_raft_core_config_t config = recovery_config(&entry, 1U);
        tr_raft_core_t *core = NULL;
        tr_raft_ready_t ready;
        tr_raft_tick_t tick = {1U, 6U};

        config.initial_commit_index = 1U;
        check_equal(tr_raft_core_create(&config, &core), TURBO_OK);
        memset(&ready, 0, sizeof(ready));
        check_equal(tr_raft_core_poll(core, &ready), TURBO_OK);
        check_equal(ready.committed_entry_count, 1U);
        check_equal(tr_raft_core_tick(core, &tick, &ready), TURBO_EPROTO);
        check_equal(tr_raft_core_advance(core), TURBO_OK);
        memset(&ready, 0, sizeof(ready));
        check_equal(tr_raft_core_tick(core, &tick, &ready), TURBO_OK);
        tr_raft_core_destroy(core);
    }
}
