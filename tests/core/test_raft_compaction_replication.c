#include <turboraft/raft_core.h>

#include <tinytest.h>
#include <turbo_error.h>

#include <string.h>

static void prepare_ready(tr_raft_ready_t *ready,
                          tr_raft_message_t messages[TR_RAFT_MAX_VOTERS])
{
    memset(ready, 0, sizeof(*ready));
    ready->messages = messages;
    ready->message_capacity = TR_RAFT_MAX_VOTERS;
}

static void advance_ready(tr_raft_core_t *core, tr_raft_ready_t *ready)
{
    if (ready->message_count != 0U || ready->hard_state_changed ||
        ready->role_changed || ready->log_changed || ready->commit_changed ||
        ready->committed_entry_count != 0U || ready->read_state_ready ||
        ready->snapshot_request_count != 0U) {
        check_int_eq(tr_raft_core_advance(core), TURBO_OK);
    }
}

spec("compacted leader replication")
{
    it("requests a snapshot before resuming AppendEntries")
    {
        const tr_raft_node_id_t voters[] = {1U, 2U};
        tr_raft_core_config_t config;
        tr_raft_entry_t suffix;
        tr_raft_core_t *core = NULL;
        tr_raft_ready_t ready;
        tr_raft_message_t messages[TR_RAFT_MAX_VOTERS];
        tr_raft_message_t response;

        memset(&config, 0, sizeof(config));
        memset(&suffix, 0, sizeof(suffix));
        suffix.index = 3U;
        suffix.term = 2U;
        suffix.command_id = 1U;
        config.self_id = 1U;
        config.voters = voters;
        config.voter_count = 2U;
        config.heartbeat_ticks = 1U;
        config.election_min_ticks = 2U;
        config.election_max_ticks = 3U;
        config.initial_election_timeout_ticks = 2U;
        config.initial_term = 2U;
        config.initial_last_log_index = 2U;
        config.initial_last_log_term = 1U;
        config.initial_log_entries = &suffix;
        config.initial_log_entry_count = 1U;
        config.initial_commit_index = 2U;
        config.initial_applied_index = 2U;
        check_int_eq(tr_raft_core_create(&config, &core), TURBO_OK);

        prepare_ready(&ready, messages);
        {
            tr_raft_tick_t tick = {2U, 2U};
            check_int_eq(tr_raft_core_tick(core, &tick, &ready), TURBO_OK);
        }
        check_size_eq(ready.message_count, 1U);
        check_int_eq(ready.messages[0].type, TR_RAFT_MSG_PRE_VOTE_REQUEST);
        advance_ready(core, &ready);

        memset(&response, 0, sizeof(response));
        response.type = TR_RAFT_MSG_PRE_VOTE_RESPONSE;
        response.from = 2U;
        response.to = 1U;
        response.term = 2U;
        response.campaign_term = 3U;
        response.granted = true;
        prepare_ready(&ready, messages);
        check_int_eq(tr_raft_core_step(core, &response, &ready), TURBO_OK);
        check_size_eq(ready.message_count, 1U);
        check_int_eq(ready.messages[0].type, TR_RAFT_MSG_VOTE_REQUEST);
        advance_ready(core, &ready);

        response.type = TR_RAFT_MSG_VOTE_RESPONSE;
        response.term = 3U;
        prepare_ready(&ready, messages);
        check_int_eq(tr_raft_core_step(core, &response, &ready), TURBO_OK);
        check_size_eq(ready.message_count, 1U);
        advance_ready(core, &ready);

        memset(&response, 0, sizeof(response));
        response.type = TR_RAFT_MSG_HEARTBEAT_RESPONSE;
        response.from = 2U;
        response.to = 1U;
        response.term = 3U;
        response.reject_hint = 1U;
        prepare_ready(&ready, messages);
        check_int_eq(tr_raft_core_step(core, &response, &ready), TURBO_OK);
        check_size_eq(ready.message_count, 0U);
        check_size_eq(ready.snapshot_request_count, 1U);
        check_long_eq(ready.snapshot_requests[0].peer_id, 2U);
        check_long_eq(ready.snapshot_requests[0].leader_term, 3U);
        check_long_eq(ready.snapshot_requests[0].snapshot_index, 2U);
        check_long_eq(ready.snapshot_requests[0].snapshot_term, 1U);
        advance_ready(core, &ready);

        prepare_ready(&ready, messages);
        check_int_eq(tr_raft_core_snapshot_completed(core, 2U, 2U, &ready),
                     TURBO_OK);
        check_size_eq(ready.snapshot_request_count, 0U);
        check_size_eq(ready.message_count, 1U);
        check_int_eq(ready.messages[0].type, TR_RAFT_MSG_APPEND_REQUEST);
        check_long_eq(ready.messages[0].previous_log_index, 2U);
        check_long_eq(ready.messages[0].previous_log_term, 1U);
        check_size_eq(ready.messages[0].entry_count, 1U);
        check_long_eq(ready.messages[0].entries[0].index, 3U);
        advance_ready(core, &ready);
        tr_raft_core_destroy(core);
    }
}
