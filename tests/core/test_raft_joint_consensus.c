#include <turboraft/raft_core.h>

#include <tinytest.h>
#include <turbo_error.h>

#include <string.h>

static void joint_ready(tr_raft_ready_t *ready,
                        tr_raft_message_t *messages,
                        size_t capacity)
{
    memset(ready, 0, sizeof(*ready));
    ready->messages = messages;
    ready->message_capacity = capacity;
}

static tr_raft_core_t *joint_single_node_leader(void)
{
    const tr_raft_node_id_t voters[] = {1U};
    tr_raft_core_config_t config;
    tr_raft_core_t *core = NULL;
    tr_raft_tick_t tick;
    tr_raft_ready_t ready;
    tr_raft_message_t messages[TR_RAFT_MAX_MEMBERS];

    memset(&config, 0, sizeof(config));
    config.self_id = 1U;
    config.voters = voters;
    config.voter_count = 1U;
    config.heartbeat_ticks = 2U;
    config.election_min_ticks = 5U;
    config.election_max_ticks = 10U;
    config.initial_election_timeout_ticks = 5U;
    config.max_log_entries = 16U;
    check_equal(tr_raft_core_create(&config, &core), TURBO_OK);

    tick.elapsed_ticks = 5U;
    tick.next_election_timeout_ticks = 6U;
    joint_ready(&ready, messages, TR_RAFT_MAX_MEMBERS);
    check_equal(tr_raft_core_tick(core, &tick, &ready), TURBO_OK);
    check_equal(ready.role, TR_RAFT_LEADER);
    check_equal(tr_raft_core_advance(core), TURBO_OK);
    return core;
}

spec("raft joint consensus")
{
    it("commits joint under old quorum and final under both quorums")
    {
        const tr_raft_node_id_t target_voters[] = {1U, 2U};
        tr_raft_core_t *core = joint_single_node_leader();
        tr_raft_membership_change_t change;
        tr_raft_ready_t ready;
        tr_raft_message_t messages[TR_RAFT_MAX_MEMBERS];
        tr_raft_message_t response;
        tr_raft_tick_t tick;
        tr_raft_status_t status;

        memset(&change, 0, sizeof(change));
        change.transition_id = 101U;
        change.voters = target_voters;
        change.voter_count = 2U;
        joint_ready(&ready, messages, TR_RAFT_MAX_MEMBERS);
        check_equal(tr_raft_core_change_membership(core, &change, &ready),
                     TURBO_OK);
        check(ready.log_changed);
        check(ready.commit_changed);
        check_equal(ready.commit_index, 1U);
        check_equal(ready.message_count, 1U);
        check_equal(ready.messages[0].to, 2U);
        check_equal(tr_raft_core_status(core, &status), TURBO_OK);
        check(status.joint_configuration);
        check_equal(status.peer_count, 2U);
        check_equal(tr_raft_core_advance(core), TURBO_OK);

        memset(&response, 0, sizeof(response));
        response.type = TR_RAFT_MSG_APPEND_RESPONSE;
        response.from = 2U;
        response.to = 1U;
        response.term = status.term;
        response.granted = true;
        response.previous_log_index = 0U;
        response.match_index = 1U;
        joint_ready(&ready, messages, TR_RAFT_MAX_MEMBERS);
        check_equal(tr_raft_core_step(core, &response, &ready), TURBO_OK);

        tick.elapsed_ticks = 1U;
        tick.next_election_timeout_ticks = 6U;
        joint_ready(&ready, messages, TR_RAFT_MAX_MEMBERS);
        check_equal(tr_raft_core_tick(core, &tick, &ready), TURBO_OK);
        check(ready.log_changed);
        check(!ready.commit_changed);
        check_equal(ready.message_count, 1U);
        check_equal(ready.messages[0].entry.index, 2U);
        check_equal(tr_raft_core_advance(core), TURBO_OK);

        response.previous_log_index = 1U;
        response.match_index = 2U;
        joint_ready(&ready, messages, TR_RAFT_MAX_MEMBERS);
        check_equal(tr_raft_core_step(core, &response, &ready), TURBO_OK);
        check(ready.commit_changed);
        check_equal(ready.commit_index, 2U);
        check_equal(tr_raft_core_status(core, &status), TURBO_OK);
        check(!status.joint_configuration);
        check_equal(status.pending_configuration_count, 0U);
        check_equal(status.voter_count, 2U);
        check_equal(tr_raft_core_advance(core), TURBO_OK);
        tr_raft_core_destroy(core);
    }
}
