#include <turboraft/raft_core.h>

#include <tinytest.h>
#include <turbo_error.h>

#include <string.h>

static tr_raft_core_config_t test_config(const tr_raft_node_id_t *voters,
                                         size_t voter_count)
{
    tr_raft_core_config_t config;

    memset(&config, 0, sizeof(config));
    config.self_id = voters[0];
    config.voters = voters;
    config.voter_count = voter_count;
    config.heartbeat_ticks = 2U;
    config.election_min_ticks = 5U;
    config.election_max_ticks = 10U;
    config.initial_election_timeout_ticks = 5U;
    return config;
}

static tr_raft_ready_t test_ready(tr_raft_message_t *messages, size_t capacity)
{
    tr_raft_ready_t ready;

    memset(&ready, 0, sizeof(ready));
    ready.messages = messages;
    ready.message_capacity = capacity;
    return ready;
}

spec("native raft election core")
{
    static const tr_raft_node_id_t voters[] = {1U, 2U, 3U};

    it("starts pre-vote without changing durable term")
    {
        tr_raft_core_t *core = NULL;
        tr_raft_core_config_t config = test_config(voters, 3U);
        tr_raft_message_t messages[2];
        tr_raft_ready_t ready = test_ready(messages, 2U);
        tr_raft_tick_t tick = {5U, 7U};

        check_int_eq(tr_raft_core_create(&config, &core), TURBO_OK);
        check_int_eq(tr_raft_core_tick(core, &tick, &ready), TURBO_OK);
        check_int_eq(ready.role, TR_RAFT_PRE_CANDIDATE);
        check_false(ready.hard_state_changed);
        check_size_eq(ready.message_count, 2U);
        check_int_eq(ready.messages[0].type,
                     TR_RAFT_MSG_PRE_VOTE_REQUEST);
        check_long_eq(ready.messages[0].campaign_term, 1U);

        tr_raft_core_destroy(core);
    }

    it("persists self vote before emitting vote requests")
    {
        tr_raft_core_t *core = NULL;
        tr_raft_core_config_t config = test_config(voters, 3U);
        tr_raft_message_t messages[2];
        tr_raft_ready_t ready = test_ready(messages, 2U);
        tr_raft_tick_t tick = {5U, 7U};
        tr_raft_message_t response;

        check_int_eq(tr_raft_core_create(&config, &core), TURBO_OK);
        check_int_eq(tr_raft_core_tick(core, &tick, &ready), TURBO_OK);
        check_int_eq(tr_raft_core_advance(core), TURBO_OK);

        memset(&response, 0, sizeof(response));
        response.type = TR_RAFT_MSG_PRE_VOTE_RESPONSE;
        response.from = 2U;
        response.to = 1U;
        response.campaign_term = 1U;
        response.granted = true;
        ready = test_ready(messages, 2U);

        check_int_eq(tr_raft_core_step(core, &response, &ready), TURBO_OK);
        check_true(ready.hard_state_changed);
        check_long_eq(ready.term, 1U);
        check_long_eq(ready.voted_for, 1U);
        check_int_eq(ready.role, TR_RAFT_CANDIDATE);
        check_size_eq(ready.message_count, 2U);
        check_int_eq(ready.messages[0].type, TR_RAFT_MSG_VOTE_REQUEST);

        tr_raft_core_destroy(core);
    }

    it("counts duplicate votes once and becomes leader on majority")
    {
        tr_raft_core_t *core = NULL;
        tr_raft_core_config_t config = test_config(voters, 3U);
        tr_raft_message_t messages[2];
        tr_raft_ready_t ready = test_ready(messages, 2U);
        tr_raft_tick_t tick = {5U, 7U};
        tr_raft_message_t response;

        check_int_eq(tr_raft_core_create(&config, &core), TURBO_OK);
        check_int_eq(tr_raft_core_tick(core, &tick, &ready), TURBO_OK);
        check_int_eq(tr_raft_core_advance(core), TURBO_OK);

        memset(&response, 0, sizeof(response));
        response.type = TR_RAFT_MSG_PRE_VOTE_RESPONSE;
        response.from = 2U;
        response.to = 1U;
        response.campaign_term = 1U;
        response.granted = true;
        ready = test_ready(messages, 2U);
        check_int_eq(tr_raft_core_step(core, &response, &ready), TURBO_OK);
        check_int_eq(tr_raft_core_advance(core), TURBO_OK);

        response.type = TR_RAFT_MSG_VOTE_RESPONSE;
        response.term = 1U;
        ready = test_ready(messages, 2U);
        check_int_eq(tr_raft_core_step(core, &response, &ready), TURBO_OK);
        check_int_eq(ready.role, TR_RAFT_LEADER);
        check_size_eq(ready.message_count, 2U);
        check_int_eq(ready.messages[0].type,
                     TR_RAFT_MSG_HEARTBEAT_REQUEST);

        tr_raft_core_destroy(core);
    }

    it("does not mutate state when output capacity is insufficient")
    {
        tr_raft_core_t *core = NULL;
        tr_raft_core_config_t config = test_config(voters, 3U);
        tr_raft_message_t message;
        tr_raft_ready_t ready = test_ready(&message, 1U);
        tr_raft_tick_t tick = {5U, 7U};
        tr_raft_status_t status;

        check_int_eq(tr_raft_core_create(&config, &core), TURBO_OK);
        check_int_eq(tr_raft_core_tick(core, &tick, &ready), TURBO_ENOSPC);
        check_int_eq(tr_raft_core_status(core, &status), TURBO_OK);
        check_int_eq(status.role, TR_RAFT_FOLLOWER);
        check_long_eq(status.term, 0U);
        check_uint_eq(status.election_elapsed_ticks, 0U);

        tr_raft_core_destroy(core);
    }
}
