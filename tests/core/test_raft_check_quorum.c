#include <turboraft/raft_core.h>

#include <tinytest.h>
#include <salts_error.h>

#include <string.h>

static void check_quorum_ready(
    tr_raft_ready_t *ready,
    tr_raft_message_t *messages,
    size_t capacity)
{
    memset(ready, 0, sizeof(*ready));
    ready->messages = messages;
    ready->message_capacity = capacity;
}

static int check_quorum_has_effects(const tr_raft_ready_t *ready)
{
    return ready->message_count != 0U || ready->hard_state_changed ||
           ready->role_changed || ready->log_changed ||
           ready->commit_changed || ready->committed_entry_count != 0U;
}

static tr_raft_term_t check_quorum_elect(
    tr_raft_core_t *core,
    tr_raft_ready_t *ready,
    tr_raft_message_t *messages)
{
    tr_raft_tick_t tick = {3U, 4U};
    tr_raft_message_t response;
    tr_raft_term_t term;
    tr_raft_term_t campaign_term;

    check_quorum_ready(ready, messages, 4U);
    check_equal(tr_raft_core_tick(core, &tick, ready), SALTS_OK);
    check_equal(ready->message_count, 2U);
    campaign_term = ready->messages[0].campaign_term;
    check_equal(tr_raft_core_advance(core), SALTS_OK);

    memset(&response, 0, sizeof(response));
    response.type = TR_RAFT_MSG_PRE_VOTE_RESPONSE;
    response.from = 2U;
    response.to = 1U;
    response.campaign_term = campaign_term;
    response.granted = true;
    check_quorum_ready(ready, messages, 4U);
    check_equal(tr_raft_core_step(core, &response, ready), SALTS_OK);
    term = ready->term;
    check_equal(ready->message_count, 2U);
    check_equal(tr_raft_core_advance(core), SALTS_OK);

    memset(&response, 0, sizeof(response));
    response.type = TR_RAFT_MSG_VOTE_RESPONSE;
    response.from = 2U;
    response.to = 1U;
    response.term = term;
    response.granted = true;
    check_quorum_ready(ready, messages, 4U);
    check_equal(tr_raft_core_step(core, &response, ready), SALTS_OK);
    check_equal(ready->message_count, 2U);
    check_equal(tr_raft_core_advance(core), SALTS_OK);
    return term;
}

static tr_raft_core_t *check_quorum_core(void)
{
    static const tr_raft_node_id_t voters[] = {1U, 2U, 3U};
    tr_raft_core_config_t config;
    tr_raft_core_t *core = NULL;

    memset(&config, 0, sizeof(config));
    config.self_id = 1U;
    config.voters = voters;
    config.voter_count = 3U;
    config.heartbeat_ticks = 1U;
    config.election_min_ticks = 3U;
    config.election_max_ticks = 5U;
    config.initial_election_timeout_ticks = 3U;
    config.max_log_entries = 16U;
    check_equal(tr_raft_core_create(&config, &core), SALTS_OK);
    return core;
}

spec("raft check quorum")
{
    it("steps down without a majority response in one election window")
    {
        tr_raft_core_t *core = check_quorum_core();
        tr_raft_message_t messages[4];
        tr_raft_ready_t ready;
        tr_raft_status_t status;
        tr_raft_tick_t tick = {4U, 4U};
        tr_raft_term_t term = check_quorum_elect(core, &ready, messages);

        check_equal(tr_raft_core_status(core, &status), SALTS_OK);
        check_equal(status.role, TR_RAFT_LEADER);
        check_quorum_ready(&ready, messages, 4U);
        check_equal(tr_raft_core_tick(core, &tick, &ready), SALTS_OK);
        check(ready.role_changed);
        check_equal(ready.role, TR_RAFT_FOLLOWER);
        check_equal(tr_raft_core_advance(core), SALTS_OK);
        check_equal(tr_raft_core_status(core, &status), SALTS_OK);
        check_equal(status.role, TR_RAFT_FOLLOWER);
        check_equal(status.term, term);

        tr_raft_core_destroy(core);
    }

    it("retains leadership with a majority response and resets the window")
    {
        tr_raft_core_t *core = check_quorum_core();
        tr_raft_message_t messages[4];
        tr_raft_message_t response;
        tr_raft_ready_t ready;
        tr_raft_status_t status;
        tr_raft_tick_t tick = {4U, 4U};
        tr_raft_term_t term = check_quorum_elect(core, &ready, messages);

        memset(&response, 0, sizeof(response));
        response.type = TR_RAFT_MSG_HEARTBEAT_RESPONSE;
        response.from = 2U;
        response.to = 1U;
        response.term = term;
        response.granted = true;
        check_quorum_ready(&ready, messages, 4U);
        check_equal(tr_raft_core_step(core, &response, &ready), SALTS_OK);
        check(!check_quorum_has_effects(&ready));

        check_quorum_ready(&ready, messages, 4U);
        check_equal(tr_raft_core_tick(core, &tick, &ready), SALTS_OK);
        check_equal(ready.message_count, 2U);
        check_equal(tr_raft_core_advance(core), SALTS_OK);
        check_equal(tr_raft_core_status(core, &status), SALTS_OK);
        check_equal(status.role, TR_RAFT_LEADER);

        check_quorum_ready(&ready, messages, 4U);
        check_equal(tr_raft_core_tick(core, &tick, &ready), SALTS_OK);
        check(ready.role_changed);
        check_equal(ready.role, TR_RAFT_FOLLOWER);
        check_equal(tr_raft_core_advance(core), SALTS_OK);

        tr_raft_core_destroy(core);
    }
}
