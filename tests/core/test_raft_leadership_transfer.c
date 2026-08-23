#include <turboraft/raft_core.h>

#include <tinytest.h>
#include <turbo_error.h>

#include <string.h>

static const tr_raft_node_id_t transfer_voters[] = {1U, 2U, 3U};

static tr_raft_ready_t transfer_ready(tr_raft_message_t *messages)
{
    tr_raft_ready_t ready;

    memset(&ready, 0, sizeof(ready));
    ready.messages = messages;
    ready.message_capacity = 4U;
    return ready;
}

static tr_raft_core_t *transfer_core(tr_raft_node_id_t self_id)
{
    tr_raft_core_config_t config;
    tr_raft_core_t *core = NULL;

    memset(&config, 0, sizeof(config));
    config.self_id = self_id;
    config.voters = transfer_voters;
    config.voter_count = 3U;
    config.heartbeat_ticks = 2U;
    config.election_min_ticks = 5U;
    config.election_max_ticks = 10U;
    config.initial_election_timeout_ticks = 5U;
    config.max_log_entries = 16U;
    check_equal(tr_raft_core_create(&config, &core), TURBO_OK);
    return core;
}

static tr_raft_term_t transfer_elect(tr_raft_core_t *core)
{
    tr_raft_message_t messages[4];
    tr_raft_ready_t ready = transfer_ready(messages);
    tr_raft_tick_t tick = {5U, 7U};
    tr_raft_message_t response;
    tr_raft_term_t term;

    check_equal(tr_raft_core_tick(core, &tick, &ready), TURBO_OK);
    term = ready.messages[0].campaign_term;
    check_equal(tr_raft_core_advance(core), TURBO_OK);
    memset(&response, 0, sizeof(response));
    response.type = TR_RAFT_MSG_PRE_VOTE_RESPONSE;
    response.from = 2U;
    response.to = 1U;
    response.campaign_term = term;
    response.granted = true;
    ready = transfer_ready(messages);
    check_equal(tr_raft_core_step(core, &response, &ready), TURBO_OK);
    term = ready.term;
    check_equal(tr_raft_core_advance(core), TURBO_OK);
    response.type = TR_RAFT_MSG_VOTE_RESPONSE;
    response.term = term;
    ready = transfer_ready(messages);
    check_equal(tr_raft_core_step(core, &response, &ready), TURBO_OK);
    check_equal(ready.role, TR_RAFT_LEADER);
    check_equal(tr_raft_core_advance(core), TURBO_OK);
    return term;
}

spec("raft leadership transfer")
{
    it("sends TimeoutNow immediately to a caught-up voter and blocks proposals")
    {
        tr_raft_core_t *core = transfer_core(1U);
        tr_raft_message_t messages[4];
        tr_raft_ready_t ready;
        tr_raft_status_t status;
        tr_raft_proposal_t proposal = {1U, "x", 1U};

        transfer_elect(core);
        ready = transfer_ready(messages);
        check_equal(tr_raft_core_transfer_leadership(core, 2U, &ready),
                     TURBO_OK);
        check_equal(ready.message_count, 1U);
        check_equal(ready.messages[0].type, TR_RAFT_MSG_TIMEOUT_NOW);
        check_equal(ready.messages[0].to, 2U);
        check_equal(tr_raft_core_advance(core), TURBO_OK);
        ready = transfer_ready(messages);
        check_equal(tr_raft_core_propose(core, &proposal, &ready),
                     TURBO_EBUSY);
        check_equal(tr_raft_core_status(core, &status), TURBO_OK);
        check_equal(status.leadership_transfer_target, 2U);
        tr_raft_core_destroy(core);
    }

    it("replicates a lagging transferee before sending TimeoutNow")
    {
        tr_raft_core_t *core = transfer_core(1U);
        tr_raft_message_t messages[4];
        tr_raft_ready_t ready;
        tr_raft_proposal_t proposal = {7U, "set", 3U};
        tr_raft_message_t response;
        tr_raft_term_t term = transfer_elect(core);

        ready = transfer_ready(messages);
        check_equal(tr_raft_core_propose(core, &proposal, &ready), TURBO_OK);
        check_equal(tr_raft_core_advance(core), TURBO_OK);
        ready = transfer_ready(messages);
        check_equal(tr_raft_core_transfer_leadership(core, 2U, &ready),
                     TURBO_OK);
        check_equal(ready.message_count, 0U);

        memset(&response, 0, sizeof(response));
        response.type = TR_RAFT_MSG_APPEND_RESPONSE;
        response.from = 2U;
        response.to = 1U;
        response.term = term;
        response.granted = true;
        response.match_index = 1U;
        ready = transfer_ready(messages);
        check_equal(tr_raft_core_step(core, &response, &ready), TURBO_OK);
        check_equal(ready.message_count, 1U);
        check_equal(ready.messages[0].type, TR_RAFT_MSG_TIMEOUT_NOW);
        tr_raft_core_destroy(core);
    }

    it("accepts TimeoutNow only from the recognized current-term leader")
    {
        tr_raft_core_t *core = transfer_core(2U);
        tr_raft_message_t messages[4];
        tr_raft_ready_t ready = transfer_ready(messages);
        tr_raft_message_t request;

        memset(&request, 0, sizeof(request));
        request.type = TR_RAFT_MSG_HEARTBEAT_REQUEST;
        request.from = 1U;
        request.to = 2U;
        request.term = 3U;
        check_equal(tr_raft_core_step(core, &request, &ready), TURBO_OK);
        check_equal(tr_raft_core_advance(core), TURBO_OK);

        request.type = TR_RAFT_MSG_TIMEOUT_NOW;
        ready = transfer_ready(messages);
        check_equal(tr_raft_core_step(core, &request, &ready), TURBO_OK);
        check(ready.hard_state_changed);
        check(ready.role_changed);
        check_equal(ready.role, TR_RAFT_CANDIDATE);
        check_equal(ready.term, 4U);
        check_equal(ready.message_count, 2U);
        check_equal(ready.messages[0].type, TR_RAFT_MSG_VOTE_REQUEST);
        tr_raft_core_destroy(core);
    }

    it("cancels a stalled transfer after one election timeout")
    {
        tr_raft_core_t *core = transfer_core(1U);
        tr_raft_message_t messages[4];
        tr_raft_ready_t ready;
        tr_raft_proposal_t first = {8U, "a", 1U};
        tr_raft_proposal_t second = {9U, "b", 1U};
        tr_raft_message_t response;
        tr_raft_tick_t tick = {7U, 7U};
        tr_raft_status_t status;
        tr_raft_term_t term = transfer_elect(core);

        ready = transfer_ready(messages);
        check_equal(tr_raft_core_propose(core, &first, &ready), TURBO_OK);
        check_equal(tr_raft_core_advance(core), TURBO_OK);
        ready = transfer_ready(messages);
        check_equal(tr_raft_core_transfer_leadership(core, 2U, &ready),
                     TURBO_OK);
        check_equal(ready.message_count, 0U);

        memset(&response, 0, sizeof(response));
        response.type = TR_RAFT_MSG_APPEND_RESPONSE;
        response.from = 3U;
        response.to = 1U;
        response.term = term;
        response.granted = true;
        response.match_index = 1U;
        ready = transfer_ready(messages);
        check_equal(tr_raft_core_step(core, &response, &ready), TURBO_OK);
        check_equal(tr_raft_core_advance(core), TURBO_OK);
        ready = transfer_ready(messages);
        check_equal(tr_raft_core_tick(core, &tick, &ready), TURBO_OK);
        check_equal(tr_raft_core_advance(core), TURBO_OK);
        check_equal(tr_raft_core_status(core, &status), TURBO_OK);
        check_equal(status.role, TR_RAFT_LEADER);
        check_equal(status.leadership_transfer_target, 0U);

        ready = transfer_ready(messages);
        check_equal(tr_raft_core_propose(core, &second, &ready), TURBO_OK);
        tr_raft_core_destroy(core);
    }
}
