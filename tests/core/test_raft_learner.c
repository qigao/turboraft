#include <turboraft/raft_core.h>

#include <tinytest.h>
#include <salts_error.h>

#include <string.h>

static const tr_raft_node_id_t learner_voters[] = {1U, 2U, 3U};
static const tr_raft_node_id_t learner_nodes[] = {4U};

static tr_raft_ready_t learner_ready(tr_raft_message_t *messages)
{
    tr_raft_ready_t ready;

    memset(&ready, 0, sizeof(ready));
    ready.messages = messages;
    ready.message_capacity = 4U;
    return ready;
}

static tr_raft_core_t *learner_core(tr_raft_node_id_t self_id)
{
    tr_raft_core_config_t config;
    tr_raft_core_t *core = NULL;

    memset(&config, 0, sizeof(config));
    config.self_id = self_id;
    config.voters = learner_voters;
    config.voter_count = 3U;
    config.learners = learner_nodes;
    config.learner_count = 1U;
    config.heartbeat_ticks = 2U;
    config.election_min_ticks = 5U;
    config.election_max_ticks = 10U;
    config.initial_election_timeout_ticks = 5U;
    config.max_log_entries = 16U;
    check_equal(tr_raft_core_create(&config, &core), SALTS_OK);
    return core;
}

static tr_raft_term_t learner_elect(tr_raft_core_t *core)
{
    tr_raft_message_t messages[4];
    tr_raft_ready_t ready = learner_ready(messages);
    tr_raft_tick_t tick = {5U, 7U};
    tr_raft_message_t response;
    tr_raft_term_t term;

    check_equal(tr_raft_core_tick(core, &tick, &ready), SALTS_OK);
    check_equal(ready.message_count, 2U);
    term = ready.messages[0].campaign_term;
    check_equal(tr_raft_core_advance(core), SALTS_OK);
    memset(&response, 0, sizeof(response));
    response.type = TR_RAFT_MSG_PRE_VOTE_RESPONSE;
    response.from = 2U;
    response.to = 1U;
    response.campaign_term = term;
    response.granted = true;
    ready = learner_ready(messages);
    check_equal(tr_raft_core_step(core, &response, &ready), SALTS_OK);
    check_equal(ready.message_count, 2U);
    term = ready.term;
    check_equal(tr_raft_core_advance(core), SALTS_OK);
    response.type = TR_RAFT_MSG_VOTE_RESPONSE;
    response.term = term;
    ready = learner_ready(messages);
    check_equal(tr_raft_core_step(core, &response, &ready), SALTS_OK);
    check_equal(ready.role, TR_RAFT_LEADER);
    check_equal(ready.message_count, 3U);
    check_equal(ready.messages[2].to, 4U);
    check_equal(tr_raft_core_advance(core), SALTS_OK);
    return term;
}

spec("raft learners")
{
    it("replicates to learners without counting them toward commit")
    {
        tr_raft_core_t *core = learner_core(1U);
        tr_raft_message_t messages[4];
        tr_raft_ready_t ready;
        tr_raft_proposal_t proposal = {1U, "x", 1U};
        tr_raft_message_t response;
        tr_raft_status_t status;
        tr_raft_term_t term = learner_elect(core);

        ready = learner_ready(messages);
        check_equal(tr_raft_core_propose(core, &proposal, &ready), SALTS_OK);
        check_equal(ready.message_count, 3U);
        check_equal(ready.messages[2].to, 4U);
        check_equal(tr_raft_core_advance(core), SALTS_OK);

        memset(&response, 0, sizeof(response));
        response.type = TR_RAFT_MSG_APPEND_RESPONSE;
        response.from = 4U;
        response.to = 1U;
        response.term = term;
        response.granted = true;
        response.match_index = 1U;
        ready = learner_ready(messages);
        check_equal(tr_raft_core_step(core, &response, &ready), SALTS_OK);
        check(!ready.commit_changed);
        check_equal(tr_raft_core_status(core, &status), SALTS_OK);
        check_equal(status.commit_index, 0U);
        check_equal(status.voter_count, 3U);
        check_equal(status.learner_count, 1U);

        response.from = 2U;
        ready = learner_ready(messages);
        check_equal(tr_raft_core_step(core, &response, &ready), SALTS_OK);
        check(ready.commit_changed);
        check_equal(ready.commit_index, 1U);
        tr_raft_core_destroy(core);
    }

    it("never campaigns or votes when self is a learner")
    {
        tr_raft_core_t *core = learner_core(4U);
        tr_raft_message_t messages[4];
        tr_raft_ready_t ready = learner_ready(messages);
        tr_raft_tick_t tick = {5U, 7U};
        tr_raft_message_t request;
        tr_raft_status_t status;

        check_equal(tr_raft_core_tick(core, &tick, &ready), SALTS_OK);
        check_equal(ready.message_count, 0U);
        check_equal(tr_raft_core_status(core, &status), SALTS_OK);
        check_equal(status.role, TR_RAFT_FOLLOWER);
        check(!status.self_is_voter);

        memset(&request, 0, sizeof(request));
        request.type = TR_RAFT_MSG_PRE_VOTE_REQUEST;
        request.from = 1U;
        request.to = 4U;
        request.campaign_term = 1U;
        ready = learner_ready(messages);
        check_equal(tr_raft_core_step(core, &request, &ready), SALTS_OK);
        check_equal(ready.message_count, 1U);
        check(!ready.messages[0].granted);
        check_equal(tr_raft_core_advance(core), SALTS_OK);

        request.type = TR_RAFT_MSG_VOTE_REQUEST;
        request.term = 1U;
        ready = learner_ready(messages);
        check_equal(tr_raft_core_step(core, &request, &ready), SALTS_OK);
        check(!ready.messages[0].granted);
        check_equal(ready.voted_for, 0U);
        tr_raft_core_destroy(core);
    }

    it("rejects overlapping voter and learner sets")
    {
        const tr_raft_node_id_t overlapping[] = {3U};
        tr_raft_core_config_t config;
        tr_raft_core_t *core = NULL;

        memset(&config, 0, sizeof(config));
        config.self_id = 1U;
        config.voters = learner_voters;
        config.voter_count = 3U;
        config.learners = overlapping;
        config.learner_count = 1U;
        config.heartbeat_ticks = 2U;
        config.election_min_ticks = 5U;
        config.election_max_ticks = 10U;
        config.initial_election_timeout_ticks = 5U;
        check_equal(tr_raft_core_create(&config, &core), SALTS_EINVAL);
        check(core == NULL);
    }
}
