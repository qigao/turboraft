#include <turboraft/raft_core.h>

#include <tinytest.h>
#include <salts_error.h>

#include <string.h>

static const tr_raft_node_id_t read_voters[] = {1U, 2U, 3U};

static tr_raft_ready_t read_ready(tr_raft_message_t *messages)
{
    tr_raft_ready_t ready;

    memset(&ready, 0, sizeof(ready));
    ready.messages = messages;
    ready.message_capacity = 4U;
    return ready;
}

static tr_raft_core_t *read_core(tr_raft_node_id_t self_id)
{
    tr_raft_core_config_t config;
    tr_raft_core_t *core = NULL;

    memset(&config, 0, sizeof(config));
    config.self_id = self_id;
    config.voters = read_voters;
    config.voter_count = 3U;
    config.heartbeat_ticks = 2U;
    config.election_min_ticks = 5U;
    config.election_max_ticks = 10U;
    config.initial_election_timeout_ticks = 5U;
    config.max_log_entries = 16U;
    config.max_pending_reads = 4U;
    check_equal(tr_raft_core_create(&config, &core), SALTS_OK);
    return core;
}

static tr_raft_term_t read_elect(tr_raft_core_t *core)
{
    tr_raft_message_t messages[4];
    tr_raft_ready_t ready = read_ready(messages);
    tr_raft_tick_t tick = {5U, 7U};
    tr_raft_message_t response;
    tr_raft_term_t term;

    check_equal(tr_raft_core_tick(core, &tick, &ready), SALTS_OK);
    term = ready.messages[0].campaign_term;
    check_equal(tr_raft_core_advance(core), SALTS_OK);
    memset(&response, 0, sizeof(response));
    response.type = TR_RAFT_MSG_PRE_VOTE_RESPONSE;
    response.from = 2U;
    response.to = 1U;
    response.campaign_term = term;
    response.granted = true;
    ready = read_ready(messages);
    check_equal(tr_raft_core_step(core, &response, &ready), SALTS_OK);
    term = ready.term;
    check_equal(tr_raft_core_advance(core), SALTS_OK);
    response.type = TR_RAFT_MSG_VOTE_RESPONSE;
    response.term = term;
    ready = read_ready(messages);
    check_equal(tr_raft_core_step(core, &response, &ready), SALTS_OK);
    check_equal(tr_raft_core_advance(core), SALTS_OK);
    return term;
}

static void read_commit_current_term(tr_raft_core_t *core,
                                     tr_raft_term_t term)
{
    tr_raft_message_t messages[4];
    tr_raft_ready_t ready = read_ready(messages);
    tr_raft_proposal_t proposal = {1U, "x", 1U};
    tr_raft_message_t response;

    check_equal(tr_raft_core_propose(core, &proposal, &ready), SALTS_OK);
    check_equal(tr_raft_core_advance(core), SALTS_OK);
    memset(&response, 0, sizeof(response));
    response.type = TR_RAFT_MSG_APPEND_RESPONSE;
    response.from = 2U;
    response.to = 1U;
    response.term = term;
    response.granted = true;
    response.match_index = 1U;
    ready = read_ready(messages);
    check_equal(tr_raft_core_step(core, &response, &ready), SALTS_OK);
    check(ready.commit_changed);
    check_equal(tr_raft_core_advance(core), SALTS_OK);
}

spec("raft read index")
{
    it("admits bounded concurrent read barriers and matches each context")
    {
        tr_raft_core_t *core = read_core(1U);
        tr_raft_message_t messages[4];
        tr_raft_ready_t ready;
        tr_raft_message_t response;
        tr_raft_status_t status;
        tr_raft_term_t term = read_elect(core);
        uint64_t context;

        ready = read_ready(messages);
        check_equal(tr_raft_core_read_index(core, 40U, &ready), SALTS_EBUSY);
        read_commit_current_term(core, term);

        for (context = 41U; context <= 44U; ++context) {
            ready = read_ready(messages);
            check_equal(tr_raft_core_read_index(core, context, &ready),
                        SALTS_OK);
            check_equal(ready.message_count, 2U);
            check_equal(ready.messages[0].type,
                        TR_RAFT_MSG_READ_INDEX_REQUEST);
            check_equal(ready.messages[0].context_id, context);
            check_equal(tr_raft_core_advance(core), SALTS_OK);
        }

        ready = read_ready(messages);
        check_equal(tr_raft_core_read_index(core, 45U, &ready),
                    SALTS_ENOSPC);

        check_equal(tr_raft_core_status(core, &status), SALTS_OK);
        check_equal(status.pending_read_count, 4U);
        check_equal(status.max_pending_reads, 4U);

        memset(&response, 0, sizeof(response));
        response.type = TR_RAFT_MSG_READ_INDEX_RESPONSE;
        response.from = 2U;
        response.to = 1U;
        response.term = term;
        response.context_id = 99U;
        ready = read_ready(messages);
        check_equal(tr_raft_core_step(core, &response, &ready), SALTS_OK);
        check_equal(ready.read_state_count, 0U);

        response.context_id = 42U;
        ready = read_ready(messages);
        check_equal(tr_raft_core_step(core, &response, &ready), SALTS_OK);
        check_equal(ready.read_state_count, 1U);
        check_equal(ready.read_states[0].context_id, 42U);
        check_equal(ready.read_states[0].index, 1U);
        check_equal(tr_raft_core_advance(core), SALTS_OK);

        check_equal(tr_raft_core_status(core, &status), SALTS_OK);
        check_equal(status.pending_read_count, 3U);

        ready = read_ready(messages);
        check_equal(tr_raft_core_read_index(core, 45U, &ready), SALTS_OK);
        check_equal(tr_raft_core_advance(core), SALTS_OK);
        check_equal(tr_raft_core_status(core, &status), SALTS_OK);
        check_equal(status.pending_read_count, 4U);

        tr_raft_core_destroy(core);
    }

    it("coalesces an explicit read batch behind one quorum barrier")
    {
        const uint64_t contexts[] = {101U, 102U, 103U};
        tr_raft_core_t *core = read_core(1U);
        tr_raft_message_t messages[4];
        tr_raft_ready_t ready;
        tr_raft_message_t response;
        tr_raft_status_t status;
        tr_raft_term_t term = read_elect(core);
        size_t index;

        read_commit_current_term(core, term);
        ready = read_ready(messages);
        check_equal(tr_raft_core_read_index_batch(
                        core, contexts,
                        sizeof(contexts) / sizeof(contexts[0]), &ready),
                    SALTS_OK);
        check_equal(ready.message_count, 2U);
        check_equal(ready.messages[0].context_id, contexts[0]);
        check_equal(ready.messages[1].context_id, contexts[0]);
        check_equal(tr_raft_core_advance(core), SALTS_OK);

        check_equal(tr_raft_core_status(core, &status), SALTS_OK);
        check_equal(status.pending_read_count, 3U);

        memset(&response, 0, sizeof(response));
        response.type = TR_RAFT_MSG_READ_INDEX_RESPONSE;
        response.from = 2U;
        response.to = 1U;
        response.term = term;
        response.context_id = contexts[0];
        ready = read_ready(messages);
        check_equal(tr_raft_core_step(core, &response, &ready), SALTS_OK);
        check_equal(ready.read_state_count, 3U);
        for (index = 0U; index < 3U; ++index) {
            check_equal(ready.read_states[index].context_id, contexts[index]);
            check_equal(ready.read_states[index].index, 1U);
        }
        check_equal(tr_raft_core_advance(core), SALTS_OK);
        check_equal(tr_raft_core_status(core, &status), SALTS_OK);
        check_equal(status.pending_read_count, 0U);

        tr_raft_core_destroy(core);
    }

    it("followers echo read context only for their recognized leader")
    {
        tr_raft_core_t *core = read_core(2U);
        tr_raft_message_t messages[4];
        tr_raft_ready_t ready = read_ready(messages);
        tr_raft_message_t request;

        memset(&request, 0, sizeof(request));
        request.type = TR_RAFT_MSG_HEARTBEAT_REQUEST;
        request.from = 1U;
        request.to = 2U;
        request.term = 3U;
        check_equal(tr_raft_core_step(core, &request, &ready), SALTS_OK);
        check_equal(tr_raft_core_advance(core), SALTS_OK);
        request.type = TR_RAFT_MSG_READ_INDEX_REQUEST;
        request.context_id = 55U;
        ready = read_ready(messages);
        check_equal(tr_raft_core_step(core, &request, &ready), SALTS_OK);
        check_equal(ready.message_count, 1U);
        check_equal(ready.messages[0].type,
                     TR_RAFT_MSG_READ_INDEX_RESPONSE);
        check_equal(ready.messages[0].context_id, 55U);
        tr_raft_core_destroy(core);
    }
}
