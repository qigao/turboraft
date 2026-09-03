#include <turboraft/raft_core.h>

#include <tinytest.h>
#include <salts_error.h>

#include <string.h>

static const tr_raft_node_id_t replication_voters[] = {1U, 2U, 3U};

static tr_raft_ready_t replication_ready_capacity(
    tr_raft_message_t *messages,
    size_t capacity)
{
    tr_raft_ready_t ready;

    memset(&ready, 0, sizeof(ready));
    ready.messages = messages;
    ready.message_capacity = capacity;
    return ready;
}

static tr_raft_ready_t replication_ready(tr_raft_message_t *messages)
{
    return replication_ready_capacity(messages, 2U);
}

static tr_raft_core_t *replication_core_with_window(size_t window)
{
    tr_raft_core_config_t config;
    tr_raft_core_t *core = NULL;

    memset(&config, 0, sizeof(config));
    config.self_id = 1U;
    config.voters = replication_voters;
    config.voter_count = 3U;
    config.heartbeat_ticks = 2U;
    config.election_min_ticks = 5U;
    config.election_max_ticks = 10U;
    config.initial_election_timeout_ticks = 5U;
    config.max_log_entries = 128U;
    config.max_inflight_append_requests = window;
    check_equal(tr_raft_core_create(&config, &core), SALTS_OK);
    return core;
}

static tr_raft_core_t *replication_core(void)
{
    return replication_core_with_window(0U);
}

static void replication_elect(tr_raft_core_t *core)
{
    tr_raft_message_t messages[2];
    tr_raft_ready_t ready = replication_ready(messages);
    tr_raft_tick_t tick = {5U, 7U};
    tr_raft_message_t response;

    check_equal(tr_raft_core_tick(core, &tick, &ready), SALTS_OK);
    check_equal(tr_raft_core_advance(core), SALTS_OK);
    memset(&response, 0, sizeof(response));
    response.type = TR_RAFT_MSG_PRE_VOTE_RESPONSE;
    response.from = 2U;
    response.to = 1U;
    response.campaign_term = 1U;
    response.granted = true;
    ready = replication_ready(messages);
    check_equal(tr_raft_core_step(core, &response, &ready), SALTS_OK);
    check_equal(tr_raft_core_advance(core), SALTS_OK);
    response.type = TR_RAFT_MSG_VOTE_RESPONSE;
    response.term = 1U;
    ready = replication_ready(messages);
    check_equal(tr_raft_core_step(core, &response, &ready), SALTS_OK);
    check_equal(ready.role, TR_RAFT_LEADER);
    check_equal(tr_raft_core_advance(core), SALTS_OK);
}

spec("native raft replication")
{
    it("returns follower log persistence before append response")
    {
        tr_raft_core_t *core = replication_core();
        tr_raft_message_t output[2];
        tr_raft_ready_t ready = replication_ready(output);
        tr_raft_message_t request;
        tr_raft_status_t status;

        memset(&request, 0, sizeof(request));
        request.type = TR_RAFT_MSG_APPEND_REQUEST;
        request.from = 2U;
        request.to = 1U;
        request.term = 1U;
        request.entry_count = 1U;
        request.entry.index = 1U;
        request.entry.term = 1U;
        request.entry.command_id = 9U;
        request.entry.data_length = 3U;
        memcpy(request.entry.data, "set", 3U);

        check_equal(tr_raft_core_step(core, &request, &ready), SALTS_OK);
        check_true(ready.hard_state_changed);
        check_true(ready.log_changed);
        check_equal(ready.log_entry_count, 1U);
        check_equal(ready.message_count, 1U);
        check_equal(ready.messages[0].type, TR_RAFT_MSG_APPEND_RESPONSE);
        check_true(ready.messages[0].granted);
        check_equal(tr_raft_core_status(core, &status), SALTS_OK);
        check_equal(status.last_log_index, 1U);
        tr_raft_core_destroy(core);
    }

    it("commits a current-term proposal after a voting majority")
    {
        tr_raft_core_t *core = replication_core();
        tr_raft_message_t messages[2];
        tr_raft_ready_t ready;
        tr_raft_proposal_t proposal = {17U, "run", 3U};
        tr_raft_message_t response;
        tr_raft_status_t status;

        replication_elect(core);
        ready = replication_ready(messages);
        check_equal(tr_raft_core_propose(core, &proposal, &ready), SALTS_OK);
        check_true(ready.log_changed);
        check_false(ready.commit_changed);
        check_equal(messages[0].type, TR_RAFT_MSG_APPEND_REQUEST);
        check_equal(tr_raft_core_advance(core), SALTS_OK);

        memset(&response, 0, sizeof(response));
        response.type = TR_RAFT_MSG_APPEND_RESPONSE;
        response.from = 2U;
        response.to = 1U;
        response.term = 1U;
        response.granted = true;
        response.match_index = 1U;
        ready = replication_ready(messages);
        check_equal(tr_raft_core_step(core, &response, &ready), SALTS_OK);
        check_true(ready.commit_changed);
        check_equal(ready.commit_index, 1U);
        check_equal(tr_raft_core_status(core, &status), SALTS_OK);
        check_equal(status.commit_index, 1U);
        tr_raft_core_destroy(core);
    }

    it("rejects a previous-log mismatch without changing the log")
    {
        tr_raft_core_t *core = replication_core();
        tr_raft_message_t output[2];
        tr_raft_ready_t ready = replication_ready(output);
        tr_raft_message_t request;
        tr_raft_status_t status;

        memset(&request, 0, sizeof(request));
        request.type = TR_RAFT_MSG_HEARTBEAT_REQUEST;
        request.from = 2U;
        request.to = 1U;
        request.term = 1U;
        request.previous_log_index = 4U;
        request.previous_log_term = 1U;
        check_equal(tr_raft_core_step(core, &request, &ready), SALTS_OK);
        check_false(ready.messages[0].granted);
        check_equal(ready.messages[0].reject_hint, 1U);
        check_false(ready.log_changed);
        check_equal(tr_raft_core_status(core, &status), SALTS_OK);
        check_equal(status.last_log_index, 0U);
        tr_raft_core_destroy(core);
    }

    it("reconciles one bounded multi-entry AppendRequest atomically")
    {
        tr_raft_core_t *core = replication_core();
        tr_raft_message_t output[2];
        tr_raft_ready_t ready = replication_ready(output);
        tr_raft_message_t request;
        tr_raft_status_t status;
        size_t index;

        memset(&request, 0, sizeof(request));
        request.type = TR_RAFT_MSG_APPEND_REQUEST;
        request.from = 2U;
        request.to = 1U;
        request.term = 1U;
        request.entry_count = 3U;
        for (index = 0U; index < request.entry_count; ++index) {
            request.entries[index].index = index + 1U;
            request.entries[index].term = 1U;
            request.entries[index].command_id = index + 10U;
            request.entries[index].data_length = 1U;
            request.entries[index].data[0] = (uint8_t) ('a' + index);
        }
        check_equal(tr_raft_core_step(core, &request, &ready), SALTS_OK);
        check(ready.log_changed);
        check_equal(ready.log_entry_count, 3U);
        check_equal(ready.messages[0].match_index, 3U);
        check_equal(tr_raft_core_status(core, &status), SALTS_OK);
        check_equal(status.last_log_index, 3U);
        tr_raft_core_destroy(core);
    }

    it("batches lagging replication with one bounded append in flight")
    {
        tr_raft_core_t *core = replication_core();
        tr_raft_message_t messages[2];
        tr_raft_ready_t ready;
        tr_raft_proposal_t proposal;
        tr_raft_message_t response;
        tr_raft_tick_t tick = {2U, 7U};
        tr_raft_status_t status;
        size_t index;

        replication_elect(core);
        for (index = 1U; index <= 10U; ++index) {
            uint8_t value = (uint8_t) index;

            proposal.command_id = index;
            proposal.data = &value;
            proposal.data_length = 1U;
            ready = replication_ready(messages);
            check_equal(tr_raft_core_propose(core, &proposal, &ready),
                         SALTS_OK);
            if (index == 1U) {
                check_equal(ready.message_count, 2U);
            } else {
                check_equal(ready.message_count, 0U);
            }
            check_equal(tr_raft_core_advance(core), SALTS_OK);
        }

        ready = replication_ready(messages);
        check_equal(tr_raft_core_tick(core, &tick, &ready), SALTS_OK);
        check_equal(ready.message_count, 2U);
        check_equal(ready.messages[0].entry_count,
                      TR_RAFT_MAX_APPEND_ENTRIES);
        check_equal(ready.messages[0].entries[7].index, 8U);
        check_equal(tr_raft_core_advance(core), SALTS_OK);

        memset(&response, 0, sizeof(response));
        response.type = TR_RAFT_MSG_APPEND_RESPONSE;
        response.from = 2U;
        response.to = 1U;
        response.term = 1U;
        response.granted = true;
        response.previous_log_index = 0U;
        response.match_index = 8U;
        ready = replication_ready(messages);
        check_equal(tr_raft_core_step(core, &response, &ready), SALTS_OK);
        check_equal(ready.message_count, 1U);
        check_equal(ready.messages[0].entry_count, 2U);
        check_equal(ready.messages[0].entries[0].index, 9U);
        check_equal(ready.messages[0].entries[1].index, 10U);
        check_equal(tr_raft_core_status(core, &status), SALTS_OK);
        check_equal(status.commit_index, 8U);
        check_equal(status.inflight_append_count, 2U);
        tr_raft_core_destroy(core);
    }

    it("fills a configured peer window within Ready capacity")
    {
        tr_raft_core_t *core = replication_core_with_window(4U);
        tr_raft_message_t messages[8];
        tr_raft_ready_t ready;
        tr_raft_proposal_t proposal;
        tr_raft_message_t response;
        tr_raft_progress_view_t progress;
        size_t index;

        replication_elect(core);
        for (index = 1U; index <= 34U; ++index) {
            uint8_t value = (uint8_t) index;

            proposal.command_id = index;
            proposal.data = &value;
            proposal.data_length = 1U;
            ready = replication_ready_capacity(messages, 8U);
            check_equal(tr_raft_core_propose(core, &proposal, &ready),
                         SALTS_OK);
            check_equal(tr_raft_core_advance(core), SALTS_OK);
        }

        memset(&response, 0, sizeof(response));
        response.type = TR_RAFT_MSG_APPEND_RESPONSE;
        response.from = 2U;
        response.to = 1U;
        response.term = 1U;
        response.granted = true;
        response.previous_log_index = 0U;
        response.match_index = 1U;
        ready = replication_ready_capacity(messages, 2U);
        check_equal(tr_raft_core_step(core, &response, &ready), SALTS_OK);
        check_equal(ready.message_count, 2U);
        check_equal(ready.messages[0].previous_log_index, 1U);
        check_equal(ready.messages[0].entries[7].index, 9U);
        check_equal(ready.messages[1].previous_log_index, 9U);
        check_equal(ready.messages[1].entries[7].index, 17U);
        check_equal(tr_raft_core_advance(core), SALTS_OK);

        ready = replication_ready_capacity(messages, 8U);
        check_equal(tr_raft_core_poll(core, &ready), SALTS_OK);
        check_equal(ready.message_count, 2U);
        check_equal(ready.messages[0].previous_log_index, 17U);
        check_equal(ready.messages[0].entries[7].index, 25U);
        check_equal(ready.messages[1].previous_log_index, 25U);
        check_equal(ready.messages[1].entries[7].index, 33U);
        check_equal(tr_raft_core_progress(core, &progress), SALTS_OK);
        check_equal(progress.peers[1].inflight_append_count, 4U);
        check_equal(progress.peers[1].max_inflight_append_requests, 4U);
        check_false(progress.peers[1].append_probe);
        check_equal(tr_raft_core_advance(core), SALTS_OK);
        tr_raft_core_destroy(core);
    }

    it("releases cumulative acknowledgements and probes after rejection")
    {
        tr_raft_core_t *core = replication_core_with_window(4U);
        tr_raft_message_t messages[8];
        tr_raft_ready_t ready;
        tr_raft_proposal_t proposal;
        tr_raft_message_t response;
        tr_raft_progress_view_t progress;
        size_t index;

        replication_elect(core);
        for (index = 1U; index <= 34U; ++index) {
            uint8_t value = (uint8_t) index;

            proposal.command_id = index;
            proposal.data = &value;
            proposal.data_length = 1U;
            ready = replication_ready_capacity(messages, 8U);
            check_equal(tr_raft_core_propose(core, &proposal, &ready),
                         SALTS_OK);
            check_equal(tr_raft_core_advance(core), SALTS_OK);
        }

        memset(&response, 0, sizeof(response));
        response.type = TR_RAFT_MSG_APPEND_RESPONSE;
        response.from = 2U;
        response.to = 1U;
        response.term = 1U;
        response.granted = true;
        response.previous_log_index = 0U;
        response.match_index = 1U;
        ready = replication_ready_capacity(messages, 8U);
        check_equal(tr_raft_core_step(core, &response, &ready), SALTS_OK);
        check_equal(ready.message_count, 4U);
        check_equal(tr_raft_core_advance(core), SALTS_OK);

        response.previous_log_index = 9U;
        response.match_index = 17U;
        ready = replication_ready_capacity(messages, 8U);
        check_equal(tr_raft_core_step(core, &response, &ready), SALTS_OK);
        check_equal(ready.message_count, 1U);
        check_equal(ready.messages[0].previous_log_index, 33U);
        check_equal(ready.messages[0].entries[0].index, 34U);
        check_equal(tr_raft_core_progress(core, &progress), SALTS_OK);
        check_equal(progress.peers[1].match_index, 17U);
        check_equal(progress.peers[1].inflight_append_count, 3U);
        check_equal(tr_raft_core_advance(core), SALTS_OK);

        response.granted = false;
        response.previous_log_index = 17U;
        response.match_index = 0U;
        response.reject_hint = 10U;
        ready = replication_ready_capacity(messages, 8U);
        check_equal(tr_raft_core_step(core, &response, &ready), SALTS_OK);
        check_equal(ready.message_count, 1U);
        check_equal(ready.messages[0].previous_log_index, 17U);
        check_equal(ready.messages[0].entry_count,
                      TR_RAFT_MAX_APPEND_ENTRIES);
        check_equal(tr_raft_core_progress(core, &progress), SALTS_OK);
        check_equal(progress.peers[1].inflight_append_count, 1U);
        check_true(progress.peers[1].append_probe);
        check_equal(progress.peers[1].next_index, 26U);
        check_equal(tr_raft_core_advance(core), SALTS_OK);

        response.previous_log_index = 9U;
        response.reject_hint = 1U;
        ready = replication_ready_capacity(messages, 8U);
        check_equal(tr_raft_core_step(core, &response, &ready), SALTS_OK);
        check_equal(ready.message_count, 0U);
        check_equal(tr_raft_core_progress(core, &progress), SALTS_OK);
        check_equal(progress.peers[1].next_index, 26U);
        check_equal(progress.peers[1].inflight_append_count, 1U);

        response.granted = true;
        response.match_index = 25U;
        ready = replication_ready_capacity(messages, 8U);
        check_equal(tr_raft_core_step(core, &response, &ready), SALTS_OK);
        check_equal(ready.message_count, 0U);
        check_equal(tr_raft_core_progress(core, &progress), SALTS_OK);
        check_equal(progress.peers[1].match_index, 17U);
        check_equal(progress.peers[1].next_index, 26U);
        check_equal(progress.peers[1].inflight_append_count, 1U);
        tr_raft_core_destroy(core);
    }

    it("rejects a replication window above the hard limit")
    {
        tr_raft_core_config_t config;
        tr_raft_core_t *core = NULL;

        memset(&config, 0, sizeof(config));
        config.self_id = 1U;
        config.voters = replication_voters;
        config.voter_count = 3U;
        config.heartbeat_ticks = 2U;
        config.election_min_ticks = 5U;
        config.election_max_ticks = 10U;
        config.initial_election_timeout_ticks = 5U;
        config.max_inflight_append_requests =
            TR_RAFT_MAX_INFLIGHT_APPEND_REQUESTS + 1U;
        check_equal(tr_raft_core_create(&config, &core), SALTS_EINVAL);
        check_null(core);
    }

    it("resets a timed-out pipeline to one probe from confirmed progress")
    {
        tr_raft_core_t *core = replication_core_with_window(4U);
        tr_raft_message_t messages[8];
        tr_raft_ready_t ready;
        tr_raft_proposal_t proposal;
        tr_raft_message_t response;
        tr_raft_tick_t tick = {2U, 7U};
        tr_raft_progress_view_t progress;
        size_t index;

        replication_elect(core);
        for (index = 1U; index <= 24U; ++index) {
            uint8_t value = (uint8_t) index;

            proposal.command_id = index;
            proposal.data = &value;
            proposal.data_length = 1U;
            ready = replication_ready_capacity(messages, 8U);
            check_equal(tr_raft_core_propose(core, &proposal, &ready),
                         SALTS_OK);
            check_equal(tr_raft_core_advance(core), SALTS_OK);
        }
        memset(&response, 0, sizeof(response));
        response.type = TR_RAFT_MSG_APPEND_RESPONSE;
        response.from = 2U;
        response.to = 1U;
        response.term = 1U;
        response.granted = true;
        response.previous_log_index = 0U;
        response.match_index = 1U;
        ready = replication_ready_capacity(messages, 8U);
        check_equal(tr_raft_core_step(core, &response, &ready), SALTS_OK);
        check_equal(ready.message_count, 3U);
        check_equal(tr_raft_core_advance(core), SALTS_OK);

        ready = replication_ready_capacity(messages, 8U);
        check_equal(tr_raft_core_tick(core, &tick, &ready), SALTS_OK);
        check_equal(ready.message_count, 2U);
        check_equal(ready.messages[0].previous_log_index, 1U);
        check_equal(ready.messages[0].entry_count,
                      TR_RAFT_MAX_APPEND_ENTRIES);
        check_equal(tr_raft_core_progress(core, &progress), SALTS_OK);
        check_equal(progress.peers[1].inflight_append_count, 1U);
        check_true(progress.peers[1].append_probe);
        check_equal(progress.peers[1].match_index, 1U);
        check_equal(progress.peers[1].next_index, 10U);
        tr_raft_core_destroy(core);
    }
}
