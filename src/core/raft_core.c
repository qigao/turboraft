#include <turboraft/raft_core.h>

#include "raft_log.h"
#include "raft_membership.h"
#include "raft_membership_transition.h"
#include "raft_peer_set.h"

#include <salts_error.h>

#include <limits.h>
#include <stdlib.h>
#include <string.h>

typedef struct tr_raft_append_inflight {
    tr_raft_index_t previous_index;
    tr_raft_index_t last_index;
    uint32_t elapsed_ticks;
} tr_raft_append_inflight_t;

typedef struct tr_raft_append_window {
    tr_raft_append_inflight_t items[TR_RAFT_MAX_INFLIGHT_APPEND_REQUESTS];
    size_t head;
    size_t count;
    bool probe;
} tr_raft_append_window_t;

struct tr_raft_core {
    tr_raft_node_id_t self_id;
    tr_raft_membership_transition_t membership_transition;
    tr_raft_peer_set_t peers;
    bool self_is_voter;
    tr_raft_role_t role;
    tr_raft_node_id_t leader_id;
    tr_raft_term_t term;
    tr_raft_node_id_t voted_for;
    tr_raft_log_t log;
    tr_raft_index_t commit_index;
    tr_raft_index_t applied_index;
    tr_raft_index_t dispatched_apply_index;
    tr_raft_index_t pending_apply_index;
    tr_raft_term_t campaign_term;
    uint32_t heartbeat_ticks;
    uint32_t election_min_ticks;
    uint32_t election_max_ticks;
    uint32_t election_timeout_ticks;
    uint32_t election_elapsed_ticks;
    uint32_t heartbeat_elapsed_ticks;
    uint32_t check_quorum_elapsed_ticks;
    uint32_t recent_active;
    tr_raft_node_id_t leadership_transfer_target;
    uint32_t leadership_transfer_elapsed_ticks;
    bool leadership_transfer_sent;
    uint64_t pending_read_context_id;
    tr_raft_index_t pending_read_index;
    uint32_t pending_read_acks;
    uint32_t pre_votes;
    uint32_t votes;
    tr_raft_index_t next_index[TR_RAFT_MAX_VOTERS];
    tr_raft_index_t match_index[TR_RAFT_MAX_VOTERS];
    tr_raft_append_window_t append_windows[TR_RAFT_MAX_VOTERS];
    size_t max_inflight_append_requests;
    bool ready_outstanding;
    bool split_apply_mode;
    bool in_call;
};

typedef struct tr_raft_before {
    tr_raft_role_t role;
    tr_raft_term_t term;
    tr_raft_node_id_t voted_for;
} tr_raft_before_t;

static void tr_become_follower(tr_raft_core_t *core, tr_raft_term_t term);

static void tr_append_window_reset(tr_raft_append_window_t *window,
                                   bool probe)
{
    memset(window, 0, sizeof(*window));
    window->probe = probe;
}

static tr_raft_append_inflight_t *tr_append_window_at(
    tr_raft_append_window_t *window,
    size_t logical_index)
{
    size_t physical_index =
        (window->head + logical_index) %
        TR_RAFT_MAX_INFLIGHT_APPEND_REQUESTS;

    return &window->items[physical_index];
}

static const tr_raft_append_inflight_t *tr_append_window_at_const(
    const tr_raft_append_window_t *window,
    size_t logical_index)
{
    size_t physical_index =
        (window->head + logical_index) %
        TR_RAFT_MAX_INFLIGHT_APPEND_REQUESTS;

    return &window->items[physical_index];
}

static bool tr_append_window_push(tr_raft_append_window_t *window,
                                  tr_raft_index_t previous_index,
                                  tr_raft_index_t last_index)
{
    tr_raft_append_inflight_t *item;

    if (window->count >= TR_RAFT_MAX_INFLIGHT_APPEND_REQUESTS) {
        return false;
    }
    item = tr_append_window_at(window, window->count);
    item->previous_index = previous_index;
    item->last_index = last_index;
    item->elapsed_ticks = 0U;
    ++window->count;
    return true;
}

/** Time O(window), space O(1); window is bounded by 64. */
static int tr_append_window_find(const tr_raft_append_window_t *window,
                                 tr_raft_index_t previous_index)
{
    size_t index;

    for (index = 0U; index < window->count; ++index) {
        if (tr_append_window_at_const(window, index)->previous_index ==
            previous_index) {
            return (int) index;
        }
    }
    return -1;
}

/** Time O(window), space O(1); releases only a confirmed prefix. */
static void tr_append_window_release_through(
    tr_raft_append_window_t *window,
    tr_raft_index_t match_index)
{
    while (window->count != 0U) {
        const tr_raft_append_inflight_t *front =
            tr_append_window_at_const(window, 0U);

        if (front->last_index > match_index) {
            break;
        }
        memset(&window->items[window->head], 0,
               sizeof(window->items[window->head]));
        window->head = (window->head + 1U) %
                       TR_RAFT_MAX_INFLIGHT_APPEND_REQUESTS;
        --window->count;
    }
    if (window->count == 0U) {
        window->head = 0U;
    }
}

static uint32_t tr_append_window_oldest_elapsed(
    const tr_raft_append_window_t *window)
{
    return window->count == 0U
               ? 0U
               : tr_append_window_at_const(window, 0U)->elapsed_ticks;
}

static bool tr_timeout_valid(const tr_raft_core_t *core, uint32_t timeout)
{
    return timeout >= core->election_min_ticks &&
           timeout <= core->election_max_ticks;
}

static const tr_raft_membership_t *tr_core_membership(
    const tr_raft_core_t *core)
{
    return tr_raft_membership_transition_committed(
        &core->membership_transition);
}

static int tr_voter_index(const tr_raft_core_t *core,
                          tr_raft_node_id_t node_id)
{
    int index = tr_raft_peer_set_index(&core->peers, node_id);

    return index >= 0 &&
                   tr_raft_membership_is_voter(tr_core_membership(core),
                                               node_id)
               ? index
               : -1;
}

static size_t tr_peer_count(const tr_raft_core_t *core)
{
    return core->peers.count;
}

static tr_raft_node_id_t tr_peer_id(const tr_raft_core_t *core,
                                    size_t peer_index)
{
    return core->peers.node_ids[peer_index];
}

static int tr_peer_index(const tr_raft_core_t *core,
                         tr_raft_node_id_t node_id)
{
    return tr_raft_peer_set_index(&core->peers, node_id);
}

static size_t tr_core_voter_count(const tr_raft_core_t *core)
{
    const tr_raft_membership_t *membership = tr_core_membership(core);
    size_t count = 0;
    size_t index;

    for (index = 0U; index < membership->member_count; ++index) {
        count += tr_raft_membership_is_voter(
            membership, membership->members[index].node_id);
    }
    return count;
}

static size_t tr_core_learner_count(const tr_raft_core_t *core)
{
    const tr_raft_membership_t *membership = tr_core_membership(core);
    size_t count = 0U;
    size_t index;

    for (index = 0U; index < membership->member_count; ++index) {
        count += membership->members[index].roles ==
                 TR_RAFT_CONF_LEARNER;
    }
    return count;
}

static bool tr_core_has_quorum(const tr_raft_core_t *core,
                               uint32_t peer_acknowledgements)
{
    const tr_raft_membership_t *membership = tr_core_membership(core);
    uint32_t membership_acknowledgements = 0U;
    size_t index;

    for (index = 0U; index < membership->member_count; ++index) {
        int peer_index = tr_peer_index(
            core, membership->members[index].node_id);

        if (peer_index >= 0 &&
            (peer_acknowledgements &
             (UINT32_C(1) << (uint32_t) peer_index)) != 0U) {
            membership_acknowledgements |=
                UINT32_C(1) << (uint32_t) index;
        }
    }
    return tr_raft_membership_has_quorum(
        membership, membership_acknowledgements);
}

static bool tr_core_match_quorum(const tr_raft_core_t *core,
                                 tr_raft_index_t candidate_index)
{
    const tr_raft_membership_t *membership = tr_core_membership(core);
    tr_raft_index_t membership_matches[TR_RAFT_MAX_MEMBERS] = {0U};
    size_t index;

    for (index = 0U; index < membership->member_count; ++index) {
        int peer_index = tr_peer_index(
            core, membership->members[index].node_id);

        if (peer_index >= 0) {
            membership_matches[index] = core->match_index[peer_index];
        }
    }
    return tr_raft_membership_match_quorum(
        membership, membership_matches, candidate_index);
}

static int tr_core_build_peer_set(
    const tr_raft_membership_transition_t *transition,
    tr_raft_peer_set_t *peers)
{
    const tr_raft_membership_t *memberships[
        TR_RAFT_PEER_SET_MAX_SOURCES];
    size_t count = 0U;
    size_t index;

    memberships[count++] =
        tr_raft_membership_transition_committed(transition);
    for (index = 0U; index < transition->pending_count; ++index) {
        memberships[count++] = &transition->pending[index];
    }
    return tr_raft_peer_set_build(memberships, count, peers);
}

static void tr_core_refresh_peers(tr_raft_core_t *core,
                                  const tr_raft_peer_set_t *next_peers)
{
    tr_raft_peer_set_t old_peers = core->peers;
    tr_raft_index_t old_match[TR_RAFT_MAX_MEMBERS];
    uint32_t old_recent_active = core->recent_active;
    tr_raft_index_t last_index = tr_raft_log_last_index(&core->log);
    size_t index;

    if (old_peers.count == next_peers->count &&
        memcmp(old_peers.node_ids, next_peers->node_ids,
               old_peers.count * sizeof(old_peers.node_ids[0])) == 0) {
        return;
    }
    memcpy(old_match, core->match_index, sizeof(old_match));
    memset(core->next_index, 0, sizeof(core->next_index));
    memset(core->match_index, 0, sizeof(core->match_index));
    memset(core->append_windows, 0, sizeof(core->append_windows));
    core->recent_active = 0U;
    core->peers = *next_peers;

    for (index = 0U; index < core->peers.count; ++index) {
        int old_index = tr_raft_peer_set_index(
            &old_peers, core->peers.node_ids[index]);

        core->next_index[index] = last_index + 1U;
        core->append_windows[index].probe = true;
        if (old_index < 0) {
            continue;
        }
        core->match_index[index] = old_match[old_index];
        core->next_index[index] = old_match[old_index] + 1U;
        if ((old_recent_active &
             (UINT32_C(1) << (uint32_t) old_index)) != 0U) {
            core->recent_active |= UINT32_C(1) << (uint32_t) index;
        }
    }
    if (core->role == TR_RAFT_LEADER) {
        int self_index = tr_peer_index(core, core->self_id);

        if (self_index >= 0) {
            core->match_index[self_index] = last_index;
            core->next_index[self_index] = last_index + 1U;
            tr_append_window_reset(&core->append_windows[self_index],
                                   false);
            core->recent_active |= UINT32_C(1) << (uint32_t) self_index;
        }
    }
}

static int tr_core_sync_membership(tr_raft_core_t *core)
{
    tr_raft_peer_set_t peers;
    int result = tr_core_build_peer_set(&core->membership_transition,
                                        &peers);

    if (result != SALTS_OK) {
        return result;
    }
    tr_core_refresh_peers(core, &peers);
    core->self_is_voter = tr_raft_membership_is_voter(
        tr_core_membership(core), core->self_id);
    if (!core->self_is_voter && core->role == TR_RAFT_LEADER) {
        tr_become_follower(core, core->term);
    }
    return SALTS_OK;
}

static int tr_core_replay_membership_log(tr_raft_core_t *core)
{
    tr_raft_index_t index;
    int result;

    for (index = core->log.base_index + 1U;
         index <= tr_raft_log_last_index(&core->log); ++index) {
        const tr_raft_entry_t *entry = tr_raft_log_get(&core->log, index);

        if (!tr_raft_conf_entry_is_configuration(entry)) {
            continue;
        }
        result = tr_raft_membership_transition_stage_entry(
            &core->membership_transition, entry);
        if (result != SALTS_OK) {
            return result;
        }
    }
    result = tr_raft_membership_transition_apply(
        &core->membership_transition, core->commit_index);
    if (result != SALTS_OK) {
        return result;
    }
    return tr_core_sync_membership(core);
}

static int tr_core_rebuild_pending_transition(
    const tr_raft_core_t *core,
    tr_raft_index_t retained_through,
    const tr_raft_entry_t *incoming,
    size_t incoming_count,
    tr_raft_membership_transition_t *rebuilt)
{
    tr_raft_index_t index;
    size_t incoming_index;
    int result;

    *rebuilt = core->membership_transition;
    memset(rebuilt->pending, 0, sizeof(rebuilt->pending));
    memset(rebuilt->pending_indices, 0, sizeof(rebuilt->pending_indices));
    rebuilt->pending_count = 0U;
    for (index = core->commit_index + 1U;
         index <= retained_through &&
         index <= tr_raft_log_last_index(&core->log);
         ++index) {
        const tr_raft_entry_t *entry = tr_raft_log_get(&core->log, index);

        if (entry != NULL && tr_raft_conf_entry_is_configuration(entry)) {
            result = tr_raft_membership_transition_stage_entry(rebuilt,
                                                               entry);
            if (result != SALTS_OK) {
                return result;
            }
        }
    }
    for (incoming_index = 0U; incoming_index < incoming_count;
         ++incoming_index) {
        if (incoming[incoming_index].index <= core->commit_index ||
            !tr_raft_conf_entry_is_configuration(&incoming[incoming_index])) {
            continue;
        }
        result = tr_raft_membership_transition_stage_entry(
            rebuilt, &incoming[incoming_index]);
        if (result != SALTS_OK) {
            return result;
        }
    }
    return SALTS_OK;
}

static bool tr_log_is_up_to_date(const tr_raft_core_t *core,
                                 const tr_raft_message_t *message)
{
    tr_raft_term_t last_term = tr_raft_log_last_term(&core->log);

    if (message->last_log_term != last_term) {
        return message->last_log_term > last_term;
    }
    return message->last_log_index >= tr_raft_log_last_index(&core->log);
}

static void tr_reset_ready(tr_raft_ready_t *ready)
{
    tr_raft_message_t *messages = ready->messages;
    size_t capacity = ready->message_capacity;

    memset(ready, 0, sizeof(*ready));
    ready->messages = messages;
    ready->message_capacity = capacity;
}

static int tr_begin(tr_raft_core_t *core,
                    tr_raft_ready_t *ready,
                    tr_raft_before_t *before)
{
    if (core == NULL || ready == NULL || before == NULL) {
        return SALTS_EINVAL;
    }
    if (ready->message_capacity != 0U && ready->messages == NULL) {
        return SALTS_EINVAL;
    }
    if (core->in_call || core->ready_outstanding) {
        return SALTS_EPROTO;
    }

    core->in_call = true;
    before->role = core->role;
    before->term = core->term;
    before->voted_for = core->voted_for;
    tr_reset_ready(ready);
    return SALTS_OK;
}

static int tr_require_capacity(tr_raft_core_t *core,
                               tr_raft_ready_t *ready,
                               size_t required)
{
    if (required <= ready->message_capacity) {
        return SALTS_OK;
    }
    core->in_call = false;
    tr_reset_ready(ready);
    return SALTS_ENOSPC;
}

static int tr_finish(tr_raft_core_t *core,
                     tr_raft_ready_t *ready,
                     const tr_raft_before_t *before)
{
    if (core->commit_index > core->dispatched_apply_index) {
        tr_raft_index_t first_index = core->dispatched_apply_index + 1U;

        ready->committed_entries = (const tr_raft_entry_t *)
            tr_raft_log_get(&core->log, first_index);
        ready->committed_entry_count =
            (size_t) (core->commit_index -
                      core->dispatched_apply_index);
        core->pending_apply_index = core->commit_index;
    }
    ready->hard_state_changed = before->term != core->term ||
                                before->voted_for != core->voted_for;
    ready->term = core->term;
    ready->voted_for = core->voted_for;
    ready->role_changed = before->role != core->role;
    ready->role = core->role;
    core->ready_outstanding = ready->message_count != 0U ||
                              ready->hard_state_changed ||
                              ready->role_changed || ready->log_changed ||
                              ready->commit_changed ||
                              ready->committed_entry_count != 0U ||
                              ready->read_state_ready ||
                              ready->snapshot_request_count != 0U;
    core->in_call = false;
    return SALTS_OK;
}

static void tr_emit(tr_raft_core_t *core,
                    tr_raft_ready_t *ready,
                    tr_raft_message_type_t type,
                    tr_raft_node_id_t to,
                    bool granted)
{
    tr_raft_message_t *message = &ready->messages[ready->message_count++];

    memset(message, 0, sizeof(*message));
    message->type = type;
    message->from = core->self_id;
    message->to = to;
    message->term = core->term;
    message->campaign_term = core->campaign_term;
    message->last_log_index = tr_raft_log_last_index(&core->log);
    message->last_log_term = tr_raft_log_last_term(&core->log);
    message->granted = granted;
}

static void tr_emit_snapshot_request(tr_raft_core_t *core,
                                     tr_raft_ready_t *ready,
                                     size_t peer_index)
{
    tr_raft_node_id_t peer_id = tr_peer_id(core, peer_index);
    size_t index;

    for (index = 0U; index < ready->snapshot_request_count; ++index) {
        if (ready->snapshot_requests[index].peer_id == peer_id) {
            return;
        }
    }
    ready->snapshot_requests[ready->snapshot_request_count].peer_id = peer_id;
    ready->snapshot_requests[ready->snapshot_request_count].leader_term =
        core->term;
    ready->snapshot_requests[ready->snapshot_request_count].snapshot_index =
        core->log.base_index;
    ready->snapshot_requests[ready->snapshot_request_count].snapshot_term =
        core->log.base_term;
    ++ready->snapshot_request_count;
}

static void tr_broadcast(tr_raft_core_t *core,
                         tr_raft_ready_t *ready,
                         tr_raft_message_type_t type)
{
    size_t index;

    for (index = 0U; index < tr_peer_count(core); ++index) {
        tr_raft_node_id_t node_id = tr_peer_id(core, index);

        if (node_id != core->self_id &&
            tr_raft_membership_is_voter(tr_core_membership(core), node_id)) {
            tr_emit(core, ready, type, node_id, false);
        }
    }
}

static void tr_broadcast_read_index(tr_raft_core_t *core,
                                    tr_raft_ready_t *ready,
                                    uint64_t context_id)
{
    size_t index;

    for (index = 0U; index < tr_peer_count(core); ++index) {
        tr_raft_node_id_t node_id = tr_peer_id(core, index);

        if (node_id != core->self_id &&
            tr_raft_membership_is_voter(tr_core_membership(core), node_id)) {
            tr_emit(core, ready, TR_RAFT_MSG_READ_INDEX_REQUEST,
                    node_id, false);
            ready->messages[ready->message_count - 1U].context_id = context_id;
        }
    }
}

static bool tr_emit_replication(tr_raft_core_t *core,
                                tr_raft_ready_t *ready,
                                size_t peer_index,
                                bool allow_heartbeat)
{
    tr_raft_append_window_t *window = &core->append_windows[peer_index];
    tr_raft_index_t next_index = core->next_index[peer_index];
    tr_raft_index_t last_index = tr_raft_log_last_index(&core->log);
    size_t window_limit = window->probe
                              ? TR_RAFT_DEFAULT_MAX_INFLIGHT_APPEND_REQUESTS
                              : core->max_inflight_append_requests;
    size_t entry_count = 0U;
    size_t index;
    tr_raft_message_type_t type = TR_RAFT_MSG_HEARTBEAT_REQUEST;

    if (next_index <= core->log.base_index) {
        tr_append_window_reset(window, true);
        tr_emit_snapshot_request(core, ready, peer_index);
        return true;
    }

    if (next_index <= last_index) {
        if (window->count >= window_limit ||
            ready->message_count >= ready->message_capacity) {
            return false;
        }
        entry_count = (size_t) (last_index - next_index + 1U);
        if (entry_count > TR_RAFT_MAX_APPEND_ENTRIES) {
            entry_count = TR_RAFT_MAX_APPEND_ENTRIES;
        }
        type = TR_RAFT_MSG_APPEND_REQUEST;
    } else if (!allow_heartbeat ||
               ready->message_count >= ready->message_capacity) {
        return false;
    }
    tr_emit(core, ready, type, tr_peer_id(core, peer_index), false);
    ready->messages[ready->message_count - 1U].previous_log_index =
        next_index - 1U;
    if (next_index - 1U == core->log.base_index) {
        ready->messages[ready->message_count - 1U].previous_log_term =
            core->log.base_term;
    } else {
        const tr_raft_log_entry_t *previous =
            tr_raft_log_get(&core->log, next_index - 1U);
        ready->messages[ready->message_count - 1U].previous_log_term =
            previous == NULL ? 0U : previous->term;
    }
    ready->messages[ready->message_count - 1U].leader_commit =
        core->commit_index;
    ready->messages[ready->message_count - 1U].entry_count = entry_count;
    for (index = 0U; index < entry_count; ++index) {
        const tr_raft_log_entry_t *entry =
            tr_raft_log_get(&core->log, next_index + index);

        ready->messages[ready->message_count - 1U].entries[index] = *entry;
    }
    if (entry_count != 0U) {
        if (!tr_append_window_push(window, next_index - 1U,
                                   next_index + entry_count - 1U)) {
            --ready->message_count;
            memset(&ready->messages[ready->message_count], 0,
                   sizeof(ready->messages[ready->message_count]));
            return false;
        }
        core->next_index[peer_index] = next_index + entry_count;
    }
    return true;
}

static void tr_broadcast_replication(tr_raft_core_t *core,
                                     tr_raft_ready_t *ready)
{
    size_t index;

    for (index = 0U; index < tr_peer_count(core); ++index) {
        if (tr_peer_id(core, index) != core->self_id) {
            tr_emit_replication(core, ready, index, true);
        }
    }
}

/**
 * Fills only data-bearing AppendEntries, round-robin across peers.
 * Time is O(emitted messages * peers), space O(1); both are caller/config
 * bounded.
 */
static void tr_fill_replication_windows(tr_raft_core_t *core,
                                        tr_raft_ready_t *ready)
{
    bool emitted;
    size_t index;

    if (core->role != TR_RAFT_LEADER) {
        return;
    }
    for (index = 0U; index < tr_peer_count(core); ++index) {
        if (tr_peer_id(core, index) != core->self_id &&
            core->next_index[index] <= core->log.base_index) {
            (void) tr_emit_replication(core, ready, index, false);
        }
    }
    do {
        emitted = false;
        for (index = 0U;
             index < tr_peer_count(core) &&
             ready->message_count < ready->message_capacity;
             ++index) {
            if (tr_peer_id(core, index) == core->self_id ||
                core->next_index[index] <= core->log.base_index ||
                core->next_index[index] >
                    tr_raft_log_last_index(&core->log)) {
                continue;
            }
            emitted = tr_emit_replication(core, ready, index, false) ||
                      emitted;
        }
    } while (emitted && ready->message_count < ready->message_capacity);
}

static void tr_fill_peer_replication_window(tr_raft_core_t *core,
                                            tr_raft_ready_t *ready,
                                            size_t peer_index)
{
    if (core->next_index[peer_index] <= core->log.base_index) {
        (void) tr_emit_replication(core, ready, peer_index, false);
        return;
    }
    while (ready->message_count < ready->message_capacity &&
           core->next_index[peer_index] <=
               tr_raft_log_last_index(&core->log) &&
           tr_emit_replication(core, ready, peer_index, false)) {
    }
}

static void tr_become_follower(tr_raft_core_t *core, tr_raft_term_t term)
{
    size_t index;

    if (term > core->term) {
        core->term = term;
        core->voted_for = 0U;
    }
    core->role = TR_RAFT_FOLLOWER;
    core->leader_id = 0U;
    core->campaign_term = 0U;
    core->pre_votes = 0U;
    core->votes = 0U;
    core->election_elapsed_ticks = 0U;
    core->heartbeat_elapsed_ticks = 0U;
    core->check_quorum_elapsed_ticks = 0U;
    core->recent_active = 0U;
    core->leadership_transfer_target = 0U;
    core->leadership_transfer_elapsed_ticks = 0U;
    core->leadership_transfer_sent = false;
    core->pending_read_context_id = 0U;
    core->pending_read_index = 0U;
    core->pending_read_acks = 0U;
    for (index = 0U; index < TR_RAFT_MAX_VOTERS; ++index) {
        tr_append_window_reset(&core->append_windows[index], true);
    }
}

static void tr_become_leader(tr_raft_core_t *core, tr_raft_ready_t *ready)
{
    tr_raft_index_t next_index = tr_raft_log_last_index(&core->log) + 1U;
    int self_index = tr_voter_index(core, core->self_id);
    size_t index;

    core->role = TR_RAFT_LEADER;
    core->leader_id = core->self_id;
    core->campaign_term = 0U;
    core->pre_votes = 0U;
    core->votes = 0U;
    core->election_elapsed_ticks = 0U;
    core->heartbeat_elapsed_ticks = 0U;
    core->check_quorum_elapsed_ticks = 0U;
    core->recent_active = UINT32_C(1) << (uint32_t) self_index;
    core->leadership_transfer_target = 0U;
    core->leadership_transfer_elapsed_ticks = 0U;
    core->leadership_transfer_sent = false;
    core->pending_read_context_id = 0U;
    core->pending_read_index = 0U;
    core->pending_read_acks = 0U;
    for (index = 0U; index < tr_peer_count(core); ++index) {
        tr_append_window_reset(&core->append_windows[index], true);
        core->next_index[index] = next_index;
        core->match_index[index] = 0U;
    }
    core->match_index[self_index] = next_index - 1U;
    core->append_windows[self_index].probe = false;
    tr_broadcast_replication(core, ready);
}

static void tr_start_election(tr_raft_core_t *core, tr_raft_ready_t *ready)
{
    int self_index = tr_voter_index(core, core->self_id);

    core->term = core->campaign_term;
    core->campaign_term = 0U;
    core->role = TR_RAFT_CANDIDATE;
    core->leader_id = 0U;
    core->voted_for = core->self_id;
    core->pre_votes = 0U;
    core->votes = UINT32_C(1) << (uint32_t) self_index;

    if (tr_core_has_quorum(core, core->votes)) {
        tr_become_leader(core, ready);
        return;
    }
    tr_broadcast(core, ready, TR_RAFT_MSG_VOTE_REQUEST);
}

static void tr_start_pre_vote(tr_raft_core_t *core, tr_raft_ready_t *ready)
{
    int self_index = tr_voter_index(core, core->self_id);

    core->role = TR_RAFT_PRE_CANDIDATE;
    core->leader_id = 0U;
    core->campaign_term = core->term + 1U;
    core->pre_votes = UINT32_C(1) << (uint32_t) self_index;
    core->votes = 0U;

    if (tr_core_has_quorum(core, core->pre_votes)) {
        tr_start_election(core, ready);
        return;
    }
    tr_broadcast(core, ready, TR_RAFT_MSG_PRE_VOTE_REQUEST);
}

static bool tr_will_pre_vote_win(const tr_raft_core_t *core,
                                 const tr_raft_message_t *message,
                                 int voter_index)
{
    uint32_t bit = UINT32_C(1) << (uint32_t) voter_index;

    return core->role == TR_RAFT_PRE_CANDIDATE &&
           message->term <= core->term &&
           message->campaign_term == core->campaign_term &&
           message->granted && (core->pre_votes & bit) == 0U &&
           tr_core_has_quorum(core, core->pre_votes | bit);
}

static bool tr_will_vote_win(const tr_raft_core_t *core,
                             const tr_raft_message_t *message,
                             int voter_index)
{
    uint32_t bit = UINT32_C(1) << (uint32_t) voter_index;

    return core->role == TR_RAFT_CANDIDATE &&
           message->term == core->term && message->granted &&
           (core->votes & bit) == 0U &&
           tr_core_has_quorum(core, core->votes | bit);
}

static void tr_step_pre_vote_request(tr_raft_core_t *core,
                                     const tr_raft_message_t *request,
                                     tr_raft_ready_t *ready)
{
    bool term_available = core->term != UINT64_MAX &&
                          request->campaign_term >= core->term + 1U;
    bool granted = core->self_is_voter && term_available &&
                   tr_log_is_up_to_date(core, request);

    core->campaign_term = request->campaign_term;
    tr_emit(core, ready, TR_RAFT_MSG_PRE_VOTE_RESPONSE, request->from,
            granted);
    core->campaign_term = 0U;
}

static void tr_step_pre_vote_response(tr_raft_core_t *core,
                                      const tr_raft_message_t *response,
                                      tr_raft_ready_t *ready,
                                      int voter_index)
{
    uint32_t bit = UINT32_C(1) << (uint32_t) voter_index;

    if (response->term > core->term) {
        tr_become_follower(core, response->term);
        return;
    }
    if (core->role != TR_RAFT_PRE_CANDIDATE ||
        response->campaign_term != core->campaign_term ||
        !response->granted || (core->pre_votes & bit) != 0U) {
        return;
    }

    core->pre_votes |= bit;
    if (tr_core_has_quorum(core, core->pre_votes)) {
        tr_start_election(core, ready);
    }
}

static void tr_step_vote_request(tr_raft_core_t *core,
                                 const tr_raft_message_t *request,
                                 tr_raft_ready_t *ready)
{
    bool granted = false;

    if (request->term > core->term) {
        tr_become_follower(core, request->term);
    }
    if (core->self_is_voter && request->term == core->term &&
        (core->voted_for == 0U || core->voted_for == request->from) &&
        tr_log_is_up_to_date(core, request)) {
        core->voted_for = request->from;
        core->election_elapsed_ticks = 0U;
        granted = true;
    }
    tr_emit(core, ready, TR_RAFT_MSG_VOTE_RESPONSE, request->from, granted);
}

static void tr_step_vote_response(tr_raft_core_t *core,
                                  const tr_raft_message_t *response,
                                  tr_raft_ready_t *ready,
                                  int voter_index)
{
    uint32_t bit = UINT32_C(1) << (uint32_t) voter_index;

    if (response->term > core->term) {
        tr_become_follower(core, response->term);
        return;
    }
    if (core->role != TR_RAFT_CANDIDATE || response->term != core->term ||
        !response->granted || (core->votes & bit) != 0U) {
        return;
    }

    core->votes |= bit;
    if (tr_core_has_quorum(core, core->votes)) {
        tr_become_leader(core, ready);
    }
}

static int tr_step_heartbeat_request(tr_raft_core_t *core,
                                     const tr_raft_message_t *request,
                                     tr_raft_ready_t *ready)
{
    tr_raft_log_reconcile_result_t reconcile;
    const tr_raft_log_entry_t *entries =
        request->entry_count == 0U ? NULL : request->entries;
    tr_raft_message_type_t response_type =
        request->type == TR_RAFT_MSG_APPEND_REQUEST
            ? TR_RAFT_MSG_APPEND_RESPONSE
            : TR_RAFT_MSG_HEARTBEAT_RESPONSE;
    tr_raft_index_t old_commit = core->commit_index;
    tr_raft_membership_transition_t rebuilt_transition =
        core->membership_transition;
    bool previous_matches;
    int result;

    if (request->term < core->term) {
        tr_emit(core, ready, response_type, request->from, false);
        ready->messages[ready->message_count - 1U].reject_hint =
            tr_raft_log_last_index(&core->log) + 1U;
        return SALTS_OK;
    }

    previous_matches = tr_raft_log_matches(
        &core->log, request->previous_log_index,
        request->previous_log_term);
    if (previous_matches) {
        result = tr_core_rebuild_pending_transition(
            core, request->previous_log_index, entries,
            request->entry_count, &rebuilt_transition);
        if (result != SALTS_OK) {
            return result;
        }
    }
    result = tr_raft_log_reconcile(&core->log,
                                   request->previous_log_index,
                                   request->previous_log_term,
                                   entries,
                                   request->entry_count,
                                   core->commit_index,
                                   &reconcile);
    if (result != SALTS_OK) {
        return result;
    }
    tr_become_follower(core, request->term);
    core->leader_id = request->from;
    core->election_elapsed_ticks = 0U;

    if (reconcile.matched) {
        tr_raft_peer_set_t peers;

        core->membership_transition = rebuilt_transition;
        result = tr_core_build_peer_set(&core->membership_transition, &peers);
        if (result != SALTS_OK) {
            return SALTS_EPROTO;
        }
        tr_core_refresh_peers(core, &peers);
    }

    if (reconcile.matched && request->leader_commit > core->commit_index) {
        tr_raft_index_t last_index = tr_raft_log_last_index(&core->log);
        tr_raft_index_t candidate = request->leader_commit < last_index
                                        ? request->leader_commit
                                        : last_index;

        core->commit_index = tr_raft_membership_transition_commit_limit(
            &core->membership_transition, candidate);
        result = tr_raft_membership_transition_apply(
            &core->membership_transition, core->commit_index);
        if (result != SALTS_OK) {
            return result;
        }
        result = tr_core_sync_membership(core);
        if (result != SALTS_OK) {
            return result;
        }
    }
    if (reconcile.changed) {
        ready->log_changed = true;
        ready->log_truncate_from = reconcile.truncate_from;
        ready->log_entries = (const tr_raft_entry_t *) tr_raft_log_get(
            &core->log, reconcile.append_from);
        ready->log_entry_count = reconcile.append_count;
    }
    if (core->commit_index != old_commit) {
        ready->commit_changed = true;
        ready->commit_index = core->commit_index;
    }

    tr_emit(core, ready, response_type, request->from, reconcile.matched);
    ready->messages[ready->message_count - 1U].match_index =
        reconcile.matched
            ? request->previous_log_index + request->entry_count
            : 0U;
    ready->messages[ready->message_count - 1U].reject_hint =
        reconcile.reject_hint;
    ready->messages[ready->message_count - 1U].previous_log_index =
        request->previous_log_index;
    return SALTS_OK;
}

static int tr_update_commit(tr_raft_core_t *core, tr_raft_ready_t *ready)
{
    tr_raft_index_t candidate =
        tr_raft_membership_transition_commit_limit(
            &core->membership_transition,
            tr_raft_log_last_index(&core->log));

    while (candidate > core->commit_index) {
        const tr_raft_log_entry_t *entry = tr_raft_log_get(&core->log,
                                                           candidate);
        if (entry->term != core->term) {
            --candidate;
            continue;
        }
        if (tr_core_match_quorum(core, candidate)) {
            core->commit_index = candidate;
            int result = tr_raft_membership_transition_apply(
                &core->membership_transition, candidate);

            if (result != SALTS_OK) {
                return result;
            }
            result = tr_core_sync_membership(core);
            if (result != SALTS_OK) {
                return result;
            }
            ready->commit_changed = true;
            ready->commit_index = candidate;
            return SALTS_OK;
        }
        --candidate;
    }
    return SALTS_OK;
}

static int tr_core_append_configuration(
    tr_raft_core_t *core,
    const tr_raft_membership_t *configuration,
    tr_raft_ready_t *ready)
{
    tr_raft_membership_transition_t staged = core->membership_transition;
    tr_raft_peer_set_t peers;
    const tr_raft_log_entry_t *entry = NULL;
    tr_raft_index_t next_index = tr_raft_log_last_index(&core->log) + 1U;
    int self_index;
    int result;

    result = tr_raft_membership_transition_stage(
        &staged, configuration, next_index);
    if (result != SALTS_OK) {
        return result;
    }
    result = tr_core_build_peer_set(&staged, &peers);
    if (result != SALTS_OK) {
        return result;
    }
    result = tr_require_capacity(core, ready, peers.count - 1U);
    if (result != SALTS_OK) {
        return result;
    }
    result = tr_raft_log_append_configuration(
        &core->log, core->term, configuration, &entry);
    if (result != SALTS_OK) {
        return result;
    }
    core->membership_transition = staged;
    tr_core_refresh_peers(core, &peers);
    self_index = tr_peer_index(core, core->self_id);
    core->match_index[self_index] = entry->index;
    core->next_index[self_index] = entry->index + 1U;
    ready->log_changed = true;
    ready->log_entries = entry;
    ready->log_entry_count = 1U;
    result = tr_update_commit(core, ready);
    if (result != SALTS_OK) {
        return result;
    }
    tr_broadcast_replication(core, ready);
    return SALTS_OK;
}

static int tr_step_heartbeat_response(tr_raft_core_t *core,
                                      const tr_raft_message_t *response,
                                      tr_raft_ready_t *ready,
                                      int peer_index)
{
    tr_raft_append_window_t *window = &core->append_windows[peer_index];
    tr_raft_index_t last_index = tr_raft_log_last_index(&core->log);
    int inflight_index = -1;

    if (response->term > core->term) {
        tr_become_follower(core, response->term);
        return SALTS_OK;
    }
    if (core->role != TR_RAFT_LEADER || response->term != core->term) {
        return SALTS_OK;
    }
    if (response->granted && response->match_index > last_index) {
        return SALTS_OK;
    }
    if (response->type == TR_RAFT_MSG_APPEND_RESPONSE) {
        inflight_index = tr_append_window_find(
            window, response->previous_log_index);
        if (inflight_index < 0) {
            return SALTS_OK;
        }
        if (response->granted &&
            response->match_index !=
                tr_append_window_at_const(
                    window, (size_t) inflight_index)->last_index) {
            return SALTS_OK;
        }
    }
    if (tr_raft_membership_is_voter(tr_core_membership(core),
                                    response->from)) {
        core->recent_active |= UINT32_C(1) << (uint32_t) peer_index;
    }
    if (response->granted) {
        tr_append_window_release_through(window, response->match_index);
        window->probe = false;
        if (response->match_index > core->match_index[peer_index]) {
            core->match_index[peer_index] = response->match_index;
            if (core->next_index[peer_index] < response->match_index + 1U) {
                core->next_index[peer_index] = response->match_index + 1U;
            }
            int result = tr_update_commit(core, ready);

            if (result != SALTS_OK) {
                return result;
            }
        }
    } else {
        tr_raft_index_t next;

        if (inflight_index >= 0) {
            next = tr_append_window_at_const(
                window, (size_t) inflight_index)->previous_index;
        } else {
            next = core->next_index[peer_index];
            if (next > core->log.base_index + 1U) {
                --next;
            }
        }
        if (response->reject_hint != 0U &&
            response->reject_hint < next) {
            next = response->reject_hint;
        }
        if (next < core->match_index[peer_index] + 1U) {
            next = core->match_index[peer_index] + 1U;
        }
        tr_append_window_reset(window, true);
        core->next_index[peer_index] = next;
    }
    if (response->granted && !core->leadership_transfer_sent &&
        core->leadership_transfer_target == response->from &&
        core->match_index[peer_index] >= last_index) {
        tr_emit(core, ready, TR_RAFT_MSG_TIMEOUT_NOW, response->from, false);
        core->leadership_transfer_sent = true;
    } else if (core->next_index[peer_index] <= last_index ||
               !response->granted) {
        tr_fill_peer_replication_window(core, ready, (size_t) peer_index);
    }
    return SALTS_OK;
}

static void tr_step_timeout_now(tr_raft_core_t *core,
                                const tr_raft_message_t *request,
                                tr_raft_ready_t *ready)
{
    if (!core->self_is_voter || core->role != TR_RAFT_FOLLOWER ||
        request->term != core->term ||
        core->leader_id != request->from || core->term == UINT64_MAX) {
        return;
    }
    core->campaign_term = core->term + 1U;
    core->election_elapsed_ticks = 0U;
    tr_start_election(core, ready);
}

static void tr_step_read_index_request(tr_raft_core_t *core,
                                       const tr_raft_message_t *request,
                                       tr_raft_ready_t *ready)
{
    if (!core->self_is_voter || request->context_id == 0U ||
        core->role != TR_RAFT_FOLLOWER ||
        request->term != core->term || core->leader_id != request->from) {
        return;
    }
    tr_emit(core, ready, TR_RAFT_MSG_READ_INDEX_RESPONSE, request->from,
            false);
    ready->messages[ready->message_count - 1U].context_id =
        request->context_id;
}

static void tr_step_read_index_response(tr_raft_core_t *core,
                                        const tr_raft_message_t *response,
                                        tr_raft_ready_t *ready,
                                        int voter_index)
{
    uint32_t bit = UINT32_C(1) << (uint32_t) voter_index;

    if (response->term > core->term) {
        tr_become_follower(core, response->term);
        return;
    }
    if (core->role != TR_RAFT_LEADER || response->term != core->term ||
        response->context_id == 0U ||
        response->context_id != core->pending_read_context_id ||
        (core->pending_read_acks & bit) != 0U) {
        return;
    }
    core->pending_read_acks |= bit;
    if (tr_core_has_quorum(core, core->pending_read_acks)) {
        ready->read_state_ready = true;
        ready->read_state.context_id = core->pending_read_context_id;
        ready->read_state.index = core->pending_read_index;
        core->pending_read_context_id = 0U;
        core->pending_read_index = 0U;
        core->pending_read_acks = 0U;
    }
}

static bool tr_has_committed_current_term(const tr_raft_core_t *core)
{
    const tr_raft_log_entry_t *entry;

    if (core->commit_index == 0U) {
        return false;
    }
    if (core->commit_index == core->log.base_index) {
        return core->log.base_term == core->term;
    }
    entry = tr_raft_log_get(&core->log, core->commit_index);
    return entry != NULL && entry->term == core->term;
}

int tr_raft_core_create(const tr_raft_core_config_t *config,
                        tr_raft_core_t **out_core)
{
    tr_raft_core_t *core;
    size_t index;
    size_t self_count = 0U;
    size_t max_log_entries;
    bool initial_vote_is_voter;
    int result;
    tr_raft_log_reconcile_result_t reconcile;
    tr_raft_index_t initial_commit;
    tr_raft_index_t initial_applied;

    if (config == NULL || out_core == NULL) {
        return SALTS_EINVAL;
    }
    *out_core = NULL;
    if (config->self_id == 0U || config->voters == NULL ||
        config->voter_count == 0U ||
        config->voter_count > TR_RAFT_MAX_VOTERS ||
        config->learner_count > TR_RAFT_MAX_MEMBERS ||
        config->voter_count + config->learner_count > TR_RAFT_MAX_MEMBERS ||
        (config->learner_count != 0U && config->learners == NULL) ||
        (config->learner_count == 0U && config->learners != NULL) ||
        config->heartbeat_ticks == 0U ||
        config->election_min_ticks <= config->heartbeat_ticks ||
        config->election_min_ticks > config->election_max_ticks ||
        config->initial_election_timeout_ticks < config->election_min_ticks ||
        config->initial_election_timeout_ticks > config->election_max_ticks ||
        config->max_inflight_append_requests >
            TR_RAFT_MAX_INFLIGHT_APPEND_REQUESTS) {
        return SALTS_EINVAL;
    }
    for (index = 0; index < config->voter_count; ++index) {
        if (config->voters[index] == 0U ||
            (index != 0U && config->voters[index - 1U] >=
                              config->voters[index])) {
            return SALTS_EINVAL;
        }
        self_count += config->voters[index] == config->self_id;
    }
    for (index = 0U; index < config->learner_count; ++index) {
        size_t voter_index;

        if (config->learners[index] == 0U ||
            (index != 0U && config->learners[index - 1U] >=
                              config->learners[index])) {
            return SALTS_EINVAL;
        }
        self_count += config->learners[index] == config->self_id;
        for (voter_index = 0U; voter_index < config->voter_count;
             ++voter_index) {
            if (config->learners[index] == config->voters[voter_index]) {
                return SALTS_EINVAL;
            }
        }
    }
    initial_vote_is_voter = config->initial_vote == 0U;
    if (config->initial_configuration != NULL) {
        const tr_raft_conf_t *configuration =
            config->initial_configuration;

        if (tr_raft_conf_validate(configuration) != SALTS_OK) {
            return SALTS_EINVAL;
        }
        self_count = 0U;
        for (index = 0U; index < configuration->member_count; ++index) {
            const tr_raft_conf_member_t *member =
                &configuration->members[index];
            bool member_is_voter =
                (member->roles & (TR_RAFT_CONF_OLD_VOTER |
                                  TR_RAFT_CONF_NEW_VOTER)) != 0U;

            self_count += member->node_id == config->self_id;
            initial_vote_is_voter = initial_vote_is_voter ||
                (member->node_id == config->initial_vote && member_is_voter);
        }
    } else {
        for (index = 0U; index < config->voter_count; ++index) {
            initial_vote_is_voter = initial_vote_is_voter ||
                                    config->voters[index] ==
                                        config->initial_vote;
        }
    }
    if (self_count != 1U) {
        return SALTS_EINVAL;
    }
    if (!initial_vote_is_voter) {
        return SALTS_EINVAL;
    }
    if (config->initial_vote != 0U) {
        bool self_is_voter = false;

        if (config->initial_configuration != NULL) {
            const tr_raft_conf_t *configuration =
                config->initial_configuration;
            for (index = 0U; index < configuration->member_count; ++index) {
                const tr_raft_conf_member_t *member =
                    &configuration->members[index];
                self_is_voter = self_is_voter ||
                    (member->node_id == config->self_id &&
                     (member->roles & (TR_RAFT_CONF_OLD_VOTER |
                                       TR_RAFT_CONF_NEW_VOTER)) != 0U);
            }
        } else {
            for (index = 0U; index < config->voter_count; ++index) {
                self_is_voter = self_is_voter ||
                                config->voters[index] == config->self_id;
            }
        }
        if (!self_is_voter) {
            return SALTS_EINVAL;
        }
    }

    core = (tr_raft_core_t *) calloc(1U, sizeof(*core));
    if (core == NULL) {
        return SALTS_ENOMEM;
    }
    core->self_id = config->self_id;
    if (config->initial_configuration != NULL) {
        result = tr_raft_membership_transition_init_configuration(
            &core->membership_transition, config->initial_configuration);
    } else {
        result = tr_raft_membership_transition_init(
            &core->membership_transition, config->voters,
            config->voter_count, config->learners, config->learner_count);
    }
    if (result != SALTS_OK) {
        free(core);
        return result;
    }
    {
        const tr_raft_membership_t *memberships[] = {
            tr_core_membership(core)};

        result = tr_raft_peer_set_build(memberships, 1U, &core->peers);
        if (result != SALTS_OK) {
            free(core);
            return result;
        }
    }
    core->self_is_voter = tr_voter_index(core, config->self_id) >= 0;
    core->role = TR_RAFT_FOLLOWER;
    core->term = config->initial_term;
    core->voted_for = config->initial_vote;
    max_log_entries = config->max_log_entries == 0U
                          ? TR_RAFT_DEFAULT_MAX_LOG_ENTRIES
                          : config->max_log_entries;
    result = tr_raft_log_init(&core->log,
                              max_log_entries,
                              config->initial_last_log_index,
                              config->initial_last_log_term);
    if (result != SALTS_OK) {
        free(core);
        return result;
    }
    if (config->initial_log_entry_count != 0U) {
        result = tr_raft_log_reconcile(
            &core->log,
            config->initial_last_log_index,
            config->initial_last_log_term,
            config->initial_log_entries,
            config->initial_log_entry_count,
            0U,
            &reconcile);
        if (result != SALTS_OK || !reconcile.matched) {
            tr_raft_log_destroy(&core->log);
            free(core);
            return result != SALTS_OK ? result : SALTS_EPROTO;
        }
    } else if (config->initial_log_entries != NULL) {
        tr_raft_log_destroy(&core->log);
        free(core);
        return SALTS_EINVAL;
    }

    initial_commit = config->initial_commit_index;
    initial_applied = config->initial_applied_index;
    if (config->initial_last_log_index != 0U) {
        if (initial_commit == 0U) {
            initial_commit = config->initial_last_log_index;
        }
        if (initial_applied == 0U) {
            initial_applied = config->initial_last_log_index;
        }
    }
    if (initial_applied < config->initial_last_log_index ||
        initial_applied > initial_commit ||
        initial_commit > tr_raft_log_last_index(&core->log)) {
        tr_raft_log_destroy(&core->log);
        free(core);
        return SALTS_EINVAL;
    }
    core->commit_index = initial_commit;
    core->applied_index = initial_applied;
    core->dispatched_apply_index = initial_applied;
    result = tr_core_replay_membership_log(core);
    if (result != SALTS_OK) {
        tr_raft_log_destroy(&core->log);
        free(core);
        return result;
    }
    core->heartbeat_ticks = config->heartbeat_ticks;
    core->election_min_ticks = config->election_min_ticks;
    core->election_max_ticks = config->election_max_ticks;
    core->election_timeout_ticks = config->initial_election_timeout_ticks;
    core->max_inflight_append_requests =
        config->max_inflight_append_requests == 0U
            ? TR_RAFT_DEFAULT_MAX_INFLIGHT_APPEND_REQUESTS
            : config->max_inflight_append_requests;
    for (index = 0U; index < TR_RAFT_MAX_VOTERS; ++index) {
        core->append_windows[index].probe = true;
    }
    *out_core = core;
    return SALTS_OK;
}

void tr_raft_core_destroy(tr_raft_core_t *core)
{
    if (core != NULL) {
        tr_raft_log_destroy(&core->log);
    }
    free(core);
}

int tr_raft_core_tick(tr_raft_core_t *core,
                      const tr_raft_tick_t *tick,
                      tr_raft_ready_t *ready)
{
    tr_raft_before_t before;
    size_t peer_count;
    size_t index;
    int result;

    if (tick == NULL || tick->elapsed_ticks == 0U) {
        return SALTS_EINVAL;
    }
    result = tr_begin(core, ready, &before);
    if (result != SALTS_OK) {
        return result;
    }
    if (!tr_timeout_valid(core, tick->next_election_timeout_ticks)) {
        core->in_call = false;
        return SALTS_EINVAL;
    }

    if (core->role == TR_RAFT_LEADER &&
        tr_raft_membership_transition_needs_final(
            &core->membership_transition)) {
        tr_raft_membership_t final_membership;

        result = tr_raft_membership_transition_final(
            &core->membership_transition, &final_membership);
        if (result == SALTS_OK) {
            result = tr_core_append_configuration(
                core, &final_membership, ready);
        }
        if (result != SALTS_OK) {
            core->in_call = false;
            tr_reset_ready(ready);
            return result;
        }
        return tr_finish(core, ready, &before);
    }

    peer_count = tr_peer_count(core) - 1U;
    if (core->role == TR_RAFT_LEADER) {
        if (UINT32_MAX - core->check_quorum_elapsed_ticks <
                tick->elapsed_ticks ||
            core->check_quorum_elapsed_ticks + tick->elapsed_ticks >=
                core->election_timeout_ticks) {
            int self_index = tr_voter_index(core, core->self_id);

            if (!tr_core_has_quorum(core, core->recent_active)) {
                tr_become_follower(core, core->term);
                return tr_finish(core, ready, &before);
            }
            core->check_quorum_elapsed_ticks = 0U;
            core->recent_active =
                UINT32_C(1) << (uint32_t) self_index;
        } else {
            core->check_quorum_elapsed_ticks += tick->elapsed_ticks;
        }
        if (core->leadership_transfer_target != 0U) {
            if (UINT32_MAX - core->leadership_transfer_elapsed_ticks <
                    tick->elapsed_ticks ||
                core->leadership_transfer_elapsed_ticks +
                        tick->elapsed_ticks >=
                    core->election_timeout_ticks) {
                core->leadership_transfer_target = 0U;
                core->leadership_transfer_elapsed_ticks = 0U;
                core->leadership_transfer_sent = false;
            } else {
                core->leadership_transfer_elapsed_ticks += tick->elapsed_ticks;
            }
        }
        for (index = 0U; index < tr_peer_count(core); ++index) {
            tr_raft_append_window_t *window = &core->append_windows[index];
            bool timed_out = false;
            size_t inflight_index;

            if (window->count == 0U) {
                continue;
            }
            for (inflight_index = 0U;
                 inflight_index < window->count; ++inflight_index) {
                tr_raft_append_inflight_t *inflight =
                    tr_append_window_at(window, inflight_index);

                if (UINT32_MAX - inflight->elapsed_ticks <
                        tick->elapsed_ticks ||
                    inflight->elapsed_ticks + tick->elapsed_ticks >=
                        core->heartbeat_ticks) {
                    timed_out = true;
                    break;
                }
                inflight->elapsed_ticks += tick->elapsed_ticks;
            }
            if (timed_out) {
                core->next_index[index] = core->match_index[index] + 1U;
                tr_append_window_reset(window, true);
            }
        }
        if (UINT32_MAX - core->heartbeat_elapsed_ticks < tick->elapsed_ticks ||
            core->heartbeat_elapsed_ticks + tick->elapsed_ticks >=
                core->heartbeat_ticks) {
            result = tr_require_capacity(core, ready, peer_count);
            if (result != SALTS_OK) {
                return result;
            }
            core->heartbeat_elapsed_ticks = 0U;
            tr_broadcast_replication(core, ready);
        } else {
            core->heartbeat_elapsed_ticks += tick->elapsed_ticks;
        }
        return tr_finish(core, ready, &before);
    }

    if (!core->self_is_voter) {
        if (UINT32_MAX - core->election_elapsed_ticks < tick->elapsed_ticks ||
            core->election_elapsed_ticks + tick->elapsed_ticks >=
                core->election_timeout_ticks) {
            core->election_elapsed_ticks = 0U;
            core->election_timeout_ticks = tick->next_election_timeout_ticks;
        } else {
            core->election_elapsed_ticks += tick->elapsed_ticks;
        }
        return tr_finish(core, ready, &before);
    }

    if (UINT32_MAX - core->election_elapsed_ticks < tick->elapsed_ticks ||
        core->election_elapsed_ticks + tick->elapsed_ticks >=
            core->election_timeout_ticks) {
        if (core->term == UINT64_MAX) {
            core->in_call = false;
            return SALTS_EPROTO;
        }
        result = tr_require_capacity(core, ready, peer_count);
        if (result != SALTS_OK) {
            return result;
        }
        core->election_elapsed_ticks = 0U;
        core->election_timeout_ticks = tick->next_election_timeout_ticks;
        tr_start_pre_vote(core, ready);
    } else {
        core->election_elapsed_ticks += tick->elapsed_ticks;
    }
    return tr_finish(core, ready, &before);
}

int tr_raft_core_step(tr_raft_core_t *core,
                      const tr_raft_message_t *message,
                      tr_raft_ready_t *ready)
{
    tr_raft_before_t before;
    size_t required = 0U;
    int peer_index;
    int voter_index;
    int result;

    if (message == NULL || core == NULL || message->to != core->self_id ||
        message->from == core->self_id) {
        return SALTS_EINVAL;
    }
    peer_index = tr_peer_index(core, message->from);
    if (peer_index < 0) {
        return SALTS_EINVAL;
    }
    voter_index = tr_voter_index(core, message->from);
    if (voter_index < 0 &&
        message->type != TR_RAFT_MSG_HEARTBEAT_RESPONSE &&
        message->type != TR_RAFT_MSG_APPEND_RESPONSE) {
        return SALTS_EINVAL;
    }
    result = tr_begin(core, ready, &before);
    if (result != SALTS_OK) {
        return result;
    }

    switch (message->type) {
    case TR_RAFT_MSG_PRE_VOTE_REQUEST:
    case TR_RAFT_MSG_VOTE_REQUEST:
    case TR_RAFT_MSG_HEARTBEAT_REQUEST:
    case TR_RAFT_MSG_APPEND_REQUEST:
        required = 1U;
        break;
    case TR_RAFT_MSG_PRE_VOTE_RESPONSE:
        if (tr_will_pre_vote_win(core, message, voter_index)) {
            required = tr_peer_count(core) - 1U;
        }
        break;
    case TR_RAFT_MSG_VOTE_RESPONSE:
        if (tr_will_vote_win(core, message, voter_index)) {
            required = tr_peer_count(core) - 1U;
        }
        break;
    case TR_RAFT_MSG_HEARTBEAT_RESPONSE:
    case TR_RAFT_MSG_APPEND_RESPONSE:
        if (core->role == TR_RAFT_LEADER && message->term == core->term &&
            message->granted && !core->leadership_transfer_sent &&
            core->leadership_transfer_target == message->from &&
            message->match_index == tr_raft_log_last_index(&core->log)) {
            required = 1U;
        }
        break;
    case TR_RAFT_MSG_TIMEOUT_NOW:
        if (core->self_is_voter && core->role == TR_RAFT_FOLLOWER &&
            message->term == core->term &&
            core->leader_id == message->from && core->term != UINT64_MAX) {
            required = tr_core_voter_count(core) - 1U;
        }
        break;
    case TR_RAFT_MSG_READ_INDEX_REQUEST:
        if (core->self_is_voter && message->context_id != 0U &&
            core->role == TR_RAFT_FOLLOWER && message->term == core->term &&
            core->leader_id == message->from) {
            required = 1U;
        }
        break;
    case TR_RAFT_MSG_READ_INDEX_RESPONSE:
        break;
    default:
        core->in_call = false;
        return SALTS_EINVAL;
    }
    result = tr_require_capacity(core, ready, required);
    if (result != SALTS_OK) {
        return result;
    }

    switch (message->type) {
    case TR_RAFT_MSG_PRE_VOTE_REQUEST:
        tr_step_pre_vote_request(core, message, ready);
        break;
    case TR_RAFT_MSG_PRE_VOTE_RESPONSE:
        tr_step_pre_vote_response(core, message, ready, voter_index);
        break;
    case TR_RAFT_MSG_VOTE_REQUEST:
        tr_step_vote_request(core, message, ready);
        break;
    case TR_RAFT_MSG_VOTE_RESPONSE:
        tr_step_vote_response(core, message, ready, voter_index);
        break;
    case TR_RAFT_MSG_HEARTBEAT_REQUEST:
    case TR_RAFT_MSG_APPEND_REQUEST:
        if (message->entry_count > TR_RAFT_MAX_APPEND_ENTRIES ||
            (message->type == TR_RAFT_MSG_HEARTBEAT_REQUEST &&
             message->entry_count != 0U)) {
            core->in_call = false;
            return SALTS_EINVAL;
        }
        result = tr_step_heartbeat_request(core, message, ready);
        if (result != SALTS_OK) {
            core->in_call = false;
            tr_reset_ready(ready);
            return result;
        }
        break;
    case TR_RAFT_MSG_HEARTBEAT_RESPONSE:
    case TR_RAFT_MSG_APPEND_RESPONSE:
        result = tr_step_heartbeat_response(core, message, ready, peer_index);
        if (result != SALTS_OK) {
            core->in_call = false;
            tr_reset_ready(ready);
            return result;
        }
        break;
    case TR_RAFT_MSG_TIMEOUT_NOW:
        tr_step_timeout_now(core, message, ready);
        break;
    case TR_RAFT_MSG_READ_INDEX_REQUEST:
        tr_step_read_index_request(core, message, ready);
        break;
    case TR_RAFT_MSG_READ_INDEX_RESPONSE:
        tr_step_read_index_response(core, message, ready, voter_index);
        break;
    default:
        core->in_call = false;
        return SALTS_EINVAL;
    }
    return tr_finish(core, ready, &before);
}

int tr_raft_core_propose(tr_raft_core_t *core,
                         const tr_raft_proposal_t *proposal,
                         tr_raft_ready_t *ready)
{
    tr_raft_before_t before;
    const tr_raft_log_entry_t *entry = NULL;
    size_t peer_count;
    int self_index;
    int result;

    if (core == NULL || proposal == NULL || proposal->command_id == 0U ||
        proposal->data_length > TR_RAFT_MAX_ENTRY_BYTES ||
        (proposal->data_length != 0U && proposal->data == NULL)) {
        return SALTS_EINVAL;
    }
    result = tr_begin(core, ready, &before);
    if (result != SALTS_OK) {
        return result;
    }
    if (core->role != TR_RAFT_LEADER) {
        core->in_call = false;
        return SALTS_EPROTO;
    }
    if (core->leadership_transfer_target != 0U) {
        core->in_call = false;
        return SALTS_EBUSY;
    }
    peer_count = tr_peer_count(core) - 1U;
    result = tr_require_capacity(core, ready, peer_count);
    if (result != SALTS_OK) {
        return result;
    }
    result = tr_raft_log_append_local(&core->log,
                                      core->term,
                                      proposal->command_id,
                                      proposal->data,
                                      proposal->data_length,
                                      &entry);
    if (result != SALTS_OK) {
        core->in_call = false;
        tr_reset_ready(ready);
        return result;
    }

    self_index = tr_voter_index(core, core->self_id);
    core->match_index[self_index] = entry->index;
    core->next_index[self_index] = entry->index + 1U;
    ready->log_changed = true;
    ready->log_entries = (const tr_raft_entry_t *) entry;
    ready->log_entry_count = 1U;
    result = tr_update_commit(core, ready);
    if (result != SALTS_OK) {
        core->in_call = false;
        tr_reset_ready(ready);
        return result;
    }
    tr_broadcast_replication(core, ready);
    return tr_finish(core, ready, &before);
}

int tr_raft_core_change_membership(
    tr_raft_core_t *core,
    const tr_raft_membership_change_t *change,
    tr_raft_ready_t *ready)
{
    tr_raft_before_t before;
    tr_raft_membership_t joint;
    int result;

    if (core == NULL || change == NULL || change->transition_id == 0U) {
        return SALTS_EINVAL;
    }
    result = tr_begin(core, ready, &before);
    if (result != SALTS_OK) {
        return result;
    }
    if (core->role != TR_RAFT_LEADER) {
        core->in_call = false;
        return SALTS_EPERM;
    }
    if (core->leadership_transfer_target != 0U ||
        core->pending_read_context_id != 0U) {
        core->in_call = false;
        return SALTS_EBUSY;
    }
    result = tr_raft_membership_transition_propose(
        &core->membership_transition, change->voters, change->voter_count,
        change->learners, change->learner_count, change->transition_id,
        &joint);
    if (result == SALTS_OK) {
        result = tr_core_append_configuration(core, &joint, ready);
    }
    if (result != SALTS_OK) {
        core->in_call = false;
        tr_reset_ready(ready);
        return result;
    }
    return tr_finish(core, ready, &before);
}

int tr_raft_core_transfer_leadership(tr_raft_core_t *core,
                                     tr_raft_node_id_t transferee_id,
                                     tr_raft_ready_t *ready)
{
    tr_raft_before_t before;
    tr_raft_index_t last_index;
    int transferee_index;
    int result;

    if (core == NULL || transferee_id == 0U ||
        transferee_id == core->self_id) {
        return SALTS_EINVAL;
    }
    transferee_index = tr_voter_index(core, transferee_id);
    if (transferee_index < 0) {
        return SALTS_EINVAL;
    }
    result = tr_begin(core, ready, &before);
    if (result != SALTS_OK) {
        return result;
    }
    if (core->role != TR_RAFT_LEADER) {
        core->in_call = false;
        return SALTS_EPROTO;
    }
    if (core->leadership_transfer_target != 0U) {
        core->in_call = false;
        return SALTS_EBUSY;
    }
    result = tr_require_capacity(core, ready, 1U);
    if (result != SALTS_OK) {
        return result;
    }

    core->leadership_transfer_target = transferee_id;
    core->leadership_transfer_elapsed_ticks = 0U;
    last_index = tr_raft_log_last_index(&core->log);
    if (core->match_index[transferee_index] >= last_index) {
        tr_emit(core, ready, TR_RAFT_MSG_TIMEOUT_NOW, transferee_id, false);
        core->leadership_transfer_sent = true;
    } else {
        (void) tr_emit_replication(core, ready,
                                   (size_t) transferee_index, false);
        core->leadership_transfer_sent = false;
    }
    return tr_finish(core, ready, &before);
}

int tr_raft_core_read_index(tr_raft_core_t *core,
                            uint64_t context_id,
                            tr_raft_ready_t *ready)
{
    tr_raft_before_t before;
    int self_index;
    int result;

    if (core == NULL || context_id == 0U) {
        return SALTS_EINVAL;
    }
    result = tr_begin(core, ready, &before);
    if (result != SALTS_OK) {
        return result;
    }
    if (core->role != TR_RAFT_LEADER) {
        core->in_call = false;
        return SALTS_EPROTO;
    }
    if (core->leadership_transfer_target != 0U ||
        core->pending_read_context_id != 0U ||
        !tr_has_committed_current_term(core)) {
        core->in_call = false;
        return SALTS_EBUSY;
    }
    result = tr_require_capacity(core, ready,
                                 tr_core_voter_count(core) - 1U);
    if (result != SALTS_OK) {
        return result;
    }

    self_index = tr_voter_index(core, core->self_id);
    core->pending_read_context_id = context_id;
    core->pending_read_index = core->commit_index;
    core->pending_read_acks = UINT32_C(1) << (uint32_t) self_index;
    if (tr_core_has_quorum(core, core->pending_read_acks)) {
        ready->read_state_ready = true;
        ready->read_state.context_id = context_id;
        ready->read_state.index = core->pending_read_index;
        core->pending_read_context_id = 0U;
        core->pending_read_index = 0U;
        core->pending_read_acks = 0U;
    } else {
        tr_broadcast_read_index(core, ready, context_id);
    }
    return tr_finish(core, ready, &before);
}

int tr_raft_core_poll(tr_raft_core_t *core, tr_raft_ready_t *ready)
{
    tr_raft_before_t before;
    int result = tr_begin(core, ready, &before);

    if (result != SALTS_OK) {
        return result;
    }
    tr_fill_replication_windows(core, ready);
    return tr_finish(core, ready, &before);
}

int tr_raft_core_acknowledge_applied_entry(tr_raft_core_t *core,
                                           tr_raft_index_t index)
{
    if (core == NULL) {
        return SALTS_EINVAL;
    }
    if (core->in_call || !core->split_apply_mode ||
        core->applied_index == UINT64_MAX ||
        index != core->applied_index + 1U ||
        index > core->dispatched_apply_index) {
        return SALTS_EPROTO;
    }
    core->applied_index = index;
    return SALTS_OK;
}

int tr_raft_core_acknowledge_ready(tr_raft_core_t *core)
{
    if (core == NULL) {
        return SALTS_EINVAL;
    }
    if (core->in_call || !core->ready_outstanding) {
        return SALTS_EPROTO;
    }
    core->split_apply_mode = true;
    if (core->pending_apply_index != 0U) {
        core->dispatched_apply_index = core->pending_apply_index;
        core->pending_apply_index = 0U;
    }
    core->ready_outstanding = false;
    return SALTS_OK;
}

int tr_raft_core_advance(tr_raft_core_t *core)
{
    if (core == NULL) {
        return SALTS_EINVAL;
    }
    if (core->in_call || !core->ready_outstanding ||
        core->split_apply_mode) {
        return SALTS_EPROTO;
    }
    if (core->pending_apply_index != 0U) {
        core->applied_index = core->pending_apply_index;
        core->dispatched_apply_index = core->pending_apply_index;
        core->pending_apply_index = 0U;
    }
    core->ready_outstanding = false;
    return SALTS_OK;
}

int tr_raft_core_snapshot_point(const tr_raft_core_t *core,
                                tr_raft_snapshot_point_t *out_point)
{
    const tr_raft_log_entry_t *entry;

    if (core == NULL || out_point == NULL) {
        return SALTS_EINVAL;
    }
    if (core->in_call || core->ready_outstanding ||
        core->applied_index != core->commit_index) {
        return SALTS_EBUSY;
    }
    if (core->applied_index <= core->log.base_index) {
        return SALTS_ENOENT;
    }
    entry = tr_raft_log_get(&core->log, core->applied_index);
    if (entry == NULL) {
        return SALTS_EPROTO;
    }

    memset(out_point, 0, sizeof(*out_point));
    out_point->index = core->applied_index;
    out_point->term = entry->term;
    out_point->configuration = *tr_core_membership(core);
    return SALTS_OK;
}

static bool tr_snapshot_configuration_equal(const tr_raft_conf_t *left,
                                            const tr_raft_conf_t *right)
{
    size_t index;

    if (left->phase != right->phase ||
        left->transition_id != right->transition_id ||
        left->member_count != right->member_count) {
        return false;
    }
    for (index = 0U; index < left->member_count; ++index) {
        if (left->members[index].node_id != right->members[index].node_id ||
            left->members[index].roles != right->members[index].roles) {
            return false;
        }
    }
    return true;
}

int tr_raft_core_compact(tr_raft_core_t *core,
                         const tr_raft_snapshot_point_t *point)
{
    tr_raft_snapshot_point_t current;
    int result;

    if (core == NULL || point == NULL) {
        return SALTS_EINVAL;
    }
    result = tr_raft_core_snapshot_point(core, &current);
    if (result != SALTS_OK) {
        return result;
    }
    if (point->index != current.index || point->term != current.term ||
        !tr_snapshot_configuration_equal(&point->configuration,
                                         &current.configuration)) {
        return SALTS_EPROTO;
    }
    return tr_raft_log_compact(&core->log, point->index, point->term);
}

int tr_raft_core_snapshot_completed(tr_raft_core_t *core,
                                    tr_raft_node_id_t peer_id,
                                    tr_raft_index_t snapshot_index,
                                    tr_raft_ready_t *ready)
{
    tr_raft_before_t before;
    int peer_index;
    int result;

    if (core == NULL || ready == NULL || peer_id == 0U ||
        peer_id == core->self_id || snapshot_index == 0U) {
        return SALTS_EINVAL;
    }
    peer_index = tr_peer_index(core, peer_id);
    if (peer_index < 0 || snapshot_index > core->log.base_index) {
        return SALTS_EINVAL;
    }
    result = tr_begin(core, ready, &before);
    if (result != SALTS_OK) {
        return result;
    }
    if (core->role != TR_RAFT_LEADER) {
        core->in_call = false;
        return SALTS_EBUSY;
    }
    result = tr_require_capacity(core, ready, 1U);
    if (result != SALTS_OK) {
        return result;
    }

    if (snapshot_index > core->match_index[peer_index]) {
        core->match_index[peer_index] = snapshot_index;
    }
    core->next_index[peer_index] = snapshot_index + 1U;
    tr_append_window_reset(&core->append_windows[peer_index], true);
    tr_fill_peer_replication_window(core, ready, (size_t) peer_index);
    return tr_finish(core, ready, &before);
}

int tr_raft_core_status(const tr_raft_core_t *core, tr_raft_status_t *status)
{
    size_t index;

    if (core == NULL || status == NULL) {
        return SALTS_EINVAL;
    }
    status->self_id = core->self_id;
    status->leader_id = core->leader_id;
    status->role = core->role;
    status->term = core->term;
    status->voted_for = core->voted_for;
    status->last_log_index = tr_raft_log_last_index(&core->log);
    status->last_log_term = tr_raft_log_last_term(&core->log);
    status->log_base_index = core->log.base_index;
    status->log_base_term = core->log.base_term;
    status->log_entry_count = tr_raft_log_count(&core->log);
    status->commit_index = core->commit_index;
    status->applied_index = core->applied_index;
    status->election_elapsed_ticks = core->election_elapsed_ticks;
    status->election_timeout_ticks = core->election_timeout_ticks;
    status->heartbeat_elapsed_ticks = core->heartbeat_elapsed_ticks;
    status->ready_outstanding = core->ready_outstanding;
    status->leadership_transfer_target = core->leadership_transfer_target;
    status->leadership_transfer_elapsed_ticks =
        core->leadership_transfer_elapsed_ticks;
    status->pending_read_context_id = core->pending_read_context_id;
    status->inflight_append_count = 0U;
    for (index = 0U; index < tr_peer_count(core); ++index) {
        status->inflight_append_count += core->append_windows[index].count;
    }
    status->self_is_voter = core->self_is_voter;
    status->voter_count = tr_core_voter_count(core);
    status->learner_count = tr_core_learner_count(core);
    status->joint_configuration =
        tr_core_membership(core)->phase == TR_RAFT_CONF_JOINT;
    status->membership_transition_id =
        tr_core_membership(core)->transition_id;
    status->pending_configuration_count =
        core->membership_transition.pending_count;
    status->peer_count = tr_peer_count(core);
    status->snapshot_required_peer_count = 0U;
    if (core->role == TR_RAFT_LEADER) {
        for (index = 0U; index < tr_peer_count(core); ++index) {
            status->snapshot_required_peer_count +=
                tr_peer_id(core, index) != core->self_id &&
                core->next_index[index] <= core->log.base_index;
        }
    }
    return SALTS_OK;
}

int tr_raft_core_configuration(const tr_raft_core_t *core,
                               tr_raft_conf_t *out_configuration)
{
    if (core == NULL || out_configuration == NULL) {
        return SALTS_EINVAL;
    }
    *out_configuration = *tr_core_membership(core);
    return SALTS_OK;
}

int tr_raft_core_progress(const tr_raft_core_t *core,
                          tr_raft_progress_view_t *out_progress)
{
    size_t index;

    if (core == NULL || out_progress == NULL) {
        return SALTS_EINVAL;
    }
    memset(out_progress, 0, sizeof(*out_progress));
    out_progress->peer_count = tr_peer_count(core);
    for (index = 0U; index < out_progress->peer_count; ++index) {
        tr_raft_peer_progress_t *peer = &out_progress->peers[index];

        peer->node_id = tr_peer_id(core, index);
        peer->match_index = core->match_index[index];
        peer->next_index = core->next_index[index];
        peer->append_inflight_elapsed_ticks =
            tr_append_window_oldest_elapsed(&core->append_windows[index]);
        peer->inflight_append_count = core->append_windows[index].count;
        peer->max_inflight_append_requests =
            core->max_inflight_append_requests;
        peer->recent_active = peer->node_id == core->self_id ||
            (core->recent_active & (UINT32_C(1) << (uint32_t) index)) != 0U;
        peer->append_inflight = core->append_windows[index].count != 0U;
        peer->append_probe = core->append_windows[index].probe;
        peer->snapshot_required = core->role == TR_RAFT_LEADER &&
            peer->node_id != core->self_id &&
            peer->next_index <= core->log.base_index;
    }
    return SALTS_OK;
}

int tr_raft_core_operation_status(const tr_raft_core_t *core,
                                  tr_raft_term_t term,
                                  tr_raft_index_t index,
                                  tr_raft_operation_status_t *out_status)
{
    const tr_raft_entry_t *entry;
    tr_raft_index_t last_index;

    if (core == NULL || out_status == NULL || term == 0U || index == 0U) {
        return SALTS_EINVAL;
    }

    memset(out_status, 0, sizeof(*out_status));
    out_status->term = term;
    out_status->index = index;
    out_status->commit_index = core->commit_index;
    out_status->applied_index = core->applied_index;
    last_index = tr_raft_log_last_index(&core->log);

    if (index < core->log.base_index) {
        out_status->state = TR_RAFT_OPERATION_EXPIRED;
        return SALTS_OK;
    }
    if (index == core->log.base_index) {
        out_status->state = term == core->log.base_term
                                ? TR_RAFT_OPERATION_APPLIED
                                : TR_RAFT_OPERATION_LOST;
        return SALTS_OK;
    }
    if (index > last_index) {
        if (term > core->term) {
            return SALTS_EINVAL;
        }
        out_status->state = TR_RAFT_OPERATION_LOST;
        return SALTS_OK;
    }

    entry = tr_raft_log_get(&core->log, index);
    if (entry == NULL) {
        return SALTS_EPROTO;
    }
    if (entry->term != term) {
        out_status->state = TR_RAFT_OPERATION_LOST;
    } else if (core->applied_index >= index) {
        out_status->state = TR_RAFT_OPERATION_APPLIED;
    } else if (core->commit_index >= index) {
        out_status->state = TR_RAFT_OPERATION_COMMITTED;
    } else {
        out_status->state = TR_RAFT_OPERATION_PENDING;
    }
    return SALTS_OK;
}
