#include "raft_peer_set.h"

#include <turbo_error.h>

#include <limits.h>
#include <string.h>

static bool tr_raft_peer_set_membership_valid(
    const tr_raft_membership_t *membership)
{
    size_t index;

    if (membership == NULL || membership->member_count == 0U ||
        membership->member_count > TR_RAFT_MAX_MEMBERS) {
        return false;
    }
    for (index = 0U; index < membership->member_count; ++index) {
        if (membership->members[index].node_id == 0U ||
            (index != 0U && membership->members[index - 1U].node_id >=
                                membership->members[index].node_id)) {
            return false;
        }
    }
    return true;
}

int tr_raft_peer_set_build(
    const tr_raft_membership_t *const *memberships,
    size_t membership_count,
    tr_raft_peer_set_t *peers)
{
    size_t positions[TR_RAFT_PEER_SET_MAX_SOURCES] = {0U};
    size_t source_index;

    if (memberships == NULL || membership_count == 0U ||
        membership_count > TR_RAFT_PEER_SET_MAX_SOURCES || peers == NULL) {
        return TURBO_EINVAL;
    }
    memset(peers, 0, sizeof(*peers));
    for (source_index = 0U; source_index < membership_count;
         ++source_index) {
        if (!tr_raft_peer_set_membership_valid(memberships[source_index])) {
            return TURBO_EINVAL;
        }
    }

    for (;;) {
        tr_raft_node_id_t next = UINT64_MAX;
        bool available = false;

        for (source_index = 0U; source_index < membership_count;
             ++source_index) {
            const tr_raft_membership_t *membership =
                memberships[source_index];

            if (positions[source_index] < membership->member_count &&
                membership->members[positions[source_index]].node_id < next) {
                next = membership->members[positions[source_index]].node_id;
                available = true;
            }
        }
        if (!available) {
            return TURBO_OK;
        }
        if (peers->count == TR_RAFT_MAX_MEMBERS) {
            memset(peers, 0, sizeof(*peers));
            return TURBO_ENOSPC;
        }
        peers->node_ids[peers->count++] = next;
        for (source_index = 0U; source_index < membership_count;
             ++source_index) {
            const tr_raft_membership_t *membership =
                memberships[source_index];

            if (positions[source_index] < membership->member_count &&
                membership->members[positions[source_index]].node_id == next) {
                ++positions[source_index];
            }
        }
    }
}

int tr_raft_peer_set_index(const tr_raft_peer_set_t *peers,
                           tr_raft_node_id_t node_id)
{
    size_t index;

    if (peers == NULL || node_id == 0U) {
        return -1;
    }
    for (index = 0U; index < peers->count; ++index) {
        if (peers->node_ids[index] == node_id) {
            return (int) index;
        }
        if (peers->node_ids[index] > node_id) {
            break;
        }
    }
    return -1;
}
