#include "raft_membership_transition.h"

#include "raft_peer_set.h"

#include <salts_error.h>

#include <string.h>

static bool tr_raft_membership_equal(const tr_raft_membership_t *left,
                                     const tr_raft_membership_t *right)
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

static bool tr_raft_joint_old_voters_match(
    const tr_raft_membership_t *stable,
    const tr_raft_membership_t *joint)
{
    size_t stable_index = 0U;
    size_t joint_index = 0U;

    while (stable_index < stable->member_count ||
           joint_index < joint->member_count) {
        while (stable_index < stable->member_count &&
               !tr_raft_membership_is_voter(
                   stable, stable->members[stable_index].node_id)) {
            ++stable_index;
        }
        while (joint_index < joint->member_count &&
               (joint->members[joint_index].roles &
                TR_RAFT_CONF_OLD_VOTER) == 0U) {
            ++joint_index;
        }
        if (stable_index == stable->member_count ||
            joint_index == joint->member_count) {
            return stable_index == stable->member_count &&
                   joint_index == joint->member_count;
        }
        if (stable->members[stable_index].node_id !=
            joint->members[joint_index].node_id) {
            return false;
        }
        ++stable_index;
        ++joint_index;
    }
    return true;
}

static int tr_raft_membership_transition_validate_next(
    const tr_raft_membership_transition_t *transition,
    const tr_raft_membership_t *configuration)
{
    const tr_raft_membership_t *base = &transition->committed;
    const tr_raft_membership_t *peer_memberships[
        TR_RAFT_PEER_SET_MAX_SOURCES];
    tr_raft_peer_set_t peers;
    size_t source_count = 0U;
    size_t index;

    if (tr_raft_conf_validate(configuration) != SALTS_OK) {
        return SALTS_EPROTO;
    }
    if (transition->pending_count >
        TR_RAFT_PEER_SET_MAX_SOURCES - 2U) {
        return SALTS_EPROTO;
    }
    peer_memberships[source_count++] = &transition->committed;
    for (index = 0U; index < transition->pending_count; ++index) {
        peer_memberships[source_count++] = &transition->pending[index];
    }
    peer_memberships[source_count++] = configuration;
    if (tr_raft_peer_set_build(peer_memberships, source_count, &peers) !=
        SALTS_OK) {
        return SALTS_EPROTO;
    }
    if (transition->pending_count != 0U) {
        base = &transition->pending[transition->pending_count - 1U];
    }
    if (configuration->phase == TR_RAFT_CONF_JOINT) {
        if (transition->pending_count != 0U ||
            transition->committed.phase != TR_RAFT_CONF_FINAL ||
            !tr_raft_joint_old_voters_match(&transition->committed,
                                            configuration)) {
            return SALTS_EPROTO;
        }
        return SALTS_OK;
    }
    if (configuration->phase == TR_RAFT_CONF_FINAL &&
        base->phase == TR_RAFT_CONF_JOINT) {
        tr_raft_membership_t expected;

        if (tr_raft_membership_final(base, &expected) != SALTS_OK ||
            !tr_raft_membership_equal(&expected, configuration)) {
            return SALTS_EPROTO;
        }
        return SALTS_OK;
    }
    return SALTS_EPROTO;
}

int tr_raft_membership_transition_init(
    tr_raft_membership_transition_t *transition,
    const tr_raft_node_id_t *voters,
    size_t voter_count,
    const tr_raft_node_id_t *learners,
    size_t learner_count)
{
    int result;

    if (transition == NULL) {
        return SALTS_EINVAL;
    }
    memset(transition, 0, sizeof(*transition));
    result = tr_raft_membership_stable(voters, voter_count, learners,
                                       learner_count,
                                       &transition->committed);
    if (result != SALTS_OK) {
        memset(transition, 0, sizeof(*transition));
    }
    return result;
}

int tr_raft_membership_transition_init_configuration(
    tr_raft_membership_transition_t *transition,
    const tr_raft_conf_t *configuration)
{
    if (transition == NULL || configuration == NULL ||
        tr_raft_conf_validate(configuration) != SALTS_OK) {
        return SALTS_EINVAL;
    }
    memset(transition, 0, sizeof(*transition));
    transition->committed = *configuration;
    return SALTS_OK;
}

int tr_raft_membership_transition_propose(
    const tr_raft_membership_transition_t *transition,
    const tr_raft_node_id_t *target_voters,
    size_t target_voter_count,
    const tr_raft_node_id_t *target_learners,
    size_t target_learner_count,
    uint64_t transition_id,
    tr_raft_membership_t *joint)
{
    if (transition == NULL || joint == NULL) {
        return SALTS_EINVAL;
    }
    if (transition->committed.phase != TR_RAFT_CONF_FINAL ||
        transition->pending_count != 0U) {
        return SALTS_EBUSY;
    }
    return tr_raft_membership_joint(
        &transition->committed, target_voters, target_voter_count,
        target_learners, target_learner_count, transition_id, joint);
}

int tr_raft_membership_transition_stage(
    tr_raft_membership_transition_t *transition,
    const tr_raft_membership_t *configuration,
    tr_raft_index_t log_index)
{
    size_t pending_index;
    int result;

    if (transition == NULL || configuration == NULL || log_index == 0U) {
        return SALTS_EINVAL;
    }
    if (transition->pending_count > TR_RAFT_MEMBERSHIP_MAX_PENDING) {
        return SALTS_EPROTO;
    }
    for (pending_index = 0U; pending_index < transition->pending_count;
         ++pending_index) {
        if (transition->pending_indices[pending_index] == log_index) {
            return tr_raft_membership_equal(
                       &transition->pending[pending_index], configuration)
                       ? SALTS_OK
                       : SALTS_EPROTO;
        }
    }
    if (transition->pending_count == TR_RAFT_MEMBERSHIP_MAX_PENDING ||
        (transition->pending_count != 0U &&
         transition->pending_indices[transition->pending_count - 1U] >=
             log_index)) {
        return SALTS_EPROTO;
    }
    result = tr_raft_membership_transition_validate_next(transition,
                                                         configuration);
    if (result != SALTS_OK) {
        return result;
    }
    pending_index = transition->pending_count++;
    transition->pending[pending_index] = *configuration;
    transition->pending_indices[pending_index] = log_index;
    return SALTS_OK;
}

int tr_raft_membership_transition_stage_entry(
    tr_raft_membership_transition_t *transition,
    const tr_raft_entry_t *entry)
{
    tr_raft_membership_t configuration;
    int result;

    if (transition == NULL || entry == NULL) {
        return SALTS_EINVAL;
    }
    result = tr_raft_conf_entry_decode(entry, &configuration);
    if (result != SALTS_OK) {
        return result;
    }
    return tr_raft_membership_transition_stage(
        transition, &configuration, entry->index);
}

tr_raft_index_t tr_raft_membership_transition_commit_limit(
    const tr_raft_membership_transition_t *transition,
    tr_raft_index_t candidate_index)
{
    if (transition == NULL || transition->pending_count == 0U ||
        candidate_index < transition->pending_indices[0]) {
        return candidate_index;
    }
    return transition->pending_indices[0];
}

int tr_raft_membership_transition_apply(
    tr_raft_membership_transition_t *transition,
    tr_raft_index_t commit_index)
{
    if (transition == NULL) {
        return SALTS_EINVAL;
    }
    while (transition->pending_count != 0U &&
           transition->pending_indices[0] <= commit_index) {
        transition->committed = transition->pending[0];
        if (transition->pending_count == 2U) {
            transition->pending[0] = transition->pending[1];
            transition->pending_indices[0] = transition->pending_indices[1];
        }
        memset(&transition->pending[transition->pending_count - 1U], 0,
               sizeof(transition->pending[0]));
        transition->pending_indices[transition->pending_count - 1U] = 0U;
        --transition->pending_count;
    }
    return SALTS_OK;
}

bool tr_raft_membership_transition_needs_final(
    const tr_raft_membership_transition_t *transition)
{
    return transition != NULL &&
           transition->committed.phase == TR_RAFT_CONF_JOINT &&
           transition->pending_count == 0U;
}

int tr_raft_membership_transition_final(
    const tr_raft_membership_transition_t *transition,
    tr_raft_membership_t *final_membership)
{
    if (!tr_raft_membership_transition_needs_final(transition) ||
        final_membership == NULL) {
        return SALTS_EINVAL;
    }
    return tr_raft_membership_final(&transition->committed,
                                    final_membership);
}

const tr_raft_membership_t *tr_raft_membership_transition_committed(
    const tr_raft_membership_transition_t *transition)
{
    return transition == NULL ? NULL : &transition->committed;
}

const tr_raft_membership_t *tr_raft_membership_transition_next(
    const tr_raft_membership_transition_t *transition)
{
    return transition == NULL || transition->pending_count == 0U
               ? NULL
               : &transition->pending[0];
}
