#include "raft_membership.h"

#include <salts_error.h>

#include <limits.h>
#include <string.h>

static int tr_raft_membership_validate_set(
    const tr_raft_node_id_t *voters,
    size_t voter_count,
    const tr_raft_node_id_t *learners,
    size_t learner_count)
{
    size_t voter_index = 0U;
    size_t learner_index = 0U;
    size_t index;

    if (voters == NULL || voter_count == 0U ||
        voter_count > TR_RAFT_MAX_MEMBERS ||
        learner_count > TR_RAFT_MAX_MEMBERS - voter_count ||
        (learner_count != 0U && learners == NULL) ||
        (learner_count == 0U && learners != NULL)) {
        return SALTS_EINVAL;
    }
    for (index = 0U; index < voter_count; ++index) {
        if (voters[index] == 0U ||
            (index != 0U && voters[index - 1U] >= voters[index])) {
            return SALTS_EINVAL;
        }
    }
    for (index = 0U; index < learner_count; ++index) {
        if (learners[index] == 0U ||
            (index != 0U && learners[index - 1U] >= learners[index])) {
            return SALTS_EINVAL;
        }
    }
    while (voter_index < voter_count && learner_index < learner_count) {
        if (voters[voter_index] == learners[learner_index]) {
            return SALTS_EINVAL;
        }
        if (voters[voter_index] < learners[learner_index]) {
            ++voter_index;
        } else {
            ++learner_index;
        }
    }
    return SALTS_OK;
}

static bool tr_raft_membership_stable_valid(
    const tr_raft_membership_t *membership)
{
    size_t voter_count = 0U;
    size_t index;

    if (membership == NULL || membership->phase != TR_RAFT_CONF_FINAL ||
        membership->member_count == 0U ||
        membership->member_count > TR_RAFT_MAX_MEMBERS) {
        return false;
    }
    for (index = 0U; index < membership->member_count; ++index) {
        const tr_raft_conf_member_t *member = &membership->members[index];
        uint8_t voter_roles = TR_RAFT_CONF_OLD_VOTER |
                              TR_RAFT_CONF_NEW_VOTER;

        if (member->node_id == 0U ||
            (index != 0U && membership->members[index - 1U].node_id >=
                                member->node_id) ||
            (member->roles != voter_roles &&
             member->roles != TR_RAFT_CONF_LEARNER)) {
            return false;
        }
        voter_count += member->roles == voter_roles;
    }
    return voter_count != 0U;
}

static size_t tr_raft_membership_union_count(
    const tr_raft_membership_t *current,
    const tr_raft_node_id_t *target_voters,
    size_t target_voter_count,
    const tr_raft_node_id_t *target_learners,
    size_t target_learner_count)
{
    size_t current_index = 0U;
    size_t voter_index = 0U;
    size_t learner_index = 0U;
    size_t count = 0U;

    while (current_index < current->member_count ||
           voter_index < target_voter_count ||
           learner_index < target_learner_count) {
        tr_raft_node_id_t next = UINT64_MAX;

        if (current_index < current->member_count &&
            current->members[current_index].node_id < next) {
            next = current->members[current_index].node_id;
        }
        if (voter_index < target_voter_count &&
            target_voters[voter_index] < next) {
            next = target_voters[voter_index];
        }
        if (learner_index < target_learner_count &&
            target_learners[learner_index] < next) {
            next = target_learners[learner_index];
        }
        ++count;
        if (current_index < current->member_count &&
            current->members[current_index].node_id == next) {
            ++current_index;
        }
        if (voter_index < target_voter_count &&
            target_voters[voter_index] == next) {
            ++voter_index;
        }
        if (learner_index < target_learner_count &&
            target_learners[learner_index] == next) {
            ++learner_index;
        }
    }
    return count;
}

int tr_raft_membership_stable(const tr_raft_node_id_t *voters,
                              size_t voter_count,
                              const tr_raft_node_id_t *learners,
                              size_t learner_count,
                              tr_raft_membership_t *membership)
{
    size_t voter_index = 0U;
    size_t learner_index = 0U;

    if (membership == NULL ||
        tr_raft_membership_validate_set(voters, voter_count, learners,
                                        learner_count) != SALTS_OK) {
        return SALTS_EINVAL;
    }
    memset(membership, 0, sizeof(*membership));
    membership->phase = TR_RAFT_CONF_FINAL;
    while (voter_index < voter_count || learner_index < learner_count) {
        tr_raft_conf_member_t *member =
            &membership->members[membership->member_count++];

        if (learner_index == learner_count ||
            (voter_index < voter_count &&
             voters[voter_index] < learners[learner_index])) {
            member->node_id = voters[voter_index++];
            member->roles = TR_RAFT_CONF_OLD_VOTER |
                            TR_RAFT_CONF_NEW_VOTER;
        } else {
            member->node_id = learners[learner_index++];
            member->roles = TR_RAFT_CONF_LEARNER;
        }
    }
    return SALTS_OK;
}

int tr_raft_membership_joint(const tr_raft_membership_t *current,
                             const tr_raft_node_id_t *target_voters,
                             size_t target_voter_count,
                             const tr_raft_node_id_t *target_learners,
                             size_t target_learner_count,
                             uint64_t transition_id,
                             tr_raft_membership_t *joint)
{
    size_t current_index = 0U;
    size_t voter_index = 0U;
    size_t learner_index = 0U;

    if (joint == NULL || transition_id == 0U ||
        !tr_raft_membership_stable_valid(current) ||
        tr_raft_membership_validate_set(
            target_voters, target_voter_count, target_learners,
            target_learner_count) != SALTS_OK) {
        return SALTS_EINVAL;
    }
    if (tr_raft_membership_union_count(
            current, target_voters, target_voter_count, target_learners,
            target_learner_count) > TR_RAFT_MAX_MEMBERS) {
        return SALTS_ENOSPC;
    }

    memset(joint, 0, sizeof(*joint));
    joint->phase = TR_RAFT_CONF_JOINT;
    joint->transition_id = transition_id;
    while (current_index < current->member_count ||
           voter_index < target_voter_count ||
           learner_index < target_learner_count) {
        tr_raft_node_id_t next = UINT64_MAX;
        uint8_t roles = 0U;
        bool current_voter = false;

        while (current_index < current->member_count &&
               current->members[current_index].roles ==
                   TR_RAFT_CONF_LEARNER) {
            ++current_index;
        }
        if (current_index < current->member_count &&
            current->members[current_index].node_id < next) {
            next = current->members[current_index].node_id;
        }
        if (voter_index < target_voter_count &&
            target_voters[voter_index] < next) {
            next = target_voters[voter_index];
        }
        if (learner_index < target_learner_count &&
            target_learners[learner_index] < next) {
            next = target_learners[learner_index];
        }
        if (next == UINT64_MAX) {
            break;
        }
        if (current_index < current->member_count &&
            current->members[current_index].node_id == next) {
            current_voter = true;
            roles |= TR_RAFT_CONF_OLD_VOTER;
            ++current_index;
        }
        if (voter_index < target_voter_count &&
            target_voters[voter_index] == next) {
            roles |= TR_RAFT_CONF_NEW_VOTER;
            ++voter_index;
        }
        if (learner_index < target_learner_count &&
            target_learners[learner_index] == next) {
            roles |= TR_RAFT_CONF_LEARNER;
            ++learner_index;
        }
        if (current_voter || roles != 0U) {
            joint->members[joint->member_count].node_id = next;
            joint->members[joint->member_count].roles = roles;
            ++joint->member_count;
        }
    }
    if (tr_raft_conf_validate(joint) != SALTS_OK) {
        memset(joint, 0, sizeof(*joint));
        return SALTS_EINVAL;
    }
    return SALTS_OK;
}

int tr_raft_membership_final(const tr_raft_membership_t *joint,
                             tr_raft_membership_t *final_membership)
{
    size_t index;

    if (joint == NULL || final_membership == NULL ||
        joint->phase != TR_RAFT_CONF_JOINT ||
        tr_raft_conf_validate(joint) != SALTS_OK) {
        return SALTS_EINVAL;
    }
    memset(final_membership, 0, sizeof(*final_membership));
    final_membership->phase = TR_RAFT_CONF_FINAL;
    final_membership->transition_id = joint->transition_id;
    for (index = 0U; index < joint->member_count; ++index) {
        const tr_raft_conf_member_t *source = &joint->members[index];
        tr_raft_conf_member_t *target;

        if ((source->roles & (TR_RAFT_CONF_NEW_VOTER |
                              TR_RAFT_CONF_LEARNER)) == 0U) {
            continue;
        }
        target = &final_membership
                      ->members[final_membership->member_count++];
        target->node_id = source->node_id;
        target->roles = (source->roles & TR_RAFT_CONF_NEW_VOTER) != 0U
                            ? TR_RAFT_CONF_OLD_VOTER |
                                  TR_RAFT_CONF_NEW_VOTER
                            : TR_RAFT_CONF_LEARNER;
    }
    return tr_raft_conf_validate(final_membership);
}

int tr_raft_membership_index(const tr_raft_membership_t *membership,
                             tr_raft_node_id_t node_id)
{
    size_t index;

    if (membership == NULL || node_id == 0U) {
        return -1;
    }
    for (index = 0U; index < membership->member_count; ++index) {
        if (membership->members[index].node_id == node_id) {
            return (int) index;
        }
        if (membership->members[index].node_id > node_id) {
            break;
        }
    }
    return -1;
}

uint8_t tr_raft_membership_roles(const tr_raft_membership_t *membership,
                                 tr_raft_node_id_t node_id)
{
    int index = tr_raft_membership_index(membership, node_id);

    return index < 0 ? 0U : membership->members[index].roles;
}

bool tr_raft_membership_is_voter(const tr_raft_membership_t *membership,
                                 tr_raft_node_id_t node_id)
{
    uint8_t roles = tr_raft_membership_roles(membership, node_id);

    return (roles & (TR_RAFT_CONF_OLD_VOTER |
                     TR_RAFT_CONF_NEW_VOTER)) != 0U;
}

bool tr_raft_membership_has_quorum(const tr_raft_membership_t *membership,
                                   uint32_t acknowledgements)
{
    size_t old_count = 0U;
    size_t old_acks = 0U;
    size_t new_count = 0U;
    size_t new_acks = 0U;
    size_t index;

    if (membership == NULL || membership->member_count == 0U ||
        membership->member_count > TR_RAFT_MAX_MEMBERS) {
        return false;
    }
    for (index = 0U; index < membership->member_count; ++index) {
        uint8_t roles = membership->members[index].roles;
        bool acknowledged =
            (acknowledgements & (UINT32_C(1) << (uint32_t) index)) != 0U;

        if ((roles & TR_RAFT_CONF_OLD_VOTER) != 0U) {
            ++old_count;
            old_acks += acknowledged;
        }
        if ((roles & TR_RAFT_CONF_NEW_VOTER) != 0U) {
            ++new_count;
            new_acks += acknowledged;
        }
    }
    return old_count != 0U && new_count != 0U &&
           old_acks >= old_count / 2U + 1U &&
           new_acks >= new_count / 2U + 1U;
}

bool tr_raft_membership_match_quorum(
    const tr_raft_membership_t *membership,
    const tr_raft_index_t *match_indices,
    tr_raft_index_t candidate_index)
{
    uint32_t acknowledgements = 0U;
    size_t index;

    if (membership == NULL || match_indices == NULL ||
        candidate_index == 0U ||
        membership->member_count > TR_RAFT_MAX_MEMBERS) {
        return false;
    }
    for (index = 0U; index < membership->member_count; ++index) {
        if (match_indices[index] >= candidate_index) {
            acknowledgements |= UINT32_C(1) << (uint32_t) index;
        }
    }
    return tr_raft_membership_has_quorum(membership, acknowledgements);
}
