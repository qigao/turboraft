#include "raft_membership.h"

#include <tinytest.h>
#include <turbo_error.h>

#include <string.h>

static uint32_t membership_acknowledgements(
    const tr_raft_membership_t *membership,
    const tr_raft_node_id_t *nodes,
    size_t node_count)
{
    uint32_t acknowledgements = 0U;
    size_t index;

    for (index = 0U; index < node_count; ++index) {
        int member_index = tr_raft_membership_index(membership, nodes[index]);

        if (member_index >= 0) {
            acknowledgements |= UINT32_C(1) << (uint32_t) member_index;
        }
    }
    return acknowledgements;
}

spec("raft membership state")
{
    it("merges stable voters and learners in node order")
    {
        const tr_raft_node_id_t voters[] = {1U, 3U};
        const tr_raft_node_id_t learners[] = {2U, 4U};
        tr_raft_membership_t membership;

        check_equal(tr_raft_membership_stable(
                         voters, 2U, learners, 2U, &membership),
                     TURBO_OK);
        check_equal(membership.phase, TR_RAFT_CONF_FINAL);
        check_equal(membership.member_count, 4U);
        check_equal(membership.members[1].node_id, 2U);
        check_equal(membership.members[1].roles,
                     TR_RAFT_CONF_LEARNER);
        check(tr_raft_membership_is_voter(&membership, 3U));
        check(!tr_raft_membership_is_voter(&membership, 4U));
    }

    it("builds promotion and demotion roles in a joint state")
    {
        const tr_raft_node_id_t voters[] = {1U, 2U, 3U};
        const tr_raft_node_id_t learners[] = {4U};
        const tr_raft_node_id_t target_voters[] = {1U, 3U, 4U};
        const tr_raft_node_id_t target_learners[] = {2U};
        tr_raft_membership_t stable;
        tr_raft_membership_t joint;

        check_equal(tr_raft_membership_stable(
                         voters, 3U, learners, 1U, &stable),
                     TURBO_OK);
        check_equal(tr_raft_membership_joint(
                         &stable, target_voters, 3U, target_learners, 1U,
                         51U, &joint),
                     TURBO_OK);
        check_equal(joint.phase, TR_RAFT_CONF_JOINT);
        check_equal(joint.transition_id, 51U);
        check_equal(tr_raft_membership_roles(&joint, 2U),
                     TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_LEARNER);
        check_equal(tr_raft_membership_roles(&joint, 4U),
                     TR_RAFT_CONF_NEW_VOTER);
    }

    it("creates a final state from target roles only")
    {
        const tr_raft_node_id_t voters[] = {1U, 2U, 3U};
        const tr_raft_node_id_t target_voters[] = {1U, 4U};
        const tr_raft_node_id_t target_learners[] = {2U};
        tr_raft_membership_t stable;
        tr_raft_membership_t joint;
        tr_raft_membership_t final_membership;

        check_equal(tr_raft_membership_stable(
                         voters, 3U, NULL, 0U, &stable),
                     TURBO_OK);
        check_equal(tr_raft_membership_joint(
                         &stable, target_voters, 2U, target_learners, 1U,
                         52U, &joint),
                     TURBO_OK);
        check_equal(tr_raft_membership_final(&joint, &final_membership),
                     TURBO_OK);
        check_equal(final_membership.member_count, 3U);
        check_equal(tr_raft_membership_index(&final_membership, 3U), -1);
        check_equal(tr_raft_membership_roles(&final_membership, 1U),
                     TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER);
        check_equal(tr_raft_membership_roles(&final_membership, 2U),
                     TR_RAFT_CONF_LEARNER);
        check_equal(tr_raft_membership_roles(&final_membership, 4U),
                     TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER);
    }

    it("requires majorities from both voter sets")
    {
        const tr_raft_node_id_t voters[] = {1U, 2U, 3U};
        const tr_raft_node_id_t target_voters[] = {1U, 3U, 4U};
        const tr_raft_node_id_t old_only[] = {1U, 2U};
        const tr_raft_node_id_t new_only[] = {1U, 4U};
        const tr_raft_node_id_t both[] = {1U, 2U, 4U};
        tr_raft_membership_t stable;
        tr_raft_membership_t joint;

        check_equal(tr_raft_membership_stable(
                         voters, 3U, NULL, 0U, &stable),
                     TURBO_OK);
        check_equal(tr_raft_membership_joint(
                         &stable, target_voters, 3U, NULL, 0U, 53U, &joint),
                     TURBO_OK);
        check(!tr_raft_membership_has_quorum(
            &joint, membership_acknowledgements(&joint, old_only, 2U)));
        check(!tr_raft_membership_has_quorum(
            &joint, membership_acknowledgements(&joint, new_only, 2U)));
        check(tr_raft_membership_has_quorum(
            &joint, membership_acknowledgements(&joint, both, 3U)));
    }

    it("rejects a temporary union beyond the bounded peer capacity")
    {
        tr_raft_node_id_t voters[TR_RAFT_MAX_MEMBERS];
        tr_raft_node_id_t target_voters[TR_RAFT_MAX_MEMBERS];
        tr_raft_membership_t stable;
        tr_raft_membership_t joint;
        size_t index;

        for (index = 0U; index < TR_RAFT_MAX_MEMBERS; ++index) {
            voters[index] = index + 1U;
            target_voters[index] = index + 2U;
        }
        check_equal(tr_raft_membership_stable(
                         voters, TR_RAFT_MAX_MEMBERS, NULL, 0U, &stable),
                     TURBO_OK);
        check_equal(tr_raft_membership_joint(
                         &stable, target_voters, TR_RAFT_MAX_MEMBERS,
                         NULL, 0U, 54U, &joint),
                     TURBO_ENOSPC);
    }
}
