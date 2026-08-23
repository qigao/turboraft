#include "raft_membership_transition.h"

#include <tinytest.h>
#include <turbo_error.h>

spec("raft membership transition")
{
    it("stages and applies a joint configuration at its commit boundary")
    {
        const tr_raft_node_id_t voters[] = {1U, 2U, 3U};
        const tr_raft_node_id_t target_voters[] = {1U, 3U, 4U};
        tr_raft_membership_transition_t transition;
        tr_raft_membership_t joint;

        check_equal(tr_raft_membership_transition_init(
                         &transition, voters, 3U, NULL, 0U),
                     TURBO_OK);
        check_equal(tr_raft_membership_transition_propose(
                         &transition, target_voters, 3U, NULL, 0U, 71U,
                         &joint),
                     TURBO_OK);
        check_equal(tr_raft_membership_transition_stage(
                         &transition, &joint, 10U),
                     TURBO_OK);
        check_equal(tr_raft_membership_transition_commit_limit(
                          &transition, 12U),
                      10U);
        check_equal(tr_raft_membership_transition_apply(&transition, 9U),
                     TURBO_OK);
        check_equal(
            tr_raft_membership_transition_committed(&transition)->phase,
            TR_RAFT_CONF_FINAL);
        check_equal(tr_raft_membership_transition_apply(&transition, 10U),
                     TURBO_OK);
        check_equal(
            tr_raft_membership_transition_committed(&transition)->phase,
            TR_RAFT_CONF_JOINT);
        check(tr_raft_membership_transition_needs_final(&transition));
    }

    it("accepts a follower batch containing matching joint and final entries")
    {
        const tr_raft_node_id_t voters[] = {1U, 2U, 3U};
        const tr_raft_node_id_t target_voters[] = {1U, 4U};
        const tr_raft_node_id_t target_learners[] = {2U};
        tr_raft_membership_transition_t transition;
        tr_raft_membership_t joint;
        tr_raft_membership_t final_membership;

        check_equal(tr_raft_membership_transition_init(
                         &transition, voters, 3U, NULL, 0U),
                     TURBO_OK);
        check_equal(tr_raft_membership_transition_propose(
                         &transition, target_voters, 2U, target_learners, 1U,
                         72U, &joint),
                     TURBO_OK);
        check_equal(tr_raft_membership_final(&joint, &final_membership),
                     TURBO_OK);
        check_equal(tr_raft_membership_transition_stage(
                         &transition, &joint, 20U),
                     TURBO_OK);
        check_equal(tr_raft_membership_transition_stage(
                         &transition, &final_membership, 21U),
                     TURBO_OK);
        check_equal(transition.pending_count, 2U);
        check_equal(tr_raft_membership_transition_stage(
                         &transition, &final_membership, 22U),
                     TURBO_EPROTO);
        check_equal(transition.pending_count, 2U);
        check_equal(tr_raft_membership_transition_apply(&transition, 21U),
                     TURBO_OK);
        check_equal(transition.pending_count, 0U);
        check_equal(
            tr_raft_membership_transition_committed(&transition)->phase,
            TR_RAFT_CONF_FINAL);
        check_equal(tr_raft_membership_index(
                         tr_raft_membership_transition_committed(&transition),
                         3U),
                     -1);
        check_equal(tr_raft_membership_roles(
                         tr_raft_membership_transition_committed(&transition),
                         2U),
                     TR_RAFT_CONF_LEARNER);
    }

    it("rejects a joint entry whose old voters differ from committed state")
    {
        const tr_raft_node_id_t voters[] = {1U, 2U, 3U};
        tr_raft_membership_transition_t transition;
        tr_raft_membership_t joint;

        check_equal(tr_raft_membership_transition_init(
                         &transition, voters, 3U, NULL, 0U),
                     TURBO_OK);
        joint.phase = TR_RAFT_CONF_JOINT;
        joint.transition_id = 73U;
        joint.member_count = 3U;
        joint.members[0].node_id = 1U;
        joint.members[0].roles =
            TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER;
        joint.members[1].node_id = 2U;
        joint.members[1].roles = TR_RAFT_CONF_NEW_VOTER;
        joint.members[2].node_id = 4U;
        joint.members[2].roles = TR_RAFT_CONF_OLD_VOTER;
        check_equal(tr_raft_membership_transition_stage(
                         &transition, &joint, 30U),
                     TURBO_EPROTO);
    }

    it("rejects a conflicting final entry")
    {
        const tr_raft_node_id_t voters[] = {1U, 2U, 3U};
        const tr_raft_node_id_t target_voters[] = {1U, 3U, 4U};
        tr_raft_membership_transition_t transition;
        tr_raft_membership_t joint;
        tr_raft_membership_t final_membership;

        check_equal(tr_raft_membership_transition_init(
                         &transition, voters, 3U, NULL, 0U),
                     TURBO_OK);
        check_equal(tr_raft_membership_transition_propose(
                         &transition, target_voters, 3U, NULL, 0U, 74U,
                         &joint),
                     TURBO_OK);
        check_equal(tr_raft_membership_final(&joint, &final_membership),
                     TURBO_OK);
        final_membership.transition_id = 75U;
        check_equal(tr_raft_membership_transition_stage(
                         &transition, &joint, 40U),
                     TURBO_OK);
        check_equal(tr_raft_membership_transition_stage(
                         &transition, &final_membership, 41U),
                     TURBO_EPROTO);
    }

    it("rejects a corrupted pending count before reading pending arrays")
    {
        const tr_raft_node_id_t voters[] = {1U};
        tr_raft_membership_transition_t transition;

        check_equal(tr_raft_membership_transition_init(
                         &transition, voters, 1U, NULL, 0U),
                     TURBO_OK);
        transition.pending_count = TR_RAFT_MEMBERSHIP_MAX_PENDING + 1U;
        check_equal(tr_raft_membership_transition_stage(
                         &transition, &transition.committed, 1U),
                     TURBO_EPROTO);
    }
}
