#include "raft_membership_transition.h"
#include "raft_peer_set.h"

#include <tinytest.h>
#include <salts_error.h>

#include <string.h>

spec("raft peer set")
{
    it("keeps current learners while adding pending target peers")
    {
        const tr_raft_node_id_t voters[] = {1U, 3U};
        const tr_raft_node_id_t learners[] = {2U};
        const tr_raft_node_id_t target_voters[] = {1U, 4U};
        const tr_raft_membership_t *sources[2];
        tr_raft_membership_t stable;
        tr_raft_membership_t joint;
        tr_raft_peer_set_t peers;

        check_equal(tr_raft_membership_stable(
                         voters, 2U, learners, 1U, &stable),
                     SALTS_OK);
        check_equal(tr_raft_membership_joint(
                         &stable, target_voters, 2U, NULL, 0U, 91U, &joint),
                     SALTS_OK);
        sources[0] = &stable;
        sources[1] = &joint;
        check_equal(tr_raft_peer_set_build(sources, 2U, &peers), SALTS_OK);
        check_equal(peers.count, 4U);
        check_equal(peers.node_ids[0], 1U);
        check_equal(peers.node_ids[1], 2U);
        check_equal(peers.node_ids[2], 3U);
        check_equal(peers.node_ids[3], 4U);
        check_equal(tr_raft_peer_set_index(&peers, 3U), 2);
    }

    it("rejects a union beyond the bounded progress capacity")
    {
        tr_raft_node_id_t first_voters[TR_RAFT_MAX_MEMBERS];
        tr_raft_node_id_t second_voters[TR_RAFT_MAX_MEMBERS];
        const tr_raft_membership_t *sources[2];
        tr_raft_membership_t first;
        tr_raft_membership_t second;
        tr_raft_peer_set_t peers;
        size_t index;

        for (index = 0U; index < TR_RAFT_MAX_MEMBERS; ++index) {
            first_voters[index] = index + 1U;
            second_voters[index] = index + TR_RAFT_MAX_MEMBERS + 1U;
        }
        check_equal(tr_raft_membership_stable(
                         first_voters, TR_RAFT_MAX_MEMBERS, NULL, 0U, &first),
                     SALTS_OK);
        check_equal(tr_raft_membership_stable(
                         second_voters, TR_RAFT_MAX_MEMBERS, NULL, 0U,
                         &second),
                     SALTS_OK);
        sources[0] = &first;
        sources[1] = &second;
        check_equal(tr_raft_peer_set_build(sources, 2U, &peers),
                     SALTS_ENOSPC);
        check_equal(peers.count, 0U);
    }

    it("rejects an incoming joint transition whose peer union overflows")
    {
        tr_raft_node_id_t learners[TR_RAFT_MAX_MEMBERS - 1U];
        const tr_raft_node_id_t voters[] = {1U};
        tr_raft_membership_transition_t transition;
        tr_raft_membership_t joint;
        size_t index;

        for (index = 0U; index < TR_RAFT_MAX_MEMBERS - 1U; ++index) {
            learners[index] = index + 2U;
        }
        check_equal(tr_raft_membership_transition_init(
                         &transition, voters, 1U, learners,
                         TR_RAFT_MAX_MEMBERS - 1U),
                     SALTS_OK);
        memset(&joint, 0, sizeof(joint));
        joint.phase = TR_RAFT_CONF_JOINT;
        joint.transition_id = 92U;
        joint.member_count = TR_RAFT_MAX_MEMBERS;
        joint.members[0].node_id = 1U;
        joint.members[0].roles =
            TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER;
        for (index = 1U; index < TR_RAFT_MAX_MEMBERS; ++index) {
            joint.members[index].node_id =
                TR_RAFT_MAX_MEMBERS + index;
            joint.members[index].roles = TR_RAFT_CONF_NEW_VOTER;
        }
        check_equal(tr_raft_membership_transition_stage(
                         &transition, &joint, 5U),
                     SALTS_EPROTO);
    }
}
