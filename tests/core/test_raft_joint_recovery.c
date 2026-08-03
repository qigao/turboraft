#include "raft_membership_transition.h"

#include <turboraft/raft_core.h>

#include <tinytest.h>
#include <turbo_error.h>

#include <string.h>

static void joint_recovery_entries(tr_raft_entry_t entries[2])
{
    const tr_raft_node_id_t voters[] = {1U};
    const tr_raft_node_id_t target_voters[] = {1U, 2U};
    tr_raft_membership_transition_t transition;
    tr_raft_membership_t joint;
    tr_raft_membership_t final_membership;

    check_int_eq(tr_raft_membership_transition_init(
                     &transition, voters, 1U, NULL, 0U),
                 TURBO_OK);
    check_int_eq(tr_raft_membership_transition_propose(
                     &transition, target_voters, 2U, NULL, 0U, 201U,
                     &joint),
                 TURBO_OK);
    check_int_eq(tr_raft_membership_final(&joint, &final_membership),
                 TURBO_OK);
    check_int_eq(tr_raft_conf_entry_encode(&joint, 1U, 1U, &entries[0]),
                 TURBO_OK);
    check_int_eq(tr_raft_conf_entry_encode(
                     &final_membership, 2U, 1U, &entries[1]),
                 TURBO_OK);
}

static int joint_recovery_core(const tr_raft_entry_t *entries,
                               size_t entry_count,
                               tr_raft_index_t commit_index,
                               tr_raft_core_t **core)
{
    const tr_raft_node_id_t voters[] = {1U};
    tr_raft_core_config_t config;

    memset(&config, 0, sizeof(config));
    config.self_id = 1U;
    config.voters = voters;
    config.voter_count = 1U;
    config.heartbeat_ticks = 2U;
    config.election_min_ticks = 5U;
    config.election_max_ticks = 10U;
    config.initial_election_timeout_ticks = 5U;
    config.initial_term = 1U;
    config.initial_log_entries = entries;
    config.initial_log_entry_count = entry_count;
    config.initial_commit_index = commit_index;
    config.max_log_entries = 16U;
    return tr_raft_core_create(&config, core);
}

spec("raft joint recovery")
{
    it("replays committed joint and final entries")
    {
        tr_raft_entry_t entries[2];
        tr_raft_core_t *core = NULL;
        tr_raft_status_t status;

        joint_recovery_entries(entries);
        check_int_eq(joint_recovery_core(entries, 2U, 2U, &core), TURBO_OK);
        check_int_eq(tr_raft_core_status(core, &status), TURBO_OK);
        check(!status.joint_configuration);
        check_long_eq(status.membership_transition_id, 201U);
        check_size_eq(status.voter_count, 2U);
        check_size_eq(status.pending_configuration_count, 0U);
        check_size_eq(status.peer_count, 2U);
        tr_raft_core_destroy(core);
    }

    it("retains an uncommitted final entry after joint recovery")
    {
        tr_raft_entry_t entries[2];
        tr_raft_core_t *core = NULL;
        tr_raft_status_t status;

        joint_recovery_entries(entries);
        check_int_eq(joint_recovery_core(entries, 2U, 1U, &core), TURBO_OK);
        check_int_eq(tr_raft_core_status(core, &status), TURBO_OK);
        check(status.joint_configuration);
        check_size_eq(status.pending_configuration_count, 1U);
        check_size_eq(status.peer_count, 2U);
        tr_raft_core_destroy(core);
    }

    it("rejects recovery from a joint entry with different old voters")
    {
        tr_raft_membership_t invalid;
        tr_raft_entry_t entry;
        tr_raft_core_t *core = NULL;

        memset(&invalid, 0, sizeof(invalid));
        invalid.phase = TR_RAFT_CONF_JOINT;
        invalid.transition_id = 202U;
        invalid.member_count = 2U;
        invalid.members[0].node_id = 1U;
        invalid.members[0].roles = TR_RAFT_CONF_NEW_VOTER;
        invalid.members[1].node_id = 2U;
        invalid.members[1].roles = TR_RAFT_CONF_OLD_VOTER;
        check_int_eq(tr_raft_conf_entry_encode(&invalid, 1U, 1U, &entry),
                     TURBO_OK);
        check_int_eq(joint_recovery_core(&entry, 1U, 1U, &core),
                     TURBO_EPROTO);
        check_null(core);
    }
}
