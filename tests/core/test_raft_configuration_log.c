#include "raft_log.h"
#include "raft_membership_transition.h"

#include <tinytest.h>
#include <salts_error.h>

spec("raft configuration log")
{
    it("appends and stages a canonical joint entry")
    {
        const tr_raft_node_id_t voters[] = {1U, 2U, 3U};
        const tr_raft_node_id_t target_voters[] = {1U, 3U, 4U};
        tr_raft_membership_transition_t transition;
        tr_raft_membership_t joint;
        tr_raft_membership_t decoded;
        tr_raft_log_t log;
        const tr_raft_log_entry_t *entry = NULL;

        check_equal(tr_raft_membership_transition_init(
                         &transition, voters, 3U, NULL, 0U),
                     SALTS_OK);
        check_equal(tr_raft_membership_transition_propose(
                         &transition, target_voters, 3U, NULL, 0U, 81U,
                         &joint),
                     SALTS_OK);
        check_equal(tr_raft_log_init(&log, 4U, 0U, 0U), SALTS_OK);
        check_equal(tr_raft_log_append_configuration(
                         &log, 5U, &joint, &entry),
                     SALTS_OK);
        check_not_null(entry);
        check_equal(entry->index, 1U);
        check_equal(entry->term, 5U);
        check_equal(entry->command_id, 0U);
        check_equal(tr_raft_conf_entry_decode(entry, &decoded), SALTS_OK);
        check_equal(decoded.transition_id, 81U);
        check_equal(tr_raft_membership_transition_stage_entry(
                         &transition, entry),
                     SALTS_OK);
        check_equal(transition.pending_count, 1U);
        tr_raft_log_destroy(&log);
    }

    it("keeps command zero unavailable to normal append")
    {
        tr_raft_log_t log;

        check_equal(tr_raft_log_init(&log, 2U, 0U, 0U), SALTS_OK);
        check_equal(tr_raft_log_append_local(
                         &log, 1U, 0U, "x", 1U, NULL),
                     SALTS_EINVAL);
        check_equal(tr_raft_log_count(&log), 0U);
        tr_raft_log_destroy(&log);
    }

    it("does not mutate a full log when configuration append fails")
    {
        const tr_raft_node_id_t voters[] = {1U};
        const tr_raft_node_id_t target_voters[] = {1U, 2U};
        tr_raft_membership_transition_t transition;
        tr_raft_membership_t joint;
        tr_raft_log_t log;

        check_equal(tr_raft_membership_transition_init(
                         &transition, voters, 1U, NULL, 0U),
                     SALTS_OK);
        check_equal(tr_raft_membership_transition_propose(
                         &transition, target_voters, 2U, NULL, 0U, 82U,
                         &joint),
                     SALTS_OK);
        check_equal(tr_raft_log_init(&log, 1U, 0U, 0U), SALTS_OK);
        check_equal(tr_raft_log_append_local(
                         &log, 1U, 1U, NULL, 0U, NULL),
                     SALTS_OK);
        check_equal(tr_raft_log_append_configuration(
                         &log, 1U, &joint, NULL),
                     SALTS_ENOSPC);
        check_equal(tr_raft_log_count(&log), 1U);
        tr_raft_log_destroy(&log);
    }
}
