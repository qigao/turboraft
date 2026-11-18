#include "multicore_fixture.h"

spec("multicore owner runtime")
{
    static multicore_test_t f;
    after_each() {
        if (f.mutex != NULL) test_release(&f);
        tr_raft_multicore_destroy(f.runtime);
        sync_cond_destroy(&f.changed);
        sync_mutex_destroy(&f.mutex);
        memset(&f, 0, sizeof(f));
    }
    for (uint32_t owners = 1U; owners <= 4U; owners *= 2U) {
        it("executes eight groups with %u owners and preserves copied proposal order", owners) {
            tr_raft_multicore_request_t q = {0};
            tr_raft_multicore_completion_t c;
            test_config(&f, owners);
            check_equal(test_start(&f), SALTS_OK);
            q.operation = TR_RAFT_MULTICORE_PROPOSE;
            q.value.proposal.size = sizeof(uint64_t);
            /* More rounds than capacity force repeated ring wrap-around. */
            for (uint64_t round = 0; round < 12U; ++round) {
                for (uint64_t id = 1U; id <= TEST_GROUPS; ++id)
                    for (uint64_t n = 1U; n <= 8U; ++n) {
                        q.request_id = round * 8U + n;
                        q.value.proposal.command_id = q.request_id;
                        memcpy(q.value.proposal.data, &q.request_id, sizeof(uint64_t));
                        check_equal(tr_raft_multicore_submit(f.runtime, id, &q), SALTS_OK);
                        memset(q.value.proposal.data, 0xff, sizeof(q.value.proposal.data));
                    }
                for (uint64_t id = 1U; id <= TEST_GROUPS; ++id)
                    for (uint64_t n = 1U; n <= 8U; ++n) {
                        check_equal(test_take(&f, id, &c), SALTS_OK);
                        check_equal(c.result, SALTS_OK);
                        check_equal(c.request_id, round * 8U + n);
                    }
            }
            check_equal(tr_raft_multicore_stop(f.runtime), SALTS_OK);
            for (size_t i = 0; i < TEST_GROUPS; ++i) {
                check_equal(f.groups[i].applied, 96U);
                check_false(f.groups[i].wrong_thread);
                check_false(f.groups[i].wrong_order);
                check_true(f.groups[i].closed);
            }
        }
    }
    it("holds completion credits until taken, then resumes admission") {
        tr_raft_multicore_request_t q = {0};
        tr_raft_multicore_completion_t c;
        tr_raft_multicore_group_status_t status;
        test_config(&f, 2U);
        f.config.capacity = f.config.work_budget = 1U;
        check_equal(test_start(&f), SALTS_OK);
        q.operation = TR_RAFT_MULTICORE_STATUS;
        check_equal(tr_raft_multicore_submit(f.runtime, 1U, &q), SALTS_OK);
        check_equal(tr_raft_multicore_submit(f.runtime, 1U, &q), SALTS_ENOSPC);
        check_equal(tr_raft_multicore_submit(f.runtime, 2U, &q), SALTS_OK);
        check_equal(test_take(&f, 2U, &c), SALTS_OK);
        check_equal(tr_raft_multicore_group_status(f.runtime, 1U, &status), SALTS_OK);
        check_equal(status.outstanding, 1U);
        check_equal(status.rejected, 1U);
        check_equal(test_take(&f, 1U, &c), SALTS_OK);
        check_equal(tr_raft_multicore_submit(f.runtime, 1U, &q), SALTS_OK);
        check_equal(test_take(&f, 1U, &c), SALTS_OK);
    }
    it("accepts concurrent producers with exactly one ordered completion per request") {
        sync_thread_t threads[4] = {0};
        multicore_producer_t producers[4] = {0};
        uint64_t next[4] = {0};
        tr_raft_multicore_completion_t completion;
        test_config(&f, 2U);
        f.config.capacity = 128U;
        check_equal(test_start(&f), SALTS_OK);
        for (size_t i = 0; i < 4U; ++i) {
            producers[i].runtime = f.runtime;
            producers[i].producer = i;
            check_equal(sync_thread_create(&threads[i], concurrent_producer, &producers[i]), 0);
        }
        for (size_t i = 0; i < 4U; ++i) {
            check_equal(sync_thread_join(&threads[i]), 0);
            check_equal(producers[i].result, SALTS_OK);
        }
        for (size_t i = 0; i < 128U; ++i) {
            check_equal(test_take(&f, 1U, &completion), SALTS_OK);
            check_equal(completion.result, SALTS_OK);
            check_less(completion.request_id, 128U);
            size_t producer = (size_t)(completion.request_id / 32U);
            check_equal(completion.request_id % 32U, next[producer]++);
        }
        for (size_t i = 0; i < 4U; ++i) check_equal(next[i], 32U);
    }
    it("isolates a stalled owner and cancels its queued work on concurrent stop") {
        tr_raft_multicore_request_t q = {0};
        tr_raft_multicore_completion_t c;
        sync_thread_t stoppers[2] = {0};
        test_config(&f, 2U);
        f.block = true;
        check_equal(test_start(&f), SALTS_OK);
        sync_mutex_lock(&f.mutex);
        while (!f.entered) sync_cond_wait(&f.changed, &f.mutex);
        sync_mutex_unlock(&f.mutex);
        q.operation = TR_RAFT_MULTICORE_STATUS;
        check_equal(tr_raft_multicore_submit(f.runtime, 1U, &q), SALTS_OK);
        check_equal(tr_raft_multicore_submit(f.runtime, 2U, &q), SALTS_OK);
        check_equal(test_take(&f, 2U, &c), SALTS_OK);
        check_equal(c.result, SALTS_OK);
        tr_raft_multicore_request_stop(f.runtime);
        check_equal(tr_raft_multicore_wait_stopped(f.runtime, 0U), SALTS_ETIMEDOUT);
        check_equal(tr_raft_multicore_submit(f.runtime, 2U, &q), SALTS_ECANCELED);
        check_equal(sync_thread_create(&stoppers[0], test_stop, f.runtime), 0);
        check_equal(sync_thread_create(&stoppers[1], test_stop, f.runtime), 0);
        test_release(&f);
        check_equal(sync_thread_join(&stoppers[0]), 0);
        check_equal(sync_thread_join(&stoppers[1]), 0);
        check_equal(tr_raft_multicore_wait_stopped(f.runtime, 0U), SALTS_OK);
        check_equal(test_take(&f, 1U, &c), SALTS_OK);
        check_equal(c.result, SALTS_ECANCELED);
        check_equal(tr_raft_multicore_stop(f.runtime), SALTS_OK);
    }
    it("rolls partial startup back on each original owner") {
        test_config(&f, 4U);
        f.fail_group = 5U;
        check_equal(test_start(&f), SALTS_EIO);
        check_null(f.runtime);
        for (size_t i = 0; i < TEST_GROUPS; ++i) {
            check_equal(f.groups[i].opened, f.groups[i].closed);
            check_false(f.groups[i].wrong_thread);
        }
        for (size_t i = 0; i < 4U; ++i) {
            check_equal(f.owner_opened[i], 1U);
            check_equal(f.owner_closed[i], 1U);
        }
    }
    it("rejects invalid topology before calling factories") {
        test_config(&f, 2U);
        f.assignments[1].group_id = 1U;
        check_equal(test_start(&f), SALTS_EINVAL);
        f.assignments[1].group_id = 2U;
        f.config.capacity = TR_RAFT_MULTICORE_MAX_CAPACITY + 1U;
        check_equal(test_start(&f), SALTS_EINVAL);
        f.config.capacity = 8U;
        f.assignments[0].owner_index = 2U;
        check_equal(test_start(&f), SALTS_EINVAL);
        check_equal(f.owner_opened[0], 0U);
    }
}
