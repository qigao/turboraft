#include "../tests/core/multicore_fixture.h"
#include "multicore_metrics.h"
#include <fmt.h>
#include <stdlib.h>
#include <stdio.h>

#define MC_BENCH_SAMPLES 4096U
#define MC_BENCH_BATCH 8U
#define MC_BENCH_OPERATIONS (MC_BENCH_SAMPLES * TEST_GROUPS * MC_BENCH_BATCH)

static int latency_compare(const void *left, const void *right)
{
    const uint64_t a = *(const uint64_t *)left, b = *(const uint64_t *)right;
    return a < b ? -1 : a > b ? 1 : 0;
}

spec("multicore proposal scheduling benchmark")
{
    static multicore_test_t fixture;
    static uint64_t latency[MC_BENCH_OPERATIONS];
    after_each() {
        if (fixture.mutex != NULL) test_release(&fixture);
        tr_raft_multicore_destroy(fixture.runtime);
        cmeta_cond_destroy(&fixture.changed);
        cmeta_mutex_destroy(&fixture.mutex);
        memset(&fixture, 0, sizeof(fixture));
    }
    for (uint32_t owners = 1U; owners <= 4U; owners *= 2U) {
        it("measures bounded proposals with %u owners", owners) {
            tr_raft_multicore_request_t request = {0};
            tr_raft_multicore_completion_t completion;
            uint64_t starts[TEST_GROUPS][MC_BENCH_BATCH];
            uint64_t cpu_before, cpu_after, peak_bytes, begin, elapsed;
            size_t sample = 0, measured = 0;
            test_config(&fixture, owners);
            fixture.max_log_entries = MC_BENCH_SAMPLES * MC_BENCH_BATCH + 1U;
            check_equal(test_start(&fixture), SALTS_OK);
            request.operation = TR_RAFT_MULTICORE_PROPOSE;
            request.value.proposal.size = sizeof(uint64_t);
            check_equal(multicore_process_metrics(&cpu_before, &peak_bytes), 0);
            begin = cmeta_hrtime();
            benchmark_ops("8 groups, 8 pending/group, in-memory storage", MC_BENCH_SAMPLES,
                          TEST_GROUPS * MC_BENCH_BATCH) {
                for (size_t group = 0; group < TEST_GROUPS; ++group) {
                    for (size_t n = 0; n < MC_BENCH_BATCH; ++n) {
                        request.request_id = sample * MC_BENCH_BATCH + n + 1U;
                        request.value.proposal.command_id = request.request_id;
                        memcpy(request.value.proposal.data, &request.request_id, sizeof(uint64_t));
                        starts[group][n] = cmeta_hrtime();
                        check_equal(tr_raft_multicore_submit(fixture.runtime, group + 1U, &request), SALTS_OK);
                    }
                }
                for (size_t group = 0; group < TEST_GROUPS; ++group) {
                    for (size_t n = 0; n < MC_BENCH_BATCH; ++n) {
                        int result;
                        do {
                            result = tr_raft_multicore_take(fixture.runtime, group + 1U, &completion);
                            if (result == SALTS_ENOENT) cmeta_thread_yield();
                        } while (result == SALTS_ENOENT && cmeta_hrtime() - starts[group][n] < UINT64_C(10000000000));
                        latency[measured++] = cmeta_hrtime() - starts[group][n];
                        check_equal(result, SALTS_OK);
                        check_equal(completion.result, SALTS_OK);
                    }
                }
                ++sample;
            }
            elapsed = cmeta_hrtime() - begin;
            check_equal(multicore_process_metrics(&cpu_after, &peak_bytes), 0);
            check_equal(tr_raft_multicore_stop(fixture.runtime), SALTS_OK);
            check_equal(measured, MC_BENCH_OPERATIONS);
            for (size_t i = 0; i < TEST_GROUPS; ++i) {
                check_equal(fixture.groups[i].applied, MC_BENCH_SAMPLES * MC_BENCH_BATCH);
                check_false(fixture.groups[i].wrong_order);
                check_false(fixture.groups[i].wrong_thread);
            }
            qsort(latency, measured, sizeof(latency[0]), latency_compare);
            tstr report = tstr_format(
                "owners={} operations={} ops_per_second={} p99_us={} process_cpu_cores={} peak_rss_bytes={} queue_bytes={}",
                owners, measured, (double)measured * 1e9 / (double)elapsed,
                (double)latency[(measured * 99U + 99U) / 100U - 1U] / 1000.0,
                (double)(cpu_after - cpu_before) / (double)elapsed, peak_bytes,
                (size_t)TEST_GROUPS * (fixture.config.capacity + 1U) *
                    (sizeof(request) + sizeof(completion)));
            check_not_null(report);
            puts(report);
            tstr_free(report);
        }
    }
}
