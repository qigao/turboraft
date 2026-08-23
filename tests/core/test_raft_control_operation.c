#include "raft_control_operation.h"

#include <tinytest.h>
#include <turbo_error.h>

#include <string.h>

spec("raft control operation JSON")
{
    it("formats every operation state without allocation")
    {
        static const tr_raft_operation_state_t states[] = {
            TR_RAFT_OPERATION_PENDING,
            TR_RAFT_OPERATION_COMMITTED,
            TR_RAFT_OPERATION_APPLIED,
            TR_RAFT_OPERATION_LOST,
            TR_RAFT_OPERATION_EXPIRED};
        static const char *names[] = {
            "PENDING", "COMMITTED", "APPLIED", "LOST", "EXPIRED"};
        tr_raft_operation_status_t status;
        char json[160];
        size_t json_size;
        size_t index;

        memset(&status, 0, sizeof(status));
        status.term = 12U;
        status.index = 481U;
        status.commit_index = 486U;
        status.applied_index = 480U;

        for (index = 0U; index < sizeof(states) / sizeof(states[0]); ++index) {
            status.state = states[index];
            check_equal(tr_control_operation_status_json(
                             &status, json, sizeof(json), &json_size),
                         TURBO_OK);
            check_equal(json_size, strlen(json));
            check_not_null(strstr(json, names[index]));
            check_not_null(strstr(json, "\"term\":12"));
            check_not_null(strstr(json, "\"index\":481"));
        }
    }

    it("fails instead of truncating a response")
    {
        tr_raft_operation_status_t status;
        char json[16];
        size_t json_size = 99U;

        memset(&status, 0, sizeof(status));
        status.state = TR_RAFT_OPERATION_PENDING;
        status.term = 1U;
        status.index = 1U;
        check_equal(tr_control_operation_status_json(
                         &status, json, sizeof(json), &json_size),
                     TURBO_ENOSPC);
        check_equal(json_size, 99U);

        status.state = (tr_raft_operation_state_t)99;
        check_equal(tr_control_operation_status_json(
                         &status, json, sizeof(json), &json_size),
                     TURBO_EINVAL);
    }
}
