#include <tinytest.h>

#include <turboraft/text_query_service.h>

suite("text query service") {
  it("rejects missing service, plan, or sink before invoking Raft") {
    tr_text_query_plan_t plan = {0};
    tr_text_query_service_sink_t sink = {0};

    check_equal(
        tr_text_query_execute_service(NULL, &plan, &sink, NULL), TURBO_EINVAL);
    check_equal(
        tr_text_query_execute_service((tr_raft_service_t *)1, NULL, &sink, NULL),
        TURBO_EINVAL);
    check_equal(
        tr_text_query_execute_service(
            (tr_raft_service_t *)1, &plan, NULL, NULL),
        TURBO_EINVAL);
  }

  it("requires all result sink callbacks") {
    tr_text_query_plan_t plan = {0};
    tr_text_query_service_sink_t sink = {0};

    check_equal(
        tr_text_query_execute_service(
            (tr_raft_service_t *)1, &plan, &sink, NULL),
        TURBO_ENOTSUP);
  }
}
