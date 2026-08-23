#include <tinytest.h>

#include <turboraft/text_query_executor.h>

#include <stdint.h>

typedef struct {
  int calls;
  int status_calls;
  int members_calls;
  int progress_calls;
  int status_result;
  int members_result;
  int progress_result;
  tr_text_query_role_t members_role;
  uint64_t progress_node_id;
} tr_text_query_executor_test_state_t;

static int tr_text_query_executor_test_status(
    void *context,
    const tr_text_query_command_t *command)
{
  tr_text_query_executor_test_state_t *state =
      (tr_text_query_executor_test_state_t *)context;

  ++state->calls;
  ++state->status_calls;
  return state->status_result;
}

static int tr_text_query_executor_test_members(
    void *context,
    const tr_text_query_command_t *command)
{
  tr_text_query_executor_test_state_t *state =
      (tr_text_query_executor_test_state_t *)context;

  ++state->calls;
  ++state->members_calls;
  state->members_role = command->role;
  return state->members_result;
}

static int tr_text_query_executor_test_progress(
    void *context,
    const tr_text_query_command_t *command)
{
  tr_text_query_executor_test_state_t *state =
      (tr_text_query_executor_test_state_t *)context;

  ++state->calls;
  ++state->progress_calls;
  state->progress_node_id = command->node_id;
  return state->progress_result;
}

static const tr_text_query_executor_ops_t tr_text_query_executor_test_ops = {
    .status = tr_text_query_executor_test_status,
    .members = tr_text_query_executor_test_members,
    .progress = tr_text_query_executor_test_progress};

suite("text query executor") {
  it("dispatches typed commands in plan order") {
    tr_text_query_plan_t plan = {0};
    tr_text_query_executor_test_state_t state = {0};

    plan.command_count = 3u;
    plan.commands[0].kind = TR_TEXT_QUERY_SHOW_STATUS;
    plan.commands[1].kind = TR_TEXT_QUERY_SHOW_MEMBERS;
    plan.commands[1].role = TR_TEXT_QUERY_ROLE_LEARNER;
    plan.commands[2].kind = TR_TEXT_QUERY_SHOW_PROGRESS;
    plan.commands[2].node_id = 7u;

    check_equal(tr_text_query_execute(
                     &plan, &tr_text_query_executor_test_ops, &state),
                 TURBO_OK);
    check_equal(state.calls, 3);
    check_equal(state.status_calls, 1);
    check_equal(state.members_calls, 1);
    check_equal(state.progress_calls, 1);
    check_equal(state.members_role, TR_TEXT_QUERY_ROLE_LEARNER);
    check_equal(state.progress_node_id, 7u);
  }

  it("rejects a command without a runtime callback") {
    tr_text_query_plan_t plan = {0};
    tr_text_query_executor_ops_t ops = {0};

    plan.command_count = 1u;
    plan.commands[0].kind = TR_TEXT_QUERY_SHOW_PROGRESS;

    check_equal(tr_text_query_execute(&plan, &ops, NULL), TURBO_ENOTSUP);
  }

  it("stops after the first callback error") {
    tr_text_query_plan_t plan = {0};
    tr_text_query_executor_test_state_t state = {
        .status_result = TURBO_EIO};

    plan.command_count = 2u;
    plan.commands[0].kind = TR_TEXT_QUERY_SHOW_STATUS;
    plan.commands[1].kind = TR_TEXT_QUERY_SHOW_MEMBERS;

    check_equal(tr_text_query_execute(
                     &plan, &tr_text_query_executor_test_ops, &state),
                 TURBO_EIO);
    check_equal(state.calls, 1);
    check_equal(state.status_calls, 1);
    check_equal(state.members_calls, 0);
  }

  it("rejects an over-capacity manually constructed plan") {
    tr_text_query_plan_t plan = {0};
    tr_text_query_executor_test_state_t state = {0};

    plan.command_count = TR_TEXT_MAX_STATEMENTS + 1u;

    check_equal(tr_text_query_execute(
                     &plan, &tr_text_query_executor_test_ops, &state),
                 TURBO_EINVAL);
    check_equal(state.calls, 0);
  }
}
