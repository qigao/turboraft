#include <tinytest.h>

#include <turboraft/raft_core.h>
#include <turboraft/raft_wire_codec.h>
#include <turboraft/text_replay_executor.h>
#include <turboraft/text_syntax.h>

#include <stdint.h>
#include <string.h>

typedef struct {
  int submit_calls;
  int poll_calls;
  int tick_calls;
  int submit_result;
  int poll_result;
  uint64_t submit_request_id;
  uint64_t submit_node_id;
  uint64_t submit_client_id;
  uint64_t submit_sequence;
  size_t payload_size;
  uint8_t payload[TR_RAFT_MAX_ENTRY_BYTES];
  uint64_t poll_request_id;
  uint64_t poll_term;
  uint64_t poll_index;
  uint64_t poll_timeout_ticks;
  tr_text_replay_poll_target_t poll_target;
  uint64_t tick_total;
} tr_text_replay_executor_test_state_t;

static int tr_text_replay_test_submit(
    void *context,
    const tr_text_replay_action_t *action,
    const uint8_t *payload,
    size_t payload_size,
    tr_text_replay_receipt_t *out_receipt)
{
  tr_text_replay_executor_test_state_t *state =
      (tr_text_replay_executor_test_state_t *)context;
  state->submit_calls++;
  state->submit_request_id = action->request_id;
  state->submit_node_id = action->node_id;
  state->submit_client_id = action->client_id;
  state->submit_sequence = action->sequence;
  state->payload_size = payload_size;
  memcpy(state->payload, payload, payload_size);
  if (state->submit_result != SALTS_OK) {
    return state->submit_result;
  }
  out_receipt->term = 11u;
  out_receipt->index = 100u + (uint64_t)state->submit_calls;
  return SALTS_OK;
}

static int tr_text_replay_test_poll(
    void *context,
    const tr_text_replay_action_t *action,
    const tr_text_replay_receipt_t *receipt)
{
  tr_text_replay_executor_test_state_t *state =
      (tr_text_replay_executor_test_state_t *)context;
  state->poll_calls++;
  state->poll_request_id = action->request_id;
  state->poll_term = receipt->term;
  state->poll_index = receipt->index;
  state->poll_timeout_ticks = action->timeout_ticks;
  state->poll_target = action->poll_target;
  return state->poll_result;
}

static int tr_text_replay_test_tick(void *context, uint64_t ticks)
{
  tr_text_replay_executor_test_state_t *state =
      (tr_text_replay_executor_test_state_t *)context;
  state->tick_calls++;
  state->tick_total += ticks;
  return SALTS_OK;
}

static const tr_text_replay_executor_ops_t tr_text_replay_test_ops = {
    .submit = tr_text_replay_test_submit,
    .poll = tr_text_replay_test_poll,
    .tick = tr_text_replay_test_tick};

spec("TurboRaft text syntax") {
  it("parses query commands into a typed plan") {
    const char input[] =
        "# inspection commands\n"
        "show status;\n"
        "show members;\n"
        "show members where role == voter;\n"
        "show progress for node 2;";
    tr_text_query_plan_t plan;
    tr_text_diagnostic_t diagnostic;

    check_equal(tr_text_query_parse(input, strlen(input), NULL, &plan,
                                     &diagnostic),
                 SALTS_OK);
    check_equal(plan.command_count, 4u);
    check_equal(plan.commands[0].kind, TR_TEXT_QUERY_SHOW_STATUS);
    check_equal(plan.commands[1].kind, TR_TEXT_QUERY_SHOW_MEMBERS);
    check_equal(plan.commands[1].role, TR_TEXT_QUERY_ROLE_ANY);
    check_equal(plan.commands[2].role, TR_TEXT_QUERY_ROLE_VOTER);
    check_equal(plan.commands[3].kind, TR_TEXT_QUERY_SHOW_PROGRESS);
    check_equal(plan.commands[3].node_id, 2u);
  }

  it("parses protocol debug frames without touching Raft state") {
    const char input[] =
        "# one protocol frame\n"
        "frame version 3 kind raft {\n"
        "  from = 1;\n"
        "  to = 2;\n"
        "  term = 7;\n"
        "  message = append_request;\n"
        "  payload = 0x0102a0ff;\n"
        "}";
    tr_text_protocol_debug_plan_t plan;
    tr_text_diagnostic_t diagnostic;

    check_equal(tr_text_protocol_debug_parse(input, strlen(input), NULL,
                                               &plan, &diagnostic),
                 SALTS_OK);
    check_equal(plan.frame_count, 1u);
    check_equal(plan.frames[0].version, 3u);
    check_equal(plan.frames[0].from, 1u);
    check_equal(plan.frames[0].to, 2u);
    check_equal(plan.frames[0].message.data, "append_request",
                 plan.frames[0].message.len);
    check_equal(plan.frames[0].payload_hex.data, "0x0102a0ff",
                 plan.frames[0].payload_hex.len);
  }

  it("keeps protocol frames without payload backward compatible") {
    const char input[] =
        "frame version 3 kind raft {\n"
        "  from = 1;\n"
        "  to = 2;\n"
        "  term = 7;\n"
        "  message = append_request;\n"
        "}";
    tr_text_protocol_debug_plan_t plan;
    tr_text_diagnostic_t diagnostic;

    check_equal(tr_text_protocol_debug_parse(input, strlen(input), NULL,
                                               &plan, &diagnostic),
                 SALTS_OK);
    check_equal(plan.frame_count, 1u);
    check_equal(plan.frames[0].payload_hex.len, 0u);
  }

  it("rejects protocol payloads with an odd hex digit count") {
    const char input[] =
        "frame version 3 kind raft {\n"
        "  from = 1;\n"
        "  to = 2;\n"
        "  term = 7;\n"
        "  message = append_request;\n"
        "  payload = 0x123;\n"
        "}";
    tr_text_protocol_debug_plan_t plan;
    tr_text_diagnostic_t diagnostic;

    check_equal(tr_text_protocol_debug_parse(input, strlen(input), NULL,
                                               &plan, &diagnostic),
                 SALTS_EPROTO);
    check_equal(diagnostic.kind, TR_TEXT_DIAGNOSTIC_SEMANTIC);
    check_equal(plan.frame_count, 0u);
  }

  it("rejects duplicate protocol frame fields") {
    const char input[] =
        "frame version 3 kind raft {\n"
        "  from = 1;\n"
        "  from = 2;\n"
        "  to = 2;\n"
        "  term = 7;\n"
        "  message = append_request;\n"
        "}";
    tr_text_protocol_debug_plan_t plan;
    tr_text_diagnostic_t diagnostic;

    check_equal(tr_text_protocol_debug_parse(input, strlen(input), NULL,
                                               &plan, &diagnostic),
                 SALTS_EPROTO);
    check_equal(diagnostic.kind, TR_TEXT_DIAGNOSTIC_SEMANTIC);
    check_equal(plan.frame_count, 0u);
  }

  it("rejects a query without a statement terminator") {
    const char input[] = "show status";
    tr_text_query_plan_t plan;
    tr_text_diagnostic_t diagnostic;

    check_equal(tr_text_query_parse(input, strlen(input), NULL, &plan,
                                     &diagnostic),
                 SALTS_EPROTO);
    check_equal(diagnostic.kind, TR_TEXT_DIAGNOSTIC_SYNTAX);
    check_equal(plan.command_count, 0u);
  }

  it("rejects protocol frames missing required fields") {
    const char input[] =
        "frame version 3 kind raft {\n"
        "  from = 1;\n"
        "  to = 2;\n"
        "  term = 7;\n"
        "}";
    tr_text_protocol_debug_plan_t plan;
    tr_text_diagnostic_t diagnostic;

    check_equal(tr_text_protocol_debug_parse(input, strlen(input), NULL,
                                               &plan, &diagnostic),
                 SALTS_EPROTO);
    check_equal(diagnostic.kind, TR_TEXT_DIAGNOSTIC_SEMANTIC);
    check_equal(plan.frame_count, 0u);
  }

  it("enforces the configured statement limit") {
    const char input[] = "show status;\nshow status;";
    tr_text_parse_options_t options = {0u, 1u};
    tr_text_query_plan_t plan;
    tr_text_diagnostic_t diagnostic;

    check_equal(tr_text_query_parse(input, strlen(input), &options, &plan,
                                     &diagnostic),
                 SALTS_ENOSPC);
    check_equal(diagnostic.kind, TR_TEXT_DIAGNOSTIC_LIMIT);
    check_equal(plan.command_count, 0u);
  }

  it("enforces the configured input limit") {
    const char input[] = "show status;";
    tr_text_parse_options_t options = {1u, 0u};
    tr_text_query_plan_t plan;
    tr_text_diagnostic_t diagnostic;

    check_equal(tr_text_query_parse(input, strlen(input), &options, &plan,
                                     &diagnostic),
                 SALTS_ENOSPC);
    check_equal(diagnostic.kind, TR_TEXT_DIAGNOSTIC_LIMIT);
    check_equal(plan.command_count, 0u);
  }

  it("parses replay actions and reports syntax locations") {
    const char input[] =
        "# deterministic setup\n"
        "node 1;\n"
        "node 2;\n"
        "tick 5;\n"
        "send node 1 -> node 2;\n"
        "drop next append_request;\n"
        "expect node 1 role == leader;\n"
        "expect node 2 commit_index >= 4;";
    tr_text_replay_plan_t plan;
    tr_text_diagnostic_t diagnostic;

    check_equal(tr_text_replay_parse(input, strlen(input), NULL, &plan,
                                      &diagnostic),
                 SALTS_OK);
    check_equal(plan.action_count, 7u);
    check_equal(plan.actions[3].kind, TR_TEXT_REPLAY_SEND);
    check_equal(plan.actions[3].node_id, 1u);
    check_equal(plan.actions[3].peer_id, 2u);
    check_equal(plan.actions[6].comparison, TR_TEXT_REPLAY_COMPARE_GE);

    check_equal(tr_text_replay_parse("tick ;", 6u, NULL, &plan,
                                      &diagnostic),
                 SALTS_EPROTO);
    check_equal(diagnostic.kind, TR_TEXT_DIAGNOSTIC_SYNTAX);
    check_equal(diagnostic.line, 1u);
    check_equal(diagnostic.column, 6u);
  }

  it("parses replay fault injection actions") {
    const char input[] =
        "partition node 1 with node 2;\n"
        "heal node 1 with node 2;\n"
        "delay next append_request by 3 ticks;\n"
        "duplicate next append_request;";
    tr_text_replay_plan_t plan;
    tr_text_diagnostic_t diagnostic;

    check_equal(tr_text_replay_parse(input, strlen(input), NULL, &plan,
                                      &diagnostic),
                 SALTS_OK);
    check_equal(plan.action_count, 4u);

    check_equal(plan.actions[0].kind, TR_TEXT_REPLAY_PARTITION);
    check_equal(plan.actions[0].node_id, 1u);
    check_equal(plan.actions[0].peer_id, 2u);

    check_equal(plan.actions[1].kind, TR_TEXT_REPLAY_HEAL);
    check_equal(plan.actions[1].node_id, 1u);
    check_equal(plan.actions[1].peer_id, 2u);

    check_equal(plan.actions[2].kind, TR_TEXT_REPLAY_DELAY_NEXT);
    check_equal(plan.actions[2].name.data, "append_request",
                 plan.actions[2].name.len);
    check_equal(plan.actions[2].value, 3u);

    check_equal(plan.actions[3].kind, TR_TEXT_REPLAY_DUPLICATE_NEXT);
    check_equal(plan.actions[3].name.data, "append_request",
                 plan.actions[3].name.len);
  }

  it("parses replay submit and bounded poll actions") {
    const char input[] =
        "submit request 3 to node 1 client 7 sequence 9 "
        "payload 0x0102a0ff;\n"
        "poll request 3 until committed timeout 20 ticks;";
    tr_text_replay_plan_t plan;
    tr_text_diagnostic_t diagnostic;

    check_equal(tr_text_replay_parse(input, strlen(input), NULL, &plan,
                                      &diagnostic),
                 SALTS_OK);
    check_equal(plan.action_count, 2u);

    check_equal(plan.actions[0].kind, TR_TEXT_REPLAY_SUBMIT);
    check_equal(plan.actions[0].request_id, 3u);
    check_equal(plan.actions[0].node_id, 1u);
    check_equal(plan.actions[0].client_id, 7u);
    check_equal(plan.actions[0].sequence, 9u);
    check_equal(plan.actions[0].payload_hex.data, "0x0102a0ff",
                 plan.actions[0].payload_hex.len);

    check_equal(plan.actions[1].kind, TR_TEXT_REPLAY_POLL);
    check_equal(plan.actions[1].request_id, 3u);
    check_equal(plan.actions[1].poll_target,
                 TR_TEXT_REPLAY_POLL_COMMITTED);
    check_equal(plan.actions[1].timeout_ticks, 20u);
  }

  it("rejects replay submit payloads with an odd hex digit count") {
    const char input[] =
        "submit request 3 to node 1 client 7 sequence 9 payload 0x123;";
    tr_text_replay_plan_t plan;
    tr_text_diagnostic_t diagnostic;

    check_equal(tr_text_replay_parse(input, strlen(input), NULL, &plan,
                                      &diagnostic),
                 SALTS_EPROTO);
    check_equal(diagnostic.kind, TR_TEXT_DIAGNOSTIC_SEMANTIC);
    check_equal(plan.action_count, 0u);
  }

  it("executes submit and poll with decoded payload and no implicit tick") {
    char input[] =
        "submit request 3 to node 1 client 7 sequence 9 "
        "payload 0x0102a0ff;\n"
        "poll request 3 until committed timeout 20 ticks;";
    tr_text_replay_plan_t plan;
    tr_text_diagnostic_t diagnostic;
    tr_text_replay_executor_test_state_t state = {0};
    const uint8_t expected_payload[] = {0x01u, 0x02u, 0xa0u, 0xffu};

    check_equal(tr_text_replay_parse(input, strlen(input), NULL, &plan,
                                      &diagnostic),
                 SALTS_OK);
    check_equal(tr_text_replay_execute(&plan, &tr_text_replay_test_ops,
                                        &state),
                 SALTS_OK);
    check_equal(state.submit_calls, 1);
    check_equal(state.submit_request_id, 3u);
    check_equal(state.submit_node_id, 1u);
    check_equal(state.submit_client_id, 7u);
    check_equal(state.submit_sequence, 9u);
    check_equal(state.payload_size, sizeof(expected_payload));
    check_equal(state.payload, expected_payload, sizeof(expected_payload));
    check_equal(state.poll_calls, 1);
    check_equal(state.poll_request_id, 3u);
    check_equal(state.poll_term, 11u);
    check_equal(state.poll_index, 101u);
    check_equal(state.poll_timeout_ticks, 20u);
    check_equal(state.poll_target, TR_TEXT_REPLAY_POLL_COMMITTED);
    check_equal(state.tick_calls, 0);
  }

  it("rejects duplicate and unknown replay request ids") {
    const char duplicate_input[] =
        "submit request 3 to node 1 client 7 sequence 9 payload 0x01;\n"
        "submit request 3 to node 1 client 7 sequence 10 payload 0x02;";
    const char unknown_input[] =
        "poll request 9 until applied timeout 4 ticks;";
    tr_text_replay_plan_t plan;
    tr_text_diagnostic_t diagnostic;
    tr_text_replay_executor_test_state_t state = {0};

    check_equal(tr_text_replay_parse(duplicate_input,
                                      strlen(duplicate_input), NULL, &plan,
                                      &diagnostic),
                 SALTS_OK);
    check_equal(tr_text_replay_execute(&plan, &tr_text_replay_test_ops,
                                        &state),
                 SALTS_EALREADY);
    check_equal(state.submit_calls, 1);

    memset(&state, 0, sizeof(state));
    check_equal(tr_text_replay_parse(unknown_input, strlen(unknown_input),
                                      NULL, &plan, &diagnostic),
                 SALTS_OK);
    check_equal(tr_text_replay_execute(&plan, &tr_text_replay_test_ops,
                                        &state),
                 SALTS_ENOENT);
    check_equal(state.poll_calls, 0);
  }

  it("stops the replay at the first callback failure") {
    const char input[] =
        "submit request 3 to node 1 client 7 sequence 9 payload 0x01;\n"
        "tick 4;";
    tr_text_replay_plan_t plan;
    tr_text_diagnostic_t diagnostic;
    tr_text_replay_executor_test_state_t state = {
        .submit_result = SALTS_EIO};

    check_equal(tr_text_replay_parse(input, strlen(input), NULL, &plan,
                                      &diagnostic),
                 SALTS_OK);
    check_equal(tr_text_replay_execute(&plan, &tr_text_replay_test_ops,
                                        &state),
                 SALTS_EIO);
    check_equal(state.submit_calls, 1);
    check_equal(state.tick_calls, 0);
  }

  it("rejects replay payloads larger than a Raft entry") {
    enum {
      PAYLOAD_TEXT_SIZE =
          2u + (TR_RAFT_MAX_ENTRY_BYTES + 1u) * 2u + 1u
    };
    char payload[PAYLOAD_TEXT_SIZE];
    tr_text_replay_plan_t plan = {0};
    tr_text_replay_executor_test_state_t state = {0};
    size_t index;

    payload[0] = '0';
    payload[1] = 'x';
    for (index = 2u; index < PAYLOAD_TEXT_SIZE - 1u; ++index) {
      payload[index] = '0';
    }
    payload[PAYLOAD_TEXT_SIZE - 1u] = '\0';
    plan.action_count = 1u;
    plan.actions[0].kind = TR_TEXT_REPLAY_SUBMIT;
    plan.actions[0].request_id = 3u;
    plan.actions[0].payload_hex.data = payload;
    plan.actions[0].payload_hex.len = strlen(payload);

    check_equal(tr_text_replay_execute(&plan, &tr_text_replay_test_ops,
                                        &state),
                 SALTS_ENOSPC);
    check_equal(state.submit_calls, 0);
  }

  it("rejects replay submit payloads larger than a Raft entry at parse") {
    enum {
      PAYLOAD_BYTES = TR_RAFT_MAX_ENTRY_BYTES + 1u,
      INPUT_CAPACITY = 128u + PAYLOAD_BYTES * 2u + 1u
    };
    char input[INPUT_CAPACITY];
    tr_text_replay_plan_t plan;
    tr_text_diagnostic_t diagnostic;
    size_t offset;
    size_t index;

    offset = (size_t)snprintf(
        input, sizeof(input),
        "submit request 3 to node 1 client 7 sequence 9 payload 0x");
    for (index = 0u; index < PAYLOAD_BYTES * 2u; ++index) {
      input[offset++] = '0';
    }
    input[offset++] = ';';
    input[offset] = '\0';

    check_equal(tr_text_replay_parse(input, offset, NULL, &plan,
                                      &diagnostic),
                 SALTS_ENOSPC);
    check_equal(diagnostic.kind, TR_TEXT_DIAGNOSTIC_LIMIT);
    check_equal(plan.action_count, 0u);
  }

  it("accepts replay submit payloads of exactly one Raft entry") {
    enum {
      PAYLOAD_BYTES = TR_RAFT_MAX_ENTRY_BYTES,
      INPUT_CAPACITY = 128u + PAYLOAD_BYTES * 2u + 1u
    };
    char input[INPUT_CAPACITY];
    tr_text_replay_plan_t plan;
    tr_text_diagnostic_t diagnostic;
    size_t offset;
    size_t index;

    offset = (size_t)snprintf(
        input, sizeof(input),
        "submit request 3 to node 1 client 7 sequence 9 payload 0x");
    for (index = 0u; index < PAYLOAD_BYTES * 2u; ++index) {
      input[offset++] = '0';
    }
    input[offset++] = ';';
    input[offset] = '\0';

    check_equal(tr_text_replay_parse(input, offset, NULL, &plan,
                                      &diagnostic),
                 SALTS_OK);
    check_equal(plan.action_count, 1u);
    check_equal(plan.actions[0].payload_hex.len,
                  2u + PAYLOAD_BYTES * 2u);
  }

  it("rejects protocol frame payloads larger than the wire frame limit") {
    enum {
      PAYLOAD_BYTES = TR_RAFT_WIRE_MAX_FRAME_SIZE + 1u,
      INPUT_CAPACITY = 256u + PAYLOAD_BYTES * 2u + 1u
    };
    char input[INPUT_CAPACITY];
    tr_text_protocol_debug_plan_t plan;
    tr_text_diagnostic_t diagnostic;
    size_t offset;
    size_t index;

    offset = (size_t)snprintf(
        input, sizeof(input),
        "frame version 3 kind raft {\n"
        "  from = 1;\n"
        "  to = 2;\n"
        "  term = 7;\n"
        "  message = append_request;\n"
        "  payload = 0x");
    for (index = 0u; index < PAYLOAD_BYTES * 2u; ++index) {
      input[offset++] = '0';
    }
    offset += (size_t)snprintf(input + offset, sizeof(input) - offset,
                               ";\n}");

    check_equal(tr_text_protocol_debug_parse(input, offset, NULL, &plan,
                                              &diagnostic),
                 SALTS_ENOSPC);
    check_equal(diagnostic.kind, TR_TEXT_DIAGNOSTIC_LIMIT);
    check_equal(plan.frame_count, 0u);
  }

  it("accepts protocol frame payloads of exactly the wire frame limit") {
    enum {
      PAYLOAD_BYTES = TR_RAFT_WIRE_MAX_FRAME_SIZE,
      INPUT_CAPACITY = 256u + PAYLOAD_BYTES * 2u + 1u
    };
    char input[INPUT_CAPACITY];
    tr_text_protocol_debug_plan_t plan;
    tr_text_diagnostic_t diagnostic;
    size_t offset;
    size_t index;

    offset = (size_t)snprintf(
        input, sizeof(input),
        "frame version 3 kind raft {\n"
        "  from = 1;\n"
        "  to = 2;\n"
        "  term = 7;\n"
        "  message = append_request;\n"
        "  payload = 0x");
    for (index = 0u; index < PAYLOAD_BYTES * 2u; ++index) {
      input[offset++] = '0';
    }
    offset += (size_t)snprintf(input + offset, sizeof(input) - offset,
                               ";\n}");

    check_equal(tr_text_protocol_debug_parse(input, offset, NULL, &plan,
                                              &diagnostic),
                 SALTS_OK);
    check_equal(plan.frame_count, 1u);
    check_equal(plan.frames[0].payload_hex.len,
                  2u + PAYLOAD_BYTES * 2u);
  }
}
