#include <tinytest.h>

#include <turboraft/text_replay_core_driver.h>
#include <turboraft/text_syntax.h>

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#define TEST_NODE_COUNT 3u

typedef struct tr_replay_driver_test_state {
    int apply_calls;
    size_t applied_entries;
} tr_replay_driver_test_state_t;

static int tr_replay_driver_test_apply(void *context,
                                       tr_raft_node_id_t node_id,
                                       const tr_raft_entry_t *entries,
                                       size_t entry_count)
{
    tr_replay_driver_test_state_t *state =
        (tr_replay_driver_test_state_t *)context;

    (void)node_id;
    (void)entries;
    state->applied_entries += entry_count;
    ++state->apply_calls;
    return TURBO_OK;
}

static int tr_replay_driver_test_create(tr_replay_driver_t **out_driver,
                                        tr_replay_driver_test_state_t *state)
{
    static tr_raft_node_id_t voters[TEST_NODE_COUNT];
    static tr_raft_core_config_t nodes[TEST_NODE_COUNT];
    tr_replay_driver_config_t config;
    size_t index;
    int result;

    memset(voters, 0, sizeof(voters));
    memset(nodes, 0, sizeof(nodes));
    for (index = 0u; index < TEST_NODE_COUNT; ++index) {
        voters[index] = (tr_raft_node_id_t)(index + 1u);
        nodes[index].self_id = voters[index];
        nodes[index].voters = voters;
        nodes[index].voter_count = TEST_NODE_COUNT;
        nodes[index].heartbeat_ticks = 1u;
        nodes[index].election_min_ticks = 3u;
        nodes[index].election_max_ticks = 8u;
        nodes[index].initial_election_timeout_ticks =
            4u + (uint32_t)index;
        nodes[index].max_log_entries = 256u;
    }
    memset(&config, 0, sizeof(config));
    config.nodes = nodes;
    config.node_count = TEST_NODE_COUNT;
    config.apply = tr_replay_driver_test_apply;
    config.apply_context = state;
    result = tr_replay_driver_create(&config, out_driver);
    return result;
}

static tr_text_replay_action_t tr_replay_driver_test_action(void)
{
    tr_text_replay_action_t action;

    memset(&action, 0, sizeof(action));
    return action;
}

static int tr_replay_driver_test_node_action(tr_replay_driver_t *driver,
                                             tr_raft_node_id_t node_id)
{
    tr_text_replay_action_t action = tr_replay_driver_test_action();

    action.kind = TR_TEXT_REPLAY_NODE;
    action.node_id = node_id;
    return tr_replay_driver_step(driver, &action);
}

static int tr_replay_driver_test_tick(tr_replay_driver_t *driver,
                                      uint64_t ticks)
{
    tr_text_replay_action_t action = tr_replay_driver_test_action();

    action.kind = TR_TEXT_REPLAY_TICK;
    action.value = ticks;
    return tr_replay_driver_step(driver, &action);
}

static int tr_replay_driver_test_expect_role(tr_replay_driver_t *driver,
                                             tr_raft_node_id_t node_id,
                                             const char *role)
{
    tr_text_replay_action_t action = tr_replay_driver_test_action();

    action.kind = TR_TEXT_REPLAY_EXPECT_ROLE;
    action.node_id = node_id;
    action.comparison = TR_TEXT_REPLAY_COMPARE_EQ;
    action.name.data = role;
    action.name.len = strlen(role);
    return tr_replay_driver_step(driver, &action);
}

static int tr_replay_driver_test_expect_commit(tr_replay_driver_t *driver,
                                               tr_raft_node_id_t node_id,
                                               uint64_t commit)
{
    tr_text_replay_action_t action = tr_replay_driver_test_action();

    action.kind = TR_TEXT_REPLAY_EXPECT_COMMIT_INDEX;
    action.node_id = node_id;
    action.comparison = TR_TEXT_REPLAY_COMPARE_GE;
    action.value = commit;
    return tr_replay_driver_step(driver, &action);
}

static tr_raft_node_id_t tr_replay_driver_test_leader(
    tr_replay_driver_t *driver)
{
    tr_raft_node_id_t index;

    for (index = 1u; index <= TEST_NODE_COUNT; ++index) {
        tr_raft_status_t status;

        if (tr_replay_driver_status(driver, index, &status) == TURBO_OK &&
            status.role == TR_RAFT_LEADER) {
            return index;
        }
    }
    return 0u;
}

spec("text replay core driver") {
  it("elects a leader and applies a submitted command") {
    tr_replay_driver_t *driver = NULL;
    tr_replay_driver_test_state_t state = {0};
    tr_text_replay_action_t action;
    tr_raft_node_id_t leader;
    tr_raft_status_t status;
    tr_raft_node_id_t index;

    check_equal(tr_replay_driver_test_create(&driver, &state), TURBO_OK);
    check_equal(tr_replay_driver_test_node_action(driver, 1u), TURBO_OK);
    check_equal(tr_replay_driver_test_node_action(driver, 2u), TURBO_OK);
    check_equal(tr_replay_driver_test_node_action(driver, 3u), TURBO_OK);
    check_equal(tr_replay_driver_test_node_action(driver, 9u),
                 TURBO_ENOENT);

    check_equal(tr_replay_driver_test_tick(driver, 30u), TURBO_OK);
    leader = tr_replay_driver_test_leader(driver);
    check_true(leader != 0u);

    action = tr_replay_driver_test_action();
    action.kind = TR_TEXT_REPLAY_SUBMIT;
    action.request_id = 7u;
    action.node_id = leader;
    action.client_id = 1u;
    action.sequence = 1u;
    action.payload_hex.data = "0x0102a0ff";
    action.payload_hex.len = 10u;
    check_equal(tr_replay_driver_step(driver, &action), TURBO_OK);

    action = tr_replay_driver_test_action();
    action.kind = TR_TEXT_REPLAY_POLL;
    action.request_id = 7u;
    action.poll_target = TR_TEXT_REPLAY_POLL_APPLIED;
    action.timeout_ticks = 40u;
    check_equal(tr_replay_driver_step(driver, &action), TURBO_OK);

    /* Let the lagging follower catch up before asserting every commit. */
    check_equal(tr_replay_driver_test_tick(driver, 5u), TURBO_OK);
    for (index = 1u; index <= TEST_NODE_COUNT; ++index) {
        check_equal(tr_replay_driver_test_expect_commit(driver, index, 1u),
                     TURBO_OK);
    }
    check_equal(tr_replay_driver_status(driver, leader, &status),
                 TURBO_OK);
    check_true(status.applied_index >= 1u);
    check_true(state.applied_entries >= 1u);
    check_true(state.apply_calls >= 1u);
    tr_replay_driver_destroy(driver);
  }

  it("steps down without quorum and recovers after heal") {
    tr_replay_driver_t *driver = NULL;
    tr_replay_driver_test_state_t state = {0};
    tr_text_replay_action_t action;
    tr_raft_node_id_t leader;

    check_equal(tr_replay_driver_test_create(&driver, &state), TURBO_OK);
    check_equal(tr_replay_driver_test_tick(driver, 30u), TURBO_OK);
    leader = tr_replay_driver_test_leader(driver);
    check_true(leader != 0u);

    action = tr_replay_driver_test_action();
    action.kind = TR_TEXT_REPLAY_PARTITION;
    action.node_id = leader;
    action.peer_id = leader == 1u ? 2u : 1u;
    check_equal(tr_replay_driver_step(driver, &action), TURBO_OK);
    action.node_id = leader;
    action.peer_id = leader == 3u ? 2u : 3u;
    check_equal(tr_replay_driver_step(driver, &action), TURBO_OK);

    check_equal(tr_replay_driver_test_tick(driver, 12u), TURBO_OK);
    {
        tr_raft_status_t status;

        check_equal(tr_replay_driver_status(driver, leader, &status),
                     TURBO_OK);
        check_true(status.role != TR_RAFT_LEADER);
    }

    action = tr_replay_driver_test_action();
    action.kind = TR_TEXT_REPLAY_HEAL;
    action.node_id = leader;
    action.peer_id = leader == 1u ? 2u : 1u;
    check_equal(tr_replay_driver_step(driver, &action), TURBO_OK);
    action.node_id = leader;
    action.peer_id = leader == 3u ? 2u : 3u;
    check_equal(tr_replay_driver_step(driver, &action), TURBO_OK);

    check_equal(tr_replay_driver_test_tick(driver, 30u), TURBO_OK);
    check_true(tr_replay_driver_test_leader(driver) != 0u);
    tr_replay_driver_destroy(driver);
  }

  it("rejects unknown message kinds, bad targets, and duplicate requests") {
    tr_replay_driver_t *driver = NULL;
    tr_replay_driver_test_state_t state = {0};
    tr_text_replay_action_t action;

    check_equal(tr_replay_driver_test_create(&driver, &state), TURBO_OK);

    action = tr_replay_driver_test_action();
    action.kind = TR_TEXT_REPLAY_DROP_NEXT;
    action.name.data = "not_a_message";
    action.name.len = strlen("not_a_message");
    check_equal(tr_replay_driver_step(driver, &action), TURBO_EINVAL);

    action = tr_replay_driver_test_action();
    action.kind = TR_TEXT_REPLAY_DELAY_NEXT;
    action.name.data = "append_request";
    action.name.len = strlen("append_request");
    action.value = 3u;
    check_equal(tr_replay_driver_step(driver, &action), TURBO_OK);

    action = tr_replay_driver_test_action();
    action.kind = TR_TEXT_REPLAY_DUPLICATE_NEXT;
    action.name.data = "vote_request";
    action.name.len = strlen("vote_request");
    check_equal(tr_replay_driver_step(driver, &action), TURBO_OK);

    action = tr_replay_driver_test_action();
    action.kind = TR_TEXT_REPLAY_POLL;
    action.request_id = 99u;
    action.poll_target = TR_TEXT_REPLAY_POLL_ACCEPTED;
    action.timeout_ticks = 1u;
    check_equal(tr_replay_driver_step(driver, &action), TURBO_ENOENT);

    check_equal(tr_replay_driver_test_tick(driver, 30u), TURBO_OK);
    {
        tr_raft_node_id_t leader = tr_replay_driver_test_leader(driver);

        check_true(leader != 0u);
        action = tr_replay_driver_test_action();
        action.kind = TR_TEXT_REPLAY_SUBMIT;
        action.request_id = 5u;
        action.node_id = leader;
        action.client_id = 1u;
        action.sequence = 1u;
        action.payload_hex.data = "0x01";
        action.payload_hex.len = 4u;
        check_equal(tr_replay_driver_step(driver, &action), TURBO_OK);
        check_equal(tr_replay_driver_step(driver, &action), TURBO_EALREADY);
    }
    tr_replay_driver_destroy(driver);
  }

  it("rejects expectation failures and invalid ticks") {
    tr_replay_driver_t *driver = NULL;
    tr_replay_driver_test_state_t state = {0};
    tr_text_replay_action_t action;

    check_equal(tr_replay_driver_test_create(&driver, &state), TURBO_OK);
    check_equal(tr_replay_driver_test_tick(driver, 30u), TURBO_OK);

    check_equal(tr_replay_driver_test_expect_role(driver, 1u, "leader") ==
                     TURBO_OK ||
                 tr_replay_driver_test_expect_role(driver, 2u, "leader") ==
                     TURBO_OK ||
                 tr_replay_driver_test_expect_role(driver, 3u, "leader") ==
                     TURBO_OK,
                 1);

    action = tr_replay_driver_test_action();
    action.kind = TR_TEXT_REPLAY_EXPECT_ROLE;
    action.node_id = 1u;
    action.comparison = TR_TEXT_REPLAY_COMPARE_EQ;
    action.name.data = "bogus_role";
    action.name.len = strlen("bogus_role");
    check_equal(tr_replay_driver_step(driver, &action), TURBO_EINVAL);

    action = tr_replay_driver_test_action();
    action.kind = TR_TEXT_REPLAY_EXPECT_COMMIT_INDEX;
    action.node_id = 1u;
    action.comparison = TR_TEXT_REPLAY_COMPARE_GE;
    action.value = UINT64_MAX;
    check_equal(tr_replay_driver_step(driver, &action), TURBO_EPROTO);

    action = tr_replay_driver_test_action();
    action.kind = TR_TEXT_REPLAY_EXPECT_ROLE;
    action.node_id = 42u;
    action.comparison = TR_TEXT_REPLAY_COMPARE_EQ;
    action.name.data = "follower";
    action.name.len = strlen("follower");
    check_equal(tr_replay_driver_step(driver, &action), TURBO_ENOENT);

    action = tr_replay_driver_test_action();
    action.kind = TR_TEXT_REPLAY_TICK;
    action.value = 0u;
    check_equal(tr_replay_driver_step(driver, &action), TURBO_ERANGE);

    tr_replay_driver_destroy(driver);
  }

  it("runs a full parsed replay plan including send") {
    static const char plan_text[] =
        "node 1;\n"
        "node 2;\n"
        "node 3;\n"
        "tick 30;\n"
        "send node 1 -> node 2;\n"
        "tick 5;\n";
    tr_replay_driver_t *driver = NULL;
    tr_replay_driver_test_state_t state = {0};
    tr_text_replay_plan_t plan;
    tr_text_diagnostic_t diagnostic;
    tr_raft_status_t status;

    check_equal(tr_replay_driver_test_create(&driver, &state), TURBO_OK);
    check_equal(tr_text_replay_parse(plan_text, strlen(plan_text), NULL,
                                      &plan, &diagnostic),
                 TURBO_OK);
    check_equal(tr_replay_driver_run(driver, &plan), TURBO_OK);
    check_equal(tr_replay_driver_status(driver, 2u, &status), TURBO_OK);
    check_true(status.leader_id != 0u);
    tr_replay_driver_destroy(driver);
  }
}
