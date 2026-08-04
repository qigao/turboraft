/*
 * TurboRaft DSL embedding demo.
 *
 * Shows how an application embeds the two most useful DSL surfaces:
 *   1. Query DSL    - parse read-only inspection commands and dispatch them
 *                     through tr_text_query_execute() callbacks.
 *   2. Replay DSL   - drive a deterministic 3-node tr_raft_core_t cluster
 *                     through the native core driver (TurboRaft::ReplayDriver):
 *                     elect a leader, submit, poll until applied, then assert
 *                     every node's commit index.
 *
 * See docs/DSL_USAGE.md for the full usage guide.
 *
 * Build (in-tree):
 *   cmake --preset win-release-user -DBUILD_EXAMPLES=ON
 *   cmake --build --preset win-release-user --target turboraft_dsl_embed_demo
 *   build/msvc-release/bin/turboraft_dsl_embed_demo
 */
#include <turboraft/raft_core.h>
#include <turboraft/text_query_executor.h>
#include <turboraft/text_replay_core_driver.h>
#include <turboraft/text_syntax.h>

#include <stdio.h>
#include <string.h>

#define DEMO_NODE_COUNT 3u

/* ------------------------------------------------------------------ */
/* Query DSL callbacks (generic executor, no service required).       */
/* ------------------------------------------------------------------ */

static int demo_query_status(void *context,
                             const tr_text_query_command_t *command)
{
    (void)context;
    (void)command;
    printf("    status -> (would call raft.status)\n");
    return TURBO_OK;
}

static int demo_query_members(void *context,
                              const tr_text_query_command_t *command)
{
    (void)context;
    printf("    members -> role=%s\n",
           command->role == TR_TEXT_QUERY_ROLE_VOTER
               ? "voter"
               : command->role == TR_TEXT_QUERY_ROLE_LEARNER ? "learner"
                                                             : "any");
    return TURBO_OK;
}

static int demo_query_progress(void *context,
                               const tr_text_query_command_t *command)
{
    (void)context;
    printf("    progress -> node=%llu\n",
           (unsigned long long)command->node_id);
    return TURBO_OK;
}

static int demo_query_example(void)
{
    static const char text[] =
        "show status;\n"
        "show members where role == voter;\n"
        "show progress for node 2;";
    tr_text_query_plan_t plan;
    tr_text_diagnostic_t diagnostic;
    tr_text_query_executor_ops_t ops = {
        .status = demo_query_status,
        .members = demo_query_members,
        .progress = demo_query_progress,
    };
    int result;

    printf("== Query DSL ==\n");
    result = tr_text_query_parse(text, strlen(text), NULL, &plan,
                                 &diagnostic);
    if (result != TURBO_OK) {
        printf("  parse failed at %zu:%zu: %s\n", diagnostic.line,
               diagnostic.column,
               diagnostic.message == NULL ? "invalid input"
                                          : diagnostic.message);
        return result;
    }
    printf("  parsed %zu command(s):\n", plan.command_count);
    return tr_text_query_execute(&plan, &ops, NULL);
}

/* ------------------------------------------------------------------ */
/* Replay DSL: deterministic cluster simulation via the core driver.  */
/* ------------------------------------------------------------------ */

static tr_text_replay_action_t demo_action(void)
{
    tr_text_replay_action_t action;

    memset(&action, 0, sizeof(action));
    return action;
}

static tr_raft_node_id_t demo_find_leader(tr_replay_driver_t *driver)
{
    tr_raft_node_id_t index;

    for (index = 1u; index <= DEMO_NODE_COUNT; ++index) {
        tr_raft_status_t status;

        if (tr_replay_driver_status(driver, index, &status) == TURBO_OK &&
            status.role == TR_RAFT_LEADER) {
            return index;
        }
    }
    return 0u;
}

static int demo_replay_example(void)
{
    static const tr_raft_node_id_t voters[DEMO_NODE_COUNT] = {1u, 2u, 3u};
    static const char setup_text[] =
        "node 1;\nnode 2;\nnode 3;\ntick 30;\n";
    tr_raft_core_config_t nodes[DEMO_NODE_COUNT];
    tr_replay_driver_config_t driver_config;
    tr_replay_driver_t *driver = NULL;
    tr_text_replay_plan_t plan;
    tr_text_diagnostic_t diagnostic;
    tr_text_replay_action_t action;
    tr_replay_driver_counters_t counters;
    tr_raft_node_id_t leader;
    size_t index;
    int result;

    printf("\n== Replay DSL ==\n");
    memset(nodes, 0, sizeof(nodes));
    for (index = 0u; index < DEMO_NODE_COUNT; ++index) {
        nodes[index].self_id = voters[index];
        nodes[index].voters = voters;
        nodes[index].voter_count = DEMO_NODE_COUNT;
        nodes[index].heartbeat_ticks = 1u;
        nodes[index].election_min_ticks = 3u;
        nodes[index].election_max_ticks = 8u;
        nodes[index].initial_election_timeout_ticks = 4u + (uint32_t)index;
        nodes[index].max_log_entries = 256u;
    }
    memset(&driver_config, 0, sizeof(driver_config));
    driver_config.nodes = nodes;
    driver_config.node_count = DEMO_NODE_COUNT;
    result = tr_replay_driver_create(&driver_config, &driver);
    if (result != TURBO_OK) {
        printf("  driver create failed: %d\n", result);
        return result;
    }

    result = tr_text_replay_parse(setup_text, strlen(setup_text), NULL,
                                  &plan, &diagnostic);
    if (result == TURBO_OK) {
        result = tr_replay_driver_run(driver, &plan);
    }
    leader = demo_find_leader(driver);
    if (result != TURBO_OK || leader == 0u) {
        printf("  setup failed: result=%d leader=%llu\n", result,
               (unsigned long long)leader);
        tr_replay_driver_destroy(driver);
        return result == TURBO_OK ? TURBO_EPROTO : result;
    }
    printf("  leader elected: node %llu\n", (unsigned long long)leader);

    action = demo_action();
    action.kind = TR_TEXT_REPLAY_SUBMIT;
    action.request_id = 7u;
    action.node_id = leader;
    action.client_id = 1u;
    action.sequence = 1u;
    action.payload_hex.data = "0x0102a0ff";
    action.payload_hex.len = 10u;
    result = tr_replay_driver_step(driver, &action);
    if (result != TURBO_OK) {
        printf("  submit failed: %d\n", result);
        tr_replay_driver_destroy(driver);
        return result;
    }
    printf("  submitted request 7 to node %llu\n",
           (unsigned long long)leader);

    action = demo_action();
    action.kind = TR_TEXT_REPLAY_POLL;
    action.request_id = 7u;
    action.poll_target = TR_TEXT_REPLAY_POLL_APPLIED;
    action.timeout_ticks = 40u;
    result = tr_replay_driver_step(driver, &action);
    printf("  poll applied -> %s (%d)\n",
           result == TURBO_OK ? "ok" : "failed", result);
    if (result != TURBO_OK) {
        tr_replay_driver_destroy(driver);
        return result;
    }

    action = demo_action();
    action.kind = TR_TEXT_REPLAY_TICK;
    action.value = 5u;
    result = tr_replay_driver_step(driver, &action);
    if (result == TURBO_OK) {
        for (index = 0u; index < DEMO_NODE_COUNT; ++index) {
            tr_text_replay_action_t expect = demo_action();

            expect.kind = TR_TEXT_REPLAY_EXPECT_COMMIT_INDEX;
            expect.node_id = voters[index];
            expect.comparison = TR_TEXT_REPLAY_COMPARE_GE;
            expect.value = 1u;
            result = tr_replay_driver_step(driver, &expect);
            printf("  expect node %llu commit >= 1 -> %s\n",
                   (unsigned long long)voters[index],
                   result == TURBO_OK ? "ok" : "failed");
            if (result != TURBO_OK) {
                break;
            }
        }
    }

    for (index = 0u; index < DEMO_NODE_COUNT; ++index) {
        tr_raft_status_t status;

        if (tr_replay_driver_status(driver, voters[index], &status) ==
            TURBO_OK) {
            printf("  node %llu role=%d term=%llu leader=%llu commit=%llu "
                   "applied=%llu last_log=%llu\n",
                   (unsigned long long)status.self_id, (int)status.role,
                   (unsigned long long)status.term,
                   (unsigned long long)status.leader_id,
                   (unsigned long long)status.commit_index,
                   (unsigned long long)status.applied_index,
                   (unsigned long long)status.last_log_index);
        }
    }
    tr_replay_driver_counters(driver, &counters);
    printf("  network: delivered=%zu dropped_filter=%zu "
           "dropped_partition=%zu duplicated=%zu\n",
           counters.delivered, counters.dropped_filter,
           counters.dropped_partition, counters.duplicated);

    tr_replay_driver_destroy(driver);
    return result;
}

int main(void)
{
    int result = demo_query_example();

    if (result != TURBO_OK) {
        fprintf(stderr, "query demo failed: %d\n", result);
        return 1;
    }
    result = demo_replay_example();
    if (result != TURBO_OK) {
        fprintf(stderr, "replay demo failed: %d\n", result);
        return 1;
    }
    return 0;
}
