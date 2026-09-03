#include <turboraft/raft_core.h>

#include <tinytest.h>
#include <salts_error.h>

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#define SIM_NODE_COUNT 3U
#define SIM_MAX_LOG_ENTRIES 64U
#define SIM_READY_MESSAGES (TR_RAFT_MAX_MEMBERS * 2U)
#define SIM_QUEUE_CAPACITY 256U
#define SIM_MAX_DELIVERIES 4096U
#define SIM_CHAOS_SEED_COUNT 32U
#define SIM_CHAOS_STEPS 96U
#define SIM_CHAOS_RECOVERY_ROUNDS 24U

typedef struct sim_durable_state {
    tr_raft_term_t term;
    tr_raft_node_id_t voted_for;
    tr_raft_index_t commit_index;
    tr_raft_index_t applied_index;
    tr_raft_entry_t entries[SIM_MAX_LOG_ENTRIES];
    size_t entry_count;
} sim_durable_state_t;

typedef struct sim_node {
    tr_raft_core_t *core;
    sim_durable_state_t durable;
    bool active;
} sim_node_t;

typedef struct sim_cluster {
    sim_node_t nodes[SIM_NODE_COUNT];
    bool links[SIM_NODE_COUNT][SIM_NODE_COUNT];
    tr_raft_message_t queue[SIM_QUEUE_CAPACITY];
    size_t queue_count;
    tr_raft_term_t observed_terms[SIM_NODE_COUNT];
    tr_raft_index_t observed_commits[SIM_NODE_COUNT];
    tr_raft_index_t observed_applied[SIM_NODE_COUNT];
    uint64_t random_state;
} sim_cluster_t;

static const tr_raft_node_id_t sim_voters[] = {1U, 2U, 3U};

static int sim_check_invariants(sim_cluster_t *cluster);

static bool sim_ready_has_effects(const tr_raft_ready_t *ready)
{
    return ready->message_count != 0U || ready->hard_state_changed ||
           ready->role_changed || ready->log_changed ||
           ready->commit_changed || ready->committed_entry_count != 0U ||
           ready->read_state_ready;
}

static tr_raft_ready_t sim_ready(tr_raft_message_t *messages)
{
    tr_raft_ready_t ready;

    memset(&ready, 0, sizeof(ready));
    ready.messages = messages;
    ready.message_capacity = SIM_READY_MESSAGES;
    return ready;
}

static int sim_node_create(sim_node_t *node, tr_raft_node_id_t node_id)
{
    tr_raft_core_config_t config;

    memset(&config, 0, sizeof(config));
    config.self_id = node_id;
    config.voters = sim_voters;
    config.voter_count = SIM_NODE_COUNT;
    config.heartbeat_ticks = 1U;
    config.election_min_ticks = 3U;
    config.election_max_ticks = 6U;
    config.initial_election_timeout_ticks = (uint32_t)(node_id + 2U);
    config.initial_term = node->durable.term;
    config.initial_vote = node->durable.voted_for;
    config.initial_log_entries = node->durable.entry_count == 0U
                                     ? NULL
                                     : node->durable.entries;
    config.initial_log_entry_count = node->durable.entry_count;
    config.initial_commit_index = node->durable.commit_index;
    config.initial_applied_index = node->durable.applied_index;
    config.max_log_entries = SIM_MAX_LOG_ENTRIES;
    return tr_raft_core_create(&config, &node->core);
}

static int sim_cluster_create(sim_cluster_t *cluster)
{
    size_t from;
    size_t to;

    memset(cluster, 0, sizeof(*cluster));
    for (from = 0U; from < SIM_NODE_COUNT; ++from) {
        for (to = 0U; to < SIM_NODE_COUNT; ++to) {
            cluster->links[from][to] = true;
        }
        if (sim_node_create(&cluster->nodes[from],
                            (tr_raft_node_id_t)(from + 1U)) != SALTS_OK) {
            while (from > 0U) {
                --from;
                tr_raft_core_destroy(cluster->nodes[from].core);
            }
            return SALTS_EPROTO;
        }
        cluster->nodes[from].active = true;
    }
    return SALTS_OK;
}

static void sim_cluster_destroy(sim_cluster_t *cluster)
{
    size_t index;

    for (index = 0U; index < SIM_NODE_COUNT; ++index) {
        tr_raft_core_destroy(cluster->nodes[index].core);
        cluster->nodes[index].core = NULL;
        cluster->nodes[index].active = false;
    }
}

static sim_cluster_t *sim_cluster_allocate(void)
{
    sim_cluster_t *cluster =
        (sim_cluster_t *)calloc(1U, sizeof(*cluster));

    if (cluster == NULL) {
        fputs("failed to allocate simulated raft cluster\n", stderr);
        exit(EXIT_FAILURE);
    }
    return cluster;
}

static int sim_persist_ready(sim_node_t *node, const tr_raft_ready_t *ready)
{
    size_t index;

    if (ready->hard_state_changed) {
        node->durable.term = ready->term;
        node->durable.voted_for = ready->voted_for;
    }
    if (ready->log_changed && ready->log_truncate_from != 0U) {
        if (ready->log_truncate_from > node->durable.entry_count + 1U) {
            return SALTS_EPROTO;
        }
        node->durable.entry_count = (size_t)ready->log_truncate_from - 1U;
    }
    for (index = 0U; index < ready->log_entry_count; ++index) {
        const tr_raft_entry_t *entry = &ready->log_entries[index];

        if (entry->index == 0U || entry->index > SIM_MAX_LOG_ENTRIES) {
            return SALTS_ERANGE;
        }
        if (entry->index > node->durable.entry_count + 1U) {
            return SALTS_EPROTO;
        }
        node->durable.entries[entry->index - 1U] = *entry;
        if (entry->index > node->durable.entry_count) {
            node->durable.entry_count = (size_t)entry->index;
        }
    }
    if (ready->commit_changed) {
        if (ready->commit_index > node->durable.entry_count) {
            return SALTS_EPROTO;
        }
        node->durable.commit_index = ready->commit_index;
    }
    if (ready->committed_entry_count != 0U) {
        tr_raft_index_t applied =
            ready->committed_entries[ready->committed_entry_count - 1U].index;

        if (applied > node->durable.commit_index) {
            return SALTS_EPROTO;
        }
        node->durable.applied_index = applied;
    }
    return SALTS_OK;
}

static int sim_finish_ready(sim_cluster_t *cluster,
                            size_t node_index,
                            const tr_raft_ready_t *ready)
{
    size_t index;
    int result = sim_persist_ready(&cluster->nodes[node_index], ready);

    if (result != SALTS_OK) {
        return result;
    }
    if (ready->message_count > SIM_QUEUE_CAPACITY - cluster->queue_count) {
        return SALTS_ENOSPC;
    }
    for (index = 0U; index < ready->message_count; ++index) {
        cluster->queue[cluster->queue_count++] = ready->messages[index];
    }
    if (sim_ready_has_effects(ready)) {
        result = tr_raft_core_advance(cluster->nodes[node_index].core);
        if (result != SALTS_OK) {
            return result;
        }
    }
    return sim_check_invariants(cluster);
}

static int sim_take_message(sim_cluster_t *cluster,
                            size_t index,
                            tr_raft_message_t *out_message)
{
    if (cluster == NULL || out_message == NULL ||
        index >= cluster->queue_count) {
        return SALTS_EINVAL;
    }
    *out_message = cluster->queue[index];
    if (index + 1U < cluster->queue_count) {
        memmove(&cluster->queue[index], &cluster->queue[index + 1U],
                (cluster->queue_count - index - 1U) *
                    sizeof(cluster->queue[0]));
    }
    --cluster->queue_count;
    return SALTS_OK;
}

static int sim_deliver(sim_cluster_t *cluster,
                       const tr_raft_message_t *message)
{
    tr_raft_message_t emitted[SIM_READY_MESSAGES];
    tr_raft_ready_t ready;
    size_t from;
    size_t to;
    int result;

    if (message == NULL || message->from == 0U ||
        message->from > SIM_NODE_COUNT || message->to == 0U ||
        message->to > SIM_NODE_COUNT) {
        return SALTS_EPROTO;
    }
    from = (size_t)message->from - 1U;
    to = (size_t)message->to - 1U;
    if (!cluster->nodes[from].active || !cluster->nodes[to].active ||
        !cluster->links[from][to]) {
        return sim_check_invariants(cluster);
    }
    ready = sim_ready(emitted);
    result = tr_raft_core_step(cluster->nodes[to].core, message, &ready);
    return result == SALTS_OK ? sim_finish_ready(cluster, to, &ready) : result;
}

static int sim_pump(sim_cluster_t *cluster)
{
    size_t deliveries = 0U;

    while (cluster->queue_count != 0U) {
        tr_raft_message_t message;
        int result;

        if (++deliveries > SIM_MAX_DELIVERIES) {
            return SALTS_EPROTO;
        }
        result = sim_take_message(cluster, 0U, &message);
        if (result != SALTS_OK) {
            return result;
        }
        result = sim_deliver(cluster, &message);
        if (result != SALTS_OK) {
            return result;
        }
    }
    return SALTS_OK;
}

static uint32_t sim_random(sim_cluster_t *cluster)
{
    uint64_t value = cluster->random_state;

    value ^= value >> 12U;
    value ^= value << 25U;
    value ^= value >> 27U;
    cluster->random_state = value;
    return (uint32_t)((value * UINT64_C(2685821657736338717)) >> 32U);
}

static int sim_chaos_deliver_one(sim_cluster_t *cluster)
{
    tr_raft_message_t message;
    uint32_t choice;
    int result;

    if (cluster->queue_count == 0U) {
        return sim_check_invariants(cluster);
    }
    choice = sim_random(cluster);
    result = sim_take_message(cluster, choice % cluster->queue_count,
                              &message);
    if (result != SALTS_OK || choice % 8U == 0U) {
        return result == SALTS_OK ? sim_check_invariants(cluster) : result;
    }
    result = sim_deliver(cluster, &message);
    if (result != SALTS_OK) {
        return result;
    }
    return choice % 8U == 1U ? sim_deliver(cluster, &message) : SALTS_OK;
}

static int sim_tick_enqueue(sim_cluster_t *cluster,
                            tr_raft_node_id_t node_id,
                            uint32_t elapsed_ticks)
{
    tr_raft_message_t messages[SIM_READY_MESSAGES];
    tr_raft_ready_t ready = sim_ready(messages);
    tr_raft_tick_t tick = {elapsed_ticks, 4U};
    size_t node_index = (size_t)node_id - 1U;
    int result;

    if (node_id == 0U || node_id > SIM_NODE_COUNT ||
        !cluster->nodes[node_index].active) {
        return SALTS_EINVAL;
    }
    result = tr_raft_core_tick(cluster->nodes[node_index].core, &tick, &ready);
    if (result != SALTS_OK) {
        return result;
    }
    return sim_finish_ready(cluster, node_index, &ready);
}

static int sim_tick(sim_cluster_t *cluster,
                    tr_raft_node_id_t node_id,
                    uint32_t elapsed_ticks)
{
    int result = sim_tick_enqueue(cluster, node_id, elapsed_ticks);

    return result == SALTS_OK ? sim_pump(cluster) : result;
}

static int sim_propose_enqueue(sim_cluster_t *cluster,
                               tr_raft_node_id_t node_id,
                               uint64_t command_id,
                               const char *data)
{
    tr_raft_message_t messages[SIM_READY_MESSAGES];
    tr_raft_ready_t ready = sim_ready(messages);
    tr_raft_proposal_t proposal = {command_id, data, strlen(data)};
    size_t node_index = (size_t)node_id - 1U;
    int result;

    result = tr_raft_core_propose(cluster->nodes[node_index].core, &proposal,
                                  &ready);
    if (result != SALTS_OK) {
        return result;
    }
    return sim_finish_ready(cluster, node_index, &ready);
}

static int sim_propose(sim_cluster_t *cluster,
                       tr_raft_node_id_t node_id,
                       uint64_t command_id,
                       const char *data)
{
    int result = sim_propose_enqueue(cluster, node_id, command_id, data);

    return result == SALTS_OK ? sim_pump(cluster) : result;
}

static void sim_partition(sim_cluster_t *cluster,
                          tr_raft_node_id_t left,
                          tr_raft_node_id_t right,
                          bool connected)
{
    cluster->links[left - 1U][right - 1U] = connected;
    cluster->links[right - 1U][left - 1U] = connected;
}

static int sim_status(sim_cluster_t *cluster,
                      tr_raft_node_id_t node_id,
                      tr_raft_status_t *status)
{
    return tr_raft_core_status(cluster->nodes[node_id - 1U].core, status);
}

static bool sim_entries_equal(const tr_raft_entry_t *left,
                              const tr_raft_entry_t *right)
{
    return left->index == right->index && left->term == right->term &&
           left->command_id == right->command_id &&
           left->data_length == right->data_length &&
           memcmp(left->data, right->data, left->data_length) == 0;
}

static int sim_check_invariants(sim_cluster_t *cluster)
{
    size_t left;
    size_t right;

    for (left = 0U; left < SIM_NODE_COUNT; ++left) {
        const sim_durable_state_t *durable = &cluster->nodes[left].durable;
        tr_raft_status_t status;

        if (durable->applied_index > durable->commit_index ||
            durable->commit_index > durable->entry_count ||
            durable->term < cluster->observed_terms[left] ||
            durable->commit_index < cluster->observed_commits[left] ||
            durable->applied_index < cluster->observed_applied[left]) {
            return SALTS_EPROTO;
        }
        cluster->observed_terms[left] = durable->term;
        cluster->observed_commits[left] = durable->commit_index;
        cluster->observed_applied[left] = durable->applied_index;
        if (!cluster->nodes[left].active) {
            continue;
        }
        if (tr_raft_core_status(cluster->nodes[left].core, &status) !=
                SALTS_OK ||
            status.term != durable->term ||
            status.commit_index != durable->commit_index ||
            status.applied_index != durable->applied_index) {
            return SALTS_EPROTO;
        }
        if (status.role != TR_RAFT_LEADER) {
            continue;
        }
        for (right = left + 1U; right < SIM_NODE_COUNT; ++right) {
            tr_raft_status_t other;

            if (cluster->nodes[right].active &&
                tr_raft_core_status(cluster->nodes[right].core, &other) ==
                    SALTS_OK &&
                other.role == TR_RAFT_LEADER && other.term == status.term) {
                return SALTS_EPROTO;
            }
        }
    }
    for (left = 0U; left < SIM_NODE_COUNT; ++left) {
        for (right = left + 1U; right < SIM_NODE_COUNT; ++right) {
            tr_raft_index_t common_commit =
                cluster->nodes[left].durable.commit_index <
                        cluster->nodes[right].durable.commit_index
                    ? cluster->nodes[left].durable.commit_index
                    : cluster->nodes[right].durable.commit_index;
            tr_raft_index_t index;

            for (index = 1U; index <= common_commit; ++index) {
                if (!sim_entries_equal(
                        &cluster->nodes[left].durable.entries[index - 1U],
                        &cluster->nodes[right].durable.entries[index - 1U])) {
                    return SALTS_EPROTO;
                }
            }
        }
    }
    return SALTS_OK;
}

static tr_raft_node_id_t sim_find_highest_term_leader(sim_cluster_t *cluster)
{
    tr_raft_node_id_t leader_id = 0U;
    tr_raft_term_t leader_term = 0U;
    size_t index;

    for (index = 0U; index < SIM_NODE_COUNT; ++index) {
        tr_raft_status_t status;

        if (!cluster->nodes[index].active ||
            tr_raft_core_status(cluster->nodes[index].core, &status) !=
                SALTS_OK ||
            status.role != TR_RAFT_LEADER || status.term < leader_term) {
            continue;
        }
        leader_id = (tr_raft_node_id_t)(index + 1U);
        leader_term = status.term;
    }
    return leader_id;
}

static int sim_crash(sim_cluster_t *cluster, tr_raft_node_id_t node_id)
{
    sim_node_t *node;

    if (node_id == 0U || node_id > SIM_NODE_COUNT) {
        return SALTS_EINVAL;
    }
    node = &cluster->nodes[node_id - 1U];
    if (!node->active) {
        return SALTS_EALREADY;
    }
    tr_raft_core_destroy(node->core);
    node->core = NULL;
    node->active = false;
    return SALTS_OK;
}

static int sim_restart(sim_cluster_t *cluster, tr_raft_node_id_t node_id)
{
    sim_node_t *node;
    int result;

    if (node_id == 0U || node_id > SIM_NODE_COUNT) {
        return SALTS_EINVAL;
    }
    node = &cluster->nodes[node_id - 1U];
    if (node->active) {
        return SALTS_EALREADY;
    }
    result = sim_node_create(node, node_id);
    if (result == SALTS_OK) {
        node->active = true;
    }
    return result;
}

spec("raft deterministic cluster simulation")
{
    it("elects a majority leader and converges after a minority partition")
    {
        sim_cluster_t *cluster = sim_cluster_allocate();
        tr_raft_status_t first;
        tr_raft_status_t second;
        tr_raft_status_t third;

        check_equal(sim_cluster_create(cluster), SALTS_OK);
        check_equal(sim_tick(cluster, 1U, 4U), SALTS_OK);
        check_equal(sim_status(cluster, 1U, &first), SALTS_OK);
        check_equal(first.role, TR_RAFT_LEADER);

        sim_partition(cluster, 1U, 2U, false);
        sim_partition(cluster, 1U, 3U, false);
        check_equal(sim_tick(cluster, 1U, 4U), SALTS_OK);
        check_equal(sim_tick(cluster, 1U, 4U), SALTS_OK);
        check_equal(sim_status(cluster, 1U, &first), SALTS_OK);
        check_equal(first.role, TR_RAFT_FOLLOWER);
        check_equal(sim_tick(cluster, 2U, 4U), SALTS_OK);
        check_equal(sim_status(cluster, 2U, &second), SALTS_OK);
        check_equal(second.role, TR_RAFT_LEADER);
        check(second.term > first.term);

        check_equal(sim_propose(cluster, 2U, 41U, "partition-value"),
                     SALTS_OK);
        check_equal(sim_tick(cluster, 2U, 1U), SALTS_OK);
        check_equal(sim_status(cluster, 2U, &second), SALTS_OK);
        check_equal(sim_status(cluster, 3U, &third), SALTS_OK);
        check_equal(second.commit_index, 1U);
        check_equal(third.commit_index, 1U);

        sim_partition(cluster, 1U, 2U, true);
        sim_partition(cluster, 1U, 3U, true);
        check_equal(sim_tick(cluster, 2U, 1U), SALTS_OK);
        check_equal(sim_tick(cluster, 2U, 1U), SALTS_OK);
        check_equal(sim_status(cluster, 1U, &first), SALTS_OK);
        check_equal(first.leader_id, 2U);
        check_equal(first.last_log_index, 1U);
        check_equal(first.commit_index, 1U);
        check_equal(first.term, second.term);
        sim_cluster_destroy(cluster);
        free(cluster);
    }

    it("restarts a follower from durable Ready state and catches it up")
    {
        sim_cluster_t *cluster = sim_cluster_allocate();
        tr_raft_status_t leader;
        tr_raft_status_t restarted;
        size_t attempt;

        check_equal(sim_cluster_create(cluster), SALTS_OK);
        check_equal(sim_tick(cluster, 1U, 3U), SALTS_OK);
        check_equal(sim_propose(cluster, 1U, 51U, "before-crash"),
                     SALTS_OK);
        check_equal(sim_tick(cluster, 1U, 1U), SALTS_OK);
        check_equal(sim_crash(cluster, 3U), SALTS_OK);
        check_equal(sim_propose(cluster, 1U, 52U, "during-crash"),
                     SALTS_OK);
        check_equal(sim_tick(cluster, 1U, 1U), SALTS_OK);
        check_equal(sim_status(cluster, 1U, &leader), SALTS_OK);
        check_equal(leader.commit_index, 2U);

        check_equal(sim_restart(cluster, 3U), SALTS_OK);
        for (attempt = 0U; attempt < 4U; ++attempt) {
            check_equal(sim_tick(cluster, 1U, 1U), SALTS_OK);
        }
        check_equal(sim_status(cluster, 3U, &restarted), SALTS_OK);
        check_equal(restarted.leader_id, 1U);
        check_equal(restarted.last_log_index, 2U);
        check_equal(restarted.commit_index, 2U);
        check_equal(restarted.applied_index, 2U);
        sim_cluster_destroy(cluster);
        free(cluster);
    }

    it("preserves election and committed-log safety under seeded chaos")
    {
        uint64_t seed;

        for (seed = 1U; seed <= SIM_CHAOS_SEED_COUNT; ++seed) {
            sim_cluster_t *cluster = sim_cluster_allocate();
            size_t step;

            check_equal(sim_cluster_create(cluster), SALTS_OK);
            cluster->random_state = seed;
            check_equal(sim_tick(cluster, 1U, 4U), SALTS_OK);
            check_equal(sim_propose(cluster, 1U, seed, "baseline"),
                         SALTS_OK);

            for (step = 0U; step < SIM_CHAOS_STEPS; ++step) {
                uint32_t choice = sim_random(cluster);
                tr_raft_node_id_t node_id =
                    (tr_raft_node_id_t)(choice % SIM_NODE_COUNT + 1U);
                uint32_t action = (choice / SIM_NODE_COUNT) % 8U;
                int result = SALTS_OK;

                if (action <= 2U &&
                    cluster->nodes[node_id - 1U].active) {
                    result = sim_tick_enqueue(cluster, node_id, 1U);
                } else if (action == 3U) {
                    tr_raft_node_id_t peer_id =
                        (tr_raft_node_id_t)(node_id % SIM_NODE_COUNT + 1U);
                    bool connected =
                        !cluster->links[node_id - 1U][peer_id - 1U];

                    sim_partition(cluster, node_id, peer_id, connected);
                } else if (action == 4U) {
                    tr_raft_node_id_t leader_id =
                        sim_find_highest_term_leader(cluster);

                    if (leader_id != 0U) {
                        result = sim_propose_enqueue(
                            cluster, leader_id,
                            seed * SIM_CHAOS_STEPS + step + 1U, "chaos");
                    }
                } else if (action == 5U &&
                           cluster->nodes[node_id - 1U].active) {
                    size_t active_count = 0U;
                    size_t index;

                    for (index = 0U; index < SIM_NODE_COUNT; ++index) {
                        active_count += cluster->nodes[index].active ? 1U : 0U;
                    }
                    if (active_count > 2U) {
                        result = sim_crash(cluster, node_id);
                    }
                } else if (action == 6U &&
                           !cluster->nodes[node_id - 1U].active) {
                    result = sim_restart(cluster, node_id);
                }
                check_equal(result, SALTS_OK);
                check_equal(sim_chaos_deliver_one(cluster), SALTS_OK);
                check_equal(sim_check_invariants(cluster), SALTS_OK);
            }

            for (step = 0U; step < SIM_NODE_COUNT; ++step) {
                if (!cluster->nodes[step].active) {
                    check_equal(sim_restart(
                                     cluster,
                                     (tr_raft_node_id_t)(step + 1U)),
                                 SALTS_OK);
                }
            }
            for (step = 0U; step < SIM_NODE_COUNT; ++step) {
                size_t peer;

                for (peer = step + 1U; peer < SIM_NODE_COUNT; ++peer) {
                    sim_partition(cluster,
                                  (tr_raft_node_id_t)(step + 1U),
                                  (tr_raft_node_id_t)(peer + 1U), true);
                }
            }
            check_equal(sim_pump(cluster), SALTS_OK);
            for (step = 0U; step < SIM_CHAOS_RECOVERY_ROUNDS; ++step) {
                tr_raft_node_id_t node_id =
                    (tr_raft_node_id_t)(step % SIM_NODE_COUNT + 1U);

                check_equal(sim_tick(cluster, node_id, 1U), SALTS_OK);
            }
            check_not_equal(sim_find_highest_term_leader(cluster), 0U);
            check_equal(sim_check_invariants(cluster), SALTS_OK);
            sim_cluster_destroy(cluster);
            free(cluster);
        }
    }
}
