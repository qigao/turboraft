#include <turboraft/text_replay_core_driver.h>

#include <turbo_error.h>

#include <limits.h>
#include <stdlib.h>
#include <string.h>

typedef enum tr_replay_filter_action {
    TR_REPLAY_FILTER_DROP = 0,
    TR_REPLAY_FILTER_DELAY,
    TR_REPLAY_FILTER_DUPLICATE
} tr_replay_filter_action_t;

typedef struct tr_replay_filter {
    bool active;
    tr_raft_message_type_t kind;
    tr_replay_filter_action_t action;
    uint32_t delay_ticks;
} tr_replay_filter_t;

typedef struct tr_replay_queued {
    tr_raft_message_t message;
    uint32_t delay_ticks;
} tr_replay_queued_t;

typedef struct tr_replay_receipt {
    bool occupied;
    uint64_t request_id;
    tr_raft_node_id_t node_id;
    tr_raft_term_t term;
    tr_raft_index_t index;
} tr_replay_receipt_t;

typedef struct tr_replay_node {
    tr_raft_core_t *core;
    tr_raft_core_config_t core_config;
    bool active;
    tr_raft_term_t durable_term;
    tr_raft_node_id_t durable_voted_for;
    tr_raft_index_t durable_commit_index;
    tr_raft_index_t durable_applied_index;
    tr_raft_ready_t ready;
    tr_raft_message_t ready_messages[TR_REPLAY_DRIVER_MAX_NODES];
} tr_replay_node_t;

struct tr_replay_driver {
    tr_replay_node_t nodes[TR_REPLAY_DRIVER_MAX_NODES];
    size_t node_count;
    bool links[TR_REPLAY_DRIVER_MAX_NODES][TR_REPLAY_DRIVER_MAX_NODES];
    tr_replay_queued_t *queue;
    size_t queue_count;
    size_t queue_capacity;
    size_t max_deliveries;
    tr_replay_filter_t filters[TR_REPLAY_DRIVER_MAX_FILTERS];
    size_t filter_count;
    tr_replay_receipt_t receipts[TR_TEXT_MAX_STATEMENTS];
    tr_replay_driver_apply_fn apply;
    void *apply_context;
    tr_replay_driver_timeout_fn next_election_timeout;
    void *timeout_context;
    uint32_t default_election_timeout_ticks;
    tr_replay_driver_counters_t counters;
};

typedef struct tr_replay_message_name {
    const char *name;
    size_t length;
    tr_raft_message_type_t type;
} tr_replay_message_name_t;

/* Mirrors the message name table used by the protocol-debug executor. */
static const tr_replay_message_name_t tr_replay_message_names[] = {
    {"pre_vote_request", sizeof("pre_vote_request") - 1u,
     TR_RAFT_MSG_PRE_VOTE_REQUEST},
    {"pre_vote_response", sizeof("pre_vote_response") - 1u,
     TR_RAFT_MSG_PRE_VOTE_RESPONSE},
    {"vote_request", sizeof("vote_request") - 1u,
     TR_RAFT_MSG_VOTE_REQUEST},
    {"vote_response", sizeof("vote_response") - 1u,
     TR_RAFT_MSG_VOTE_RESPONSE},
    {"heartbeat_request", sizeof("heartbeat_request") - 1u,
     TR_RAFT_MSG_HEARTBEAT_REQUEST},
    {"heartbeat_response", sizeof("heartbeat_response") - 1u,
     TR_RAFT_MSG_HEARTBEAT_RESPONSE},
    {"append_request", sizeof("append_request") - 1u,
     TR_RAFT_MSG_APPEND_REQUEST},
    {"append_response", sizeof("append_response") - 1u,
     TR_RAFT_MSG_APPEND_RESPONSE},
    {"timeout_now", sizeof("timeout_now") - 1u,
     TR_RAFT_MSG_TIMEOUT_NOW},
    {"read_index_request", sizeof("read_index_request") - 1u,
     TR_RAFT_MSG_READ_INDEX_REQUEST},
    {"read_index_response", sizeof("read_index_response") - 1u,
     TR_RAFT_MSG_READ_INDEX_RESPONSE},
};

static bool tr_replay_name_equals(vstr value, const char *literal)
{
    size_t literal_length;

    if (value.data == NULL || literal == NULL) {
        return false;
    }
    literal_length = strlen(literal);
    return value.len == literal_length &&
           memcmp(value.data, literal, literal_length) == 0;
}

static int tr_replay_message_type(vstr name,
                                  tr_raft_message_type_t *out_type)
{
    size_t index;

    for (index = 0U;
         index < sizeof(tr_replay_message_names) /
                     sizeof(tr_replay_message_names[0]);
         ++index) {
        const tr_replay_message_name_t *entry =
            &tr_replay_message_names[index];

        if (tr_replay_name_equals(name, entry->name)) {
            *out_type = entry->type;
            return TURBO_OK;
        }
    }
    return TURBO_EINVAL;
}

static int tr_replay_role(vstr name, tr_raft_role_t *out_role)
{
    if (tr_replay_name_equals(name, "follower")) {
        *out_role = TR_RAFT_FOLLOWER;
        return TURBO_OK;
    }
    if (tr_replay_name_equals(name, "pre-candidate") ||
        tr_replay_name_equals(name, "pre_candidate")) {
        *out_role = TR_RAFT_PRE_CANDIDATE;
        return TURBO_OK;
    }
    if (tr_replay_name_equals(name, "candidate")) {
        *out_role = TR_RAFT_CANDIDATE;
        return TURBO_OK;
    }
    if (tr_replay_name_equals(name, "leader")) {
        *out_role = TR_RAFT_LEADER;
        return TURBO_OK;
    }
    return TURBO_EINVAL;
}

static int tr_replay_hex_value(char value)
{
    if (value >= '0' && value <= '9') {
        return (int)(value - '0');
    }
    if (value >= 'a' && value <= 'f') {
        return (int)(value - 'a') + 10;
    }
    if (value >= 'A' && value <= 'F') {
        return (int)(value - 'A') + 10;
    }
    return -1;
}

static int tr_replay_decode_hex(vstr input,
                                uint8_t *output,
                                size_t output_capacity,
                                size_t *output_length)
{
    size_t hex_digits;
    size_t decoded_length;
    size_t index;

    if (input.data == NULL || output == NULL || output_length == NULL ||
        input.len < 3u || input.data[0] != '0' ||
        (input.data[1] != 'x' && input.data[1] != 'X')) {
        return TURBO_EPROTO;
    }
    hex_digits = input.len - 2u;
    if ((hex_digits & 1u) != 0u) {
        return TURBO_EPROTO;
    }
    decoded_length = hex_digits / 2u;
    if (decoded_length > output_capacity) {
        return TURBO_ENOSPC;
    }
    for (index = 0u; index < decoded_length; ++index) {
        int high = tr_replay_hex_value(input.data[2u + index * 2u]);
        int low = tr_replay_hex_value(input.data[3u + index * 2u]);

        if (high < 0 || low < 0) {
            return TURBO_EPROTO;
        }
        output[index] = (uint8_t)((high << 4) | low);
    }
    *output_length = decoded_length;
    return TURBO_OK;
}

static int tr_replay_find_node(const tr_replay_driver_t *driver,
                               tr_raft_node_id_t node_id)
{
    size_t index;

    for (index = 0U; index < driver->node_count; ++index) {
        if (driver->nodes[index].active &&
            driver->nodes[index].core_config.self_id == node_id) {
            return (int)index;
        }
    }
    return -1;
}

static bool tr_replay_ready_has_effects(const tr_raft_ready_t *ready)
{
    return ready->message_count != 0u || ready->hard_state_changed ||
           ready->role_changed || ready->log_changed ||
           ready->commit_changed || ready->committed_entry_count != 0u ||
           ready->read_state_ready || ready->snapshot_request_count != 0u;
}

static int tr_replay_persist_ready(tr_replay_driver_t *driver,
                                   tr_replay_node_t *node,
                                   const tr_raft_ready_t *ready)
{
    tr_raft_index_t applied;
    int result;

    if (ready->hard_state_changed) {
        node->durable_term = ready->term;
        node->durable_voted_for = ready->voted_for;
    }
    if (ready->commit_changed) {
        node->durable_commit_index = ready->commit_index;
    }
    if (ready->committed_entry_count == 0u) {
        return TURBO_OK;
    }
    applied = ready->committed_entries[ready->committed_entry_count - 1u].index;
    if (applied > node->durable_commit_index) {
        return TURBO_EPROTO;
    }
    node->durable_applied_index = applied;
    if (driver->apply == NULL) {
        return TURBO_OK;
    }
    result = driver->apply(driver->apply_context, node->core_config.self_id,
                           ready->committed_entries,
                           ready->committed_entry_count);
    return result;
}

static int tr_replay_enqueue(tr_replay_driver_t *driver,
                             const tr_raft_message_t *message,
                             uint32_t delay_ticks)
{
    size_t index;

    for (index = 0U; index < driver->filter_count; ++index) {
        tr_replay_filter_t *filter = &driver->filters[index];

        if (!filter->active || filter->kind != message->type) {
            continue;
        }
        filter->active = false;
        if (filter->action == TR_REPLAY_FILTER_DROP) {
            ++driver->counters.dropped_filter;
            return TURBO_OK;
        }
        if (filter->action == TR_REPLAY_FILTER_DELAY) {
            delay_ticks = filter->delay_ticks;
            break;
        }
        /* DUPLICATE: enqueue the message twice. */
        if (driver->queue_count + 2u > driver->queue_capacity) {
            return TURBO_ENOSPC;
        }
        driver->queue[driver->queue_count++] =
            (tr_replay_queued_t){*message, delay_ticks};
        driver->queue[driver->queue_count++] =
            (tr_replay_queued_t){*message, delay_ticks};
        ++driver->counters.duplicated;
        return TURBO_OK;
    }
    if (driver->queue_count >= driver->queue_capacity) {
        return TURBO_ENOSPC;
    }
    driver->queue[driver->queue_count++] =
        (tr_replay_queued_t){*message, delay_ticks};
    return TURBO_OK;
}

static int tr_replay_process_ready(tr_replay_driver_t *driver,
                                   size_t node_index,
                                   const tr_raft_ready_t *ready)
{
    tr_replay_node_t *node = &driver->nodes[node_index];
    size_t index;
    int result = tr_replay_persist_ready(driver, node, ready);

    if (result != TURBO_OK) {
        return result;
    }
    for (index = 0u; index < ready->message_count; ++index) {
        result = tr_replay_enqueue(driver, &ready->messages[index], 0u);
        if (result != TURBO_OK) {
            return result;
        }
    }
    if (tr_replay_ready_has_effects(ready)) {
        result = tr_raft_core_advance(node->core);
        if (result != TURBO_OK) {
            return result;
        }
    }
    return TURBO_OK;
}

static int tr_replay_timeout_for(tr_replay_driver_t *driver,
                                 const tr_replay_node_t *node,
                                 uint32_t *out_ticks)
{
    int result;

    if (driver->next_election_timeout != NULL) {
        result = driver->next_election_timeout(driver->timeout_context,
                                               out_ticks);
        return result;
    }
    if (driver->default_election_timeout_ticks != 0u) {
        *out_ticks = driver->default_election_timeout_ticks;
        return TURBO_OK;
    }
    *out_ticks = node->core_config.initial_election_timeout_ticks;
    return *out_ticks == 0u ? TURBO_EINVAL : TURBO_OK;
}

static int tr_replay_pump(tr_replay_driver_t *driver)
{
    size_t deliveries = 0u;

    for (;;) {
        tr_replay_queued_t entry;
        tr_raft_message_t emitted[TR_REPLAY_DRIVER_MAX_NODES];
        tr_raft_ready_t ready;
        size_t due_index = SIZE_MAX;
        size_t index;
        int from_index;
        int to_index;
        int result;

        for (index = 0u; index < driver->queue_count; ++index) {
            if (driver->queue[index].delay_ticks == 0u) {
                due_index = index;
                break;
            }
        }
        if (due_index == SIZE_MAX) {
            return TURBO_OK;
        }
        if (++deliveries > driver->max_deliveries) {
            return TURBO_EPROTO;
        }
        entry = driver->queue[due_index];
        if (due_index + 1u < driver->queue_count) {
            memmove(&driver->queue[due_index], &driver->queue[due_index + 1u],
                    (driver->queue_count - due_index - 1u) *
                        sizeof(driver->queue[0]));
        }
        --driver->queue_count;

        to_index = tr_replay_find_node(driver, entry.message.to);
        from_index = tr_replay_find_node(driver, entry.message.from);
        if (to_index < 0 || from_index < 0) {
            continue;
        }
        if (!driver->links[from_index][to_index]) {
            ++driver->counters.dropped_partition;
            continue;
        }
        memset(&ready, 0, sizeof(ready));
        ready.messages = emitted;
        ready.message_capacity = TR_REPLAY_DRIVER_MAX_NODES;
        result = tr_raft_core_step(
            driver->nodes[to_index].core, &entry.message, &ready);
        if (result != TURBO_OK) {
            return result;
        }
        result = tr_replay_process_ready(driver, (size_t)to_index, &ready);
        if (result != TURBO_OK) {
            return result;
        }
        ++driver->counters.delivered;
    }
}

static int tr_replay_tick_unit(tr_replay_driver_t *driver)
{
    size_t index;

    for (index = 0u; index < driver->queue_count; ++index) {
        if (driver->queue[index].delay_ticks != 0u) {
            --driver->queue[index].delay_ticks;
        }
    }
    for (index = 0u; index < driver->node_count; ++index) {
        tr_replay_node_t *node = &driver->nodes[index];
        tr_raft_tick_t tick;
        uint32_t timeout_ticks;
        int result;

        if (!node->active) {
            continue;
        }
        result = tr_replay_timeout_for(driver, node, &timeout_ticks);
        if (result != TURBO_OK) {
            return result;
        }
        tick = (tr_raft_tick_t){1u, timeout_ticks};
        memset(&node->ready, 0, sizeof(node->ready));
        node->ready.messages = node->ready_messages;
        node->ready.message_capacity = TR_REPLAY_DRIVER_MAX_NODES;
        result = tr_raft_core_tick(node->core, &tick, &node->ready);
        if (result != TURBO_OK) {
            return result;
        }
        result = tr_replay_process_ready(driver, index, &node->ready);
        if (result != TURBO_OK) {
            return result;
        }
    }
    return tr_replay_pump(driver);
}

static int tr_replay_tick(tr_replay_driver_t *driver, uint32_t elapsed_ticks)
{
    uint32_t tick_index;

    if (elapsed_ticks == 0u) {
        return TURBO_EINVAL;
    }
    for (tick_index = 0u; tick_index < elapsed_ticks; ++tick_index) {
        int result = tr_replay_tick_unit(driver);

        if (result != TURBO_OK) {
            return result;
        }
    }
    driver->counters.ticks += elapsed_ticks;
    return TURBO_OK;
}

static int tr_replay_send(tr_replay_driver_t *driver,
                          tr_raft_node_id_t from,
                          tr_raft_node_id_t to)
{
    tr_raft_message_t message;
    tr_raft_status_t status;
    int from_index = tr_replay_find_node(driver, from);
    int to_index = tr_replay_find_node(driver, to);
    int result;

    if (from_index < 0 || to_index < 0) {
        return TURBO_ENOENT;
    }
    result = tr_raft_core_status(driver->nodes[from_index].core, &status);
    if (result != TURBO_OK) {
        return result;
    }
    memset(&message, 0, sizeof(message));
    message.type = TR_RAFT_MSG_HEARTBEAT_REQUEST;
    message.from = from;
    message.to = to;
    message.term = status.term;
    message.last_log_index = status.last_log_index;
    message.last_log_term = status.last_log_term;
    message.leader_commit = status.commit_index;
    result = tr_replay_enqueue(driver, &message, 0u);
    if (result != TURBO_OK) {
        return result;
    }
    return tr_replay_pump(driver);
}

static int tr_replay_submit(tr_replay_driver_t *driver,
                            const tr_text_replay_action_t *action)
{
    uint8_t payload[TR_RAFT_MAX_ENTRY_BYTES];
    tr_raft_proposal_t proposal;
    tr_raft_status_t status;
    size_t payload_size;
    size_t index;
    int node_index = tr_replay_find_node(driver, action->node_id);
    int result;

    if (node_index < 0) {
        return TURBO_ENOENT;
    }
    for (index = 0u; index < TR_TEXT_MAX_STATEMENTS; ++index) {
        if (driver->receipts[index].occupied &&
            driver->receipts[index].request_id == action->request_id) {
            return TURBO_EALREADY;
        }
    }
    result = tr_replay_decode_hex(action->payload_hex, payload,
                                  sizeof(payload), &payload_size);
    if (result != TURBO_OK) {
        return result;
    }
    memset(&proposal, 0, sizeof(proposal));
    proposal.command_id = action->request_id;
    proposal.data = payload;
    proposal.data_length = payload_size;
    memset(&driver->nodes[node_index].ready, 0,
           sizeof(driver->nodes[node_index].ready));
    driver->nodes[node_index].ready.messages =
        driver->nodes[node_index].ready_messages;
    driver->nodes[node_index].ready.message_capacity =
        TR_REPLAY_DRIVER_MAX_NODES;
    result = tr_raft_core_propose(driver->nodes[node_index].core, &proposal,
                                  &driver->nodes[node_index].ready);
    if (result != TURBO_OK) {
        return result;
    }
    result = tr_replay_process_ready(driver, (size_t)node_index,
                                     &driver->nodes[node_index].ready);
    if (result != TURBO_OK) {
        return result;
    }
    result = tr_raft_core_status(driver->nodes[node_index].core, &status);
    if (result != TURBO_OK) {
        return result;
    }
    for (index = 0u; index < TR_TEXT_MAX_STATEMENTS; ++index) {
        if (!driver->receipts[index].occupied) {
            driver->receipts[index].occupied = true;
            driver->receipts[index].request_id = action->request_id;
            driver->receipts[index].node_id = action->node_id;
            driver->receipts[index].term = status.term;
            driver->receipts[index].index = status.last_log_index;
            return TURBO_OK;
        }
    }
    return TURBO_ENOSPC;
}

static bool tr_replay_poll_reached(tr_text_replay_poll_target_t target,
                                   tr_raft_operation_state_t state)
{
    switch (target) {
    case TR_TEXT_REPLAY_POLL_ACCEPTED:
        return state == TR_RAFT_OPERATION_PENDING ||
               state == TR_RAFT_OPERATION_COMMITTED ||
               state == TR_RAFT_OPERATION_APPLIED;
    case TR_TEXT_REPLAY_POLL_COMMITTED:
        return state == TR_RAFT_OPERATION_COMMITTED ||
               state == TR_RAFT_OPERATION_APPLIED;
    case TR_TEXT_REPLAY_POLL_APPLIED:
        return state == TR_RAFT_OPERATION_APPLIED;
    default:
        return false;
    }
}

static int tr_replay_poll(tr_replay_driver_t *driver,
                          const tr_text_replay_action_t *action)
{
    const tr_replay_receipt_t *receipt = NULL;
    uint64_t ticks;
    size_t index;

    for (index = 0u; index < TR_TEXT_MAX_STATEMENTS; ++index) {
        if (driver->receipts[index].occupied &&
            driver->receipts[index].request_id == action->request_id) {
            receipt = &driver->receipts[index];
            break;
        }
    }
    if (receipt == NULL) {
        return TURBO_ENOENT;
    }
    {
        int node_index = tr_replay_find_node(driver, receipt->node_id);
        if (node_index < 0) {
            return TURBO_ENOENT;
        }
        for (ticks = 0u; ticks <= action->timeout_ticks; ++ticks) {
            tr_raft_operation_status_t operation;

            memset(&operation, 0, sizeof(operation));
            {
                int result = tr_raft_core_operation_status(
                    driver->nodes[node_index].core, receipt->term,
                    receipt->index, &operation);

                if (result != TURBO_OK) {
                    return result;
                }
            }
            if (tr_replay_poll_reached(action->poll_target,
                                       operation.state)) {
                return TURBO_OK;
            }
            if (operation.state == TR_RAFT_OPERATION_LOST ||
                operation.state == TR_RAFT_OPERATION_EXPIRED) {
                return TURBO_EPROTO;
            }
            if (ticks == action->timeout_ticks) {
                break;
            }
            {
                int result = tr_replay_tick(driver, 1u);

                if (result != TURBO_OK) {
                    return result;
                }
            }
        }
    }
    return TURBO_ETIMEDOUT;
}

static int tr_replay_expect_role(tr_replay_driver_t *driver,
                                 const tr_text_replay_action_t *action)
{
    tr_raft_role_t expected;
    tr_raft_status_t status;
    int node_index = tr_replay_find_node(driver, action->node_id);
    int result;

    if (node_index < 0) {
        return TURBO_ENOENT;
    }
    result = tr_replay_role(action->name, &expected);
    if (result != TURBO_OK) {
        return result;
    }
    result = tr_raft_core_status(driver->nodes[node_index].core, &status);
    if (result != TURBO_OK) {
        return result;
    }
    return status.role == expected ? TURBO_OK : TURBO_EPROTO;
}

static int tr_replay_expect_commit(tr_replay_driver_t *driver,
                                   const tr_text_replay_action_t *action)
{
    tr_raft_status_t status;
    int node_index = tr_replay_find_node(driver, action->node_id);
    int result;

    if (node_index < 0) {
        return TURBO_ENOENT;
    }
    result = tr_raft_core_status(driver->nodes[node_index].core, &status);
    if (result != TURBO_OK) {
        return result;
    }
    return status.commit_index >= action->value ? TURBO_OK : TURBO_EPROTO;
}

static int tr_replay_set_filter(tr_replay_driver_t *driver,
                                vstr name,
                                tr_replay_filter_action_t action,
                                uint32_t delay_ticks)
{
    tr_raft_message_type_t kind;
    int result = tr_replay_message_type(name, &kind);

    if (result != TURBO_OK) {
        return result;
    }
    if (driver->filter_count >= TR_REPLAY_DRIVER_MAX_FILTERS) {
        return TURBO_ENOSPC;
    }
    driver->filters[driver->filter_count].active = true;
    driver->filters[driver->filter_count].kind = kind;
    driver->filters[driver->filter_count].action = action;
    driver->filters[driver->filter_count].delay_ticks = delay_ticks;
    ++driver->filter_count;
    return TURBO_OK;
}

int tr_replay_driver_create(const tr_replay_driver_config_t *config,
                            tr_replay_driver_t **out_driver)
{
    tr_replay_driver_t *driver;
    size_t index;
    size_t peer;

    if (config == NULL || out_driver == NULL || config->nodes == NULL ||
        config->node_count == 0u ||
        config->node_count > TR_REPLAY_DRIVER_MAX_NODES) {
        return TURBO_EINVAL;
    }
    for (index = 0u; index < config->node_count; ++index) {
        for (peer = index + 1u; peer < config->node_count; ++peer) {
            if (config->nodes[index].self_id == config->nodes[peer].self_id) {
                return TURBO_EINVAL;
            }
        }
    }
    *out_driver = NULL;
    driver = (tr_replay_driver_t *)calloc(1u, sizeof(*driver));
    if (driver == NULL) {
        return TURBO_ENOMEM;
    }
    driver->node_count = config->node_count;
    driver->queue_capacity = config->max_queued != 0u
                                 ? config->max_queued
                                 : config->node_count *
                                       TR_REPLAY_DRIVER_DEFAULT_QUEUE_PER_NODE;
    driver->max_deliveries = config->max_deliveries != 0u
                                 ? config->max_deliveries
                                 : driver->queue_capacity * 16u;
    driver->apply = config->apply;
    driver->apply_context = config->apply_context;
    driver->next_election_timeout = config->next_election_timeout;
    driver->timeout_context = config->timeout_context;
    driver->default_election_timeout_ticks =
        config->default_election_timeout_ticks;
    driver->queue =
        (tr_replay_queued_t *)calloc(driver->queue_capacity,
                                     sizeof(driver->queue[0]));
    if (driver->queue == NULL) {
        free(driver);
        return TURBO_ENOMEM;
    }
    for (index = 0u; index < driver->node_count; ++index) {
        int result = tr_raft_core_create(&config->nodes[index],
                                         &driver->nodes[index].core);

        if (result != TURBO_OK) {
            while (index > 0u) {
                --index;
                tr_raft_core_destroy(driver->nodes[index].core);
            }
            free(driver->queue);
            free(driver);
            return result;
        }
        driver->nodes[index].core_config = config->nodes[index];
        driver->nodes[index].active = true;
    }
    for (index = 0u; index < driver->node_count; ++index) {
        for (peer = 0u; peer < driver->node_count; ++peer) {
            driver->links[index][peer] = true;
        }
    }
    *out_driver = driver;
    return TURBO_OK;
}

void tr_replay_driver_destroy(tr_replay_driver_t *driver)
{
    size_t index;

    if (driver == NULL) {
        return;
    }
    for (index = 0u; index < driver->node_count; ++index) {
        if (driver->nodes[index].core != NULL) {
            tr_raft_core_destroy(driver->nodes[index].core);
            driver->nodes[index].core = NULL;
        }
        driver->nodes[index].active = false;
    }
    free(driver->queue);
    free(driver);
}

int tr_replay_driver_step(tr_replay_driver_t *driver,
                          const tr_text_replay_action_t *action)
{
    int from_index;
    int to_index;

    if (driver == NULL || action == NULL) {
        return TURBO_EINVAL;
    }
    switch (action->kind) {
    case TR_TEXT_REPLAY_NODE:
        return tr_replay_find_node(driver, action->node_id) < 0
                   ? TURBO_ENOENT
                   : TURBO_OK;
    case TR_TEXT_REPLAY_TICK:
        if (action->value == 0u || action->value > UINT32_MAX) {
            return TURBO_ERANGE;
        }
        return tr_replay_tick(driver, (uint32_t)action->value);
    case TR_TEXT_REPLAY_SEND:
        return tr_replay_send(driver, action->node_id, action->peer_id);
    case TR_TEXT_REPLAY_DROP_NEXT:
        return tr_replay_set_filter(driver, action->name,
                                    TR_REPLAY_FILTER_DROP, 0u);
    case TR_TEXT_REPLAY_DELAY_NEXT:
        if (action->value > UINT32_MAX) {
            return TURBO_ERANGE;
        }
        return tr_replay_set_filter(driver, action->name,
                                    TR_REPLAY_FILTER_DELAY,
                                    (uint32_t)action->value);
    case TR_TEXT_REPLAY_DUPLICATE_NEXT:
        return tr_replay_set_filter(driver, action->name,
                                    TR_REPLAY_FILTER_DUPLICATE, 0u);
    case TR_TEXT_REPLAY_PARTITION:
        from_index = tr_replay_find_node(driver, action->node_id);
        to_index = tr_replay_find_node(driver, action->peer_id);
        if (from_index < 0 || to_index < 0) {
            return TURBO_ENOENT;
        }
        driver->links[from_index][to_index] = false;
        return TURBO_OK;
    case TR_TEXT_REPLAY_HEAL:
        from_index = tr_replay_find_node(driver, action->node_id);
        to_index = tr_replay_find_node(driver, action->peer_id);
        if (from_index < 0 || to_index < 0) {
            return TURBO_ENOENT;
        }
        driver->links[from_index][to_index] = true;
        return TURBO_OK;
    case TR_TEXT_REPLAY_SUBMIT:
        return tr_replay_submit(driver, action);
    case TR_TEXT_REPLAY_POLL:
        return tr_replay_poll(driver, action);
    case TR_TEXT_REPLAY_EXPECT_ROLE:
        return tr_replay_expect_role(driver, action);
    case TR_TEXT_REPLAY_EXPECT_COMMIT_INDEX:
        return tr_replay_expect_commit(driver, action);
    default:
        return TURBO_ENOTSUP;
    }
}

int tr_replay_driver_run(tr_replay_driver_t *driver,
                         const tr_text_replay_plan_t *plan)
{
    size_t index;

    if (driver == NULL || plan == NULL ||
        plan->action_count > TR_TEXT_MAX_STATEMENTS) {
        return TURBO_EINVAL;
    }
    for (index = 0u; index < plan->action_count; ++index) {
        int result = tr_replay_driver_step(driver, &plan->actions[index]);

        if (result != TURBO_OK) {
            return result;
        }
    }
    return TURBO_OK;
}

int tr_replay_driver_status(const tr_replay_driver_t *driver,
                            tr_raft_node_id_t node_id,
                            tr_raft_status_t *out_status)
{
    int node_index;

    if (driver == NULL || out_status == NULL) {
        return TURBO_EINVAL;
    }
    node_index = tr_replay_find_node(driver, node_id);
    if (node_index < 0) {
        return TURBO_ENOENT;
    }
    return tr_raft_core_status(driver->nodes[node_index].core, out_status);
}

size_t tr_replay_driver_queued(const tr_replay_driver_t *driver)
{
    return driver == NULL ? 0u : driver->queue_count;
}

void tr_replay_driver_counters(const tr_replay_driver_t *driver,
                               tr_replay_driver_counters_t *out_counters)
{
    if (driver == NULL || out_counters == NULL) {
        return;
    }
    *out_counters = driver->counters;
}
