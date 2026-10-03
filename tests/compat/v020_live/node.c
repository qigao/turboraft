#include <turboraft/raft_core.h>
#include <turboraft/raft_wire_codec.h>

#include <salts_error.h>

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define LIVE_LINE_CAPACITY (TR_RAFT_WIRE_MAX_FRAME_SIZE * 2U + 128U)

static char live_line[LIVE_LINE_CAPACITY];
static char live_hex[TR_RAFT_WIRE_MAX_FRAME_SIZE * 2U + 1U];
static uint8_t live_frame[TR_RAFT_WIRE_MAX_FRAME_SIZE];

typedef struct live_node {
    tr_raft_node_id_t node_id;
    tr_raft_core_t *core;
    tr_raft_wire_codec_t *codec;
    tr_raft_cluster_id_t cluster_id;
    uint64_t next_message_id;
} live_node_t;

static const tr_raft_node_id_t live_voters[] = {1U, 2U};

static void live_fill_cluster(tr_raft_cluster_id_t *cluster)
{
    size_t index;

    memset(cluster, 0, sizeof(*cluster));
    for (index = 0U; index < sizeof(cluster->bytes); ++index) {
        cluster->bytes[index] = (uint8_t)(0x30U + index);
    }
}

static int live_cluster_equal(const tr_raft_cluster_id_t *left,
                              const tr_raft_cluster_id_t *right)
{
    return memcmp(left->bytes, right->bytes, sizeof(left->bytes)) == 0;
}

static int live_hex_value(char value)
{
    if (value >= '0' && value <= '9') return value - '0';
    if (value >= 'a' && value <= 'f') return value - 'a' + 10;
    if (value >= 'A' && value <= 'F') return value - 'A' + 10;
    return -1;
}

static int live_hex_decode(const char *text,
                           uint8_t *output,
                           size_t capacity,
                           size_t *out_size)
{
    size_t length;
    size_t index;

    if (text == NULL || output == NULL || out_size == NULL) {
        return SALTS_EINVAL;
    }
    length = strlen(text);
    if ((length & 1U) != 0U || length / 2U > capacity) {
        return SALTS_ERANGE;
    }
    for (index = 0U; index < length / 2U; ++index) {
        int high = live_hex_value(text[index * 2U]);
        int low = live_hex_value(text[index * 2U + 1U]);

        if (high < 0 || low < 0) {
            return SALTS_EINVAL;
        }
        output[index] = (uint8_t)((high << 4U) | low);
    }
    *out_size = length / 2U;
    return SALTS_OK;
}

static void live_hex_encode(const uint8_t *input,
                            size_t size,
                            char *output)
{
    static const char digits[] = "0123456789abcdef";
    size_t index;

    for (index = 0U; index < size; ++index) {
        output[index * 2U] = digits[input[index] >> 4U];
        output[index * 2U + 1U] = digits[input[index] & 0x0fU];
    }
    output[size * 2U] = '\0';
}

static int live_parse_u64(const char *text, uint64_t *out_value)
{
    char *end = NULL;
    unsigned long long value;

    if (text == NULL || text[0] == '\0' || out_value == NULL) {
        return SALTS_EINVAL;
    }
    value = strtoull(text, &end, 10);
    if (end == text || *end != '\0') {
        return SALTS_EINVAL;
    }
    *out_value = (uint64_t)value;
    return SALTS_OK;
}

static int live_emit_result(live_node_t *node,
                            int operation_result,
                            tr_raft_ready_t *ready)
{
    tr_raft_status_t status;
    size_t index;
    int result;

    result = tr_raft_core_status(node->core, &status);
    if (result != SALTS_OK) {
        return result;
    }
    printf("RESULT %d %d %" PRIu64 " %" PRIu64 " %" PRIu64
           " %" PRIu64 " %zu\n",
           operation_result, (int)status.role, status.term,
           status.leader_id, status.last_log_index, status.commit_index,
           ready == NULL ? 0U : ready->message_count);

    if (ready != NULL) {
        for (index = 0U; index < ready->message_count; ++index) {
            tr_raft_wire_metadata_t metadata;
            size_t frame_size = 0U;

            memset(&metadata, 0, sizeof(metadata));
            metadata.cluster_id = node->cluster_id;
            metadata.group_id = UINT64_C(100);
            metadata.message_id = node->next_message_id++;
            result = tr_raft_wire_encode(
                node->codec, &metadata, &ready->messages[index],
                live_frame, sizeof(live_frame), &frame_size);
            if (result != SALTS_OK) {
                return result;
            }
            live_hex_encode(live_frame, frame_size, live_hex);
            printf("FRAME %s\n", live_hex);
        }
    }
    puts("END");
    return fflush(stdout) == 0 ? SALTS_OK : SALTS_EPIPE;
}

static int live_finish_ready(live_node_t *node,
                             int operation_result,
                             tr_raft_ready_t *ready)
{
    tr_raft_status_t status;
    int result = live_emit_result(node, operation_result, ready);

    if (result != SALTS_OK || operation_result != SALTS_OK ||
        ready == NULL) {
        return result;
    }
    memset(&status, 0, sizeof(status));
    result = tr_raft_core_status(node->core, &status);
    if (result != SALTS_OK) {
        return result;
    }
    return status.ready_outstanding
               ? tr_raft_core_advance(node->core)
               : SALTS_OK;
}

static int live_command_tick(live_node_t *node,
                             const char *elapsed_text,
                             const char *timeout_text)
{
    tr_raft_message_t messages[4];
    tr_raft_ready_t ready;
    tr_raft_tick_t tick;
    uint64_t elapsed;
    uint64_t timeout;
    int result;

    if (live_parse_u64(elapsed_text, &elapsed) != SALTS_OK ||
        live_parse_u64(timeout_text, &timeout) != SALTS_OK ||
        elapsed > UINT32_MAX || timeout > UINT32_MAX) {
        return SALTS_EINVAL;
    }
    memset(&ready, 0, sizeof(ready));
    ready.messages = messages;
    ready.message_capacity = 4U;
    tick.elapsed_ticks = (uint32_t)elapsed;
    tick.next_election_timeout_ticks = (uint32_t)timeout;
    result = tr_raft_core_tick(node->core, &tick, &ready);
    return live_finish_ready(node, result, &ready);
}

static int live_command_step(live_node_t *node, const char *frame_text)
{
    tr_raft_wire_metadata_t metadata;
    tr_raft_message_t message;
    tr_raft_message_t messages[4];
    tr_raft_ready_t ready;
    size_t frame_size = 0U;
    uint16_t version = 0U;
    int result;

    result = live_hex_decode(frame_text, live_frame,
                             sizeof(live_frame), &frame_size);
    if (result != SALTS_OK) {
        return result;
    }
    result = tr_raft_wire_peek_version(
        live_frame, frame_size, &version);
    if (result != SALTS_OK || version != TR_RAFT_WIRE_VERSION) {
        return result == SALTS_OK ? SALTS_EPROTO : result;
    }
    memset(&metadata, 0, sizeof(metadata));
    memset(&message, 0, sizeof(message));
    result = tr_raft_wire_decode(
        node->codec, live_frame, frame_size, &metadata, &message);
    if (result != SALTS_OK) {
        return result;
    }
    if (!live_cluster_equal(&metadata.cluster_id, &node->cluster_id) ||
        metadata.group_id != UINT64_C(100) ||
        message.to != node->node_id) {
        return SALTS_EPROTO;
    }

    memset(&ready, 0, sizeof(ready));
    ready.messages = messages;
    ready.message_capacity = 4U;
    result = tr_raft_core_step(node->core, &message, &ready);
    return live_finish_ready(node, result, &ready);
}

static int live_command_propose(live_node_t *node,
                                const char *command_text,
                                const char *payload_text)
{
    tr_raft_message_t messages[4];
    tr_raft_ready_t ready;
    tr_raft_proposal_t proposal;
    uint8_t payload[TR_RAFT_MAX_ENTRY_BYTES];
    size_t payload_size = 0U;
    uint64_t command_id;
    int result;

    if (live_parse_u64(command_text, &command_id) != SALTS_OK) {
        return SALTS_EINVAL;
    }
    result = live_hex_decode(payload_text, payload,
                             sizeof(payload), &payload_size);
    if (result != SALTS_OK || payload_size == 0U) {
        return result == SALTS_OK ? SALTS_EINVAL : result;
    }

    memset(&proposal, 0, sizeof(proposal));
    proposal.command_id = command_id;
    proposal.data = payload;
    proposal.data_length = payload_size;
    memset(&ready, 0, sizeof(ready));
    ready.messages = messages;
    ready.message_capacity = 4U;
    result = tr_raft_core_propose(node->core, &proposal, &ready);
    return live_finish_ready(node, result, &ready);
}

static int live_command_status(live_node_t *node)
{
    return live_emit_result(node, SALTS_OK, NULL);
}

int main(int argc, char **argv)
{
    tr_raft_core_config_t config;
    live_node_t node;
    uint64_t parsed_node_id;
    int result;

    if (argc != 2 ||
        live_parse_u64(argv[1], &parsed_node_id) != SALTS_OK ||
        parsed_node_id < 1U || parsed_node_id > 2U) {
        fprintf(stderr, "usage: %s <node-id:1|2>\n", argv[0]);
        return 2;
    }

    memset(&node, 0, sizeof(node));
    node.node_id = (tr_raft_node_id_t)parsed_node_id;
    node.next_message_id = 1U;
    live_fill_cluster(&node.cluster_id);

    memset(&config, 0, sizeof(config));
    config.self_id = node.node_id;
    config.voters = live_voters;
    config.voter_count = 2U;
    config.heartbeat_ticks = 1U;
    config.election_min_ticks = 3U;
    config.election_max_ticks = 7U;
    config.initial_election_timeout_ticks = 5U;
    config.max_log_entries = 32U;
    config.max_pending_reads = 4U;

    result = tr_raft_core_create(&config, &node.core);
    if (result == SALTS_OK) {
        result = tr_raft_wire_codec_create(&node.codec);
    }
    if (result != SALTS_OK) {
        tr_raft_core_destroy(node.core);
        fprintf(stderr, "live peer init failed: %d\n", result);
        return 3;
    }

    while (fgets(live_line, sizeof(live_line), stdin) != NULL) {
        char *command;
        char *arg1;
        char *arg2;
        char *newline = strchr(live_line, '\n');

        if (newline != NULL) {
            *newline = '\0';
        }
        command = strtok(live_line, " ");
        arg1 = strtok(NULL, " ");
        arg2 = strtok(NULL, " ");
        if (command == NULL) {
            continue;
        }

        if (strcmp(command, "TICK") == 0 && arg1 != NULL && arg2 != NULL) {
            result = live_command_tick(&node, arg1, arg2);
        } else if (strcmp(command, "STEP") == 0 && arg1 != NULL) {
            result = live_command_step(&node, arg1);
        } else if (strcmp(command, "PROPOSE") == 0 &&
                   arg1 != NULL && arg2 != NULL) {
            result = live_command_propose(&node, arg1, arg2);
        } else if (strcmp(command, "STATUS") == 0) {
            result = live_command_status(&node);
        } else if (strcmp(command, "STOP") == 0) {
            result = live_emit_result(&node, SALTS_OK, NULL);
            break;
        } else {
            result = live_emit_result(&node, SALTS_EINVAL, NULL);
        }
        if (result != SALTS_OK) {
            fprintf(stderr, "live peer command failed: %d\n", result);
            break;
        }
    }

    tr_raft_wire_codec_destroy(node.codec);
    tr_raft_core_destroy(node.core);
    return result == SALTS_OK ? 0 : 4;
}
