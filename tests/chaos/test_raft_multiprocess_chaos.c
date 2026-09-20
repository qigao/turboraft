#include "raft_multiprocess_protocol.h"

#include <turboraft/raft_core.h>
#include <turboraft/raft_wire_codec.h>

#include <tinytest.h>
#include <salts_error.h>
#include <salts_process.h>
#include <salts_thread.h>

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define TR_CHAOS_DEFAULT_FIRST_SEED 1U
#define TR_CHAOS_DEFAULT_SEED_COUNT 4U
#define TR_CHAOS_MAX_SEED_COUNT 16U
#define TR_CHAOS_ROUND_COUNT 112U
#define TR_CHAOS_IO_TIMEOUT_MS 5000U
#define TR_CHAOS_MAX_TRACKED_INDEX 512U
#define TR_CHAOS_MAX_TRACKED_TERM 256U
#define TR_CHAOS_RECOVERY_ROUND_COUNT 48U
#define TR_CHAOS_RECOVERY_DELIVERY_LIMIT 64U

typedef struct tr_chaos_seed_range {
    uint32_t first;
    uint32_t count;
} tr_chaos_seed_range_t;

static int tr_chaos_parse_positive_u32(const char *text,
                                       uint32_t default_value,
                                       uint32_t maximum,
                                       uint32_t *out_value)
{
    const char *cursor;
    char *end = NULL;
    unsigned long value;

    if (out_value == NULL) return SALTS_EINVAL;
    if (text == NULL) {
        *out_value = default_value;
        return SALTS_OK;
    }
    if (text[0] == '\0') return SALTS_EINVAL;
    for (cursor = text; *cursor != '\0'; ++cursor) {
        if (*cursor < '0' || *cursor > '9') return SALTS_EINVAL;
    }

    errno = 0;
    value = strtoul(text, &end, 10);
    if (errno == ERANGE || value > maximum) return SALTS_ERANGE;
    if (end == text || *end != '\0' || value == 0U) return SALTS_EINVAL;
    *out_value = (uint32_t) value;
    return SALTS_OK;
}

static int tr_chaos_parse_seed_range(const char *first_text,
                                     const char *count_text,
                                     tr_chaos_seed_range_t *out_range)
{
    tr_chaos_seed_range_t range;
    int result;

    if (out_range == NULL) return SALTS_EINVAL;
    result = tr_chaos_parse_positive_u32(first_text,
                                         TR_CHAOS_DEFAULT_FIRST_SEED,
                                         UINT32_MAX, &range.first);
    if (result != SALTS_OK) return result;
    result = tr_chaos_parse_positive_u32(count_text,
                                         TR_CHAOS_DEFAULT_SEED_COUNT,
                                         TR_CHAOS_MAX_SEED_COUNT,
                                         &range.count);
    if (result != SALTS_OK) return result;
    if (range.first > UINT32_MAX - (range.count - 1U)) return SALTS_ERANGE;
    *out_range = range;
    return SALTS_OK;
}

static tr_chaos_seed_range_t tr_chaos_configured_seed_range;
static int tr_chaos_configuration_status = SALTS_EINVAL;
static const char *tr_chaos_configured_first_text;
static const char *tr_chaos_configured_count_text;

typedef struct tr_chaos_response {
    int operation_result;
    uint32_t message_count;
    uint32_t payload_size;
    tr_raft_node_id_t node_id;
    tr_raft_role_t role;
    int faulted;
    int cause;
    uint32_t user_apply_count;
    tr_raft_term_t term;
    tr_raft_node_id_t leader_id;
    tr_raft_index_t last_log_index;
    tr_raft_index_t commit_index;
    tr_raft_index_t applied_index;
    uint64_t applied_hash;
} tr_chaos_response_t;

typedef struct tr_chaos_process_node {
    tr_raft_node_id_t id;
    char database_path[512];
    salts_process_t *process;
    uint32_t next_request_id;
    int alive;
    tr_chaos_response_t status;
    tr_chaos_response_t group_status[TR_CHAOS_GROUP_COUNT];
} tr_chaos_process_node_t;

typedef struct tr_chaos_frame {
    tr_raft_node_id_t from;
    tr_raft_node_id_t to;
    tr_raft_group_id_t group_id;
    uint32_t size;
    uint8_t data[4096];
} tr_chaos_frame_t;

typedef struct tr_chaos_network {
    tr_chaos_frame_t *frames;
    size_t count;
    uint32_t dropped;
    uint32_t duplicated;
} tr_chaos_network_t;

typedef struct tr_chaos_safety {
    tr_raft_term_t last_term[3];
    tr_raft_index_t last_commit[3];
    tr_raft_node_id_t leader_by_term[TR_CHAOS_MAX_TRACKED_TERM];
    uint64_t hash_by_index[TR_CHAOS_MAX_TRACKED_INDEX];
    uint8_t hash_known[TR_CHAOS_MAX_TRACKED_INDEX];
    uint32_t max_user_apply_count;
} tr_chaos_safety_t;

static uint32_t tr_chaos_random(uint32_t *state)
{
    uint32_t value = *state;

    value ^= value << 13U;
    value ^= value >> 17U;
    value ^= value << 5U;
    *state = value;
    return value;
}

static int tr_chaos_process_read_exact(salts_process_t *process,
                                       void *output,
                                       size_t size)
{
    uint8_t *bytes = (uint8_t *) output;
    size_t total = 0U;
    uint32_t waited = 0U;

    while (total < size && waited < TR_CHAOS_IO_TIMEOUT_MS) {
        size_t count = 0U;
        int result = salts_process_read_stdout(
            process, bytes + total, size - total, &count);

        total += count;
        if (total == size) {
            return SALTS_OK;
        }
        if (result == SALTS_EOF) {
            return SALTS_EPIPE;
        }
        if (result != SALTS_OK) {
            return result;
        }
        salts_sleep_ms(1U);
        waited++;
    }
    return SALTS_ETIMEDOUT;
}

static int tr_chaos_process_write_exact(salts_process_t *process,
                                        const void *input,
                                        size_t size)
{
    const uint8_t *bytes = (const uint8_t *) input;
    size_t total = 0U;

    while (total < size) {
        size_t count = 0U;
        int result = salts_process_write_stdin(
            process, bytes + total, size - total, &count);

        if (result != SALTS_OK) {
            return result;
        }
        if (count == 0U) {
            return SALTS_EPIPE;
        }
        total += count;
    }
    return SALTS_OK;
}

static int tr_chaos_node_group_command(
    tr_chaos_process_node_t *node,
    tr_raft_group_id_t group_id,
    tr_chaos_command_kind_t kind,
    const uint8_t *payload,
    size_t payload_size,
    uint8_t *response_payload,
    tr_chaos_response_t *response)
{
    uint8_t command[TR_CHAOS_COMMAND_HEADER_SIZE];
    uint8_t header[TR_CHAOS_RESPONSE_HEADER_SIZE];
    uint32_t request_id;
    int result;

    if (node == NULL || !node->alive || response == NULL ||
        payload_size > TR_CHAOS_MAX_FRAME_BYTES ||
        (payload == NULL && payload_size != 0U)) {
        return SALTS_EINVAL;
    }
    request_id = node->next_request_id++;
    memset(command, 0, sizeof(command));
    memcpy(command, tr_chaos_command_magic, sizeof(tr_chaos_command_magic));
    tr_chaos_put_u16(command + 4U, TR_CHAOS_PROTOCOL_VERSION);
    tr_chaos_put_u16(command + 6U, (uint16_t) kind);
    tr_chaos_put_u32(command + 8U, request_id);
    tr_chaos_put_u32(command + 12U, (uint32_t) payload_size);
    tr_chaos_put_u64(command + 16U, group_id);
    result = tr_chaos_process_write_exact(node->process, command,
                                          sizeof(command));
    if (result == SALTS_OK && payload_size != 0U) {
        result = tr_chaos_process_write_exact(node->process, payload,
                                              payload_size);
    }
    if (result == SALTS_OK) {
        result = tr_chaos_process_read_exact(node->process, header,
                                             sizeof(header));
    }
    if (result != SALTS_OK) {
        fprintf(stderr,
                "chaos command io node=%llu kind=%d request=%u result=%d\n",
                (unsigned long long) node->id, (int) kind, request_id,
                result);
        return result;
    }
    if (memcmp(header, tr_chaos_response_magic,
               sizeof(tr_chaos_response_magic)) != 0 ||
        tr_chaos_get_u16(header + 4U) != TR_CHAOS_PROTOCOL_VERSION ||
        tr_chaos_get_u32(header + 8U) != request_id) {
        fprintf(stderr,
                "chaos command header node=%llu kind=%d request=%u "
                "magic=%02x%02x%02x%02x version=%u response-request=%u "
                "operation=%d messages=%u payload=%u response-node=%llu\n",
                (unsigned long long) node->id, (int) kind, request_id,
                header[0], header[1], header[2], header[3],
                (unsigned int) tr_chaos_get_u16(header + 4U),
                tr_chaos_get_u32(header + 8U),
                (int) tr_chaos_get_u32(header + 12U),
                tr_chaos_get_u32(header + 16U),
                tr_chaos_get_u32(header + 20U),
                (unsigned long long) tr_chaos_get_u64(header + 24U));
        return SALTS_EPROTO;
    }
    memset(response, 0, sizeof(*response));
    response->operation_result = (int) tr_chaos_get_u32(header + 12U);
    response->message_count = tr_chaos_get_u32(header + 16U);
    response->payload_size = tr_chaos_get_u32(header + 20U);
    response->node_id = tr_chaos_get_u64(header + 24U);
    response->role = (tr_raft_role_t) tr_chaos_get_u32(header + 32U);
    response->faulted = tr_chaos_get_u32(header + 36U) != 0U;
    response->cause = (int) tr_chaos_get_u32(header + 40U);
    response->user_apply_count = tr_chaos_get_u32(header + 44U);
    response->term = tr_chaos_get_u64(header + 48U);
    response->leader_id = tr_chaos_get_u64(header + 56U);
    response->last_log_index = tr_chaos_get_u64(header + 64U);
    response->commit_index = tr_chaos_get_u64(header + 72U);
    response->applied_index = tr_chaos_get_u64(header + 80U);
    response->applied_hash = tr_chaos_get_u64(header + 88U);
    if (response->node_id != node->id ||
        response->payload_size > TR_CHAOS_MAX_RESPONSE_BYTES ||
        (response->payload_size != 0U && response_payload == NULL)) {
        fprintf(stderr,
                "chaos command bounds expected-node=%llu response-node=%llu "
                "messages=%u payload=%u\n",
                (unsigned long long) node->id,
                (unsigned long long) response->node_id,
                response->message_count, response->payload_size);
        return SALTS_EPROTO;
    }
    if (response->payload_size != 0U) {
        result = tr_chaos_process_read_exact(
            node->process, response_payload, response->payload_size);
        if (result != SALTS_OK) {
            fprintf(stderr,
                    "chaos command payload node=%llu messages=%u payload=%u "
                    "result=%d\n",
                    (unsigned long long) node->id,
                    response->message_count, response->payload_size, result);
        }
    }
    if (result == SALTS_OK) {
        size_t slot = tr_chaos_group_slot(group_id);

        if (slot == SIZE_MAX) {
            return SALTS_EPROTO;
        }
        node->group_status[slot] = *response;
        if (group_id == TR_CHAOS_PRIMARY_GROUP_ID) {
            node->status = *response;
        }
    }
    return result;
}

static int tr_chaos_node_command(tr_chaos_process_node_t *node,
                                 tr_chaos_command_kind_t kind,
                                 const uint8_t *payload,
                                 size_t payload_size,
                                 uint8_t *response_payload,
                                 tr_chaos_response_t *response)
{
    return tr_chaos_node_group_command(
        node, TR_CHAOS_PRIMARY_GROUP_ID, kind, payload, payload_size,
        response_payload, response);
}

static int tr_chaos_track_status(tr_chaos_safety_t *safety,
                                 const tr_chaos_response_t *status)
{
    size_t node_index;

    if (status->node_id == 0U || status->node_id > 3U || status->faulted ||
        status->applied_index > status->commit_index ||
        status->commit_index > status->last_log_index ||
        status->term >= TR_CHAOS_MAX_TRACKED_TERM ||
        status->applied_index >= TR_CHAOS_MAX_TRACKED_INDEX) {
        fprintf(stderr,
                "chaos safety bounds node=%llu faulted=%d cause=%d term=%llu "
                "last=%llu commit=%llu applied=%llu\n",
                (unsigned long long) status->node_id, status->faulted,
                status->cause, (unsigned long long) status->term,
                (unsigned long long) status->last_log_index,
                (unsigned long long) status->commit_index,
                (unsigned long long) status->applied_index);
        return SALTS_EPROTO;
    }
    node_index = (size_t) status->node_id - 1U;
    if (status->term < safety->last_term[node_index] ||
        status->commit_index < safety->last_commit[node_index]) {
        fprintf(stderr,
                "chaos safety regression node=%llu term=%llu previous-term=%llu "
                "commit=%llu previous-commit=%llu\n",
                (unsigned long long) status->node_id,
                (unsigned long long) status->term,
                (unsigned long long) safety->last_term[node_index],
                (unsigned long long) status->commit_index,
                (unsigned long long) safety->last_commit[node_index]);
        return SALTS_EPROTO;
    }
    safety->last_term[node_index] = status->term;
    safety->last_commit[node_index] = status->commit_index;
    if (status->role == TR_RAFT_LEADER) {
        tr_raft_node_id_t known = safety->leader_by_term[status->term];

        if (known != 0U && known != status->node_id) {
            fprintf(stderr,
                    "chaos safety dual-leader term=%llu first=%llu second=%llu\n",
                    (unsigned long long) status->term,
                    (unsigned long long) known,
                    (unsigned long long) status->node_id);
            return SALTS_EPROTO;
        }
        safety->leader_by_term[status->term] = status->node_id;
    }
    if (status->applied_index != 0U) {
        size_t applied = (size_t) status->applied_index;

        if (safety->hash_known[applied] &&
            safety->hash_by_index[applied] != status->applied_hash) {
            fprintf(stderr,
                    "chaos safety hash index=%zu expected=%llu actual=%llu "
                    "node=%llu\n",
                    applied,
                    (unsigned long long) safety->hash_by_index[applied],
                    (unsigned long long) status->applied_hash,
                    (unsigned long long) status->node_id);
            return SALTS_EPROTO;
        }
        safety->hash_known[applied] = 1U;
        safety->hash_by_index[applied] = status->applied_hash;
    }
    if (status->user_apply_count > safety->max_user_apply_count) {
        safety->max_user_apply_count = status->user_apply_count;
    }
    return SALTS_OK;
}

static int tr_chaos_network_collect_group(
    tr_chaos_network_t *network,
    tr_raft_wire_codec_t *codec,
    const uint8_t *payload,
    const tr_chaos_response_t *response,
    tr_raft_group_id_t expected_group_id)
{
    size_t offset = 0U;
    uint32_t index;

    for (index = 0U; index < response->message_count; ++index) {
        tr_raft_wire_metadata_t metadata;
        tr_raft_message_t message;
        uint32_t frame_size;
        tr_chaos_frame_t *frame;
        int result;

        if (offset + 4U > response->payload_size) {
            fprintf(stderr,
                    "chaos collect truncated-header index=%u count=%u "
                    "offset=%zu payload=%u\n",
                    index, response->message_count, offset,
                    response->payload_size);
            return SALTS_EPROTO;
        }
        frame_size = tr_chaos_get_u32(payload + offset);
        offset += 4U;
        if (frame_size == 0U || frame_size > sizeof(network->frames[0].data) ||
            offset + frame_size > response->payload_size ||
            network->count >= TR_CHAOS_MAX_QUEUED_FRAMES) {
            fprintf(stderr,
                    "chaos collect bounds index=%u frame=%u offset=%zu "
                    "payload=%u queued=%zu\n",
                    index, frame_size, offset, response->payload_size,
                    network->count);
            return SALTS_ENOSPC;
        }
        result = tr_raft_wire_decode(codec, payload + offset, frame_size,
                                     &metadata, &message);
        if (result != SALTS_OK ||
            metadata.group_id != expected_group_id ||
            message.from != response->node_id ||
            message.to == 0U || message.to > 3U) {
            fprintf(stderr,
                    "chaos collect decode index=%u result=%d response-node=%llu "
                    "group=%llu from=%llu to=%llu frame=%u\n",
                    index, result,
                    (unsigned long long) response->node_id,
                    (unsigned long long) metadata.group_id,
                    (unsigned long long) message.from,
                    (unsigned long long) message.to, frame_size);
            return SALTS_EPROTO;
        }
        frame = &network->frames[network->count++];
        frame->from = message.from;
        frame->to = message.to;
        frame->group_id = metadata.group_id;
        frame->size = frame_size;
        memcpy(frame->data, payload + offset, frame_size);
        offset += frame_size;
    }
    if (offset != response->payload_size) {
        fprintf(stderr,
                "chaos collect trailing count=%u offset=%zu payload=%u\n",
                response->message_count, offset, response->payload_size);
        return SALTS_EPROTO;
    }
    return SALTS_OK;
}

static int tr_chaos_network_collect(
    tr_chaos_network_t *network,
    tr_raft_wire_codec_t *codec,
    const uint8_t *payload,
    const tr_chaos_response_t *response)
{
    return tr_chaos_network_collect_group(
        network, codec, payload, response, TR_CHAOS_PRIMARY_GROUP_ID);
}

static int tr_chaos_node_spawn(tr_chaos_process_node_t *node,
                               const char *program,
                               tr_chaos_safety_t *safety,
                               uint8_t *response_payload)
{
    salts_process_options_t options;
    tr_chaos_response_t response;
    char node_id[16];
    const char *args[5];
    int result;

    snprintf(node_id, sizeof(node_id), "%llu",
             (unsigned long long) node->id);
    args[0] = "--node";
    args[1] = node_id;
    args[2] = "--db";
    args[3] = node->database_path;
    args[4] = NULL;
    salts_process_options_init(&options);
    options.program = program;
    options.args = args;
    options.flags |= SALTS_PROCESS_PIPE_STDIN;
    options.timeout_ms = 60000U;
    options.max_output_bytes = 8U * 1024U * 1024U;
    result = salts_process_spawn(&options, &node->process);
    if (result != SALTS_OK) {
        return result;
    }
    node->alive = 1;
    node->next_request_id = 1U;
    result = tr_chaos_node_command(node, TR_CHAOS_COMMAND_STATUS, NULL, 0U,
                                   response_payload, &response);
    if (result == SALTS_OK) {
        result = tr_chaos_track_status(safety, &response);
    }
    return result;
}

static int tr_chaos_node_terminate(tr_chaos_process_node_t *node)
{
    salts_process_result_t result;
    int operation_result;

    if (!node->alive) {
        return SALTS_OK;
    }
    operation_result = salts_process_terminate(node->process);
    if (operation_result == SALTS_OK) {
        operation_result = salts_process_wait(node->process, &result);
    }
    salts_process_destroy(node->process);
    node->process = NULL;
    node->alive = 0;
    return operation_result;
}

static int tr_chaos_node_stop(tr_chaos_process_node_t *node,
                              uint8_t *response_payload)
{
    tr_chaos_response_t response;
    salts_process_result_t process_result;
    int result;

    memset(&process_result, 0, sizeof(process_result));

    if (!node->alive) {
        return SALTS_OK;
    }
    result = tr_chaos_node_command(node, TR_CHAOS_COMMAND_STOP, NULL, 0U,
                                   response_payload, &response);
    if (result == SALTS_OK) {
        result = salts_process_wait(node->process, &process_result);
    }
    if (result == SALTS_OK &&
        (process_result.state != SALTS_PROCESS_EXITED ||
         process_result.exit_code != 0)) {
        result = SALTS_EPROTO;
    }
    salts_process_destroy(node->process);
    node->process = NULL;
    node->alive = 0;
    return result;
}

static int tr_chaos_collect_command(
    tr_chaos_process_node_t *node,
    tr_chaos_command_kind_t kind,
    const uint8_t *command_payload,
    size_t command_size,
    tr_chaos_network_t *network,
    tr_raft_wire_codec_t *codec,
    tr_chaos_safety_t *safety,
    uint8_t *response_payload,
    int *out_operation_result);

static int tr_chaos_node_backup_handoff(
    tr_chaos_process_node_t *node,
    tr_chaos_network_t *network,
    tr_raft_wire_codec_t *codec,
    tr_chaos_safety_t *safety,
    uint8_t *response_payload)
{
    int operation_result = SALTS_OK;
    int result = tr_chaos_collect_command(
        node, TR_CHAOS_COMMAND_BACKUP_HANDOFF, NULL, 0U, network, codec,
        safety, response_payload, &operation_result);

    return result != SALTS_OK ? result : operation_result;
}

static int tr_chaos_collect_command(
    tr_chaos_process_node_t *node,
    tr_chaos_command_kind_t kind,
    const uint8_t *command_payload,
    size_t command_size,
    tr_chaos_network_t *network,
    tr_raft_wire_codec_t *codec,
    tr_chaos_safety_t *safety,
    uint8_t *response_payload,
    int *out_operation_result)
{
    tr_chaos_response_t response;
    int result = tr_chaos_node_command(
        node, kind, command_payload, command_size, response_payload,
        &response);

    if (result == SALTS_OK) {
        result = tr_chaos_track_status(safety, &response);
    }
    if (result == SALTS_OK) {
        result = tr_chaos_network_collect(network, codec, response_payload,
                                          &response);
    }
    if (out_operation_result != NULL) {
        *out_operation_result = response.operation_result;
    }
    return result;
}

static int tr_chaos_deliver_one(
    tr_chaos_process_node_t nodes[3],
    tr_chaos_network_t *network,
    tr_raft_wire_codec_t *codec,
    tr_chaos_safety_t *safety,
    uint8_t *response_payload,
    uint32_t *random_state,
    int faults_enabled,
    tr_raft_node_id_t partitioned_node)
{
    size_t selected;
    tr_chaos_frame_t frame;
    tr_chaos_process_node_t *target;
    uint32_t decision;
    int deliveries = 1;
    int index;

    if (network->count == 0U) {
        return SALTS_OK;
    }
    selected = tr_chaos_random(random_state) % network->count;
    frame = network->frames[selected];
    network->frames[selected] = network->frames[network->count - 1U];
    network->count--;
    target = &nodes[frame.to - 1U];
    if (!target->alive ||
        (partitioned_node != 0U &&
         (frame.from == partitioned_node || frame.to == partitioned_node))) {
        network->dropped++;
        return SALTS_OK;
    }
    decision = tr_chaos_random(random_state) % 100U;
    if (faults_enabled && decision < 18U) {
        network->dropped++;
        return SALTS_OK;
    }
    if (faults_enabled && decision >= 18U && decision < 28U) {
        deliveries = 2;
        network->duplicated++;
    }
    for (index = 0; index < deliveries; ++index) {
        int operation_result = SALTS_OK;
        int result = tr_chaos_collect_command(
            target, TR_CHAOS_COMMAND_STEP, frame.data, frame.size, network,
            codec, safety, response_payload, &operation_result);

        if (result != SALTS_OK || operation_result != SALTS_OK) {
            fprintf(stderr,
                    "chaos deliver from=%llu to=%llu copy=%d result=%d "
                    "operation=%d faulted=%d cause=%d term=%llu role=%d "
                    "commit=%llu applied=%llu\n",
                    (unsigned long long) frame.from,
                    (unsigned long long) frame.to, index, result,
                    operation_result, target->status.faulted,
                    target->status.cause,
                    (unsigned long long) target->status.term,
                    (int) target->status.role,
                    (unsigned long long) target->status.commit_index,
                    (unsigned long long) target->status.applied_index);
            return result != SALTS_OK ? result : operation_result;
        }
    }
    return SALTS_OK;
}

static int tr_chaos_recover_after_handoff(
    tr_chaos_process_node_t nodes[3],
    tr_chaos_network_t *network,
    tr_raft_wire_codec_t *codec,
    tr_chaos_safety_t *safety,
    uint8_t *response_payload,
    uint32_t *random_state,
    tr_raft_index_t target_applied_index)
{
    tr_raft_index_t final_applied_index = 0U;
    uint64_t final_applied_hash = 0U;
    uint32_t round;
    size_t index;

    if (network == NULL || codec == NULL || safety == NULL ||
        response_payload == NULL || random_state == NULL ||
        target_applied_index == 0U) {
        return SALTS_EINVAL;
    }
    for (round = 0U; round < TR_CHAOS_RECOVERY_ROUND_COUNT; ++round) {
        uint8_t tick[8];
        size_t delivery;

        for (index = 0U; index < 3U; ++index) {
            int operation_result = SALTS_OK;
            int result;

            if (!nodes[index].alive) {
                return SALTS_EPROTO;
            }
            tr_chaos_put_u32(tick, 1U);
            tr_chaos_put_u32(tick + 4U, 3U + (uint32_t) index);
            result = tr_chaos_collect_command(
                &nodes[index], TR_CHAOS_COMMAND_TICK, tick, sizeof(tick),
                network, codec, safety, response_payload,
                &operation_result);
            if (result != SALTS_OK || operation_result != SALTS_OK) {
                return result != SALTS_OK ? result : operation_result;
            }
        }
        for (delivery = 0U;
             delivery < TR_CHAOS_RECOVERY_DELIVERY_LIMIT &&
             network->count != 0U;
             ++delivery) {
            int result = tr_chaos_deliver_one(
                nodes, network, codec, safety, response_payload,
                random_state, 0, 0U);

            if (result != SALTS_OK) {
                return result;
            }
        }
        if (network->count == 0U) {
            int converged = 1;

            for (index = 0U; index < 3U; ++index) {
                int operation_result = SALTS_OK;
                int result = tr_chaos_collect_command(
                    &nodes[index], TR_CHAOS_COMMAND_STATUS, NULL, 0U,
                    network, codec, safety, response_payload,
                    &operation_result);

                if (result != SALTS_OK) {
                    return result;
                }
                if (operation_result != SALTS_OK) {
                    return operation_result;
                }
                if (index == 0U) {
                    final_applied_index =
                        nodes[index].status.applied_index;
                    final_applied_hash = nodes[index].status.applied_hash;
                }
                if (nodes[index].status.applied_index <
                        target_applied_index ||
                    nodes[index].status.applied_index !=
                        final_applied_index ||
                    nodes[index].status.applied_hash !=
                        final_applied_hash) {
                    converged = 0;
                }
            }
            if (converged) {
                return SALTS_OK;
            }
        }
    }
    return SALTS_EPROTO;
}

static int tr_chaos_run_seed(const char *program,
                             const char *directory,
                             uint32_t seed)
{
    tr_chaos_process_node_t nodes[3];
    tr_chaos_network_t network;
    tr_chaos_safety_t safety;
    tr_raft_wire_codec_t *codec = NULL;
    uint8_t *response_payload = NULL;
    uint32_t random_state = seed * 0x9e3779b9U + 1U;
    int killed_node = -1;
    int accepted_proposals = 0;
    int leader_handoffs = 0;
    int follower_handoffs = 0;
    tr_raft_index_t post_handoff_target = 0U;
    uint32_t round = 0U;
    size_t index;
    int result = SALTS_OK;
    const char *stage = "allocate";

    memset(nodes, 0, sizeof(nodes));
    memset(&network, 0, sizeof(network));
    memset(&safety, 0, sizeof(safety));
    network.frames = (tr_chaos_frame_t *) calloc(
        TR_CHAOS_MAX_QUEUED_FRAMES, sizeof(*network.frames));
    response_payload = (uint8_t *) malloc(TR_CHAOS_MAX_RESPONSE_BYTES);
    if (network.frames == NULL || response_payload == NULL) {
        result = SALTS_ENOMEM;
        goto cleanup;
    }
    result = tr_raft_wire_codec_create(&codec);
    if (result != SALTS_OK) {
        goto cleanup;
    }
    for (index = 0U; index < 3U; ++index) {
        stage = "spawn";
        nodes[index].id = index + 1U;
        snprintf(nodes[index].database_path,
                 sizeof(nodes[index].database_path), "%s/seed-%u-node-%zu.db",
                 directory, seed, index + 1U);
        result = tr_chaos_node_spawn(&nodes[index], program, &safety,
                                     response_payload);
        if (result != SALTS_OK) {
            goto cleanup;
        }
    }

    for (round = 0U; round < TR_CHAOS_ROUND_COUNT; ++round) {
        int faults_enabled = (round >= 16U && round < 36U) ||
                             (round >= 68U && round < 84U);
        tr_raft_node_id_t partitioned_node =
            round >= 56U && round < 68U ? 1U : 0U;
        uint8_t tick[8];
        size_t delivery;

        if (round == 36U) {
            stage = "terminate-leader";
            for (index = 0U; index < 3U; ++index) {
                if (nodes[index].alive &&
                    nodes[index].status.role == TR_RAFT_LEADER) {
                    killed_node = (int) index;
                    break;
                }
            }
            if (killed_node < 0) {
                killed_node = 0;
            }
            result = tr_chaos_node_terminate(&nodes[killed_node]);
            if (result != SALTS_OK) {
                goto cleanup;
            }
        }
        if (round == 46U && killed_node >= 0) {
            stage = "restart-node";
            result = tr_chaos_node_spawn(&nodes[killed_node], program,
                                         &safety, response_payload);
            if (result != SALTS_OK) {
                goto cleanup;
            }
        }
        if (round == 12U || round == 24U) {
            tr_raft_role_t wanted_role = round == 12U
                                             ? TR_RAFT_LEADER
                                             : TR_RAFT_FOLLOWER;

            stage = round == 12U ? "backup-leader" : "backup-follower";
            for (index = 0U; index < 3U; ++index) {
                if (nodes[index].alive &&
                    nodes[index].status.role == wanted_role) {
                    result = tr_chaos_node_backup_handoff(
                        &nodes[index], &network, codec, &safety,
                        response_payload);
                    if (result != SALTS_OK) {
                        goto cleanup;
                    }
                    if (round == 12U) {
                        leader_handoffs++;
                    } else {
                        follower_handoffs++;
                    }
                    break;
                }
            }
        }

        for (index = 0U; index < 3U; ++index) {
            int operation_result = SALTS_OK;

            if (!nodes[index].alive) {
                continue;
            }
            tr_chaos_put_u32(tick, 1U);
            tr_chaos_put_u32(tick + 4U,
                             3U + tr_chaos_random(&random_state) % 4U);
            stage = "tick";
            result = tr_chaos_collect_command(
                &nodes[index], TR_CHAOS_COMMAND_TICK, tick, sizeof(tick),
                &network, codec, &safety, response_payload,
                &operation_result);
            if (result != SALTS_OK || operation_result != SALTS_OK) {
                result = result != SALTS_OK ? result : operation_result;
                goto cleanup;
            }
        }

        for (delivery = 0U;
             delivery < 12U && network.count != 0U;
             ++delivery) {
            stage = "deliver";
            result = tr_chaos_deliver_one(
                nodes, &network, codec, &safety, response_payload,
                &random_state, faults_enabled, partitioned_node);
            if (result != SALTS_OK) {
                goto cleanup;
            }
        }

        if (round == 14U || round == 28U || round == 52U ||
            round == 78U || round == 94U) {
            for (index = 0U; index < 3U; ++index) {
                if (nodes[index].alive &&
                    nodes[index].status.role == TR_RAFT_LEADER) {
                    uint8_t proposal[20];
                    int operation_result = SALTS_OK;

                    tr_chaos_put_u64(proposal,
                                     (uint64_t) seed * 1000U + round + 1U);
                    tr_chaos_put_u32(proposal + 8U, 8U);
                    tr_chaos_put_u32(proposal + 12U, seed);
                    tr_chaos_put_u32(proposal + 16U, round);
                    stage = "propose";
                    result = tr_chaos_collect_command(
                        &nodes[index], TR_CHAOS_COMMAND_PROPOSE, proposal,
                        sizeof(proposal), &network, codec, &safety,
                        response_payload, &operation_result);
                    if (result != SALTS_OK) {
                        goto cleanup;
                    }
                    if (operation_result == SALTS_OK) {
                        accepted_proposals++;
                        if (round > 24U) {
                            post_handoff_target =
                                nodes[index].status.last_log_index;
                        }
                    }
                    break;
                }
            }
        }
    }

    if (post_handoff_target == 0U) {
        stage = "post-handoff-proposal";
        result = SALTS_EPROTO;
        goto cleanup;
    }
    stage = "post-handoff-recovery";
    result = tr_chaos_recover_after_handoff(
        nodes, &network, codec, &safety, response_payload, &random_state,
        post_handoff_target);
    if (result != SALTS_OK) {
        goto cleanup;
    }
    if (accepted_proposals == 0 || safety.max_user_apply_count == 0U ||
        leader_handoffs == 0 || follower_handoffs == 0) {
        stage = "liveness";
        result = SALTS_EPROTO;
    }

cleanup:
    if (result != SALTS_OK) {
        fprintf(stderr,
                "chaos seed=%u stage=%s round=%u result=%d queue=%zu "
                "accepted=%d user_applied=%u "
                "n1={term=%llu role=%d commit=%llu applied=%llu} "
                "n2={term=%llu role=%d commit=%llu applied=%llu} "
                "n3={term=%llu role=%d commit=%llu applied=%llu}\n",
                seed, stage, round, result, network.count,
                accepted_proposals, safety.max_user_apply_count,
                (unsigned long long) nodes[0].status.term,
                (int) nodes[0].status.role,
                (unsigned long long) nodes[0].status.commit_index,
                (unsigned long long) nodes[0].status.applied_index,
                (unsigned long long) nodes[1].status.term,
                (int) nodes[1].status.role,
                (unsigned long long) nodes[1].status.commit_index,
                (unsigned long long) nodes[1].status.applied_index,
                (unsigned long long) nodes[2].status.term,
                (int) nodes[2].status.role,
                (unsigned long long) nodes[2].status.commit_index,
                (unsigned long long) nodes[2].status.applied_index);
    }
    for (index = 0U; index < 3U; ++index) {
        int close_result = tr_chaos_node_stop(&nodes[index],
                                              response_payload);
        if (result == SALTS_OK && close_result != SALTS_OK) {
            result = close_result;
        }
    }
    tr_raft_wire_codec_destroy(codec);
    free(response_payload);
    free(network.frames);
    return result;
}

spec("raft multi-process deterministic chaos")
{
    before_all()
    {
        tr_chaos_configured_first_text =
            getenv("TURBORAFT_CHAOS_FIRST_SEED");
        tr_chaos_configured_count_text =
            getenv("TURBORAFT_CHAOS_SEED_COUNT");
        tr_chaos_configuration_status = tr_chaos_parse_seed_range(
            tr_chaos_configured_first_text, tr_chaos_configured_count_text,
            &tr_chaos_configured_seed_range);
        if (tr_chaos_configuration_status != SALTS_OK) {
            fprintf(stderr,
                    "invalid chaos seed range: first=%s count=%s status=%d\n",
                    tr_chaos_configured_first_text != NULL
                        ? tr_chaos_configured_first_text
                        : "<default>",
                    tr_chaos_configured_count_text != NULL
                        ? tr_chaos_configured_count_text
                        : "<default>",
                    tr_chaos_configuration_status);
        }
        check_equal(tr_chaos_configuration_status, SALTS_OK);
    }

    it("uses the bounded default seed range when configuration is absent")
    {
        tr_chaos_seed_range_t range;

        check_equal(tr_chaos_parse_seed_range(NULL, NULL, &range), SALTS_OK);
        check_equal(range.first, UINT32_C(1));
        check_equal(range.count, UINT32_C(4));
    }

    it("accepts an explicit seed range")
    {
        tr_chaos_seed_range_t range;

        check_equal(tr_chaos_parse_seed_range("41", "2", &range), SALTS_OK);
        check_equal(range.first, UINT32_C(41));
        check_equal(range.count, UINT32_C(2));
    }

    it("rejects malformed and zero seed range values")
    {
        tr_chaos_seed_range_t range;

        check_equal(tr_chaos_parse_seed_range("", "1", &range),
                    SALTS_EINVAL);
        check_equal(tr_chaos_parse_seed_range("+1", "1", &range),
                    SALTS_EINVAL);
        check_equal(tr_chaos_parse_seed_range("-1", "1", &range),
                    SALTS_EINVAL);
        check_equal(tr_chaos_parse_seed_range(" 1", "1", &range),
                    SALTS_EINVAL);
        check_equal(tr_chaos_parse_seed_range("1 ", "1", &range),
                    SALTS_EINVAL);
        check_equal(tr_chaos_parse_seed_range("0", "1", &range),
                    SALTS_EINVAL);
        check_equal(tr_chaos_parse_seed_range("1x", "1", &range),
                    SALTS_EINVAL);
        check_equal(tr_chaos_parse_seed_range("1", "0", &range),
                    SALTS_EINVAL);
    }

    it("rejects seed counts and ranges beyond their hard bounds")
    {
        tr_chaos_seed_range_t range;

        check_equal(tr_chaos_parse_seed_range("1", "17", &range),
                    SALTS_ERANGE);
        check_equal(tr_chaos_parse_seed_range("4294967296", "1", &range),
                    SALTS_ERANGE);
        check_equal(
            tr_chaos_parse_seed_range("4294967295", "2", &range),
            SALTS_ERANGE);
    }

    it("accepts the maximum bounded seed ranges")
    {
        tr_chaos_seed_range_t range;

        check_equal(tr_chaos_parse_seed_range("1", "16", &range),
                    SALTS_OK);
        check_equal(range.first, UINT32_C(1));
        check_equal(range.count, UINT32_C(16));
        check_equal(
            tr_chaos_parse_seed_range("4294967295", "1", &range),
            SALTS_OK);
        check_equal(range.first, UINT32_MAX);
        check_equal(range.count, UINT32_C(1));
    }

    it("stops a node when WAL reopen fails after prepare")
    {
        const char *program = getenv("TURBORAFT_CHAOS_NODE");
        char *directory = NULL;
        tr_chaos_process_node_t node;
        tr_chaos_safety_t safety;
        tr_chaos_response_t response;
        salts_process_result_t process_result;
        uint8_t fail_reopen[4];
        uint8_t *response_payload = NULL;
        int result = SALTS_EINVAL;

        memset(&node, 0, sizeof(node));
        memset(&safety, 0, sizeof(safety));
        memset(&response, 0, sizeof(response));
        memset(&process_result, 0, sizeof(process_result));
        if (tr_chaos_configuration_status == SALTS_OK) {
            check_not_null(program);
            directory = tt_make_temp_dir("turboraft-backup-failure");
            response_payload =
                (uint8_t *) malloc(TR_CHAOS_MAX_RESPONSE_BYTES);
            check_not_null(directory);
            check_not_null(response_payload);
        }
        if (program != NULL && directory != NULL &&
            response_payload != NULL) {
            node.id = 1U;
            snprintf(node.database_path, sizeof(node.database_path),
                     "%s/node.db", directory);
            result = tr_chaos_node_spawn(&node, program, &safety,
                                         response_payload);
            check_equal(result, SALTS_OK);
        }
        if (result == SALTS_OK) {
            tr_chaos_put_u32(fail_reopen,
                             TR_CHAOS_BACKUP_HANDOFF_FAIL_REOPEN);
            result = tr_chaos_node_command(
                &node, TR_CHAOS_COMMAND_BACKUP_HANDOFF, fail_reopen,
                sizeof(fail_reopen), response_payload, &response);
            check_equal(result, SALTS_OK);
            check_equal(response.operation_result, SALTS_EIO);
        }
        if (result == SALTS_OK &&
            response.operation_result == SALTS_EIO) {
            result = salts_process_wait(node.process, &process_result);
            check_equal(result, SALTS_OK);
            check_equal(process_result.state, SALTS_PROCESS_EXITED);
            check_not_equal(process_result.exit_code, 0);
            salts_process_destroy(node.process);
            node.process = NULL;
            node.alive = 0;
        }
        if (node.alive) {
            check_equal(tr_chaos_node_terminate(&node), SALTS_OK);
        }
        if (directory != NULL) {
            check_equal(tt_remove_tree(directory), 0);
        }
        free(response_payload);
        free(directory);
    }

    it("preserves durable election and committed-log safety across faults")
    {
        const char *program = getenv("TURBORAFT_CHAOS_NODE");
        char *directory = NULL;
        uint32_t offset;
        uint32_t seed;

        if (tr_chaos_configuration_status == SALTS_OK) {
            check_not_null(program);
        }
        if (program != NULL && tr_chaos_configuration_status == SALTS_OK) {
            directory = tt_make_temp_dir("turboraft-multiprocess-chaos");
            check_not_null(directory);
        }
        if (directory != NULL) {
            for (offset = 0U; offset < tr_chaos_configured_seed_range.count;
                 ++offset) {
                seed = tr_chaos_configured_seed_range.first + offset;
                fprintf(stderr,
                        "chaos seed=%u started "
                        "reproduce_env=TURBORAFT_CHAOS_FIRST_SEED=%u,"
                        "TURBORAFT_CHAOS_SEED_COUNT=1\n",
                        seed, seed);
                check_equal(tr_chaos_run_seed(program, directory, seed),
                            SALTS_OK);
                fprintf(stderr, "chaos seed=%u completed\n", seed);
            }
            check_equal(tt_remove_tree(directory), 0);
        }
        free(directory);
    }
}
