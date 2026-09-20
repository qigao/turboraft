#include "raft_multiprocess_protocol.h"

#include <turboraft/raft_service.h>
#include <turboraft/raft_wal_storage.h>
#include <turboraft/raft_wire_codec.h>

#include <salts_error.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#include <fcntl.h>
#include <io.h>
#endif

#define TR_CHAOS_NODE_MAX_LOG_ENTRIES 256U
#define TR_CHAOS_HASH_OFFSET UINT64_C(1469598103934665603)
#define TR_CHAOS_HASH_PRIME UINT64_C(1099511628211)

typedef struct tr_chaos_node {
    tr_raft_node_id_t node_id;
    tr_raft_cluster_id_t cluster_id;
    tr_raft_wal_storage_config_t storage_config;
    tr_raft_wal_storage_t *storage;
    tr_raft_service_t *service;
    tr_raft_wire_codec_t *codec;
    uint64_t next_message_id;
    uint64_t applied_hash;
    uint32_t user_apply_count;
    uint8_t *outbound;
    size_t outbound_size;
    uint32_t outbound_count;
} tr_chaos_node_t;

static int tr_chaos_stdio_read(void *output, size_t size)
{
    uint8_t *bytes = (uint8_t *) output;
    size_t total = 0U;

    while (total < size) {
        size_t count = fread(bytes + total, 1U, size - total, stdin);
        if (count == 0U) {
            return feof(stdin) ? SALTS_EOF : SALTS_EPROTO;
        }
        total += count;
    }
    return SALTS_OK;
}

static int tr_chaos_stdio_write(const void *input, size_t size)
{
    const uint8_t *bytes = (const uint8_t *) input;
    size_t total = 0U;

    while (total < size) {
        size_t count = fwrite(bytes + total, 1U, size - total, stdout);
        if (count == 0U) {
            return SALTS_EPIPE;
        }
        total += count;
    }
    return fflush(stdout) == 0 ? SALTS_OK : SALTS_EPIPE;
}

static void tr_chaos_fill_cluster(tr_raft_cluster_id_t *cluster)
{
    size_t index;

    for (index = 0U; index < sizeof(cluster->bytes); ++index) {
        cluster->bytes[index] = (uint8_t) (0x40U + index);
    }
}

static void tr_chaos_hash_bytes(uint64_t *hash,
                                const void *data,
                                size_t size)
{
    const uint8_t *bytes = (const uint8_t *) data;
    size_t index;

    for (index = 0U; index < size; ++index) {
        *hash ^= bytes[index];
        *hash *= TR_CHAOS_HASH_PRIME;
    }
}

static void tr_chaos_hash_u64(uint64_t *hash, uint64_t value)
{
    uint8_t bytes[8];

    tr_chaos_put_u64(bytes, value);
    tr_chaos_hash_bytes(hash, bytes, sizeof(bytes));
}

static int tr_chaos_apply(void *context,
                          const tr_raft_entry_t *entries,
                          size_t entry_count)
{
    tr_chaos_node_t *node = (tr_chaos_node_t *) context;
    size_t index;

    if (node == NULL || (entries == NULL && entry_count != 0U)) {
        return SALTS_EINVAL;
    }
    for (index = 0U; index < entry_count; ++index) {
        tr_chaos_hash_u64(&node->applied_hash, entries[index].index);
        tr_chaos_hash_u64(&node->applied_hash, entries[index].term);
        tr_chaos_hash_u64(&node->applied_hash, entries[index].command_id);
        tr_chaos_hash_bytes(&node->applied_hash, entries[index].data,
                            entries[index].data_length);
        if (entries[index].command_id != 0U) {
            node->user_apply_count++;
        }
    }
    return SALTS_OK;
}

static int tr_chaos_enqueue(void *context, const tr_raft_message_t *message)
{
    tr_chaos_node_t *node = (tr_chaos_node_t *) context;
    tr_raft_wire_metadata_t metadata;
    size_t frame_size = 0U;
    size_t remaining;
    int result;

    if (node == NULL || message == NULL ||
        node->outbound_size + 4U >= TR_CHAOS_MAX_RESPONSE_BYTES) {
        return SALTS_ENOSPC;
    }
    remaining = TR_CHAOS_MAX_RESPONSE_BYTES - node->outbound_size - 4U;
    memset(&metadata, 0, sizeof(metadata));
    metadata.cluster_id = node->cluster_id;
    metadata.group_id = TR_CHAOS_DEFAULT_GROUP_ID;
    metadata.message_id = node->next_message_id;
    result = tr_raft_wire_encode(
        node->codec, &metadata, message,
        node->outbound + node->outbound_size + 4U, remaining, &frame_size);
    if (result != SALTS_OK || frame_size > UINT32_MAX) {
        return result != SALTS_OK ? result : SALTS_EPROTO;
    }
    tr_chaos_put_u32(node->outbound + node->outbound_size,
                     (uint32_t) frame_size);
    node->outbound_size += 4U + frame_size;
    node->outbound_count++;
    node->next_message_id++;
    return SALTS_OK;
}

static int tr_chaos_node_open(tr_chaos_node_t *node,
                              tr_raft_node_id_t node_id,
                              const char *database_path)
{
    static const tr_raft_node_id_t voters[] = {1U, 2U, 3U};
    tr_raft_wal_recovery_t recovery;
    tr_raft_storage_t storage_adapter;
    tr_raft_service_config_t service_config;
    int result;

    memset(node, 0, sizeof(*node));
    memset(&node->storage_config, 0, sizeof(node->storage_config));
    memset(&recovery, 0, sizeof(recovery));
    memset(&storage_adapter, 0, sizeof(storage_adapter));
    memset(&service_config, 0, sizeof(service_config));
    node->node_id = node_id;
    node->next_message_id = 1U;
    node->applied_hash = TR_CHAOS_HASH_OFFSET;
    tr_chaos_fill_cluster(&node->cluster_id);
    node->outbound = (uint8_t *) malloc(TR_CHAOS_MAX_RESPONSE_BYTES);
    if (node->outbound == NULL) {
        return SALTS_ENOMEM;
    }
    result = tr_raft_wire_codec_create(&node->codec);
    if (result != SALTS_OK) {
        free(node->outbound);
        node->outbound = NULL;
        return result;
    }

    node->storage_config.path_prefix = database_path;
    node->storage_config.segment_bytes = TR_RAFT_WAL_MIN_SEGMENT_BYTES;
    node->storage_config.max_transaction_bytes = 32U * 1024U;
    node->storage_config.max_segments = 64U;
    node->storage_config.max_log_entries = TR_CHAOS_NODE_MAX_LOG_ENTRIES;
    node->storage_config.create_if_missing = true;
    node->storage_config.max_snapshot_bytes = 1024U * 1024U;
    result = tr_raft_wal_storage_open(&node->storage_config, &node->storage);
    if (result == SALTS_OK) {
        result = tr_raft_wal_storage_bind(node->storage, &storage_adapter);
    }
    if (result == SALTS_OK) {
        result = tr_raft_wal_storage_load(node->storage, &recovery);
    }
    if (result != SALTS_OK) {
        if (node->storage != NULL) {
            tr_raft_wal_storage_close(node->storage);
        }
        tr_raft_wire_codec_destroy(node->codec);
        free(node->outbound);
        memset(node, 0, sizeof(*node));
        return result;
    }

    service_config.core.self_id = node_id;
    service_config.core.voters = voters;
    service_config.core.voter_count = 3U;
    service_config.core.heartbeat_ticks = 1U;
    service_config.core.election_min_ticks = 3U;
    service_config.core.election_max_ticks = 7U;
    service_config.core.initial_election_timeout_ticks =
        (uint32_t) (2U + node_id);
    service_config.core.initial_term = recovery.term;
    service_config.core.initial_vote = recovery.voted_for;
    service_config.core.initial_last_log_index = recovery.snapshot_index;
    service_config.core.initial_last_log_term = recovery.snapshot_term;
    service_config.core.initial_log_entries = recovery.entry_count == 0U
                                                  ? NULL
                                                  : recovery.entries;
    service_config.core.initial_log_entry_count = recovery.entry_count;
    service_config.core.initial_commit_index = recovery.commit_index;
    service_config.core.initial_applied_index = recovery.snapshot_index;
    service_config.core.max_log_entries = TR_CHAOS_NODE_MAX_LOG_ENTRIES;
    service_config.storage = storage_adapter;
    service_config.transport.context = node;
    service_config.transport.enqueue = tr_chaos_enqueue;
    service_config.state_machine.context = node;
    service_config.state_machine.apply_batch = tr_chaos_apply;
    result = tr_raft_service_create(&service_config, &node->service);
    tr_raft_wal_recovery_destroy(&recovery);
    if (result == SALTS_OK) {
        result = tr_raft_service_poll(node->service);
    }
    if (result != SALTS_OK) {
        if (node->service != NULL) {
            tr_raft_service_destroy(node->service);
        }
        tr_raft_wal_storage_close(node->storage);
        tr_raft_wire_codec_destroy(node->codec);
        free(node->outbound);
        memset(node, 0, sizeof(*node));
    }
    return result;
}

static void tr_chaos_node_close(tr_chaos_node_t *node)
{
    if (node->service != NULL) {
        tr_raft_service_destroy(node->service);
    }
    if (node->storage != NULL) {
        tr_raft_wal_storage_close(node->storage);
    }
    tr_raft_wire_codec_destroy(node->codec);
    free(node->outbound);
    memset(node, 0, sizeof(*node));
}

static int tr_chaos_node_backup_handoff(
    tr_chaos_node_t *node,
    tr_chaos_backup_handoff_mode_t mode)
{
    tr_raft_wal_storage_t *replacement = NULL;
    tr_raft_storage_t replacement_adapter;
    int result;

    memset(&replacement_adapter, 0, sizeof(replacement_adapter));
    result = tr_raft_service_prepare_backup(node->service);
    if (result != SALTS_OK) {
        return result;
    }
    result = tr_raft_wal_storage_close(node->storage);
    node->storage = NULL;
    if (result != SALTS_OK) {
        return result;
    }
    result = mode == TR_CHAOS_BACKUP_HANDOFF_FAIL_REOPEN
                 ? SALTS_EIO
                 : tr_raft_wal_storage_open(&node->storage_config,
                                            &replacement);
    if (result == SALTS_OK) {
        result = tr_raft_wal_storage_bind(replacement, &replacement_adapter);
    }
    if (result == SALTS_OK) {
        result = tr_raft_service_resume_backup(node->service,
                                               &replacement_adapter);
    }
    if (result != SALTS_OK) {
        if (replacement != NULL) {
            tr_raft_wal_storage_close(replacement);
        }
        return result;
    }
    node->storage = replacement;
    return SALTS_OK;
}

static int tr_chaos_execute(tr_chaos_node_t *node,
                            tr_raft_group_id_t group_id,
                            tr_chaos_command_kind_t kind,
                            const uint8_t *payload,
                            size_t payload_size)
{
    if (kind != TR_CHAOS_COMMAND_STOP &&
        group_id != TR_CHAOS_DEFAULT_GROUP_ID) {
        return SALTS_ENOENT;
    }
    if (kind == TR_CHAOS_COMMAND_STOP && group_id != 0U) {
        return SALTS_EINVAL;
    }
    switch (kind) {
    case TR_CHAOS_COMMAND_TICK:
        if (payload_size == 8U) {
            tr_raft_tick_t tick;

            tick.elapsed_ticks = tr_chaos_get_u32(payload);
            tick.next_election_timeout_ticks =
                tr_chaos_get_u32(payload + 4U);
            return tr_raft_service_tick(node->service, &tick);
        }
        return SALTS_EINVAL;
    case TR_CHAOS_COMMAND_STEP:
        if (payload_size != 0U) {
            tr_raft_wire_metadata_t metadata;
            tr_raft_message_t message;
            int result = tr_raft_wire_decode(node->codec, payload,
                                             payload_size, &metadata,
                                             &message);

            if (result != SALTS_OK ||
                metadata.group_id != group_id ||
                memcmp(metadata.cluster_id.bytes, node->cluster_id.bytes,
                       sizeof(node->cluster_id.bytes)) != 0 ||
                message.to != node->node_id) {
                return SALTS_EPROTO;
            }
            return tr_raft_service_step(node->service, &message);
        }
        return SALTS_EINVAL;
    case TR_CHAOS_COMMAND_PROPOSE:
        if (payload_size >= 12U &&
            tr_chaos_get_u32(payload + 8U) == payload_size - 12U) {
            tr_raft_proposal_t proposal;

            proposal.command_id = tr_chaos_get_u64(payload);
            proposal.data_length = tr_chaos_get_u32(payload + 8U);
            proposal.data = payload + 12U;
            return tr_raft_service_propose(node->service, &proposal);
        }
        return SALTS_EINVAL;
    case TR_CHAOS_COMMAND_BACKUP_HANDOFF:
        if (payload_size == 0U) {
            return tr_chaos_node_backup_handoff(
                node, TR_CHAOS_BACKUP_HANDOFF_NORMAL);
        }
        if (payload_size == sizeof(uint32_t) &&
            tr_chaos_get_u32(payload) ==
                TR_CHAOS_BACKUP_HANDOFF_FAIL_REOPEN) {
            return tr_chaos_node_backup_handoff(
                node, TR_CHAOS_BACKUP_HANDOFF_FAIL_REOPEN);
        }
        return SALTS_EINVAL;
    case TR_CHAOS_COMMAND_STATUS:
    case TR_CHAOS_COMMAND_STOP:
        return payload_size == 0U ? SALTS_OK : SALTS_EINVAL;
    default:
        return SALTS_EPROTO;
    }
}

static int tr_chaos_respond(tr_chaos_node_t *node,
                            tr_raft_group_id_t group_id,
                            uint32_t request_id,
                            int operation_result)
{
    tr_raft_service_status_t status;
    uint8_t header[TR_CHAOS_RESPONSE_HEADER_SIZE];
    int result = tr_raft_service_status(node->service, &status);

    if (result != SALTS_OK) {
        return result;
    }
    memset(header, 0, sizeof(header));
    memcpy(header, tr_chaos_response_magic,
           sizeof(tr_chaos_response_magic));
    tr_chaos_put_u16(header + 4U, TR_CHAOS_PROTOCOL_VERSION);
    tr_chaos_put_u32(header + 8U, request_id);
    tr_chaos_put_u32(header + 12U, (uint32_t) operation_result);
    tr_chaos_put_u32(header + 16U, node->outbound_count);
    tr_chaos_put_u32(header + 20U, (uint32_t) node->outbound_size);
    tr_chaos_put_u64(header + 24U, status.core.self_id);
    tr_chaos_put_u32(header + 32U, (uint32_t) status.core.role);
    tr_chaos_put_u32(header + 36U, status.faulted ? 1U : 0U);
    tr_chaos_put_u32(header + 40U, (uint32_t) status.cause);
    tr_chaos_put_u32(header + 44U, node->user_apply_count);
    tr_chaos_put_u64(header + 48U, status.core.term);
    tr_chaos_put_u64(header + 56U, status.core.leader_id);
    tr_chaos_put_u64(header + 64U, status.core.last_log_index);
    tr_chaos_put_u64(header + 72U, status.core.commit_index);
    tr_chaos_put_u64(header + 80U, status.core.applied_index);
    tr_chaos_put_u64(header + 88U, node->applied_hash);
    tr_chaos_put_u64(header + TR_CHAOS_RESPONSE_GROUP_OFFSET, group_id);
    result = tr_chaos_stdio_write(header, sizeof(header));
    if (result == SALTS_OK && node->outbound_size != 0U) {
        result = tr_chaos_stdio_write(node->outbound, node->outbound_size);
    }
    return result;
}

int main(int argc, char **argv)
{
    tr_chaos_node_t node;
    tr_raft_node_id_t node_id;
    uint8_t header[TR_CHAOS_COMMAND_HEADER_SIZE];
    uint8_t *payload;
    int exit_code = 0;

    if (argc != 5 || strcmp(argv[1], "--node") != 0 ||
        strcmp(argv[3], "--db") != 0) {
        return 2;
    }
    node_id = (tr_raft_node_id_t) strtoull(argv[2], NULL, 10);
    if (node_id == 0U || node_id > 3U || argv[4][0] == '\0') {
        return 2;
    }
#ifdef _WIN32
    if (_setmode(_fileno(stdin), _O_BINARY) == -1 ||
        _setmode(_fileno(stdout), _O_BINARY) == -1) {
        return 3;
    }
#endif
    payload = (uint8_t *) malloc(TR_CHAOS_MAX_FRAME_BYTES);
    if (payload == NULL) {
        return 4;
    }
    if (tr_chaos_node_open(&node, node_id, argv[4]) != SALTS_OK) {
        free(payload);
        return 5;
    }
    setvbuf(stdout, NULL, _IONBF, 0);

    for (;;) {
        uint32_t request_id;
        uint32_t payload_size;
        tr_raft_group_id_t group_id;
        tr_chaos_command_kind_t kind;
        int result = tr_chaos_stdio_read(header, sizeof(header));

        if (result == SALTS_EOF) {
            break;
        }
        if (result != SALTS_OK ||
            memcmp(header, tr_chaos_command_magic,
                   sizeof(tr_chaos_command_magic)) != 0 ||
            tr_chaos_get_u16(header + 4U) != TR_CHAOS_PROTOCOL_VERSION) {
            exit_code = 5;
            break;
        }
        kind = (tr_chaos_command_kind_t) tr_chaos_get_u16(header + 6U);
        request_id = tr_chaos_get_u32(header + 8U);
        payload_size = tr_chaos_get_u32(header + 12U);
        group_id = tr_chaos_get_u64(
            header + TR_CHAOS_COMMAND_GROUP_OFFSET);
        if (payload_size > TR_CHAOS_MAX_FRAME_BYTES ||
            (payload_size != 0U &&
             tr_chaos_stdio_read(payload, payload_size) != SALTS_OK)) {
            exit_code = 7;
            break;
        }
        node.outbound_size = 0U;
        node.outbound_count = 0U;
        result = tr_chaos_execute(
            &node, group_id, kind, payload, payload_size);
        if (tr_chaos_respond(
                &node, group_id, request_id, result) != SALTS_OK) {
            exit_code = 8;
            break;
        }
        if (kind == TR_CHAOS_COMMAND_BACKUP_HANDOFF &&
            result != SALTS_OK && node.storage == NULL) {
            exit_code = 9;
            break;
        }
        if (kind == TR_CHAOS_COMMAND_STOP) {
            break;
        }
    }
    tr_chaos_node_close(&node);
    free(payload);
    return exit_code;
}
