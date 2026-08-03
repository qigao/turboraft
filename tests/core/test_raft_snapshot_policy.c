#include <turboraft/raft_service.h>

#include <tinytest.h>
#include <turbo_error.h>

#include <string.h>

typedef struct snapshot_policy_capture {
    size_t applied_count;
    size_t create_count;
    size_t store_count;
    tr_raft_snapshot_point_t stored;
    size_t stored_size;
    int store_result;
} snapshot_policy_capture_t;

static int storage_ok(void *context)
{
    (void) context;
    return TURBO_OK;
}

static int storage_hard_state(void *context, tr_raft_term_t term,
                              tr_raft_node_id_t vote)
{
    (void) context;
    (void) term;
    (void) vote;
    return TURBO_OK;
}

static int storage_truncate(void *context, tr_raft_index_t index)
{
    (void) context;
    (void) index;
    return TURBO_OK;
}

static int storage_append(void *context, const tr_raft_entry_t *entries,
                          size_t count)
{
    (void) context;
    (void) entries;
    (void) count;
    return TURBO_OK;
}

static int storage_commit_index(void *context, tr_raft_index_t index)
{
    (void) context;
    (void) index;
    return TURBO_OK;
}

static int transport_enqueue(void *context, const tr_raft_message_t *message)
{
    (void) context;
    (void) message;
    return TURBO_OK;
}

static int state_machine_apply(void *context, const tr_raft_entry_t *entries,
                               size_t count)
{
    snapshot_policy_capture_t *capture =
        (snapshot_policy_capture_t *) context;

    (void) entries;
    capture->applied_count += count;
    return TURBO_OK;
}

static int snapshot_create(void *context, tr_raft_index_t applied_index,
                           uint8_t *buffer, size_t capacity,
                           size_t *out_size)
{
    static const uint8_t data[] = {0x52U, 0x41U, 0x46U, 0x54U};
    snapshot_policy_capture_t *capture =
        (snapshot_policy_capture_t *) context;

    if (applied_index != 2U || capacity < sizeof(data)) {
        return TURBO_EINVAL;
    }
    memcpy(buffer, data, sizeof(data));
    *out_size = sizeof(data);
    ++capture->create_count;
    return TURBO_OK;
}

static int snapshot_store(void *context, tr_raft_index_t snapshot_index,
                          tr_raft_term_t snapshot_term,
                          const tr_raft_conf_t *configuration,
                          const uint8_t *data, size_t size)
{
    snapshot_policy_capture_t *capture =
        (snapshot_policy_capture_t *) context;

    (void) data;
    ++capture->store_count;
    capture->stored.index = snapshot_index;
    capture->stored.term = snapshot_term;
    capture->stored.configuration = *configuration;
    capture->stored_size = size;
    return capture->store_result;
}

static void snapshot_policy_config(tr_raft_service_config_t *config,
                                   snapshot_policy_capture_t *capture,
                                   tr_raft_entry_t entries[2])
{
    static const tr_raft_node_id_t voters[] = {1U};

    memset(config, 0, sizeof(*config));
    memset(entries, 0, 2U * sizeof(entries[0]));
    entries[0].index = 1U;
    entries[0].term = 1U;
    entries[0].command_id = 1U;
    entries[1].index = 2U;
    entries[1].term = 1U;
    entries[1].command_id = 2U;
    config->core.self_id = 1U;
    config->core.voters = voters;
    config->core.voter_count = 1U;
    config->core.heartbeat_ticks = 1U;
    config->core.election_min_ticks = 2U;
    config->core.election_max_ticks = 3U;
    config->core.initial_election_timeout_ticks = 2U;
    config->core.initial_term = 1U;
    config->core.initial_log_entries = entries;
    config->core.initial_log_entry_count = 2U;
    config->core.initial_commit_index = 2U;
    config->core.max_log_entries = 4U;
    config->storage.context = capture;
    config->storage.begin = storage_ok;
    config->storage.write_hard_state = storage_hard_state;
    config->storage.truncate_log = storage_truncate;
    config->storage.append_log = storage_append;
    config->storage.write_commit_index = storage_commit_index;
    config->storage.commit = storage_ok;
    config->storage.rollback = storage_ok;
    config->transport.context = capture;
    config->transport.enqueue = transport_enqueue;
    config->state_machine.context = capture;
    config->state_machine.apply_batch = state_machine_apply;
    config->snapshot_policy.applied_entry_threshold = 2U;
    config->snapshot_policy.max_snapshot_bytes = 16U;
    config->snapshot_policy.create = snapshot_create;
    config->snapshot_policy.create_context = capture;
    config->snapshot_policy.store = snapshot_store;
    config->snapshot_policy.store_context = capture;
}

spec("automatic snapshot policy")
{
    it("stores the applied boundary before compacting the live Core")
    {
        snapshot_policy_capture_t capture;
        tr_raft_service_config_t config;
        tr_raft_service_status_t status;
        tr_raft_service_t *service = NULL;
        tr_raft_entry_t entries[2];

        memset(&capture, 0, sizeof(capture));
        snapshot_policy_config(&config, &capture, entries);
        check_int_eq(tr_raft_service_create(&config, &service), TURBO_OK);
        check_int_eq(tr_raft_service_poll(service), TURBO_OK);
        check_int_eq(tr_raft_service_status(service, &status), TURBO_OK);
        check_size_eq(capture.applied_count, 2U);
        check_size_eq(capture.create_count, 1U);
        check_size_eq(capture.store_count, 1U);
        check_long_eq(capture.stored.index, 2U);
        check_long_eq(capture.stored.term, 1U);
        check_size_eq(capture.stored.configuration.member_count, 1U);
        check_size_eq(capture.stored_size, 4U);
        check_long_eq(status.core.log_base_index, 2U);
        check_size_eq(status.core.log_entry_count, 0U);
        check_false(status.faulted);
        tr_raft_service_destroy(service);
    }

    it("faults without compacting when durable snapshot storage fails")
    {
        snapshot_policy_capture_t capture;
        tr_raft_service_config_t config;
        tr_raft_service_status_t status;
        tr_raft_service_t *service = NULL;
        tr_raft_entry_t entries[2];

        memset(&capture, 0, sizeof(capture));
        capture.store_result = TURBO_EIO;
        snapshot_policy_config(&config, &capture, entries);
        check_int_eq(tr_raft_service_create(&config, &service), TURBO_OK);
        check_int_eq(tr_raft_service_poll(service), TURBO_EIO);
        check_int_eq(tr_raft_service_status(service, &status), TURBO_OK);
        check(status.faulted);
        check_int_eq(status.cause, TURBO_EIO);
        check_long_eq(status.core.log_base_index, 0U);
        check_size_eq(status.core.log_entry_count, 2U);
        tr_raft_service_destroy(service);
    }
}
