#include <turboraft/raft_apply_runtime.h>

#include <salts_error.h>
#include <tinytest.h>

#include <string.h>

typedef struct apply_probe {
    tr_raft_apply_admission_t next_admission;
    tr_raft_apply_settlement_t settlement;
    int poll_result;
    bool settlement_ready;
    size_t apply_calls;
    tr_raft_index_t applied_indexes[4];
    uint64_t tokens[4];
} apply_probe_t;

typedef struct io_probe {
    size_t begin_calls;
    size_t commit_calls;
    size_t enqueue_calls;
} io_probe_t;

static tr_raft_entry_t make_entry(tr_raft_index_t index, const char *payload)
{
    tr_raft_entry_t entry;

    memset(&entry, 0, sizeof(entry));
    entry.index = index;
    entry.term = 1U;
    entry.command_id = 100U + index;
    entry.data_length = strlen(payload);
    memcpy(entry.data, payload, entry.data_length);
    return entry;
}

static int create_restored_core(const tr_raft_entry_t *entries,
                                size_t entry_count,
                                tr_raft_core_t **out_core)
{
    static const tr_raft_node_id_t voter[] = {1U};
    tr_raft_core_config_t config;

    memset(&config, 0, sizeof(config));
    config.self_id = 1U;
    config.voters = voter;
    config.voter_count = 1U;
    config.heartbeat_ticks = 2U;
    config.election_min_ticks = 5U;
    config.election_max_ticks = 10U;
    config.initial_election_timeout_ticks = 5U;
    config.initial_term = 1U;
    config.initial_log_entries = entries;
    config.initial_log_entry_count = entry_count;
    config.initial_commit_index = (tr_raft_index_t) entry_count;
    config.max_log_entries = 8U;
    return tr_raft_core_create(&config, out_core);
}

static int storage_begin(void *context)
{
    ++((io_probe_t *) context)->begin_calls;
    return SALTS_OK;
}

static int storage_noop_hard(void *context,
                             tr_raft_term_t term,
                             tr_raft_node_id_t vote)
{
    (void) context;
    (void) term;
    (void) vote;
    return SALTS_OK;
}

static int storage_noop_truncate(void *context, tr_raft_index_t index)
{
    (void) context;
    (void) index;
    return SALTS_OK;
}

static int storage_noop_append(void *context,
                               const tr_raft_entry_t *entries,
                               size_t entry_count)
{
    (void) context;
    (void) entries;
    (void) entry_count;
    return SALTS_OK;
}

static int storage_noop_commit_index(void *context, tr_raft_index_t index)
{
    (void) context;
    (void) index;
    return SALTS_OK;
}

static int storage_commit(void *context)
{
    ++((io_probe_t *) context)->commit_calls;
    return SALTS_OK;
}

static int storage_rollback(void *context)
{
    (void) context;
    return SALTS_OK;
}

static int transport_enqueue(void *context, const tr_raft_message_t *message)
{
    (void) message;
    ++((io_probe_t *) context)->enqueue_calls;
    return SALTS_OK;
}

static tr_raft_apply_admission_t try_apply(void *context,
                                           const tr_raft_entry_t *entry,
                                           uint64_t token,
                                           int *out_cause)
{
    apply_probe_t *probe = (apply_probe_t *) context;
    size_t call = probe->apply_calls++;
    tr_raft_apply_admission_t admission = probe->next_admission;

    probe->applied_indexes[call] = entry->index;
    probe->tokens[call] = token;
    *out_cause = admission == TR_RAFT_APPLY_ADMISSION_FAILED
                     ? SALTS_EIO
                     : SALTS_OK;
    if (admission == TR_RAFT_APPLY_ADMISSION_FULL) {
        probe->next_admission = TR_RAFT_APPLY_ADMISSION_ACCEPTED;
    }
    return admission;
}

static int poll_settlement(void *context,
                           tr_raft_apply_settlement_t *out_settlement,
                           bool *out_ready)
{
    apply_probe_t *probe = (apply_probe_t *) context;

    if (probe->poll_result != SALTS_OK) {
        return probe->poll_result;
    }
    *out_ready = probe->settlement_ready;
    if (probe->settlement_ready) {
        *out_settlement = probe->settlement;
        probe->settlement_ready = false;
    }
    return SALTS_OK;
}

static tr_raft_apply_runtime_config_v1_t runtime_config(
    tr_raft_core_t *core,
    io_probe_t *io,
    apply_probe_t *apply,
    size_t max_pending_entries)
{
    tr_raft_apply_runtime_config_v1_t config;

    memset(&config, 0, sizeof(config));
    config.abi_version = TR_RAFT_APPLY_RUNTIME_CONFIG_ABI_V1;
    config.struct_size = sizeof(config);
    config.core = core;
    config.max_pending_entries = max_pending_entries;
    config.storage.context = io;
    config.storage.begin = storage_begin;
    config.storage.write_hard_state = storage_noop_hard;
    config.storage.truncate_log = storage_noop_truncate;
    config.storage.append_log = storage_noop_append;
    config.storage.write_commit_index = storage_noop_commit_index;
    config.storage.commit = storage_commit;
    config.storage.rollback = storage_rollback;
    config.transport.context = io;
    config.transport.enqueue = transport_enqueue;
    config.state_machine.abi_version = TR_RAFT_ENTRY_STATE_MACHINE_ABI_V1;
    config.state_machine.struct_size = sizeof(config.state_machine);
    config.state_machine.context = apply;
    config.state_machine.try_apply = try_apply;
    config.state_machine.poll_settlement = poll_settlement;
    return config;
}

static void make_ready(tr_raft_core_t *core,
                       tr_raft_ready_t *ready,
                       tr_raft_message_t *message)
{
    memset(ready, 0, sizeof(*ready));
    ready->messages = message;
    ready->message_capacity = 1U;
    check_equal(tr_raft_core_poll(core, ready), SALTS_OK);
    ready->hard_state_changed = true;
    ready->term = 1U;
    ready->message_count = 1U;
}

spec("bounded Raft entry apply runtime")
{
    it("advances only the settled prefix when a later entry is unknown")
    {
        tr_raft_entry_t entries[2] = {make_entry(1U, "one"),
                                      make_entry(2U, "two")};
        tr_raft_core_t *core = NULL;
        tr_raft_apply_runtime_t *runtime = NULL;
        tr_raft_apply_runtime_result_t result;
        tr_raft_apply_runtime_config_v1_t config;
        tr_raft_ready_t ready;
        tr_raft_message_t message;
        tr_raft_status_t status;
        io_probe_t io = {0};
        apply_probe_t apply = {0};

        apply.next_admission = TR_RAFT_APPLY_ADMISSION_ACCEPTED;
        check_equal(create_restored_core(entries, 2U, &core), SALTS_OK);
        make_ready(core, &ready, &message);
        config = runtime_config(core, &io, &apply, 2U);
        check_equal(tr_raft_apply_runtime_create(&config, &runtime), SALTS_OK);

        check_equal(tr_raft_apply_runtime_start(runtime, &ready, &result),
                    SALTS_OK);
        check_equal(result.state,
                    TR_RAFT_APPLY_RUNTIME_WAITING_SETTLEMENT);
        check_equal(apply.apply_calls, 1U);
        check_equal(apply.applied_indexes[0], 1U);
        check_equal(apply.tokens[0], 1U);
        check_equal(io.begin_calls, 1U);
        check_equal(io.commit_calls, 1U);
        check_equal(io.enqueue_calls, 1U);
        check_equal(tr_raft_core_status(core, &status), SALTS_OK);
        check_false(status.ready_outstanding);

        apply.settlement.token = 1U;
        apply.settlement.outcome = TR_RAFT_APPLY_OUTCOME_APPLIED;
        apply.settlement.cause = SALTS_OK;
        apply.settlement_ready = true;
        check_equal(tr_raft_apply_runtime_poll(runtime, &result), SALTS_OK);
        check_equal(result.state,
                    TR_RAFT_APPLY_RUNTIME_WAITING_SETTLEMENT);
        check_equal(result.applied_through, 1U);
        check_equal(apply.apply_calls, 2U);
        check_equal(apply.applied_indexes[1], 2U);
        check_equal(tr_raft_core_status(core, &status), SALTS_OK);
        check_equal(status.applied_index, 1U);

        apply.settlement.token = 2U;
        apply.settlement.outcome = TR_RAFT_APPLY_OUTCOME_UNKNOWN;
        apply.settlement.cause = SALTS_EIO;
        apply.settlement_ready = true;
        check_equal(tr_raft_apply_runtime_poll(runtime, &result), SALTS_EIO);
        check_equal(result.state, TR_RAFT_APPLY_RUNTIME_FAULTED);
        check_true(tr_raft_apply_runtime_is_faulted(runtime));
        check_equal(tr_raft_core_status(core, &status), SALTS_OK);
        check_equal(status.applied_index, 1U);
        check_false(status.ready_outstanding);
        check_equal(io.begin_calls, 1U);
        check_equal(io.commit_calls, 1U);
        check_equal(io.enqueue_calls, 1U);

        check_equal(tr_raft_apply_runtime_destroy(runtime), SALTS_OK);
        tr_raft_core_destroy(core);
    }

    it("accepts an exact APPLIED reconciliation without replaying the entry")
    {
        tr_raft_entry_t entry = make_entry(1U, "one");
        tr_raft_core_t *core = NULL;
        tr_raft_apply_runtime_t *runtime = NULL;
        tr_raft_apply_runtime_result_t result;
        tr_raft_apply_runtime_config_v1_t config;
        tr_raft_ready_t ready;
        tr_raft_message_t message;
        tr_raft_status_t status;
        io_probe_t io = {0};
        apply_probe_t apply = {0};

        apply.next_admission = TR_RAFT_APPLY_ADMISSION_ACCEPTED;
        check_equal(create_restored_core(&entry, 1U, &core), SALTS_OK);
        make_ready(core, &ready, &message);
        config = runtime_config(core, &io, &apply, 1U);
        check_equal(tr_raft_apply_runtime_create(&config, &runtime), SALTS_OK);
        check_equal(tr_raft_apply_runtime_start(runtime, &ready, &result),
                    SALTS_OK);

        apply.settlement.token = 1U;
        apply.settlement.outcome = TR_RAFT_APPLY_OUTCOME_UNKNOWN;
        apply.settlement.cause = SALTS_EIO;
        apply.settlement_ready = true;
        check_equal(tr_raft_apply_runtime_poll(runtime, &result), SALTS_EIO);
        check_equal(result.state, TR_RAFT_APPLY_RUNTIME_FAULTED);
        check_equal(result.in_flight_token, 1U);

        check_equal(tr_raft_apply_runtime_reconcile(
                        runtime, &entry, TR_RAFT_APPLY_OUTCOME_APPLIED,
                        &result),
                    SALTS_OK);
        check_equal(result.state, TR_RAFT_APPLY_RUNTIME_COMPLETE);
        check_equal(result.applied_through, 1U);
        check_equal(result.in_flight_token, 0U);
        check_equal(apply.apply_calls, 1U);
        check_equal(tr_raft_core_status(core, &status), SALTS_OK);
        check_equal(status.applied_index, 1U);

        check_equal(tr_raft_apply_runtime_destroy(runtime), SALTS_OK);
        tr_raft_core_destroy(core);
    }

    it("retries only the exact entry after a PENDING reconciliation")
    {
        tr_raft_entry_t entry = make_entry(1U, "one");
        tr_raft_core_t *core = NULL;
        tr_raft_apply_runtime_t *runtime = NULL;
        tr_raft_apply_runtime_result_t result;
        tr_raft_apply_runtime_config_v1_t config;
        tr_raft_ready_t ready;
        tr_raft_message_t message;
        tr_raft_status_t status;
        io_probe_t io = {0};
        apply_probe_t apply = {0};

        apply.next_admission = TR_RAFT_APPLY_ADMISSION_ACCEPTED;
        check_equal(create_restored_core(&entry, 1U, &core), SALTS_OK);
        make_ready(core, &ready, &message);
        config = runtime_config(core, &io, &apply, 1U);
        check_equal(tr_raft_apply_runtime_create(&config, &runtime), SALTS_OK);
        check_equal(tr_raft_apply_runtime_start(runtime, &ready, &result),
                    SALTS_OK);

        apply.settlement.token = 1U;
        apply.settlement.outcome = TR_RAFT_APPLY_OUTCOME_GAP;
        apply.settlement.cause = SALTS_EPROTO;
        apply.settlement_ready = true;
        check_equal(tr_raft_apply_runtime_poll(runtime, &result),
                    SALTS_EPROTO);
        check_equal(tr_raft_apply_runtime_reconcile(
                        runtime, &entry, TR_RAFT_APPLY_OUTCOME_PENDING,
                        &result),
                    SALTS_OK);
        check_equal(result.state,
                    TR_RAFT_APPLY_RUNTIME_WAITING_SETTLEMENT);
        check_equal(result.in_flight_token, 1U);
        check_equal(apply.apply_calls, 2U);
        check_equal(apply.tokens[0], 1U);
        check_equal(apply.tokens[1], 1U);
        check_equal(tr_raft_core_status(core, &status), SALTS_OK);
        check_equal(status.applied_index, 0U);

        apply.settlement.outcome = TR_RAFT_APPLY_OUTCOME_APPLIED;
        apply.settlement.cause = SALTS_OK;
        apply.settlement_ready = true;
        check_equal(tr_raft_apply_runtime_poll(runtime, &result), SALTS_OK);
        check_equal(result.state, TR_RAFT_APPLY_RUNTIME_COMPLETE);
        check_equal(tr_raft_core_status(core, &status), SALTS_OK);
        check_equal(status.applied_index, 1U);

        check_equal(tr_raft_apply_runtime_destroy(runtime), SALTS_OK);
        tr_raft_core_destroy(core);
    }

    it("keeps a recoverable fault when reconciliation identity is not exact")
    {
        tr_raft_entry_t entry = make_entry(1U, "one");
        tr_raft_entry_t mismatch;
        tr_raft_core_t *core = NULL;
        tr_raft_apply_runtime_t *runtime = NULL;
        tr_raft_apply_runtime_result_t result;
        tr_raft_apply_runtime_config_v1_t config;
        tr_raft_ready_t ready;
        tr_raft_message_t message;
        tr_raft_status_t status;
        io_probe_t io = {0};
        apply_probe_t apply = {0};

        apply.next_admission = TR_RAFT_APPLY_ADMISSION_ACCEPTED;
        check_equal(create_restored_core(&entry, 1U, &core), SALTS_OK);
        make_ready(core, &ready, &message);
        config = runtime_config(core, &io, &apply, 1U);
        check_equal(tr_raft_apply_runtime_create(&config, &runtime), SALTS_OK);
        check_equal(tr_raft_apply_runtime_start(runtime, &ready, &result),
                    SALTS_OK);
        apply.settlement.token = 1U;
        apply.settlement.outcome = TR_RAFT_APPLY_OUTCOME_CONFLICT;
        apply.settlement.cause = SALTS_EPROTO;
        apply.settlement_ready = true;
        check_equal(tr_raft_apply_runtime_poll(runtime, &result),
                    SALTS_EPROTO);

        mismatch = entry;
        mismatch.term = 2U;
        check_equal(tr_raft_apply_runtime_reconcile(
                        runtime, &mismatch, TR_RAFT_APPLY_OUTCOME_APPLIED,
                        &result),
                    SALTS_EPROTO);
        mismatch = entry;
        mismatch.command_id += 1U;
        check_equal(tr_raft_apply_runtime_reconcile(
                        runtime, &mismatch, TR_RAFT_APPLY_OUTCOME_APPLIED,
                        &result),
                    SALTS_EPROTO);
        mismatch = entry;
        mismatch.data_length -= 1U;
        check_equal(tr_raft_apply_runtime_reconcile(
                        runtime, &mismatch, TR_RAFT_APPLY_OUTCOME_APPLIED,
                        &result),
                    SALTS_EPROTO);
        mismatch = entry;
        mismatch.data[0] ^= 0x01U;
        check_equal(tr_raft_apply_runtime_reconcile(
                        runtime, &mismatch, TR_RAFT_APPLY_OUTCOME_APPLIED,
                        &result),
                    SALTS_EPROTO);
        check_equal(result.state, TR_RAFT_APPLY_RUNTIME_FAULTED);
        check_equal(result.cause, SALTS_EPROTO);
        check_equal(result.in_flight_token, 1U);
        check_equal(tr_raft_apply_runtime_reconcile(
                        runtime, &entry, TR_RAFT_APPLY_OUTCOME_UNKNOWN,
                        &result),
                    SALTS_EINVAL);
        check_equal(result.state, TR_RAFT_APPLY_RUNTIME_FAULTED);
        check_equal(result.in_flight_token, 1U);
        check_equal(tr_raft_core_status(core, &status), SALTS_OK);
        check_equal(status.applied_index, 0U);

        check_equal(tr_raft_apply_runtime_reconcile(
                        runtime, &entry, TR_RAFT_APPLY_OUTCOME_APPLIED,
                        &result),
                    SALTS_OK);
        check_equal(result.state, TR_RAFT_APPLY_RUNTIME_COMPLETE);
        check_equal(tr_raft_apply_runtime_destroy(runtime), SALTS_OK);
        tr_raft_core_destroy(core);
    }

    it("does not reconcile a state-machine callback failure")
    {
        tr_raft_entry_t entry = make_entry(1U, "one");
        tr_raft_core_t *core = NULL;
        tr_raft_apply_runtime_t *runtime = NULL;
        tr_raft_apply_runtime_result_t result;
        tr_raft_apply_runtime_config_v1_t config;
        tr_raft_ready_t ready;
        tr_raft_message_t message;
        tr_raft_status_t status;
        io_probe_t io = {0};
        apply_probe_t apply = {0};

        apply.next_admission = TR_RAFT_APPLY_ADMISSION_ACCEPTED;
        apply.poll_result = SALTS_EIO;
        check_equal(create_restored_core(&entry, 1U, &core), SALTS_OK);
        make_ready(core, &ready, &message);
        config = runtime_config(core, &io, &apply, 1U);
        check_equal(tr_raft_apply_runtime_create(&config, &runtime), SALTS_OK);
        check_equal(tr_raft_apply_runtime_start(runtime, &ready, &result),
                    SALTS_OK);
        check_equal(tr_raft_apply_runtime_poll(runtime, &result), SALTS_EIO);
        check_equal(result.state, TR_RAFT_APPLY_RUNTIME_FAULTED);
        check_equal(result.in_flight_token, 0U);
        check_equal(tr_raft_apply_runtime_reconcile(
                        runtime, &entry, TR_RAFT_APPLY_OUTCOME_APPLIED,
                        &result),
                    SALTS_EPROTO);
        check_equal(tr_raft_core_status(core, &status), SALTS_OK);
        check_equal(status.applied_index, 0U);

        check_equal(tr_raft_apply_runtime_destroy(runtime), SALTS_OK);
        tr_raft_core_destroy(core);
    }

    it("retries only the same token after FULL and PENDING")
    {
        tr_raft_entry_t entry = make_entry(1U, "one");
        tr_raft_core_t *core = NULL;
        tr_raft_apply_runtime_t *runtime = NULL;
        tr_raft_apply_runtime_result_t result;
        tr_raft_apply_runtime_config_v1_t config;
        tr_raft_ready_t ready;
        tr_raft_message_t message;
        tr_raft_status_t status;
        io_probe_t io = {0};
        apply_probe_t apply = {0};

        apply.next_admission = TR_RAFT_APPLY_ADMISSION_FULL;
        check_equal(create_restored_core(&entry, 1U, &core), SALTS_OK);
        make_ready(core, &ready, &message);
        config = runtime_config(core, &io, &apply, 1U);
        check_equal(tr_raft_apply_runtime_create(&config, &runtime), SALTS_OK);

        check_equal(tr_raft_apply_runtime_start(runtime, &ready, &result),
                    SALTS_OK);
        check_equal(result.state, TR_RAFT_APPLY_RUNTIME_WAITING_ADMISSION);
        check_equal(apply.apply_calls, 1U);
        check_equal(tr_raft_apply_runtime_poll(runtime, &result), SALTS_OK);
        check_equal(result.state,
                    TR_RAFT_APPLY_RUNTIME_WAITING_SETTLEMENT);
        check_equal(apply.apply_calls, 2U);

        apply.settlement.token = 1U;
        apply.settlement.outcome = TR_RAFT_APPLY_OUTCOME_PENDING;
        apply.settlement.cause = SALTS_OK;
        apply.settlement_ready = true;
        check_equal(tr_raft_apply_runtime_poll(runtime, &result), SALTS_OK);
        check_equal(result.state,
                    TR_RAFT_APPLY_RUNTIME_WAITING_SETTLEMENT);
        check_equal(apply.apply_calls, 3U);
        check_equal(apply.tokens[0], 1U);
        check_equal(apply.tokens[1], 1U);
        check_equal(apply.tokens[2], 1U);

        apply.settlement.outcome = TR_RAFT_APPLY_OUTCOME_APPLIED;
        apply.settlement_ready = true;
        check_equal(tr_raft_apply_runtime_poll(runtime, &result), SALTS_OK);
        check_equal(result.state, TR_RAFT_APPLY_RUNTIME_COMPLETE);
        check_equal(result.applied_through, 1U);
        check_equal(tr_raft_core_status(core, &status), SALTS_OK);
        check_equal(status.applied_index, 1U);
        check_false(status.ready_outstanding);
        check_equal(io.begin_calls, 1U);
        check_equal(io.commit_calls, 1U);
        check_equal(io.enqueue_calls, 1U);

        check_equal(tr_raft_apply_runtime_destroy(runtime), SALTS_OK);
        tr_raft_core_destroy(core);
    }

    it("rejects an oversized Ready before external side effects")
    {
        tr_raft_entry_t entries[2] = {make_entry(1U, "one"),
                                      make_entry(2U, "two")};
        tr_raft_core_t *core = NULL;
        tr_raft_apply_runtime_t *runtime = NULL;
        tr_raft_apply_runtime_result_t result;
        tr_raft_apply_runtime_config_v1_t config;
        tr_raft_ready_t ready;
        tr_raft_message_t message;
        io_probe_t io = {0};
        apply_probe_t apply = {0};

        apply.next_admission = TR_RAFT_APPLY_ADMISSION_ACCEPTED;
        check_equal(create_restored_core(entries, 2U, &core), SALTS_OK);
        make_ready(core, &ready, &message);
        config = runtime_config(core, &io, &apply, 1U);
        check_equal(tr_raft_apply_runtime_create(&config, &runtime), SALTS_OK);

        check_equal(tr_raft_apply_runtime_start(runtime, &ready, &result),
                    SALTS_ENOBUFS);
        check_equal(result.state, TR_RAFT_APPLY_RUNTIME_IDLE);
        check_false(tr_raft_apply_runtime_is_faulted(runtime));
        check_equal(io.begin_calls, 0U);
        check_equal(io.commit_calls, 0U);
        check_equal(io.enqueue_calls, 0U);
        check_equal(apply.apply_calls, 0U);

        check_equal(tr_raft_apply_runtime_destroy(runtime), SALTS_OK);
        tr_raft_core_destroy(core);
    }

    it("faults on a settlement token that does not match the in-flight index")
    {
        tr_raft_entry_t entry = make_entry(1U, "one");
        tr_raft_core_t *core = NULL;
        tr_raft_apply_runtime_t *runtime = NULL;
        tr_raft_apply_runtime_result_t result;
        tr_raft_apply_runtime_config_v1_t config;
        tr_raft_ready_t ready;
        tr_raft_message_t message;
        tr_raft_status_t status;
        io_probe_t io = {0};
        apply_probe_t apply = {0};

        apply.next_admission = TR_RAFT_APPLY_ADMISSION_ACCEPTED;
        check_equal(create_restored_core(&entry, 1U, &core), SALTS_OK);
        make_ready(core, &ready, &message);
        config = runtime_config(core, &io, &apply, 1U);
        check_equal(tr_raft_apply_runtime_create(&config, &runtime), SALTS_OK);
        check_equal(tr_raft_apply_runtime_start(runtime, &ready, &result),
                    SALTS_OK);

        apply.settlement.token = 2U;
        apply.settlement.outcome = TR_RAFT_APPLY_OUTCOME_APPLIED;
        apply.settlement.cause = SALTS_OK;
        apply.settlement_ready = true;
        check_equal(tr_raft_apply_runtime_poll(runtime, &result),
                    SALTS_EPROTO);
        check_equal(result.state, TR_RAFT_APPLY_RUNTIME_FAULTED);
        check_equal(tr_raft_core_status(core, &status), SALTS_OK);
        check_equal(status.applied_index, 0U);
        check_false(status.ready_outstanding);
        check_equal(tr_raft_apply_runtime_reconcile(
                        runtime, &entry, TR_RAFT_APPLY_OUTCOME_APPLIED,
                        &result),
                    SALTS_EPROTO);
        check_equal(result.state, TR_RAFT_APPLY_RUNTIME_FAULTED);
        check_equal(result.in_flight_token, 0U);

        check_equal(tr_raft_apply_runtime_destroy(runtime), SALTS_OK);
        tr_raft_core_destroy(core);
    }

    it("advances a Core-owned configuration entry before application admission")
    {
        tr_raft_conf_t joint;
        tr_raft_conf_t final_membership;
        tr_raft_entry_t joint_entry;
        tr_raft_entry_t final_entry;
        tr_raft_entry_t entries[3];
        tr_raft_core_t *core = NULL;
        tr_raft_apply_runtime_t *runtime = NULL;
        tr_raft_apply_runtime_result_t result;
        tr_raft_apply_runtime_config_v1_t config;
        tr_raft_ready_t ready;
        tr_raft_message_t message;
        tr_raft_status_t status;
        io_probe_t io = {0};
        apply_probe_t apply = {0};

        memset(&joint, 0, sizeof(joint));
        joint.phase = TR_RAFT_CONF_JOINT;
        joint.transition_id = 1U;
        joint.member_count = 2U;
        joint.members[0].node_id = 1U;
        joint.members[0].roles =
            TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER;
        joint.members[1].node_id = 2U;
        joint.members[1].roles = TR_RAFT_CONF_NEW_VOTER;
        memset(&joint_entry, 0, sizeof(joint_entry));
        joint_entry.index = 1U;
        joint_entry.term = 1U;
        check_equal(tr_raft_conf_encode(&joint, joint_entry.data,
                                        sizeof(joint_entry.data),
                                        &joint_entry.data_length),
                    SALTS_OK);
        memset(&final_membership, 0, sizeof(final_membership));
        final_membership.phase = TR_RAFT_CONF_FINAL;
        final_membership.transition_id = 1U;
        final_membership.member_count = 2U;
        final_membership.members[0] = joint.members[0];
        final_membership.members[1].node_id = 2U;
        final_membership.members[1].roles =
            TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER;
        memset(&final_entry, 0, sizeof(final_entry));
        final_entry.index = 2U;
        final_entry.term = 1U;
        check_equal(tr_raft_conf_encode(&final_membership, final_entry.data,
                                        sizeof(final_entry.data),
                                        &final_entry.data_length),
                    SALTS_OK);
        entries[0] = joint_entry;
        entries[1] = final_entry;
        entries[2] = make_entry(3U, "three");

        apply.next_admission = TR_RAFT_APPLY_ADMISSION_ACCEPTED;
        check_equal(create_restored_core(entries, 3U, &core), SALTS_OK);
        make_ready(core, &ready, &message);
        config = runtime_config(core, &io, &apply, 3U);
        check_equal(tr_raft_apply_runtime_create(&config, &runtime), SALTS_OK);
        check_equal(tr_raft_apply_runtime_start(runtime, &ready, &result),
                    SALTS_OK);
        check_equal(result.state,
                    TR_RAFT_APPLY_RUNTIME_WAITING_SETTLEMENT);
        check_equal(result.applied_through, 2U);
        check_equal(apply.apply_calls, 1U);
        check_equal(apply.applied_indexes[0], 3U);
        check_equal(tr_raft_core_status(core, &status), SALTS_OK);
        check_equal(status.applied_index, 2U);

        apply.settlement.token = 3U;
        apply.settlement.outcome = TR_RAFT_APPLY_OUTCOME_APPLIED;
        apply.settlement.cause = SALTS_OK;
        apply.settlement_ready = true;
        check_equal(tr_raft_apply_runtime_poll(runtime, &result), SALTS_OK);
        check_equal(result.state, TR_RAFT_APPLY_RUNTIME_COMPLETE);
        check_equal(result.applied_through, 3U);
        check_equal(tr_raft_core_status(core, &status), SALTS_OK);
        check_equal(status.applied_index, 3U);

        check_equal(tr_raft_apply_runtime_destroy(runtime), SALTS_OK);
        tr_raft_core_destroy(core);
    }

    it("refuses destruction while an accepted entry is unsettled")
    {
        tr_raft_entry_t entry = make_entry(1U, "one");
        tr_raft_core_t *core = NULL;
        tr_raft_apply_runtime_t *runtime = NULL;
        tr_raft_apply_runtime_result_t result;
        tr_raft_apply_runtime_config_v1_t config;
        tr_raft_ready_t ready;
        tr_raft_message_t message;
        io_probe_t io = {0};
        apply_probe_t apply = {0};

        apply.next_admission = TR_RAFT_APPLY_ADMISSION_ACCEPTED;
        check_equal(create_restored_core(&entry, 1U, &core), SALTS_OK);
        make_ready(core, &ready, &message);
        config = runtime_config(core, &io, &apply, 1U);
        check_equal(tr_raft_apply_runtime_create(&config, &runtime), SALTS_OK);
        check_equal(tr_raft_apply_runtime_start(runtime, &ready, &result),
                    SALTS_OK);
        check_equal(result.state,
                    TR_RAFT_APPLY_RUNTIME_WAITING_SETTLEMENT);

        check_equal(tr_raft_apply_runtime_start(runtime, &ready, &result),
                    SALTS_EBUSY);
        check_equal(result.state,
                    TR_RAFT_APPLY_RUNTIME_WAITING_SETTLEMENT);
        check_equal(tr_raft_apply_runtime_destroy(runtime), SALTS_EBUSY);

        apply.settlement.token = 1U;
        apply.settlement.outcome = TR_RAFT_APPLY_OUTCOME_APPLIED;
        apply.settlement.cause = SALTS_OK;
        apply.settlement_ready = true;
        check_equal(tr_raft_apply_runtime_poll(runtime, &result), SALTS_OK);
        check_equal(result.state, TR_RAFT_APPLY_RUNTIME_COMPLETE);
        check_equal(tr_raft_apply_runtime_destroy(runtime), SALTS_OK);
        tr_raft_core_destroy(core);
    }
}
