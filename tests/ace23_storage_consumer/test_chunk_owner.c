#if !defined(_WIN32)
#define _POSIX_C_SOURCE 200809L
#endif

#include <turboraft/raft_multicore_ingress.h>
#include <turboraft/raft_multicore_chunk_receivers.h>
#include <salts/clock.h>
#include <salts/thread.h>
#include <cmeta_error.h>
#include <tinytest.h>

#include <stdio.h>
#include <string.h>

#if defined(_WIN32)
#include <io.h>
#else
#include <unistd.h>
#endif

static const uint8_t DIGEST[32] = {
    0xbeU, 0xf5U, 0x7eU, 0xc7U, 0xf5U, 0x3aU, 0x6dU, 0x40U,
    0xbeU, 0xb6U, 0x40U, 0xa7U, 0x80U, 0xa6U, 0x39U, 0xc8U,
    0x3bU, 0xc2U, 0x9aU, 0xc8U, 0xa9U, 0x81U, 0x6fU, 0x1fU,
    0xc6U, 0xc5U, 0xc6U, 0xdcU, 0xd9U, 0x3cU, 0x47U, 0x21U
};

typedef struct owner_sink {
    const void *thread;
    FILE *file;
    size_t bytes;
    unsigned fsync_count, abort_count;
    bool wrong_owner, closed;
    tr_raft_data_stream_receiver_t *data;
    tr_raft_snapshot_receiver_t *snapshot;
} owner_sink;

typedef struct owner_fixture {
    owner_sink sinks[2];
    tr_raft_node_id_t voters[3];
    tr_raft_group_assignment_t groups[2];
    tr_raft_multicore_config_t config;
    tr_raft_multicore_t *runtime;
    tr_raft_multicore_ingress_t *ingress;
} owner_fixture;

static owner_sink *sink_for(owner_fixture *f, uint64_t group)
{
    return &f->sinks[group == 101U ? 0U : 1U];
}

static int check_owner(void *ctx)
{
    owner_sink *sink = (owner_sink *)ctx;
    if (sink->thread != cmeta_thread_current_token())
        sink->wrong_owner = true;
    return SALTS_OK;
}

static int persist_hard(void *ctx, tr_raft_term_t term, tr_raft_node_id_t vote)
{
    (void)term; (void)vote;
    return check_owner(ctx);
}
static int persist_index(void *ctx, tr_raft_index_t index)
{
    (void)index;
    return check_owner(ctx);
}
static int persist_append(void *ctx, const tr_raft_entry_t *entries, size_t n)
{
    (void)entries; (void)n;
    return check_owner(ctx);
}
static int persist_send(void *ctx, const tr_raft_message_t *message)
{
    (void)message;
    return check_owner(ctx);
}
static int persist_apply(void *ctx, const tr_raft_entry_t *entries, size_t n)
{
    (void)entries; (void)n;
    return check_owner(ctx);
}

static int begin_sink(owner_sink *sink, uint64_t bytes)
{
    if (check_owner(sink) != SALTS_OK ||
        sink->file == NULL || bytes != 6U)
        return SALTS_EPROTO;
    sink->bytes = 0U;
    return SALTS_OK;
}

static int data_begin(void *ctx, tr_raft_node_id_t from, tr_raft_term_t term,
                      uint64_t stream, uint64_t size,
                      const uint8_t digest[32])
{
    if (from != 1U || term != 5U || stream != 9U ||
        memcmp(digest, DIGEST, sizeof(DIGEST)) != 0)
        return SALTS_EPROTO;
    return begin_sink((owner_sink *)ctx, size);
}

static int snap_begin(void *ctx, tr_raft_term_t term, tr_raft_index_t index,
                      tr_raft_term_t snap_term, const tr_raft_conf_t *conf,
                      uint64_t bytes)
{
    if (term != 5U || index != 9U || snap_term != 4U ||
        conf == NULL || conf->member_count != 1U)
        return SALTS_EPROTO;
    return begin_sink((owner_sink *)ctx, bytes);
}

static int sink_write(void *ctx, uint64_t offset, const uint8_t *data, size_t n)
{
    owner_sink *sink = (owner_sink *)ctx;
    if (sink->thread != cmeta_thread_current_token() ||
        sink->file == NULL || data == NULL ||
        offset != sink->bytes || n > 6U - sink->bytes)
        return SALTS_EPROTO;
    if (fwrite(data, 1U, n, sink->file) != n)
        return SALTS_EIO;
    sink->bytes += n;
    return SALTS_OK;
}

static int sink_commit(void *ctx)
{
    owner_sink *sink = (owner_sink *)ctx;
    if (sink->thread != cmeta_thread_current_token() ||
        sink->file == NULL || sink->bytes != 6U)
        return SALTS_EPROTO;
    if (fflush(sink->file) != 0) return SALTS_EIO;
#if defined(_WIN32)
    if (_commit(_fileno(sink->file)) != 0) return SALTS_EIO;
#else
    if (fsync(fileno(sink->file)) != 0) return SALTS_EIO;
#endif
    ++sink->fsync_count;
    return SALTS_OK;
}

static void sink_abort(void *ctx)
{
    owner_sink *sink = (owner_sink *)ctx;
    if (sink->thread != cmeta_thread_current_token())
        sink->wrong_owner = true;
    ++sink->abort_count;
}

static int group_open(void *ctx, tr_raft_owner_t *owner, uint64_t group,
                      tr_raft_service_t **out_service)
{
    owner_fixture *f = (owner_fixture *)ctx;
    owner_sink *sink = sink_for(f, group);
    tr_raft_service_config_t service = {0};
    int rc;

    *out_service = NULL;
    sink->thread = cmeta_thread_current_token();
    if (!tr_raft_owner_contains(owner, group))
        sink->wrong_owner = true;
    sink->file = tmpfile();
    if (sink->file == NULL) return SALTS_EIO;

    if (group == 101U) {
        tr_raft_data_stream_receiver_config_t data = {0};
        data.self_id = 2U;
        data.max_stream_bytes = 32U;
        data.sink = (tr_raft_data_stream_sink_t){
            data_begin, sink_write, sink_commit, sink_abort, sink};
        rc = tr_raft_data_stream_receiver_create(&data, &sink->data);
    } else {
        tr_raft_snapshot_receiver_config_t snapshot = {0};
        snapshot.self_id = 2U;
        snapshot.max_snapshot_bytes = 32U;
        snapshot.stream = (tr_raft_snapshot_stream_sink_t){
            snap_begin, sink_write, sink_commit, sink_abort, sink};
        rc = tr_raft_snapshot_receiver_create(&snapshot, &sink->snapshot);
    }
    if (rc != SALTS_OK) goto failed;

    service.core.self_id = 2U;
    service.core.voters = f->voters;
    service.core.voter_count = 3U;
    service.core.heartbeat_ticks = 2U;
    service.core.election_min_ticks = 5U;
    service.core.election_max_ticks = 9U;
    service.core.initial_election_timeout_ticks = 5U;
    service.core.max_log_entries = 32U;
    service.storage.context = sink;
    service.storage.begin = check_owner;
    service.storage.write_hard_state = persist_hard;
    service.storage.truncate_log = persist_index;
    service.storage.append_log = persist_append;
    service.storage.write_commit_index = persist_index;
    service.storage.commit = check_owner;
    service.storage.rollback = check_owner;
    service.transport.context = sink;
    service.transport.enqueue = persist_send;
    service.state_machine.context = sink;
    service.state_machine.apply_batch = persist_apply;
    rc = tr_raft_service_create(&service, out_service);
    if (rc == SALTS_OK) return SALTS_OK;
failed:
    tr_raft_snapshot_receiver_destroy(sink->snapshot);
    sink->snapshot = NULL;
    tr_raft_data_stream_receiver_destroy(sink->data);
    sink->data = NULL;
    fclose(sink->file);
    sink->file = NULL;
    return rc;
}

static void group_close(void *ctx, tr_raft_owner_t *owner, uint64_t group)
{
    owner_sink *sink = sink_for((owner_fixture *)ctx, group);
    if (sink->thread != cmeta_thread_current_token() ||
        tr_raft_owner_service(owner, group) != NULL)
        sink->wrong_owner = true;
    tr_raft_snapshot_receiver_destroy(sink->snapshot);
    tr_raft_data_stream_receiver_destroy(sink->data);
    sink->snapshot = NULL;
    sink->data = NULL;
    if (sink->file != NULL) fclose(sink->file);
    sink->file = NULL;
    sink->closed = true;
}

static int group_receive(void *ctx, tr_raft_owner_t *owner, uint64_t group,
                         const tr_raft_transport_payload_t *chunk,
                         tr_raft_multicore_chunk_result_t *out)
{
    owner_sink *sink = sink_for((owner_fixture *)ctx, group);
    tr_raft_multicore_chunk_receivers_t receivers = {sink->snapshot, sink->data};
    if (!tr_raft_owner_contains(owner, group) ||
        sink->thread != cmeta_thread_current_token())
        sink->wrong_owner = true;
    return tr_raft_multicore_chunk_receivers_handle(&receivers, chunk, out);
}

static int fixture_create(owner_fixture *f)
{
    tr_raft_multicore_factory_t factory = {0};
    int rc;
    f->voters[0] = 1U;
    f->voters[1] = 2U;
    f->voters[2] = 3U;
    for (size_t i = 0U; i < 2U; ++i) {
        f->groups[i].group_id = i == 0U ? 101U : 103U;
        f->groups[i].owner_index = (uint32_t)i;
        f->groups[i].election_min_ticks = 5U;
        f->groups[i].election_max_ticks = 9U;
    }
    f->config.version = TR_RAFT_MULTICORE_VERSION;
    f->config.owner_count = 2U;
    f->config.capacity = 2U;
    f->config.work_budget = 1U;
    f->config.tick_ms = 1000U;
    f->config.idle_ms = 1U;
    f->config.groups = f->groups;
    f->config.group_count = 2U;
    f->config.owned_chunk_bytes_per_group = 512U;
    factory.context = f;
    factory.group_open = group_open;
    factory.group_close = group_close;
    factory.receive_chunk = group_receive;
    rc = tr_raft_multicore_create(&f->config, &factory, &f->runtime);
    if (rc != SALTS_OK) return rc;
    return tr_raft_multicore_ingress_create(f->runtime, 1900U, &f->ingress);
}

static int take(owner_fixture *f, uint64_t group,
                tr_raft_multicore_completion_t *out)
{
    const uint64_t deadline = cmeta_monotonic_ms() + UINT64_C(5000);
    int rc;
    do {
        rc = tr_raft_multicore_take(f->runtime, group, out);
        if (rc != SALTS_ENOENT) return rc;
        cmeta_sleep_ms(1U);
    } while (cmeta_monotonic_ms() < deadline);
    return SALTS_ETIMEDOUT;
}

static int fixture_destroy(owner_fixture *f)
{
    int rc = SALTS_OK;
    tr_raft_multicore_request_stop(f->runtime);
    rc = tr_raft_multicore_stop(f->runtime);
    if (tr_raft_multicore_ingress_destroy(f->ingress) != SALTS_OK)
        rc = SALTS_EPROTO;
    tr_raft_multicore_destroy(f->runtime);
    f->ingress = NULL;
    f->runtime = NULL;
    if (!f->sinks[0].closed || !f->sinks[1].closed ||
        f->sinks[0].wrong_owner || f->sinks[1].wrong_owner)
        rc = SALTS_EPROTO;
    return rc;
}

static tr_raft_transport_payload_t chunk(
    uint64_t group, const uint8_t *data, size_t offset, bool last)
{
    tr_raft_transport_payload_t p = {0};
    p.group_id = group;
    if (group == 101U) {
        tr_raft_data_chunk_t *d = &p.data.data_chunk;
        p.kind = TR_RAFT_WIRE_PAYLOAD_DATA_CHUNK;
        d->from = 1U; d->to = 2U; d->term = 5U; d->stream_id = 9U;
        d->stream_offset = offset; d->stream_size = 6U;
        d->data_length = 3U; d->data = data; d->done = last;
        memcpy(d->stream_digest, DIGEST, sizeof(DIGEST));
    } else {
        tr_raft_snapshot_chunk_t *d = &p.data.snapshot_chunk;
        p.kind = TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_CHUNK;
        d->from = 3U; d->to = 2U; d->term = 5U;
        d->snapshot_index = 9U; d->snapshot_term = 4U;
        d->snapshot_offset = offset; d->snapshot_size = 6U;
        d->data_length = 3U; d->data = data; d->done = last;
        d->has_configuration = offset == 0U;
        if (d->has_configuration) {
            d->configuration.phase = TR_RAFT_CONF_FINAL;
            d->configuration.member_count = 1U;
            d->configuration.members[0].node_id = 2U;
            d->configuration.members[0].roles =
                TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER;
        }
        memcpy(d->snapshot_digest, DIGEST, sizeof(DIGEST));
    }
    return p;
}

spec("ACE 2.3 actual receiver on fixed Multicore Group Owner")
{
    it("correlates DATA/SNAPSHOT per Group and emits durable ACK only after fsync")
    {
        owner_fixture f = {0};
        uint8_t data[2][3] = {{'a','b','c'}, {'d','e','f'}};
        uint8_t snapshot[2][3] = {{'a','b','c'}, {'d','e','f'}};
        uint64_t ids[2][2] = {{0}};
        tr_raft_transport_reply_origin_t origins[2] = {{0}};
        tr_raft_multicore_completion_t done = {0};
        tr_raft_multicore_group_status_t status = {0};

        check_equal(fixture_create(&f), SALTS_OK);
        for (size_t g = 0U; g < 2U; ++g) {
            origins[g] = (tr_raft_transport_reply_origin_t){
                .channel_instance = 1000U + (uint64_t)g,
                .authenticated_peer_node_id = g == 0U ? 1U : 3U,
                .group_id = g == 0U ? 101U : 103U,
                .connection_token = UINT64_C(5001) + (uint64_t)g
            };
        }
        {
            const tr_raft_transport_payload_t invalid =
                chunk(101U, (const uint8_t *)"abc", 0U, false);
            tr_raft_transport_reply_origin_t wrong = origins[0];
            uint64_t rejected_id = 77U;
            wrong.group_id = 103U;
            check_equal(tr_raft_multicore_ingress_submit_with_origin(
                f.ingress, &invalid, &wrong, &rejected_id), SALTS_EPROTO);
            check_equal(rejected_id, UINT64_C(0));
            wrong = origins[0];
            wrong.authenticated_peer_node_id = 3U;
            check_equal(tr_raft_multicore_ingress_submit_with_origin(
                f.ingress, &invalid, &wrong, &rejected_id), SALTS_EPROTO);
            check_equal(rejected_id, UINT64_C(0));
        }
        for (size_t g = 0U; g < 2U; ++g) {
            const uint64_t group = g == 0U ? 101U : 103U;
            uint8_t (*bytes)[3] = g == 0U ? data : snapshot;
            for (size_t i = 0U; i < 2U; ++i) {
                tr_raft_transport_payload_t msg =
                    chunk(group, bytes[i], i * 3U, i == 1U);
                check_equal(tr_raft_multicore_ingress_submit_with_origin(
                    f.ingress, &msg, &origins[g], &ids[g][i]), SALTS_OK);
                check_true(ids[g][i] != 0U);
                memset(bytes[i], 0xff, sizeof(bytes[i]));
            }
        }
        for (size_t g = 0U; g < 2U; ++g) {
            const uint64_t group = g == 0U ? 101U : 103U;
            for (size_t i = 0U; i < 2U; ++i) {
                check_equal(take(&f, group, &done), SALTS_OK);
                check_equal(done.request_id, ids[g][i]);
                check_equal(done.operation, TR_RAFT_MULTICORE_RECEIVE_CHUNK);
                check_equal(done.result, SALTS_OK);
                check_equal(done.reply_origin.channel_instance,
                            origins[g].channel_instance);
                check_equal(done.reply_origin.authenticated_peer_node_id,
                            origins[g].authenticated_peer_node_id);
                check_equal(done.reply_origin.group_id,
                            origins[g].group_id);
                check_equal(done.reply_origin.connection_token,
                            origins[g].connection_token);
                check_true(done.value.chunk.ack_valid);
                check_equal(done.value.chunk.durable_or_installed, i == 1U);
                if (g == 0U) {
                    check_equal(done.value.chunk.kind,
                                TR_RAFT_WIRE_PAYLOAD_DATA_CHUNK);
                    check_equal(done.value.chunk.ack.data.next_offset,
                                (uint64_t)(3U * (i + 1U)));
                    check_equal(done.value.chunk.ack.data.durable, i == 1U);
                } else {
                    check_equal(done.value.chunk.kind,
                                TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_CHUNK);
                    check_equal(done.value.chunk.ack.snapshot.next_offset,
                                (uint64_t)(3U * (i + 1U)));
                }
            }
            check_equal(f.sinks[g].fsync_count, 1U);
            check_equal(f.sinks[g].abort_count, 0U);
            check_equal(f.sinks[g].bytes, (size_t)6U);
            check_equal(tr_raft_multicore_group_status(
                f.runtime, group, &status), SALTS_OK);
            check_equal(status.owned_chunk_bytes, (size_t)0U);
            check_equal(status.outstanding, (size_t)0U);
        }
        check_equal(fixture_destroy(&f), SALTS_OK);
    }
}
