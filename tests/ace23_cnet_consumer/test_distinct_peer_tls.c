#include <turboraft/raft_cnet_channel.h>
#include <turboraft/raft_cnet_peer_directory.h>
#include <turboraft/raft_multicore_ingress.h>
#include <salts/clock.h>
#include <salts/thread.h>

#include <cmeta_error.h>
#include <tinytest.h>

#include <stdio.h>
#include <string.h>

#ifdef TURBORAFT_TEST_DURABLE_NET_SNAPSHOT
#include <turboraft/raft_wal_storage.h>
#include <turboraft/raft_snapshot_receiver.h>
#include <turboraft/raft_multicore_chunk_receivers.h>
#include <cmeta_crypto.h>
#include <cmeta_fs.h>
#include <stdlib.h>
#ifdef TURBORAFT_TEST_DURABLE_NET_SNAPSHOT_FAILURE
#include "raft_wal_storage_internal.h" /* only test, never installed ABI */
#endif
#endif

#ifndef TURBORAFT_ACE23_FIXTURE_DIR
#error "TURBORAFT_ACE23_FIXTURE_DIR must be defined for TLS fixture tests"
#endif

/* Both trust chains are checked-in loopback-only; node3 uses the separate
 * rotated *client* certificate, NOT a second copy of Node1 credentials. */
#define CERT_NODE1 "eb571a92b33237897216c79501066b3e77391047eaeb6be084526c4769657549"
#define CERT_NODE3 "d696b3ab8d0596e3e8e8abcecc4ca87856eda0f5055f31a7dfb6c213c2c2b1f3"
#define CERT_SERVER_NODE2 "44e8fe3ce37ede1c2a1b5d36efa345cb662887d4250d17a000ebea2e834aed95"

enum { LINK_COUNT = 2, MAX_PROGRESS = 4500 };

typedef struct remote_probe {
    tr_raft_node_id_t node_id;
    size_t received;
    int wrong_payload;
#ifdef TURBORAFT_TEST_DURABLE_NET_SNAPSHOT
    size_t snapshot_acks;
#endif
} remote_probe;

typedef struct server_probe {
    size_t received_from_one;
    size_t received_from_three;
    size_t received_data;
    size_t received_snapshot;
    size_t wrong_payloads;
} server_probe;

typedef struct owner_raft_probe {
    const void *thread;
    int wrong_owner;
    int closed;
    size_t egress_replies;
    size_t chunk_calls;
    size_t chunk_bytes;
    int chunk_error;
#ifdef TURBORAFT_TEST_DURABLE_NET_SNAPSHOT
    tr_raft_wal_storage_t *snapshot_wal; /* owned by Group 103 thread */
    tr_raft_snapshot_receiver_t *snapshot_receiver;
    size_t installed_snapshots;
#ifdef TURBORAFT_TEST_DURABLE_NET_SNAPSHOT_FAILURE
    size_t snapshot_write_faults;
#endif
#endif
} owner_raft_probe;

/* CNet's verified inbound callback borrows one exact Channel generation.
 * Preserve that pointer until the enclosing CNet Owner has quiesced. */
struct identity_fixture;
typedef struct verified_payload_context {
    struct identity_fixture *fixture;
    tr_raft_cnet_channel_t *channel;
    tr_raft_transport_reply_origin_t chunk_origin; /* captured on CNet owner */
    uint64_t chunk_request_id; /* most recent admitted chunk */
#ifdef TURBORAFT_TEST_DURABLE_NET_SNAPSHOT_SEGMENTED
    tr_raft_transport_reply_origin_t prior_chunk_origin;
    uint64_t prior_chunk_request_id;
#endif
} verified_payload_context;

typedef struct identity_fixture {
    tr_raft_multicore_t *raft_owners;
    tr_raft_multicore_ingress_t *owner_ingress;
    tr_raft_group_assignment_t owner_assignments[LINK_COUNT];
    tr_raft_multicore_config_t owner_config;
    owner_raft_probe owner_groups[LINK_COUNT];
    tr_raft_node_id_t owner_voters[3];
    cnet_client clients; /* one CNet I/O Owner for the two different logical test nodes */
    cnet_client server;
    cnet_listener listener;
    cnet_tls_server server_tls;
    cnet_manager directory_manager;
    tr_raft_cnet_managed_peer_t *authorized_peers[LINK_COUNT];
    tr_raft_cnet_peer_directory_entry_t authorized_routes[LINK_COUNT];
    tr_raft_cnet_peer_directory_t directory;
    tr_raft_group_id_t admitted_groups[LINK_COUNT];
    tr_raft_cnet_directory_ingress_t ingress[LINK_COUNT];
    verified_payload_context verified_payloads[LINK_COUNT];
    cnet_tls_client_config directory_profiles[LINK_COUNT];
    tr_raft_cnet_channel_t *outbound[LINK_COUNT];
    tr_raft_cnet_channel_t *inbound[LINK_COUNT];
    cnet_connection outbound_handles[LINK_COUNT];
    cnet_connection inbound_handles[LINK_COUNT];
    tr_raft_cnet_peer_identity_t server_peers[LINK_COUNT];
    tr_raft_cnet_identity_policy_t server_policy;
    tr_raft_cnet_peer_identity_t client_peer[LINK_COUNT];
    tr_raft_cnet_identity_policy_t client_policy[LINK_COUNT];
    const char *client_peer_cert[LINK_COUNT][1];
    const char *server_peer_cert[LINK_COUNT][1];
    cnet_tls_client_config client_tls[LINK_COUNT];
    tr_raft_cnet_channel_config_t server_channel_config;
    remote_probe clients_received[LINK_COUNT];
    server_probe server_received;
    /* CNet delivers callbacks inline from its one progress Owner. These
     * counters are never accessed by another thread during progress. */
    size_t server_payload_inflight;
    size_t server_payload_completed;
    unsigned stop_from_payload_checks;
    int stop_from_payload_result;
    int destroy_from_payload_result;
    int clients_live, server_live, listener_live, server_tls_live;
    int directory_manager_live;
    int accepted;
#ifdef TURBORAFT_TEST_DURABLE_NET_SNAPSHOT
    char *snapshot_prefix; /* host owns path, Group 103 owns open file */
#endif
} identity_fixture;

static const tr_raft_node_id_t NODE_IDS[LINK_COUNT] = {1U, 3U};

static int fixture_path(char *out, size_t cap, const char *filename)
{
    int n = snprintf(out, cap, "%s/%s",
                     TURBORAFT_ACE23_FIXTURE_DIR, filename);
    return n > 0 && (size_t)n < cap ? SALTS_OK : SALTS_ERANGE;
}

static cnet_client_config cnet_config(void)
{
    return (cnet_client_config){
#if defined(_WIN32)
        .backend = NATIVE_IO_BACKEND_IOCP,
#elif defined(__linux__)
        .backend = NATIVE_IO_BACKEND_EPOLL,
#else
        .backend = NATIVE_IO_BACKEND_KQUEUE,
#endif
        .connection_capacity = LINK_COUNT,
        .command_capacity = 16U,
        .request_capacity = 16U,
        .completion_batch_capacity = 8U,
        .event_capacity = 16U,
        .max_send_bytes = 1024U,
        .receive_buffer_bytes = 1024U,
        .connect_timeout_ms = 2000U,
        .read_timeout_ms = 2000U,
        .write_timeout_ms = 2000U,
        .tls_io_buffer_bytes = CNET_TLS_MIN_IO_BUFFER_BYTES,
        .tls_handshake_timeout_ms = 2000U
    };
}

static tr_raft_handshake_config_t hello_config(tr_raft_node_id_t node)
{
    tr_raft_handshake_config_t config = {0};
    size_t i;
    for (i = 0U; i < sizeof(config.cluster_id.bytes); ++i)
        config.cluster_id.bytes[i] = (uint8_t)(i + 17U);
    config.local_node_id = node;
    config.process_incarnation.bytes[0] = (uint8_t)node;
    config.config_epoch = 1U;
    config.wire_major_min = TR_RAFT_HANDSHAKE_WIRE_MAJOR;
    config.wire_major_max = TR_RAFT_HANDSHAKE_WIRE_MAJOR;
    config.wire_minor_min = TR_RAFT_HANDSHAKE_WIRE_MINOR;
    config.wire_minor_max = TR_RAFT_HANDSHAKE_WIRE_MINOR;
    config.max_frame_size = TR_RAFT_WIRE_MAX_FRAME_SIZE;
    config.max_snapshot_chunk_size = TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES;
    return config;
}

/* Caller only sees decoded payload AFTER certified TLS and reciprocal ACK.
 * Any Node1 message mislabeled as Node3 is not accepted by this sink. */
static int on_server_payload(void *user, const tr_raft_transport_payload_t *msg)
{
    server_probe *sink = (server_probe *)user;
    if (msg == NULL) goto invalid;
    if (msg->kind == TR_RAFT_WIRE_PAYLOAD_RAFT) {
        if (msg->data.raft.to != 2U ||
            msg->data.raft.type != TR_RAFT_MSG_HEARTBEAT_REQUEST ||
            msg->data.raft.term != 3U) goto invalid;
        if (msg->data.raft.from == 1U && msg->group_id == 101U)
            ++sink->received_from_one;
        else if (msg->data.raft.from == 3U && msg->group_id == 103U)
            ++sink->received_from_three;
        else goto invalid;
    } else if (msg->kind == TR_RAFT_WIRE_PAYLOAD_DATA_CHUNK) {
        const tr_raft_data_chunk_t *d = &msg->data.data_chunk;
        if (msg->group_id != 101U || d->from != 1U || d->to != 2U ||
            d->stream_id != 9U || d->data_length != 16U || d->data == NULL)
            goto invalid;
        for (size_t i = 0U; i < d->data_length; ++i)
            if (d->data[i] != 0x39U) goto invalid;
        ++sink->received_data;
    } else if (msg->kind == TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_CHUNK) {
        const tr_raft_snapshot_chunk_t *d = &msg->data.snapshot_chunk;
        if (msg->group_id != 103U || d->from != 3U || d->to != 2U ||
            d->snapshot_index != 19U ||
#ifdef TURBORAFT_TEST_DURABLE_NET_SNAPSHOT_SEGMENTED
            d->data_length != 12U ||
            d->snapshot_offset != sink->received_snapshot * 12U ||
#else
            d->data_length != 24U ||
#endif
            d->data == NULL) goto invalid;
        for (size_t i = 0U; i < d->data_length; ++i)
            if (d->data[i] != 0x7bU) goto invalid;
        ++sink->received_snapshot;
    } else goto invalid;

    return SALTS_OK;
invalid:
    ++sink->wrong_payloads;
    return SALTS_EPROTO;
}

/* Two independent Raft group owners, separate from the TLS/CNet progress
 * Owner. This is the exact production Multicore inbox, not a shadow queue. */
static size_t raft_group_slot(uint64_t group_id)
{
    return group_id == 101U ? 0U : 1U;
}

static int owner_storage(void *context)
{
    owner_raft_probe *probe = (owner_raft_probe *)context;
    if (probe->thread != cmeta_thread_current_token())
        probe->wrong_owner = 1;
    return SALTS_OK;
}
static int owner_storage_hard(void *context, tr_raft_term_t term,
                              tr_raft_node_id_t vote)
{
    (void)term; (void)vote;
    return owner_storage(context);
}
static int owner_storage_index(void *context, tr_raft_index_t index)
{
    (void)index;
    return owner_storage(context);
}
static int owner_storage_append(void *context, const tr_raft_entry_t *entries,
                                size_t count)
{
    (void)entries; (void)count;
    return owner_storage(context);
}
static int owner_transport_send(void *context, const tr_raft_message_t *message)
{
    owner_raft_probe *probe = (owner_raft_probe *)context;
    if (message == NULL) return SALTS_EINVAL;
    ++probe->egress_replies; /* network responses have a distinct Host path */
    return owner_storage(context);
}
static int owner_apply(void *context, const tr_raft_entry_t *entries,
                       size_t count)
{
    (void)entries; (void)count;
    return owner_storage(context);
}

#ifdef TURBORAFT_TEST_DURABLE_NET_SNAPSHOT
/* Runs ONLY on Group 103's actual Multicore Owner thread. A successful
 * install performs production WAL file/segment/manifest durability BEFORE
 * SnapshotReceiver can mark the terminal response installed. */
static int durable_snapshot_install(
    void *context, tr_raft_term_t leader_term,
    tr_raft_index_t snapshot_index, tr_raft_term_t snapshot_term,
    const tr_raft_conf_t *configuration, const uint8_t *bytes, size_t size)
{
    owner_raft_probe *probe = (owner_raft_probe *)context;
    int result;
    if (probe == NULL || probe->snapshot_wal == NULL ||
        probe->thread != cmeta_thread_current_token()) {
        if (probe != NULL) probe->wrong_owner = 1;
        return SALTS_EPROTO;
    }
    result = tr_raft_wal_storage_install_snapshot(
        probe->snapshot_wal, leader_term, snapshot_index,
        snapshot_term, configuration, bytes, size);
    if (result == SALTS_OK) ++probe->installed_snapshots;
    return result;
}
#ifdef TURBORAFT_TEST_DURABLE_NET_SNAPSHOT_FAILURE
/* Inject one real WalStorage snapshot staging write failure. The production
 * provider is otherwise unchanged; no public API or environment switches. */
static int durable_snapshot_fail_write(
    void *context, tr_raft_wal_io_phase_t phase,
    uint64_t ordinal, size_t *inout_size)
{
    owner_raft_probe *probe = (owner_raft_probe *)context;
    (void)inout_size;
    if (probe != NULL && phase == TR_RAFT_WAL_IO_SNAPSHOT_WRITE &&
        ordinal == 1U) {
        ++probe->snapshot_write_faults;
        return SALTS_EIO;
    }
    return SALTS_OK;
}
#endif
#endif

static int owner_group_open(void *context, tr_raft_owner_t *owner,
                            uint64_t group_id, tr_raft_service_t **out_service)
{
    identity_fixture *fixture = (identity_fixture *)context;
    const size_t index = raft_group_slot(group_id);
    owner_raft_probe *probe = &fixture->owner_groups[index];
    tr_raft_service_config_t service = {0};

    *out_service = NULL;
    probe->thread = cmeta_thread_current_token();
    if (!tr_raft_owner_contains(owner, group_id) ||
        tr_raft_owner_index(owner) != index)
        probe->wrong_owner = 1;

#ifdef TURBORAFT_TEST_DURABLE_NET_SNAPSHOT
    if (group_id == 103U) {
        tr_raft_wal_storage_config_t wal = {0};
        tr_raft_snapshot_receiver_config_t receiver = {0};
        int rc;
        wal.path_prefix = fixture->snapshot_prefix;
        wal.segment_bytes = TR_RAFT_WAL_MIN_SEGMENT_BYTES;
        wal.max_transaction_bytes = 8U * 1024U;
        wal.max_live_segments = 4U;
        wal.max_log_entries = 16U;
        wal.max_snapshot_bytes = 1024U;
        wal.create_if_missing = true;
        rc = tr_raft_wal_storage_open(&wal, &probe->snapshot_wal);
        if (rc != SALTS_OK) return rc;
#ifdef TURBORAFT_TEST_DURABLE_NET_SNAPSHOT_FAILURE
        {
            tr_raft_wal_io_fault_provider_t faults = {0};
            faults.before_io = durable_snapshot_fail_write;
            faults.context = probe;
            rc = tr_raft_wal_storage_set_io_fault_provider_for_test(
                probe->snapshot_wal, &faults);
            if (rc != SALTS_OK) {
                (void)tr_raft_wal_storage_close(probe->snapshot_wal);
                probe->snapshot_wal = NULL;
                return rc;
            }
        }
#endif
        receiver.self_id = 2U;
        receiver.max_snapshot_bytes = 1024U;
        receiver.max_buffered_snapshot_bytes = 1024U;
        receiver.install = durable_snapshot_install;
        receiver.install_context = probe;
        rc = tr_raft_snapshot_receiver_create(
            &receiver, &probe->snapshot_receiver);
        if (rc != SALTS_OK) {
            (void)tr_raft_wal_storage_close(probe->snapshot_wal);
            probe->snapshot_wal = NULL;
            return rc;
        }
    }
#endif

    service.core.self_id = 2U;
    service.core.voters = fixture->owner_voters;
    service.core.voter_count = 3U;
    service.core.heartbeat_ticks = 2U;
    service.core.election_min_ticks = 5U;
    service.core.election_max_ticks = 9U;
    service.core.initial_election_timeout_ticks = 5U;
    service.core.max_log_entries = 32U;
    service.storage.context = probe;
    service.storage.begin = owner_storage;
    service.storage.write_hard_state = owner_storage_hard;
    service.storage.truncate_log = owner_storage_index;
    service.storage.append_log = owner_storage_append;
    service.storage.write_commit_index = owner_storage_index;
    service.storage.commit = owner_storage;
    service.storage.rollback = owner_storage;
    service.transport.context = probe;
    service.transport.enqueue = owner_transport_send;
    service.state_machine.context = probe;
    service.state_machine.apply_batch = owner_apply;
#ifdef TURBORAFT_TEST_DURABLE_NET_SNAPSHOT
    {
        int rc = tr_raft_service_create(&service, out_service);
        if (rc != SALTS_OK && probe->snapshot_wal != NULL) {
            tr_raft_snapshot_receiver_destroy(probe->snapshot_receiver);
            probe->snapshot_receiver = NULL;
            (void)tr_raft_wal_storage_close(probe->snapshot_wal);
            probe->snapshot_wal = NULL;
        }
        return rc;
    }
#else
    return tr_raft_service_create(&service, out_service);
#endif
}

static void owner_group_close(void *context, tr_raft_owner_t *owner,
                              uint64_t group_id)
{
    identity_fixture *fixture = (identity_fixture *)context;
    owner_raft_probe *probe = &fixture->owner_groups[
        raft_group_slot(group_id)];
    if (probe->thread != cmeta_thread_current_token() ||
        tr_raft_owner_service(owner, group_id) != NULL)
        probe->wrong_owner = 1;
#ifdef TURBORAFT_TEST_DURABLE_NET_SNAPSHOT
    tr_raft_snapshot_receiver_destroy(probe->snapshot_receiver);
    probe->snapshot_receiver = NULL;
    if (probe->snapshot_wal != NULL) {
        if (tr_raft_wal_storage_close(probe->snapshot_wal) != SALTS_OK)
            probe->wrong_owner = 1;
        probe->snapshot_wal = NULL;
    }
#endif
    probe->closed = 1;
}

/* A real authenticated CNet callback owns its view only until return.
 * These test sinks verify the Core-owned copy on the exact Raft Group
 * thread and deliberately avoid fabricating any persisted ACK. */
static int owner_receive_chunk(
    void *context, tr_raft_owner_t *owner, uint64_t group_id,
    const tr_raft_transport_payload_t *payload,
    tr_raft_multicore_chunk_result_t *out)
{
    identity_fixture *f = (identity_fixture *)context;
    owner_raft_probe *probe = &f->owner_groups[raft_group_slot(group_id)];
    const uint8_t *data;
    size_t bytes;
    uint8_t expected;
    if (probe->thread != cmeta_thread_current_token() ||
        !tr_raft_owner_contains(owner, group_id)) probe->wrong_owner = 1;

#ifdef TURBORAFT_TEST_DURABLE_NET_SNAPSHOT
    if (group_id == 103U &&
        payload->kind == TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_CHUNK) {
        tr_raft_multicore_chunk_receivers_t receivers = {0};
        int result;
        receivers.snapshot = probe->snapshot_receiver;
        result = tr_raft_multicore_chunk_receivers_handle(
            &receivers, payload, out);
        ++probe->chunk_calls;
        probe->chunk_bytes += payload->data.snapshot_chunk.data_length;
        if (result != SALTS_OK) probe->chunk_error = 1;
        return result;
    }
#endif

    if (payload->kind == TR_RAFT_WIRE_PAYLOAD_DATA_CHUNK) {
        data = payload->data.data_chunk.data;
        bytes = payload->data.data_chunk.data_length;
        expected = 0x39U;
    } else if (payload->kind == TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_CHUNK) {
        data = payload->data.snapshot_chunk.data;
        bytes = payload->data.snapshot_chunk.data_length;
        expected = 0x7bU;
    } else return SALTS_EPROTO;
    if (data == NULL || bytes == 0U) return SALTS_EPROTO;
    for (size_t i = 0U; i < bytes; ++i)
        if (data[i] != expected) probe->chunk_error = 1;
    probe->chunk_bytes += bytes;
    ++probe->chunk_calls;
    out->ack_valid = false; /* no actual Snapshot/Data durability here */
    out->durable_or_installed = false;
    return probe->chunk_error ? SALTS_EPROTO : SALTS_OK;
}

static int start_raft_group_owners(identity_fixture *f)
{
    tr_raft_multicore_factory_t factory = {0};
    size_t i;
    int result;

    f->owner_voters[0] = 1U;
    f->owner_voters[1] = 2U;
    f->owner_voters[2] = 3U;
    for (i = 0U; i < LINK_COUNT; ++i) {
        f->owner_assignments[i].group_id = 100U + NODE_IDS[i];
        f->owner_assignments[i].owner_index = (uint32_t)i;
        f->owner_assignments[i].election_min_ticks = 5U;
        f->owner_assignments[i].election_max_ticks = 9U;
    }
    f->owner_config.version = TR_RAFT_MULTICORE_VERSION;
    f->owner_config.owner_count = LINK_COUNT;
    f->owner_config.capacity = 2U;
    f->owner_config.owned_chunk_bytes_per_group = 512U;
    f->owner_config.work_budget = 1U;
    f->owner_config.tick_ms = 1000U;
    f->owner_config.idle_ms = 1U;
    f->owner_config.groups = f->owner_assignments;
    f->owner_config.group_count = LINK_COUNT;
    factory.context = f;
    factory.group_open = owner_group_open;
    factory.group_close = owner_group_close;
    factory.receive_chunk = owner_receive_chunk;
    result = tr_raft_multicore_create(
        &f->owner_config, &factory, &f->raft_owners);
    if (result != SALTS_OK) return result;
    return tr_raft_multicore_ingress_create(
        f->raft_owners, 7001U, &f->owner_ingress);
}

static int on_server_payload_and_forward(
    void *context, const tr_raft_transport_payload_t *payload)
{
    verified_payload_context *binding = (verified_payload_context *)context;
    identity_fixture *fixture = binding->fixture;
    int result;

    ++fixture->server_payload_inflight;
    result = on_server_payload(&fixture->server_received, payload);
    if (result == SALTS_OK) {
        /* Directory authenticates this exact mTLS peer and Group first.
         * Transfer only VALUES (Channel incarnation/Node/Group) and one
         * explicitly owned chunk into the existing Group Owner ring. */
        if (payload->kind == TR_RAFT_WIRE_PAYLOAD_DATA_CHUNK ||
            payload->kind == TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_CHUNK) {
            tr_raft_transport_reply_origin_t origin = {0};
            uint64_t request_id = 0U;
            result = tr_raft_cnet_channel_capture_reply_origin(
                binding->channel, payload->group_id, &origin);
            if (result == SALTS_OK)
                result = tr_raft_multicore_ingress_submit_with_origin(
                    fixture->owner_ingress, payload, &origin, &request_id);
            if (result == SALTS_OK) {
#ifdef TURBORAFT_TEST_DURABLE_NET_SNAPSHOT_SEGMENTED
                if (binding->chunk_request_id != 0U) {
                    binding->prior_chunk_origin = binding->chunk_origin;
                    binding->prior_chunk_request_id =
                        binding->chunk_request_id;
                }
#endif
                binding->chunk_origin = origin;
                binding->chunk_request_id = request_id;
            }
        } else {
            result = tr_raft_multicore_ingress_receive(
                fixture->owner_ingress, payload);
        }
    }
    if (result == SALTS_OK && fixture->stop_from_payload_checks == 0U) {
        /* This is a *real* TLS receive callback, not a simulated producer.
         * A stop attempt from inside progress must fail without tearing down
         * this callback's borrowed Directory/ingress context. */
        fixture->stop_from_payload_result =
            cnet_client_stop(&fixture->server, 0U);
        /* The callback owns this exact Channel generation. Even though
         * transport is ACTIVE, its destruction must fail while the CNet
         * receive callback still borrows its context and transport. */
        fixture->destroy_from_payload_result =
            tr_raft_cnet_channel_destroy(binding->channel);
        ++fixture->stop_from_payload_checks;
    }
    --fixture->server_payload_inflight;
    ++fixture->server_payload_completed;
    return result;
}

static int take_owner_completion(
    identity_fixture *f, size_t index,
    tr_raft_multicore_completion_t *out)
{
    const uint64_t end = cmeta_monotonic_ms() + 4000U;
    int result;

    do {
        result = tr_raft_multicore_take(
            f->raft_owners, 100U + NODE_IDS[index], out);
        if (result != SALTS_ENOENT) return result;
        cmeta_sleep_ms(1U);
    } while (cmeta_monotonic_ms() < end);
    return SALTS_ETIMEDOUT;
}

static int on_client_payload(void *user, const tr_raft_transport_payload_t *msg)
{
    remote_probe *sink = (remote_probe *)user;
#ifdef TURBORAFT_TEST_DURABLE_NET_SNAPSHOT
    if (msg != NULL && msg->kind == TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_ACK) {
        uint8_t bytes[24], digest[TR_RAFT_WIRE_SNAPSHOT_DIGEST_SIZE];
        const tr_raft_snapshot_ack_t *ack = &msg->data.snapshot_ack;
        memset(bytes, 0x7b, sizeof(bytes));
        if (cmeta_sha256(bytes, sizeof(bytes), digest) != SALTS_OK ||
            sink->node_id != 3U || msg->group_id != 103U ||
            ack->from != 2U || ack->to != 3U || ack->term != 3U ||
            ack->snapshot_index != 19U ||
            ack->snapshot_size != sizeof(bytes) ||
            ack->next_offset != sizeof(bytes) || !ack->accepted ||
            memcmp(ack->snapshot_digest, digest, sizeof(digest)) != 0) {
            ++sink->wrong_payload;
            return SALTS_EPROTO;
        }
        ++sink->snapshot_acks;
        return SALTS_OK;
    }
#endif
    if (msg == NULL || msg->kind != TR_RAFT_WIRE_PAYLOAD_RAFT ||
        msg->group_id != 100U + sink->node_id ||
        msg->data.raft.from != 2U ||
        msg->data.raft.to != sink->node_id ||
        msg->data.raft.type != TR_RAFT_MSG_HEARTBEAT_RESPONSE ||
        msg->data.raft.term != 3U) {
        ++sink->wrong_payload;
        return SALTS_EPROTO;
    }
    ++sink->received;
    return SALTS_OK;
}

static int fixture_cleanup(identity_fixture *f)
{
    size_t i;
    int result, first = SALTS_OK;
#define CLEAN_STEP(expr) do { result = (expr); \
    if (first == SALTS_OK && result != SALTS_OK) first = result; } while (0)

    for (i = 0U; i < LINK_COUNT; ++i) {
        if (f->outbound[i] != NULL)
            (void)tr_raft_cnet_channel_stop(f->outbound[i]);
        if (f->inbound[i] != NULL)
            (void)tr_raft_cnet_channel_stop(f->inbound[i]);
    }
    /* A close request is not a terminal callback barrier. The two
     * caller-driven CNet owners must finish their entire callback drain
     * before any borrowed Directory/ingress context can be freed. */
    if (f->clients_live) {
        result = cnet_client_stop(&f->clients, 2000U);
        if (result != SALTS_OK && result != SALTS_EALREADY)
            return result; /* fail closed: borrowed observers stay alive */
    }
    if (f->server_live) {
        result = cnet_client_stop(&f->server, 2000U);
        if (result != SALTS_OK && result != SALTS_EALREADY)
            return result; /* never free in-flight on_payload context */
    }
    if (f->server_payload_inflight != 0U)
        return SALTS_EBUSY;
    for (i = 0U; i < LINK_COUNT; ++i) {
        tr_raft_cnet_channel_status_t status = {0};
        if (f->outbound[i] != NULL) {
            CLEAN_STEP(tr_raft_cnet_channel_get_status(
                f->outbound[i], &status));
            if (!status.terminal) return SALTS_EBUSY;
            if (status.payload_writes_pending != 0U ||
                status.payloads_admitted !=
                    status.payloads_completed + status.payloads_canceled)
                return SALTS_EPROTO;
        }
        if (f->inbound[i] != NULL) {
            status = (tr_raft_cnet_channel_status_t){0};
            CLEAN_STEP(tr_raft_cnet_channel_get_status(
                f->inbound[i], &status));
            if (!status.terminal) return SALTS_EBUSY;
            if (status.payload_writes_pending != 0U ||
                status.payloads_admitted !=
                    status.payloads_completed + status.payloads_canceled)
                return SALTS_EPROTO;
        }
    }
    for (i = 0U; i < LINK_COUNT; ++i) {
        if (f->outbound[i] != NULL)
            CLEAN_STEP(tr_raft_cnet_channel_destroy(f->outbound[i]));
        if (f->inbound[i] != NULL)
            CLEAN_STEP(tr_raft_cnet_channel_destroy(f->inbound[i]));
    }

    /* CNet callbacks are now quiescent. Retire the two Raft owner threads
     * and their exact Group Services, then release the borrowed ingress. */
    if (f->raft_owners != NULL) {
        tr_raft_multicore_request_stop(f->raft_owners);
        CLEAN_STEP(tr_raft_multicore_stop(f->raft_owners));
    }
    if (f->owner_ingress != NULL)
        CLEAN_STEP(tr_raft_multicore_ingress_destroy(f->owner_ingress));
    tr_raft_multicore_destroy(f->raft_owners);
    for (i = 0U; i < LINK_COUNT; ++i)
        if (f->raft_owners != NULL &&
            (!f->owner_groups[i].closed || f->owner_groups[i].wrong_owner) &&
            first == SALTS_OK)
            first = SALTS_EPROTO;

    /* No registered Service/Channel may retain a borrowed Directory once
     * it is destroyed. Route providers never opened sockets of their own. */
    if (f->directory.active)
        CLEAN_STEP(tr_raft_cnet_peer_directory_destroy(&f->directory));
    for (i = 0U; i < LINK_COUNT; ++i) {
        if (f->authorized_peers[i] != NULL) {
            CLEAN_STEP(tr_raft_cnet_managed_peer_stop(f->authorized_peers[i]));
            CLEAN_STEP(tr_raft_cnet_managed_peer_destroy(f->authorized_peers[i]));
        }
    }
    if (f->directory_manager_live)
        CLEAN_STEP(cnet_manager_destroy(&f->directory_manager));
    if (f->clients_live) CLEAN_STEP(cnet_client_destroy(&f->clients));
    if (f->server_live) CLEAN_STEP(cnet_client_destroy(&f->server));
    if (f->listener_live) {
        CLEAN_STEP(cnet_listener_close(&f->listener));
        CLEAN_STEP(cnet_listener_destroy(&f->listener));
    }
    if (f->server_tls_live)
        CLEAN_STEP(cnet_tls_server_destroy(&f->server_tls));
#undef CLEAN_STEP
    return first;
}

#ifdef TURBORAFT_TEST_DURABLE_NET_SNAPSHOT
/* Verify durable disk state only AFTER the CNet and Multicore Owner threads
 * have terminated, closed their writer lock and released their borrows. */
static int durable_snapshot_verify_and_catch_up(const char *prefix)
{
    tr_raft_wal_storage_t *storage = NULL;
    tr_raft_wal_recovery_t recovery = {0};
    tr_raft_wal_storage_config_t config = {0};
    tr_raft_storage_t adapter = {0};
    tr_raft_entry_t next = {0};
    uint8_t bytes[24] = {0};
    size_t actual = 0U;
    int result;

#define REQUIRE(expr) do { if (!(expr)) { result = SALTS_EPROTO; goto done; } } while (0)
    config.path_prefix = prefix;
    config.segment_bytes = TR_RAFT_WAL_MIN_SEGMENT_BYTES;
    config.max_transaction_bytes = 8U * 1024U;
    config.max_live_segments = 4U;
    config.max_log_entries = 16U;
    config.max_snapshot_bytes = 1024U;
    config.create_if_missing = false;
    result = tr_raft_wal_storage_open(&config, &storage);
    if (result != SALTS_OK) return result;
    result = tr_raft_wal_storage_load(storage, &recovery);
    if (result != SALTS_OK) goto done;
    REQUIRE(recovery.term == 3U && recovery.commit_index == 19U &&
            recovery.snapshot_index == 19U && recovery.snapshot_term == 3U &&
            recovery.snapshot_size == sizeof(bytes) &&
            recovery.has_snapshot_configuration &&
            recovery.snapshot_configuration.member_count == 1U &&
            recovery.snapshot_configuration.members[0].node_id == 2U &&
            recovery.entry_count == 0U &&
            recovery.snapshot_source.read_at != NULL);
    result = recovery.snapshot_source.read_at(
        recovery.snapshot_source.context, 0U,
        bytes, sizeof(bytes), &actual);
    if (result != SALTS_OK) goto done;
    REQUIRE(actual == sizeof(bytes));
    for (size_t i = 0U; i < sizeof(bytes); ++i)
        REQUIRE(bytes[i] == 0x7bU);
    tr_raft_wal_recovery_destroy(&recovery);

    result = tr_raft_wal_storage_bind(storage, &adapter);
    if (result != SALTS_OK) goto done;
    next.index = 20U;
    next.term = 3U;
    next.command_id = 20U;
    next.data_length = 3U;
    memcpy(next.data, "new", 3U);
    result = adapter.begin(adapter.context);
    if (result != SALTS_OK) goto done;
    result = adapter.append_log(adapter.context, &next, 1U);
    if (result == SALTS_OK)
        result = adapter.write_commit_index(adapter.context, 20U);
    if (result == SALTS_OK)
        result = adapter.commit(adapter.context);
    else (void)adapter.rollback(adapter.context);
    if (result != SALTS_OK) goto done;
    result = tr_raft_wal_storage_close(storage);
    storage = NULL;
    if (result != SALTS_OK) return result;

    result = tr_raft_wal_storage_open(&config, &storage);
    if (result != SALTS_OK) return result;
    result = tr_raft_wal_storage_load(storage, &recovery);
    if (result != SALTS_OK) goto done;
    REQUIRE(recovery.snapshot_index == 19U &&
            recovery.snapshot_term == 3U &&
            recovery.commit_index == 20U &&
            recovery.entry_count == 1U &&
            recovery.entries[0].index == 20U &&
            recovery.entries[0].term == 3U &&
            recovery.entries[0].command_id == 20U &&
            recovery.entries[0].data_length == 3U &&
            memcmp(recovery.entries[0].data, "new", 3U) == 0);
done:
    tr_raft_wal_recovery_destroy(&recovery);
    if (storage != NULL) {
        int close_result = tr_raft_wal_storage_close(storage);
        if (result == SALTS_OK) result = close_result;
    }
#undef REQUIRE
    return result;
}

#ifdef TURBORAFT_TEST_DURABLE_NET_SNAPSHOT_FAILURE
static int durable_snapshot_verify_failed(const char *prefix)
{
    tr_raft_wal_storage_t *storage = NULL;
    tr_raft_wal_recovery_t recovery = {0};
    tr_raft_wal_storage_config_t config = {0};
    char published[SALTS_FS_MAX_PATH];
    int result;
    config.path_prefix = prefix;
    config.segment_bytes = TR_RAFT_WAL_MIN_SEGMENT_BYTES;
    config.max_transaction_bytes = 8U * 1024U;
    config.max_live_segments = 4U;
    config.max_log_entries = 16U;
    config.max_snapshot_bytes = 1024U;
    config.create_if_missing = false;
    result = tr_raft_wal_storage_open(&config, &storage);
    if (result != SALTS_OK) return result;
    result = tr_raft_wal_storage_load(storage, &recovery);
    if (result == SALTS_OK) {
        if (recovery.snapshot_index != 0U ||
            recovery.snapshot_size != 0U ||
            recovery.commit_index != 0U ||
            recovery.entry_count != 0U)
            result = SALTS_EPROTO;
    }
    tr_raft_wal_recovery_destroy(&recovery);
    if (result == SALTS_OK) {
        int length = snprintf(published, sizeof(published),
                              "%s.snapshot.19.3", prefix);
        if (length < 0 || (size_t)length >= sizeof(published) ||
            cmeta_fs_access(published, SALTS_FS_ACCESS_EXISTS) == SALTS_OK)
            result = SALTS_EPROTO;
    }
    {
        int close_result = tr_raft_wal_storage_close(storage);
        if (result == SALTS_OK) result = close_result;
    }
    return result;
}
#endif

static void durable_snapshot_cleanup_files(const char *prefix)
{
    char path[SALTS_FS_MAX_PATH];
    if (prefix == NULL) return;
    for (size_t i = 1U; i <= 4U; ++i) {
        (void)snprintf(path, sizeof(path), "%s.%08zu.wal", prefix, i);
        (void)cmeta_fs_unlink(path);
        (void)snprintf(path, sizeof(path), "%s.%08zu.wal.tmp", prefix, i);
        (void)cmeta_fs_unlink(path);
    }
    (void)snprintf(path, sizeof(path), "%s.snapshot.19.3", prefix);
    (void)cmeta_fs_unlink(path);
    (void)snprintf(path, sizeof(path), "%s.snapshot.19.3.tmp", prefix);
    (void)cmeta_fs_unlink(path);
    (void)snprintf(path, sizeof(path), "%s.manifest", prefix);
    (void)cmeta_fs_unlink(path);
    (void)snprintf(path, sizeof(path), "%s.manifest.tmp", prefix);
    (void)cmeta_fs_unlink(path);
    (void)snprintf(path, sizeof(path), "%s.lock", prefix);
    (void)cmeta_fs_unlink(path);
    (void)tt_remove_file(prefix);
}
#endif

static int run_two_distinct_peers(int forge_node_three)
{
    identity_fixture f = {0};
    cnet_client_config net = cnet_config();
    cnet_listener_config listen = {
        .backend = net.backend, .host = "127.0.0.1",
        .port = 0U, .backlog = LINK_COUNT
    };
    cnet_tls_server_config tls = {0};
    cnet_connect_options options = {0};
    tr_raft_cnet_channel_config_t channel = {0};
    char ca_file[512], multi_ca_file[512], client_cert[LINK_COUNT][512];
    char client_key[LINK_COUNT][512], server_cert[512], server_key[512];
    char uri[128];
    uint16_t port = 0U;
    size_t events = 0U, i;
    int result = SALTS_OK;
    int sent = 0;
    unsigned round;
    const char *failed_stage = "none";

#define TRY_STAGE(expr) do { result = (expr); \
    if (result != SALTS_OK) { failed_stage = #expr; goto cleanup; } } while (0)

    TRY_STAGE(fixture_path(ca_file, sizeof(ca_file), "ca.pem"));
    TRY_STAGE(fixture_path(multi_ca_file, sizeof(multi_ca_file),
                           "three-node-client-ca.pem"));
    TRY_STAGE(fixture_path(client_cert[0], sizeof(client_cert[0]), "node1-cert.pem"));
    TRY_STAGE(fixture_path(client_key[0], sizeof(client_key[0]), "node1-key.pem"));
    TRY_STAGE(fixture_path(client_cert[1], sizeof(client_cert[1]), "node2-client-cert.pem"));
    TRY_STAGE(fixture_path(client_key[1], sizeof(client_key[1]), "node2-client-key.pem"));
    TRY_STAGE(fixture_path(server_cert, sizeof(server_cert), "node2-cert.pem"));
    TRY_STAGE(fixture_path(server_key, sizeof(server_key), "node2-key.pem"));

    f.server_peer_cert[0][0] = CERT_NODE1;
    f.server_peer_cert[1][0] = CERT_NODE3;
    f.server_peers[0] = (tr_raft_cnet_peer_identity_t){
        1U, f.server_peer_cert[0], 1U
    };
    f.server_peers[1] = (tr_raft_cnet_peer_identity_t){
        3U, f.server_peer_cert[1], 1U
    };
    f.server_policy = (tr_raft_cnet_identity_policy_t){
        2U, f.server_peers, LINK_COUNT
    };
    TRY_STAGE(tr_raft_cnet_identity_policy_validate(&f.server_policy));

    tls.size = sizeof(tls);
    tls.ca_file = multi_ca_file;
    tls.cert_file = server_cert;
    tls.key_file = server_key;
    tls.client_auth = CNET_TLS_CLIENT_AUTH_REQUIRED;
    TRY_STAGE(cnet_tls_server_init(&f.server_tls, &tls));
    f.server_tls_live = 1;

    TRY_STAGE(cnet_client_init(&f.clients, &net));
    f.clients_live = 1;
    TRY_STAGE(cnet_client_init(&f.server, &net));
    f.server_live = 1;
    TRY_STAGE(cnet_listener_init(&f.listener, &listen));
    f.listener_live = 1;
    TRY_STAGE(cnet_listener_port(&f.listener, &port));
    if (snprintf(uri, sizeof(uri), "tls://127.0.0.1:%u",
                 (unsigned)port) <= 0) {
        result = SALTS_EINVAL; failed_stage = "format TLS URI"; goto cleanup;
    }

    /*
     * Route preflight runs on the SAME CNet Owner as incoming server TLS.
     * Authorized ManagedPeer descriptors are real Salts::Manager/ManagedDial
     * instances, but remain undialed: inbound accepted streams use the
     * listener's CNet lifecycle, never a hidden outbound connection.
     */
    {
        cnet_manager_config manager = {
            sizeof(cnet_manager_config), CNET_MANAGER_VERSION,
            &f.server, LINK_COUNT, LINK_COUNT
        };
        tr_raft_cnet_peer_directory_config_t directory_config = {0};

        TRY_STAGE(cnet_manager_init(&f.directory_manager, &manager));
        f.directory_manager_live = 1;
        for (i = 0U; i < LINK_COUNT; ++i) {
            tr_raft_cnet_managed_peer_config_t cfg = {0};
            cnet_reconnect_config reconnect = {0};

            f.directory_profiles[i].size = sizeof(f.directory_profiles[i]);
            f.directory_profiles[i].ca_file = ca_file;
            f.directory_profiles[i].server_name = "node-2.mesh";
            reconnect.size = sizeof(reconnect);
            reconnect.version = CNET_RECOVERY_POLICY_VERSION;
            reconnect.max_attempts = 2U;
            reconnect.deadline_ms = UINT64_MAX - 1U;
            reconnect.initial_backoff_ms = 4U;
            reconnect.maximum_backoff_ms = 16U;
            reconnect.jitter_seed = i + 1U;
            cfg.manager = &f.directory_manager;
            cfg.channel.client = &f.server;
            cfg.channel.identity = &f.server_policy;
            cfg.channel.handshake = hello_config(2U);
            cfg.channel.first_outbound_message_id = 1U;
            cfg.channel.host_module_generation = UINT64_C(90010001);
            cfg.channel.on_payload = on_server_payload;
            cfg.channel.payload_context = &f.server_received;
            cfg.expected_peer_node_id = NODE_IDS[i];
            cfg.uri = "tls://127.0.0.1:1"; /* reserved, never dialed */
            cfg.tls = &f.directory_profiles[i];
            cfg.reconnect = reconnect;
            cfg.recovery_episode_ms = 1000U;
            TRY_STAGE(tr_raft_cnet_managed_peer_create(
                &cfg, &f.authorized_peers[i]));
            f.authorized_routes[i].node_id = NODE_IDS[i];
            f.authorized_routes[i].peer = f.authorized_peers[i];
            f.admitted_groups[i] = 100U + NODE_IDS[i];
        }

        directory_config.version = TR_RAFT_CNET_PEER_DIRECTORY_VERSION;
        directory_config.local_node_id = 2U;
        directory_config.owner_index = 0U;
        directory_config.owner_count = 1U;
        directory_config.identities = &f.server_policy;
        directory_config.entries = f.authorized_routes;
        directory_config.entry_count = LINK_COUNT;
        directory_config.groups = f.admitted_groups;
        directory_config.group_count = LINK_COUNT;
        TRY_STAGE(tr_raft_cnet_peer_directory_init(
            &f.directory, &directory_config));
    }

#ifdef TURBORAFT_TEST_DURABLE_NET_SNAPSHOT
    f.snapshot_prefix = tt_make_temp_file("turboraft-cnet-snapshot", ".data");
    if (f.snapshot_prefix == NULL) {
        result = SALTS_ENOMEM;
        failed_stage = "create isolated Snapshot WAL prefix";
        goto cleanup;
    }
#endif
    TRY_STAGE(start_raft_group_owners(&f));

    f.server_channel_config.client = &f.server;
    f.server_channel_config.identity = &f.server_policy;
    f.server_channel_config.handshake = hello_config(2U);
    f.server_channel_config.first_outbound_message_id = 1U;
    f.server_channel_config.host_module_generation = UINT64_C(90010001);
    f.server_channel_config.on_payload = on_server_payload;
    f.server_channel_config.payload_context = &f.server_received;

    for (i = 0U; i < LINK_COUNT; ++i) {
        /* These are distinct physical client certificates. Both have a
         * single explicitly pinned Node2 server identity and exact SNI. */
        f.client_peer_cert[i][0] = CERT_SERVER_NODE2;
        f.client_peer[i] = (tr_raft_cnet_peer_identity_t){
            2U, f.client_peer_cert[i], 1U
        };
        f.client_policy[i] = (tr_raft_cnet_identity_policy_t){
            NODE_IDS[i], &f.client_peer[i], 1U
        };
        f.clients_received[i].node_id = NODE_IDS[i];

        f.client_tls[i].size = sizeof(f.client_tls[i]);
        f.client_tls[i].ca_file = ca_file;
        f.client_tls[i].cert_file = client_cert[i];
        f.client_tls[i].key_file = client_key[i];
        f.client_tls[i].server_name = "node-2.mesh";
        channel = (tr_raft_cnet_channel_config_t){0};
        channel.client = &f.clients;
        channel.identity = &f.client_policy[i];
        channel.handshake = hello_config(
            forge_node_three && i == 1U ? 1U : NODE_IDS[i]);
        if (forge_node_three && i == 1U) {
            /* This invalid assignment is only the test's malicious HELLO.
             * The local client identity policy follows the claimed ID,
             * while Node2's trusted fingerprint table still maps it to 3. */
            f.client_policy[i].local_node_id = 1U;
        }
        channel.first_outbound_message_id = 1U;
        channel.host_module_generation = UINT64_C(90010001);
        channel.on_payload = on_client_payload;
        channel.payload_context = &f.clients_received[i];
        TRY_STAGE(tr_raft_cnet_channel_create(&channel, &f.outbound[i]));

        options = (cnet_connect_options){0};
        options.uri = uri;
        options.tls = &f.client_tls[i];
        options.observer = tr_raft_cnet_channel_observer(f.outbound[i]);
        TRY_STAGE(cnet_connect(&f.clients, &options, &f.outbound_handles[i]));
        TRY_STAGE(tr_raft_cnet_channel_attach(
            f.outbound[i], f.outbound_handles[i]));
    }

    for (round = 0U; round < MAX_PROGRESS; ++round) {
        int ready = 0;
        TRY_STAGE(cnet_client_poll(&f.clients, 1U, &events));
        if (f.accepted < LINK_COUNT) {
            TRY_STAGE(cnet_listener_wait(&f.listener, 0U, &ready));
            if (ready) {
                const size_t position = (size_t)f.accepted;
                tr_raft_cnet_channel_config_t verified = f.server_channel_config;
                tr_raft_transport_payload_t preauth = {0};
                cnet_observer observer;

                f.ingress[position].directory = &f.directory;
                f.ingress[position].on_payload =
                    on_server_payload_and_forward;
                f.verified_payloads[position].fixture = &f;
                f.ingress[position].context = &f.verified_payloads[position];
                verified.on_payload = tr_raft_cnet_peer_directory_receive;
                verified.payload_context = &f.ingress[position];
                TRY_STAGE(tr_raft_cnet_channel_create(
                    &verified, &f.inbound[position]));
                f.ingress[position].channel = f.inbound[position];
                f.verified_payloads[position].channel = f.inbound[position];

                /* Even a known Node/Group cannot enter before this exact
                 * TLS Channel has successfully negotiated reciprocal ACK. */
                preauth.group_id = 101U;
                preauth.kind = TR_RAFT_WIRE_PAYLOAD_RAFT;
                preauth.data.raft.from = 1U;
                preauth.data.raft.to = 2U;
                if (tr_raft_cnet_peer_directory_receive(
                        &f.ingress[position], &preauth) != SALTS_EBUSY) {
                    result = SALTS_EPROTO;
                    failed_stage = "pre-TLS directory ingress admission";
                    goto cleanup;
                }
                observer = tr_raft_cnet_channel_observer(f.inbound[position]);
                TRY_STAGE(cnet_listener_accept_tls(
                    &f.listener, &f.server, &f.server_tls,
                    &observer, &f.inbound_handles[position]));
                TRY_STAGE(tr_raft_cnet_channel_attach(
                    f.inbound[position], f.inbound_handles[position]));
                ++f.accepted;
            }
        }
        TRY_STAGE(cnet_client_poll(&f.server, 1U, &events));

        if (!sent) {
            int both_clients_ready = 1;
            int both_server_links_ready = 1;
            int reject_seen = 0;
            for (i = 0U; i < LINK_COUNT; ++i) {
                tr_raft_cnet_channel_status_t st = {0};
                if (f.outbound[i] == NULL || f.inbound[i] == NULL) {
                    both_clients_ready = both_server_links_ready = 0;
                    continue;
                }
                TRY_STAGE(tr_raft_cnet_channel_get_status(f.outbound[i], &st));
                if (st.phase != TR_RAFT_CNET_CHANNEL_ACTIVE)
                    both_clients_ready = 0;
                TRY_STAGE(tr_raft_cnet_channel_get_status(f.inbound[i], &st));
                if (st.phase != TR_RAFT_CNET_CHANNEL_ACTIVE)
                    both_server_links_ready = 0;
                if (st.phase == TR_RAFT_CNET_CHANNEL_FAILED)
                    reject_seen = 1;
            }

            if (forge_node_three && reject_seen) {
                tr_raft_cnet_channel_status_t healthy = {0};
                tr_raft_cnet_channel_status_t rejected = {0};
                tr_raft_cnet_channel_status_t node1_client = {0};
                size_t healthy_socket = LINK_COUNT;

                /* The bad Node3 certificate is genuine but its HELLO
                 * falsely declares Node1. The healthy Node1 must still
                 * establish its own reciprocal session and exchange Raft.
                 * Accept order is not an identity or scheduling guarantee. */
                for (i = 0U; i < LINK_COUNT; ++i) {
                    tr_raft_cnet_channel_status_t st = {0};
                    TRY_STAGE(tr_raft_cnet_channel_get_status(f.inbound[i], &st));
                    if (st.authenticated_peer_node_id == 1U) {
                        healthy = st;
                        healthy_socket = i;
                    } else if (st.authenticated_peer_node_id == 3U)
                        rejected = st;
                }
                TRY_STAGE(tr_raft_cnet_channel_get_status(
                    f.outbound[0], &node1_client));
                if (healthy_socket == LINK_COUNT ||
                    healthy.phase != TR_RAFT_CNET_CHANNEL_ACTIVE ||
                    node1_client.phase != TR_RAFT_CNET_CHANNEL_ACTIVE ||
                    rejected.phase != TR_RAFT_CNET_CHANNEL_FAILED)
                    continue;

                if (!sent) {
                    tr_raft_transport_payload_t request = {0};
                    tr_raft_transport_payload_t response = {0};
                    request.group_id = 101U;
                    request.kind = TR_RAFT_WIRE_PAYLOAD_RAFT;
                    request.data.raft.type = TR_RAFT_MSG_HEARTBEAT_REQUEST;
                    request.data.raft.term = 3U;
                    request.data.raft.from = 1U;
                    request.data.raft.to = 2U;
                    response.group_id = 101U;
                    response.kind = TR_RAFT_WIRE_PAYLOAD_RAFT;
                    response.data.raft.type = TR_RAFT_MSG_HEARTBEAT_RESPONSE;
                    response.data.raft.term = 3U;
                    response.data.raft.from = 2U;
                    response.data.raft.to = 1U;
                    TRY_STAGE(tr_raft_cnet_channel_send(
                        f.outbound[0], &request));
                    TRY_STAGE(tr_raft_cnet_channel_send(
                        f.inbound[healthy_socket], &response));
                    sent = 1;
                }
                if (f.server_received.received_from_one == 1U &&
                    f.clients_received[0].received == 1U)
                    break;
                continue;
            }
            if (!forge_node_three && both_clients_ready &&
                both_server_links_ready) {
                /* Reject inconsistent Node/Group even if the TLS channel
                 * itself is authenticated and the connection is ACTIVE.
                 * No invalid payload reaches the borrowed Service sink. */
                for (i = 0U; i < LINK_COUNT; ++i) {
                    tr_raft_cnet_channel_status_t st = {0};
                    tr_raft_transport_payload_t invalid = {0};
                    TRY_STAGE(tr_raft_cnet_channel_get_status(f.inbound[i], &st));
                    invalid.kind = TR_RAFT_WIRE_PAYLOAD_RAFT;
                    invalid.group_id = 100U + st.authenticated_peer_node_id;
                    invalid.data.raft.type = TR_RAFT_MSG_HEARTBEAT_REQUEST;
                    invalid.data.raft.term = 3U;
                    invalid.data.raft.from =
                        st.authenticated_peer_node_id == 1U ? 3U : 1U;
                    invalid.data.raft.to = 2U;
                    if (tr_raft_cnet_peer_directory_receive(
                            &f.ingress[i], &invalid) != SALTS_EPROTO) {
                        result = SALTS_EPROTO;
                        failed_stage = "valid TLS cannot forge another source";
                        goto cleanup;
                    }
                    invalid.data.raft.from = st.authenticated_peer_node_id;
                    invalid.group_id = 999U;
                    if (tr_raft_cnet_peer_directory_receive(
                            &f.ingress[i], &invalid) != SALTS_ENOENT) {
                        result = SALTS_EPROTO;
                        failed_stage = "valid TLS cannot enter unknown Group";
                        goto cleanup;
                    }
                }
                if (f.server_received.received_from_one != 0U ||
                    f.server_received.received_from_three != 0U ||
                    f.server_received.wrong_payloads != 0U) {
                    result = SALTS_EPROTO;
                    failed_stage = "invalid ingress reached Service callback";
                    goto cleanup;
                }
                /* Correlate each server connection to its *TLS-authenticated*
                 * Node ID, never to accept order or a raw caller-supplied name. */
                for (i = 0U; i < LINK_COUNT; ++i) {
                    tr_raft_transport_payload_t payload = {0};
                    payload.group_id = 100U + NODE_IDS[i];
                    payload.kind = TR_RAFT_WIRE_PAYLOAD_RAFT;
                    payload.data.raft.type = TR_RAFT_MSG_HEARTBEAT_REQUEST;
                    payload.data.raft.term = 3U;
                    payload.data.raft.from = NODE_IDS[i];
                    payload.data.raft.to = 2U;
                    TRY_STAGE(tr_raft_cnet_channel_send(
                        f.outbound[i], &payload));
                }
                for (i = 0U; i < LINK_COUNT; ++i) {
                    tr_raft_cnet_channel_status_t st = {0};
                    tr_raft_transport_payload_t payload = {0};
                    TRY_STAGE(tr_raft_cnet_channel_get_status(f.inbound[i], &st));
                    if (st.authenticated_peer_node_id != 1U &&
                        st.authenticated_peer_node_id != 3U) {
                        result = SALTS_EPROTO;
                        failed_stage = "remote TLS identity selection";
                        goto cleanup;
                    }
                    payload.group_id = 100U + st.authenticated_peer_node_id;
                    payload.kind = TR_RAFT_WIRE_PAYLOAD_RAFT;
                    payload.data.raft.type = TR_RAFT_MSG_HEARTBEAT_RESPONSE;
                    payload.data.raft.term = 3U;
                    payload.data.raft.from = 2U;
                    payload.data.raft.to = st.authenticated_peer_node_id;
                    TRY_STAGE(tr_raft_cnet_channel_send(f.inbound[i], &payload));
                }
                /* Two distinct TLS-certified peer IDs send a DATA and a
                 * SNAPSHOT chunk. CNet owns their retained SG writes;
                 * Directory admits them to the existing Multicore inbox. */
                {
                    tr_raft_transport_payload_t data = {0}, snapshot = {0};
                    uint8_t d[16], s[24];
                    memset(d, 0x39, sizeof(d));
                    memset(s, 0x7b, sizeof(s));
                    data.group_id = 101U;
                    data.kind = TR_RAFT_WIRE_PAYLOAD_DATA_CHUNK;
                    data.data.data_chunk.from = 1U;
                    data.data.data_chunk.to = 2U;
                    data.data.data_chunk.term = 3U;
                    data.data.data_chunk.stream_id = 9U;
                    data.data.data_chunk.stream_size = sizeof(d);
                    data.data.data_chunk.data_length = sizeof(d);
                    data.data.data_chunk.data = d;
                    data.data.data_chunk.done = true;

                    snapshot.group_id = 103U;
                    snapshot.kind = TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_CHUNK;
                    snapshot.data.snapshot_chunk.from = 3U;
                    snapshot.data.snapshot_chunk.to = 2U;
                    snapshot.data.snapshot_chunk.term = 3U;
                    snapshot.data.snapshot_chunk.snapshot_index = 19U;
                    snapshot.data.snapshot_chunk.snapshot_term = 3U;
                    snapshot.data.snapshot_chunk.snapshot_size = sizeof(s);
                    snapshot.data.snapshot_chunk.data_length = sizeof(s);
                    snapshot.data.snapshot_chunk.data = s;
                    snapshot.data.snapshot_chunk.done = true;
                    snapshot.data.snapshot_chunk.has_configuration = true;
                    snapshot.data.snapshot_chunk.configuration.phase =
                        TR_RAFT_CONF_FINAL;
                    snapshot.data.snapshot_chunk.configuration.member_count = 1U;
                    snapshot.data.snapshot_chunk.configuration.members[0].node_id = 2U;
                    snapshot.data.snapshot_chunk.configuration.members[0].roles =
                        TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER;
#ifdef TURBORAFT_TEST_DURABLE_NET_SNAPSHOT
                    TRY_STAGE(cmeta_sha256(
                        s, sizeof(s),
                        snapshot.data.snapshot_chunk.snapshot_digest));
#endif
                    TRY_STAGE(tr_raft_cnet_channel_send(f.outbound[0], &data));
#ifdef TURBORAFT_TEST_DURABLE_NET_SNAPSHOT_SEGMENTED
                    snapshot.data.snapshot_chunk.data_length = 12U;
                    snapshot.data.snapshot_chunk.done = false;
                    TRY_STAGE(tr_raft_cnet_channel_send(f.outbound[1], &snapshot));
                    snapshot.data.snapshot_chunk.snapshot_offset = 12U;
                    snapshot.data.snapshot_chunk.data_length = 12U;
                    snapshot.data.snapshot_chunk.data = s + 12U;
                    snapshot.data.snapshot_chunk.done = true;
                    snapshot.data.snapshot_chunk.has_configuration = false;
#endif
                    TRY_STAGE(tr_raft_cnet_channel_send(f.outbound[1], &snapshot));
                    /* This memory is borrowed until send returns, NOT held
                     * until TLS send terminal or Owner callback. */
                    memset(d, 0x44, sizeof(d));
                    memset(s, 0x44, sizeof(s));
                }
                sent = 1;
            }
        }

        if (forge_node_three && sent &&
            f.server_received.received_from_one == 1U &&
            f.clients_received[0].received == 1U)
            break;
        if (sent && f.server_received.received_from_one == 1U &&
            f.server_received.received_from_three == 1U &&
            f.server_received.received_data == 1U &&
#ifdef TURBORAFT_TEST_DURABLE_NET_SNAPSHOT_SEGMENTED
            f.server_received.received_snapshot == 2U &&
#else
            f.server_received.received_snapshot == 1U &&
#endif
            f.clients_received[0].received == 1U &&
            f.clients_received[1].received == 1U)
            break;
    }

    if (!forge_node_three) {
        tr_raft_cnet_channel_status_t st[2] = {{0}};
        if (!sent || f.accepted != LINK_COUNT ||
            f.server_received.received_from_one != 1U ||
            f.server_received.received_from_three != 1U ||
            f.server_received.received_data != 1U ||
#ifdef TURBORAFT_TEST_DURABLE_NET_SNAPSHOT_SEGMENTED
            f.server_received.received_snapshot != 2U ||
#else
            f.server_received.received_snapshot != 1U ||
#endif
            f.server_received.wrong_payloads ||
            f.clients_received[0].received != 1U ||
            f.clients_received[1].received != 1U ||
            f.clients_received[0].wrong_payload ||
            f.clients_received[1].wrong_payload) {
            result = SALTS_EPROTO;
            failed_stage = "separate Node1/Node3 Raft group isolation";
            goto cleanup;
        }
        TRY_STAGE(tr_raft_cnet_channel_get_status(f.inbound[0], &st[0]));
        TRY_STAGE(tr_raft_cnet_channel_get_status(f.inbound[1], &st[1]));
        if (st[0].authenticated_peer_node_id == st[1].authenticated_peer_node_id ||
            (st[0].authenticated_peer_node_id != 1U &&
             st[0].authenticated_peer_node_id != 3U) ||
            (st[1].authenticated_peer_node_id != 1U &&
             st[1].authenticated_peer_node_id != 3U)) {
            result = SALTS_EPROTO;
            failed_stage = "distinct authenticated certificate identities";
        }
    } else if (!sent || f.accepted != LINK_COUNT ||
               f.server_received.received_from_one != 1U ||
               f.clients_received[0].received != 1U ||
               f.clients_received[0].wrong_payload != 0 ||
               f.clients_received[1].received != 0U ||
               f.server_received.received_from_three != 0U ||
               f.server_received.wrong_payloads != 0U) {
        result = SALTS_EPROTO;
        failed_stage = "forgery must have no delivered payload";
    }

    /* The first actual verified-TLS on_payload attempted to stop its own
     * CNet progress Owner. It must have been rejected *inside* the callback,
     * while normal delivery and independent Raft Owner completion continue. */
    if (result == SALTS_OK &&
        (f.stop_from_payload_checks != 1U ||
         f.stop_from_payload_result != SALTS_EBUSY ||
         f.destroy_from_payload_result != SALTS_EBUSY ||
         f.server_payload_inflight != 0U ||
         f.server_payload_completed !=
             (forge_node_three ? 1U : 2U * LINK_COUNT
#ifdef TURBORAFT_TEST_DURABLE_NET_SNAPSHOT_SEGMENTED
              + 1U
#endif
             ))) {
        result = SALTS_EPROTO;
        failed_stage = "real CNet callback must reject stop and Channel destroy";
    }

    /* A successful TLS receive has one Multicore completion on the exact
     * different Raft Group Owner. A forged Node3 HELLO admits none to
     * Group103; it cannot fake a terminal or steal that completion credit. */
    if (result == SALTS_OK && f.owner_ingress != NULL) {
        for (i = 0U; i < LINK_COUNT; ++i) {
            tr_raft_multicore_completion_t completion = {0};
            const int expected = !forge_node_three || i == 0U;
            int observed = SALTS_ENOENT;

            if (expected)
                observed = take_owner_completion(&f, i, &completion);
            else
                observed = tr_raft_multicore_take(
                    f.raft_owners, 100U + NODE_IDS[i], &completion);

            if (expected &&
                (observed != SALTS_OK ||
                 completion.operation != TR_RAFT_MULTICORE_STEP ||
                 completion.result != SALTS_OK ||
                 completion.request_id == 0U)) {
                failed_stage = "verified Raft payload did not finish on Group Owner";
                result = SALTS_EPROTO;
                break;
            }
            if (!expected && observed != SALTS_ENOENT) {
                failed_stage = "forged TLS peer reached the other Raft Group";
                result = SALTS_EPROTO;
                break;
            }
            if (expected && !forge_node_three) {
                tr_raft_multicore_completion_t chunk_completion = {0};
                tr_raft_multicore_group_status_t status = {0};
                verified_payload_context *binding = NULL;
                observed = take_owner_completion(&f, i, &chunk_completion);
                for (size_t k = 0U; k < LINK_COUNT; ++k) {
                    if (f.verified_payloads[k].chunk_origin.authenticated_peer_node_id ==
                            NODE_IDS[i]) {
                        binding = &f.verified_payloads[k];
                        break;
                    }
                }
                if (observed != SALTS_OK || binding == NULL ||
                    binding->chunk_request_id == 0U ||
                    chunk_completion.request_id != binding->chunk_request_id ||
                    chunk_completion.reply_origin.host_module_generation !=
                        binding->chunk_origin.host_module_generation ||
                    chunk_completion.reply_origin.host_module_generation !=
                        UINT64_C(90010001) ||
                    chunk_completion.reply_origin.channel_instance !=
                        binding->chunk_origin.channel_instance ||
                    chunk_completion.reply_origin.connection_token !=
                        binding->chunk_origin.connection_token ||
                    chunk_completion.reply_origin.group_id != 100U + NODE_IDS[i] ||
                    chunk_completion.reply_origin.authenticated_peer_node_id !=
                        NODE_IDS[i] ||
                    chunk_completion.operation != TR_RAFT_MULTICORE_RECEIVE_CHUNK ||
#ifdef TURBORAFT_TEST_DURABLE_NET_SNAPSHOT_FAILURE
                    chunk_completion.result !=
                        (i == 1U ? SALTS_EIO : SALTS_OK) ||
#else
                    chunk_completion.result != SALTS_OK ||
#endif
#ifdef TURBORAFT_TEST_DURABLE_NET_SNAPSHOT
                    (i == 0U && (chunk_completion.value.chunk.ack_valid ||
                                  chunk_completion.value.chunk.durable_or_installed)) ||
#ifdef TURBORAFT_TEST_DURABLE_NET_SNAPSHOT_FAILURE
                    (i == 1U && (chunk_completion.value.chunk.ack_valid ||
                                  chunk_completion.value.chunk.durable_or_installed)) ||
#else
                    (i == 1U && (!chunk_completion.value.chunk.ack_valid ||
                                  !chunk_completion.value.chunk.durable_or_installed)) ||
#endif
#else
                    chunk_completion.value.chunk.ack_valid ||
                    chunk_completion.value.chunk.durable_or_installed ||
#endif
                    chunk_completion.value.chunk.kind !=
                        (i == 0U ? TR_RAFT_WIRE_PAYLOAD_DATA_CHUNK
                                 : TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_CHUNK) ||
                    f.owner_groups[i].chunk_calls !=
#ifdef TURBORAFT_TEST_DURABLE_NET_SNAPSHOT_SEGMENTED
                        (i == 1U ? 2U : 1U) ||
#else
                        1U ||
#endif

#ifdef TURBORAFT_TEST_DURABLE_NET_SNAPSHOT_FAILURE
                    (i == 0U && f.owner_groups[i].chunk_error) ||
                    (i == 1U && !f.owner_groups[i].chunk_error) ||
#else
                    f.owner_groups[i].chunk_error ||
#endif
                    f.owner_groups[i].chunk_bytes != (i == 0U ? 16U : 24U) ||
                    tr_raft_multicore_group_status(
                        f.raft_owners, 100U + NODE_IDS[i], &status) != SALTS_OK ||
                    status.owned_chunk_bytes != 0U ||
                    status.outstanding != 0U) {
                    failed_stage = "verified TLS origin was not returned by exact Group Owner";
                    result = SALTS_EPROTO;
                    break;
                }
#ifdef TURBORAFT_TEST_DURABLE_NET_SNAPSHOT
                if (i == 1U) {
#ifdef TURBORAFT_TEST_DURABLE_NET_SNAPSHOT_FAILURE
                    /* An actual Snapshot staging write failed in the
                     * Group Owner. CNet must never manufacture an ACK. */
                    if (f.owner_groups[1].installed_snapshots != 0U ||
                        f.owner_groups[1].snapshot_write_faults != 1U ||
                        f.clients_received[1].snapshot_acks != 0U ||
                        tr_raft_cnet_channel_send_chunk_completion(
                            binding->channel, &chunk_completion) != SALTS_ENOTSUP) {
                        failed_stage = "failed WAL snapshot must not ACK or retry";
                        result = SALTS_EPROTO;
                        break;
                    }
#else
                    tr_raft_multicore_completion_t stale = chunk_completion;
                    tr_raft_cnet_channel_status_t before = {0}, after = {0};
                    tr_raft_cnet_channel_t *other =
                        f.verified_payloads[0].channel == binding->channel
                            ? f.verified_payloads[1].channel
                            : f.verified_payloads[0].channel;
                    TRY_STAGE(tr_raft_cnet_channel_get_status(
                        binding->channel, &before));
                    /* The real WAL commit has finished on the Group Owner.
                     * Foreign TLS/Component generation cannot send its ACK
                     * or reserve any CNet send credit. */
                    stale.reply_origin.host_module_generation += 1U;
                    if (f.owner_groups[1].installed_snapshots != 1U ||
                        f.clients_received[1].snapshot_acks != 0U ||
                        tr_raft_cnet_channel_send_chunk_completion(
                            binding->channel, &stale) != SALTS_ECANCELED ||
                        tr_raft_cnet_channel_send_chunk_completion(
                            other, &chunk_completion) != SALTS_ECANCELED) {
                        failed_stage = "durable Snapshot stale-generation ACK fencing";
                        result = SALTS_EPROTO;
                        break;
                    }
                    TRY_STAGE(tr_raft_cnet_channel_get_status(
                        binding->channel, &after));
                    if (before.payloads_admitted != after.payloads_admitted ||
                        before.payload_writes_pending !=
                            after.payload_writes_pending) {
                        failed_stage = "rejected stale ACK consumed send credits";
                        result = SALTS_EPROTO;
                        break;
                    }
                    TRY_STAGE(tr_raft_cnet_channel_send_chunk_completion(
                        binding->channel, &chunk_completion));
#endif
                } else
#endif
                {
                    /* Group101 DATA handler never fsyncs, so cannot ACK. */
                    if (tr_raft_cnet_channel_send_chunk_completion(
                            binding->channel, &chunk_completion) != SALTS_ENOTSUP) {
                        failed_stage = "mock-owned DATA forged positive network ACK";
                        result = SALTS_EPROTO;
                        break;
                    }
                }
            }
        }
    }

#ifdef TURBORAFT_TEST_DURABLE_NET_SNAPSHOT
#ifdef TURBORAFT_TEST_DURABLE_NET_SNAPSHOT_FAILURE
    if (!forge_node_three && result == SALTS_OK &&
        f.clients_received[1].snapshot_acks != 0U) {
        failed_stage = "failed snapshot unexpectedly ACKed before close";
        result = SALTS_EPROTO;
    }
#else
    if (!forge_node_three && result == SALTS_OK) {
        /* The receiver reached disk before CNet admitted this ACK. Poll
         * only the original two CNet Owners, no background retry/queue. */
        for (round = 0U; round < MAX_PROGRESS &&
             f.clients_received[1].snapshot_acks == 0U; ++round) {
            TRY_STAGE(cnet_client_poll(&f.server, 1U, &events));
            TRY_STAGE(cnet_client_poll(&f.clients, 1U, &events));
        }
        if (f.clients_received[1].snapshot_acks != 1U ||
            f.clients_received[1].wrong_payload != 0) {
            failed_stage = "certified Node3 did not receive exactly one durable Snapshot ACK";
            result = SALTS_EPROTO;
        }
    }
#endif
#endif

cleanup:
    {
        int close_status = fixture_cleanup(&f);
        if (result == SALTS_OK && close_status != SALTS_OK) {
            failed_stage = "complete CNet TLS terminal teardown";
            result = close_status;
        }
    }
#ifdef TURBORAFT_TEST_DURABLE_NET_SNAPSHOT
    if (f.snapshot_prefix != NULL) {
        if (result == SALTS_OK && !forge_node_three) {
#ifdef TURBORAFT_TEST_DURABLE_NET_SNAPSHOT_FAILURE
            result = durable_snapshot_verify_failed(f.snapshot_prefix);
            if (result != SALTS_OK)
                failed_stage = "failed TLS Snapshot leaked into reopened WAL";
#else
            result = durable_snapshot_verify_and_catch_up(f.snapshot_prefix);
            if (result != SALTS_OK)
                failed_stage = "reopen installed WAL and commit index20 catch-up";
#endif
        }
        durable_snapshot_cleanup_files(f.snapshot_prefix);
        free(f.snapshot_prefix);
        f.snapshot_prefix = NULL;
    }
#endif
#undef TRY_STAGE
    if (result != SALTS_OK) {
        fprintf(stderr,
                "distinct CNet TLS peers mode=%d stage=%s result=%d accepted=%d "
                "node1=%zu node3=%zu client1=%zu client3=%zu wrong=%zu\n",
                forge_node_three, failed_stage, result, f.accepted,
                f.server_received.received_from_one,
                f.server_received.received_from_three,
                f.clients_received[0].received,
                f.clients_received[1].received,
                f.server_received.wrong_payloads);
    }
    return result;
}

#ifdef TURBORAFT_TEST_DURABLE_NET_SNAPSHOT
#ifdef TURBORAFT_TEST_DURABLE_NET_SNAPSHOT_FAILURE
spec("ACE 2.3 authenticated TLS Snapshot write fault is fail-closed")
{
    it("returns errored Group completion, emits no ACK and reopens old WAL")
    {
        check_equal(run_two_distinct_peers(0), SALTS_OK);
    }
}
#else
spec("ACE 2.3 real mTLS Snapshot to WAL Group Owner and durable reply")
{
    it("installs Node3 Snapshot on exact Group Owner, fences stale ACK, restarts and catches up")
    {
        check_equal(run_two_distinct_peers(0), SALTS_OK);
    }
}
#endif
#else
spec("ACE 2.3 two real distinct TLS-certified Raft Node IDs")
{
    it("simultaneously admits Node1 and Node3 certificates and isolates bidirectional Raft groups")
    {
        check_equal(run_two_distinct_peers(0), SALTS_OK);
    }

    it("rejects rotated-certificate peer forging another Node ID without poisoning Node1")
    {
        check_equal(run_two_distinct_peers(1), SALTS_OK);
    }
}
#endif
