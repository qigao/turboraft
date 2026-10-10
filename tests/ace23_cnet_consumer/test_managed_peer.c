#include <turboraft/raft_cnet_managed_peer.h>

#include <cmeta_error.h>
#include <salts/clock.h>
#include <salts/thread.h>
#include <tinytest.h>

#include <stdio.h>
#include <string.h>

#ifndef TURBORAFT_ACE23_FIXTURE_DIR
#error "TURBORAFT_ACE23_FIXTURE_DIR must name the test-only certificates"
#endif

#define CERT_NODE1 "eb571a92b33237897216c79501066b3e77391047eaeb6be084526c4769657549"
#define CERT_NODE2 "44e8fe3ce37ede1c2a1b5d36efa345cb662887d4250d17a000ebea2e834aed95"

enum { PEER_COUNT = 2, PROGRESS_BUDGET = 6500 };

typedef struct managed_probe {
    size_t received;
    size_t rejected;
    tr_raft_node_id_t from;
    tr_raft_node_id_t to;
    size_t data_received;
    size_t ack_received;
} managed_probe;

typedef struct managed_fixture {
    cnet_client client;
    cnet_client server;
    cnet_manager manager;
    cnet_listener listener;
    cnet_tls_server server_tls;
    cnet_tls_client_config client_tls;
    tr_raft_cnet_managed_peer_t *peers[PEER_COUNT];
    tr_raft_cnet_managed_group_binding_t groups[PEER_COUNT];
    tr_raft_transport_t runtime_transports[PEER_COUNT];
    tr_raft_cnet_channel_t *server_channels[PEER_COUNT];
    tr_raft_cnet_channel_config_t server_config;
    managed_probe client_probes[PEER_COUNT];
    managed_probe server_probes[PEER_COUNT];
    tr_raft_cnet_peer_identity_t client_identity;
    tr_raft_cnet_peer_identity_t server_identity;
    tr_raft_cnet_identity_policy_t client_policy;
    tr_raft_cnet_identity_policy_t server_policy;
    const char *server_fingerprint[1];
    const char *client_fingerprint[1];
    char ca[512], node1_cert[512], node1_key[512];
    char node2_cert[512], node2_key[512], uri[128];
    cnet_managed_connection stale;
    int manager_open, client_open, server_open, listener_open, server_tls_open;
    int accept_count;
} managed_fixture;

static int fixture_path(char *out, size_t size, const char *name)
{
    const int written = snprintf(out, size, "%s/%s",
                                 TURBORAFT_ACE23_FIXTURE_DIR, name);
    return written > 0 && (size_t)written < size ? SALTS_OK : SALTS_ERANGE;
}

static cnet_client_config client_config(void)
{
    return (cnet_client_config){
#if defined(_WIN32)
        .backend = NATIVE_IO_BACKEND_IOCP,
#elif defined(__linux__)
        .backend = NATIVE_IO_BACKEND_EPOLL,
#else
        .backend = NATIVE_IO_BACKEND_KQUEUE,
#endif
        .connection_capacity = 3U,
        .command_capacity = 32U,
        .request_capacity = 16U,
        .completion_batch_capacity = 16U,
        .event_capacity = 32U,
        .max_send_bytes = 4096U,
        .receive_buffer_bytes = 4096U,
        .connect_timeout_ms = 2000U,
        .read_timeout_ms = 2000U,
        .write_timeout_ms = 2000U,
        .tls_io_buffer_bytes = CNET_TLS_MIN_IO_BUFFER_BYTES,
        .tls_handshake_timeout_ms = 2000U
    };
}

static tr_raft_handshake_config_t handshake_config(
    tr_raft_node_id_t local_id)
{
    tr_raft_handshake_config_t config = {0};
    size_t i;

    for (i = 0; i < sizeof(config.cluster_id.bytes); ++i)
        config.cluster_id.bytes[i] = (uint8_t)(i + 17U);
    config.local_node_id = local_id;
    config.process_incarnation.bytes[0] = (uint8_t)local_id;
    config.config_epoch = 1U;
    config.wire_major_min = TR_RAFT_HANDSHAKE_WIRE_MAJOR;
    config.wire_major_max = TR_RAFT_HANDSHAKE_WIRE_MAJOR;
    config.wire_minor_min = TR_RAFT_HANDSHAKE_WIRE_MINOR;
    config.wire_minor_max = TR_RAFT_HANDSHAKE_WIRE_MINOR;
    config.max_frame_size = TR_RAFT_WIRE_MAX_FRAME_SIZE;
    config.max_snapshot_chunk_size = TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES;
    return config;
}

static int capture_payload(void *context,
                           const tr_raft_transport_payload_t *payload)
{
    managed_probe *probe = (managed_probe *)context;
    if (payload != NULL &&
        payload->kind == TR_RAFT_WIRE_PAYLOAD_DATA_CHUNK &&
        payload->group_id == 43U &&
        payload->data.data_chunk.from == probe->from &&
        payload->data.data_chunk.to == probe->to &&
        payload->data.data_chunk.stream_id == 99U &&
        payload->data.data_chunk.data_length == 256U &&
        payload->data.data_chunk.data != NULL &&
        payload->data.data_chunk.done) {
        const uint8_t *data = payload->data.data_chunk.data;
        size_t i;
        for (i = 0U; i < 256U; ++i)
            if (data[i] != 0xa7U) break;
        if (i == 256U) {
            ++probe->data_received;
            return SALTS_OK;
        }
    }
    if (payload != NULL &&
        payload->kind == TR_RAFT_WIRE_PAYLOAD_DATA_ACK &&
        payload->group_id == 43U &&
        payload->data.data_ack.from == probe->from &&
        payload->data.data_ack.to == probe->to &&
        payload->data.data_ack.stream_id == 99U &&
        payload->data.data_ack.accepted &&
        !payload->data.data_ack.durable) {
        ++probe->ack_received;
        return SALTS_OK;
    }
    if (payload == NULL ||
        payload->kind != TR_RAFT_WIRE_PAYLOAD_RAFT ||
        payload->group_id != 42U ||
        payload->data.raft.type != TR_RAFT_MSG_HEARTBEAT_REQUEST ||
        payload->data.raft.from != probe->from ||
        payload->data.raft.to != probe->to) {
        ++probe->rejected;
        return SALTS_EPROTO;
    }
    ++probe->received;
    return SALTS_OK;
}

static int init_fixture(managed_fixture *fixture, int security_reject)
{
    cnet_client_config cfg = client_config();
    cnet_listener_config listener = {
        .backend = cfg.backend, .host = "127.0.0.1", .port = 0U,
        .backlog = 3U
    };
    cnet_manager_config manager = {0};
    cnet_tls_server_config tls = {0};
    uint16_t port = 0U;
    int result;
#define INIT_TRY(expr) do { result = (expr); if (result != SALTS_OK) return result; } while (0)

    INIT_TRY(fixture_path(fixture->ca, sizeof(fixture->ca), "ca.pem"));
    INIT_TRY(fixture_path(fixture->node1_cert, sizeof(fixture->node1_cert),
                          "node1-cert.pem"));
    INIT_TRY(fixture_path(fixture->node1_key, sizeof(fixture->node1_key),
                          "node1-key.pem"));
    INIT_TRY(fixture_path(fixture->node2_cert, sizeof(fixture->node2_cert),
                          "node2-cert.pem"));
    INIT_TRY(fixture_path(fixture->node2_key, sizeof(fixture->node2_key),
                          "node2-key.pem"));
    INIT_TRY(cnet_client_init(&fixture->client, &cfg));
    fixture->client_open = 1;
    INIT_TRY(cnet_client_init(&fixture->server, &cfg));
    fixture->server_open = 1;
    INIT_TRY(cnet_listener_init(&fixture->listener, &listener));
    fixture->listener_open = 1;
    INIT_TRY(cnet_listener_port(&fixture->listener, &port));
    if (snprintf(fixture->uri, sizeof(fixture->uri),
                 "tls://127.0.0.1:%u", (unsigned)port) <= 0)
        return SALTS_ERANGE;

    tls.size = sizeof(tls);
    tls.ca_file = fixture->ca;
    tls.cert_file = fixture->node2_cert;
    tls.key_file = fixture->node2_key;
    tls.client_auth = CNET_TLS_CLIENT_AUTH_REQUIRED;
    INIT_TRY(cnet_tls_server_init(&fixture->server_tls, &tls));
    fixture->server_tls_open = 1;

    manager = (cnet_manager_config){
        sizeof(manager), CNET_MANAGER_VERSION, &fixture->client,
        PEER_COUNT, PEER_COUNT
    };
    INIT_TRY(cnet_manager_init(&fixture->manager, &manager));
    fixture->manager_open = 1;

    fixture->client_tls.size = sizeof(fixture->client_tls);
    fixture->client_tls.ca_file = fixture->ca;
    fixture->client_tls.cert_file = fixture->node1_cert;
    fixture->client_tls.key_file = fixture->node1_key;
    fixture->client_tls.server_name = "node-2.mesh";

    fixture->server_fingerprint[0] =
        security_reject ? CERT_NODE1 : CERT_NODE2;
    fixture->client_fingerprint[0] = CERT_NODE1;
    fixture->client_identity = (tr_raft_cnet_peer_identity_t){
        2U, fixture->server_fingerprint, 1U
    };
    fixture->server_identity = (tr_raft_cnet_peer_identity_t){
        1U, fixture->client_fingerprint, 1U
    };
    fixture->client_policy = (tr_raft_cnet_identity_policy_t){
        1U, &fixture->client_identity, 1U
    };
    fixture->server_policy = (tr_raft_cnet_identity_policy_t){
        2U, &fixture->server_identity, 1U
    };
    fixture->server_config.client = &fixture->server;
    fixture->server_config.identity = &fixture->server_policy;
    fixture->server_config.handshake = handshake_config(2U);
    fixture->server_config.first_outbound_message_id = 1U;
    fixture->server_config.host_module_generation = UINT64_C(90010001);
    fixture->server_config.on_payload = capture_payload;
#undef INIT_TRY
    return SALTS_OK;
}

static int make_managed_peer(managed_fixture *f, size_t index,
                             uint64_t now_ms)
{
    tr_raft_cnet_managed_peer_config_t config = {0};
    tr_raft_handshake_config_t local = handshake_config(1U);
    cnet_reconnect_config recovery = {0};

    if (index >= PEER_COUNT) return SALTS_EINVAL;
    f->client_probes[index] = (managed_probe){.from = 2U, .to = 1U};
    recovery.size = sizeof(recovery);
    recovery.version = CNET_RECOVERY_POLICY_VERSION;
    recovery.max_attempts = 3U;
    recovery.deadline_ms = now_ms + 16000U;
    recovery.initial_backoff_ms = 5U;
    recovery.maximum_backoff_ms = 40U;
    recovery.jitter_seed = 100U + index;
    config.manager = &f->manager;
    config.channel.client = &f->client;
    config.channel.identity = &f->client_policy;
    config.channel.handshake = local;
    config.channel.first_outbound_message_id = 1U;
    config.channel.host_module_generation = UINT64_C(90010001);
    config.channel.on_payload = capture_payload;
    config.channel.payload_context = &f->client_probes[index];
    config.expected_peer_node_id = 2U;
    config.uri = f->uri;
    config.tls = &f->client_tls;
    config.reconnect = recovery;
    config.recovery_episode_ms = 3000U;
    {
        int status = tr_raft_cnet_managed_peer_create(
            &config, &f->peers[index]);
        if (status != SALTS_OK) return status;
    }

    f->groups[index].peer = f->peers[index];
    f->groups[index].group_id = 42U;
    f->runtime_transports[index].snapshot_context = f;
    {
        int status = tr_raft_cnet_managed_group_transport_bind(
            &f->groups[index], &f->runtime_transports[index]);
        if (status != SALTS_OK) return status;
    }
    if (f->runtime_transports[index].snapshot_context != f ||
        f->runtime_transports[index].context != &f->groups[index] ||
        tr_raft_cnet_managed_group_transport_bind(
            &f->groups[index],
            &f->runtime_transports[index]) != SALTS_EALREADY)
        return SALTS_EPROTO;
    return SALTS_OK;
}

static int accept_server_peer(managed_fixture *f, size_t index)
{
    tr_raft_cnet_channel_config_t config = f->server_config;
    cnet_observer observer;
    cnet_connection connection = {0};
    int result;

    if (index >= PEER_COUNT || f->server_channels[index] != NULL)
        return SALTS_EALREADY;
    f->server_probes[index] = (managed_probe){.from = 1U, .to = 2U};
    config.payload_context = &f->server_probes[index];
    result = tr_raft_cnet_channel_create(
        &config, &f->server_channels[index]);
    if (result != SALTS_OK) return result;
    observer = tr_raft_cnet_channel_observer(f->server_channels[index]);
    result = cnet_listener_accept_tls(
        &f->listener, &f->server, &f->server_tls, &observer, &connection);
    if (result != SALTS_OK) return result;
    return tr_raft_cnet_channel_attach(
        f->server_channels[index], connection);
}

static int managed_progress(managed_fixture *f,
                            uint64_t now_ms, int start_first)
{
    size_t events = 0U;
    size_t work = 0U;
    size_t i;
    int result;

    result = cnet_client_poll(&f->client, 1U, &events);
    if (result != SALTS_OK) return result;

    {
        int ready = 0;
        result = cnet_listener_wait(&f->listener, 0U, &ready);
        if (result != SALTS_OK) return result;
        if (ready) {
            /* All 2 connections on the same bounded manager share one
             * target certificate/node in this capacity qualification.
             * The real Node directory must reject duplicate logical IDs. */
            for (i = 0U; i < PEER_COUNT; ++i) {
                if (f->server_channels[i] == NULL) {
                    result = accept_server_peer(f, i);
                    if (result != SALTS_OK) return result;
                    ++f->accept_count;
                    break;
                }
            }
        }
    }
    result = cnet_client_poll(&f->server, 1U, &events);
    if (result != SALTS_OK) return result;
    result = cnet_manager_advance(&f->manager, PEER_COUNT, &work);
    if (result != SALTS_OK) return result;

    for (i = 0U; i < PEER_COUNT; ++i) {
        uint64_t wait_ms = 0U;
        if (f->peers[i] == NULL || (!start_first && i == 0U))
            continue;
        result = tr_raft_cnet_managed_peer_advance(
            f->peers[i], now_ms, &wait_ms);
        if (result == SALTS_ESHUTDOWN) {
            tr_raft_cnet_managed_peer_status_t status = {0};
            int inspected = tr_raft_cnet_managed_peer_get_status(
                f->peers[i], &status);
            if (inspected != SALTS_OK) return inspected;
            if (status.dial.recovery.sealed) continue;
        }
        if (result != SALTS_OK && result != SALTS_EBUSY &&
            result != SALTS_ENOBUFS) return result;
    }
    return SALTS_OK;
}

/* Cleanup is part of acceptance: no test silently ignores stale managed
 * contexts, unfinished native connection callbacks or leaked owner credits. */
static int cleanup_fixture(managed_fixture *f)
{
    size_t i, work;
    int first_error = SALTS_OK;
    int status;
#define CLEAN_STEP(expr) do { \
    status = (expr); \
    if (first_error == SALTS_OK && status != SALTS_OK) \
        first_error = status; \
} while (0)
    if (f->manager_open)
        CLEAN_STEP(cnet_manager_request_close(&f->manager));
    for (i = 0U; i < PEER_COUNT; ++i) {
        if (f->peers[i] != NULL)
            CLEAN_STEP(tr_raft_cnet_managed_peer_stop(f->peers[i]));
    }

    /* The final CNet owner settles terminal callbacks. stop() may report a
     * genuine handshake/progress error in the negative identity fixture;
     * final destroy and Manager drained status remain mandatory. */
    if (f->client_open)
        (void)cnet_client_stop(&f->client, 2000U);
    if (f->server_open)
        (void)cnet_client_stop(&f->server, 2000U);

    if (f->manager_open) {
        cnet_manager_snapshot snapshot = {0};
        for (i = 0U; i < PEER_COUNT * 8U; ++i) {
            CLEAN_STEP(cnet_manager_get_snapshot(&f->manager, &snapshot));
            if (first_error != SALTS_OK || snapshot.drained)
                break;
            CLEAN_STEP(cnet_manager_advance(
                &f->manager, PEER_COUNT, &work));
            if (first_error != SALTS_OK)
                break;
        }
        if (first_error == SALTS_OK && !snapshot.drained)
            first_error = SALTS_EBUSY;
    }
    for (i = 0U; i < PEER_COUNT; ++i) {
        if (f->peers[i] != NULL)
            CLEAN_STEP(tr_raft_cnet_managed_peer_destroy(f->peers[i]));
        if (f->server_channels[i] != NULL)
            CLEAN_STEP(tr_raft_cnet_channel_destroy(f->server_channels[i]));
    }

    if (f->manager_open)
        CLEAN_STEP(cnet_manager_destroy(&f->manager));
    if (f->client_open)
        CLEAN_STEP(cnet_client_destroy(&f->client));
    if (f->server_open)
        CLEAN_STEP(cnet_client_destroy(&f->server));
    if (f->listener_open) {
        CLEAN_STEP(cnet_listener_close(&f->listener));
        CLEAN_STEP(cnet_listener_destroy(&f->listener));
    }
    if (f->server_tls_open)
        CLEAN_STEP(cnet_tls_server_destroy(&f->server_tls));
#undef CLEAN_STEP
    return first_error;
}

static int send_heartbeat(managed_fixture *f, size_t index)
{
    tr_raft_message_t message = {0};
    tr_raft_transport_payload_t payload = {0};

    if (f == NULL || index >= PEER_COUNT) return SALTS_EINVAL;
    message.from = 1U;
    message.to = 2U;
    message.term = 3U;
    message.type = TR_RAFT_MSG_HEARTBEAT_REQUEST;
    if (index == 0U) {
        /* Raft Service uses this SAME stable callback before and after
         * connection N -> N+1, without rebinding its borrowed self. */
        return f->runtime_transports[index].enqueue(
            f->runtime_transports[index].context, &message);
    }
    payload.group_id = 42U;
    payload.kind = TR_RAFT_WIRE_PAYLOAD_RAFT;
    payload.data.raft = message;
    return tr_raft_cnet_managed_peer_send(f->peers[index], &payload);
}

static void ignore_third_state(void *user, cnet_connection connection,
                               cnet_connection_state state,
                               const cnet_error *error)
{
    (void)user;
    (void)connection;
    (void)state;
    (void)error;
}

/* A value-only synthetic *partial* ACK tests transport fencing; real
 * receiver fsync/durability is qualified separately in storage CTests.
 * This fixture never fabricates a durable=true receipt. */
static tr_raft_multicore_completion_t partial_ack(
    const tr_raft_transport_reply_origin_t *origin)
{
    tr_raft_multicore_completion_t completion = {0};
    completion.request_id = UINT64_C(88);
    completion.operation = TR_RAFT_MULTICORE_RECEIVE_CHUNK;
    completion.result = SALTS_OK;
    completion.reply_origin = *origin;
    completion.value.chunk.kind = TR_RAFT_WIRE_PAYLOAD_DATA_CHUNK;
    completion.value.chunk.ack_valid = true;
    completion.value.chunk.durable_or_installed = false;
    completion.value.chunk.ack.data.from = 1U;
    completion.value.chunk.ack.data.to = 2U;
    completion.value.chunk.ack.data.term = 3U;
    completion.value.chunk.ack.data.stream_id = 99U;
    completion.value.chunk.ack.data.stream_size = 256U;
    completion.value.chunk.ack.data.next_offset = 128U;
    completion.value.chunk.ack.data.accepted = true;
    return completion;
}

typedef struct foreign_ack_attempt {
    tr_raft_cnet_managed_peer_t *peer;
    tr_raft_multicore_completion_t completion;
    tr_raft_transport_reply_origin_t captured;
    int capture_result;
    int send_result;
} foreign_ack_attempt;

static void foreign_owner_attempt(void *context)
{
    foreign_ack_attempt *attempt = (foreign_ack_attempt *)context;
    attempt->capture_result =
        tr_raft_cnet_managed_peer_capture_reply_origin(
            attempt->peer, 43U, &attempt->captured);
    attempt->send_result =
        tr_raft_cnet_managed_peer_send_chunk_completion(
            attempt->peer, &attempt->completion);
}

static int test_bounded_multi_link(void)
{
    managed_fixture f = {0};
    tr_raft_cnet_managed_peer_status_t a = {0}, b = {0};
    cnet_manager_snapshot slots = {0};
    tr_raft_transport_reply_origin_t old_origin = {0}, fresh_origin = {0};
    tr_raft_multicore_completion_t late_ack = {0};
    const uint64_t began = cmeta_monotonic_ms();
    unsigned i;
    int result;
    const char *failed = "none";

#define CHECK_TRY(expr) do { \
    result = (expr); \
    if (result != SALTS_OK) { failed = #expr; goto done; } \
} while (0)

    CHECK_TRY(init_fixture(&f, 0));
    CHECK_TRY(make_managed_peer(&f, 0U, began));
    CHECK_TRY(make_managed_peer(&f, 1U, began));

    if (send_heartbeat(&f, 0U) != SALTS_ENOSPC) {
        result = SALTS_EPROTO;
        failed = "pre-handshake rejects RAFT";
        goto done;
    }

    CHECK_TRY(tr_raft_cnet_managed_peer_advance(f.peers[0], began, &(uint64_t){0}));
    CHECK_TRY(tr_raft_cnet_managed_peer_advance(f.peers[1], began, &(uint64_t){0}));
    CHECK_TRY(cnet_manager_get_snapshot(&f.manager, &slots));
    if (slots.bound != PEER_COUNT ||
        slots.connection_capacity != PEER_COUNT) {
        result = SALTS_EPROTO;
        failed = "two physical Manager credits";
        goto done;
    }
    /* The shared Manager must reject a third physical connection without
     * fabricating a callback, connection or new record. */
    {
        cnet_manager_attachment unexpected = {0};
        cnet_managed_connection third = {0};
        int rejected;
        unexpected.observer.on_state = ignore_third_state;
        rejected = cnet_manager_reserve(&f.manager, &unexpected, &third);
        if (rejected == SALTS_OK)
            (void)cnet_manager_cancel(&f.manager, third);
        if (rejected != SALTS_ENOBUFS || third.slot != 0U) {
            result = SALTS_EPROTO;
            failed = "shared Manager third-credit rejection";
            goto done;
        }
    }

    CHECK_TRY(tr_raft_cnet_managed_peer_get_status(f.peers[0], &a));
    f.stale = a.dial.managed;

    for (i = 0U; i < PROGRESS_BUDGET; ++i) {
        CHECK_TRY(managed_progress(&f, cmeta_monotonic_ms(), 1));
        CHECK_TRY(tr_raft_cnet_managed_peer_get_status(f.peers[0], &a));
        CHECK_TRY(tr_raft_cnet_managed_peer_get_status(f.peers[1], &b));
        if (a.dial.recovery.protocol_ready &&
            b.dial.recovery.protocol_ready)
            break;
    }

    if (!a.dial.recovery.protocol_ready || !b.dial.recovery.protocol_ready ||
        a.protocol_ready_count != 1U || b.protocol_ready_count != 1U ||
        f.accept_count != 2) {
        result = SALTS_EPROTO;
        failed = "both connections must complete protocol readiness";
        goto done;
    }

    CHECK_TRY(tr_raft_cnet_managed_peer_capture_reply_origin(
        f.peers[0], 43U, &old_origin));
    if (old_origin.host_module_generation != UINT64_C(90010001) ||
        old_origin.channel_instance == 0U ||
        old_origin.authenticated_peer_node_id != 2U ||
        old_origin.group_id != 43U ||
        old_origin.connection_token == 0U) {
        result = SALTS_EPROTO;
        failed = "authenticated old Channel origin is not a unique value";
        goto done;
    }
    late_ack = partial_ack(&old_origin);
    {
        foreign_ack_attempt foreign = {0};
        cmeta_thread_t other = {0};
        foreign.peer = f.peers[0];
        foreign.completion = late_ack;
        CHECK_TRY(cmeta_thread_create(&other, foreign_owner_attempt, &foreign));
        CHECK_TRY(cmeta_thread_join(&other));
        if (foreign.capture_result != SALTS_EPERM ||
            foreign.send_result != SALTS_EPERM ||
            foreign.captured.channel_instance != 0U) {
            result = SALTS_EPROTO;
            failed = "foreign thread borrowed CNet owner ACK state";
            goto done;
        }
    }
    CHECK_TRY(send_heartbeat(&f, 0U));
    CHECK_TRY(send_heartbeat(&f, 1U));
    for (i = 0U; i < PROGRESS_BUDGET; ++i) {
        CHECK_TRY(managed_progress(&f, cmeta_monotonic_ms(), 1));
        if (f.server_probes[0].received == 1U &&
            f.server_probes[1].received == 1U)
            break;
    }
    if (f.server_probes[0].received != 1U ||
        f.server_probes[1].received != 1U ||
        f.server_probes[0].rejected || f.server_probes[1].rejected) {
        result = SALTS_EPROTO;
        failed = "two separate authorized link payloads";
        goto done;
    }

    /* ManagedDial must forward a real CNet send terminal through Manager
     * to this exact Channel generation. A mere received frame cannot prove
     * on_send was forwarded, so continue Owner progress until both settle. */
    for (i = 0U; i < PROGRESS_BUDGET; ++i) {
        CHECK_TRY(managed_progress(&f, cmeta_monotonic_ms(), 1));
        CHECK_TRY(tr_raft_cnet_managed_peer_get_status(f.peers[0], &a));
        CHECK_TRY(tr_raft_cnet_managed_peer_get_status(f.peers[1], &b));
        if (a.channel.payloads_completed == 1U &&
            b.channel.payloads_completed == 1U)
            break;
    }
    if (i == PROGRESS_BUDGET) {
        result = SALTS_ETIMEDOUT;
        failed = "ManagedDial did not forward logical send completion";
        goto done;
    }
    CHECK_TRY(tr_raft_cnet_managed_peer_get_status(f.peers[0], &a));
    CHECK_TRY(tr_raft_cnet_managed_peer_get_status(f.peers[1], &b));
    if (a.channel.payloads_admitted != 1U ||
        b.channel.payloads_admitted != 1U ||
        a.channel.payloads_admitted !=
            a.channel.payloads_completed + a.channel.payloads_canceled +
            a.channel.payload_writes_pending ||
        b.channel.payloads_admitted !=
            b.channel.payloads_completed + b.channel.payloads_canceled +
            b.channel.payload_writes_pending) {
        result = SALTS_EPROTO;
        failed = "managed send callback must conserve per-generation credits";
        goto done;
    }

    /* Submit one SG DATA chunk to the *old* verified Channel and stop the
     * server without another CNet progress turn. The borrowed stack bytes
     * become a bounded Salts Core owned slice; this old-generation
     * admission must never be automatically replayed after reconnect. */
    {
        tr_raft_transport_payload_t sg = {0};
        uint8_t bytes[256];
        memset(bytes, 0xa7, sizeof(bytes));
        sg.group_id = 43U;
        sg.kind = TR_RAFT_WIRE_PAYLOAD_DATA_CHUNK;
        sg.data.data_chunk.from = 1U;
        sg.data.data_chunk.to = 2U;
        sg.data.data_chunk.term = 3U;
        sg.data.data_chunk.stream_id = 99U;
        sg.data.data_chunk.stream_size = sizeof(bytes);
        sg.data.data_chunk.data_length = sizeof(bytes);
        sg.data.data_chunk.data = bytes;
        sg.data.data_chunk.done = true;
        CHECK_TRY(tr_raft_cnet_managed_peer_send(f.peers[0], &sg));
        memset(bytes, 0xff, sizeof(bytes));
    }
    CHECK_TRY(tr_raft_cnet_managed_peer_get_status(f.peers[0], &a));
    if (a.channel.sg_chunks_admitted != 1U ||
        a.channel.payloads_admitted != 2U ||
        a.channel.payloads_completed != 1U ||
        a.channel.payloads_canceled != 0U ||
        a.channel.payload_writes_pending != 1U) {
        result = SALTS_EPROTO;
        failed = "old TLS generation must own one outstanding SG write";
        goto done;
    }

    /* Transport loss never replays an accepted Raft or SG message.
     * The application alone decides whether an operation is unsettled. */
    CHECK_TRY(tr_raft_cnet_channel_stop(f.server_channels[0]));
    for (i = 0U; i < PROGRESS_BUDGET; ++i) {
        tr_raft_cnet_channel_status_t closed = {0};
        CHECK_TRY(managed_progress(&f, cmeta_monotonic_ms(), 0));
        CHECK_TRY(tr_raft_cnet_channel_get_status(
            f.server_channels[0], &closed));
        if (closed.terminal) break;
    }
    if (i == PROGRESS_BUDGET) {
        result = SALTS_ETIMEDOUT;
        failed = "terminal old server connection";
        goto done;
    }
    /* Do not allow ManagedDial peer0 to create generation N+1 until
     * generation N is terminal and has completely settled its SG write.
     * CNet may finish it during shutdown or cancel it, but not both. */
    for (i = 0U; i < PROGRESS_BUDGET; ++i) {
        CHECK_TRY(managed_progress(&f, cmeta_monotonic_ms(), 0));
        CHECK_TRY(tr_raft_cnet_managed_peer_get_status(f.peers[0], &a));
        if (a.channel.terminal) break;
    }
    if (i == PROGRESS_BUDGET ||
        a.channel.payloads_admitted != 2U ||
        a.channel.sg_chunks_admitted != 1U ||
        a.channel.payload_writes_pending != 0U ||
        a.channel.payloads_completed + a.channel.payloads_canceled != 2U) {
        result = SALTS_EPROTO;
        failed = "old SG generation must settle before Manager recycle";
        goto done;
    }
    CHECK_TRY(tr_raft_cnet_channel_destroy(f.server_channels[0]));
    f.server_channels[0] = NULL;

    for (i = 0U; i < PROGRESS_BUDGET; ++i) {
        CHECK_TRY(managed_progress(&f, cmeta_monotonic_ms(), 1));
        CHECK_TRY(tr_raft_cnet_managed_peer_get_status(f.peers[0], &a));
        CHECK_TRY(tr_raft_cnet_managed_peer_get_status(f.peers[1], &b));
        if (a.protocol_ready_count >= 2U &&
            a.dial.recovery.protocol_ready &&
            b.dial.recovery.protocol_ready)
            break;
    }
    if (a.protocol_ready_count < 2U || a.connections_started < 2U ||
        b.protocol_ready_count != 1U ||
        f.accept_count != 3 ||
        f.server_probes[0].received != 0U ||
        f.server_probes[0].data_received != 0U ||
        a.channel.payloads_admitted != 0U ||
        a.channel.sg_chunks_admitted != 0U ||
        a.channel.payloads_completed != 0U ||
        a.channel.payloads_canceled != 0U ||
        a.channel.payload_writes_pending != 0U) {
        result = SALTS_EPROTO;
        failed = "manual reconnect must not replay payload";
        goto done;
    }

    if (a.dial.managed.generation == f.stale.generation &&
        a.dial.managed.incarnation == f.stale.incarnation) {
        result = SALTS_EPROTO;
        failed = "reconnected generation must be distinct";
        goto done;
    }
    {
        cnet_manager_entry stale = {0};
        if (cnet_manager_lookup(&f.manager, f.stale, &stale) != SALTS_ENOENT) {
            result = SALTS_EPROTO;
            failed = "old Manager generation must be stale";
            goto done;
        }
    }

    CHECK_TRY(tr_raft_cnet_managed_peer_capture_reply_origin(
        f.peers[0], 43U, &fresh_origin));
    if (fresh_origin.host_module_generation != UINT64_C(90010001) ||
        fresh_origin.channel_instance == 0U ||
        fresh_origin.channel_instance == old_origin.channel_instance ||
        fresh_origin.authenticated_peer_node_id !=
            old_origin.authenticated_peer_node_id) {
        result = SALTS_EPROTO;
        failed = "reconnected Channel did not fence prior generation";
        goto done;
    }
    /* A finished Group operation from N must NOT be routed via ManagedDial
     * or another physical peer after N+1 becomes READY. Denial must consume
     * no new outbound send credit and must not contact the remote peer. */
    if (tr_raft_cnet_managed_peer_send_chunk_completion(
            f.peers[0], &late_ack) != SALTS_ECANCELED ||
        tr_raft_cnet_managed_peer_send_chunk_completion(
            f.peers[1], &late_ack) != SALTS_ECANCELED) {
        result = SALTS_EPROTO;
        failed = "stale or cross-peer ACK was admitted";
        goto done;
    }
    /* A ticketless or malformed receipt must also fail before CNet send.
     * In particular a caller cannot infer that an Owner completion means
     * durable ACK or change the authenticated receiving Node ID. */
    {
        tr_raft_multicore_completion_t invalid = late_ack;
        invalid.reply_origin = (tr_raft_transport_reply_origin_t){0};
        if (tr_raft_cnet_managed_peer_send_chunk_completion(
                f.peers[0], &invalid) != SALTS_ECANCELED) {
            result = SALTS_EPROTO;
            failed = "ticketless completion escaped generation fence";
            goto done;
        }
        /* Simulate a module reload ABA: even if the DSO-local serial and
         * underlying CNet token happened to repeat, an older host epoch
         * must reject the reply before touching a single send credit. */
        invalid = partial_ack(&fresh_origin);
        invalid.reply_origin.host_module_generation =
            fresh_origin.host_module_generation - 1U;
        if (tr_raft_cnet_managed_peer_send_chunk_completion(
                f.peers[0], &invalid) != SALTS_ECANCELED) {
            result = SALTS_EPROTO;
            failed = "DSO reload host generation ABA escaped ACK fence";
            goto done;
        }
        invalid = partial_ack(&fresh_origin);
        invalid.value.chunk.ack.data.to = 3U;
        if (tr_raft_cnet_managed_peer_send_chunk_completion(
                f.peers[0], &invalid) != SALTS_EPROTO) {
            result = SALTS_EPROTO;
            failed = "forged ACK destination escaped exact peer gate";
            goto done;
        }
        invalid = partial_ack(&fresh_origin);
        invalid.value.chunk.durable_or_installed = true;
        if (tr_raft_cnet_managed_peer_send_chunk_completion(
                f.peers[0], &invalid) != SALTS_EPROTO) {
            result = SALTS_EPROTO;
            failed = "unverified ACK durability escaped CNet fence";
            goto done;
        }
    }
    CHECK_TRY(tr_raft_cnet_managed_peer_get_status(f.peers[0], &a));
    if (a.channel.payloads_admitted != 0U ||
        a.channel.payload_writes_pending != 0U ||
        f.server_probes[0].ack_received != 0U) {
        result = SALTS_EPROTO;
        failed = "stale ACK changed fresh-generation send accounting";
        goto done;
    }
    {
        const tr_raft_multicore_completion_t current =
            partial_ack(&fresh_origin);
        CHECK_TRY(tr_raft_cnet_managed_peer_send_chunk_completion(
            f.peers[0], &current));
    }
    for (i = 0U; i < PROGRESS_BUDGET; ++i) {
        CHECK_TRY(managed_progress(&f, cmeta_monotonic_ms(), 1));
        if (f.server_probes[0].ack_received == 1U) break;
    }
    if (i == PROGRESS_BUDGET ||
        f.server_probes[0].ack_received != 1U ||
        f.server_probes[1].ack_received != 0U) {
        result = SALTS_EPROTO;
        failed = "only current verified Channel may deliver partial ACK";
        goto done;
    }

    CHECK_TRY(send_heartbeat(&f, 0U));
    for (i = 0U; i < PROGRESS_BUDGET; ++i) {
        CHECK_TRY(managed_progress(&f, cmeta_monotonic_ms(), 1));
        if (f.server_probes[0].received == 1U) break;
    }
    if (f.server_probes[0].received != 1U) {
        result = SALTS_EPROTO;
        failed = "explicit send after authorized reconnect";
    }
    if (result == SALTS_OK) {
        /* Recycled Manager connection records must never transplant the old
         * generation's send settlement into this freshly authorized link. */
        for (i = 0U; i < PROGRESS_BUDGET; ++i) {
            CHECK_TRY(managed_progress(&f, cmeta_monotonic_ms(), 1));
            CHECK_TRY(tr_raft_cnet_managed_peer_get_status(f.peers[0], &a));
            if (a.channel.payloads_completed == 2U) break;
        }
        if (i == PROGRESS_BUDGET) {
            result = SALTS_ETIMEDOUT;
            failed = "reconnected generation lacks CNet on_send terminal";
            goto done;
        }
        CHECK_TRY(tr_raft_cnet_managed_peer_get_status(f.peers[0], &a));
        if (a.channel.payloads_admitted != 2U ||
            a.channel.payloads_admitted !=
                a.channel.payloads_completed + a.channel.payloads_canceled +
                a.channel.payload_writes_pending) {
            result = SALTS_EPROTO;
            failed = "reconnected TLS generation must start with a fresh ledger";
        }
    }
done:
    if (result != SALTS_OK)
        fprintf(stderr, "managed peers: %s result=%d accepting=%d aReady=%zu bReady=%zu aConn=%zu\n",
                failed, result, f.accept_count, a.protocol_ready_count,
                b.protocol_ready_count, a.connections_started);
    {
        const int cleanup = cleanup_fixture(&f);
        if (result == SALTS_OK && cleanup != SALTS_OK) {
            fprintf(stderr, "managed peer cleanup failed: %d\n", cleanup);
            result = cleanup;
        }
    }
#undef CHECK_TRY
    return result;
}

static int test_security_seals_reconnect(void)
{
    managed_fixture f = {0};
    tr_raft_cnet_managed_peer_status_t status = {0};
    const uint64_t began = cmeta_monotonic_ms();
    unsigned i;
    int result;
    const char *failed = "none";
#define SEC_TRY(expr) do { result=(expr); if(result != SALTS_OK) { failed=#expr; goto done; } } while(0)
    SEC_TRY(init_fixture(&f, 1));
    SEC_TRY(make_managed_peer(&f, 0U, began));
    SEC_TRY(tr_raft_cnet_managed_peer_advance(f.peers[0], began, &(uint64_t){0}));
    for (i = 0U; i < PROGRESS_BUDGET; ++i) {
        SEC_TRY(managed_progress(&f, cmeta_monotonic_ms(), 1));
        SEC_TRY(tr_raft_cnet_managed_peer_get_status(f.peers[0], &status));
        if (status.dial.recovery.sealed) break;
    }
    if (!status.dial.recovery.sealed ||
        status.protocol_ready_count != 0U ||
        status.connections_started != 1U ||
        status.security_rejections == 0U) {
        failed = "invalid TLS identity must seal reconnect without READY";
        result = SALTS_EPROTO;
        goto done;
    }
    for (i = 0U; i < 8U; ++i) {
        uint64_t wait_ms = 0U;
        result = tr_raft_cnet_managed_peer_advance(
            f.peers[0], cmeta_monotonic_ms(), &wait_ms);
        if (result != SALTS_ESHUTDOWN &&
            result != SALTS_EBUSY) {
            failed = "sealed dial cannot establish a second connection";
            result = SALTS_EPROTO;
            goto done;
        }
    }
    result = SALTS_OK;
done:
    if (result != SALTS_OK)
        fprintf(stderr, "security peer: %s result=%d attempts=%zu seals=%zu\n",
                failed, result, status.connections_started,
                status.security_rejections);
    {
        const int cleanup = cleanup_fixture(&f);
        if (result == SALTS_OK && cleanup != SALTS_OK) {
            fprintf(stderr, "security peer cleanup failed: %d\n", cleanup);
            result = cleanup;
        }
    }
#undef SEC_TRY
    return result;
}

spec("ACE 2.3 CNetManager owner-local Raft managed dial")
{
    it("bounds two real TLS connections, keeps peer isolation and only caller-drives reconnect")
    {
        check_equal(test_bounded_multi_link(), SALTS_OK);
    }

    it("seals bad TLS fingerprint without downgrade or implicit reconnect")
    {
        check_equal(test_security_seals_reconnect(), SALTS_OK);
    }
}
