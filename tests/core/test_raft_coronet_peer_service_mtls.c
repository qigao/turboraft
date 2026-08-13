#include <turboraft/raft_coronet_peer_service.h>
#include <turboraft/raft_snapshot_receiver.h>
#include <turboraft/raft_snapshot_sender.h>

#include <CoroNet.h>
#include <tinytest.h>
#include <turbo_error.h>

#include <stdint.h>
#include <string.h>

static const tr_raft_conf_t peer_service_snapshot_configuration = {
    TR_RAFT_CONF_FINAL, 0U, 2U,
    {
        {1U, TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER},
        {2U, TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER}
    }
};

#include "raft_coronet_mtls_test_support.h"

#define PEER_SERVICE_MTLS_TEST_TIMEOUT_MS 10000U
#define PEER_SERVICE_MTLS_SOCKET_TIMEOUT_MS 5000U
#define PEER_SERVICE_MTLS_QUEUE_CAPACITY 8U
#define PEER_SERVICE_MTLS_FIXTURE(name) \
    TURBORAFT_TEST_FIXTURE_DIR "/" name

static const char peer_service_node1_fingerprint[] =
    "sha256:cb334522bda1caf62ec1e374f43b0df0b9d7aee5c91d064ff81ac82f005f2aff";
static const char peer_service_node2_fingerprint[] =
    "sha256:483ac612f03ae69445ea33f6b8ef27342ce9fad891416413e91f57c3b3693733";

enum {
    PEER_SERVICE_MTLS_BATCH_SIZE = 3U,
    PEER_SERVICE_MTLS_MESSAGE_COUNT = 6U
};

typedef struct peer_service_mtls_state {
    coro_context_t *context;
    tr_raft_coronet_peer_service_t *client_service;
    tr_raft_coronet_peer_service_t *server_service;
    tr_raft_coronet_peer_service_step_result_t client_step;
    unsigned short port;
    int client_result;
    int server_result;
    tr_raft_node_id_t server_peer_node_id;
    uint64_t received_terms[PEER_SERVICE_MTLS_MESSAGE_COUNT];
    size_t received_count;
    tr_raft_snapshot_receiver_t *snapshot_receiver;
    tr_raft_snapshot_ack_t snapshot_ack;
    size_t snapshot_ack_count;
    size_t snapshot_install_count;
    uint8_t installed_snapshot[16];
    size_t installed_snapshot_size;
    int snapshot_sender_result;
    bool snapshot_sender_complete;
} peer_service_mtls_state_t;

static int peer_service_mtls_install_snapshot(
    void *context,
    tr_raft_term_t leader_term,
    tr_raft_index_t snapshot_index,
    tr_raft_term_t snapshot_term,
    const tr_raft_conf_t *configuration,
    const uint8_t *data,
    size_t size)
{
    peer_service_mtls_state_t *state =
        (peer_service_mtls_state_t *) context;

    if (state == NULL || configuration == NULL || leader_term != 7U ||
        snapshot_index != 9U ||
        snapshot_term != 6U || size > sizeof(state->installed_snapshot)) {
        return TURBO_EPROTO;
    }
    if (size != 0U) {
        memcpy(state->installed_snapshot, data, size);
    }
    state->installed_snapshot_size = size;
    ++state->snapshot_install_count;
    return TURBO_OK;
}

static int peer_service_mtls_collect_snapshot_ack(
    void *context,
    const tr_raft_coronet_payload_t *payload)
{
    peer_service_mtls_state_t *state =
        (peer_service_mtls_state_t *) context;
    tr_raft_snapshot_sender_config_t config;
    tr_raft_snapshot_sender_t *sender = NULL;
    tr_raft_snapshot_sender_status_t status = {0};
    tr_raft_snapshot_chunk_t chunk;
    int result;

    if (state == NULL || payload == NULL ||
        payload->kind != TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_ACK) {
        return TURBO_EPROTO;
    }
    state->snapshot_ack = payload->data.snapshot_ack;
    ++state->snapshot_ack_count;

    memset(&config, 0, sizeof(config));
    config.self_id = state->snapshot_ack.to;
    config.peer_id = state->snapshot_ack.from;
    config.max_snapshot_bytes = sizeof(state->installed_snapshot);
    result = tr_raft_snapshot_sender_create(&config, &sender);
    if (result == TURBO_OK) {
        result = tr_raft_snapshot_sender_begin(
            sender, state->snapshot_ack.term,
            state->snapshot_ack.snapshot_index, 6U,
            &peer_service_snapshot_configuration,
            state->installed_snapshot, state->installed_snapshot_size);
    }
    if (result == TURBO_OK) {
        result = tr_raft_snapshot_sender_next_chunk(sender, &chunk);
    }
    if (result == TURBO_OK) {
        result = tr_raft_snapshot_sender_acknowledge(
            sender, &state->snapshot_ack);
    }
    if (result == TURBO_OK) {
        result = tr_raft_snapshot_sender_get_status(sender, &status);
    }
    if (result == TURBO_OK && !status.complete) {
        result = TURBO_EPROTO;
    }
    state->snapshot_sender_result = result;
    state->snapshot_sender_complete = result == TURBO_OK;
    tr_raft_snapshot_sender_destroy(sender);
    return result;
}

static tr_raft_handshake_config_t peer_service_mtls_handshake(
    uint8_t incarnation_seed,
    tr_raft_node_id_t local_node_id)
{
    tr_raft_handshake_config_t config;
    size_t index;

    memset(&config, 0, sizeof(config));
    for (index = 0U; index < sizeof(config.cluster_id.bytes); ++index) {
        config.cluster_id.bytes[index] = (uint8_t) (40U + index);
        config.process_incarnation.bytes[index] =
            (uint8_t) (incarnation_seed + index);
    }
    config.local_node_id = local_node_id;
    config.config_epoch = 1U;
    config.feature_bits = TR_RAFT_HANDSHAKE_FEATURE_CURRENT;
    config.wire_major_min = TR_RAFT_HANDSHAKE_WIRE_MAJOR;
    config.wire_major_max = TR_RAFT_HANDSHAKE_WIRE_MAJOR;
    config.wire_minor_min = TR_RAFT_HANDSHAKE_WIRE_MINOR;
    config.wire_minor_max = TR_RAFT_HANDSHAKE_WIRE_MINOR;
    config.max_frame_size = TR_RAFT_HANDSHAKE_MIN_FRAME_SIZE;
    config.max_snapshot_chunk_size =
        TR_RAFT_HANDSHAKE_MIN_SNAPSHOT_CHUNK_SIZE;
    return config;
}

static int peer_service_mtls_ignore_message(
    void *context,
    const tr_raft_message_t *message)
{
    peer_service_mtls_state_t *state =
        (peer_service_mtls_state_t *) context;

    if (state == NULL || message == NULL) {
        return TURBO_EINVAL;
    }
    if (state->received_count >=
        sizeof(state->received_terms) / sizeof(state->received_terms[0])) {
        return TURBO_ENOSPC;
    }
    state->received_terms[state->received_count] = message->term;
    ++state->received_count;
    return TURBO_OK;
}

static void peer_service_mtls_collect_inbound(
    void *context,
    int result,
    tr_raft_node_id_t peer_node_id)
{
    peer_service_mtls_state_t *state =
        (peer_service_mtls_state_t *) context;

    state->server_result = result;
    state->server_peer_node_id = peer_node_id;
}

static int peer_service_mtls_resolve_endpoint(
    void *context,
    tr_raft_node_id_t peer_node_id,
    tr_raft_coronet_endpoint_t *out_endpoint)
{
    static const char connect_host[] = "127.0.0.1";
    static const char request_host[] = "node-2.mesh";
    peer_service_mtls_state_t *state =
        (peer_service_mtls_state_t *) context;

    if (state == NULL || out_endpoint == NULL || peer_node_id != 2U) {
        return TURBO_EINVAL;
    }
    memcpy(out_endpoint->connect_host, connect_host, sizeof(connect_host));
    memcpy(out_endpoint->request_host, request_host, sizeof(request_host));
    out_endpoint->port = state->port;
    return TURBO_OK;
}

static int peer_service_mtls_retry_io(void *context, int error_code)
{
    (void) context;
    return error_code == TURBO_EIO;
}

static void peer_service_mtls_connect(coro_t *coroutine, void *context)
{
    peer_service_mtls_state_t *state =
        (peer_service_mtls_state_t *) context;

    (void) coroutine;
    state->client_result = tr_raft_coronet_peer_service_step(
        state->client_service, turbo_monotonic_ms(), &state->client_step);
}

static int peer_service_mtls_complete(
    const peer_service_mtls_state_t *state)
{
    return state->client_result != TURBO_EBUSY &&
           state->server_result != TURBO_EBUSY;
}

spec("raft CoroNet peer service mTLS integration")
{
    it("orchestrates a verified two-node mTLS session")
    {
        const tr_raft_node_id_t client_peers[] = {2U};
        const tr_raft_node_id_t server_peers[] = {1U};
        tr_raft_handshake_config_t client_handshake =
            peer_service_mtls_handshake(60U, 1U);
        tr_raft_handshake_config_t server_handshake =
            peer_service_mtls_handshake(80U, 2U);
        tr_raft_coronet_identity_entry_t client_identity;
        tr_raft_coronet_identity_entry_t server_identity;
        tr_raft_coronet_peer_service_config_t client_config;
        tr_raft_coronet_peer_service_config_t server_config;
        tr_raft_coronet_inbound_service_config_t inbound_config;
        tr_raft_coronet_dial_scheduler_config_t dial_config;
        tr_raft_coronet_peer_service_status_t client_status;
        tr_raft_coronet_peer_service_status_t server_status;
        tr_raft_snapshot_receiver_config_t receiver_config;
        turbo_tls_server_config_t tls_server_config;
        peer_service_mtls_state_t state;
        coro_socket_t *listener = NULL;
        uint64_t deadline;

        memset(&client_identity, 0, sizeof(client_identity));
        memset(&server_identity, 0, sizeof(server_identity));
        memset(&client_config, 0, sizeof(client_config));
        memset(&server_config, 0, sizeof(server_config));
        memset(&inbound_config, 0, sizeof(inbound_config));
        memset(&dial_config, 0, sizeof(dial_config));
        memset(&client_status, 0, sizeof(client_status));
        memset(&server_status, 0, sizeof(server_status));
        memset(&receiver_config, 0, sizeof(receiver_config));
        memset(&tls_server_config, 0, sizeof(tls_server_config));
        memset(&state, 0, sizeof(state));
        state.client_result = TURBO_EBUSY;
        state.server_result = TURBO_EBUSY;

        memcpy(client_identity.certificate_sha256,
               peer_service_node2_fingerprint,
               sizeof(peer_service_node2_fingerprint));
        client_identity.node_id = 2U;
        memcpy(server_identity.certificate_sha256,
               peer_service_node1_fingerprint,
               sizeof(peer_service_node1_fingerprint));
        server_identity.node_id = 1U;

        client_config.manager.cluster_id = client_handshake.cluster_id;
        client_config.manager.local_node_id = 1U;
        client_config.manager.peer_node_ids = client_peers;
        client_config.manager.peer_count = 1U;
        client_config.identity_entries = &client_identity;
        client_config.identity_entry_count = 1U;
        client_config.admit_owned_socket =
            tr_raft_coronet_peer_manager_admit_owned_socket;
        client_config.connect_outbound =
            tr_raft_coronet_peer_manager_connect_outbound;

        server_config.manager.cluster_id = server_handshake.cluster_id;
        server_config.manager.local_node_id = 2U;
        server_config.manager.peer_node_ids = server_peers;
        server_config.manager.peer_count = 1U;
        server_config.identity_entries = &server_identity;
        server_config.identity_entry_count = 1U;
        server_config.admit_owned_socket =
            tr_raft_coronet_peer_manager_admit_owned_socket;
        server_config.connect_outbound =
            tr_raft_coronet_peer_manager_connect_outbound;

        receiver_config.self_id = 2U;
        receiver_config.max_snapshot_bytes = 1024U;
        receiver_config.install = peer_service_mtls_install_snapshot;
        receiver_config.install_context = &state;
        check_int_eq(tr_raft_snapshot_receiver_create(
                         &receiver_config, &state.snapshot_receiver),
                     TURBO_OK);
        server_config.snapshot_receiver = state.snapshot_receiver;
        client_config.on_snapshot_ack =
            peer_service_mtls_collect_snapshot_ack;
        client_config.snapshot_ack_context = &state;

        state.context = coro_context_create(NULL);
        check_not_null(state.context);
        client_config.context = state.context;
        client_config.outbound_queue_capacity =
            PEER_SERVICE_MTLS_QUEUE_CAPACITY;
        server_config.context = state.context;
        server_config.outbound_queue_capacity =
            PEER_SERVICE_MTLS_QUEUE_CAPACITY;
        check_int_eq(tr_raft_coronet_peer_service_create(
                         &client_config, &state.client_service),
                     TURBO_OK);
        check_int_eq(tr_raft_coronet_peer_service_create(
                         &server_config, &state.server_service),
                     TURBO_OK);

        inbound_config.admission.handshake.handshake = &server_handshake;
        inbound_config.admission.handshake.timeout_ms =
            PEER_SERVICE_MTLS_SOCKET_TIMEOUT_MS;
        inbound_config.admission.direction =
            TR_RAFT_CORONET_CONNECTION_INBOUND;
        inbound_config.admission.first_outbound_message_id = 1U;
        inbound_config.admission.peer_idle_timeout_ms =
            PEER_SERVICE_MTLS_SOCKET_TIMEOUT_MS;
        inbound_config.admission.on_message =
            peer_service_mtls_ignore_message;
        inbound_config.admission.message_context = &state;
        inbound_config.on_result = peer_service_mtls_collect_inbound;
        inbound_config.result_context = &state;
        check_int_eq(tr_raft_coronet_peer_service_configure_inbound(
                         state.server_service, &inbound_config),
                     TURBO_OK);

        check_int_eq(tr_test_reserve_loopback_port(&state.port), TURBO_OK);
        listener = coro_socket_create(state.context, CORO_SOCKET_TLS);
        check_not_null(listener);
        tls_server_config.size = sizeof(tls_server_config);
        tls_server_config.cert_file =
            PEER_SERVICE_MTLS_FIXTURE("node2-cert.pem");
        tls_server_config.key_file =
            PEER_SERVICE_MTLS_FIXTURE("node2-key.pem");
        tls_server_config.ca_file = PEER_SERVICE_MTLS_FIXTURE("ca.pem");
        tls_server_config.client_auth = TURBO_TLS_CLIENT_AUTH_REQUIRED;
        check_int_eq(coro_socket_set_tls_server_config(
                         listener, &tls_server_config),
                     TURBO_OK);
        check_int_eq(coro_socket_listen_on(
                         listener, "127.0.0.1", state.port,
                         tr_raft_coronet_peer_service_handle_inbound,
                         state.server_service),
                     TURBO_OK);

        dial_config.outbound.context = state.context;
        dial_config.outbound.connect_timeout_ms =
            PEER_SERVICE_MTLS_SOCKET_TIMEOUT_MS;
        dial_config.outbound.tls.ca_file =
            PEER_SERVICE_MTLS_FIXTURE("ca.pem");
        dial_config.outbound.tls.cert_file =
            PEER_SERVICE_MTLS_FIXTURE("node1-cert.pem");
        dial_config.outbound.tls.key_file =
            PEER_SERVICE_MTLS_FIXTURE("node1-key.pem");
        dial_config.outbound.tls.verify_peer = 1;
        dial_config.outbound.admission.handshake.handshake =
            &client_handshake;
        dial_config.outbound.admission.handshake.timeout_ms =
            PEER_SERVICE_MTLS_SOCKET_TIMEOUT_MS;
        dial_config.outbound.admission.direction =
            TR_RAFT_CORONET_CONNECTION_OUTBOUND;
        dial_config.outbound.admission.expected_peer_node_id = 2U;
        dial_config.outbound.admission.first_outbound_message_id = 1U;
        dial_config.outbound.admission.peer_idle_timeout_ms =
            PEER_SERVICE_MTLS_SOCKET_TIMEOUT_MS;
        dial_config.outbound.admission.on_message =
            peer_service_mtls_ignore_message;
        dial_config.resolve_endpoint = peer_service_mtls_resolve_endpoint;
        dial_config.resolve_context = &state;
        dial_config.is_retryable = peer_service_mtls_retry_io;
        dial_config.initial_retry_delay_ms = 1U;
        dial_config.max_retry_delay_ms = 1U;
        dial_config.max_attempts = 1U;
        check_int_eq(tr_raft_coronet_peer_service_add_outbound(
                         state.client_service, &dial_config),
                     TURBO_OK);

        check_int_eq(coro_context_spawn(state.context,
                                        peer_service_mtls_connect,
                                        &state),
                     TURBO_OK);
        deadline = turbo_monotonic_ms() + PEER_SERVICE_MTLS_TEST_TIMEOUT_MS;
        while (!peer_service_mtls_complete(&state) &&
               turbo_monotonic_ms() < deadline) {
            coro_context_run(state.context, TURBO_RUN_ONCE);
        }

        check_int_eq(state.client_result, TURBO_OK);
        check_int_eq(state.server_result, TURBO_OK);
        check_int_eq(state.server_peer_node_id, 1);
        {
            tr_raft_message_t message;
            size_t index;

            memset(&message, 0, sizeof(message));
            message.type = TR_RAFT_MSG_HEARTBEAT_REQUEST;
            message.from = 1U;
            message.to = 2U;
            for (index = 0U; index < PEER_SERVICE_MTLS_BATCH_SIZE;
                 ++index) {
                message.term = index + 1U;
                check_int_eq(tr_raft_coronet_peer_service_enqueue(
                                 state.client_service, &message),
                             TURBO_OK);
            }
            for (index = 0U;
                 index < 1000U &&
                 state.received_count <
                     sizeof(state.received_terms) /
                         sizeof(state.received_terms[0]);
                 ++index) {
                coro_context_run(state.context, TURBO_RUN_NOWAIT);
            }
            check_size_eq(state.received_count, 3U);
            check_long_eq(state.received_terms[0], 1U);
            check_long_eq(state.received_terms[1], 2U);
            check_long_eq(state.received_terms[2], 3U);
            check_int_eq(tr_raft_coronet_peer_service_get_status(
                             state.client_service, &client_status),
                         TURBO_OK);
            check_size_eq(client_status.queued_message_count, 0U);
        }
        {
            static const uint8_t abc_sha256[
                TR_RAFT_WIRE_SNAPSHOT_DIGEST_SIZE] = {
                0xbaU, 0x78U, 0x16U, 0xbfU, 0x8fU, 0x01U, 0xcfU, 0xeaU,
                0x41U, 0x41U, 0x40U, 0xdeU, 0x5dU, 0xaeU, 0x22U, 0x23U,
                0xb0U, 0x03U, 0x61U, 0xa3U, 0x96U, 0x17U, 0x7aU, 0x9cU,
                0xb4U, 0x10U, 0xffU, 0x61U, 0xf2U, 0x00U, 0x15U, 0xadU
            };
            tr_raft_coronet_payload_t payload;
            uint8_t chunk_data[3] = {'a', 'b', 'c'};
            size_t run_count;

            memset(&payload, 0, sizeof(payload));
            payload.kind = TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_CHUNK;
            payload.data.snapshot_chunk.from = 1U;
            payload.data.snapshot_chunk.to = 2U;
            payload.data.snapshot_chunk.term = 7U;
            payload.data.snapshot_chunk.snapshot_index = 9U;
            payload.data.snapshot_chunk.snapshot_term = 6U;
            payload.data.snapshot_chunk.snapshot_size = 3U;
            payload.data.snapshot_chunk.has_configuration = true;
            payload.data.snapshot_chunk.configuration =
                peer_service_snapshot_configuration;
            payload.data.snapshot_chunk.data_length = 3U;
            payload.data.snapshot_chunk.data = chunk_data;
            payload.data.snapshot_chunk.done = true;
            memcpy(payload.data.snapshot_chunk.snapshot_digest, abc_sha256,
                   sizeof(abc_sha256));
            check_int_eq(tr_raft_coronet_peer_service_enqueue_payload(
                             state.client_service, &payload),
                         TURBO_OK);
            memset(chunk_data, 0, sizeof(chunk_data));
            for (run_count = 0U;
                 run_count < 1000U && state.snapshot_ack_count == 0U;
                 ++run_count) {
                coro_context_run(state.context, TURBO_RUN_NOWAIT);
            }
            check_size_eq(state.snapshot_install_count, 1U);
            check_size_eq(state.installed_snapshot_size, 3U);
            check_mem_eq(state.installed_snapshot, "abc", 3U);
            check_size_eq(state.snapshot_ack_count, 1U);
            check(state.snapshot_ack.accepted);
            check_long_eq(state.snapshot_ack.next_offset, 3U);
            check_int_eq(tr_raft_coronet_peer_service_get_status(
                             state.server_service, &server_status),
                         TURBO_OK);
            check_long_eq(server_status.snapshot_install_count, 1U);
            check_int_eq(tr_raft_coronet_peer_service_get_status(
                             state.client_service, &client_status),
                         TURBO_OK);
            check_long_eq(client_status.snapshot_ack_count, 1U);
        }
        check_int_eq(tr_raft_coronet_peer_service_disconnect_peer(
                         state.client_service, 2U, turbo_monotonic_ms()),
                     TURBO_OK);
        {
            tr_raft_message_t message;
            size_t index;

            memset(&message, 0, sizeof(message));
            message.type = TR_RAFT_MSG_HEARTBEAT_REQUEST;
            message.from = 1U;
            message.to = 2U;
            for (index = PEER_SERVICE_MTLS_BATCH_SIZE;
                 index < PEER_SERVICE_MTLS_MESSAGE_COUNT; ++index) {
                message.term = index + 1U;
                check_int_eq(tr_raft_coronet_peer_service_enqueue(
                                 state.client_service, &message),
                             TURBO_OK);
            }
        }
        deadline = turbo_monotonic_ms() + PEER_SERVICE_MTLS_TEST_TIMEOUT_MS;
        do {
            coro_context_run(state.context, TURBO_RUN_ONCE);
            check_int_eq(tr_raft_coronet_peer_service_get_status(
                             state.client_service, &client_status),
                         TURBO_OK);
            check_int_eq(tr_raft_coronet_peer_service_get_status(
                             state.server_service, &server_status),
                         TURBO_OK);
        } while ((client_status.active_reader_count != 0U ||
                  server_status.active_reader_count != 0U) &&
                 turbo_monotonic_ms() < deadline);
        check_size_eq(client_status.active_reader_count, 0U);
        check_size_eq(server_status.active_reader_count, 0U);
        check_size_eq(client_status.queued_message_count,
                      PEER_SERVICE_MTLS_BATCH_SIZE);

        state.client_result = TURBO_EBUSY;
        state.server_result = TURBO_EBUSY;
        state.server_peer_node_id = 0U;
        check_int_eq(coro_context_spawn(state.context,
                                        peer_service_mtls_connect,
                                        &state),
                     TURBO_OK);
        deadline = turbo_monotonic_ms() + PEER_SERVICE_MTLS_TEST_TIMEOUT_MS;
        while (!peer_service_mtls_complete(&state) &&
               turbo_monotonic_ms() < deadline) {
            coro_context_run(state.context, TURBO_RUN_ONCE);
        }
        check_int_eq(state.client_result, TURBO_OK);
        check_int_eq(state.server_result, TURBO_OK);
        check_int_eq(state.server_peer_node_id, 1);
        deadline = turbo_monotonic_ms() + PEER_SERVICE_MTLS_TEST_TIMEOUT_MS;
        while (state.received_count < PEER_SERVICE_MTLS_MESSAGE_COUNT &&
               turbo_monotonic_ms() < deadline) {
            coro_context_run(state.context, TURBO_RUN_ONCE);
        }
        check_size_eq(state.received_count,
                      PEER_SERVICE_MTLS_MESSAGE_COUNT);
        check_long_eq(state.received_terms[3], 4U);
        check_long_eq(state.received_terms[4], 5U);
        check_long_eq(state.received_terms[5], 6U);
        check_int_eq(tr_raft_coronet_peer_service_get_status(
                         state.client_service, &client_status),
                     TURBO_OK);
        check_size_eq(client_status.queued_message_count, 0U);
        check_int_eq(state.client_step.scheduler_count, 1);
        check_int_eq(state.client_step.attempted_count, 1);
        check_int_eq(state.client_step.newly_connected_count, 1);
        check_int_eq(state.client_step.failed_count, 0);
        check_int_eq(tr_raft_coronet_peer_service_get_status(
                         state.client_service, &client_status),
                     TURBO_OK);
        check_int_eq(tr_raft_coronet_peer_service_get_status(
                         state.server_service, &server_status),
                     TURBO_OK);
        check_long_eq(client_status.identity_generation, 1U);
        check_long_eq(server_status.identity_generation, 1U);
        check_int_eq(server_status.inbound_configured, 1);

        coro_socket_destroy(listener);
        check_int_eq(tr_raft_coronet_peer_service_stop(state.client_service),
                     TURBO_OK);
        check_int_eq(tr_raft_coronet_peer_service_stop(state.server_service),
                     TURBO_OK);
        deadline = turbo_monotonic_ms() +
                   PEER_SERVICE_MTLS_SOCKET_TIMEOUT_MS;
        do {
            check_int_eq(tr_raft_coronet_peer_service_get_status(
                             state.client_service, &client_status),
                         TURBO_OK);
            check_int_eq(tr_raft_coronet_peer_service_get_status(
                             state.server_service, &server_status),
                         TURBO_OK);
            if (client_status.active_operation_count == 0U &&
                server_status.active_operation_count == 0U) {
                break;
            }
            coro_context_run(state.context, TURBO_RUN_NOWAIT);
        } while (turbo_monotonic_ms() < deadline);
        check_int_eq(client_status.active_operation_count, 0);
        check_int_eq(server_status.active_operation_count, 0);
        check_int_eq(tr_raft_coronet_peer_service_destroy(
                         state.client_service),
                     TURBO_OK);
        check_int_eq(tr_raft_coronet_peer_service_destroy(
                         state.server_service),
                     TURBO_OK);
        tr_raft_snapshot_receiver_destroy(state.snapshot_receiver);
        coro_context_destroy(state.context);
    }
}
