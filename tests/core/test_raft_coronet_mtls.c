#include <turboraft/raft_coronet_transport.h>

#include <CoroNet.h>
#include <tinytest.h>
#include <turbo_error.h>

#include <stdint.h>
#include <string.h>

#include "raft_coronet_mtls_test_support.h"

#define MTLS_TEST_TIMEOUT_MS 10000U
#define MTLS_SOCKET_TIMEOUT_MS 5000U
#define MTLS_FIXTURE(name) TURBORAFT_TEST_FIXTURE_DIR "/" name

typedef struct mtls_identity_context {
    tr_raft_node_id_t peer_node_id;
} mtls_identity_context_t;

typedef struct mtls_test_state {
    coro_context_t *context;
    tr_raft_coronet_peer_manager_t *client_manager;
    tr_raft_coronet_peer_manager_t *server_manager;
    tr_raft_coronet_inbound_service_t *inbound_service;
    tr_raft_coronet_outbound_config_t outbound;
    int client_result;
    int server_result;
    tr_raft_node_id_t client_peer_node_id;
    tr_raft_node_id_t server_peer_node_id;
} mtls_test_state_t;

static tr_raft_handshake_config_t mtls_make_handshake(
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

static int mtls_resolve_identity(void *context,
                                 const char *verified_certificate_sha256,
                                 tr_raft_node_id_t *out_peer_node_id)
{
    mtls_identity_context_t *identity =
        (mtls_identity_context_t *) context;

    if (identity == NULL || verified_certificate_sha256 == NULL ||
        out_peer_node_id == NULL ||
        strncmp(verified_certificate_sha256, "sha256:", 7U) != 0 ||
        strlen(verified_certificate_sha256) !=
            CORO_TLS_PEER_CERT_SHA256_CAPACITY - 1U) {
        return TURBO_EPROTO;
    }
    *out_peer_node_id = identity->peer_node_id;
    return TURBO_OK;
}

static int mtls_ignore_message(void *context,
                               const tr_raft_message_t *message)
{
    (void) context;
    (void) message;
    return TURBO_OK;
}

static void mtls_collect_inbound_result(void *context,
                                        int result,
                                        tr_raft_node_id_t peer_node_id)
{
    mtls_test_state_t *state = (mtls_test_state_t *) context;

    state->server_result = result;
    state->server_peer_node_id = peer_node_id;
}

static void mtls_connect_client(coro_t *coroutine, void *context)
{
    mtls_test_state_t *state = (mtls_test_state_t *) context;

    (void) coroutine;
    state->client_result = tr_raft_coronet_peer_manager_connect_outbound(
        state->client_manager, &state->outbound,
        &state->client_peer_node_id);
}

static int mtls_case_complete(const mtls_test_state_t *state)
{
    return state->client_result != TURBO_EBUSY &&
           state->server_result != TURBO_EBUSY;
}

static void mtls_run_identity_mismatch_case(void)
{
    const tr_raft_node_id_t client_peers[] = {2U};
    const tr_raft_node_id_t server_peers[] = {1U};
    tr_raft_handshake_config_t client_handshake =
        mtls_make_handshake(60U, 1U);
    tr_raft_handshake_config_t server_handshake =
        mtls_make_handshake(80U, 2U);
    tr_raft_coronet_peer_manager_config_t client_manager_config;
    tr_raft_coronet_peer_manager_config_t server_manager_config;
    tr_raft_coronet_inbound_service_config_t inbound_config;
    mtls_identity_context_t client_identity;
    mtls_identity_context_t server_identity;
    turbo_tls_server_config_t tls_server_config;
    coro_socket_t *server = NULL;
    mtls_test_state_t state;
    unsigned short port = 0U;
    uint64_t deadline;

    memset(&client_manager_config, 0, sizeof(client_manager_config));
    memset(&server_manager_config, 0, sizeof(server_manager_config));
    memset(&inbound_config, 0, sizeof(inbound_config));
    memset(&tls_server_config, 0, sizeof(tls_server_config));
    memset(&state, 0, sizeof(state));
    client_identity.peer_node_id = 3U;
    server_identity.peer_node_id = 4U;
    state.client_result = TURBO_EBUSY;
    state.server_result = TURBO_EBUSY;

    client_manager_config.cluster_id = client_handshake.cluster_id;
    client_manager_config.local_node_id = 1U;
    client_manager_config.peer_node_ids = client_peers;
    client_manager_config.peer_count = 1U;
    server_manager_config.cluster_id = server_handshake.cluster_id;
    server_manager_config.local_node_id = 2U;
    server_manager_config.peer_node_ids = server_peers;
    server_manager_config.peer_count = 1U;

    state.context = coro_context_create(NULL);
    check_not_null(state.context);
    check_int_eq(tr_raft_coronet_peer_manager_create(
                     &client_manager_config, &state.client_manager),
                 TURBO_OK);
    check_int_eq(tr_raft_coronet_peer_manager_create(
                     &server_manager_config, &state.server_manager),
                 TURBO_OK);

    inbound_config.manager = state.server_manager;
    inbound_config.admission.handshake.handshake = &server_handshake;
    inbound_config.admission.handshake.timeout_ms = MTLS_SOCKET_TIMEOUT_MS;
    inbound_config.admission.handshake.resolve_peer_identity =
        mtls_resolve_identity;
    inbound_config.admission.handshake.identity_context = &server_identity;
    inbound_config.admission.direction = TR_RAFT_CORONET_CONNECTION_INBOUND;
    inbound_config.admission.first_outbound_message_id = 1U;
    inbound_config.admission.peer_idle_timeout_ms = MTLS_SOCKET_TIMEOUT_MS;
    inbound_config.admission.on_message = mtls_ignore_message;
    inbound_config.admit_owned_socket =
        tr_raft_coronet_peer_manager_admit_owned_socket;
    inbound_config.on_result = mtls_collect_inbound_result;
    inbound_config.result_context = &state;
    check_int_eq(tr_raft_coronet_inbound_service_create(
                     &inbound_config, &state.inbound_service),
                 TURBO_OK);

    check_int_eq(tr_test_reserve_loopback_port(&port), TURBO_OK);
    server = coro_socket_create(state.context, CORO_SOCKET_TLS);
    check_not_null(server);
    tls_server_config.size = sizeof(tls_server_config);
    tls_server_config.cert_file = MTLS_FIXTURE("node2-cert.pem");
    tls_server_config.key_file = MTLS_FIXTURE("node2-key.pem");
    tls_server_config.ca_file = MTLS_FIXTURE("ca.pem");
    tls_server_config.client_auth = TURBO_TLS_CLIENT_AUTH_REQUIRED;
    check_int_eq(coro_socket_set_tls_server_config(server, &tls_server_config),
                 TURBO_OK);
    check_int_eq(coro_socket_listen_on(
                     server, "127.0.0.1", port,
                     tr_raft_coronet_inbound_service_handle,
                     state.inbound_service),
                 TURBO_OK);

    state.outbound.context = state.context;
    state.outbound.connect_host = "127.0.0.1";
    state.outbound.request_host = "node-2.mesh";
    state.outbound.port = port;
    state.outbound.connect_timeout_ms = MTLS_SOCKET_TIMEOUT_MS;
    state.outbound.tls.ca_file = MTLS_FIXTURE("ca.pem");
    state.outbound.tls.cert_file = MTLS_FIXTURE("node1-cert.pem");
    state.outbound.tls.key_file = MTLS_FIXTURE("node1-key.pem");
    state.outbound.tls.verify_peer = 1;
    state.outbound.admission.handshake.handshake = &client_handshake;
    state.outbound.admission.handshake.timeout_ms = MTLS_SOCKET_TIMEOUT_MS;
    state.outbound.admission.handshake.resolve_peer_identity =
        mtls_resolve_identity;
    state.outbound.admission.handshake.identity_context = &client_identity;
    state.outbound.admission.direction =
        TR_RAFT_CORONET_CONNECTION_OUTBOUND;
    state.outbound.admission.expected_peer_node_id = 2U;
    state.outbound.admission.first_outbound_message_id = 1U;
    state.outbound.admission.peer_idle_timeout_ms = MTLS_SOCKET_TIMEOUT_MS;
    state.outbound.admission.on_message = mtls_ignore_message;

    check_int_eq(coro_context_spawn(state.context, mtls_connect_client,
                                    &state),
                 TURBO_OK);
    deadline = turbo_monotonic_ms() + MTLS_TEST_TIMEOUT_MS;
    while (!mtls_case_complete(&state) &&
           turbo_monotonic_ms() < deadline) {
        coro_context_run(state.context, TURBO_RUN_ONCE);
    }

    check_int_eq(state.client_result, TURBO_EPROTO);
    check_int_eq(state.server_result, TURBO_EPROTO);
    check_int_eq(state.client_peer_node_id, 0);
    check_int_eq(state.server_peer_node_id, 0);

    coro_socket_destroy(server);
    check_int_eq(tr_raft_coronet_inbound_service_destroy(
                     state.inbound_service),
                 TURBO_OK);
    check_int_eq(tr_raft_coronet_peer_manager_destroy(state.client_manager),
                 TURBO_OK);
    check_int_eq(tr_raft_coronet_peer_manager_destroy(state.server_manager),
                 TURBO_OK);
    deadline = turbo_monotonic_ms() + MTLS_SOCKET_TIMEOUT_MS;
    while (coro_context_alive(state.context) &&
           turbo_monotonic_ms() < deadline) {
        coro_context_run(state.context, TURBO_RUN_NOWAIT);
    }
    coro_context_destroy(state.context);
}

spec("raft CoroNet mTLS integration")
{
    it("admits both sides through verified mTLS and HELLO exchange")
    {
        const tr_raft_node_id_t client_peers[] = {2U};
        const tr_raft_node_id_t server_peers[] = {1U};
        tr_raft_handshake_config_t client_handshake =
            mtls_make_handshake(60U, 1U);
        tr_raft_handshake_config_t server_handshake =
            mtls_make_handshake(80U, 2U);
        tr_raft_coronet_peer_manager_config_t client_manager_config;
        tr_raft_coronet_peer_manager_config_t server_manager_config;
        tr_raft_coronet_inbound_service_config_t inbound_config;
        mtls_identity_context_t client_identity;
        mtls_identity_context_t server_identity;
        turbo_tls_server_config_t tls_server_config;
        coro_socket_t *server = NULL;
        mtls_test_state_t state;
        unsigned short port = 0U;
        uint64_t deadline;

        memset(&client_manager_config, 0, sizeof(client_manager_config));
        memset(&server_manager_config, 0, sizeof(server_manager_config));
        memset(&inbound_config, 0, sizeof(inbound_config));
        memset(&tls_server_config, 0, sizeof(tls_server_config));
        memset(&state, 0, sizeof(state));
        client_identity.peer_node_id = 2U;
        server_identity.peer_node_id = 1U;
        state.client_result = TURBO_EBUSY;
        state.server_result = TURBO_EBUSY;

        client_manager_config.cluster_id = client_handshake.cluster_id;
        client_manager_config.local_node_id = 1U;
        client_manager_config.peer_node_ids = client_peers;
        client_manager_config.peer_count = 1U;
        server_manager_config.cluster_id = server_handshake.cluster_id;
        server_manager_config.local_node_id = 2U;
        server_manager_config.peer_node_ids = server_peers;
        server_manager_config.peer_count = 1U;

        state.context = coro_context_create(NULL);
        check_not_null(state.context);
        check_int_eq(tr_raft_coronet_peer_manager_create(
                         &client_manager_config, &state.client_manager),
                     TURBO_OK);
        check_int_eq(tr_raft_coronet_peer_manager_create(
                         &server_manager_config, &state.server_manager),
                     TURBO_OK);

        inbound_config.manager = state.server_manager;
        inbound_config.admission.handshake.handshake = &server_handshake;
        inbound_config.admission.handshake.timeout_ms =
            MTLS_SOCKET_TIMEOUT_MS;
        inbound_config.admission.handshake.resolve_peer_identity =
            mtls_resolve_identity;
        inbound_config.admission.handshake.identity_context = &server_identity;
        inbound_config.admission.direction =
            TR_RAFT_CORONET_CONNECTION_INBOUND;
        inbound_config.admission.first_outbound_message_id = 1U;
        inbound_config.admission.peer_idle_timeout_ms =
            MTLS_SOCKET_TIMEOUT_MS;
        inbound_config.admission.on_message = mtls_ignore_message;
        inbound_config.admit_owned_socket =
            tr_raft_coronet_peer_manager_admit_owned_socket;
        inbound_config.on_result = mtls_collect_inbound_result;
        inbound_config.result_context = &state;
        check_int_eq(tr_raft_coronet_inbound_service_create(
                         &inbound_config, &state.inbound_service),
                     TURBO_OK);

        check_int_eq(tr_test_reserve_loopback_port(&port), TURBO_OK);
        server = coro_socket_create(state.context, CORO_SOCKET_TLS);
        check_not_null(server);
        tls_server_config.size = sizeof(tls_server_config);
        tls_server_config.cert_file = MTLS_FIXTURE("node2-cert.pem");
        tls_server_config.key_file = MTLS_FIXTURE("node2-key.pem");
        tls_server_config.ca_file = MTLS_FIXTURE("ca.pem");
        tls_server_config.client_auth = TURBO_TLS_CLIENT_AUTH_REQUIRED;
        check_int_eq(coro_socket_set_tls_server_config(
                         server, &tls_server_config),
                     TURBO_OK);
        check_int_eq(coro_socket_listen_on(
                         server, "127.0.0.1", port,
                         tr_raft_coronet_inbound_service_handle,
                         state.inbound_service),
                     TURBO_OK);
        state.outbound.context = state.context;
        state.outbound.connect_host = "127.0.0.1";
        state.outbound.request_host = "node-2.mesh";
        state.outbound.port = port;
        state.outbound.connect_timeout_ms = MTLS_SOCKET_TIMEOUT_MS;
        state.outbound.tls.ca_file = MTLS_FIXTURE("ca.pem");
        state.outbound.tls.cert_file = MTLS_FIXTURE("node1-cert.pem");
        state.outbound.tls.key_file = MTLS_FIXTURE("node1-key.pem");
        state.outbound.tls.verify_peer = 1;
        state.outbound.admission.handshake.handshake = &client_handshake;
        state.outbound.admission.handshake.timeout_ms =
            MTLS_SOCKET_TIMEOUT_MS;
        state.outbound.admission.handshake.resolve_peer_identity =
            mtls_resolve_identity;
        state.outbound.admission.handshake.identity_context = &client_identity;
        state.outbound.admission.direction =
            TR_RAFT_CORONET_CONNECTION_OUTBOUND;
        state.outbound.admission.expected_peer_node_id = 2U;
        state.outbound.admission.first_outbound_message_id = 1U;
        state.outbound.admission.peer_idle_timeout_ms =
            MTLS_SOCKET_TIMEOUT_MS;
        state.outbound.admission.on_message = mtls_ignore_message;

        check_int_eq(coro_context_spawn(state.context, mtls_connect_client,
                                        &state),
                     TURBO_OK);
        deadline = turbo_monotonic_ms() + MTLS_TEST_TIMEOUT_MS;
        while (!mtls_case_complete(&state) &&
               turbo_monotonic_ms() < deadline) {
            coro_context_run(state.context, TURBO_RUN_ONCE);
        }

        check_int_eq(state.client_result, TURBO_OK);
        check_int_eq(state.server_result, TURBO_OK);
        check_int_eq(state.client_peer_node_id, 2);
        check_int_eq(state.server_peer_node_id, 1);

        coro_socket_destroy(server);
        check_int_eq(tr_raft_coronet_inbound_service_destroy(
                         state.inbound_service),
                     TURBO_OK);
        check_int_eq(tr_raft_coronet_peer_manager_destroy(
                         state.client_manager),
                     TURBO_OK);
        check_int_eq(tr_raft_coronet_peer_manager_destroy(
                         state.server_manager),
                     TURBO_OK);
        deadline = turbo_monotonic_ms() + MTLS_SOCKET_TIMEOUT_MS;
        while (coro_context_alive(state.context) &&
               turbo_monotonic_ms() < deadline) {
            coro_context_run(state.context, TURBO_RUN_NOWAIT);
        }
        coro_context_destroy(state.context);
    }

    it("rejects certificate identities that differ from HELLO node ids")
    {
        mtls_run_identity_mismatch_case();
    }
}
