#include <turboraft/raft_coronet_transport.h>

#include <CoroNet.h>
#include <tinytest.h>
#include <turbo_error.h>

#include <stdint.h>
#include <string.h>

#include "raft_coronet_mtls_test_support.h"

#define MTLS_REJECTION_TIMEOUT_MS 10000U
#define MTLS_SOCKET_TIMEOUT_MS 5000U
#define MTLS_FIXTURE(name) TURBORAFT_TEST_FIXTURE_DIR "/" name

typedef struct mtls_rejection_state {
    coro_context_t *context;
    tr_raft_coronet_peer_manager_t *manager;
    tr_raft_coronet_outbound_config_t outbound;
    int connect_result;
    int identity_resolver_calls;
    int server_handler_calls;
} mtls_rejection_state_t;

static tr_raft_handshake_config_t mtls_rejection_handshake(void)
{
    tr_raft_handshake_config_t config;
    size_t index;

    memset(&config, 0, sizeof(config));
    for (index = 0U; index < sizeof(config.cluster_id.bytes); ++index) {
        config.cluster_id.bytes[index] = (uint8_t) (40U + index);
        config.process_incarnation.bytes[index] = (uint8_t) (60U + index);
    }
    config.local_node_id = 1U;
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

static int mtls_rejection_resolve_identity(
    void *context,
    const char *verified_certificate_sha256,
    tr_raft_node_id_t *out_peer_node_id)
{
    mtls_rejection_state_t *state = (mtls_rejection_state_t *) context;

    (void) verified_certificate_sha256;
    ++state->identity_resolver_calls;
    *out_peer_node_id = 2U;
    return TURBO_OK;
}

static int mtls_rejection_ignore_message(
    void *context,
    const tr_raft_message_t *message)
{
    (void) context;
    (void) message;
    return TURBO_OK;
}

static void mtls_rejection_server_handler(coro_socket_t *socket, void *context)
{
    mtls_rejection_state_t *state = (mtls_rejection_state_t *) context;

    ++state->server_handler_calls;
    coro_socket_destroy(socket);
}

static void mtls_rejection_connect(coro_t *coroutine, void *context)
{
    mtls_rejection_state_t *state = (mtls_rejection_state_t *) context;
    tr_raft_node_id_t peer_node_id = 0U;

    (void) coroutine;
    state->connect_result = tr_raft_coronet_peer_manager_connect_outbound(
        state->manager, &state->outbound, &peer_node_id);
}

static void mtls_run_rejection_case(const char *request_host,
                                    const char *ca_file)
{
    const tr_raft_node_id_t peer_node_ids[] = {2U};
    tr_raft_handshake_config_t handshake = mtls_rejection_handshake();
    tr_raft_coronet_peer_manager_config_t manager_config;
    turbo_tls_server_config_t server_tls;
    mtls_rejection_state_t state;
    coro_socket_t *server;
    unsigned short port = 0U;
    uint64_t deadline;

    memset(&manager_config, 0, sizeof(manager_config));
    memset(&server_tls, 0, sizeof(server_tls));
    memset(&state, 0, sizeof(state));
    state.connect_result = TURBO_EBUSY;
    manager_config.cluster_id = handshake.cluster_id;
    manager_config.local_node_id = 1U;
    manager_config.peer_node_ids = peer_node_ids;
    manager_config.peer_count = 1U;

    check_equal(tr_test_reserve_loopback_port(&port), TURBO_OK);
    state.context = coro_context_create(NULL);
    check_not_null(state.context);
    check_equal(tr_raft_coronet_peer_manager_create(&manager_config,
                                                      &state.manager),
                 TURBO_OK);
    server = coro_socket_create(state.context, CORO_SOCKET_TLS);
    check_not_null(server);
    server_tls.size = sizeof(server_tls);
    server_tls.cert_file = MTLS_FIXTURE("node2-cert.pem");
    server_tls.key_file = MTLS_FIXTURE("node2-key.pem");
    server_tls.ca_file = MTLS_FIXTURE("ca.pem");
    server_tls.client_auth = TURBO_TLS_CLIENT_AUTH_REQUIRED;
    check_equal(coro_socket_set_tls_server_config(server, &server_tls),
                 TURBO_OK);
    check_equal(coro_socket_listen_on(server, "127.0.0.1", port,
                                       mtls_rejection_server_handler, &state),
                 TURBO_OK);

    state.outbound.context = state.context;
    state.outbound.connect_host = "127.0.0.1";
    state.outbound.request_host = request_host;
    state.outbound.port = port;
    state.outbound.connect_timeout_ms = MTLS_SOCKET_TIMEOUT_MS;
    state.outbound.tls.ca_file = ca_file;
    state.outbound.tls.cert_file = MTLS_FIXTURE("node1-cert.pem");
    state.outbound.tls.key_file = MTLS_FIXTURE("node1-key.pem");
    state.outbound.tls.verify_peer = 1;
    state.outbound.admission.handshake.handshake = &handshake;
    state.outbound.admission.handshake.timeout_ms = MTLS_SOCKET_TIMEOUT_MS;
    state.outbound.admission.handshake.resolve_peer_identity =
        mtls_rejection_resolve_identity;
    state.outbound.admission.handshake.identity_context = &state;
    state.outbound.admission.direction =
        TR_RAFT_CORONET_CONNECTION_OUTBOUND;
    state.outbound.admission.expected_peer_node_id = 2U;
    state.outbound.admission.first_outbound_message_id = 1U;
    state.outbound.admission.peer_idle_timeout_ms = MTLS_SOCKET_TIMEOUT_MS;
    state.outbound.admission.on_message = mtls_rejection_ignore_message;

    check_equal(coro_context_spawn(state.context, mtls_rejection_connect,
                                    &state),
                 TURBO_OK);
    deadline = turbo_monotonic_ms() + MTLS_REJECTION_TIMEOUT_MS;
    while (state.connect_result == TURBO_EBUSY &&
           turbo_monotonic_ms() < deadline) {
        coro_context_run(state.context, TURBO_RUN_ONCE);
    }

    check(state.connect_result != TURBO_EBUSY);
    check(state.connect_result != TURBO_OK);
    check_equal(state.identity_resolver_calls, 0);

    coro_socket_destroy(server);
    check_equal(tr_raft_coronet_peer_manager_destroy(state.manager),
                 TURBO_OK);
    deadline = turbo_monotonic_ms() + MTLS_SOCKET_TIMEOUT_MS;
    while (coro_context_alive(state.context) &&
           turbo_monotonic_ms() < deadline) {
        coro_context_run(state.context, TURBO_RUN_NOWAIT);
    }
    coro_context_destroy(state.context);
}

spec("raft CoroNet mTLS rejection")
{
    it("rejects a virtual domain that is absent from the certificate")
    {
        mtls_run_rejection_case("wrong.mesh", MTLS_FIXTURE("ca.pem"));
    }

    it("rejects a server certificate from an untrusted authority")
    {
        mtls_run_rejection_case("node-2.mesh",
                                MTLS_FIXTURE("node1-cert.pem"));
    }
}
