#include <turboraft/raft_coronet_peer_service.h>

#include <CoroNet.h>
#include <tinytest.h>
#include <turbo_error.h>

#include <stdint.h>
#include <string.h>

#include "raft_coronet_mtls_test_support.h"

#define THREE_NODE_COUNT 3U
#define THREE_NODE_PEER_COUNT 2U
#define THREE_NODE_TIMEOUT_MS 10000U
#define THREE_NODE_SOCKET_TIMEOUT_MS 5000U
#define THREE_NODE_FIXTURE(name) TURBORAFT_TEST_FIXTURE_DIR "/" name

static const char three_node_fingerprints[THREE_NODE_COUNT][72] = {
    "sha256:cb334522bda1caf62ec1e374f43b0df0b9d7aee5c91d064ff81ac82f005f2aff",
    "sha256:483ac612f03ae69445ea33f6b8ef27342ce9fad891416413e91f57c3b3693733",
    "sha256:4b5010e8baf75a949909e18e25ab420d71b1e2df8362b34b5aa1fdebd91e2d3b"
};
static const char three_node_node2_client_fingerprint[] =
    "sha256:d696b3ab8d0596e3e8e8abcecc4ca87856eda0f5055f31a7dfb6c213c2c2b1f3";

typedef struct three_node_cluster three_node_cluster_t;

typedef struct three_node_peer {
    three_node_cluster_t *cluster;
    tr_raft_node_id_t node_id;
    tr_raft_node_id_t peer_ids[THREE_NODE_PEER_COUNT];
    tr_raft_handshake_config_t handshake;
    tr_raft_coronet_peer_service_t *service;
    coro_socket_t *listener;
    unsigned short port;
    tr_raft_coronet_peer_service_step_result_t step_result;
    int step_call_result;
    int inbound_error;
    size_t inbound_count;
    int message_error;
    size_t message_count;
    uint32_t sender_mask;
} three_node_peer_t;

struct three_node_cluster {
    coro_context_t *context;
    three_node_peer_t nodes[THREE_NODE_COUNT];
    int send_result;
    int send_complete;
};

static three_node_peer_t *three_node_find(
    three_node_cluster_t *cluster,
    tr_raft_node_id_t node_id)
{
    if (cluster == NULL || node_id == 0U || node_id > THREE_NODE_COUNT) {
        return NULL;
    }
    return &cluster->nodes[node_id - 1U];
}

static const char *three_node_certificate(tr_raft_node_id_t node_id)
{
    static const char *const certificates[THREE_NODE_COUNT] = {
        THREE_NODE_FIXTURE("node1-cert.pem"),
        THREE_NODE_FIXTURE("node2-cert.pem"),
        THREE_NODE_FIXTURE("node3-cert.pem")
    };

    return node_id == 0U || node_id > THREE_NODE_COUNT
               ? NULL
               : certificates[node_id - 1U];
}

static const char *three_node_private_key(tr_raft_node_id_t node_id)
{
    static const char *const private_keys[THREE_NODE_COUNT] = {
        THREE_NODE_FIXTURE("node1-key.pem"),
        THREE_NODE_FIXTURE("node2-key.pem"),
        THREE_NODE_FIXTURE("node3-key.pem")
    };

    return node_id == 0U || node_id > THREE_NODE_COUNT
               ? NULL
               : private_keys[node_id - 1U];
}

static const char *three_node_trust_for_peer(tr_raft_node_id_t node_id)
{
    return node_id == 3U ? THREE_NODE_FIXTURE("node3-ca.pem")
                         : THREE_NODE_FIXTURE("ca.pem");
}

static tr_raft_handshake_config_t three_node_handshake(
    tr_raft_node_id_t node_id)
{
    tr_raft_handshake_config_t config;
    size_t index;

    memset(&config, 0, sizeof(config));
    for (index = 0U; index < sizeof(config.cluster_id.bytes); ++index) {
        config.cluster_id.bytes[index] = (uint8_t) (40U + index);
        config.process_incarnation.bytes[index] =
            (uint8_t) (60U + node_id * 20U + index);
    }
    config.local_node_id = node_id;
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

static int three_node_receive_message(void *context,
                                      const tr_raft_message_t *message)
{
    three_node_peer_t *node = (three_node_peer_t *) context;
    uint32_t sender_bit;

    if (node == NULL || message == NULL || message->to != node->node_id ||
        message->from == 0U || message->from > THREE_NODE_COUNT ||
        message->from == node->node_id) {
        if (node != NULL) {
            node->message_error = TURBO_EPROTO;
        }
        return TURBO_EPROTO;
    }
    sender_bit = 1U << (uint32_t) (message->from - 1U);
    if ((node->sender_mask & sender_bit) != 0U) {
        node->message_error = TURBO_EPROTO;
        return TURBO_EPROTO;
    }
    node->sender_mask |= sender_bit;
    ++node->message_count;
    return TURBO_OK;
}

static void three_node_collect_inbound(void *context,
                                       int result,
                                       tr_raft_node_id_t peer_node_id)
{
    three_node_peer_t *node = (three_node_peer_t *) context;

    if (node == NULL || peer_node_id == 0U ||
        peer_node_id == node->node_id) {
        if (node != NULL && node->inbound_error == TURBO_OK) {
            node->inbound_error = TURBO_EPROTO;
        }
        return;
    }
    ++node->inbound_count;
    if (result != TURBO_OK && node->inbound_error == TURBO_OK) {
        node->inbound_error = result;
    }
}

static int three_node_resolve_endpoint(
    void *context,
    tr_raft_node_id_t peer_node_id,
    tr_raft_coronet_endpoint_t *out_endpoint)
{
    static const char connect_host[] = "127.0.0.1";
    static const char request_hosts[THREE_NODE_COUNT][12] = {
        "node-1.mesh",
        "node-2.mesh",
        "node-3.mesh"
    };
    three_node_peer_t *local = (three_node_peer_t *) context;
    three_node_peer_t *peer;

    if (local == NULL || out_endpoint == NULL ||
        peer_node_id <= local->node_id) {
        return TURBO_EINVAL;
    }
    peer = three_node_find(local->cluster, peer_node_id);
    if (peer == NULL || peer->port == 0U) {
        return TURBO_EINVAL;
    }
    memcpy(out_endpoint->connect_host, connect_host, sizeof(connect_host));
    memcpy(out_endpoint->request_host, request_hosts[peer_node_id - 1U],
           sizeof(request_hosts[0]));
    out_endpoint->port = peer->port;
    return TURBO_OK;
}

static int three_node_retry_io(void *context, int error_code)
{
    (void) context;
    return error_code == TURBO_EIO;
}

static void three_node_step_service(coro_t *coroutine, void *context)
{
    three_node_peer_t *node = (three_node_peer_t *) context;

    (void) coroutine;
    node->step_call_result = tr_raft_coronet_peer_service_step(
        node->service, 1U, &node->step_result);
}

static int three_node_connections_complete(
    const three_node_cluster_t *cluster)
{
    return cluster->nodes[0].step_call_result != TURBO_EBUSY &&
           cluster->nodes[1].step_call_result != TURBO_EBUSY;
}

static int three_node_messages_complete(
    const three_node_cluster_t *cluster)
{
    size_t index;

    if (!cluster->send_complete) {
        return 0;
    }
    if (cluster->send_result != TURBO_OK) {
        return 1;
    }
    for (index = 0U; index < THREE_NODE_COUNT; ++index) {
        if (cluster->nodes[index].message_count != THREE_NODE_PEER_COUNT) {
            return 0;
        }
    }
    return 1;
}

static void three_node_send_all(coro_t *coroutine, void *context)
{
    three_node_cluster_t *cluster = (three_node_cluster_t *) context;
    size_t local_index;
    tr_raft_node_id_t peer_id;

    (void) coroutine;
    cluster->send_result = TURBO_OK;
    for (local_index = 0U; local_index < THREE_NODE_COUNT; ++local_index) {
        for (peer_id = 1U; peer_id <= THREE_NODE_COUNT; ++peer_id) {
            tr_raft_message_t message;
            int result;

            if (peer_id == local_index + 1U) {
                continue;
            }
            memset(&message, 0, sizeof(message));
            message.type = TR_RAFT_MSG_HEARTBEAT_REQUEST;
            message.from = local_index + 1U;
            message.to = peer_id;
            message.term = 1U;
            result = tr_raft_coronet_peer_service_enqueue(
                cluster->nodes[local_index].service, &message);
            if (result != TURBO_OK) {
                cluster->send_result = result;
                cluster->send_complete = 1;
                return;
            }
        }
    }
    cluster->send_complete = 1;
}

static void three_node_run_until(three_node_cluster_t *cluster,
                                 int (*complete)(
                                     const three_node_cluster_t *))
{
    uint64_t deadline = turbo_monotonic_ms() + THREE_NODE_TIMEOUT_MS;

    while (!complete(cluster) && turbo_monotonic_ms() < deadline) {
        coro_context_run(cluster->context, TURBO_RUN_ONCE);
    }
}

static void three_node_initialize_peer_ids(three_node_peer_t *node)
{
    tr_raft_node_id_t candidate;
    size_t peer_index = 0U;

    for (candidate = 1U; candidate <= THREE_NODE_COUNT; ++candidate) {
        if (candidate != node->node_id) {
            node->peer_ids[peer_index++] = candidate;
        }
    }
}

static void three_node_create_service(three_node_peer_t *node)
{
    tr_raft_coronet_identity_entry_t identities[THREE_NODE_PEER_COUNT];
    tr_raft_coronet_peer_service_config_t config;
    size_t index;

    memset(&config, 0, sizeof(config));
    memset(identities, 0, sizeof(identities));
    for (index = 0U; index < THREE_NODE_PEER_COUNT; ++index) {
        tr_raft_node_id_t peer_id = node->peer_ids[index];
        const char *fingerprint =
            node->node_id == 3U && peer_id == 2U
                ? three_node_node2_client_fingerprint
                : three_node_fingerprints[peer_id - 1U];
        memcpy(identities[index].certificate_sha256, fingerprint,
               sizeof(identities[index].certificate_sha256));
        identities[index].node_id = peer_id;
    }
    config.manager.cluster_id = node->handshake.cluster_id;
    config.context = node->cluster->context;
    config.outbound_queue_capacity = 8U;
    config.manager.local_node_id = node->node_id;
    config.manager.peer_node_ids = node->peer_ids;
    config.manager.peer_count = THREE_NODE_PEER_COUNT;
    config.identity_entries = identities;
    config.identity_entry_count = THREE_NODE_PEER_COUNT;
    config.admit_owned_socket =
        tr_raft_coronet_peer_manager_admit_owned_socket;
    config.connect_outbound =
        tr_raft_coronet_peer_manager_connect_outbound;
    check_equal(tr_raft_coronet_peer_service_create(&config, &node->service),
                 TURBO_OK);
}

static void three_node_configure_inbound(three_node_peer_t *node)
{
    tr_raft_coronet_inbound_service_config_t config;

    memset(&config, 0, sizeof(config));
    config.admission.handshake.handshake = &node->handshake;
    config.admission.handshake.timeout_ms = THREE_NODE_SOCKET_TIMEOUT_MS;
    config.admission.direction = TR_RAFT_CORONET_CONNECTION_INBOUND;
    config.admission.first_outbound_message_id = 1U;
    config.admission.peer_idle_timeout_ms = THREE_NODE_SOCKET_TIMEOUT_MS;
    config.admission.on_message = three_node_receive_message;
    config.admission.message_context = node;
    config.on_result = three_node_collect_inbound;
    config.result_context = node;
    check_equal(tr_raft_coronet_peer_service_configure_inbound(
                     node->service, &config),
                 TURBO_OK);
}

static void three_node_start_listener(three_node_peer_t *node)
{
    turbo_tls_server_config_t config;

    memset(&config, 0, sizeof(config));
    check_equal(tr_test_reserve_loopback_port(&node->port), TURBO_OK);
    node->listener = coro_socket_create(node->cluster->context,
                                        CORO_SOCKET_TLS);
    check_not_null(node->listener);
    config.size = sizeof(config);
    config.cert_file = three_node_certificate(node->node_id);
    config.key_file = three_node_private_key(node->node_id);
    config.ca_file =
        node->node_id == 3U
            ? THREE_NODE_FIXTURE("three-node-client-ca.pem")
            : THREE_NODE_FIXTURE("ca.pem");
    config.client_auth = TURBO_TLS_CLIENT_AUTH_REQUIRED;
    check_equal(coro_socket_set_tls_server_config(node->listener, &config),
                 TURBO_OK);
    check_equal(coro_socket_listen_on(
                     node->listener, "127.0.0.1", node->port,
                     tr_raft_coronet_peer_service_handle_inbound,
                     node->service),
                 TURBO_OK);
}

static void three_node_add_outbound(three_node_peer_t *node,
                                    tr_raft_node_id_t peer_node_id)
{
    tr_raft_coronet_dial_scheduler_config_t config;

    memset(&config, 0, sizeof(config));
    config.outbound.context = node->cluster->context;
    config.outbound.connect_timeout_ms = THREE_NODE_SOCKET_TIMEOUT_MS;
    config.outbound.tls.ca_file = three_node_trust_for_peer(peer_node_id);
    if (node->node_id == 2U && peer_node_id == 3U) {
        config.outbound.tls.cert_file =
            THREE_NODE_FIXTURE("node2-client-cert.pem");
        config.outbound.tls.key_file =
            THREE_NODE_FIXTURE("node2-client-key.pem");
    } else {
        config.outbound.tls.cert_file = three_node_certificate(node->node_id);
        config.outbound.tls.key_file = three_node_private_key(node->node_id);
    }
    config.outbound.tls.verify_peer = 1;
    config.outbound.admission.handshake.handshake = &node->handshake;
    config.outbound.admission.handshake.timeout_ms =
        THREE_NODE_SOCKET_TIMEOUT_MS;
    config.outbound.admission.direction =
        TR_RAFT_CORONET_CONNECTION_OUTBOUND;
    config.outbound.admission.expected_peer_node_id = peer_node_id;
    config.outbound.admission.first_outbound_message_id = 1U;
    config.outbound.admission.peer_idle_timeout_ms =
        THREE_NODE_SOCKET_TIMEOUT_MS;
    config.outbound.admission.on_message = three_node_receive_message;
    config.outbound.admission.message_context = node;
    config.resolve_endpoint = three_node_resolve_endpoint;
    config.resolve_context = node;
    config.is_retryable = three_node_retry_io;
    config.initial_retry_delay_ms = 1U;
    config.max_retry_delay_ms = 1U;
    config.max_attempts = 1U;
    check_equal(tr_raft_coronet_peer_service_add_outbound(
                     node->service, &config),
                 TURBO_OK);
}

spec("raft CoroNet three-node mTLS mesh")
{
    it("routes bidirectional messages across every authenticated session")
    {
        three_node_cluster_t cluster;
        size_t local_index;
        uint64_t deadline;

        memset(&cluster, 0, sizeof(cluster));
        cluster.send_result = TURBO_EBUSY;
        cluster.context = coro_context_create(NULL);
        check_not_null(cluster.context);
        for (local_index = 0U; local_index < THREE_NODE_COUNT;
             ++local_index) {
            three_node_peer_t *node = &cluster.nodes[local_index];
            node->cluster = &cluster;
            node->node_id = local_index + 1U;
            node->step_call_result = TURBO_EBUSY;
            node->handshake = three_node_handshake(node->node_id);
            three_node_initialize_peer_ids(node);
            three_node_create_service(node);
            three_node_configure_inbound(node);
            three_node_start_listener(node);
        }

        three_node_add_outbound(&cluster.nodes[0], 2U);
        three_node_add_outbound(&cluster.nodes[0], 3U);
        three_node_add_outbound(&cluster.nodes[1], 3U);
        cluster.nodes[2].step_call_result = TURBO_OK;
        check_equal(coro_context_spawn(cluster.context,
                                        three_node_step_service,
                                        &cluster.nodes[0]),
                     TURBO_OK);
        check_equal(coro_context_spawn(cluster.context,
                                        three_node_step_service,
                                        &cluster.nodes[1]),
                     TURBO_OK);
        three_node_run_until(&cluster, three_node_connections_complete);

        check_equal(cluster.nodes[0].step_call_result, TURBO_OK);
        check_equal(cluster.nodes[1].step_call_result, TURBO_OK);
        check_equal(cluster.nodes[0].step_result.newly_connected_count, 2);
        check_equal(cluster.nodes[1].step_result.newly_connected_count, 1);
        check_equal(cluster.nodes[0].inbound_count, 0U);
        check_equal(cluster.nodes[1].inbound_count, 1U);
        check_equal(cluster.nodes[2].inbound_count, 2U);
        for (local_index = 0U; local_index < THREE_NODE_COUNT;
             ++local_index) {
            check_equal(cluster.nodes[local_index].inbound_error, TURBO_OK);
        }

        check_equal(coro_context_spawn(cluster.context,
                                        three_node_send_all,
                                        &cluster),
                     TURBO_OK);
        three_node_run_until(&cluster, three_node_messages_complete);

        check_equal(cluster.send_result, TURBO_OK);
        check_equal(cluster.nodes[0].sender_mask, 6);
        check_equal(cluster.nodes[1].sender_mask, 5);
        check_equal(cluster.nodes[2].sender_mask, 3);
        for (local_index = 0U; local_index < THREE_NODE_COUNT;
             ++local_index) {
            check_equal(cluster.nodes[local_index].message_error, TURBO_OK);
            coro_socket_destroy(cluster.nodes[local_index].listener);
            check_equal(tr_raft_coronet_peer_service_stop(
                             cluster.nodes[local_index].service),
                         TURBO_OK);
        }
        deadline = turbo_monotonic_ms() + THREE_NODE_SOCKET_TIMEOUT_MS;
        for (;;) {
            int pumps_stopped = 1;
            for (local_index = 0U; local_index < THREE_NODE_COUNT;
                 ++local_index) {
                tr_raft_coronet_peer_service_status_t status;
                check_equal(tr_raft_coronet_peer_service_get_status(
                                 cluster.nodes[local_index].service,
                                 &status),
                             TURBO_OK);
                if (status.active_operation_count != 0U) {
                    pumps_stopped = 0;
                }
            }
            if (pumps_stopped || turbo_monotonic_ms() >= deadline) {
                break;
            }
            coro_context_run(cluster.context, TURBO_RUN_NOWAIT);
        }
        for (local_index = 0U; local_index < THREE_NODE_COUNT;
             ++local_index) {
            check_equal(tr_raft_coronet_peer_service_destroy(
                             cluster.nodes[local_index].service),
                         TURBO_OK);
        }
        deadline = turbo_monotonic_ms() + THREE_NODE_SOCKET_TIMEOUT_MS;
        while (coro_context_alive(cluster.context) &&
               turbo_monotonic_ms() < deadline) {
            coro_context_run(cluster.context, TURBO_RUN_NOWAIT);
        }
        coro_context_destroy(cluster.context);
    }
}
