#include <turboraft/raft_flowmq_peer_service.h>

#include <cnet/cnet.h>
#include <salts_error.h>
#include <salts_thread.h>
#include <tinytest.h>

#include <stdio.h>
#include <string.h>

#define TR_FLOWMQ_TEST_ENDPOINT_CAPACITY 128U
#define TR_FLOWMQ_TEST_PATH_CAPACITY 512U
#define TR_FLOWMQ_TEST_QUEUE_CAPACITY 8U
#define TR_FLOWMQ_TEST_HWM_BYTES (1024U * 1024U)
#define TR_FLOWMQ_TEST_PROGRESS_LIMIT 2000U
#define TR_FLOWMQ_TEST_DRAIN_STEPS 16U
#define TR_FLOWMQ_TEST_CNET_CONNECTION_CAPACITY 1U
#define TR_FLOWMQ_TEST_CNET_COMMAND_CAPACITY 8U
#define TR_FLOWMQ_TEST_CNET_REQUEST_CAPACITY 4U
#define TR_FLOWMQ_TEST_CNET_EVENT_CAPACITY 8U
#define TR_FLOWMQ_TEST_CNET_IO_BYTES 1024U
#define TR_FLOWMQ_TEST_CNET_TIMEOUT_MS 2000U
#define TR_FLOWMQ_TEST_CNET_STOP_TIMEOUT_MS 5000U

typedef struct tr_flowmq_message_capture {
    tr_raft_group_id_t group_id;
    tr_raft_message_t message;
    size_t count;
} tr_flowmq_message_capture_t;

typedef struct tr_flowmq_live_pair {
    tr_raft_flowmq_peer_service_t *node1;
    tr_raft_flowmq_peer_service_t *node2;
    tr_flowmq_message_capture_t node1_capture;
    tr_flowmq_message_capture_t node2_capture;
    char node1_endpoint[TR_FLOWMQ_TEST_ENDPOINT_CAPACITY];
    char node2_endpoint[TR_FLOWMQ_TEST_ENDPOINT_CAPACITY];
    int node1_started;
    int node2_started;
} tr_flowmq_live_pair_t;

typedef struct tr_flowmq_tls_probe {
    cnet_client client;
    cnet_connection connection;
    int initialized;
    int connected;
    int terminal;
    int failed;
    int receive_status;
    int failure_status;
    const char *failure_stage;
} tr_flowmq_tls_probe_t;

typedef struct tr_flowmq_live_service_config {
    tr_raft_node_id_t local_node_id;
    const char *local_identity;
    const char *bind_endpoint;
    tr_raft_flowmq_tls_config_t listener_tls;
    tr_raft_node_id_t peer_node_id;
    const char *peer_identity;
    const char *peer_endpoint;
    tr_raft_flowmq_tls_config_t peer_tls;
    tr_flowmq_message_capture_t *capture;
} tr_flowmq_live_service_config_t;

static int ignore_payload(void *context,
                          const tr_raft_transport_payload_t *payload)
{
    (void)context;
    (void)payload;
    return SALTS_OK;
}

static tr_raft_handshake_config_t protocol_config(
    tr_raft_node_id_t local_node_id)
{
    tr_raft_handshake_config_t config;
    size_t index;

    memset(&config, 0, sizeof(config));
    for (index = 0U; index < sizeof(config.cluster_id.bytes); ++index) {
        config.cluster_id.bytes[index] = (uint8_t)(11U + index);
    }
    config.local_node_id = local_node_id;
    config.process_incarnation.bytes[0] = (uint8_t)local_node_id;
    config.config_epoch = 1U;
    config.feature_bits = 0U;
    config.wire_major_min = TR_RAFT_HANDSHAKE_WIRE_MAJOR;
    config.wire_major_max = TR_RAFT_HANDSHAKE_WIRE_MAJOR;
    config.wire_minor_min = TR_RAFT_HANDSHAKE_WIRE_MINOR;
    config.wire_minor_max = TR_RAFT_HANDSHAKE_WIRE_MINOR;
    config.max_frame_size = TR_RAFT_WIRE_MAX_FRAME_SIZE;
    config.max_snapshot_chunk_size = TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES;
    return config;
}

static tr_raft_flowmq_peer_service_t *make_service(
    size_t total_capacity,
    size_t max_groups,
    size_t per_group_capacity)
{
    tr_raft_flowmq_peer_service_config_t config;
    tr_raft_flowmq_peer_config_t peer;
    tr_raft_handshake_result_t handshake;
    tr_raft_flowmq_peer_service_t *service = NULL;

    memset(&config, 0, sizeof(config));
    memset(&peer, 0, sizeof(peer));
    memset(&handshake, 0, sizeof(handshake));
    config.protocol = protocol_config(1U);
    peer.node_id = 2U;
    handshake.complete = 1;
    handshake.cluster_id = config.protocol.cluster_id;
    handshake.local_node_id = 1U;
    handshake.peer_node_id = 2U;
    handshake.peer_process_incarnation.bytes[0] = 1U;
    handshake.feature_bits = 0U;
    handshake.wire_major = TR_RAFT_HANDSHAKE_WIRE_MAJOR;
    handshake.wire_minor = TR_RAFT_HANDSHAKE_WIRE_MINOR;
    handshake.max_frame_size = TR_RAFT_WIRE_MAX_FRAME_SIZE;
    handshake.max_snapshot_chunk_size =
        TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES;
    peer.handshake = &handshake;
    peer.identity = "node-2";
    peer.endpoint = "tcp://127.0.0.1:57992";
    config.bind_endpoint = "tcp://127.0.0.1:0";
    config.local_identity = "node-1";
    config.peers = &peer;
    config.peer_count = 1U;
    config.outbound_limits.total_item_capacity = total_capacity;
    config.outbound_limits.total_data_bytes =
        TR_RAFT_FLOWMQ_RECOMMENDED_INFLIGHT_DATA_BYTES;
    config.outbound_limits.max_active_groups = max_groups;
    config.outbound_limits.per_group_item_capacity = per_group_capacity;
    config.outbound_limits.per_group_data_bytes =
        TR_RAFT_FLOWMQ_RECOMMENDED_INFLIGHT_DATA_BYTES;
    config.max_send_batch_items =
        TR_RAFT_FLOWMQ_RECOMMENDED_SEND_BATCH_ITEMS;
    config.max_receive_batch_items =
        TR_RAFT_FLOWMQ_RECOMMENDED_RECEIVE_BATCH_ITEMS;
    config.send_hwm_messages = total_capacity;
    config.receive_hwm_messages = total_capacity;
    config.send_hwm_bytes = 8U * 1024U * 1024U;
    config.receive_hwm_bytes = 8U * 1024U * 1024U;
    config.reconnect_initial_ms = 100U;
    config.reconnect_max_ms = 1000U;
    config.on_payload = ignore_payload;
    check_equal(tr_raft_flowmq_peer_service_create(&config, &service),
                SALTS_OK);
    return service;
}

static int capture_payload(void *context,
                           const tr_raft_transport_payload_t *payload)
{
    tr_flowmq_message_capture_t *capture =
        (tr_flowmq_message_capture_t *)context;

    if (capture == NULL || payload == NULL ||
        payload->kind != TR_RAFT_WIRE_PAYLOAD_RAFT ||
        payload->group_id == 0U) {
        return SALTS_EINVAL;
    }
    capture->group_id = payload->group_id;
    capture->message = payload->data.raft;
    capture->count++;
    return SALTS_OK;
}

static tr_raft_handshake_result_t handshake_result(
    const tr_raft_handshake_config_t *protocol,
    tr_raft_node_id_t peer_node_id)
{
    tr_raft_handshake_result_t result;

    memset(&result, 0, sizeof(result));
    result.complete = 1;
    result.cluster_id = protocol->cluster_id;
    result.local_node_id = protocol->local_node_id;
    result.peer_node_id = peer_node_id;
    result.peer_process_incarnation.bytes[0] = (uint8_t)peer_node_id;
    result.peer_config_epoch = 1U;
    result.feature_bits = 0U;
    result.wire_major = TR_RAFT_HANDSHAKE_WIRE_MAJOR;
    result.wire_minor = TR_RAFT_HANDSHAKE_WIRE_MINOR;
    result.max_frame_size = TR_RAFT_WIRE_MAX_FRAME_SIZE;
    result.max_snapshot_chunk_size =
        TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES;
    return result;
}

static int reserve_loopback_endpoint(char *output,
                                     size_t output_capacity,
                                     int tls)
{
    flowmq_ctx_t *context = NULL;
    flowmq_socket_t *socket = NULL;
    size_t endpoint_size = 0U;
    int result = SALTS_OK;
    int close_result;

    if (output == NULL || output_capacity == 0U) {
        return SALTS_EINVAL;
    }
    context = flowmq_ctx_new();
    if (context == NULL) {
        return SALTS_ENOMEM;
    }
    socket = flowmq_socket(context, FLOWMQ_PAIR);
    if (socket == NULL) {
        flowmq_ctx_term(context);
        return SALTS_ENOMEM;
    }
    result = flowmq_bind(socket, "tcp://127.0.0.1:0");
    if (result == SALTS_OK) {
        result = flowmq_last_endpoint(socket, output, output_capacity,
                                      &endpoint_size);
    }
    if (result == SALTS_OK &&
        (endpoint_size == 0U ||
         strncmp(output, "tcp://", sizeof("tcp://") - 1U) != 0)) {
        result = SALTS_EPROTO;
    }
    close_result = flowmq_close(socket);
    if (result == SALTS_OK && close_result != SALTS_OK) {
        result = close_result;
    }
    close_result = flowmq_ctx_term(context);
    if (result == SALTS_OK && close_result != SALTS_OK) {
        result = close_result;
    }
    if (result == SALTS_OK && tls) {
        memcpy(output, "tls", sizeof("tls") - 1U);
    }
    return result;
}

static int fixture_path(char *output,
                        size_t output_capacity,
                        const char *name)
{
    int count;

    if (output == NULL || output_capacity == 0U || name == NULL) {
        return SALTS_EINVAL;
    }
    count = snprintf(output, output_capacity, "%s/%s",
                     TURBORAFT_TEST_FIXTURE_DIR, name);
    return count < 0 || (size_t)count >= output_capacity ? SALTS_ERANGE
                                                         : SALTS_OK;
}

static cnet_client_config tls_probe_config(void)
{
    const cnet_client_config config = {
#if defined(_WIN32)
        .backend = NATIVE_IO_BACKEND_IOCP,
#elif defined(__linux__)
        .backend = NATIVE_IO_BACKEND_EPOLL,
#else
        .backend = NATIVE_IO_BACKEND_KQUEUE,
#endif
        .connection_capacity = TR_FLOWMQ_TEST_CNET_CONNECTION_CAPACITY,
        .command_capacity = TR_FLOWMQ_TEST_CNET_COMMAND_CAPACITY,
        .request_capacity = TR_FLOWMQ_TEST_CNET_REQUEST_CAPACITY,
        .completion_batch_capacity = TR_FLOWMQ_TEST_CNET_REQUEST_CAPACITY,
        .event_capacity = TR_FLOWMQ_TEST_CNET_EVENT_CAPACITY,
        .max_send_bytes = TR_FLOWMQ_TEST_CNET_IO_BYTES,
        .receive_buffer_bytes = TR_FLOWMQ_TEST_CNET_IO_BYTES,
        .connect_timeout_ms = TR_FLOWMQ_TEST_CNET_TIMEOUT_MS,
        .read_timeout_ms = TR_FLOWMQ_TEST_CNET_TIMEOUT_MS,
        .write_timeout_ms = TR_FLOWMQ_TEST_CNET_TIMEOUT_MS,
        .tls_io_buffer_bytes = CNET_TLS_MIN_IO_BUFFER_BYTES,
        .tls_handshake_timeout_ms = TR_FLOWMQ_TEST_CNET_TIMEOUT_MS};

    return config;
}

static void tls_probe_on_state(void *context,
                               cnet_connection connection,
                               cnet_connection_state state,
                               const cnet_error *error)
{
    tr_flowmq_tls_probe_t *probe = (tr_flowmq_tls_probe_t *)context;

    probe->connection = connection;
    if (state == CNET_CONNECTION_CONNECTED) {
        probe->connected = 1;
        probe->receive_status = cnet_receive(&probe->client, connection, 1U);
    } else if (state == CNET_CONNECTION_CLOSED ||
               state == CNET_CONNECTION_FAILED) {
        probe->terminal = 1;
        if (state == CNET_CONNECTION_FAILED || error != NULL) {
            probe->failed = 1;
            if (error != NULL) {
                probe->failure_status = error->status;
                probe->failure_stage = error->stage;
            }
        }
    }
}

static void tls_probe_on_receive(void *context,
                                 cnet_connection connection,
                                 const cnet_receive_view *view)
{
    tr_flowmq_tls_probe_t *probe = (tr_flowmq_tls_probe_t *)context;

    (void)connection;
    (void)view;
    probe->terminal = 1;
    probe->failed = 1;
    probe->failure_status = SALTS_EPROTO;
    probe->failure_stage = "unexpected-data";
}

static int open_unauthenticated_tls_probe(const char *endpoint,
                                          tr_flowmq_tls_probe_t *probe)
{
    cnet_client_config client_config = tls_probe_config();
    cnet_tls_client_config tls_config;
    cnet_connect_options options;
    char ca_path[TR_FLOWMQ_TEST_PATH_CAPACITY];
    int result;

    if (endpoint == NULL || probe == NULL) {
        return SALTS_EINVAL;
    }
    memset(probe, 0, sizeof(*probe));
    probe->receive_status = SALTS_EBUSY;
    memset(&tls_config, 0, sizeof(tls_config));
    memset(&options, 0, sizeof(options));
    result = fixture_path(ca_path, sizeof(ca_path), "ca.pem");
    if (result != SALTS_OK) {
        return result;
    }
    result = cnet_client_init(&probe->client, &client_config);
    if (result != SALTS_OK) {
        return result;
    }
    probe->initialized = 1;
    tls_config.size = sizeof(tls_config);
    tls_config.ca_file = ca_path;
    tls_config.server_name = "node-2.mesh";
    options.uri = endpoint;
    options.observer.on_state = tls_probe_on_state;
    options.observer.on_receive = tls_probe_on_receive;
    options.observer.user = probe;
    options.tls = &tls_config;
    return cnet_connect(&probe->client, &options, &probe->connection);
}

static int close_tls_probe(tr_flowmq_tls_probe_t *probe)
{
    int first_error = SALTS_OK;
    int result;

    if (probe == NULL) {
        return SALTS_EINVAL;
    }
    if (probe->initialized) {
        result = cnet_client_stop(&probe->client,
                                  TR_FLOWMQ_TEST_CNET_STOP_TIMEOUT_MS);
        if (result != SALTS_OK) {
            first_error = result;
        }
        result = cnet_client_destroy(&probe->client);
        if (first_error == SALTS_OK && result != SALTS_OK) {
            first_error = result;
        }
    }
    memset(probe, 0, sizeof(*probe));
    return first_error;
}

static int create_live_service(
    const tr_flowmq_live_service_config_t *live,
    tr_raft_flowmq_peer_service_t **out_service)
{
    tr_raft_flowmq_peer_service_config_t config;
    tr_raft_flowmq_peer_config_t peer;
    tr_raft_handshake_result_t handshake;

    if (live == NULL || out_service == NULL) {
        return SALTS_EINVAL;
    }
    memset(&config, 0, sizeof(config));
    memset(&peer, 0, sizeof(peer));
    config.protocol = protocol_config(live->local_node_id);
    handshake = handshake_result(&config.protocol, live->peer_node_id);
    peer.node_id = live->peer_node_id;
    peer.handshake = &handshake;
    peer.identity = live->peer_identity;
    peer.endpoint = live->peer_endpoint;
    peer.tls = live->peer_tls;
    config.bind_endpoint = live->bind_endpoint;
    config.local_identity = live->local_identity;
    config.tls = live->listener_tls;
    config.peers = &peer;
    config.peer_count = 1U;
    config.outbound_limits.total_item_capacity =
        TR_FLOWMQ_TEST_QUEUE_CAPACITY;
    config.outbound_limits.total_data_bytes =
        TR_RAFT_FLOWMQ_RECOMMENDED_INFLIGHT_DATA_BYTES;
    config.outbound_limits.max_active_groups = 4U;
    config.outbound_limits.per_group_item_capacity =
        TR_FLOWMQ_TEST_QUEUE_CAPACITY;
    config.outbound_limits.per_group_data_bytes =
        TR_RAFT_FLOWMQ_RECOMMENDED_INFLIGHT_DATA_BYTES;
    config.max_send_batch_items =
        TR_RAFT_FLOWMQ_RECOMMENDED_SEND_BATCH_ITEMS;
    config.max_receive_batch_items =
        TR_RAFT_FLOWMQ_RECOMMENDED_RECEIVE_BATCH_ITEMS;
    config.send_hwm_messages = TR_FLOWMQ_TEST_QUEUE_CAPACITY;
    config.receive_hwm_messages = TR_FLOWMQ_TEST_QUEUE_CAPACITY;
    config.send_hwm_bytes = TR_FLOWMQ_TEST_HWM_BYTES;
    config.receive_hwm_bytes = TR_FLOWMQ_TEST_HWM_BYTES;
    config.reconnect_initial_ms = 1U;
    config.reconnect_max_ms = 16U;
    config.on_payload = capture_payload;
    config.payload_context = live->capture;
    return tr_raft_flowmq_peer_service_create(&config, out_service);
}

static int close_live_pair(tr_flowmq_live_pair_t *pair)
{
    int first_error = SALTS_OK;
    int result;

    if (pair == NULL) {
        return SALTS_EINVAL;
    }
    if (pair->node1_started) {
        result = tr_raft_flowmq_peer_service_stop(pair->node1);
        if (first_error == SALTS_OK && result != SALTS_OK) {
            first_error = result;
        }
    }
    if (pair->node2_started) {
        result = tr_raft_flowmq_peer_service_stop(pair->node2);
        if (first_error == SALTS_OK && result != SALTS_OK) {
            first_error = result;
        }
    }
    if (pair->node1 != NULL) {
        result = tr_raft_flowmq_peer_service_destroy(pair->node1);
        if (first_error == SALTS_OK && result != SALTS_OK) {
            first_error = result;
        }
    }
    if (pair->node2 != NULL) {
        result = tr_raft_flowmq_peer_service_destroy(pair->node2);
        if (first_error == SALTS_OK && result != SALTS_OK) {
            first_error = result;
        }
    }
    memset(pair, 0, sizeof(*pair));
    return first_error;
}

static int open_live_pair(tr_flowmq_live_pair_t *pair,
                          int provide_client_certificate)
{
    char ca_path[TR_FLOWMQ_TEST_PATH_CAPACITY];
    char node1_cert_path[TR_FLOWMQ_TEST_PATH_CAPACITY];
    char node1_key_path[TR_FLOWMQ_TEST_PATH_CAPACITY];
    char node2_cert_path[TR_FLOWMQ_TEST_PATH_CAPACITY];
    char node2_key_path[TR_FLOWMQ_TEST_PATH_CAPACITY];
    tr_flowmq_live_service_config_t node1;
    tr_flowmq_live_service_config_t node2;
    const char *stage = "reserve-node1";
    int result;

    if (pair == NULL) {
        return SALTS_EINVAL;
    }
    memset(pair, 0, sizeof(*pair));
    memset(&node1, 0, sizeof(node1));
    memset(&node2, 0, sizeof(node2));
    result = reserve_loopback_endpoint(
        pair->node1_endpoint, sizeof(pair->node1_endpoint), 0);
    if (result == SALTS_OK) {
        stage = "reserve-node2";
        result = reserve_loopback_endpoint(
            pair->node2_endpoint, sizeof(pair->node2_endpoint), 1);
    }
    if (result == SALTS_OK) {
        stage = "ca-path";
        result = fixture_path(ca_path, sizeof(ca_path), "ca.pem");
    }
    if (result == SALTS_OK) {
        stage = "node1-cert-path";
        result = fixture_path(node1_cert_path, sizeof(node1_cert_path),
                              "node1-cert.pem");
    }
    if (result == SALTS_OK) {
        stage = "node1-key-path";
        result = fixture_path(node1_key_path, sizeof(node1_key_path),
                              "node1-key.pem");
    }
    if (result == SALTS_OK) {
        stage = "node2-cert-path";
        result = fixture_path(node2_cert_path, sizeof(node2_cert_path),
                              "node2-cert.pem");
    }
    if (result == SALTS_OK) {
        stage = "node2-key-path";
        result = fixture_path(node2_key_path, sizeof(node2_key_path),
                              "node2-key.pem");
    }
    if (result != SALTS_OK) {
        return result;
    }

    node1.local_node_id = 1U;
    node1.local_identity = "node-1";
    node1.bind_endpoint = pair->node1_endpoint;
    node1.peer_node_id = 2U;
    node1.peer_identity = "node-2";
    node1.peer_endpoint = pair->node2_endpoint;
    node1.peer_tls.ca_file = ca_path;
    node1.peer_tls.server_name = "node-2.mesh";
    if (provide_client_certificate) {
        node1.peer_tls.cert_file = node1_cert_path;
        node1.peer_tls.key_file = node1_key_path;
    }
    node1.capture = &pair->node1_capture;

    node2.local_node_id = 2U;
    node2.local_identity = "node-2";
    node2.bind_endpoint = pair->node2_endpoint;
    node2.listener_tls.ca_file = ca_path;
    node2.listener_tls.cert_file = node2_cert_path;
    node2.listener_tls.key_file = node2_key_path;
    node2.listener_tls.require_client_certificate = 1;
    node2.peer_node_id = 1U;
    node2.peer_identity = "node-1";
    node2.peer_endpoint = pair->node1_endpoint;
    node2.capture = &pair->node2_capture;

    stage = "create-node2";
    result = create_live_service(&node2, &pair->node2);
    if (result == SALTS_OK) {
        stage = "create-node1";
        result = create_live_service(&node1, &pair->node1);
    }
    if (result == SALTS_OK) {
        stage = "start-node2";
        result = tr_raft_flowmq_peer_service_start(pair->node2);
        pair->node2_started = result == SALTS_OK;
    }
    if (result == SALTS_OK) {
        stage = "start-node1";
        result = tr_raft_flowmq_peer_service_start(pair->node1);
        pair->node1_started = result == SALTS_OK;
    }
    if (result != SALTS_OK) {
        fprintf(stderr, "FlowMQ live pair stage=%s result=%d\n",
                stage, result);
        (void)close_live_pair(pair);
    }
    return result;
}

static int step_live_pair(
    tr_flowmq_live_pair_t *pair,
    tr_raft_flowmq_peer_service_step_result_t *node1_step,
    tr_raft_flowmq_peer_service_step_result_t *node2_step)
{
    int result = tr_raft_flowmq_peer_service_step(pair->node1, node1_step);

    return result != SALTS_OK
               ? result
               : tr_raft_flowmq_peer_service_step(pair->node2, node2_step);
}

static tr_raft_message_t heartbeat(void)
{
    tr_raft_message_t message;

    memset(&message, 0, sizeof(message));
    message.type = TR_RAFT_MSG_HEARTBEAT_REQUEST;
    message.from = 1U;
    message.to = 2U;
    message.term = 4U;
    return message;
}

spec("Raft FlowMQ caller-driven service")
{
    it("delivers one Raft frame over mutual TLS")
    {
        tr_flowmq_live_pair_t pair;
        tr_raft_flowmq_peer_service_step_result_t node1_step;
        tr_raft_flowmq_peer_service_step_result_t node2_step;
        tr_raft_flowmq_peer_service_status_t node1_status;
        tr_raft_flowmq_peer_service_status_t node2_status;
        tr_raft_message_t message = heartbeat();
        uint32_t progress;
        int result = open_live_pair(&pair, 1);

        memset(&node1_status, 0, sizeof(node1_status));
        memset(&node2_status, 0, sizeof(node2_status));
        check_equal(result, SALTS_OK);
        if (result == SALTS_OK) {
            message.term = 7U;
            result = tr_raft_flowmq_peer_service_enqueue_group(pair.node1, 1U,
                                                               &message);
            check_equal(result, SALTS_OK);
        }
        for (progress = 0U;
             result == SALTS_OK && pair.node2_capture.count == 0U &&
             progress < TR_FLOWMQ_TEST_PROGRESS_LIMIT;
             ++progress) {
            memset(&node1_step, 0, sizeof(node1_step));
            memset(&node2_step, 0, sizeof(node2_step));
            result = step_live_pair(&pair, &node1_step, &node2_step);
            if (pair.node2_capture.count == 0U) {
                salts_sleep_ms(1U);
            }
        }
        check_equal(result, SALTS_OK);
        check_equal(pair.node2_capture.count, 1U);
        if (pair.node2_capture.count == 1U) {
            check_equal(pair.node2_capture.group_id, 1U);
            check_equal(pair.node2_capture.message.type,
                        TR_RAFT_MSG_HEARTBEAT_REQUEST);
            check_equal(pair.node2_capture.message.from, 1U);
            check_equal(pair.node2_capture.message.to, 2U);
            check_equal(pair.node2_capture.message.term, 7U);
            check_equal(pair.node2_capture.message.entry_count, 0U);
            check_equal(pair.node2_capture.message.leader_commit, 0U);
        }
        for (progress = 0U;
             result == SALTS_OK && progress < TR_FLOWMQ_TEST_DRAIN_STEPS;
             ++progress) {
            memset(&node1_step, 0, sizeof(node1_step));
            memset(&node2_step, 0, sizeof(node2_step));
            result = step_live_pair(&pair, &node1_step, &node2_step);
        }
        check_equal(result, SALTS_OK);
        check_equal(pair.node2_capture.count, 1U);
        if (result == SALTS_OK) {
            result = tr_raft_flowmq_peer_service_get_status(
                pair.node1, &node1_status);
            check_equal(result, SALTS_OK);
        }
        if (result == SALTS_OK) {
            result = tr_raft_flowmq_peer_service_get_status(
                pair.node2, &node2_status);
            check_equal(result, SALTS_OK);
        }
        if (result == SALTS_OK) {
            check_greater(node1_status.frames_sent, 0U);
            check_greater(node2_status.frames_received, 0U);
            check_equal(node1_status.last_error, SALTS_OK);
            check_equal(node2_status.last_error, SALTS_OK);
        }
        check_equal(close_live_pair(&pair), SALTS_OK);
    }

    it("rejects a peer without a client certificate")
    {
        tr_flowmq_live_pair_t pair;
        tr_flowmq_tls_probe_t probe;
        tr_raft_flowmq_peer_service_step_result_t node1_step;
        tr_raft_flowmq_peer_service_step_result_t node2_step;
        tr_raft_message_t message = heartbeat();
        size_t probe_events = 0U;
        uint32_t progress;
        int result;

        memset(&probe, 0, sizeof(probe));
        result = open_live_pair(&pair, 0);

        check_equal(result, SALTS_OK);
        if (result == SALTS_OK) {
            result = open_unauthenticated_tls_probe(pair.node2_endpoint,
                                                    &probe);
            check_equal(result, SALTS_OK);
        }
        if (result == SALTS_OK) {
            message.term = 7U;
            result = tr_raft_flowmq_peer_service_enqueue_group(pair.node1, 1U,
                                                               &message);
            check_equal(result, SALTS_OK);
        }
        for (progress = 0U;
             result == SALTS_OK && !probe.terminal &&
             pair.node2_capture.count == 0U &&
             progress < TR_FLOWMQ_TEST_PROGRESS_LIMIT;
             ++progress) {
            memset(&node1_step, 0, sizeof(node1_step));
            memset(&node2_step, 0, sizeof(node2_step));
            result = step_live_pair(&pair, &node1_step, &node2_step);
            if (result == SALTS_OK) {
                result = cnet_client_poll(&probe.client, 1U,
                                          &probe_events);
            }
        }
        check_equal(result, SALTS_OK);
        check_true(probe.terminal);
        check_true(probe.failed);
        check_true(probe.connected);
        check_equal(probe.receive_status, SALTS_OK);
        check_equal(probe.failure_status, SALTS_ECONNABORTED);
        check_not_null(probe.failure_stage);
        if (probe.failure_stage != NULL) {
            check_equal(strcmp(probe.failure_stage, "read"), 0);
        }
        check_equal(pair.node2_capture.count, 0U);
        check_equal(close_tls_probe(&probe), SALTS_OK);
        check_equal(close_live_pair(&pair), SALTS_OK);
    }

    it("copies messages into a bounded peer FIFO")
    {
        tr_raft_flowmq_peer_service_t *service = make_service(2U, 1U, 2U);
        tr_raft_flowmq_peer_service_status_t status;
        tr_raft_message_t message = heartbeat();

        check_equal(tr_raft_flowmq_peer_service_enqueue_group(service, 1U, &message),
                    SALTS_OK);
        check_equal(tr_raft_flowmq_peer_service_enqueue_group(service, 1U, &message),
                    SALTS_OK);
        check_equal(tr_raft_flowmq_peer_service_enqueue_group(service, 1U, &message),
                    SALTS_ENOSPC);
        check_equal(tr_raft_flowmq_peer_service_get_status(service, &status),
                    SALTS_OK);
        check_equal(status.queued_payload_count, 2U);
        check_equal(status.outbound_limits.total_item_capacity, 2U);
        check_equal(tr_raft_flowmq_peer_service_destroy(service), SALTS_OK);
    }

    it("enforces per-group queue capacity and active group limits")
    {
        tr_raft_flowmq_peer_service_t *service =
            make_service(6U, 2U, 2U);
        tr_raft_flowmq_peer_service_status_t status;
        tr_raft_transport_group_queue_status_t group_status;
        tr_raft_message_t message = heartbeat();

        check_equal(tr_raft_flowmq_peer_service_enqueue_group(
                        service, 10U, &message),
                    SALTS_OK);
        message.term = 5U;
        check_equal(tr_raft_flowmq_peer_service_enqueue_group(
                        service, 10U, &message),
                    SALTS_OK);
        message.term = 6U;
        check_equal(tr_raft_flowmq_peer_service_enqueue_group(
                        service, 10U, &message),
                    SALTS_ENOSPC);

        check_equal(tr_raft_flowmq_peer_service_enqueue_group(
                        service, 20U, &message),
                    SALTS_OK);
        check_equal(tr_raft_flowmq_peer_service_enqueue_group(
                        service, 30U, &message),
                    SALTS_ENOSPC);

        check_equal(tr_raft_flowmq_peer_service_get_status(service, &status),
                    SALTS_OK);
        check_equal(status.active_group_count, 2U);
        check_equal(status.queued_payload_count, 3U);
        check_equal(status.outbound_limits.max_active_groups, 2U);
        check_equal(status.outbound_limits.per_group_item_capacity, 2U);

        check_equal(tr_raft_flowmq_peer_service_get_group_status(
                        service, 2U, 10U, &group_status),
                    SALTS_OK);
        check_equal(group_status.group_id, 10U);
        check_equal(group_status.queued_payload_count, 2U);
        check_equal(group_status.queued_data_bytes, 0U);
        check_equal(tr_raft_flowmq_peer_service_get_group_status(
                        service, 2U, 30U, &group_status),
                    SALTS_ENOENT);

        check_equal(tr_raft_flowmq_peer_service_destroy(service), SALTS_OK);
    }

    it("has explicit start stop ownership")
    {
        tr_raft_flowmq_peer_service_t *service = make_service(4U, 4U, 4U);
        tr_raft_flowmq_peer_service_step_result_t step;
        tr_raft_message_t message = heartbeat();

        check_equal(tr_raft_flowmq_peer_service_start(service), SALTS_OK);
        check_equal(tr_raft_flowmq_peer_service_destroy(service), SALTS_EBUSY);
        check_equal(tr_raft_flowmq_peer_service_enqueue_group(service, 1U, &message),
                    SALTS_OK);
        memset(&step, 0, sizeof(step));
        check_equal(tr_raft_flowmq_peer_service_step(service, &step), SALTS_OK);
        check_equal(tr_raft_flowmq_peer_service_stop(service), SALTS_OK);
        check_equal(tr_raft_flowmq_peer_service_enqueue_group(service, 1U, &message),
                    SALTS_EPIPE);
        check_equal(tr_raft_flowmq_peer_service_destroy(service), SALTS_OK);
    }
}
