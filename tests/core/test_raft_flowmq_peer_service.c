#include <turboraft/raft_flowmq_peer_service.h>
#include <turboraft/raft_data_stream.h>

#include <platform.h>
#include <tinytest.h>
#include <turbo_error.h>

#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "raft_coronet_mtls_test_support.h"

#define FLOWMQ_TEST_FIXTURE(name) TURBORAFT_TEST_FIXTURE_DIR "/" name
#define FLOWMQ_TEST_TIMEOUT_MS 10000U
#define FLOWMQ_TEST_OUTBOUND_CAPACITY 4U
#define FLOWMQ_TEST_SEND_BATCH_ITEMS 4U
enum {
  FLOWMQ_TEST_STREAM_BYTES = 256U * 1024U,
  FLOWMQ_TEST_STREAM_ID = 44U,
  FLOWMQ_TEST_STREAM_TERM = 12U
};

typedef struct flowmq_stream_sink {
  size_t used;
  size_t writes;
  int committed;
  int aborted;
} flowmq_stream_sink_t;

typedef struct flowmq_node {
  tr_raft_node_id_t node_id;
  const char *identity;
  const char *peer_identity;
  const char *certificate;
  const char *private_key;
  const char *server_name;
  unsigned short port;
  flowmq_coronet_tls_server_config_t router_tls;
  flowmq_coronet_tls_client_config_t dealer_tls;
  flowmq_router_endpoint_config_t router_endpoint;
  flowmq_connect_endpoint_config_t dealer_endpoint;
  tr_raft_flowmq_peer_config_t peer;
  tr_raft_flowmq_peer_service_t *service;
  tr_raft_data_stream_sender_t *stream_sender;
  tr_raft_data_stream_receiver_t *stream_receiver;
  flowmq_stream_sink_t stream_sink;
  size_t received_count;
  tr_raft_node_id_t received_from;
  tr_raft_term_t received_terms[FLOWMQ_TEST_OUTBOUND_CAPACITY];
  size_t received_stream_chunks;
  size_t received_stream_bytes;
  size_t received_stream_acks;
} flowmq_node_t;

typedef struct flowmq_cluster {
  flowmq_node_t nodes[2];
  int backpressure_ok;
  size_t payload_batches_sent;
  size_t payload_frames_sent;
  int driver_result;
} flowmq_cluster_t;

static tr_raft_handshake_config_t flowmq_handshake(tr_raft_node_id_t node_id) {
  tr_raft_handshake_config_t config;
  size_t index;

  memset(&config, 0, sizeof(config));
  for (index = 0U; index < sizeof(config.cluster_id.bytes); ++index) {
    config.cluster_id.bytes[index] = (uint8_t)(10U + index);
    config.process_incarnation.bytes[index] = (uint8_t)(30U + node_id * 20U + index);
  }
  config.local_node_id = node_id;
  config.config_epoch = 1U;
  config.feature_bits = TR_RAFT_HANDSHAKE_FEATURE_CURRENT;
  config.wire_major_min = TR_RAFT_HANDSHAKE_WIRE_MAJOR;
  config.wire_major_max = TR_RAFT_HANDSHAKE_WIRE_MAJOR;
  config.wire_minor_min = TR_RAFT_HANDSHAKE_WIRE_MINOR;
  config.wire_minor_max = TR_RAFT_HANDSHAKE_WIRE_MINOR;
  config.max_frame_size = TR_RAFT_WIRE_MAX_FRAME_SIZE;
  config.max_snapshot_chunk_size = TR_RAFT_HANDSHAKE_MIN_SNAPSHOT_CHUNK_SIZE;
  return config;
}

static int flowmq_receive_message(void *context, const tr_raft_message_t *message) {
  flowmq_node_t *node = (flowmq_node_t *)context;

  if (node == NULL || message == NULL || message->to != node->node_id) {
    return TURBO_EPROTO;
  }
  ++node->received_count;
  node->received_from = message->from;
  if (node->received_count <= FLOWMQ_TEST_OUTBOUND_CAPACITY) {
    node->received_terms[node->received_count - 1U] = message->term;
  }
  return TURBO_OK;
}

static int flowmq_stream_begin(
    void *context, tr_raft_node_id_t leader_id, tr_raft_term_t term,
    uint64_t stream_id, uint64_t stream_size,
    const uint8_t digest[TR_RAFT_WIRE_DATA_DIGEST_SIZE]) {
  flowmq_stream_sink_t *sink = (flowmq_stream_sink_t *)context;
  (void)digest;
  if (sink == NULL || leader_id != 1U || term != FLOWMQ_TEST_STREAM_TERM ||
      stream_id != FLOWMQ_TEST_STREAM_ID ||
      stream_size != FLOWMQ_TEST_STREAM_BYTES) {
    return TURBO_EPROTO;
  }
  sink->used = 0U;
  sink->writes = 0U;
  sink->committed = 0;
  sink->aborted = 0;
  return TURBO_OK;
}

static int flowmq_stream_write(
    void *context, uint64_t offset, const uint8_t *data, size_t size) {
  flowmq_stream_sink_t *sink = (flowmq_stream_sink_t *)context;
  if (sink == NULL || data == NULL || offset != sink->used ||
      size > FLOWMQ_TEST_STREAM_BYTES - sink->used) {
    return TURBO_EPROTO;
  }
  sink->used += size;
  ++sink->writes;
  return TURBO_OK;
}

static int flowmq_stream_commit(void *context) {
  flowmq_stream_sink_t *sink = (flowmq_stream_sink_t *)context;
  if (sink == NULL || sink->used != FLOWMQ_TEST_STREAM_BYTES) {
    return TURBO_EPROTO;
  }
  sink->committed = 1;
  return TURBO_OK;
}

static void flowmq_stream_abort(void *context) {
  flowmq_stream_sink_t *sink = (flowmq_stream_sink_t *)context;
  if (sink != NULL) sink->aborted = 1;
}

static int flowmq_receive_payload(
    void *context, const tr_raft_coronet_payload_t *payload) {
  flowmq_node_t *node = (flowmq_node_t *)context;
  if (node == NULL || payload == NULL) {
    return TURBO_EPROTO;
  }
  if (payload->kind == TR_RAFT_WIRE_PAYLOAD_DATA_CHUNK &&
      node->stream_receiver != NULL) {
    tr_raft_data_stream_receive_result_t received;
    tr_raft_coronet_payload_t ack_payload;
    int result = tr_raft_data_stream_receiver_handle(
        node->stream_receiver, &payload->data.data_chunk, &received);
    if (result != TURBO_OK) return result;
    ++node->received_stream_chunks;
    node->received_stream_bytes += payload->data.data_chunk.data_length;
    memset(&ack_payload, 0, sizeof(ack_payload));
    ack_payload.kind = TR_RAFT_WIRE_PAYLOAD_DATA_ACK;
    ack_payload.data.data_ack = received.ack;
    return tr_raft_flowmq_peer_service_enqueue_payload(node->service,
                                                       &ack_payload);
  }
  if (payload->kind == TR_RAFT_WIRE_PAYLOAD_DATA_ACK &&
      node->stream_sender != NULL) {
    int result = tr_raft_data_stream_sender_acknowledge(
        node->stream_sender, &payload->data.data_ack);
    if (result == TURBO_OK) ++node->received_stream_acks;
    return result;
  }
  return TURBO_EPROTO;
}

static void flowmq_node_endpoint_initialize(flowmq_node_t *node, const flowmq_node_t *peer) {
  memset(&node->router_tls, 0, sizeof(node->router_tls));
  node->router_tls.ca_file = FLOWMQ_TEST_FIXTURE("ca.pem");
  node->router_tls.cert_file = node->certificate;
  node->router_tls.key_file = node->private_key;
  node->router_tls.require_client_certificate = 1;
  memset(&node->dealer_tls, 0, sizeof(node->dealer_tls));
  node->dealer_tls.ca_file = FLOWMQ_TEST_FIXTURE("ca.pem");
  node->dealer_tls.cert_file = FLOWMQ_TEST_FIXTURE("node1-cert.pem");
  node->dealer_tls.key_file = FLOWMQ_TEST_FIXTURE("node1-key.pem");
  node->dealer_tls.server_name = peer->server_name;
  node->dealer_tls.verify_peer = 1;

  flowmq_router_endpoint_config_init(&node->router_endpoint);
  node->router_endpoint.transport = FLOWMQ_TRANSPORT_WSS;
  node->router_endpoint.host = "127.0.0.1";
  node->router_endpoint.port = node->port;
  node->router_endpoint.path = "/raft/peer";
  node->router_endpoint.topic = "raft";
  node->router_endpoint.identity = node->identity;
  node->router_endpoint.tls = &node->router_tls;
  node->router_endpoint.max_connections = 1U;
  node->router_endpoint.heartbeat_interval_ms = 100U;
  node->router_endpoint.heartbeat_timeout_ms = 1000U;
  flowmq_connect_endpoint_config_init(&node->dealer_endpoint);
  node->dealer_endpoint.transport = FLOWMQ_TRANSPORT_WSS;
  node->dealer_endpoint.pattern = FLOWMQ_PROTOCOL_DEALER;
  node->dealer_endpoint.host = "127.0.0.1";
  node->dealer_endpoint.port = peer->port;
  node->dealer_endpoint.path = "/raft/peer";
  node->dealer_endpoint.topic = "raft";
  node->dealer_endpoint.identity = node->identity;
  node->dealer_endpoint.tls = &node->dealer_tls;
  node->dealer_endpoint.reconnect_initial_ms = 10U;
  node->dealer_endpoint.reconnect_max_ms = 100U;
  node->dealer_endpoint.heartbeat_interval_ms = 100U;
  node->dealer_endpoint.heartbeat_timeout_ms = 1000U;
}

static void flowmq_node_service_create(flowmq_node_t *node, const flowmq_node_t *peer) {
  tr_raft_flowmq_peer_service_config_t config;

  memset(&config, 0, sizeof(config));
  node->peer.node_id = peer->node_id;
  node->peer.peer_identity = peer->identity;
  node->peer.endpoint = node->dealer_endpoint;
  config.handshake = flowmq_handshake(node->node_id);
  config.router_endpoint = node->router_endpoint;
  config.peers = &node->peer;
  config.peer_count = 1U;
  config.outbound_queue_capacity = FLOWMQ_TEST_OUTBOUND_CAPACITY;
  config.max_send_batch_items = FLOWMQ_TEST_SEND_BATCH_ITEMS;
  config.inbound_queue_capacity_bytes = TR_RAFT_FLOWMQ_MIN_INBOUND_QUEUE_BYTES;
  config.on_message = flowmq_receive_message;
  config.message_context = node;
  config.on_snapshot = flowmq_receive_payload;
  config.snapshot_context = node;
  check_equal(tr_raft_flowmq_peer_service_create(&config, &node->service), TURBO_OK);
}

static int flowmq_handshakes_complete(flowmq_cluster_t *cluster) {
  size_t index;

  for (index = 0U; index < 2U; ++index) {
    tr_raft_flowmq_peer_service_status_t status;
    if (tr_raft_flowmq_peer_service_get_status(cluster->nodes[index].service, &status) !=
            TURBO_OK ||
        status.handshake_complete_count != 1U) {
      return 0;
    }
  }
  return 1;
}

static int flowmq_step_both(flowmq_cluster_t *cluster) {
  size_t index;

  for (index = 0U; index < 2U; ++index) {
    tr_raft_flowmq_peer_service_step_result_t step;
    int result = tr_raft_flowmq_peer_service_step(cluster->nodes[index].service, &step);
    if (result != TURBO_OK) {
      return result;
    }
    if (step.failed_peer_count != 0U) {
      return step.first_error == TURBO_OK ? TURBO_EPROTO : step.first_error;
    }
    cluster->payload_batches_sent += step.payload_batches_sent;
    cluster->payload_frames_sent += step.payload_frames_sent;
  }
  return TURBO_OK;
}

static int flowmq_drive(flowmq_cluster_t *cluster) {
  uint64_t deadline = turbo_monotonic_ms() + FLOWMQ_TEST_TIMEOUT_MS;
  int driver_result = TURBO_OK;
  size_t index;

  while (!flowmq_handshakes_complete(cluster) && driver_result == TURBO_OK &&
         turbo_monotonic_ms() < deadline) {
    driver_result = flowmq_step_both(cluster);
    if (driver_result != TURBO_OK) {
      break;
    }
    turbo_sleep_ms(1U);
  }
  if (driver_result == TURBO_OK && !flowmq_handshakes_complete(cluster)) {
    driver_result = TURBO_ETIMEDOUT;
  }
  for (index = 0U; driver_result == TURBO_OK && index < 2U; ++index) {
    size_t message_index;

    for (message_index = 0U;
         driver_result == TURBO_OK && message_index < FLOWMQ_TEST_OUTBOUND_CAPACITY;
         ++message_index) {
      tr_raft_message_t message;
      memset(&message, 0, sizeof(message));
      message.type = TR_RAFT_MSG_HEARTBEAT_REQUEST;
      message.from = cluster->nodes[index].node_id;
      message.to = cluster->nodes[1U - index].node_id;
      message.term = 7U + message_index;
      driver_result = tr_raft_flowmq_peer_service_enqueue(cluster->nodes[index].service, &message);
    }
  }
  cluster->backpressure_ok = 1;
  for (index = 0U; driver_result == TURBO_OK && index < 2U; ++index) {
    tr_raft_message_t message;
    memset(&message, 0, sizeof(message));
    message.type = TR_RAFT_MSG_HEARTBEAT_REQUEST;
    message.from = cluster->nodes[index].node_id;
    message.to = cluster->nodes[1U - index].node_id;
    message.term = 99U;
    if (tr_raft_flowmq_peer_service_enqueue(cluster->nodes[index].service, &message) !=
        TURBO_ENOSPC) {
      cluster->backpressure_ok = 0;
      driver_result = TURBO_EPROTO;
    }
  }
  while (driver_result == TURBO_OK &&
         (cluster->nodes[0].received_count != FLOWMQ_TEST_OUTBOUND_CAPACITY ||
          cluster->nodes[1].received_count != FLOWMQ_TEST_OUTBOUND_CAPACITY) &&
         turbo_monotonic_ms() < deadline) {
    driver_result = flowmq_step_both(cluster);
    turbo_sleep_ms(1U);
  }
  if (driver_result == TURBO_OK &&
      (cluster->nodes[0].received_count != FLOWMQ_TEST_OUTBOUND_CAPACITY ||
       cluster->nodes[1].received_count != FLOWMQ_TEST_OUTBOUND_CAPACITY)) {
    driver_result = TURBO_ETIMEDOUT;
  }
  if (driver_result == TURBO_OK) {
    static uint8_t stream_data[FLOWMQ_TEST_STREAM_BYTES];
    size_t chunk_index;
    memset(stream_data, 0x5c, sizeof(stream_data));
    driver_result = tr_raft_data_stream_sender_begin(
        cluster->nodes[0].stream_sender, FLOWMQ_TEST_STREAM_TERM,
        FLOWMQ_TEST_STREAM_ID, stream_data, sizeof(stream_data));
    for (chunk_index = 0U; chunk_index < 4U; ++chunk_index) {
      tr_raft_coronet_payload_t payload;
      memset(&payload, 0, sizeof(payload));
      payload.kind = TR_RAFT_WIRE_PAYLOAD_DATA_CHUNK;
      if (driver_result == TURBO_OK) {
        driver_result = tr_raft_data_stream_sender_next(
            cluster->nodes[0].stream_sender, &payload.data.data_chunk);
      }
      if (driver_result != TURBO_OK) break;
      driver_result = tr_raft_flowmq_peer_service_enqueue_payload(
          cluster->nodes[0].service, &payload);
      if (driver_result != TURBO_OK) break;
    }
  }
  while (driver_result == TURBO_OK &&
         (!cluster->nodes[1].stream_sink.committed ||
          cluster->nodes[0].received_stream_acks != 4U) &&
         turbo_monotonic_ms() < deadline) {
    driver_result = flowmq_step_both(cluster);
    turbo_sleep_ms(1U);
  }
  if (driver_result == TURBO_OK &&
      (!cluster->nodes[1].stream_sink.committed ||
       cluster->nodes[0].received_stream_acks != 4U)) {
    driver_result = TURBO_ETIMEDOUT;
  }
  for (index = 0U; index < 2U; ++index) {
    int result = tr_raft_flowmq_peer_service_stop(cluster->nodes[index].service);
    if (driver_result == TURBO_OK && result != TURBO_OK) {
      driver_result = result;
    }
  }
  return driver_result;
}

spec("Raft FlowMQ peer service") {
  it("replicates bidirectionally over public WSS ROUTER DEALER endpoints") {
    flowmq_cluster_t cluster;
    tr_raft_data_stream_sender_config_t sender_config;
    tr_raft_data_stream_receiver_config_t receiver_config;
    tr_raft_data_stream_sender_status_t sender_status;
    size_t index;

    memset(&cluster, 0, sizeof(cluster));
    memset(&sender_config, 0, sizeof(sender_config));
    memset(&receiver_config, 0, sizeof(receiver_config));
    cluster.nodes[0].node_id = 1U;
    cluster.nodes[0].identity = "node-1";
    cluster.nodes[0].peer_identity = "node-2";
    cluster.nodes[0].certificate = FLOWMQ_TEST_FIXTURE("node2-cert.pem");
    cluster.nodes[0].private_key = FLOWMQ_TEST_FIXTURE("node2-key.pem");
    cluster.nodes[0].server_name = "node-2.mesh";
    cluster.nodes[1].node_id = 2U;
    cluster.nodes[1].identity = "node-2";
    cluster.nodes[1].peer_identity = "node-1";
    cluster.nodes[1].certificate = FLOWMQ_TEST_FIXTURE("node2-cert.pem");
    cluster.nodes[1].private_key = FLOWMQ_TEST_FIXTURE("node2-key.pem");
    cluster.nodes[1].server_name = "node-2.mesh";
    sender_config.self_id = cluster.nodes[0].node_id;
    sender_config.peer_id = cluster.nodes[1].node_id;
    sender_config.max_stream_bytes = FLOWMQ_TEST_STREAM_BYTES;
    receiver_config.self_id = cluster.nodes[1].node_id;
    receiver_config.max_stream_bytes = FLOWMQ_TEST_STREAM_BYTES;
    receiver_config.sink.begin = flowmq_stream_begin;
    receiver_config.sink.write = flowmq_stream_write;
    receiver_config.sink.commit = flowmq_stream_commit;
    receiver_config.sink.abort = flowmq_stream_abort;
    receiver_config.sink.context = &cluster.nodes[1].stream_sink;
    check_equal(tr_raft_data_stream_sender_create(
                     &sender_config, &cluster.nodes[0].stream_sender),
                 TURBO_OK);
    check_equal(tr_raft_data_stream_receiver_create(
                     &receiver_config, &cluster.nodes[1].stream_receiver),
                 TURBO_OK);
    check_equal(tr_test_reserve_loopback_port(&cluster.nodes[0].port), TURBO_OK);
    check_equal(tr_test_reserve_loopback_port(&cluster.nodes[1].port), TURBO_OK);
    check_not_equal(cluster.nodes[0].port, cluster.nodes[1].port);

    flowmq_node_endpoint_initialize(&cluster.nodes[0], &cluster.nodes[1]);
    flowmq_node_endpoint_initialize(&cluster.nodes[1], &cluster.nodes[0]);
    flowmq_node_service_create(&cluster.nodes[0], &cluster.nodes[1]);
    flowmq_node_service_create(&cluster.nodes[1], &cluster.nodes[0]);
    check_equal(tr_raft_flowmq_peer_service_start(cluster.nodes[0].service), TURBO_OK);
    check_equal(tr_raft_flowmq_peer_service_start(cluster.nodes[1].service), TURBO_OK);
    cluster.driver_result = flowmq_drive(&cluster);
    check_equal(cluster.driver_result, TURBO_OK);
    check(cluster.backpressure_ok);
    check_equal(cluster.payload_batches_sent, 4U);
    check_equal(cluster.payload_frames_sent,
                  4U * FLOWMQ_TEST_OUTBOUND_CAPACITY);
    check_equal(cluster.nodes[0].received_count, FLOWMQ_TEST_OUTBOUND_CAPACITY);
    check_equal(cluster.nodes[1].received_count, FLOWMQ_TEST_OUTBOUND_CAPACITY);
    check_equal(cluster.nodes[1].received_stream_chunks, 4U);
    check_equal(cluster.nodes[1].received_stream_bytes, 256U * 1024U);
    check_equal(cluster.nodes[0].received_stream_acks, 4U);
    check_equal(cluster.nodes[1].stream_sink.writes, 4U);
    check_false(cluster.nodes[1].stream_sink.aborted);
    check_equal(tr_raft_data_stream_sender_get_status(
                     cluster.nodes[0].stream_sender, &sender_status),
                 TURBO_OK);
    check_true(sender_status.complete);
    check_equal(sender_status.inflight_chunks, 0U);
    check_equal(cluster.nodes[0].received_from, 2U);
    check_equal(cluster.nodes[1].received_from, 1U);
    for (index = 0U; index < FLOWMQ_TEST_OUTBOUND_CAPACITY; ++index) {
      check_equal(cluster.nodes[0].received_terms[index], 7U + index);
      check_equal(cluster.nodes[1].received_terms[index], 7U + index);
    }

    for (index = 0U; index < 2U; ++index) {
      check_equal(tr_raft_flowmq_peer_service_destroy(cluster.nodes[index].service), TURBO_OK);
    }
    tr_raft_data_stream_receiver_destroy(cluster.nodes[1].stream_receiver);
    tr_raft_data_stream_sender_destroy(cluster.nodes[0].stream_sender);
  }
}
