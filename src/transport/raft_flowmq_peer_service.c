#include <turboraft/raft_flowmq_peer_service.h>

#include "raft_coronet_payload_storage.h"
#include "../turboraft_stl_status.h"

#include <ring_buffer_spsc.h>
#include <turbostl/deque.h>
#include <turbo_error.h>
#include <turbo_thread.h>

#include <stdatomic.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#define TR_RAFT_FLOWMQ_CONTROL_CAPACITY 2U
#define TR_RAFT_FLOWMQ_STOP_POLL_MS 10U
#define TR_RAFT_FLOWMQ_NS_PER_MS UINT64_C(1000000)

typedef enum tr_raft_flowmq_ingress_kind {
  TR_RAFT_FLOWMQ_INGRESS_RESET = 1,
  TR_RAFT_FLOWMQ_INGRESS_PACKET = 2
} tr_raft_flowmq_ingress_kind_t;

typedef struct tr_raft_flowmq_ingress_header {
  uint32_t kind;
  uint32_t peer_index;
  uint32_t payload_size;
  uint32_t reserved;
} tr_raft_flowmq_ingress_header_t;

typedef struct tr_raft_flowmq_control_queue {
  uint8_t packets[TR_RAFT_FLOWMQ_CONTROL_CAPACITY][TR_RAFT_HANDSHAKE_PACKET_SIZE];
  size_t sizes[TR_RAFT_FLOWMQ_CONTROL_CAPACITY];
  size_t head;
  size_t count;
} tr_raft_flowmq_control_queue_t;

typedef struct tr_raft_flowmq_peer tr_raft_flowmq_peer_t;

typedef struct tr_raft_flowmq_send_command {
  tr_raft_flowmq_peer_t *peer;
  tstr *frames;
  size_t frame_count;
  size_t submitted;
  int result;
  int pending;
  turbo_mutex_t mutex;
  turbo_cond_t complete;
  int sync_initialized;
} tr_raft_flowmq_send_command_t;

struct tr_raft_flowmq_peer {
  tr_raft_flowmq_peer_service_t *service;
  size_t index;
  tr_raft_node_id_t node_id;
  char identity[TR_RAFT_FLOWMQ_MAX_IDENTITY_SIZE + 1U];
  flowmq_connect_endpoint_t *dealer;
  turbo_thread_t connector_thread;
  uint64_t start_timeout_ns;
  uint64_t reconnect_delay_ms;
  atomic_int connector_stop;
  atomic_int connector_error;
  atomic_int dealer_ready;
  int connector_thread_created;
  tr_raft_handshake_exchange_t *exchange;
  tr_raft_coronet_session_t *session;
  deque_t outbound;
  size_t outbound_snapshot_bytes;
  size_t outbound_data_bytes;
  tr_raft_flowmq_control_queue_t control;
  tr_raft_flowmq_send_command_t send_command;
  uint64_t next_fmq_message_id;
  size_t max_frame_size;
  int handshake_complete;
  int faulted;
};

struct tr_raft_flowmq_peer_service {
  tr_raft_handshake_config_t handshake;
  flowmq_router_endpoint_t *router;
  uint64_t router_start_timeout_ns;
  tr_raft_flowmq_peer_t peers[TR_RAFT_MAX_VOTERS - 1U];
  size_t peer_count;
  size_t initialized_peer_count;
  size_t outbound_queue_capacity;
  size_t max_send_batch_items;
  size_t max_send_batch_bytes;
  size_t max_inflight_data_bytes;
  tstr *send_frames;
  uint8_t *send_packets;
  ring_spsc_t inbound;
  turbo_mutex_t inbound_producer_mutex;
  uint8_t *inbound_storage;
  size_t inbound_queue_capacity_bytes;
  tr_raft_coronet_message_handler_fn on_message;
  void *message_context;
  tr_raft_coronet_snapshot_handler_fn on_snapshot;
  void *snapshot_context;
  atomic_uint callback_depth;
  atomic_int inbound_error;
  int started;
  int router_started;
  int stopping;
  int step_active;
  int last_error;
};

static int tr_raft_flowmq_is_power_of_two(size_t value) {
  return value != 0U && (value & (value - 1U)) == 0U;
}

static int tr_raft_flowmq_checked_multiply(size_t left, size_t right, size_t *out_value) {
  if (out_value == NULL || (right != 0U && left > SIZE_MAX / right)) {
    return TURBO_EINVAL;
  }
  *out_value = left * right;
  return TURBO_OK;
}

static uint64_t tr_raft_flowmq_timeout_ns(const flowmq_coronet_timeout_config_t *timeouts) {
  uint64_t timeout_ms;

  if (timeouts == NULL) {
    return FLOWMQ_CONNECT_ENDPOINT_DEFAULT_TIMEOUT_MS * TR_RAFT_FLOWMQ_NS_PER_MS;
  }
  timeout_ms =
      timeouts->handshake_timeout_ms != 0U ? timeouts->handshake_timeout_ms : timeouts->timeout_ms;
  if (timeout_ms == 0U) {
    timeout_ms = FLOWMQ_CONNECT_ENDPOINT_DEFAULT_TIMEOUT_MS;
  }
  return timeout_ms > UINT64_MAX / TR_RAFT_FLOWMQ_NS_PER_MS ? UINT64_MAX
                                                            : timeout_ms * TR_RAFT_FLOWMQ_NS_PER_MS;
}

static int tr_raft_flowmq_send_is_blocked(int result) {
  return result == TURBO_EBUSY || result == TURBO_ENOTCONN || result == TURBO_ECONNRESET ||
         result == TURBO_ECONNREFUSED || result == TURBO_EHOSTUNREACH || result == TURBO_ENETDOWN ||
         result == TURBO_ENETUNREACH || result == TURBO_ETIMEDOUT;
}

static void tr_raft_flowmq_connector_wait(const tr_raft_flowmq_peer_t *peer) {
  uint64_t remaining = peer->reconnect_delay_ms;

  while (remaining != 0U && !atomic_load_explicit(&peer->connector_stop, memory_order_acquire)) {
    uint32_t delay =
        remaining > TR_RAFT_FLOWMQ_STOP_POLL_MS ? TR_RAFT_FLOWMQ_STOP_POLL_MS : (uint32_t)remaining;
    turbo_sleep_ms(delay);
    remaining -= delay;
  }
}

static void tr_raft_flowmq_connector_run(void *context) {
  tr_raft_flowmq_peer_t *peer = (tr_raft_flowmq_peer_t *)context;

  while (!atomic_load_explicit(&peer->connector_stop, memory_order_acquire)) {
    int result = flowmq_connect_endpoint_start(peer->dealer, peer->start_timeout_ns);

    if (result == TURBO_OK || result == TURBO_EALREADY) {
      return;
    }
    if (!tr_raft_flowmq_send_is_blocked(result) || peer->reconnect_delay_ms == 0U) {
      atomic_store_explicit(&peer->connector_error, result, memory_order_release);
      return;
    }
    tr_raft_flowmq_connector_wait(peer);
  }
}

static int tr_raft_flowmq_ingress_push(tr_raft_flowmq_peer_service_t *service,
                                       tr_raft_flowmq_ingress_kind_t kind, size_t peer_index,
                                       const void *payload, size_t payload_size) {
  tr_raft_flowmq_ingress_header_t header;
  uint8_t *record;
  size_t record_size;

  if (service == NULL || peer_index >= service->peer_count ||
      (payload_size != 0U && payload == NULL) || payload_size > TR_RAFT_CORONET_MAX_PACKET_SIZE ||
      payload_size > SIZE_MAX - sizeof(header)) {
    return TURBO_EINVAL;
  }
  record_size = sizeof(header) + payload_size;
  turbo_mutex_lock(&service->inbound_producer_mutex);
  record = ring_spsc_write_acquire(&service->inbound, record_size);
  if (record == NULL) {
    turbo_mutex_unlock(&service->inbound_producer_mutex);
    atomic_store_explicit(&service->inbound_error, TURBO_ENOSPC, memory_order_release);
    return TURBO_ENOSPC;
  }
  header.kind = (uint32_t)kind;
  header.peer_index = (uint32_t)peer_index;
  header.payload_size = (uint32_t)payload_size;
  header.reserved = 0U;
  memcpy(record, &header, sizeof(header));
  if (payload_size != 0U) {
    memcpy(record + sizeof(header), payload, payload_size);
  }
  ring_spsc_write_release(&service->inbound, record_size);
  turbo_mutex_unlock(&service->inbound_producer_mutex);
  return TURBO_OK;
}

static int tr_raft_flowmq_transport_is_secure(flowmq_coronet_transport_t transport) {
  return transport == FLOWMQ_TRANSPORT_TLS || transport == FLOWMQ_TRANSPORT_WSS;
}

static int tr_raft_flowmq_router_validate(const flowmq_router_endpoint_config_t *endpoint) {
  const flowmq_coronet_tls_server_config_t *tls;

  if (endpoint == NULL || endpoint->size < sizeof(*endpoint) ||
      !tr_raft_flowmq_transport_is_secure(endpoint->transport) || endpoint->host == NULL ||
      endpoint->path == NULL || endpoint->topic == NULL || endpoint->identity == NULL ||
      endpoint->tls == NULL || endpoint->context != NULL || endpoint->on_frame != NULL ||
      endpoint->on_state != NULL || endpoint->on_event != NULL || endpoint->callback_ctx != NULL ||
      endpoint->max_connections == 0U) {
    return TURBO_EINVAL;
  }
  tls = endpoint->tls;
  if (tls->ca_file == NULL || !tls->ca_file[0] || tls->cert_file == NULL || !tls->cert_file[0] ||
      tls->key_file == NULL || !tls->key_file[0] || !tls->require_client_certificate) {
    return TURBO_EINVAL;
  }
  return endpoint->max_frame_size < TR_RAFT_CORONET_MAX_PACKET_SIZE ? TURBO_EMSGSIZE : TURBO_OK;
}

static int tr_raft_flowmq_dealer_validate(const flowmq_connect_endpoint_config_t *endpoint) {
  const flowmq_coronet_tls_client_config_t *tls;

  if (endpoint == NULL || endpoint->size < sizeof(*endpoint) ||
      !tr_raft_flowmq_transport_is_secure(endpoint->transport) ||
      endpoint->pattern != FLOWMQ_PROTOCOL_DEALER || endpoint->host == NULL ||
      endpoint->path == NULL || endpoint->topic == NULL || endpoint->identity == NULL ||
      !endpoint->identity[0] || endpoint->tls == NULL || endpoint->context != NULL ||
      endpoint->on_frame != NULL || endpoint->receive_ready != NULL || endpoint->on_state != NULL ||
      endpoint->on_event != NULL || endpoint->callback_ctx != NULL) {
    return TURBO_EINVAL;
  }
  tls = endpoint->tls;
  if (tls->ca_file == NULL || !tls->ca_file[0] || tls->cert_file == NULL || !tls->cert_file[0] ||
      tls->key_file == NULL || !tls->key_file[0] || tls->server_name == NULL ||
      !tls->server_name[0] || !tls->verify_peer) {
    return TURBO_EINVAL;
  }
  return endpoint->max_frame_size < TR_RAFT_CORONET_MAX_PACKET_SIZE ? TURBO_EMSGSIZE : TURBO_OK;
}

static int tr_raft_flowmq_control_push(tr_raft_flowmq_control_queue_t *queue, const uint8_t *packet,
                                       size_t size) {
  size_t index;

  if (queue == NULL || packet == NULL || size != TR_RAFT_HANDSHAKE_PACKET_SIZE) {
    return TURBO_EINVAL;
  }
  if (queue->count == TR_RAFT_FLOWMQ_CONTROL_CAPACITY) {
    return TURBO_ENOSPC;
  }
  index = (queue->head + queue->count) % TR_RAFT_FLOWMQ_CONTROL_CAPACITY;
  memcpy(queue->packets[index], packet, size);
  queue->sizes[index] = size;
  ++queue->count;
  return TURBO_OK;
}

static void tr_raft_flowmq_control_pop(tr_raft_flowmq_control_queue_t *queue) {
  if (queue == NULL || queue->count == 0U) {
    return;
  }
  queue->sizes[queue->head] = 0U;
  queue->head = (queue->head + 1U) % TR_RAFT_FLOWMQ_CONTROL_CAPACITY;
  --queue->count;
}

static tr_raft_flowmq_peer_t *tr_raft_flowmq_find_peer_node(tr_raft_flowmq_peer_service_t *service,
                                                            tr_raft_node_id_t node_id) {
  size_t index;

  if (service == NULL || node_id == 0U) {
    return NULL;
  }
  for (index = 0U; index < service->peer_count; ++index) {
    if (service->peers[index].node_id == node_id) {
      return &service->peers[index];
    }
  }
  return NULL;
}

static tr_raft_flowmq_peer_t *
tr_raft_flowmq_find_peer_identity(tr_raft_flowmq_peer_service_t *service, vstr identity) {
  size_t index;

  if (service == NULL || identity.data == NULL || identity.len == 0U) {
    return NULL;
  }
  for (index = 0U; index < service->peer_count; ++index) {
    size_t expected = strlen(service->peers[index].identity);
    if (identity.len == expected &&
        memcmp(identity.data, service->peers[index].identity, expected) == 0) {
      return &service->peers[index];
    }
  }
  return NULL;
}

static int tr_raft_flowmq_peer_reset(tr_raft_flowmq_peer_t *peer) {
  uint8_t hello[TR_RAFT_HANDSHAKE_PACKET_SIZE];
  size_t hello_size = 0U;
  int result;

  if (peer == NULL || peer->service == NULL) {
    return TURBO_EINVAL;
  }
  tr_raft_coronet_session_destroy(peer->session);
  peer->session = NULL;
  tr_raft_handshake_exchange_destroy(peer->exchange);
  peer->exchange = NULL;
  memset(&peer->control, 0, sizeof(peer->control));
  peer->handshake_complete = 0;
  peer->faulted = 0;

  result =
      tr_raft_handshake_exchange_create(&peer->service->handshake, peer->node_id, &peer->exchange);
  if (result == TURBO_OK) {
    result = tr_raft_handshake_exchange_start(peer->exchange, hello, sizeof(hello), &hello_size);
  }
  if (result == TURBO_OK) {
    result = tr_raft_flowmq_control_push(&peer->control, hello, hello_size);
  }
  if (result != TURBO_OK) {
    tr_raft_handshake_exchange_destroy(peer->exchange);
    peer->exchange = NULL;
    peer->faulted = 1;
    peer->service->last_error = result;
  }
  return result;
}

static void tr_raft_flowmq_dealer_state(void *context,
                                        flowmq_connect_endpoint_connection_state_t state,
                                        int status, size_t connections_current) {
  tr_raft_flowmq_peer_t *peer = (tr_raft_flowmq_peer_t *)context;
  tr_raft_flowmq_peer_service_t *service;
  int was_ready;

  (void)status;
  (void)connections_current;
  if (peer == NULL || peer->service == NULL) {
    return;
  }
  service = peer->service;
  atomic_fetch_add_explicit(&service->callback_depth, 1U, memory_order_acq_rel);
  was_ready = atomic_exchange_explicit(
      &peer->dealer_ready, state == FLOWMQ_ENDPOINT_CONNECTION_READY, memory_order_acq_rel);
  if (state == FLOWMQ_ENDPOINT_CONNECTION_READY) {
    atomic_store_explicit(&peer->connector_error, TURBO_OK, memory_order_release);
  } else if (was_ready) {
    (void)tr_raft_flowmq_ingress_push(service, TR_RAFT_FLOWMQ_INGRESS_RESET, peer->index, NULL, 0U);
  }
  atomic_fetch_sub_explicit(&service->callback_depth, 1U, memory_order_acq_rel);
}

static void tr_raft_flowmq_router_event(void *context,
                                        const flowmq_router_endpoint_event_t *event) {
  tr_raft_flowmq_peer_service_t *service = (tr_raft_flowmq_peer_service_t *)context;
  tr_raft_flowmq_peer_t *peer;

  if (service == NULL || event == NULL) {
    return;
  }
  atomic_fetch_add_explicit(&service->callback_depth, 1U, memory_order_acq_rel);
  if (event->kind == FLOWMQ_ROUTER_EVENT_PEER_DISCONNECTED ||
      event->kind == FLOWMQ_ROUTER_EVENT_HEARTBEAT_TIMEOUT) {
    peer = tr_raft_flowmq_find_peer_identity(service, event->peer_identity);
    if (peer != NULL) {
      (void)tr_raft_flowmq_ingress_push(service, TR_RAFT_FLOWMQ_INGRESS_RESET, peer->index, NULL,
                                        0U);
    }
  }
  atomic_fetch_sub_explicit(&service->callback_depth, 1U, memory_order_acq_rel);
}

static int tr_raft_flowmq_peer_create_session(tr_raft_flowmq_peer_t *peer) {
  tr_raft_handshake_result_t handshake;
  tr_raft_coronet_session_config_t config;
  int result;

  memset(&handshake, 0, sizeof(handshake));
  result = tr_raft_handshake_exchange_get_result(peer->exchange, &handshake);
  if (result != TURBO_OK) {
    return result;
  }
  memset(&config, 0, sizeof(config));
  config.cluster_id = peer->service->handshake.cluster_id;
  config.local_node_id = peer->service->handshake.local_node_id;
  config.peer_node_id = peer->node_id;
  config.first_outbound_message_id = 1U;
  config.handshake = &handshake;
  config.on_message = peer->service->on_message;
  config.message_context = peer->service->message_context;
  config.on_snapshot = peer->service->on_snapshot;
  config.snapshot_context = peer->service->snapshot_context;
  result = tr_raft_coronet_session_create(&config, &peer->session);
  if (result == TURBO_OK) {
    peer->handshake_complete = 1;
  }
  return result;
}

static int tr_raft_flowmq_peer_receive_handshake(tr_raft_flowmq_peer_t *peer, const uint8_t *data,
                                                 size_t size) {
  uint8_t response[TR_RAFT_HANDSHAKE_PACKET_SIZE];
  size_t consumed = 0U;
  size_t response_size = 0U;
  tr_raft_handshake_exchange_state_t state;
  int result;

  if (peer->exchange == NULL) {
    return TURBO_EPROTO;
  }
  result = tr_raft_handshake_exchange_feed(peer->exchange, data, size, &consumed, response,
                                           sizeof(response), &response_size);
  if (result != TURBO_OK || consumed != size) {
    return result == TURBO_OK ? TURBO_EPROTO : result;
  }
  if (response_size != 0U) {
    result = tr_raft_flowmq_control_push(&peer->control, response, response_size);
    if (result != TURBO_OK) {
      return result;
    }
  }
  result = tr_raft_handshake_exchange_get_state(peer->exchange, &state);
  if (result == TURBO_OK && state == TR_RAFT_HANDSHAKE_EXCHANGE_COMPLETE &&
      !peer->handshake_complete) {
    result = tr_raft_flowmq_peer_create_session(peer);
  }
  return result;
}

static int tr_raft_flowmq_router_frame(void *context, const flowmq_router_route_t *route,
                                       vstr peer_identity, vstr peer_topic,
                                       const flowmq_protocol_frame_t *frame) {
  tr_raft_flowmq_peer_service_t *service = (tr_raft_flowmq_peer_service_t *)context;
  tr_raft_flowmq_peer_t *peer;
  int result;

  (void)route;
  (void)peer_topic;
  if (service == NULL || frame == NULL || frame->kind != FLOWMQ_PROTOCOL_FRAME_DATA) {
    return TURBO_EINVAL;
  }
  atomic_fetch_add_explicit(&service->callback_depth, 1U, memory_order_acq_rel);
  peer = tr_raft_flowmq_find_peer_identity(service, peer_identity);
  if (peer == NULL) {
    result = TURBO_EPROTO;
    goto done;
  }
  result = tr_raft_flowmq_ingress_push(service, TR_RAFT_FLOWMQ_INGRESS_PACKET, peer->index,
                                       frame->payload.data, frame->payload.len);

done:
  if (result != TURBO_OK) {
    atomic_store_explicit(&service->inbound_error, result, memory_order_release);
  }
  atomic_fetch_sub_explicit(&service->callback_depth, 1U, memory_order_acq_rel);
  return result;
}

static void tr_raft_flowmq_send_post(void *arg1, void *arg2) {
  tr_raft_flowmq_send_command_t *command = (tr_raft_flowmq_send_command_t *)arg1;
  tr_raft_flowmq_peer_t *peer;
  tstr *frames;
  size_t frame_count;
  size_t submitted = 0U;
  int result = TURBO_OK;

  (void)arg2;
  turbo_mutex_lock(&command->mutex);
  peer = command->peer;
  frames = command->frames;
  frame_count = command->frame_count;
  turbo_mutex_unlock(&command->mutex);
  while (submitted < frame_count) {
    result =
        flowmq_connect_endpoint_send(peer->dealer, frames[submitted], tstr_len(frames[submitted]));
    if (result != TURBO_OK) {
      break;
    }
    ++submitted;
  }
  turbo_mutex_lock(&command->mutex);
  command->submitted = submitted;
  command->result = result;
  command->pending = 0;
  turbo_cond_broadcast(&command->complete);
  turbo_mutex_unlock(&command->mutex);
}

static int tr_raft_flowmq_send_frames(tr_raft_flowmq_peer_t *peer, tstr *frames,
                                      size_t frame_count, size_t *out_submitted) {
  tr_raft_flowmq_send_command_t *command;
  int result;

  if (peer == NULL || frames == NULL || frame_count == 0U || out_submitted == NULL) {
    return TURBO_EINVAL;
  }
  *out_submitted = 0U;
  command = &peer->send_command;
  turbo_mutex_lock(&command->mutex);
  if (command->pending) {
    turbo_mutex_unlock(&command->mutex);
    return TURBO_EBUSY;
  }
  command->peer = peer;
  command->frames = frames;
  command->frame_count = frame_count;
  command->submitted = 0U;
  command->result = TURBO_EALREADY;
  command->pending = 1;
  result = coro_post(flowmq_connect_endpoint_context(peer->dealer), tr_raft_flowmq_send_post,
                     command, NULL);
  if (result != TURBO_OK) {
    command->pending = 0;
    command->result = result;
  } else {
    while (command->pending) {
      turbo_cond_wait(&command->complete, &command->mutex);
    }
    result = command->result;
    *out_submitted = command->submitted;
  }
  turbo_mutex_unlock(&command->mutex);
  return result;
}

static int tr_raft_flowmq_encode_data(tr_raft_flowmq_peer_t *peer, const void *payload,
                                      size_t payload_size, tstr *out_frame) {
  flowmq_protocol_frame_t frame;
  int result;

  if (peer == NULL || payload == NULL || payload_size == 0U || out_frame == NULL ||
      peer->next_fmq_message_id == UINT64_MAX) {
    return TURBO_EINVAL;
  }
  memset(&frame, 0, sizeof(frame));
  frame.kind = FLOWMQ_PROTOCOL_FRAME_DATA;
  frame.pattern = FLOWMQ_PROTOCOL_DEALER;
  frame.message_id = peer->next_fmq_message_id;
  frame.payload = vstr_from_buf(payload, payload_size);
  result = flowmq_protocol_encode_frame(&frame, peer->max_frame_size, out_frame);
  if (result == TURBO_OK) {
    ++peer->next_fmq_message_id;
  }
  return result;
}

static void tr_raft_flowmq_clear_send_frames(tr_raft_flowmq_peer_service_t *service,
                                             size_t frame_count) {
  size_t index;

  if (service == NULL || service->send_frames == NULL) {
    return;
  }
  if (frame_count > service->max_send_batch_items) {
    frame_count = service->max_send_batch_items;
  }
  for (index = 0U; index < frame_count; ++index) {
    tstr_freep(&service->send_frames[index]);
  }
}

static void tr_raft_flowmq_peer_cleanup(tr_raft_flowmq_peer_t *peer) {
  tr_raft_owned_coronet_payload_t owned;

  if (peer == NULL) {
    return;
  }
  flowmq_connect_endpoint_destroy(peer->dealer);
  peer->dealer = NULL;
  if (peer->send_command.sync_initialized) {
    turbo_cond_destroy(&peer->send_command.complete);
    turbo_mutex_destroy(&peer->send_command.mutex);
    peer->send_command.sync_initialized = 0;
  }
  tr_raft_coronet_session_destroy(peer->session);
  peer->session = NULL;
  tr_raft_handshake_exchange_destroy(peer->exchange);
  peer->exchange = NULL;
  while (deque_pop_front(&peer->outbound, &owned) == STL_OK) {
    tr_raft_owned_coronet_payload_release(&owned);
  }
  deque_destroy(&peer->outbound);
}

static void tr_raft_flowmq_service_cleanup(tr_raft_flowmq_peer_service_t *service) {
  size_t index;

  if (service == NULL) {
    return;
  }
  for (index = 0U; index < service->initialized_peer_count; ++index) {
    tr_raft_flowmq_peer_cleanup(&service->peers[index]);
  }
  flowmq_router_endpoint_destroy(service->router);
  service->router = NULL;
  turbo_mutex_destroy(&service->inbound_producer_mutex);
  free(service->inbound_storage);
  service->inbound_storage = NULL;
  free(service->send_packets);
  service->send_packets = NULL;
  if (service->send_frames != NULL) {
    for (index = 0U; index < service->max_send_batch_items; ++index) {
      tstr_freep(&service->send_frames[index]);
    }
  }
  free(service->send_frames);
  service->send_frames = NULL;
}

static int tr_raft_flowmq_peers_validate(const tr_raft_flowmq_peer_service_config_t *config) {
  size_t left;

  if (config->peers == NULL || config->peer_count == 0U ||
      config->peer_count > TR_RAFT_MAX_VOTERS - 1U) {
    return TURBO_EINVAL;
  }
  for (left = 0U; left < config->peer_count; ++left) {
    const tr_raft_flowmq_peer_config_t *peer = &config->peers[left];
    size_t identity_size;
    size_t right;
    int result;

    if (peer->node_id == 0U || peer->node_id == config->handshake.local_node_id ||
        peer->peer_identity == NULL || !peer->peer_identity[0]) {
      return TURBO_EINVAL;
    }
    identity_size = strlen(peer->peer_identity);
    if (identity_size > TR_RAFT_FLOWMQ_MAX_IDENTITY_SIZE) {
      return TURBO_ENAMETOOLONG;
    }
    result = tr_raft_flowmq_dealer_validate(&peer->endpoint);
    if (result != TURBO_OK) {
      return result;
    }
    for (right = left + 1U; right < config->peer_count; ++right) {
      if (peer->node_id == config->peers[right].node_id ||
          strcmp(peer->peer_identity, config->peers[right].peer_identity) == 0) {
        return TURBO_EINVAL;
      }
    }
  }
  return TURBO_OK;
}

int tr_raft_flowmq_peer_service_create(const tr_raft_flowmq_peer_service_config_t *config,
                                       tr_raft_flowmq_peer_service_t **out_service) {
  flowmq_router_endpoint_config_t router_endpoint;
  tr_raft_flowmq_peer_service_t *service;
  size_t send_batch_items;
  size_t send_batch_bytes;
  size_t send_packet_storage_bytes;
  size_t index;
  int result;

  if (out_service == NULL) {
    return TURBO_EINVAL;
  }
  *out_service = NULL;
  if (config == NULL || config->on_message == NULL || config->handshake.local_node_id == 0U ||
      config->outbound_queue_capacity == 0U ||
      config->outbound_queue_capacity > TR_RAFT_FLOWMQ_MAX_OUTBOUND_QUEUE_CAPACITY ||
      config->max_send_batch_items > TR_RAFT_FLOWMQ_MAX_SEND_BATCH_ITEMS ||
      config->inbound_queue_capacity_bytes < TR_RAFT_FLOWMQ_MIN_INBOUND_QUEUE_BYTES ||
      config->inbound_queue_capacity_bytes > TR_RAFT_FLOWMQ_MAX_INBOUND_QUEUE_BYTES ||
      config->max_inflight_data_bytes > TR_RAFT_FLOWMQ_MAX_INFLIGHT_DATA_BYTES ||
      !tr_raft_flowmq_is_power_of_two(config->inbound_queue_capacity_bytes)) {
    return TURBO_EINVAL;
  }
  send_batch_items = config->max_send_batch_items == 0U ? TR_RAFT_FLOWMQ_DEFAULT_SEND_BATCH_ITEMS
                                                        : config->max_send_batch_items;
  if (send_batch_items > config->outbound_queue_capacity ||
      tr_raft_flowmq_checked_multiply(send_batch_items, TR_RAFT_CORONET_MAX_PACKET_SIZE,
                                      &send_packet_storage_bytes) != TURBO_OK) {
    return TURBO_EINVAL;
  }
  send_batch_bytes =
      config->max_send_batch_bytes == 0U ? send_packet_storage_bytes : config->max_send_batch_bytes;
  if (send_batch_bytes == 0U || send_batch_bytes > send_packet_storage_bytes) {
    return TURBO_EINVAL;
  }
  result = tr_raft_flowmq_router_validate(&config->router_endpoint);
  if (result == TURBO_OK && config->router_endpoint.max_connections < config->peer_count) {
    result = TURBO_EINVAL;
  }
  if (result == TURBO_OK) {
    result = tr_raft_flowmq_peers_validate(config);
  }
  if (result != TURBO_OK) {
    return result;
  }

  service = (tr_raft_flowmq_peer_service_t *)calloc(1U, sizeof(*service));
  if (service == NULL) {
    return TURBO_ENOMEM;
  }
  service->handshake = config->handshake;
  turbo_mutex_init(&service->inbound_producer_mutex);
  service->peer_count = config->peer_count;
  service->outbound_queue_capacity = config->outbound_queue_capacity;
  service->max_send_batch_items = send_batch_items;
  service->max_send_batch_bytes = send_batch_bytes;
  service->max_inflight_data_bytes =
      config->max_inflight_data_bytes == 0U
          ? TR_RAFT_FLOWMQ_DEFAULT_INFLIGHT_DATA_BYTES
          : config->max_inflight_data_bytes;
  service->inbound_queue_capacity_bytes = config->inbound_queue_capacity_bytes;
  service->on_message = config->on_message;
  service->message_context = config->message_context;
  service->on_snapshot = config->on_snapshot;
  service->snapshot_context = config->snapshot_context;
  atomic_init(&service->callback_depth, 0U);
  atomic_init(&service->inbound_error, TURBO_OK);
  service->send_frames = (tstr *)calloc(send_batch_items, sizeof(*service->send_frames));
  service->send_packets = (uint8_t *)malloc(send_packet_storage_bytes);
  service->inbound_storage = (uint8_t *)malloc(service->inbound_queue_capacity_bytes);
  if (service->send_frames == NULL || service->send_packets == NULL ||
      service->inbound_storage == NULL) {
    turbo_mutex_destroy(&service->inbound_producer_mutex);
    free(service->inbound_storage);
    free(service->send_packets);
    free(service->send_frames);
    free(service);
    return TURBO_ENOMEM;
  }
  if (!ring_spsc_init(&service->inbound, service->inbound_storage,
                      service->inbound_queue_capacity_bytes)) {
    free(service->inbound_storage);
    free(service->send_packets);
    free(service->send_frames);
    turbo_mutex_destroy(&service->inbound_producer_mutex);
    free(service);
    return TURBO_EINVAL;
  }

  router_endpoint = config->router_endpoint;
  if (router_endpoint.stream_recv_buffer_bytes == 0U) {
    router_endpoint.stream_recv_buffer_bytes =
        TR_RAFT_FLOWMQ_DEFAULT_STREAM_RECV_BUFFER_BYTES;
  }
  flowmq_coronet_timeouts_resolve(&router_endpoint.timeouts,
                                  FLOWMQ_ROUTER_ENDPOINT_DEFAULT_TIMEOUT_MS);
  service->router_start_timeout_ns = tr_raft_flowmq_timeout_ns(&router_endpoint.timeouts);
  router_endpoint.context = NULL;
  router_endpoint.drive_context = 1;
  router_endpoint.own_context = 1;
  router_endpoint.on_frame = tr_raft_flowmq_router_frame;
  router_endpoint.on_state = NULL;
  router_endpoint.on_event = tr_raft_flowmq_router_event;
  router_endpoint.callback_ctx = service;
  result = flowmq_router_endpoint_create(&router_endpoint, &service->router);
  if (result != TURBO_OK) {
    turbo_mutex_destroy(&service->inbound_producer_mutex);
    free(service->inbound_storage);
    free(service->send_packets);
    free(service->send_frames);
    free(service);
    return result;
  }

  for (index = 0U; index < service->peer_count; ++index) {
    const tr_raft_flowmq_peer_config_t *source = &config->peers[index];
    tr_raft_flowmq_peer_t *peer = &service->peers[index];
    flowmq_connect_endpoint_config_t dealer_endpoint = source->endpoint;
    if (dealer_endpoint.stream_recv_buffer_bytes == 0U) {
      dealer_endpoint.stream_recv_buffer_bytes =
          TR_RAFT_FLOWMQ_DEFAULT_STREAM_RECV_BUFFER_BYTES;
    }

    peer->service = service;
    peer->index = index;
    peer->node_id = source->node_id;
    flowmq_coronet_timeouts_resolve(&dealer_endpoint.timeouts,
                                    FLOWMQ_CONNECT_ENDPOINT_DEFAULT_TIMEOUT_MS);
    peer->start_timeout_ns = tr_raft_flowmq_timeout_ns(&dealer_endpoint.timeouts);
    peer->reconnect_delay_ms = source->endpoint.reconnect_initial_ms;
    peer->max_frame_size = source->endpoint.max_frame_size;
    peer->next_fmq_message_id = 1U;
    atomic_init(&peer->connector_stop, 0);
    atomic_init(&peer->connector_error, TURBO_OK);
    atomic_init(&peer->dealer_ready, 0);
    memcpy(peer->identity, source->peer_identity, strlen(source->peer_identity) + 1U);
    result = tr_raft_stl_status_to_error(deque_init_bytes(
        &peer->outbound, sizeof(tr_raft_owned_coronet_payload_t),
        _Alignof(tr_raft_owned_coronet_payload_t),
        service->outbound_queue_capacity));
    if (result != TURBO_OK) {
      break;
    }
    turbo_mutex_init(&peer->send_command.mutex);
    turbo_cond_init(&peer->send_command.complete);
    peer->send_command.sync_initialized = 1;
    ++service->initialized_peer_count;
    result = tr_raft_stl_status_to_error(
        deque_reserve(&peer->outbound, service->outbound_queue_capacity));
    if (result != TURBO_OK) {
      break;
    }
    result = tr_raft_flowmq_peer_reset(peer);
    if (result != TURBO_OK) {
      break;
    }

    dealer_endpoint.context = NULL;
    dealer_endpoint.drive_context = 1;
    dealer_endpoint.own_context = 1;
    dealer_endpoint.on_frame = NULL;
    dealer_endpoint.receive_ready = NULL;
    dealer_endpoint.on_state = tr_raft_flowmq_dealer_state;
    dealer_endpoint.on_event = NULL;
    dealer_endpoint.callback_ctx = peer;
    result = flowmq_connect_endpoint_create(&dealer_endpoint, &peer->dealer);
    if (result != TURBO_OK) {
      break;
    }
  }
  if (result != TURBO_OK) {
    tr_raft_flowmq_service_cleanup(service);
    free(service);
    return result;
  }
  *out_service = service;
  return TURBO_OK;
}

int tr_raft_flowmq_peer_service_start(tr_raft_flowmq_peer_service_t *service) {
  size_t index;
  int result;

  if (service == NULL || service->started || service->stopping ||
      atomic_load_explicit(&service->callback_depth, memory_order_acquire) != 0U ||
      service->step_active) {
    return TURBO_EINVAL;
  }
  result = flowmq_router_endpoint_start(service->router, service->router_start_timeout_ns);
  if (result != TURBO_OK) {
    return result;
  }
  service->router_started = 1;
  for (index = 0U; index < service->peer_count; ++index) {
    tr_raft_flowmq_peer_t *peer = &service->peers[index];
    atomic_store_explicit(&peer->connector_stop, 0, memory_order_release);
    atomic_store_explicit(&peer->connector_error, TURBO_OK, memory_order_release);
    atomic_store_explicit(&peer->dealer_ready, 0, memory_order_release);
    result = turbo_thread_create(&peer->connector_thread, tr_raft_flowmq_connector_run, peer);
    if (result != TURBO_OK) {
      break;
    }
    peer->connector_thread_created = 1;
  }
  if (result != TURBO_OK) {
    size_t created;
    for (created = 0U; created < index; ++created) {
      atomic_store_explicit(&service->peers[created].connector_stop, 1, memory_order_release);
      flowmq_connect_endpoint_stop(service->peers[created].dealer);
    }
    for (created = 0U; created < index; ++created) {
      (void)turbo_thread_join(&service->peers[created].connector_thread);
      service->peers[created].connector_thread_created = 0;
      atomic_store_explicit(&service->peers[created].dealer_ready, 0, memory_order_release);
    }
    flowmq_router_endpoint_stop(service->router);
    service->router_started = 0;
    return result;
  }
  service->started = 1;
  return TURBO_OK;
}

static int tr_raft_flowmq_peer_process_packet(tr_raft_flowmq_peer_t *peer, const uint8_t *packet,
                                              size_t packet_size) {
  tr_raft_handshake_message_t handshake_message;

  if (peer == NULL || packet == NULL || packet_size == 0U) {
    return TURBO_EINVAL;
  }
  if (packet_size == TR_RAFT_HANDSHAKE_PACKET_SIZE &&
      tr_raft_handshake_decode(packet, packet_size, &handshake_message) == TURBO_OK) {
    return tr_raft_flowmq_peer_receive_handshake(peer, packet, packet_size);
  }
  if (!peer->handshake_complete || peer->session == NULL) {
    return TURBO_EPROTO;
  }
  return tr_raft_coronet_feed(peer->session, packet, packet_size);
}

static int tr_raft_flowmq_service_drain_ingress(tr_raft_flowmq_peer_service_t *service) {
  int ingress_error = atomic_load_explicit(&service->inbound_error, memory_order_acquire);

  if (ingress_error != TURBO_OK) {
    return ingress_error;
  }
  for (;;) {
    tr_raft_flowmq_ingress_header_t header;
    tr_raft_flowmq_peer_t *peer;
    uint8_t *record;
    size_t available = 0U;
    size_t record_size;
    int result;

    record = ring_spsc_read_acquire(&service->inbound, &available);
    if (record == NULL) {
      return TURBO_OK;
    }
    if (available < sizeof(header)) {
      return TURBO_EPROTO;
    }
    memcpy(&header, record, sizeof(header));
    if (header.reserved != 0U || header.peer_index >= service->peer_count ||
        header.payload_size > TR_RAFT_CORONET_MAX_PACKET_SIZE ||
        (size_t)header.payload_size > SIZE_MAX - sizeof(header)) {
      return TURBO_EPROTO;
    }
    record_size = sizeof(header) + (size_t)header.payload_size;
    if (record_size > available) {
      return TURBO_EPROTO;
    }
    peer = &service->peers[header.peer_index];
    if (header.kind == TR_RAFT_FLOWMQ_INGRESS_RESET && header.payload_size == 0U) {
      result = tr_raft_flowmq_peer_reset(peer);
    } else if (header.kind == TR_RAFT_FLOWMQ_INGRESS_PACKET && header.payload_size != 0U) {
      result =
          tr_raft_flowmq_peer_process_packet(peer, record + sizeof(header), header.payload_size);
    } else {
      result = TURBO_EPROTO;
    }
    ring_spsc_read_release(&service->inbound, record_size);
    if (result != TURBO_OK) {
      peer->faulted = 1;
      service->last_error = result;
      return result;
    }
  }
}

static int tr_raft_flowmq_peer_step(tr_raft_flowmq_peer_t *peer,
                                    tr_raft_flowmq_peer_service_step_result_t *out_result) {
  tr_raft_flowmq_peer_service_t *service = peer->service;
  int result;

  if (peer->faulted) {
    return peer->service->last_error == TURBO_OK ? TURBO_EPROTO : peer->service->last_error;
  }
  result = atomic_load_explicit(&peer->connector_error, memory_order_acquire);
  if (result != TURBO_OK) {
    return result;
  }
  if (!atomic_load_explicit(&peer->dealer_ready, memory_order_acquire)) {
    return TURBO_ENOTCONN;
  }
  while (peer->control.count != 0U) {
    size_t index = peer->control.head;
    uint8_t packet[TR_RAFT_HANDSHAKE_PACKET_SIZE];
    size_t packet_size = peer->control.sizes[index];
    size_t submitted = 0U;

    memcpy(packet, peer->control.packets[index], packet_size);
    tr_raft_flowmq_clear_send_frames(service, 1U);
    result = tr_raft_flowmq_encode_data(peer, packet, packet_size, &service->send_frames[0]);
    if (result == TURBO_OK) {
      result = tr_raft_flowmq_send_frames(peer, service->send_frames, 1U, &submitted);
    }
    tr_raft_flowmq_clear_send_frames(service, 1U);
    if (result != TURBO_OK) {
      return result;
    }
    if (submitted != 1U) {
      return TURBO_EPROTO;
    }
    tr_raft_flowmq_control_pop(&peer->control);
    ++out_result->control_frames_sent;
  }
  if (peer->handshake_complete && peer->session != NULL) {
    size_t available = deque_size(&peer->outbound);
    size_t batch_count =
        available < service->max_send_batch_items ? available : service->max_send_batch_items;
    size_t batch_bytes = 0U;
    size_t submitted = 0U;
    size_t index;

    tr_raft_flowmq_clear_send_frames(service, service->max_send_batch_items);
    for (index = 0U; index < batch_count; ++index) {
      const tr_raft_owned_coronet_payload_t *owned =
          (const tr_raft_owned_coronet_payload_t *)deque_at_const(
              &peer->outbound, index);
      uint8_t *packet = service->send_packets + index * TR_RAFT_CORONET_MAX_PACKET_SIZE;
      size_t packet_size = 0U;

      if (owned == NULL) {
        return TURBO_EPROTO;
      }
      result = tr_raft_coronet_encode_payload_packet(peer->session, &owned->payload, packet,
                                                     TR_RAFT_CORONET_MAX_PACKET_SIZE, &packet_size);
      if (result != TURBO_OK) {
        return result;
      }
      if (packet_size > service->max_send_batch_bytes - batch_bytes) {
        if (index == 0U) {
          return TURBO_EMSGSIZE;
        }
        batch_count = index;
        break;
      }
      result = tr_raft_flowmq_encode_data(peer, packet, packet_size, &service->send_frames[index]);
      if (result != TURBO_OK) {
        tr_raft_flowmq_clear_send_frames(service, index + 1U);
        return result;
      }
      batch_bytes += packet_size;
    }
    if (batch_count > 0U) {
      result = tr_raft_flowmq_send_frames(peer, service->send_frames, batch_count, &submitted);
    } else {
      result = TURBO_OK;
    }
    tr_raft_flowmq_clear_send_frames(service, batch_count);
    if (submitted > batch_count) {
      return TURBO_EPROTO;
    }
    for (index = 0U; index < submitted; ++index) {
      tr_raft_owned_coronet_payload_t discarded;

      if (deque_pop_front(&peer->outbound, &discarded) != STL_OK) {
        return TURBO_EPROTO;
      }
      if (discarded.payload.kind == TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_CHUNK) {
        peer->outbound_snapshot_bytes -= discarded.payload.data.snapshot_chunk.data_length;
      } else if (discarded.payload.kind == TR_RAFT_WIRE_PAYLOAD_DATA_CHUNK) {
        peer->outbound_data_bytes -= discarded.payload.data.data_chunk.data_length;
      }
      tr_raft_owned_coronet_payload_release(&discarded);
    }
    out_result->payload_frames_sent += submitted;
    out_result->payload_batches_sent += submitted != 0U;
    if (result != TURBO_OK) {
      return result;
    }
  }
  return TURBO_OK;
}

int tr_raft_flowmq_peer_service_step(tr_raft_flowmq_peer_service_t *service,
                                     tr_raft_flowmq_peer_service_step_result_t *out_result) {
  size_t index;

  if (service == NULL || out_result == NULL) {
    return TURBO_EINVAL;
  }
  memset(out_result, 0, sizeof(*out_result));
  if (!service->started || service->stopping) {
    return TURBO_EPIPE;
  }
  if (service->step_active) {
    return TURBO_EBUSY;
  }
  service->step_active = 1;
  {
    int result = tr_raft_flowmq_service_drain_ingress(service);
    if (result != TURBO_OK) {
      service->last_error = result;
      service->step_active = 0;
      return result;
    }
  }
  out_result->peer_count = service->peer_count;
  for (index = 0U; index < service->peer_count; ++index) {
    int result = tr_raft_flowmq_peer_step(&service->peers[index], out_result);
    if (result == TURBO_OK) {
      continue;
    }
    if (atomic_load_explicit(&service->peers[index].connector_error, memory_order_acquire) ==
            TURBO_OK &&
        tr_raft_flowmq_send_is_blocked(result)) {
      ++out_result->blocked_peer_count;
    } else {
      ++out_result->failed_peer_count;
      service->last_error = result;
    }
    if (out_result->first_error == TURBO_OK) {
      out_result->first_error = result;
    }
  }
  service->step_active = 0;
  return TURBO_OK;
}

int tr_raft_flowmq_peer_service_stop(tr_raft_flowmq_peer_service_t *service) {
  int first_error = TURBO_OK;

  if (service == NULL) {
    return TURBO_EINVAL;
  }
  if (service->step_active) {
    return TURBO_EBUSY;
  }
  if (!service->started) {
    service->stopping = 1;
    return TURBO_OK;
  }
  service->stopping = 1;
  {
    size_t index;
    for (index = 0U; index < service->peer_count; ++index) {
      atomic_store_explicit(&service->peers[index].connector_stop, 1, memory_order_release);
      flowmq_connect_endpoint_stop(service->peers[index].dealer);
    }
    for (index = 0U; index < service->peer_count; ++index) {
      tr_raft_flowmq_peer_t *peer = &service->peers[index];
      if (peer->connector_thread_created) {
        int result = turbo_thread_join(&peer->connector_thread);
        peer->connector_thread_created = 0;
        if (result != TURBO_OK && first_error == TURBO_OK) {
          first_error = result;
        }
      }
      atomic_store_explicit(&peer->dealer_ready, 0, memory_order_release);
    }
  }
  if (service->router_started) {
    flowmq_router_endpoint_stop(service->router);
    service->router_started = 0;
  }
  service->started = 0;
  if (first_error != TURBO_OK) {
    service->last_error = first_error;
  }
  return first_error;
}

int tr_raft_flowmq_peer_service_destroy(tr_raft_flowmq_peer_service_t *service) {
  if (service == NULL) {
    return TURBO_OK;
  }
  if (service->started ||
      atomic_load_explicit(&service->callback_depth, memory_order_acquire) != 0U ||
      service->step_active) {
    return TURBO_EBUSY;
  }
  tr_raft_flowmq_service_cleanup(service);
  free(service);
  return TURBO_OK;
}

static int tr_raft_flowmq_payload_nodes(const tr_raft_coronet_payload_t *payload,
                                        tr_raft_node_id_t *from, tr_raft_node_id_t *to) {
  if (payload == NULL || from == NULL || to == NULL) {
    return TURBO_EINVAL;
  }
  switch (payload->kind) {
  case TR_RAFT_WIRE_PAYLOAD_RAFT:
    *from = payload->data.raft.from;
    *to = payload->data.raft.to;
    return TURBO_OK;
  case TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_CHUNK:
    *from = payload->data.snapshot_chunk.from;
    *to = payload->data.snapshot_chunk.to;
    return TURBO_OK;
  case TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_ACK:
    *from = payload->data.snapshot_ack.from;
    *to = payload->data.snapshot_ack.to;
    return TURBO_OK;
  case TR_RAFT_WIRE_PAYLOAD_DATA_CHUNK:
    *from = payload->data.data_chunk.from;
    *to = payload->data.data_chunk.to;
    return TURBO_OK;
  case TR_RAFT_WIRE_PAYLOAD_DATA_ACK:
    *from = payload->data.data_ack.from;
    *to = payload->data.data_ack.to;
    return TURBO_OK;
  default:
    return TURBO_EPROTO;
  }
}

int tr_raft_flowmq_peer_service_enqueue(void *context, const tr_raft_message_t *message) {
  tr_raft_coronet_payload_t payload;

  if (message == NULL) {
    return TURBO_EINVAL;
  }
  memset(&payload, 0, sizeof(payload));
  payload.kind = TR_RAFT_WIRE_PAYLOAD_RAFT;
  payload.data.raft = *message;
  return tr_raft_flowmq_peer_service_enqueue_payload((tr_raft_flowmq_peer_service_t *)context,
                                                     &payload);
}

int tr_raft_flowmq_peer_service_enqueue_payload(tr_raft_flowmq_peer_service_t *service,
                                                const tr_raft_coronet_payload_t *payload) {
  tr_raft_owned_coronet_payload_t owned;
  tr_raft_flowmq_peer_t *peer;
  tr_raft_node_id_t from;
  tr_raft_node_id_t to;
  int result;

  if (service == NULL || payload == NULL) {
    return TURBO_EINVAL;
  }
  if (service->stopping) {
    return TURBO_EPIPE;
  }
  result = tr_raft_flowmq_payload_nodes(payload, &from, &to);
  if (result != TURBO_OK || from != service->handshake.local_node_id) {
    return TURBO_EPROTO;
  }
  peer = tr_raft_flowmq_find_peer_node(service, to);
  if (peer == NULL) {
    return TURBO_EPROTO;
  }
  if (deque_size(&peer->outbound) >= service->outbound_queue_capacity) {
    return TURBO_ENOSPC;
  }
  if (payload->kind == TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_CHUNK &&
      (peer->outbound_snapshot_bytes > TR_RAFT_WIRE_MAX_INFLIGHT_SNAPSHOT_BYTES ||
       payload->data.snapshot_chunk.data_length >
           TR_RAFT_WIRE_MAX_INFLIGHT_SNAPSHOT_BYTES - peer->outbound_snapshot_bytes)) {
    return TURBO_ENOSPC;
  }
  if (payload->kind == TR_RAFT_WIRE_PAYLOAD_DATA_CHUNK &&
      (peer->outbound_data_bytes > service->max_inflight_data_bytes ||
       payload->data.data_chunk.data_length >
           service->max_inflight_data_bytes - peer->outbound_data_bytes)) {
    return TURBO_ENOSPC;
  }
  result = tr_raft_owned_coronet_payload_copy(&owned, payload);
  if (result != TURBO_OK) {
    return result;
  }
  result = tr_raft_stl_status_to_error(
      deque_push_back(&peer->outbound, &owned));
  if (result != TURBO_OK) {
    tr_raft_owned_coronet_payload_release(&owned);
  } else if (payload->kind == TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_CHUNK) {
    peer->outbound_snapshot_bytes += payload->data.snapshot_chunk.data_length;
  } else if (payload->kind == TR_RAFT_WIRE_PAYLOAD_DATA_CHUNK) {
    peer->outbound_data_bytes += payload->data.data_chunk.data_length;
  }
  return result;
}

int tr_raft_flowmq_peer_service_get_status(const tr_raft_flowmq_peer_service_t *service,
                                           tr_raft_flowmq_peer_service_status_t *out_status) {
  size_t index;

  if (service == NULL || out_status == NULL) {
    return TURBO_EINVAL;
  }
  memset(out_status, 0, sizeof(*out_status));
  out_status->peer_count = service->peer_count;
  out_status->outbound_queue_capacity = service->outbound_queue_capacity;
  out_status->max_send_batch_items = service->max_send_batch_items;
  out_status->max_send_batch_bytes = service->max_send_batch_bytes;
  out_status->max_inflight_data_bytes = service->max_inflight_data_bytes;
  out_status->inbound_queue_capacity_bytes = service->inbound_queue_capacity_bytes;
  out_status->queued_inbound_bytes = ring_spsc_read_available(&service->inbound);
  out_status->callback_depth = atomic_load_explicit(&service->callback_depth, memory_order_acquire);
  out_status->started = service->started;
  out_status->stopping = service->stopping;
  out_status->step_active = service->step_active;
  out_status->last_error = service->last_error;
  for (index = 0U; index < service->peer_count; ++index) {
    out_status->queued_payload_count +=
        deque_size(&service->peers[index].outbound);
    out_status->queued_data_bytes += service->peers[index].outbound_data_bytes;
    if (service->peers[index].handshake_complete) {
      ++out_status->handshake_complete_count;
    }
  }
  return TURBO_OK;
}
