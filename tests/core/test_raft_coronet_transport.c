#include <turboraft/raft_coronet_transport.h>

#include <tinytest.h>
#include <turbo_error.h>

#include <string.h>

typedef struct received_messages {
    tr_raft_message_t values[2];
    size_t count;
} received_messages_t;

typedef struct received_snapshots {
    tr_raft_coronet_payload_t values[2];
    size_t count;
} received_snapshots_t;

typedef struct reentrant_detach_context {
    tr_raft_coronet_peer_manager_t *manager;
    tr_raft_node_id_t peer_node_id;
    int detach_result;
} reentrant_detach_context_t;

static int collect_message(void *context, const tr_raft_message_t *message)
{
    received_messages_t *received = (received_messages_t *) context;

    if (received->count >= 2U) {
        return TURBO_ENOSPC;
    }
    received->values[received->count++] = *message;
    return TURBO_OK;
}

static int collect_snapshot(void *context,
                            const tr_raft_coronet_payload_t *payload)
{
    received_snapshots_t *received = (received_snapshots_t *) context;

    if (received->count >= 2U) {
        return TURBO_ENOSPC;
    }
    received->values[received->count++] = *payload;
    return TURBO_OK;
}

static int resolve_peer_identity(void *context,
                                 const char *verified_certificate_sha256,
                                 tr_raft_node_id_t *out_peer_node_id)
{
    (void) context;
    (void) verified_certificate_sha256;
    *out_peer_node_id = 2U;
    return TURBO_OK;
}

typedef struct dial_test_context {
    int resolve_count;
    int connect_count;
    int failures_remaining;
} dial_test_context_t;

static int resolve_dial_endpoint(void *context,
                                 tr_raft_node_id_t peer_node_id,
                                 tr_raft_coronet_endpoint_t *out_endpoint)
{
    static const char connect_host[] = "100.64.0.2";
    static const char request_host[] = "node-2.mesh";
    dial_test_context_t *test = (dial_test_context_t *) context;

    if (peer_node_id != 2U) {
        return TURBO_EPROTO;
    }
    ++test->resolve_count;
    memcpy(out_endpoint->connect_host, connect_host, sizeof(connect_host));
    memcpy(out_endpoint->request_host, request_host, sizeof(request_host));
    out_endpoint->port = 7443;
    return TURBO_OK;
}

static int connect_dial_endpoint(
    tr_raft_coronet_peer_manager_t *manager,
    const tr_raft_coronet_outbound_config_t *config,
    tr_raft_node_id_t *out_peer_node_id)
{
    dial_test_context_t *test =
        (dial_test_context_t *) config->admission.message_context;

    (void) manager;
    ++test->connect_count;
    if (test->failures_remaining > 0) {
        --test->failures_remaining;
        return TURBO_EIO;
    }
    *out_peer_node_id = config->admission.expected_peer_node_id;
    return TURBO_OK;
}

static int retry_dial_io(void *context, int error_code)
{
    (void) context;
    return error_code == TURBO_EIO;
}

typedef struct inbound_test_context {
    tr_raft_coronet_inbound_service_t *service;
    int admission_result;
    int result_count;
    int destroy_result;
    tr_raft_node_id_t result_peer_node_id;
} inbound_test_context_t;

static int admit_inbound_test_socket(
    tr_raft_coronet_peer_manager_t *manager,
    coro_socket_t *socket,
    const tr_raft_coronet_owned_socket_admission_config_t *config,
    tr_raft_node_id_t *out_peer_node_id)
{
    inbound_test_context_t *test =
        (inbound_test_context_t *) config->message_context;

    (void) manager;
    (void) socket;
    if (test->admission_result == TURBO_OK) {
        *out_peer_node_id = 2U;
    }
    return test->admission_result;
}

static void collect_inbound_result(void *context,
                                   int result,
                                   tr_raft_node_id_t peer_node_id)
{
    inbound_test_context_t *test = (inbound_test_context_t *) context;

    ++test->result_count;
    test->admission_result = result;
    test->result_peer_node_id = peer_node_id;
    test->destroy_result =
        tr_raft_coronet_inbound_service_destroy(test->service);
}

static int try_reentrant_detach(void *context,
                                const tr_raft_message_t *message)
{
    reentrant_detach_context_t *detach =
        (reentrant_detach_context_t *) context;
    tr_raft_coronet_session_t *session = NULL;

    (void) message;
    detach->detach_result = tr_raft_coronet_peer_manager_detach(
        detach->manager, detach->peer_node_id, &session);
    return TURBO_OK;
}

static tr_raft_coronet_session_t *make_session(
    uint8_t cluster_seed,
    tr_raft_node_id_t local_node_id,
    tr_raft_node_id_t peer_node_id,
    received_messages_t *received)
{
    tr_raft_coronet_session_config_t config;
    tr_raft_coronet_session_t *session = NULL;
    size_t index;

    memset(&config, 0, sizeof(config));
    for (index = 0U; index < sizeof(config.cluster_id.bytes); ++index) {
        config.cluster_id.bytes[index] = (uint8_t) (cluster_seed + index);
    }
    config.local_node_id = local_node_id;
    config.peer_node_id = peer_node_id;
    config.first_outbound_message_id = 1U;
    config.on_message = collect_message;
    config.message_context = received;
    check_int_eq(tr_raft_coronet_session_create(&config, &session), TURBO_OK);
    return session;
}

static tr_raft_coronet_session_t *make_connected_session(
    uint8_t cluster_seed,
    tr_raft_node_id_t local_node_id,
    tr_raft_node_id_t peer_node_id,
    received_messages_t *received)
{
    tr_raft_coronet_session_config_t config;
    tr_raft_handshake_result_t handshake;
    tr_raft_coronet_session_t *session = NULL;
    size_t index;

    memset(&config, 0, sizeof(config));
    memset(&handshake, 0, sizeof(handshake));
    config.socket = (coro_socket_t *) (uintptr_t) 1U;
    for (index = 0U; index < sizeof(config.cluster_id.bytes); ++index) {
        config.cluster_id.bytes[index] = (uint8_t) (cluster_seed + index);
    }
    config.local_node_id = local_node_id;
    config.peer_node_id = peer_node_id;
    config.first_outbound_message_id = 1U;
    handshake.complete = 1;
    handshake.feature_bits = TR_RAFT_HANDSHAKE_FEATURE_CURRENT;
    handshake.cluster_id = config.cluster_id;
    handshake.local_node_id = local_node_id;
    handshake.peer_node_id = peer_node_id;
    handshake.peer_process_incarnation.bytes[0] = 1U;
    handshake.wire_major = TR_RAFT_HANDSHAKE_WIRE_MAJOR;
    handshake.wire_minor = TR_RAFT_HANDSHAKE_WIRE_MINOR;
    handshake.max_frame_size = TR_RAFT_HANDSHAKE_MIN_FRAME_SIZE;
    handshake.max_snapshot_chunk_size =
        TR_RAFT_HANDSHAKE_MIN_SNAPSHOT_CHUNK_SIZE;
    config.handshake = &handshake;
    config.on_message = collect_message;
    config.message_context = received;
    check_int_eq(tr_raft_coronet_session_create(&config, &session), TURBO_OK);
    return session;
}

static tr_raft_message_t make_heartbeat(tr_raft_node_id_t from,
                                        tr_raft_node_id_t to,
                                        tr_raft_term_t term)
{
    tr_raft_message_t message;

    memset(&message, 0, sizeof(message));
    message.type = TR_RAFT_MSG_HEARTBEAT_REQUEST;
    message.from = from;
    message.to = to;
    message.term = term;
    return message;
}

spec("raft CoroNet transport")
{
    it("routes snapshot chunks and acknowledgements by payload kind")
    {
        tr_raft_coronet_session_config_t sender_config;
        tr_raft_coronet_session_config_t receiver_config;
        tr_raft_coronet_session_t *sender = NULL;
        tr_raft_coronet_session_t *receiver = NULL;
        received_messages_t sender_messages;
        received_messages_t receiver_messages;
        received_snapshots_t sender_snapshots;
        received_snapshots_t receiver_snapshots;
        tr_raft_coronet_payload_t payload;
        uint8_t packet[TR_RAFT_CORONET_MAX_PACKET_SIZE];
        size_t packet_size = 0U;
        size_t index;

        memset(&sender_config, 0, sizeof(sender_config));
        memset(&receiver_config, 0, sizeof(receiver_config));
        memset(&sender_messages, 0, sizeof(sender_messages));
        memset(&receiver_messages, 0, sizeof(receiver_messages));
        memset(&sender_snapshots, 0, sizeof(sender_snapshots));
        memset(&receiver_snapshots, 0, sizeof(receiver_snapshots));
        for (index = 0U; index < sizeof(sender_config.cluster_id.bytes);
             ++index) {
            sender_config.cluster_id.bytes[index] = (uint8_t) (81U + index);
        }
        receiver_config.cluster_id = sender_config.cluster_id;
        sender_config.local_node_id = 1U;
        sender_config.peer_node_id = 2U;
        sender_config.first_outbound_message_id = 1U;
        sender_config.on_message = collect_message;
        sender_config.message_context = &sender_messages;
        sender_config.on_snapshot = collect_snapshot;
        sender_config.snapshot_context = &sender_snapshots;
        receiver_config.local_node_id = 2U;
        receiver_config.peer_node_id = 1U;
        receiver_config.first_outbound_message_id = 1U;
        receiver_config.on_message = collect_message;
        receiver_config.message_context = &receiver_messages;
        receiver_config.on_snapshot = collect_snapshot;
        receiver_config.snapshot_context = &receiver_snapshots;
        check_int_eq(tr_raft_coronet_session_create(&sender_config, &sender),
                     TURBO_OK);
        check_int_eq(tr_raft_coronet_session_create(&receiver_config,
                                                     &receiver),
                     TURBO_OK);

        memset(&payload, 0, sizeof(payload));
        payload.kind = TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_CHUNK;
        payload.data.snapshot_chunk.from = 1U;
        payload.data.snapshot_chunk.to = 2U;
        payload.data.snapshot_chunk.term = 3U;
        payload.data.snapshot_chunk.snapshot_index = 4U;
        payload.data.snapshot_chunk.snapshot_term = 2U;
        payload.data.snapshot_chunk.snapshot_size = 3U;
        payload.data.snapshot_chunk.has_configuration = true;
        payload.data.snapshot_chunk.configuration.phase = TR_RAFT_CONF_FINAL;
        payload.data.snapshot_chunk.configuration.member_count = 1U;
        payload.data.snapshot_chunk.configuration.members[0].node_id = 2U;
        payload.data.snapshot_chunk.configuration.members[0].roles =
            TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER;
        payload.data.snapshot_chunk.data_length = 3U;
        payload.data.snapshot_chunk.done = true;
        memset(payload.data.snapshot_chunk.snapshot_digest, 1,
               sizeof(payload.data.snapshot_chunk.snapshot_digest));
        memcpy(payload.data.snapshot_chunk.data, "abc", 3U);
        check_int_eq(tr_raft_coronet_encode_payload_packet(
                         sender, &payload, packet, sizeof(packet),
                         &packet_size),
                     TURBO_OK);
        check_int_eq(tr_raft_coronet_feed(receiver, packet, packet_size),
                     TURBO_OK);
        check_size_eq(receiver_snapshots.count, 1U);
        check_int_eq(receiver_snapshots.values[0].kind,
                     TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_CHUNK);
        check_mem_eq(receiver_snapshots.values[0].data.snapshot_chunk.data,
                     "abc", 3U);

        memset(&payload, 0, sizeof(payload));
        payload.kind = TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_ACK;
        payload.data.snapshot_ack.from = 2U;
        payload.data.snapshot_ack.to = 1U;
        payload.data.snapshot_ack.term = 3U;
        payload.data.snapshot_ack.snapshot_index = 4U;
        payload.data.snapshot_ack.snapshot_size = 3U;
        payload.data.snapshot_ack.next_offset = 3U;
        payload.data.snapshot_ack.accepted = true;
        memset(payload.data.snapshot_ack.snapshot_digest, 1,
               sizeof(payload.data.snapshot_ack.snapshot_digest));
        check_int_eq(tr_raft_coronet_encode_payload_packet(
                         receiver, &payload, packet, sizeof(packet),
                         &packet_size),
                     TURBO_OK);
        check_int_eq(tr_raft_coronet_feed(sender, packet, packet_size),
                     TURBO_OK);
        check_size_eq(sender_snapshots.count, 1U);
        check_int_eq(sender_snapshots.values[0].kind,
                     TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_ACK);
        check_long_eq(sender_snapshots.values[0].data.snapshot_ack.next_offset,
                      3U);
        tr_raft_coronet_session_destroy(receiver);
        tr_raft_coronet_session_destroy(sender);
    }
    it("dispatches fragmented and coalesced packets")
    {
        received_messages_t sender_received;
        received_messages_t receiver_received;
        tr_raft_coronet_session_t *sender;
        tr_raft_coronet_session_t *receiver;
        tr_raft_message_t first;
        tr_raft_message_t second;
        uint8_t packets[TR_RAFT_CORONET_MAX_PACKET_SIZE * 2U];
        size_t first_size = 0U;
        size_t second_size = 0U;

        memset(&sender_received, 0, sizeof(sender_received));
        memset(&receiver_received, 0, sizeof(receiver_received));
        sender = make_session(7U, 1U, 2U, &sender_received);
        receiver = make_session(7U, 2U, 1U, &receiver_received);
        first = make_heartbeat(1U, 2U, 4U);
        second = make_heartbeat(1U, 2U, 5U);

        check_int_eq(tr_raft_coronet_encode_packet(
                         sender, &first, packets,
                         TR_RAFT_CORONET_MAX_PACKET_SIZE, &first_size),
                     TURBO_OK);
        check_int_eq(tr_raft_coronet_encode_packet(
                         sender, &second, packets + first_size,
                         sizeof(packets) - first_size, &second_size),
                     TURBO_OK);
        check_int_eq(tr_raft_coronet_feed(receiver, packets, 2U), TURBO_OK);
        check_int_eq(tr_raft_coronet_feed(
                         receiver, packets + 2U,
                         first_size + second_size - 2U),
                     TURBO_OK);
        check_size_eq(receiver_received.count, 2U);
        check_long_eq(receiver_received.values[0].term, 4U);
        check_long_eq(receiver_received.values[1].term, 5U);

        tr_raft_coronet_session_destroy(receiver);
        tr_raft_coronet_session_destroy(sender);
    }

    it("faults a session on a wrong cluster id")
    {
        received_messages_t sender_received;
        received_messages_t receiver_received;
        tr_raft_coronet_session_t *sender;
        tr_raft_coronet_session_t *receiver;
        tr_raft_message_t message;
        tr_raft_coronet_status_t status;
        uint8_t packet[TR_RAFT_CORONET_MAX_PACKET_SIZE];
        size_t packet_size = 0U;

        memset(&sender_received, 0, sizeof(sender_received));
        memset(&receiver_received, 0, sizeof(receiver_received));
        sender = make_session(11U, 1U, 2U, &sender_received);
        receiver = make_session(12U, 2U, 1U, &receiver_received);
        message = make_heartbeat(1U, 2U, 6U);

        check_int_eq(tr_raft_coronet_encode_packet(
                         sender, &message, packet, sizeof(packet), &packet_size),
                     TURBO_OK);
        check_int_eq(tr_raft_coronet_feed(receiver, packet, packet_size),
                     TURBO_EPROTO);
        check_int_eq(tr_raft_coronet_get_status(receiver, &status), TURBO_OK);
        check_int_eq(status.state, TR_RAFT_CORONET_STATE_FAULTED);
        check_size_eq(receiver_received.count, 0U);

        tr_raft_coronet_session_destroy(receiver);
        tr_raft_coronet_session_destroy(sender);
    }

    it("faults a session on a replayed message id")
    {
        received_messages_t sender_received;
        received_messages_t receiver_received;
        tr_raft_coronet_session_t *sender;
        tr_raft_coronet_session_t *receiver;
        tr_raft_message_t message;
        uint8_t packet[TR_RAFT_CORONET_MAX_PACKET_SIZE];
        size_t packet_size = 0U;

        memset(&sender_received, 0, sizeof(sender_received));
        memset(&receiver_received, 0, sizeof(receiver_received));
        sender = make_session(21U, 1U, 2U, &sender_received);
        receiver = make_session(21U, 2U, 1U, &receiver_received);
        message = make_heartbeat(1U, 2U, 7U);

        check_int_eq(tr_raft_coronet_encode_packet(
                         sender, &message, packet, sizeof(packet), &packet_size),
                     TURBO_OK);
        check_int_eq(tr_raft_coronet_feed(receiver, packet, packet_size),
                     TURBO_OK);
        check_int_eq(tr_raft_coronet_feed(receiver, packet, packet_size),
                     TURBO_EPROTO);

        tr_raft_coronet_session_destroy(receiver);
        tr_raft_coronet_session_destroy(sender);
    }

    it("owns and reports bounded peer sessions")
    {
        const tr_raft_node_id_t peer_ids[] = {2U, 3U};
        tr_raft_coronet_peer_manager_config_t manager_config;
        tr_raft_coronet_peer_manager_t *manager = NULL;
        tr_raft_coronet_peer_manager_status_t manager_status;
        tr_raft_coronet_peer_status_t peer_status;
        tr_raft_coronet_session_t *session;
        tr_raft_coronet_session_t *detached = NULL;
        received_messages_t received;
        size_t index;

        memset(&received, 0, sizeof(received));
        memset(&manager_config, 0, sizeof(manager_config));
        for (index = 0U; index < sizeof(manager_config.cluster_id.bytes);
             ++index) {
            manager_config.cluster_id.bytes[index] = (uint8_t) (31U + index);
        }
        manager_config.local_node_id = 1U;
        manager_config.peer_node_ids = peer_ids;
        manager_config.peer_count = 2U;
        check_int_eq(tr_raft_coronet_peer_manager_create(&manager_config,
                                                          &manager),
                     TURBO_OK);

        session = make_session(31U, 1U, 2U, &received);
        check_int_eq(tr_raft_coronet_peer_manager_attach(manager, session),
                     TURBO_OK);
        check_int_eq(tr_raft_coronet_peer_manager_get_status(
                         manager, &manager_status),
                     TURBO_OK);
        check_size_eq(manager_status.configured_count, 2U);
        check_size_eq(manager_status.attached_count, 1U);
        check_size_eq(manager_status.detached_count, 1U);
        check_size_eq(manager_status.connected_count, 0U);
        check_int_eq(tr_raft_coronet_peer_manager_get_peer_status(
                         manager, 2U, &peer_status),
                     TURBO_OK);
        check_int_eq(peer_status.attached, 1);
        check_int_eq(peer_status.session.state,
                     TR_RAFT_CORONET_STATE_DETACHED);

        check_int_eq(tr_raft_coronet_peer_manager_detach(
                         manager, 2U, &detached),
                     TURBO_OK);
        tr_raft_coronet_session_destroy(detached);
        tr_raft_coronet_peer_manager_destroy(manager);
    }

    it("rejects a session outside the configured cluster")
    {
        const tr_raft_node_id_t peer_ids[] = {2U};
        tr_raft_coronet_peer_manager_config_t manager_config;
        tr_raft_coronet_peer_manager_t *manager = NULL;
        tr_raft_coronet_session_t *session;
        received_messages_t received;
        size_t index;

        memset(&received, 0, sizeof(received));
        memset(&manager_config, 0, sizeof(manager_config));
        for (index = 0U; index < sizeof(manager_config.cluster_id.bytes);
             ++index) {
            manager_config.cluster_id.bytes[index] = (uint8_t) (41U + index);
        }
        manager_config.local_node_id = 1U;
        manager_config.peer_node_ids = peer_ids;
        manager_config.peer_count = 1U;
        check_int_eq(tr_raft_coronet_peer_manager_create(&manager_config,
                                                          &manager),
                     TURBO_OK);

        session = make_session(42U, 1U, 2U, &received);
        check_int_eq(tr_raft_coronet_peer_manager_attach(manager, session),
                     TURBO_EPROTO);
        tr_raft_coronet_session_destroy(session);
        tr_raft_coronet_peer_manager_destroy(manager);
    }

    it("admits only the deterministic connection direction")
    {
        const tr_raft_node_id_t peer_ids[] = {2U};
        tr_raft_coronet_peer_manager_config_t manager_config;
        tr_raft_coronet_peer_manager_t *manager = NULL;
        tr_raft_coronet_session_t *accepted;
        tr_raft_coronet_session_t *wrong_direction;
        tr_raft_coronet_session_t *duplicate;
        tr_raft_coronet_connection_direction_t direction;
        received_messages_t received;
        size_t index;

        memset(&received, 0, sizeof(received));
        memset(&manager_config, 0, sizeof(manager_config));
        for (index = 0U; index < sizeof(manager_config.cluster_id.bytes);
             ++index) {
            manager_config.cluster_id.bytes[index] = (uint8_t) (51U + index);
        }
        manager_config.local_node_id = 1U;
        manager_config.peer_node_ids = peer_ids;
        manager_config.peer_count = 1U;
        check_int_eq(tr_raft_coronet_peer_manager_create(&manager_config,
                                                          &manager),
                     TURBO_OK);
        check_int_eq(tr_raft_coronet_expected_direction(1U, 2U, &direction),
                     TURBO_OK);
        check_int_eq(direction, TR_RAFT_CORONET_CONNECTION_OUTBOUND);
        check_int_eq(tr_raft_coronet_expected_direction(2U, 1U, &direction),
                     TURBO_OK);
        check_int_eq(direction, TR_RAFT_CORONET_CONNECTION_INBOUND);

        wrong_direction = make_connected_session(51U, 1U, 2U, &received);
        check_int_eq(tr_raft_coronet_peer_manager_admit(
                         manager, TR_RAFT_CORONET_CONNECTION_INBOUND,
                         wrong_direction),
                     TURBO_EPROTO);
        tr_raft_coronet_session_destroy(wrong_direction);

        accepted = make_connected_session(51U, 1U, 2U, &received);
        check_int_eq(tr_raft_coronet_peer_manager_admit(
                         manager, TR_RAFT_CORONET_CONNECTION_OUTBOUND,
                         accepted),
                     TURBO_OK);
        duplicate = make_connected_session(51U, 1U, 2U, &received);
        check_int_eq(tr_raft_coronet_peer_manager_admit(
                         manager, TR_RAFT_CORONET_CONNECTION_OUTBOUND,
                         duplicate),
                     TURBO_EPROTO);
        tr_raft_coronet_session_destroy(duplicate);
        tr_raft_coronet_peer_manager_destroy(manager);
    }

    it("rejects socket ownership without a socket")
    {
        tr_raft_coronet_session_config_t config;
        tr_raft_coronet_session_t *session = NULL;
        received_messages_t received;
        size_t index;

        memset(&received, 0, sizeof(received));
        memset(&config, 0, sizeof(config));
        for (index = 0U; index < sizeof(config.cluster_id.bytes); ++index) {
            config.cluster_id.bytes[index] = (uint8_t) (61U + index);
        }
        config.owns_socket = 1;
        config.local_node_id = 1U;
        config.peer_node_id = 2U;
        config.first_outbound_message_id = 1U;
        config.on_message = collect_message;
        config.message_context = &received;
        check_int_eq(tr_raft_coronet_session_create(&config, &session),
                     TURBO_EINVAL);
    }

    it("validates fail-closed outbound TLS configuration")
    {
        tr_raft_coronet_outbound_config_t config;

        memset(&config, 0, sizeof(config));
        config.context = (coro_context_t *) (uintptr_t) 1;
        config.connect_host = "100.64.0.2";
        config.request_host = "node-2.mesh";
        config.port = 7443;
        config.connect_timeout_ms = 5000U;
        config.tls.verify_peer = 1;
        config.admission.handshake.handshake =
            (const tr_raft_handshake_config_t *) (uintptr_t) 1;
        config.admission.handshake.timeout_ms = 3000U;
        config.admission.handshake.resolve_peer_identity =
            resolve_peer_identity;
        config.admission.direction = TR_RAFT_CORONET_CONNECTION_OUTBOUND;
        config.admission.expected_peer_node_id = 2U;
        config.admission.first_outbound_message_id = 1U;
        config.admission.peer_idle_timeout_ms = 10000U;
        config.admission.on_message = collect_message;

        check_int_eq(tr_raft_coronet_outbound_config_validate(&config),
                     TURBO_OK);
        config.tls.verify_peer = 0;
        check_int_eq(tr_raft_coronet_outbound_config_validate(&config),
                     TURBO_EINVAL);
        config.tls.verify_peer = 1;
        config.request_host = NULL;
        check_int_eq(tr_raft_coronet_outbound_config_validate(&config),
                     TURBO_EINVAL);
        config.request_host = "node-2.mesh";
        config.admission.direction = TR_RAFT_CORONET_CONNECTION_INBOUND;
        check_int_eq(tr_raft_coronet_outbound_config_validate(&config),
                     TURBO_EINVAL);
    }

    it("adopts inbound sockets and reports admission results")
    {
        tr_raft_coronet_inbound_service_config_t config;
        tr_raft_coronet_inbound_service_t *service = NULL;
        tr_raft_coronet_inbound_status_t status;
        inbound_test_context_t test;

        memset(&config, 0, sizeof(config));
        memset(&status, 0, sizeof(status));
        memset(&test, 0, sizeof(test));
        config.manager =
            (tr_raft_coronet_peer_manager_t *) (uintptr_t) 1;
        config.admission.handshake.handshake =
            (const tr_raft_handshake_config_t *) (uintptr_t) 1;
        config.admission.handshake.timeout_ms = 3000U;
        config.admission.handshake.resolve_peer_identity =
            resolve_peer_identity;
        config.admission.direction = TR_RAFT_CORONET_CONNECTION_INBOUND;
        config.admission.first_outbound_message_id = 1U;
        config.admission.peer_idle_timeout_ms = 10000U;
        config.admission.on_message = collect_message;
        config.admission.message_context = &test;
        config.admit_owned_socket = admit_inbound_test_socket;
        config.on_result = collect_inbound_result;
        config.result_context = &test;

        check_int_eq(tr_raft_coronet_inbound_service_config_validate(&config),
                     TURBO_OK);
        check_int_eq(tr_raft_coronet_inbound_service_create(&config, &service),
                     TURBO_OK);
        test.service = service;
        test.admission_result = TURBO_OK;
        tr_raft_coronet_inbound_service_handle(
            (coro_socket_t *) (uintptr_t) 1, service);
        check_int_eq(test.result_count, 1);
        check_int_eq(test.result_peer_node_id, 2);
        check_int_eq(test.destroy_result, TURBO_EBUSY);

        test.admission_result = TURBO_EPROTO;
        tr_raft_coronet_inbound_service_handle(
            (coro_socket_t *) (uintptr_t) 1, service);
        check_int_eq(test.result_count, 2);
        check_int_eq(test.result_peer_node_id, 0);
        check_int_eq(tr_raft_coronet_inbound_service_get_status(service,
                                                                &status),
                     TURBO_OK);
        check_long_eq(status.accepted_socket_count, 2U);
        check_long_eq(status.admitted_socket_count, 1U);
        check_long_eq(status.rejected_socket_count, 1U);
        check_int_eq(status.active_admission_count, 0);
        check_int_eq(status.last_error, TURBO_EPROTO);
        check_int_eq(tr_raft_coronet_inbound_service_destroy(service),
                     TURBO_OK);

        config.admission.expected_peer_node_id = 2U;
        check_int_eq(tr_raft_coronet_inbound_service_config_validate(&config),
                     TURBO_EINVAL);
    }

    it("refreshes endpoints and applies bounded dial backoff")
    {
        tr_raft_coronet_dial_scheduler_config_t config;
        tr_raft_coronet_dial_scheduler_t *scheduler = NULL;
        tr_raft_coronet_dial_status_t status;
        dial_test_context_t test;

        memset(&config, 0, sizeof(config));
        memset(&status, 0, sizeof(status));
        memset(&test, 0, sizeof(test));
        config.manager =
            (tr_raft_coronet_peer_manager_t *) (uintptr_t) 1;
        config.outbound.context = (coro_context_t *) (uintptr_t) 1;
        config.outbound.connect_timeout_ms = 5000U;
        config.outbound.tls.verify_peer = 1;
        config.outbound.admission.handshake.handshake =
            (const tr_raft_handshake_config_t *) (uintptr_t) 1;
        config.outbound.admission.handshake.timeout_ms = 3000U;
        config.outbound.admission.handshake.resolve_peer_identity =
            resolve_peer_identity;
        config.outbound.admission.direction =
            TR_RAFT_CORONET_CONNECTION_OUTBOUND;
        config.outbound.admission.expected_peer_node_id = 2U;
        config.outbound.admission.first_outbound_message_id = 1U;
        config.outbound.admission.peer_idle_timeout_ms = 10000U;
        config.outbound.admission.on_message = collect_message;
        config.outbound.admission.message_context = &test;
        config.resolve_endpoint = resolve_dial_endpoint;
        config.resolve_context = &test;
        config.connect_outbound = connect_dial_endpoint;
        config.is_retryable = retry_dial_io;
        config.initial_retry_delay_ms = 10U;
        config.max_retry_delay_ms = 20U;
        config.max_attempts = 3U;
        test.failures_remaining = 2;

        check_int_eq(tr_raft_coronet_dial_scheduler_create(&config,
                                                            &scheduler),
                     TURBO_OK);
        check_int_eq(tr_raft_coronet_dial_scheduler_step(scheduler, 100U),
                     TURBO_EIO);
        check_int_eq(tr_raft_coronet_dial_scheduler_get_status(scheduler,
                                                               &status),
                     TURBO_OK);
        check_int_eq(status.state, TR_RAFT_CORONET_DIAL_WAITING);
        check_int_eq(status.attempt_count, 1);
        check_long_eq(status.next_attempt_ms, 110U);
        check_int_eq(tr_raft_coronet_dial_scheduler_step(scheduler, 109U),
                     TURBO_EBUSY);
        check_int_eq(test.connect_count, 1);

        check_int_eq(tr_raft_coronet_dial_scheduler_step(scheduler, 110U),
                     TURBO_EIO);
        check_int_eq(tr_raft_coronet_dial_scheduler_get_status(scheduler,
                                                               &status),
                     TURBO_OK);
        check_long_eq(status.next_attempt_ms, 130U);
        check_int_eq(tr_raft_coronet_dial_scheduler_step(scheduler, 130U),
                     TURBO_OK);
        check_int_eq(tr_raft_coronet_dial_scheduler_get_status(scheduler,
                                                               &status),
                     TURBO_OK);
        check_int_eq(status.state, TR_RAFT_CORONET_DIAL_CONNECTED);
        check_int_eq(status.attempt_count, 3);
        check_int_eq(test.resolve_count, 3);
        check_int_eq(test.connect_count, 3);

        test.failures_remaining = 3;
        check_int_eq(tr_raft_coronet_dial_scheduler_reset(scheduler, 200U),
                     TURBO_OK);
        check_int_eq(tr_raft_coronet_dial_scheduler_step(scheduler, 200U),
                     TURBO_EIO);
        check_int_eq(tr_raft_coronet_dial_scheduler_step(scheduler, 210U),
                     TURBO_EIO);
        check_int_eq(tr_raft_coronet_dial_scheduler_step(scheduler, 230U),
                     TURBO_EIO);
        check_int_eq(tr_raft_coronet_dial_scheduler_get_status(scheduler,
                                                               &status),
                     TURBO_OK);
        check_int_eq(status.state, TR_RAFT_CORONET_DIAL_EXHAUSTED);
        check_int_eq(status.attempt_count, 3);
        check_int_eq(tr_raft_coronet_dial_scheduler_step(scheduler, 250U),
                     TURBO_EBUSY);
        tr_raft_coronet_dial_scheduler_destroy(scheduler);
    }

    it("rejects manager lifecycle mutation inside message callback")
    {
        const tr_raft_node_id_t peer_ids[] = {1U};
        tr_raft_coronet_peer_manager_config_t manager_config;
        tr_raft_coronet_peer_manager_t *manager = NULL;
        tr_raft_coronet_session_config_t receiver_config;
        tr_raft_coronet_session_t *receiver = NULL;
        tr_raft_coronet_session_t *detached = NULL;
        tr_raft_coronet_session_t *sender;
        reentrant_detach_context_t callback_context;
        received_messages_t sender_received;
        tr_raft_message_t message;
        uint8_t packet[TR_RAFT_CORONET_MAX_PACKET_SIZE];
        size_t packet_size = 0U;
        size_t index;

        memset(&sender_received, 0, sizeof(sender_received));
        memset(&manager_config, 0, sizeof(manager_config));
        memset(&receiver_config, 0, sizeof(receiver_config));
        memset(&callback_context, 0, sizeof(callback_context));
        for (index = 0U; index < sizeof(manager_config.cluster_id.bytes);
             ++index) {
            manager_config.cluster_id.bytes[index] = (uint8_t) (71U + index);
        }
        manager_config.local_node_id = 2U;
        manager_config.peer_node_ids = peer_ids;
        manager_config.peer_count = 1U;
        check_int_eq(tr_raft_coronet_peer_manager_create(&manager_config,
                                                          &manager),
                     TURBO_OK);

        receiver_config.cluster_id = manager_config.cluster_id;
        receiver_config.local_node_id = 2U;
        receiver_config.peer_node_id = 1U;
        receiver_config.first_outbound_message_id = 1U;
        receiver_config.on_message = try_reentrant_detach;
        receiver_config.message_context = &callback_context;
        check_int_eq(tr_raft_coronet_session_create(&receiver_config,
                                                     &receiver),
                     TURBO_OK);
        check_int_eq(tr_raft_coronet_peer_manager_attach(manager, receiver),
                     TURBO_OK);
        callback_context.manager = manager;
        callback_context.peer_node_id = 1U;
        sender = make_session(71U, 1U, 2U, &sender_received);
        message = make_heartbeat(1U, 2U, 9U);
        check_int_eq(tr_raft_coronet_encode_packet(
                         sender, &message, packet, sizeof(packet), &packet_size),
                     TURBO_OK);
        check_int_eq(tr_raft_coronet_feed(receiver, packet, packet_size),
                     TURBO_OK);
        check_int_eq(callback_context.detach_result, TURBO_EBUSY);
        check_int_eq(tr_raft_coronet_peer_manager_detach(
                         manager, 1U, &detached),
                     TURBO_OK);

        tr_raft_coronet_session_destroy(detached);
        tr_raft_coronet_session_destroy(sender);
        tr_raft_coronet_peer_manager_destroy(manager);
    }
}
