#include <turboraft/raft_cnet_channel.h>

#include <cmeta_error.h>
#include <tinytest.h>

#include <stdio.h>
#include <string.h>

#ifndef TURBORAFT_ACE23_FIXTURE_DIR
#error "Set the directory containing the checked-in loopback-only TLS fixtures"
#endif

#define CERT_NODE1 "eb571a92b33237897216c79501066b3e77391047eaeb6be084526c4769657549"
#define CERT_NODE2 "44e8fe3ce37ede1c2a1b5d36efa345cb662887d4250d17a000ebea2e834aed95"

enum peer_mode {
    PEER_VALID = 0,
    PEER_STREAMS,
    PEER_FOREIGN_CLUSTER,
    PEER_FORGED_NODE,
    PEER_UNAUTHORIZED_CERT
};

typedef struct peer_sink {
    tr_raft_node_id_t expected_from;
    tr_raft_node_id_t expected_to;
    size_t count;
    size_t raft_count;
    size_t data_count;
    size_t snapshot_count;
    int violation;
} peer_sink_t;

typedef struct peer_fixture {
    cnet_client client;
    cnet_client server;
    cnet_listener listener;
    cnet_tls_server tls_server;
    tr_raft_cnet_channel_t *client_channel;
    tr_raft_cnet_channel_t *server_channel;
    cnet_connection outbound;
    cnet_connection inbound;
    peer_sink_t client_sink;
    peer_sink_t server_sink;
    tr_raft_cnet_peer_identity_t client_peer;
    tr_raft_cnet_peer_identity_t server_peer;
    tr_raft_cnet_identity_policy_t client_policy;
    tr_raft_cnet_identity_policy_t server_policy;
    const char *client_fingerprints[1];
    const char *server_fingerprints[1];
    int client_open;
    int server_open;
    int listener_open;
    int tls_open;
} peer_fixture_t;

static tr_raft_handshake_config_t peer_handshake(
    tr_raft_node_id_t local_id, int foreign_cluster)
{
    tr_raft_handshake_config_t config = {0};
    size_t i;
    for (i = 0U; i < sizeof(config.cluster_id.bytes); ++i)
        config.cluster_id.bytes[i] = (uint8_t)(i + 17U);
    if (foreign_cluster) config.cluster_id.bytes[0] ^= 0x55U;
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

static cnet_client_config peer_client_config(void)
{
    const cnet_client_config config = {
#if defined(_WIN32)
        .backend = NATIVE_IO_BACKEND_IOCP,
#elif defined(__linux__)
        .backend = NATIVE_IO_BACKEND_EPOLL,
#else
        .backend = NATIVE_IO_BACKEND_KQUEUE,
#endif
        .connection_capacity = 2U,
        .command_capacity = 16U,
        .request_capacity = 8U,
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
    return config;
}

static int peer_record(void *context,
                       const tr_raft_transport_payload_t *payload)
{
    peer_sink_t *sink = (peer_sink_t *)context;
    if (payload == NULL) goto invalid;

    if (payload->kind == TR_RAFT_WIRE_PAYLOAD_RAFT &&
        payload->group_id == 42U &&
        payload->data.raft.from == sink->expected_from &&
        payload->data.raft.to == sink->expected_to &&
        (payload->data.raft.type == TR_RAFT_MSG_HEARTBEAT_REQUEST ||
         payload->data.raft.type == TR_RAFT_MSG_HEARTBEAT_RESPONSE)) {
        ++sink->raft_count;
    } else if (payload->kind == TR_RAFT_WIRE_PAYLOAD_DATA_CHUNK &&
               payload->group_id == 43U &&
               payload->data.data_chunk.from == sink->expected_from &&
               payload->data.data_chunk.to == sink->expected_to &&
               payload->data.data_chunk.stream_id == 9U &&
               payload->data.data_chunk.stream_size == 3U &&
               payload->data.data_chunk.data_length == 3U &&
               payload->data.data_chunk.data != NULL &&
               memcmp(payload->data.data_chunk.data, "abc", 3U) == 0 &&
               payload->data.data_chunk.done) {
        ++sink->data_count;
    } else if (payload->kind == TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_CHUNK &&
               payload->group_id == 43U &&
               payload->data.snapshot_chunk.from == sink->expected_from &&
               payload->data.snapshot_chunk.to == sink->expected_to &&
               payload->data.snapshot_chunk.snapshot_index == 19U &&
               payload->data.snapshot_chunk.snapshot_size == 3U &&
               payload->data.snapshot_chunk.data_length == 3U &&
               payload->data.snapshot_chunk.data != NULL &&
               memcmp(payload->data.snapshot_chunk.data, "xyz", 3U) == 0 &&
               payload->data.snapshot_chunk.done &&
               payload->data.snapshot_chunk.has_configuration) {
        ++sink->snapshot_count;
    } else {
        goto invalid;
    }

    ++sink->count;
    return SALTS_OK;
invalid:
    sink->violation = 1;
    return SALTS_EPROTO;
}

/* The Raft-specific binding must not replace the caller's snapshot SPI. */
static int peer_snapshot_stub(void *context,
                              const tr_raft_snapshot_request_t *request)
{
    (void)context;
    (void)request;
    return SALTS_OK;
}

static int peer_fixture_path(char *output, size_t capacity,
                             const char *file)
{
    int n = snprintf(output, capacity, "%s/%s",
                     TURBORAFT_ACE23_FIXTURE_DIR, file);
    return n > 0 && (size_t)n < capacity ? SALTS_OK : SALTS_ERANGE;
}

static int peer_case_run(int mode)
{
    peer_fixture_t f = {0};
    cnet_client_config client_config = peer_client_config();
    cnet_listener_config listen = {
        .backend = client_config.backend,
        .host = "127.0.0.1",
        .port = 0U,
        .backlog = 2U
    };
    cnet_tls_server_config server_tls = {0};
    cnet_tls_client_config client_tls = {0};
    tr_raft_cnet_channel_config_t client_channel = {0};
    tr_raft_cnet_channel_config_t server_channel = {0};
    tr_raft_cnet_channel_status_t cs = {0};
    tr_raft_cnet_channel_status_t ss = {0};
    cnet_connect_options options = {0};
    tr_raft_cnet_channel_group_binding_t raft_group = {0};
    tr_raft_transport_t raft_transport = {0};
    char ca_file[512], client_cert[512], client_key[512];
    char server_cert[512], server_key[512], uri[128];
    uint16_t port = 0U;
    size_t events = 0U;
    unsigned iteration;
    int accepted = 0;
    int sent = 0;
    int result = SALTS_OK;
    const char *error_stage = "none";

#define PEER_TRY(expr) do { \
    result = (expr); \
    if (result != SALTS_OK) { error_stage = #expr; goto cleanup; } \
} while (0)

    f.client_sink.expected_from = 2U;
    f.client_sink.expected_to = mode == PEER_FORGED_NODE ? 3U : 1U;
    f.server_sink.expected_from = 1U;
    f.server_sink.expected_to = 2U;

    f.client_fingerprints[0] = CERT_NODE2;
    f.server_fingerprints[0] = mode == PEER_UNAUTHORIZED_CERT
        ? CERT_NODE2 : CERT_NODE1;
    f.client_peer = (tr_raft_cnet_peer_identity_t){
        2U, f.client_fingerprints, 1U
    };
    f.server_peer = (tr_raft_cnet_peer_identity_t){
        1U, f.server_fingerprints, 1U
    };
    f.client_policy = (tr_raft_cnet_identity_policy_t){
        mode == PEER_FORGED_NODE ? 3U : 1U, &f.client_peer, 1U
    };
    f.server_policy = (tr_raft_cnet_identity_policy_t){
        2U, &f.server_peer, 1U
    };

    PEER_TRY(peer_fixture_path(ca_file, sizeof(ca_file), "ca.pem"));
    PEER_TRY(peer_fixture_path(client_cert, sizeof(client_cert), "node1-cert.pem"));
    PEER_TRY(peer_fixture_path(client_key, sizeof(client_key), "node1-key.pem"));
    PEER_TRY(peer_fixture_path(server_cert, sizeof(server_cert), "node2-cert.pem"));
    PEER_TRY(peer_fixture_path(server_key, sizeof(server_key), "node2-key.pem"));

    server_tls.size = sizeof(server_tls);
    server_tls.cert_file = server_cert;
    server_tls.key_file = server_key;
    server_tls.ca_file = ca_file;
    server_tls.client_auth = CNET_TLS_CLIENT_AUTH_REQUIRED;
    PEER_TRY(cnet_tls_server_init(&f.tls_server, &server_tls));
    f.tls_open = 1;
    PEER_TRY(cnet_client_init(&f.server, &client_config));
    f.server_open = 1;
    PEER_TRY(cnet_client_init(&f.client, &client_config));
    f.client_open = 1;
    PEER_TRY(cnet_listener_init(&f.listener, &listen));
    f.listener_open = 1;
    PEER_TRY(cnet_listener_port(&f.listener, &port));
    if (snprintf(uri, sizeof(uri), "tls://127.0.0.1:%u",
                 (unsigned)port) <= 0) {
        result = SALTS_EINVAL;
        goto cleanup;
    }

    client_channel.client = &f.client;
    client_channel.identity = &f.client_policy;
    client_channel.handshake = peer_handshake(
        mode == PEER_FORGED_NODE ? 3U : 1U,
        mode == PEER_FOREIGN_CLUSTER);
    client_channel.first_outbound_message_id = 1U;
    client_channel.on_payload = peer_record;
    client_channel.payload_context = &f.client_sink;
    server_channel.client = &f.server;
    server_channel.identity = &f.server_policy;
    server_channel.handshake = peer_handshake(2U, 0);
    server_channel.first_outbound_message_id = 1U;
    server_channel.on_payload = peer_record;
    server_channel.payload_context = &f.server_sink;
    PEER_TRY(tr_raft_cnet_channel_create(
        &client_channel, &f.client_channel));
    PEER_TRY(tr_raft_cnet_channel_create(
        &server_channel, &f.server_channel));

    /* Compose the existing Service/Runtime Transport SPI without creating a
     * second queue or moving the network connection across owners. */
    raft_group.channel = f.client_channel;
    raft_group.group_id = 42U;
    raft_transport.snapshot_context = &f;
    raft_transport.enqueue_snapshot = peer_snapshot_stub;
    PEER_TRY(tr_raft_cnet_channel_group_transport_bind(
        &raft_group, &raft_transport));
    if (raft_transport.context != &raft_group ||
        raft_transport.snapshot_context != &f ||
        raft_transport.enqueue_snapshot != peer_snapshot_stub ||
        tr_raft_cnet_channel_group_transport_bind(
            &raft_group, &raft_transport) != SALTS_EALREADY) {
        error_stage = "single-bind owned transport adapter";
        result = SALTS_EPROTO;
        goto cleanup;
    }

    /* A Raft payload cannot be emitted before reciprocal HELLO/ACK. */
    {
        tr_raft_transport_payload_t premature = {0};
        premature.group_id = 42U;
        premature.kind = TR_RAFT_WIRE_PAYLOAD_RAFT;
        if (tr_raft_cnet_channel_send(f.client_channel, &premature) !=
                SALTS_EBUSY ||
            raft_transport.enqueue(raft_transport.context,
                                   &premature.data.raft) != SALTS_ENOSPC) {
            error_stage = "admission before TLS";
            result = SALTS_EPROTO;
            goto cleanup;
        }
    }

    client_tls.size = sizeof(client_tls);
    client_tls.ca_file = ca_file;
    client_tls.cert_file = client_cert;
    client_tls.key_file = client_key;
    client_tls.server_name = "node-2.mesh";
    options.uri = uri;
    options.tls = &client_tls;
    options.observer = tr_raft_cnet_channel_observer(f.client_channel);
    PEER_TRY(cnet_connect(&f.client, &options, &f.outbound));
    PEER_TRY(tr_raft_cnet_channel_attach(f.client_channel, f.outbound));

    for (iteration = 0U; iteration < 4000U; ++iteration) {
        int ready = 0;
        PEER_TRY(cnet_client_poll(&f.client, 1U, &events));
        if (!accepted) {
            PEER_TRY(cnet_listener_wait(&f.listener, 0U, &ready));
            if (ready) {
                cnet_observer observer =
                    tr_raft_cnet_channel_observer(f.server_channel);
                PEER_TRY(cnet_listener_accept_tls(
                    &f.listener, &f.server, &f.tls_server, &observer, &f.inbound));
                PEER_TRY(tr_raft_cnet_channel_attach(f.server_channel, f.inbound));
                accepted = 1;
            }
        }
        PEER_TRY(cnet_client_poll(&f.server, 1U, &events));
        PEER_TRY(tr_raft_cnet_channel_get_status(f.client_channel, &cs));
        PEER_TRY(tr_raft_cnet_channel_get_status(f.server_channel, &ss));

        if (mode != PEER_VALID && mode != PEER_STREAMS) {
            if (ss.phase == TR_RAFT_CNET_CHANNEL_FAILED)
                break;
            continue;
        }

        if (cs.phase == TR_RAFT_CNET_CHANNEL_FAILED ||
            ss.phase == TR_RAFT_CNET_CHANNEL_FAILED) {
            result = SALTS_EPROTO;
            error_stage = "unexpected handshake failure";
            goto cleanup;
        }

        if (!sent &&
            cs.phase == TR_RAFT_CNET_CHANNEL_ACTIVE &&
            ss.phase == TR_RAFT_CNET_CHANNEL_ACTIVE) {
            tr_raft_transport_payload_t request = {0};
            tr_raft_transport_payload_t response = {0};
            request.group_id = 42U;
            request.kind = TR_RAFT_WIRE_PAYLOAD_RAFT;
            request.data.raft.type = TR_RAFT_MSG_HEARTBEAT_REQUEST;
            request.data.raft.from = 1U;
            request.data.raft.to = 2U;
            request.data.raft.term = 3U;
            response.group_id = 42U;
            response.kind = TR_RAFT_WIRE_PAYLOAD_RAFT;
            response.data.raft.type = TR_RAFT_MSG_HEARTBEAT_RESPONSE;
            response.data.raft.from = 2U;
            response.data.raft.to = 1U;
            response.data.raft.term = 3U;
            PEER_TRY(raft_transport.enqueue(raft_transport.context,
                                             &request.data.raft));
            PEER_TRY(tr_raft_cnet_channel_send(f.server_channel, &response));
            if (mode == PEER_STREAMS) {
                tr_raft_transport_payload_t data = {0};
                tr_raft_transport_payload_t snapshot = {0};
                size_t i;
                data.group_id = 43U;
                data.kind = TR_RAFT_WIRE_PAYLOAD_DATA_CHUNK;
                data.data.data_chunk.from = 1U;
                data.data.data_chunk.to = 2U;
                data.data.data_chunk.term = 3U;
                data.data.data_chunk.stream_id = 9U;
                data.data.data_chunk.stream_size = 3U;
                data.data.data_chunk.data = (const uint8_t *)"abc";
                data.data.data_chunk.data_length = 3U;
                data.data.data_chunk.done = true;
                memset(data.data.data_chunk.stream_digest, 0xa4,
                       sizeof(data.data.data_chunk.stream_digest));

                snapshot.group_id = 43U;
                snapshot.kind = TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_CHUNK;
                snapshot.data.snapshot_chunk.from = 1U;
                snapshot.data.snapshot_chunk.to = 2U;
                snapshot.data.snapshot_chunk.term = 3U;
                snapshot.data.snapshot_chunk.snapshot_index = 19U;
                snapshot.data.snapshot_chunk.snapshot_term = 3U;
                snapshot.data.snapshot_chunk.snapshot_size = 3U;
                snapshot.data.snapshot_chunk.data = (const uint8_t *)"xyz";
                snapshot.data.snapshot_chunk.data_length = 3U;
                snapshot.data.snapshot_chunk.done = true;
                snapshot.data.snapshot_chunk.has_configuration = true;
                snapshot.data.snapshot_chunk.configuration.phase =
                    TR_RAFT_CONF_FINAL;
                snapshot.data.snapshot_chunk.configuration.member_count = 1U;
                snapshot.data.snapshot_chunk.configuration.members[0].node_id = 2U;
                snapshot.data.snapshot_chunk.configuration.members[0].roles =
                    TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER;
                for (i = 0U;
                     i < sizeof(snapshot.data.snapshot_chunk.snapshot_digest);
                     ++i)
                    snapshot.data.snapshot_chunk.snapshot_digest[i] =
                        (uint8_t)(0x50U + i);

                PEER_TRY(tr_raft_cnet_channel_send(f.client_channel, &data));
                PEER_TRY(tr_raft_cnet_channel_send(f.client_channel, &snapshot));
            }
            sent = 1;
        }
        if (sent && f.client_sink.count == 1U &&
            f.server_sink.count == (mode == PEER_STREAMS ? 3U : 1U))
            break;
    }

    if (mode == PEER_VALID || mode == PEER_STREAMS) {
        const size_t expected_server = mode == PEER_STREAMS ? 3U : 1U;
        PEER_TRY(tr_raft_cnet_channel_get_status(f.client_channel, &cs));
        PEER_TRY(tr_raft_cnet_channel_get_status(f.server_channel, &ss));
        if (cs.payloads_admitted != expected_server ||
            ss.payloads_admitted != 1U ||
            cs.payloads_admitted != cs.payloads_completed +
                cs.payloads_canceled + cs.payload_writes_pending ||
            ss.payloads_admitted != ss.payloads_completed +
                ss.payloads_canceled + ss.payload_writes_pending) {
            result = SALTS_EPROTO;
            error_stage = "live TLS logical-send credit conservation";
            goto cleanup;
        }
        if (cs.phase != TR_RAFT_CNET_CHANNEL_ACTIVE ||
            ss.phase != TR_RAFT_CNET_CHANNEL_ACTIVE ||
            f.client_sink.count != 1U || f.server_sink.count != expected_server ||
            f.client_sink.violation || f.server_sink.violation ||
            f.client_sink.raft_count != 1U || f.server_sink.raft_count != 1U ||
            f.server_sink.data_count != (mode == PEER_STREAMS ? 1U : 0U) ||
            f.server_sink.snapshot_count != (mode == PEER_STREAMS ? 1U : 0U) ||
            cs.payloads_received != 1U ||
            ss.payloads_received != expected_server ||
            cs.handshake_packets_sent != 2U ||
            ss.handshake_packets_sent != 2U) {
            result = SALTS_EPROTO;
            error_stage = "bidirectional verified handshakes and Raft";
        }
        if (result == SALTS_OK &&
            tr_raft_cnet_channel_destroy(f.client_channel) != SALTS_EBUSY) {
            result = SALTS_EPROTO;
            error_stage = "destroy before terminal";
        }
    } else if (ss.phase != TR_RAFT_CNET_CHANNEL_FAILED ||
               ss.last_error != SALTS_EPROTO ||
               ss.payloads_received != 0U ||
               f.client_sink.count != 0U || f.server_sink.count != 0U) {
        result = SALTS_EPROTO;
        error_stage = "negative identity or cluster gate";
    }

cleanup:
    if (f.client_channel != NULL)
        (void)tr_raft_cnet_channel_stop(f.client_channel);
    if (f.server_channel != NULL)
        (void)tr_raft_cnet_channel_stop(f.server_channel);
    if (f.client_open) {
        int close_result = cnet_client_stop(&f.client, 2000U);
        if (result == SALTS_OK && close_result != SALTS_OK) {
            result = close_result;
            error_stage = "client stop";
        }
    }
    if (f.server_open) {
        int close_result = cnet_client_stop(&f.server, 2000U);
        if (result == SALTS_OK && close_result != SALTS_OK) {
            result = close_result;
            error_stage = "server stop";
        }
    }
    /* Terminal callbacks, not socket-close requests, settle every locally
     * admitted write. This includes DATA and SNAPSHOT in the TLS streams case,
     * without interpreting local completion as remote fsync/delivery. */
    if (f.client_channel != NULL && f.server_channel != NULL &&
        f.client_open && f.server_open && result == SALTS_OK) {
        tr_raft_cnet_channel_status_t end_client = {0};
        tr_raft_cnet_channel_status_t end_server = {0};
        int left = tr_raft_cnet_channel_get_status(
            f.client_channel, &end_client);
        int right = tr_raft_cnet_channel_get_status(
            f.server_channel, &end_server);
        if (left != SALTS_OK || right != SALTS_OK ||
            !end_client.terminal || !end_server.terminal ||
            end_client.payload_writes_pending != 0U ||
            end_server.payload_writes_pending != 0U ||
            end_client.payloads_admitted !=
                end_client.payloads_completed + end_client.payloads_canceled ||
            end_server.payloads_admitted !=
                end_server.payloads_completed + end_server.payloads_canceled) {
            result = SALTS_EPROTO;
            error_stage = "CNet terminal write ledger must drain exactly once";
        }
    }
    if (f.client_channel != NULL) {
        int close_result = tr_raft_cnet_channel_destroy(f.client_channel);
        if (result == SALTS_OK && close_result != SALTS_OK)
            result = close_result;
    }
    if (f.server_channel != NULL) {
        int close_result = tr_raft_cnet_channel_destroy(f.server_channel);
        if (result == SALTS_OK && close_result != SALTS_OK)
            result = close_result;
    }
    if (f.client_open) (void)cnet_client_destroy(&f.client);
    if (f.server_open) (void)cnet_client_destroy(&f.server);
    if (f.listener_open) {
        (void)cnet_listener_close(&f.listener);
        (void)cnet_listener_destroy(&f.listener);
    }
    if (f.tls_open) (void)cnet_tls_server_destroy(&f.tls_server);
#undef PEER_TRY

    if (result != SALTS_OK)
        fprintf(stderr, "CNet peer channel mode=%d stage=%s result=%d "
                "client-phase=%d server-phase=%d client-last=%d server-last=%d "
                "client-received=%zu server-received=%zu\n",
                mode, error_stage, result,
                (int)cs.phase, (int)ss.phase, cs.last_error, ss.last_error,
                f.client_sink.count, f.server_sink.count);
    return result;
}

spec("ACE 2.3 CNet owner-bound authenticated Raft channel")
{
    it("adopts a verified mTLS peer, completes reciprocal ACKs, and exchanges Raft")
    {
        check_equal(peer_case_run(PEER_VALID), SALTS_OK);
    }

    it("sends group-aware DATA and SNAPSHOT chunks over the same mTLS channel")
    {
        check_equal(peer_case_run(PEER_STREAMS), SALTS_OK);
    }

    it("rejects a different cluster before delivering any Raft payload")
    {
        check_equal(peer_case_run(PEER_FOREIGN_CLUSTER), SALTS_OK);
    }

    it("rejects a forged Raft Node ID despite a valid client certificate")
    {
        check_equal(peer_case_run(PEER_FORGED_NODE), SALTS_OK);
    }

    it("rejects a CA-valid but unauthorized client certificate")
    {
        check_equal(peer_case_run(PEER_UNAUTHORIZED_CERT), SALTS_OK);
    }
}
