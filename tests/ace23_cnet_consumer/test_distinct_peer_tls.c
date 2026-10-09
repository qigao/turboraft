#include <turboraft/raft_cnet_channel.h>

#include <cmeta_error.h>
#include <tinytest.h>

#include <stdio.h>
#include <string.h>

#ifndef TURBORAFT_ACE23_FIXTURE_DIR
#error "TURBORAFT_ACE23_FIXTURE_DIR must be defined for TLS fixture tests"
#endif

/* Both trust chains are checked-in loopback-only; node3 uses the separate
 * rotated *client* certificate, NOT a second copy of Node1 credentials. */
#define CERT_NODE1 "eb571a92b33237897216c79501066b3e77391047eaeb6be084526c4769657549"
#define CERT_NODE3 "d696b3ab8d0596e3e8e8abcecc4ca87856eda0f5055f31a7dfb6c213c2c2b1f3"
#define CERT_SERVER_NODE2 "44e8fe3ce37ede1c2a1b5d36efa345cb662887d4250d17a000ebea2e834aed95"

enum { LINK_COUNT = 2, MAX_PROGRESS = 4500 };

typedef struct remote_probe {
    tr_raft_node_id_t node_id;
    size_t received;
    int wrong_payload;
} remote_probe;

typedef struct server_probe {
    size_t received_from_one;
    size_t received_from_three;
    size_t wrong_payloads;
} server_probe;

typedef struct identity_fixture {
    cnet_client clients; /* one CNet I/O Owner for the two different logical test nodes */
    cnet_client server;
    cnet_listener listener;
    cnet_tls_server server_tls;
    tr_raft_cnet_channel_t *outbound[LINK_COUNT];
    tr_raft_cnet_channel_t *inbound[LINK_COUNT];
    cnet_connection outbound_handles[LINK_COUNT];
    cnet_connection inbound_handles[LINK_COUNT];
    tr_raft_cnet_peer_identity_t server_peers[LINK_COUNT];
    tr_raft_cnet_identity_policy_t server_policy;
    tr_raft_cnet_peer_identity_t client_peer[LINK_COUNT];
    tr_raft_cnet_identity_policy_t client_policy[LINK_COUNT];
    const char *client_peer_cert[LINK_COUNT][1];
    const char *server_peer_cert[LINK_COUNT][1];
    cnet_tls_client_config client_tls[LINK_COUNT];
    tr_raft_cnet_channel_config_t server_channel_config;
    remote_probe clients_received[LINK_COUNT];
    server_probe server_received;
    int clients_live, server_live, listener_live, server_tls_live;
    int accepted;
} identity_fixture;

static const tr_raft_node_id_t NODE_IDS[LINK_COUNT] = {1U, 3U};

static int fixture_path(char *out, size_t cap, const char *filename)
{
    int n = snprintf(out, cap, "%s/%s",
                     TURBORAFT_ACE23_FIXTURE_DIR, filename);
    return n > 0 && (size_t)n < cap ? SALTS_OK : SALTS_ERANGE;
}

static cnet_client_config cnet_config(void)
{
    return (cnet_client_config){
#if defined(_WIN32)
        .backend = NATIVE_IO_BACKEND_IOCP,
#elif defined(__linux__)
        .backend = NATIVE_IO_BACKEND_EPOLL,
#else
        .backend = NATIVE_IO_BACKEND_KQUEUE,
#endif
        .connection_capacity = LINK_COUNT,
        .command_capacity = 16U,
        .request_capacity = 16U,
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
}

static tr_raft_handshake_config_t hello_config(tr_raft_node_id_t node)
{
    tr_raft_handshake_config_t config = {0};
    size_t i;
    for (i = 0U; i < sizeof(config.cluster_id.bytes); ++i)
        config.cluster_id.bytes[i] = (uint8_t)(i + 17U);
    config.local_node_id = node;
    config.process_incarnation.bytes[0] = (uint8_t)node;
    config.config_epoch = 1U;
    config.wire_major_min = TR_RAFT_HANDSHAKE_WIRE_MAJOR;
    config.wire_major_max = TR_RAFT_HANDSHAKE_WIRE_MAJOR;
    config.wire_minor_min = TR_RAFT_HANDSHAKE_WIRE_MINOR;
    config.wire_minor_max = TR_RAFT_HANDSHAKE_WIRE_MINOR;
    config.max_frame_size = TR_RAFT_WIRE_MAX_FRAME_SIZE;
    config.max_snapshot_chunk_size = TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES;
    return config;
}

/* Caller only sees decoded payload AFTER certified TLS and reciprocal ACK.
 * Any Node1 message mislabeled as Node3 is not accepted by this sink. */
static int on_server_payload(void *user, const tr_raft_transport_payload_t *msg)
{
    server_probe *sink = (server_probe *)user;
    if (msg == NULL || msg->kind != TR_RAFT_WIRE_PAYLOAD_RAFT ||
        msg->data.raft.to != 2U ||
        msg->data.raft.type != TR_RAFT_MSG_HEARTBEAT_REQUEST ||
        msg->data.raft.term != 3U) goto invalid;

    if (msg->data.raft.from == 1U && msg->group_id == 101U)
        ++sink->received_from_one;
    else if (msg->data.raft.from == 3U && msg->group_id == 103U)
        ++sink->received_from_three;
    else goto invalid;

    return SALTS_OK;
invalid:
    ++sink->wrong_payloads;
    return SALTS_EPROTO;
}

static int on_client_payload(void *user, const tr_raft_transport_payload_t *msg)
{
    remote_probe *sink = (remote_probe *)user;
    if (msg == NULL || msg->kind != TR_RAFT_WIRE_PAYLOAD_RAFT ||
        msg->group_id != 100U + sink->node_id ||
        msg->data.raft.from != 2U ||
        msg->data.raft.to != sink->node_id ||
        msg->data.raft.type != TR_RAFT_MSG_HEARTBEAT_RESPONSE ||
        msg->data.raft.term != 3U) {
        ++sink->wrong_payload;
        return SALTS_EPROTO;
    }
    ++sink->received;
    return SALTS_OK;
}

static int fixture_cleanup(identity_fixture *f)
{
    size_t i;
    int result, first = SALTS_OK;
#define CLEAN_STEP(expr) do { result = (expr); \
    if (first == SALTS_OK && result != SALTS_OK) first = result; } while (0)

    for (i = 0U; i < LINK_COUNT; ++i) {
        if (f->outbound[i] != NULL)
            (void)tr_raft_cnet_channel_stop(f->outbound[i]);
        if (f->inbound[i] != NULL)
            (void)tr_raft_cnet_channel_stop(f->inbound[i]);
    }
    /* Each CNet owner drains terminal callbacks before Channel teardown. */
    if (f->clients_live)
        CLEAN_STEP(cnet_client_stop(&f->clients, 2000U));
    if (f->server_live)
        CLEAN_STEP(cnet_client_stop(&f->server, 2000U));
    for (i = 0U; i < LINK_COUNT; ++i) {
        if (f->outbound[i] != NULL)
            CLEAN_STEP(tr_raft_cnet_channel_destroy(f->outbound[i]));
        if (f->inbound[i] != NULL)
            CLEAN_STEP(tr_raft_cnet_channel_destroy(f->inbound[i]));
    }
    if (f->clients_live) CLEAN_STEP(cnet_client_destroy(&f->clients));
    if (f->server_live) CLEAN_STEP(cnet_client_destroy(&f->server));
    if (f->listener_live) {
        CLEAN_STEP(cnet_listener_close(&f->listener));
        CLEAN_STEP(cnet_listener_destroy(&f->listener));
    }
    if (f->server_tls_live)
        CLEAN_STEP(cnet_tls_server_destroy(&f->server_tls));
#undef CLEAN_STEP
    return first;
}

static int run_two_distinct_peers(int forge_node_three)
{
    identity_fixture f = {0};
    cnet_client_config net = cnet_config();
    cnet_listener_config listen = {
        .backend = net.backend, .host = "127.0.0.1",
        .port = 0U, .backlog = LINK_COUNT
    };
    cnet_tls_server_config tls = {0};
    cnet_connect_options options = {0};
    tr_raft_cnet_channel_config_t channel = {0};
    char ca_file[512], multi_ca_file[512], client_cert[LINK_COUNT][512];
    char client_key[LINK_COUNT][512], server_cert[512], server_key[512];
    char uri[128];
    uint16_t port = 0U;
    size_t events = 0U, i;
    int result = SALTS_OK;
    int sent = 0;
    unsigned round;
    const char *failed_stage = "none";

#define TRY_STAGE(expr) do { result = (expr); \
    if (result != SALTS_OK) { failed_stage = #expr; goto cleanup; } } while (0)

    TRY_STAGE(fixture_path(ca_file, sizeof(ca_file), "ca.pem"));
    TRY_STAGE(fixture_path(multi_ca_file, sizeof(multi_ca_file),
                           "three-node-client-ca.pem"));
    TRY_STAGE(fixture_path(client_cert[0], sizeof(client_cert[0]), "node1-cert.pem"));
    TRY_STAGE(fixture_path(client_key[0], sizeof(client_key[0]), "node1-key.pem"));
    TRY_STAGE(fixture_path(client_cert[1], sizeof(client_cert[1]), "node2-client-cert.pem"));
    TRY_STAGE(fixture_path(client_key[1], sizeof(client_key[1]), "node2-client-key.pem"));
    TRY_STAGE(fixture_path(server_cert, sizeof(server_cert), "node2-cert.pem"));
    TRY_STAGE(fixture_path(server_key, sizeof(server_key), "node2-key.pem"));

    f.server_peer_cert[0][0] = CERT_NODE1;
    f.server_peer_cert[1][0] = CERT_NODE3;
    f.server_peers[0] = (tr_raft_cnet_peer_identity_t){
        1U, f.server_peer_cert[0], 1U
    };
    f.server_peers[1] = (tr_raft_cnet_peer_identity_t){
        3U, f.server_peer_cert[1], 1U
    };
    f.server_policy = (tr_raft_cnet_identity_policy_t){
        2U, f.server_peers, LINK_COUNT
    };
    TRY_STAGE(tr_raft_cnet_identity_policy_validate(&f.server_policy));

    tls.size = sizeof(tls);
    tls.ca_file = multi_ca_file;
    tls.cert_file = server_cert;
    tls.key_file = server_key;
    tls.client_auth = CNET_TLS_CLIENT_AUTH_REQUIRED;
    TRY_STAGE(cnet_tls_server_init(&f.server_tls, &tls));
    f.server_tls_live = 1;

    TRY_STAGE(cnet_client_init(&f.clients, &net));
    f.clients_live = 1;
    TRY_STAGE(cnet_client_init(&f.server, &net));
    f.server_live = 1;
    TRY_STAGE(cnet_listener_init(&f.listener, &listen));
    f.listener_live = 1;
    TRY_STAGE(cnet_listener_port(&f.listener, &port));
    if (snprintf(uri, sizeof(uri), "tls://127.0.0.1:%u",
                 (unsigned)port) <= 0) {
        result = SALTS_EINVAL; failed_stage = "format TLS URI"; goto cleanup;
    }

    f.server_channel_config.client = &f.server;
    f.server_channel_config.identity = &f.server_policy;
    f.server_channel_config.handshake = hello_config(2U);
    f.server_channel_config.first_outbound_message_id = 1U;
    f.server_channel_config.on_payload = on_server_payload;
    f.server_channel_config.payload_context = &f.server_received;

    for (i = 0U; i < LINK_COUNT; ++i) {
        /* These are distinct physical client certificates. Both have a
         * single explicitly pinned Node2 server identity and exact SNI. */
        f.client_peer_cert[i][0] = CERT_SERVER_NODE2;
        f.client_peer[i] = (tr_raft_cnet_peer_identity_t){
            2U, f.client_peer_cert[i], 1U
        };
        f.client_policy[i] = (tr_raft_cnet_identity_policy_t){
            NODE_IDS[i], &f.client_peer[i], 1U
        };
        f.clients_received[i].node_id = NODE_IDS[i];

        f.client_tls[i].size = sizeof(f.client_tls[i]);
        f.client_tls[i].ca_file = ca_file;
        f.client_tls[i].cert_file = client_cert[i];
        f.client_tls[i].key_file = client_key[i];
        f.client_tls[i].server_name = "node-2.mesh";
        channel = (tr_raft_cnet_channel_config_t){0};
        channel.client = &f.clients;
        channel.identity = &f.client_policy[i];
        channel.handshake = hello_config(
            forge_node_three && i == 1U ? 1U : NODE_IDS[i]);
        if (forge_node_three && i == 1U) {
            /* This invalid assignment is only the test's malicious HELLO.
             * The local client identity policy follows the claimed ID,
             * while Node2's trusted fingerprint table still maps it to 3. */
            f.client_policy[i].local_node_id = 1U;
        }
        channel.first_outbound_message_id = 1U;
        channel.on_payload = on_client_payload;
        channel.payload_context = &f.clients_received[i];
        TRY_STAGE(tr_raft_cnet_channel_create(&channel, &f.outbound[i]));

        options = (cnet_connect_options){0};
        options.uri = uri;
        options.tls = &f.client_tls[i];
        options.observer = tr_raft_cnet_channel_observer(f.outbound[i]);
        TRY_STAGE(cnet_connect(&f.clients, &options, &f.outbound_handles[i]));
        TRY_STAGE(tr_raft_cnet_channel_attach(
            f.outbound[i], f.outbound_handles[i]));
    }

    for (round = 0U; round < MAX_PROGRESS; ++round) {
        int ready = 0;
        TRY_STAGE(cnet_client_poll(&f.clients, 1U, &events));
        if (f.accepted < LINK_COUNT) {
            TRY_STAGE(cnet_listener_wait(&f.listener, 0U, &ready));
            if (ready) {
                const size_t position = (size_t)f.accepted;
                cnet_observer observer;
                TRY_STAGE(tr_raft_cnet_channel_create(
                    &f.server_channel_config, &f.inbound[position]));
                observer = tr_raft_cnet_channel_observer(f.inbound[position]);
                TRY_STAGE(cnet_listener_accept_tls(
                    &f.listener, &f.server, &f.server_tls,
                    &observer, &f.inbound_handles[position]));
                TRY_STAGE(tr_raft_cnet_channel_attach(
                    f.inbound[position], f.inbound_handles[position]));
                ++f.accepted;
            }
        }
        TRY_STAGE(cnet_client_poll(&f.server, 1U, &events));

        if (!sent) {
            int both_clients_ready = 1;
            int both_server_links_ready = 1;
            int reject_seen = 0;
            for (i = 0U; i < LINK_COUNT; ++i) {
                tr_raft_cnet_channel_status_t st = {0};
                if (f.outbound[i] == NULL || f.inbound[i] == NULL) {
                    both_clients_ready = both_server_links_ready = 0;
                    continue;
                }
                TRY_STAGE(tr_raft_cnet_channel_get_status(f.outbound[i], &st));
                if (st.phase != TR_RAFT_CNET_CHANNEL_ACTIVE)
                    both_clients_ready = 0;
                TRY_STAGE(tr_raft_cnet_channel_get_status(f.inbound[i], &st));
                if (st.phase != TR_RAFT_CNET_CHANNEL_ACTIVE)
                    both_server_links_ready = 0;
                if (st.phase == TR_RAFT_CNET_CHANNEL_FAILED)
                    reject_seen = 1;
            }

            if (forge_node_three && reject_seen) {
                tr_raft_cnet_channel_status_t healthy = {0};
                tr_raft_cnet_channel_status_t rejected = {0};
                tr_raft_cnet_channel_status_t node1_client = {0};
                size_t healthy_socket = LINK_COUNT;

                /* The bad Node3 certificate is genuine but its HELLO
                 * falsely declares Node1. The healthy Node1 must still
                 * establish its own reciprocal session and exchange Raft.
                 * Accept order is not an identity or scheduling guarantee. */
                for (i = 0U; i < LINK_COUNT; ++i) {
                    tr_raft_cnet_channel_status_t st = {0};
                    TRY_STAGE(tr_raft_cnet_channel_get_status(f.inbound[i], &st));
                    if (st.authenticated_peer_node_id == 1U) {
                        healthy = st;
                        healthy_socket = i;
                    } else if (st.authenticated_peer_node_id == 3U)
                        rejected = st;
                }
                TRY_STAGE(tr_raft_cnet_channel_get_status(
                    f.outbound[0], &node1_client));
                if (healthy_socket == LINK_COUNT ||
                    healthy.phase != TR_RAFT_CNET_CHANNEL_ACTIVE ||
                    node1_client.phase != TR_RAFT_CNET_CHANNEL_ACTIVE ||
                    rejected.phase != TR_RAFT_CNET_CHANNEL_FAILED)
                    continue;

                if (!sent) {
                    tr_raft_transport_payload_t request = {0};
                    tr_raft_transport_payload_t response = {0};
                    request.group_id = 101U;
                    request.kind = TR_RAFT_WIRE_PAYLOAD_RAFT;
                    request.data.raft.type = TR_RAFT_MSG_HEARTBEAT_REQUEST;
                    request.data.raft.term = 3U;
                    request.data.raft.from = 1U;
                    request.data.raft.to = 2U;
                    response.group_id = 101U;
                    response.kind = TR_RAFT_WIRE_PAYLOAD_RAFT;
                    response.data.raft.type = TR_RAFT_MSG_HEARTBEAT_RESPONSE;
                    response.data.raft.term = 3U;
                    response.data.raft.from = 2U;
                    response.data.raft.to = 1U;
                    TRY_STAGE(tr_raft_cnet_channel_send(
                        f.outbound[0], &request));
                    TRY_STAGE(tr_raft_cnet_channel_send(
                        f.inbound[healthy_socket], &response));
                    sent = 1;
                }
                if (f.server_received.received_from_one == 1U &&
                    f.clients_received[0].received == 1U)
                    break;
                continue;
            }
            if (!forge_node_three && both_clients_ready &&
                both_server_links_ready) {
                /* Correlate each server connection to its *TLS-authenticated*
                 * Node ID, never to accept order or a raw caller-supplied name. */
                for (i = 0U; i < LINK_COUNT; ++i) {
                    tr_raft_transport_payload_t payload = {0};
                    payload.group_id = 100U + NODE_IDS[i];
                    payload.kind = TR_RAFT_WIRE_PAYLOAD_RAFT;
                    payload.data.raft.type = TR_RAFT_MSG_HEARTBEAT_REQUEST;
                    payload.data.raft.term = 3U;
                    payload.data.raft.from = NODE_IDS[i];
                    payload.data.raft.to = 2U;
                    TRY_STAGE(tr_raft_cnet_channel_send(
                        f.outbound[i], &payload));
                }
                for (i = 0U; i < LINK_COUNT; ++i) {
                    tr_raft_cnet_channel_status_t st = {0};
                    tr_raft_transport_payload_t payload = {0};
                    TRY_STAGE(tr_raft_cnet_channel_get_status(f.inbound[i], &st));
                    if (st.authenticated_peer_node_id != 1U &&
                        st.authenticated_peer_node_id != 3U) {
                        result = SALTS_EPROTO;
                        failed_stage = "remote TLS identity selection";
                        goto cleanup;
                    }
                    payload.group_id = 100U + st.authenticated_peer_node_id;
                    payload.kind = TR_RAFT_WIRE_PAYLOAD_RAFT;
                    payload.data.raft.type = TR_RAFT_MSG_HEARTBEAT_RESPONSE;
                    payload.data.raft.term = 3U;
                    payload.data.raft.from = 2U;
                    payload.data.raft.to = st.authenticated_peer_node_id;
                    TRY_STAGE(tr_raft_cnet_channel_send(f.inbound[i], &payload));
                }
                sent = 1;
            }
        }

        if (sent && f.server_received.received_from_one == 1U &&
            f.server_received.received_from_three == 1U &&
            f.clients_received[0].received == 1U &&
            f.clients_received[1].received == 1U)
            break;
    }

    if (!forge_node_three) {
        tr_raft_cnet_channel_status_t st[2] = {{0}};
        if (!sent || f.accepted != LINK_COUNT ||
            f.server_received.received_from_one != 1U ||
            f.server_received.received_from_three != 1U ||
            f.server_received.wrong_payloads ||
            f.clients_received[0].received != 1U ||
            f.clients_received[1].received != 1U ||
            f.clients_received[0].wrong_payload ||
            f.clients_received[1].wrong_payload) {
            result = SALTS_EPROTO;
            failed_stage = "separate Node1/Node3 Raft group isolation";
            goto cleanup;
        }
        TRY_STAGE(tr_raft_cnet_channel_get_status(f.inbound[0], &st[0]));
        TRY_STAGE(tr_raft_cnet_channel_get_status(f.inbound[1], &st[1]));
        if (st[0].authenticated_peer_node_id == st[1].authenticated_peer_node_id ||
            (st[0].authenticated_peer_node_id != 1U &&
             st[0].authenticated_peer_node_id != 3U) ||
            (st[1].authenticated_peer_node_id != 1U &&
             st[1].authenticated_peer_node_id != 3U)) {
            result = SALTS_EPROTO;
            failed_stage = "distinct authenticated certificate identities";
        }
    } else if (!sent || f.accepted != LINK_COUNT ||
               f.server_received.received_from_one != 1U ||
               f.clients_received[0].received != 1U ||
               f.clients_received[0].wrong_payload != 0 ||
               f.clients_received[1].received != 0U ||
               f.server_received.received_from_three != 0U ||
               f.server_received.wrong_payloads != 0U) {
        result = SALTS_EPROTO;
        failed_stage = "forgery must have no delivered payload";
    }

cleanup:
    {
        int close_status = fixture_cleanup(&f);
        if (result == SALTS_OK && close_status != SALTS_OK) {
            failed_stage = "complete CNet TLS terminal teardown";
            result = close_status;
        }
    }
#undef TRY_STAGE
    if (result != SALTS_OK) {
        fprintf(stderr,
                "distinct CNet TLS peers mode=%d stage=%s result=%d accepted=%d "
                "node1=%zu node3=%zu client1=%zu client3=%zu wrong=%zu\n",
                forge_node_three, failed_stage, result, f.accepted,
                f.server_received.received_from_one,
                f.server_received.received_from_three,
                f.clients_received[0].received,
                f.clients_received[1].received,
                f.server_received.wrong_payloads);
    }
    return result;
}

spec("ACE 2.3 two real distinct TLS-certified Raft Node IDs")
{
    it("simultaneously admits Node1 and Node3 certificates and isolates bidirectional Raft groups")
    {
        check_equal(run_two_distinct_peers(0), SALTS_OK);
    }

    it("rejects rotated-certificate peer forging another Node ID without poisoning Node1")
    {
        check_equal(run_two_distinct_peers(1), SALTS_OK);
    }
}
