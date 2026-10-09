#include <turboraft/raft_cnet_identity.h>
#include <cnet/handoff.h>
#include <salts/thread.h>

#include <stdatomic.h>

#include <cmeta_error.h>
#include <tinytest.h>

#include <stdio.h>
#include <string.h>

#ifndef TURBORAFT_ACE23_FIXTURE_DIR
#error "TURBORAFT_ACE23_FIXTURE_DIR must name checked-in test-only TLS fixtures"
#endif

#define CERT_NODE1 "eb571a92b33237897216c79501066b3e77391047eaeb6be084526c4769657549"
#define CERT_NODE2 "44e8fe3ce37ede1c2a1b5d36efa345cb662887d4250d17a000ebea2e834aed95"

enum {
    TLS_CASE_VALID = 0,
    TLS_CASE_WRONG_CERTIFICATE,
    TLS_CASE_FORGED_NODE_ID,
    TLS_CASE_WRONG_CLUSTER,
    TLS_CASE_BOUNDED_HANDOFF,
    TLS_CASE_THREADED_HANDOFF,
    TLS_PROGRESS_MAX = 4000
};

typedef struct tls_fixture tls_fixture_t;

typedef struct tls_probe {
    tls_fixture_t *fixture;
    cnet_client *owner;
    cnet_connection connection;
    size_t received_size;
    uint8_t received[TR_RAFT_HANDSHAKE_PACKET_SIZE];
    int server_side;
    int connected;
    int terminal;
    int failed;
    int callback_error;
    int auth_done;
    int auth_status;
    int negotiated_done;
    int negotiation_status;
    int sent;
    tr_raft_node_id_t authenticated_node;
    tr_raft_handshake_result_t handshake;
} tls_probe_t;

struct tls_fixture {
    cnet_client client;
    cnet_client server;
    cnet_listener listener;
    cnet_tls_server tls_server;
    cnet_handoff handoff;
    cnet_handoff_ticket ticket;
    cmeta_thread_t accept_thread;
    atomic_int accept_result;
    atomic_bool accept_done;
    cnet_connection outbound;
    tls_probe_t client_probe;
    tls_probe_t server_probe;
    tr_raft_cnet_peer_identity_t peer;
    tr_raft_cnet_identity_policy_t policy;
    const char *certificate[1];
    tr_raft_handshake_config_t local;
    int mode;
    int client_initialized;
    int server_initialized;
    int listener_initialized;
    int tls_initialized;
    int handoff_initialized;
    int accept_thread_started;
    /* 0 none, 1 reserved, 2 queued, 3 taken, 4 released. */
    int handoff_phase;
    int accepted;
    int callback_error;
};

static tr_raft_handshake_config_t tls_config(tr_raft_node_id_t local_id)
{
    tr_raft_handshake_config_t config = {0};
    size_t i;

    for (i = 0U; i < sizeof(config.cluster_id.bytes); ++i)
        config.cluster_id.bytes[i] = (uint8_t)(i + 17U);
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

static cnet_client_config tls_client_config(void)
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

static void tls_state(void *context, cnet_connection connection,
                      cnet_connection_state state, const cnet_error *error)
{
    tls_probe_t *probe = (tls_probe_t *)context;
    tls_fixture_t *fixture = probe->fixture;
    int status;

    probe->connection = connection;
    if (state == CNET_CONNECTION_CONNECTED) {
        probe->connected = 1;
        if (probe->server_side) {
            probe->auth_status = tr_raft_cnet_identity_admit_tls(
                &fixture->policy, probe->owner, connection,
                &probe->authenticated_node);
            probe->auth_done = 1;
            if (probe->auth_status == SALTS_OK) {
                status = cnet_receive(probe->owner, connection,
                                      TR_RAFT_HANDSHAKE_PACKET_SIZE);
                if (status != SALTS_OK) probe->callback_error = status;
            }
        } else {
            tr_raft_handshake_config_t peer =
                tls_config(fixture->mode == TLS_CASE_FORGED_NODE_ID ? 3U : 1U);
            tr_raft_handshake_message_t hello;
            if (fixture->mode == TLS_CASE_WRONG_CLUSTER)
                peer.cluster_id.bytes[0] ^= 0x55U;
            uint8_t packet[TR_RAFT_HANDSHAKE_PACKET_SIZE];
            size_t size = 0U;
            mem_buffer_t *buffer;

            status = tr_raft_handshake_make_hello(&peer, &hello);
            if (status == SALTS_OK)
                status = tr_raft_handshake_encode(
                    &hello, packet, sizeof(packet), &size);
            if (status != SALTS_OK) {
                probe->callback_error = status;
                return;
            }
            buffer = mem_get_buffer(mem_global(), size);
            if (buffer == NULL) {
                probe->callback_error = SALTS_ENOMEM;
                return;
            }
            memcpy(mem_buffer_data(buffer), packet, size);
            mem_set_used(buffer, size);
            probe->callback_error =
                cnet_send_buffer(probe->owner, connection, buffer);
            mem_buffer_release(buffer);
            probe->sent = probe->callback_error == SALTS_OK;
        }
    } else if (state == CNET_CONNECTION_CLOSED ||
               state == CNET_CONNECTION_FAILED) {
        probe->terminal = 1;
        if (state == CNET_CONNECTION_FAILED || error != NULL)
            probe->failed = 1;
    }
}

static void tls_receive(void *context, cnet_connection connection,
                        const cnet_receive_view *view)
{
    tls_probe_t *probe = (tls_probe_t *)context;
    tls_fixture_t *fixture = probe->fixture;
    tr_raft_handshake_message_t hello;
    tr_raft_handshake_message_t ack = {0};

    if (!probe->server_side || !probe->auth_done ||
        probe->auth_status != SALTS_OK || view == NULL ||
        view->kind != CNET_MESSAGE_BYTES ||
        view->size > sizeof(probe->received) - probe->received_size) {
        probe->callback_error = SALTS_EPROTO;
        return;
    }
    memcpy(probe->received + probe->received_size, view->data, view->size);
    probe->received_size += view->size;
    if (probe->received_size != TR_RAFT_HANDSHAKE_PACKET_SIZE)
        return;

    probe->negotiated_done = 1;
    probe->negotiation_status = tr_raft_handshake_decode(
        probe->received, probe->received_size, &hello);
    if (probe->negotiation_status != SALTS_OK) return;

    /* The HELLO was read from this same CNet TLS connection, not passed by
     * an out-of-band identity string. The authenticated node ID is enforced
     * by the canonical Raft negotiation before any payload dispatch. */
    probe->negotiation_status = tr_raft_cnet_identity_negotiate_tls(
        &fixture->policy, probe->owner, connection, &fixture->local,
        &hello, &ack, &probe->handshake);
}

static int tls_path(char *destination, size_t capacity, const char *name)
{
    const int n = snprintf(
        destination, capacity, "%s/%s", TURBORAFT_ACE23_FIXTURE_DIR, name);
    return n > 0 && (size_t)n < capacity ? SALTS_OK : SALTS_ERANGE;
}

static int tls_accept_via_handoff(
    tls_fixture_t *fixture, const cnet_observer *observer)
{
    cnet_accepted_stream accepted = CNET_ACCEPTED_STREAM_INIT;
    cnet_accepted_stream transferred = CNET_ACCEPTED_STREAM_INIT;
    cnet_handoff_ticket taken = {0};
    int status;

    status = cnet_listener_accept_detached(&fixture->listener, &accepted);
    if (status != SALTS_OK) return status;

    /* Reserve/publish moves the accepted socket but creates no TLS state.
     * Exactly one final CNet owner adopts TLS after taking the ticket. */
    status = cnet_handoff_publish(
        &fixture->handoff, fixture->ticket, &accepted);
    if (status != SALTS_OK) {
        (void)cnet_accepted_stream_close(&accepted);
        return status;
    }
    fixture->handoff_phase = 2;
    status = cnet_handoff_take(&fixture->handoff, &taken, &transferred);
    if (status != SALTS_OK) return status;
    fixture->handoff_phase = 3;
    if (taken.incarnation != fixture->ticket.incarnation ||
        taken.generation != fixture->ticket.generation ||
        taken.slot != fixture->ticket.slot) {
        (void)cnet_accepted_stream_close(&transferred);
        return SALTS_EPROTO;
    }

    return cnet_client_adopt_accepted_tls(
        &fixture->server, &transferred, &fixture->tls_server, observer,
        &fixture->server_probe.connection);
}

/* Test-only ACE Acceptor / final-owner handoff: the listener belongs to this
 * independent worker for the entire wait/detach/publish sequence. It never
 * polls or invokes the final owner's CNet client, starts TLS, or touches Raft.
 * CNet's existing bounded cnet_handoff is the ONLY cross-thread queue. */
static void tls_handoff_accept_thread(void *context)
{
    tls_fixture_t *fixture = (tls_fixture_t *)context;
    cnet_accepted_stream accepted = CNET_ACCEPTED_STREAM_INIT;
    int result = SALTS_ETIMEDOUT;
    unsigned i;

    for (i = 0U; i < 200U; ++i) {
        int ready = 0;
        int wait_result = cnet_listener_wait(&fixture->listener, 5U, &ready);
        if (wait_result != SALTS_OK) {
            result = wait_result;
            break;
        }
        if (!ready) continue;

        result = cnet_listener_accept_detached(&fixture->listener, &accepted);
        if (result == SALTS_OK) {
            result = cnet_handoff_publish(
                &fixture->handoff, fixture->ticket, &accepted);
            if (result != SALTS_OK)
                (void)cnet_accepted_stream_close(&accepted);
        }
        break;
    }
    atomic_store_explicit(&fixture->accept_result, result, memory_order_release);
    atomic_store_explicit(&fixture->accept_done, true, memory_order_release);
}

static int tls_case_run(int mode)
{
    tls_fixture_t fixture = {0};
    cnet_client_config client_config = tls_client_config();
    cnet_listener_config listener_config = {
        .backend = client_config.backend, .host = "127.0.0.1",
        .port = 0U, .backlog = 2U
    };
    cnet_tls_server_config tls_server_config = {0};
    cnet_tls_client_config tls_client_config = {0};
    cnet_connect_options options = {0};
    char ca_file[512], client_cert[512], client_key[512];
    char server_cert[512], server_key[512], uri[128];
    uint16_t port = 0U;
    size_t events = 0U;
    unsigned iteration;
    int ready = 0;
    int status = SALTS_OK;
    const char *failed_stage = "none";

#define TLS_TRY(expression) do { \
    status = (expression); \
    if (status != SALTS_OK) { failed_stage = #expression; goto cleanup; } \
} while (0)

    fixture.mode = mode;
    atomic_init(&fixture.accept_result, SALTS_EBUSY);
    atomic_init(&fixture.accept_done, false);
    fixture.local = tls_config(2U);
    fixture.client_probe.fixture = &fixture;
    fixture.client_probe.owner = &fixture.client;
    fixture.server_probe.fixture = &fixture;
    fixture.server_probe.owner = &fixture.server;
    fixture.server_probe.server_side = 1;
    fixture.certificate[0] = mode == TLS_CASE_WRONG_CERTIFICATE ?
        CERT_NODE2 : CERT_NODE1;
    fixture.peer = (tr_raft_cnet_peer_identity_t){
        1U, fixture.certificate, 1U
    };
    fixture.policy = (tr_raft_cnet_identity_policy_t){
        2U, &fixture.peer, 1U
    };

    TLS_TRY(tr_raft_cnet_identity_policy_validate(&fixture.policy));
    TLS_TRY(tls_path(ca_file, sizeof(ca_file), "ca.pem"));
    TLS_TRY(tls_path(client_cert, sizeof(client_cert), "node1-cert.pem"));
    TLS_TRY(tls_path(client_key, sizeof(client_key), "node1-key.pem"));
    TLS_TRY(tls_path(server_cert, sizeof(server_cert), "node2-cert.pem"));
    TLS_TRY(tls_path(server_key, sizeof(server_key), "node2-key.pem"));

    tls_server_config.size = sizeof(tls_server_config);
    tls_server_config.cert_file = server_cert;
    tls_server_config.key_file = server_key;
    tls_server_config.ca_file = ca_file;
    tls_server_config.client_auth = CNET_TLS_CLIENT_AUTH_REQUIRED;
    TLS_TRY(cnet_tls_server_init(&fixture.tls_server, &tls_server_config));
    fixture.tls_initialized = 1;

    TLS_TRY(cnet_client_init(&fixture.server, &client_config));
    fixture.server_initialized = 1;
    TLS_TRY(cnet_client_init(&fixture.client, &client_config));
    fixture.client_initialized = 1;
    TLS_TRY(cnet_listener_init(&fixture.listener, &listener_config));
    fixture.listener_initialized = 1;
    if (mode == TLS_CASE_BOUNDED_HANDOFF ||
        mode == TLS_CASE_THREADED_HANDOFF) {
        cnet_handoff_config config = {
            sizeof(cnet_handoff_config), CNET_HANDOFF_VERSION, 1U, 1U
        };
        cnet_handoff_ticket extra = {0};
        int capacity_result;
        TLS_TRY(cnet_handoff_init(&fixture.handoff, &config));
        fixture.handoff_initialized = 1;
        TLS_TRY(cnet_handoff_reserve(&fixture.handoff, &fixture.ticket));
        fixture.handoff_phase = 1;
        capacity_result = cnet_handoff_reserve(&fixture.handoff, &extra);
        if (capacity_result != SALTS_ENOBUFS) {
            if (capacity_result == SALTS_OK)
                (void)cnet_handoff_release(&fixture.handoff, extra);
            failed_stage = "handoff credit bound";
            status = SALTS_EPROTO;
            goto cleanup;
        }
    }
    TLS_TRY(cnet_listener_port(&fixture.listener, &port));
    if (snprintf(uri, sizeof(uri), "tls://127.0.0.1:%u", (unsigned)port) <= 0) {
        status = SALTS_EINVAL;
        goto cleanup;
    }

    tls_client_config.size = sizeof(tls_client_config);
    tls_client_config.ca_file = ca_file;
    tls_client_config.cert_file = client_cert;
    tls_client_config.key_file = client_key;
    tls_client_config.server_name = "node-2.mesh";
    options.uri = uri;
    options.tls = &tls_client_config;
    options.observer = (cnet_observer){
        .on_state = tls_state,
        .on_receive = tls_receive,
        .user = &fixture.client_probe
    };
    TLS_TRY(cnet_connect(&fixture.client, &options, &fixture.outbound));
    if (mode == TLS_CASE_THREADED_HANDOFF) {
        TLS_TRY(cmeta_thread_create(
            &fixture.accept_thread, tls_handoff_accept_thread, &fixture));
        fixture.accept_thread_started = 1;
    }

    for (iteration = 0U; iteration < TLS_PROGRESS_MAX; ++iteration) {
        TLS_TRY(cnet_client_poll(&fixture.client, 1U, &events));
        if (!fixture.accepted && mode == TLS_CASE_THREADED_HANDOFF) {
            cnet_handoff_ticket taken = {0};
            cnet_accepted_stream detached = CNET_ACCEPTED_STREAM_INIT;
            int take_result = cnet_handoff_take(
                &fixture.handoff, &taken, &detached);
            if (take_result == SALTS_OK) {
                const cnet_observer observer = {
                    .on_state = tls_state,
                    .on_receive = tls_receive,
                    .user = &fixture.server_probe
                };
                fixture.handoff_phase = 3;
                if (taken.incarnation != fixture.ticket.incarnation ||
                    taken.generation != fixture.ticket.generation ||
                    taken.slot != fixture.ticket.slot) {
                    (void)cnet_accepted_stream_close(&detached);
                    failed_stage = "cross-thread ticket identity";
                    status = SALTS_EPROTO;
                    goto cleanup;
                }
                TLS_TRY(cnet_client_adopt_accepted_tls(
                    &fixture.server, &detached, &fixture.tls_server,
                    &observer, &fixture.server_probe.connection));
                fixture.accepted = 1;
            } else if (take_result != SALTS_ENOENT) {
                failed_stage = "cross-thread inbox take";
                status = take_result;
                goto cleanup;
            }
            if (!fixture.accepted &&
                atomic_load_explicit(&fixture.accept_done, memory_order_acquire)) {
                const int publish_result = atomic_load_explicit(
                    &fixture.accept_result, memory_order_acquire);
                if (publish_result != SALTS_OK) {
                    failed_stage = "cross-thread accept/publish";
                    status = publish_result;
                    goto cleanup;
                }
            }
        } else if (!fixture.accepted) {
            TLS_TRY(cnet_listener_wait(&fixture.listener, 0U, &ready));
            if (ready) {
                const cnet_observer observer = {
                    .on_state = tls_state,
                    .on_receive = tls_receive,
                    .user = &fixture.server_probe
                };
                if (mode == TLS_CASE_BOUNDED_HANDOFF)
                    TLS_TRY(tls_accept_via_handoff(&fixture, &observer));
                else
                    TLS_TRY(cnet_listener_accept_tls(
                        &fixture.listener, &fixture.server, &fixture.tls_server,
                        &observer, &fixture.server_probe.connection));
                fixture.accepted = 1;
            }
        }
        TLS_TRY(cnet_client_poll(&fixture.server, 1U, &events));
        if (fixture.client_probe.callback_error != SALTS_OK ||
            fixture.server_probe.callback_error != SALTS_OK) {
            status = SALTS_EPROTO;
            goto cleanup;
        }
        if (fixture.server_probe.auth_done &&
            (fixture.server_probe.auth_status != SALTS_OK ||
             fixture.server_probe.negotiated_done))
            break;
        if (fixture.client_probe.failed || fixture.server_probe.failed) {
            status = SALTS_EPROTO;
            goto cleanup;
        }
    }
    if (!fixture.client_probe.connected || !fixture.server_probe.connected) {
        status = SALTS_ETIMEDOUT;
        goto cleanup;
    }
    if (mode == TLS_CASE_WRONG_CERTIFICATE) {
        if (fixture.server_probe.auth_status != SALTS_EPROTO ||
            fixture.server_probe.authenticated_node != 0U ||
            fixture.server_probe.received_size != 0U)
            status = SALTS_EPROTO;
    } else {
        if (fixture.server_probe.auth_status != SALTS_OK ||
            fixture.server_probe.authenticated_node != 1U ||
            !fixture.server_probe.negotiated_done ||
            !fixture.client_probe.sent) {
            status = SALTS_EPROTO;
        } else if (mode == TLS_CASE_FORGED_NODE_ID ||
                   mode == TLS_CASE_WRONG_CLUSTER) {
            if (fixture.server_probe.negotiation_status != SALTS_EPROTO ||
                fixture.server_probe.handshake.complete)
                status = SALTS_EPROTO;
        } else if (fixture.server_probe.negotiation_status != SALTS_OK ||
                   fixture.server_probe.handshake.peer_node_id != 1U ||
                   fixture.server_probe.handshake.local_node_id != 2U) {
            status = SALTS_EPROTO;
        }
    }
    if (status == SALTS_OK &&
        (mode == TLS_CASE_BOUNDED_HANDOFF ||
         mode == TLS_CASE_THREADED_HANDOFF)) {
        cnet_handoff_snapshot snapshot = {0};
        TLS_TRY(cnet_handoff_get_snapshot(&fixture.handoff, &snapshot));
        if (snapshot.reserved != 0U || snapshot.queued != 0U ||
            snapshot.taken != 1U || snapshot.connection_capacity != 1U) {
            failed_stage = "handoff active ticket accounting";
            status = SALTS_EPROTO;
        }
    }

cleanup:
    /* Do not touch/destroy the listener until the independent accept owner
     * has quiesced. Worker publishes through the existing MPSC only; a
     * successful TAKEN credit survives until final CNet TLS shutdown. */
    if (fixture.accept_thread_started) {
        int join_status = cmeta_thread_join(&fixture.accept_thread);
        if (status == SALTS_OK && join_status != SALTS_OK) {
            status = join_status;
            failed_stage = "cross-thread join";
        }
        cmeta_thread_destroy(&fixture.accept_thread);
        if (atomic_load_explicit(&fixture.accept_result,
                                 memory_order_acquire) == SALTS_OK &&
            fixture.handoff_phase == 1)
            fixture.handoff_phase = 2;
    }
    if (fixture.client_initialized) {
        int close_status = cnet_client_stop(&fixture.client, 2000U);
        if (status == SALTS_OK && close_status != SALTS_OK) { status = close_status; failed_stage = "client stop"; }
        close_status = cnet_client_destroy(&fixture.client);
        if (status == SALTS_OK && close_status != SALTS_OK) status = close_status;
    }
    if (fixture.server_initialized) {
        int close_status = cnet_client_stop(&fixture.server, 2000U);
        if (status == SALTS_OK && close_status != SALTS_OK) { status = close_status; failed_stage = "server stop"; }
        close_status = cnet_client_destroy(&fixture.server);
        if (status == SALTS_OK && close_status != SALTS_OK) status = close_status;
    }
    if (fixture.listener_initialized) {
        /* A listening socket is a separate, domain-owned CNet resource.
         * Destroy refuses an OPEN listener with SALTS_EBUSY; first withdraw
         * admission, then release the closed owner exactly once. */
        int close_status = cnet_listener_close(&fixture.listener);
        if (status == SALTS_OK && close_status != SALTS_OK) {
            status = close_status;
            failed_stage = "listener close";
        }
        close_status = cnet_listener_destroy(&fixture.listener);
        if (status == SALTS_OK && close_status != SALTS_OK) {
            status = close_status;
            failed_stage = "listener destroy";
        }
    }
    if (fixture.handoff_initialized) {
        int close_status = cnet_handoff_seal(&fixture.handoff);
        if (status == SALTS_OK && close_status != SALTS_OK) {
            status = close_status;
            failed_stage = "handoff seal";
        }
        if (fixture.handoff_phase == 2) {
            cnet_handoff_ticket taken = {0};
            cnet_accepted_stream pending = CNET_ACCEPTED_STREAM_INIT;
            close_status = cnet_handoff_take(
                &fixture.handoff, &taken, &pending);
            if (close_status == SALTS_OK) {
                (void)cnet_accepted_stream_close(&pending);
                fixture.ticket = taken;
                fixture.handoff_phase = 3;
            } else if (status == SALTS_OK) {
                status = close_status;
                failed_stage = "handoff drain queued";
            }
        }
        if (fixture.handoff_phase == 1 ||
            fixture.handoff_phase == 3) {
            close_status = cnet_handoff_release(
                &fixture.handoff, fixture.ticket);
            if (status == SALTS_OK && close_status != SALTS_OK) {
                status = close_status;
                failed_stage = "handoff release";
            }
            if (close_status == SALTS_OK)
                fixture.handoff_phase = 4;
        }
        close_status = cnet_handoff_destroy(&fixture.handoff);
        if (status == SALTS_OK && close_status != SALTS_OK) {
            status = close_status;
            failed_stage = "handoff destroy";
        }
    }
    if (fixture.tls_initialized) {
        int close_status = cnet_tls_server_destroy(&fixture.tls_server);
        if (status == SALTS_OK && close_status != SALTS_OK) status = close_status;
    }
#undef TLS_TRY
    if (status != SALTS_OK)
        fprintf(stderr,
                "mTLS case=%d status=%d stage=%s client_connected=%d "
                "server_connected=%d auth=%d done=%d nego=%d sent=%d "
                "client_callback=%d server_callback=%d client_failed=%d "
                "server_failed=%d received=%zu\\n",
                mode, status, failed_stage, fixture.client_probe.connected,
                fixture.server_probe.connected, fixture.server_probe.auth_status,
                fixture.server_probe.negotiated_done,
                fixture.server_probe.negotiation_status,
                fixture.client_probe.sent,
                fixture.client_probe.callback_error,
                fixture.server_probe.callback_error,
                fixture.client_probe.failed, fixture.server_probe.failed,
                fixture.server_probe.received_size);
    return status;
}

spec("ACE 2.3 real CNet mTLS Raft Node ID admission")
{
    it("binds a verified client certificate to its exact Raft HELLO node")
    {
        check_equal(tls_case_run(TLS_CASE_VALID), SALTS_OK);
    }

    it("rejects a CA-valid TLS client when policy names another certificate")
    {
        check_equal(tls_case_run(TLS_CASE_WRONG_CERTIFICATE), SALTS_OK);
    }

    it("rejects a forged Raft HELLO node ID on an authenticated TLS stream")
    {
        check_equal(tls_case_run(TLS_CASE_FORGED_NODE_ID), SALTS_OK);
    }

    it("rejects a foreign cluster HELLO despite a valid TLS client certificate")
    {
        check_equal(tls_case_run(TLS_CASE_WRONG_CLUSTER), SALTS_OK);
    }

    it("takes a bounded accepted stream and starts TLS on its final owner")
    {
        check_equal(tls_case_run(TLS_CASE_BOUNDED_HANDOFF), SALTS_OK);
    }

    it("hands a detached socket across real OS threads before owner-local TLS")
    {
        check_equal(tls_case_run(TLS_CASE_THREADED_HANDOFF), SALTS_OK);
    }
}
