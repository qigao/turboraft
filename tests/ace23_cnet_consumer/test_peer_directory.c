#include <turboraft/raft_cnet_peer_directory.h>

#include <cmeta_error.h>
#include <salts/thread.h>
#include <tinytest.h>

#include <string.h>

/* Real CNetManager/ManagedDial objects, not fake ManagedPeer pointers.
 * No network connections are admitted: this tests pure, bounded routing
 * and identity/owner preflight before transport side effects. */
#define TEST_CERT_1 "eb571a92b33237897216c79501066b3e77391047eaeb6be084526c4769657549"
#define TEST_CERT_3 "d696b3ab8d0596e3e8e8abcecc4ca87856eda0f5055f31a7dfb6c213c2c2b1f3"

typedef struct directory_fixture {
    cnet_client client;
    cnet_manager manager;
    cnet_tls_client_config tls[2];
    tr_raft_cnet_managed_peer_t *peers[2];
    tr_raft_cnet_peer_identity_t identities[2];
    const char *certs[2][1];
    tr_raft_cnet_identity_policy_t identity_policy;
    tr_raft_cnet_peer_directory_entry_t routes[2];
    tr_raft_group_id_t groups[2];
    tr_raft_cnet_peer_directory_t directory;
    tr_raft_cnet_peer_directory_config_t directory_config;
    int client_live;
    int manager_live;
} directory_fixture;

static cnet_client_config test_client_config(void)
{
    return (cnet_client_config){
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
        .tls_io_buffer_bytes = CNET_TLS_MIN_IO_BUFFER_BYTES,
        .tls_handshake_timeout_ms = 2000U
    };
}

static tr_raft_handshake_config_t test_hello(void)
{
    tr_raft_handshake_config_t hello = {0};
    size_t i;

    for (i = 0U; i < sizeof(hello.cluster_id.bytes); ++i)
        hello.cluster_id.bytes[i] = (uint8_t)(i + 17U);
    hello.local_node_id = 2U;
    hello.process_incarnation.bytes[0] = 2U;
    hello.config_epoch = 1U;
    hello.wire_major_min = TR_RAFT_HANDSHAKE_WIRE_MAJOR;
    hello.wire_major_max = TR_RAFT_HANDSHAKE_WIRE_MAJOR;
    hello.wire_minor_min = TR_RAFT_HANDSHAKE_WIRE_MINOR;
    hello.wire_minor_max = TR_RAFT_HANDSHAKE_WIRE_MINOR;
    hello.max_frame_size = TR_RAFT_WIRE_MAX_FRAME_SIZE;
    hello.max_snapshot_chunk_size = TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES;
    return hello;
}

static int reject_unexpected_data(
    void *context, const tr_raft_transport_payload_t *payload)
{
    (void)context;
    (void)payload;
    return SALTS_EPROTO;
}

static int fixture_init(directory_fixture *f)
{
    cnet_client_config cnet = test_client_config();
    cnet_manager_config m = {0};
    size_t i;
    int result;

    result = cnet_client_init(&f->client, &cnet);
    if (result != SALTS_OK) return result;
    f->client_live = 1;

    m.size = sizeof(m);
    m.version = CNET_MANAGER_VERSION;
    m.client = &f->client;
    m.record_capacity = 2U;
    m.connection_capacity = 2U;
    result = cnet_manager_init(&f->manager, &m);
    if (result != SALTS_OK) return result;
    f->manager_live = 1;

    f->certs[0][0] = TEST_CERT_1;
    f->certs[1][0] = TEST_CERT_3;
    f->identities[0] = (tr_raft_cnet_peer_identity_t){
        1U, f->certs[0], 1U
    };
    f->identities[1] = (tr_raft_cnet_peer_identity_t){
        3U, f->certs[1], 1U
    };
    f->identity_policy = (tr_raft_cnet_identity_policy_t){
        2U, f->identities, 2U
    };

    for (i = 0U; i < 2U; ++i) {
        const tr_raft_node_id_t node_id = f->identities[i].node_id;
        tr_raft_cnet_managed_peer_config_t cfg = {0};
        cnet_reconnect_config retry = {0};

        f->tls[i].size = sizeof(f->tls[i]);
        f->tls[i].server_name = "not-dialed.example.test";
        retry.size = sizeof(retry);
        retry.version = CNET_RECOVERY_POLICY_VERSION;
        retry.max_attempts = 2U;
        retry.deadline_ms = UINT64_C(1000000);
        retry.initial_backoff_ms = 4U;
        retry.maximum_backoff_ms = 16U;
        retry.jitter_seed = 1U + i;
        cfg.manager = &f->manager;
        cfg.channel.client = &f->client;
        cfg.channel.identity = &f->identity_policy;
        cfg.channel.handshake = test_hello();
        cfg.channel.first_outbound_message_id = 1U;
        cfg.channel.on_payload = reject_unexpected_data;
        cfg.expected_peer_node_id = node_id;
        cfg.uri = "tls://127.0.0.1:38921";
        cfg.tls = &f->tls[i];
        cfg.reconnect = retry;
        cfg.recovery_episode_ms = 1000U;

        result = tr_raft_cnet_managed_peer_create(&cfg, &f->peers[i]);
        if (result != SALTS_OK) return result;
        f->routes[i].node_id = node_id;
        f->routes[i].peer = f->peers[i];
    }

    f->groups[0] = 42U;
    f->groups[1] = 43U;
    f->directory_config = (tr_raft_cnet_peer_directory_config_t){
        TR_RAFT_CNET_PEER_DIRECTORY_VERSION,
        2U, 0U, 1U, &f->identity_policy,
        f->routes, 2U, f->groups, 2U
    };
    return SALTS_OK;
}

static int fixture_close(directory_fixture *f)
{
    int result, first = SALTS_OK;
    size_t i;

#define CLOSE_STEP(expr) do { \
    result = (expr); \
    if (first == SALTS_OK && result != SALTS_OK) first = result; \
} while (0)
    if (f->directory.active)
        CLOSE_STEP(tr_raft_cnet_peer_directory_destroy(&f->directory));
    for (i = 0U; i < 2U; ++i) {
        if (f->peers[i] != NULL) {
            CLOSE_STEP(tr_raft_cnet_managed_peer_stop(f->peers[i]));
            CLOSE_STEP(tr_raft_cnet_managed_peer_destroy(f->peers[i]));
        }
    }
    if (f->manager_live)
        CLOSE_STEP(cnet_manager_destroy(&f->manager));
    if (f->client_live) {
        CLOSE_STEP(cnet_client_stop(&f->client, 1000U));
        CLOSE_STEP(cnet_client_destroy(&f->client));
    }
#undef CLOSE_STEP
    return first;
}

typedef struct foreign_lookup {
    tr_raft_cnet_peer_directory_t *directory;
    int result;
    int borrowed;
} foreign_lookup;

static void foreign_owner_lookup(void *context)
{
    foreign_lookup *task = (foreign_lookup *)context;
    tr_raft_cnet_managed_peer_t *peer =
        (tr_raft_cnet_managed_peer_t *)(uintptr_t)1U;

    task->result = tr_raft_cnet_peer_directory_lookup(
        task->directory, 1U, &peer);
    task->borrowed = peer != NULL;
}

spec("ACE 2.3 distinct Node ID CNet directory, strict fixed owner")
{
    it("admits exactly Node 1 and 3 on Owner 0 with independent Group 42/43 routing")
    {
        directory_fixture f = {0};
        tr_raft_cnet_managed_peer_t *found = NULL;
        tr_raft_transport_t transport = {0};
        tr_raft_cnet_directory_group_binding_t binding = {0};
        tr_raft_cnet_directory_group_binding_t invalid_binding = {0};
        tr_raft_transport_t unbound_transport = {0};
        tr_raft_message_t message = {0};
        tr_raft_transport_payload_t payload = {0};
        tr_raft_cnet_managed_peer_status_t peer_status = {0};
        cnet_manager_snapshot manager = {0};
        foreign_lookup other = {0};
        cmeta_thread_t thread = {0};
        int finished;

        check_equal(fixture_init(&f), SALTS_OK);
        check_equal(tr_raft_cnet_peer_directory_init(
            &f.directory, &f.directory_config), SALTS_OK);
        check_equal(tr_raft_cnet_peer_directory_init(
            &f.directory, &f.directory_config), SALTS_EALREADY);

        check_equal(tr_raft_cnet_peer_directory_lookup(
            &f.directory, 1U, &found), SALTS_OK);
        check(found == f.peers[0]);
        check_equal(tr_raft_cnet_peer_directory_lookup(
            &f.directory, 3U, &found), SALTS_OK);
        check(found == f.peers[1]);
        check_equal(tr_raft_cnet_peer_directory_lookup(
            &f.directory, 9U, &found), SALTS_ENOENT);
        check_null(found);
        check_equal(tr_raft_cnet_peer_directory_lookup(
            &f.directory, 2U, &found), SALTS_EINVAL);
        check_null(found);

        /* Directory ACK routing is the same strict Node+Group gate,
         * followed by ManagedPeer's exact live TLS generation check. A
         * long-lived Node ID route alone must NEVER authorize a delayed
         * receiver ACK on an unready or recycled connection. */
        {
            tr_raft_multicore_completion_t ack = {0};
            ack.request_id = 44U;
            ack.operation = TR_RAFT_MULTICORE_RECEIVE_CHUNK;
            ack.result = SALTS_OK;
            ack.value.chunk.kind = TR_RAFT_WIRE_PAYLOAD_DATA_CHUNK;
            ack.value.chunk.ack_valid = true;
            ack.value.chunk.ack.data.from = 2U;
            ack.value.chunk.ack.data.to = 1U;
            ack.value.chunk.ack.data.term = 5U;
            ack.value.chunk.ack.data.stream_id = 9U;
            ack.value.chunk.ack.data.accepted = true;
            ack.reply_origin = (tr_raft_transport_reply_origin_t){
                .channel_instance = 7U,
                .authenticated_peer_node_id = 1U,
                .group_id = 43U,
                .connection_token = UINT64_C(4294967300)
            };
            check_equal(tr_raft_cnet_peer_directory_send_chunk_completion(
                &f.directory, &ack), SALTS_ECANCELED);
            ack.reply_origin.group_id = 44U;
            check_equal(tr_raft_cnet_peer_directory_send_chunk_completion(
                &f.directory, &ack), SALTS_ENOENT);
            ack.reply_origin.group_id = 43U;
            ack.reply_origin.authenticated_peer_node_id = 9U;
            check_equal(tr_raft_cnet_peer_directory_send_chunk_completion(
                &f.directory, &ack), SALTS_ENOENT);
            ack.reply_origin.channel_instance = 0U;
            check_equal(tr_raft_cnet_peer_directory_send_chunk_completion(
                &f.directory, &ack), SALTS_ENOTSUP);
        }

        /* A future unknown Group cannot acquire an apparently valid
         * Service callback and only be rejected after WAL commit. */
        invalid_binding.directory = &f.directory;
        invalid_binding.group_id = 44U;
        check_equal(tr_raft_cnet_peer_directory_transport_bind(
            &invalid_binding, &unbound_transport), SALTS_ENOENT);
        check_null(unbound_transport.enqueue);
        check_null(unbound_transport.context);

        binding.directory = &f.directory;
        binding.group_id = 42U;
        transport.snapshot_context = &f;
        check_equal(tr_raft_cnet_peer_directory_transport_bind(
            &binding, &transport), SALTS_OK);
        check_equal(tr_raft_cnet_peer_directory_transport_bind(
            &binding, &transport), SALTS_EALREADY);
        check(transport.snapshot_context == &f);
        check(transport.context == &binding);
        check_not_null(transport.enqueue);

        message.from = 2U;
        message.to = 1U;
        message.type = TR_RAFT_MSG_HEARTBEAT_REQUEST;
        check_equal(transport.enqueue(transport.context, &message),
                    SALTS_ENOSPC);
        message.to = 3U;
        check_equal(transport.enqueue(transport.context, &message),
                    SALTS_ENOSPC);
        message.to = 9U;
        check_equal(transport.enqueue(transport.context, &message),
                    SALTS_ENOENT);
        message.from = 1U;
        message.to = 3U;
        check_equal(transport.enqueue(transport.context, &message),
                    SALTS_EPROTO);

        payload.kind = TR_RAFT_WIRE_PAYLOAD_DATA_CHUNK;
        payload.group_id = 43U;
        payload.data.data_chunk.from = 2U;
        payload.data.data_chunk.to = 3U;
        check_equal(tr_raft_cnet_peer_directory_send(
            &f.directory, &payload), SALTS_ENOSPC);
        payload.group_id = 44U;
        check_equal(tr_raft_cnet_peer_directory_send(
            &f.directory, &payload), SALTS_ENOENT);
        payload.group_id = 43U;
        payload.data.data_chunk.to = 2U;
        check_equal(tr_raft_cnet_peer_directory_send(
            &f.directory, &payload), SALTS_EPROTO);

        other.directory = &f.directory;
        check_equal(cmeta_thread_create(
            &thread, foreign_owner_lookup, &other), SALTS_OK);
        check_equal(cmeta_thread_join(&thread), SALTS_OK);
        cmeta_thread_destroy(&thread);
        check_equal(other.result, SALTS_EPERM);
        check_equal(other.borrowed, 0);

        check_equal(cnet_manager_get_snapshot(&f.manager, &manager),
                    SALTS_OK);
        check_equal(manager.reserved, (size_t)0U);
        check_equal(manager.bound, (size_t)0U);
        check_equal(tr_raft_cnet_managed_peer_get_status(
            f.peers[0], &peer_status), SALTS_OK);
        check_equal(peer_status.connections_started, (size_t)0U);
        check_equal(tr_raft_cnet_managed_peer_get_status(
            f.peers[1], &peer_status), SALTS_OK);
        check_equal(peer_status.connections_started, (size_t)0U);

        finished = fixture_close(&f);
        check_equal(finished, SALTS_OK);
    }

    it("rejects duplicate identity, wrong fixed owner and ambiguous Group before admission")
    {
        directory_fixture f = {0};
        tr_raft_cnet_peer_directory_config_t cfg;
        tr_raft_cnet_peer_identity_t authorities[3];
        const char *other_cert[1] = {
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        };
        tr_raft_cnet_managed_peer_t *foreign = (tr_raft_cnet_managed_peer_t *)
            (uintptr_t)1U;
        tr_raft_message_t routed = {0};
        tr_raft_transport_payload_t payload = {0};
        int result;

        check_equal(fixture_init(&f), SALTS_OK);
        cfg = f.directory_config;
        cfg.owner_count = 2U;
        cfg.owner_index = 0U;
        check_equal(tr_raft_cnet_peer_directory_init(
            &f.directory, &cfg), SALTS_EINVAL);
        check_equal(f.directory.active, 0);
        /* Node 4 is authorized globally, but STRICT_KEY pins its CNet
         * connection to Owner 0. Owner 1 has no right to borrow it. */
        authorities[0] = f.identities[0];
        authorities[1] = f.identities[1];
        authorities[2] = (tr_raft_cnet_peer_identity_t){
            4U, other_cert, 1U
        };
        f.identity_policy.peers = authorities;
        f.identity_policy.peer_count = 3U;
        cfg.owner_index = 1U; /* Node 1 and 3 pin to Owner 1. */
        check_equal(tr_raft_cnet_peer_directory_init(
            &f.directory, &cfg), SALTS_OK);
        check_equal(tr_raft_cnet_peer_directory_lookup(
            &f.directory, 4U, &foreign), SALTS_EPERM);
        check_null(foreign);
        routed.from = 2U;
        routed.to = 4U;
        routed.type = TR_RAFT_MSG_HEARTBEAT_REQUEST;
        payload.group_id = 42U;
        payload.kind = TR_RAFT_WIRE_PAYLOAD_RAFT;
        payload.data.raft = routed;
        check_equal(tr_raft_cnet_peer_directory_send(
            &f.directory, &payload), SALTS_EPERM);
        check_equal(tr_raft_cnet_peer_directory_destroy(&f.directory),
                    SALTS_OK);
        f.identity_policy.peers = f.identities;
        f.identity_policy.peer_count = 2U;

        cfg = f.directory_config;
        f.routes[1].node_id = 1U;
        check_equal(tr_raft_cnet_peer_directory_init(
            &f.directory, &cfg), SALTS_EINVAL);
        f.routes[1].node_id = 3U;
        f.routes[1].peer = f.peers[0];
        check_equal(tr_raft_cnet_peer_directory_init(
            &f.directory, &cfg), SALTS_EINVAL);
        f.routes[1].peer = f.peers[1];

        f.groups[1] = f.groups[0];
        check_equal(tr_raft_cnet_peer_directory_init(
            &f.directory, &cfg), SALTS_EINVAL);
        f.groups[1] = 43U;
        f.identity_policy.local_node_id = 4U;
        check_equal(tr_raft_cnet_peer_directory_init(
            &f.directory, &cfg), SALTS_EINVAL);
        f.identity_policy.local_node_id = 2U;

        check_equal(tr_raft_cnet_peer_directory_init(
            &f.directory, &cfg), SALTS_OK);
        result = fixture_close(&f);
        check_equal(result, SALTS_OK);
    }
}
