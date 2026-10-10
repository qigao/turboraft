#include <turboraft/raft_core.h>
#include <turboraft/raft_cnet_channel.h>
#include <cmeta_error.h>
#include <cmeta_fs.h>
#include <salts/clock.h>

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Physical-link loss reuses the ticked-minority Core safety assertions.
 * No fallback transport is added to production or the test harness. */
#ifdef TURBORAFT_TEST_CERTIFIED_QUORUM_PHYSICAL_LOSS
#define TURBORAFT_TEST_CERTIFIED_QUORUM_TICKED_LOSS 1
#endif
/* A separately registered ticked-loss executable shares the negative
 * majority fixture; never change the positive quorum executable. */
#ifdef TURBORAFT_TEST_CERTIFIED_QUORUM_TICKED_LOSS
#define TURBORAFT_TEST_CERTIFIED_QUORUM_LOSS 1
#endif

/*
 * REAL certified mTLS transport for three independent RaftCore instances.
 * Node2 is the candidate/leader. Node2<->Node1 and Node2<->Node3 each
 * have their own actual CNet TLS Channel, certificate/HELLO and wire codec.
 * Every vote and Append request/response in the 2/3 quorum path traverses
 * CNet. Node1's Core is muted at the test receive callback during the
 * initial majority campaign (not a claim of physical link partition).
 *
 * Core Ready is persisted to a bounded TEST IN-MEMORY durable-value struct
 * before any emitted message enters a bounded test-only outbound queue.
 * This is not WAL durability, not three OS processes, and NOT an extra
 * production mailbox/scheduler. The separate WAL SIGKILL tests qualify
 * real authoritative disk recovery. There are NO direct Node1<->Node3
 * links in this test; leader2 communicates with each remote separately.
 */
enum {
    PEERS = 2U,
    NODES = 3U,
    MAX_LOG = 32U,
    READY_MESSAGES = TR_RAFT_MAX_MEMBERS * 2U,
    PENDING_CAPACITY = 128U,
    MAX_PROGRESS = 11000U
};
static const tr_raft_node_id_t VOTERS[NODES] = {1U,2U,3U};
static const tr_raft_node_id_t PEER_IDS[PEERS] = {1U,3U};

#define NODE1_FINGERPRINT "eb571a92b33237897216c79501066b3e77391047eaeb6be084526c4769657549"
#define NODE3_FINGERPRINT "d696b3ab8d0596e3e8e8abcecc4ca87856eda0f5055f31a7dfb6c213c2c2b1f3"
#define NODE2_FINGERPRINT "44e8fe3ce37ede1c2a1b5d36efa345cb662887d4250d17a000ebea2e834aed95"

typedef struct durable_values {
    tr_raft_term_t term;
    tr_raft_node_id_t vote;
    tr_raft_index_t committed;
    tr_raft_index_t applied;
    tr_raft_entry_t entries[MAX_LOG];
    size_t length;
} durable_values_t;

typedef struct core_node {
    tr_raft_core_t *core;
    durable_values_t durable;
} core_node_t;

struct quorum_fixture;
typedef struct peer_endpoint {
    struct quorum_fixture *fixture;
    tr_raft_cnet_channel_t *channel;
    tr_raft_node_id_t expected_node; /* client: self; server: discovered */
} peer_endpoint_t;

typedef struct quorum_fixture {
    core_node_t nodes[NODES];
    tr_raft_message_t pending[PENDING_CAPACITY];
    size_t pending_count;
    size_t wire_sent;
    size_t wire_recv;
    size_t real_votes_to_leader;
    size_t real_appends_to_three;
    size_t real_append_acks_from_three;
    size_t lost_node_one;
    size_t caught_up_node_one;
#ifdef TURBORAFT_TEST_CERTIFIED_QUORUM_LOSS
    size_t lost_node_three;
    size_t lost_append_three;
    unsigned quorum_loss_ticks;
    int saw_check_quorum_demotion;
    int node_three_muted;
#endif
#ifdef TURBORAFT_TEST_CERTIFIED_QUORUM_PHYSICAL_LOSS
    int node_three_link_down;
    unsigned node_three_socket_slot;
    unsigned node_three_reconnected;
    size_t offline_raft_outputs;
    tr_raft_transport_reply_origin_t node_three_old_origin;
    tr_raft_transport_reply_origin_t node_three_new_origin;
#endif
    int node_one_muted;
    int callback_error;
    cnet_client clients, server;
    cnet_listener listener;
    cnet_tls_server server_tls;
    int clients_live, server_live, listener_live, tls_live;
    peer_endpoint_t client_endpoint[PEERS], server_endpoint[PEERS];
    tr_raft_cnet_channel_t *outbound[PEERS], *inbound[PEERS];
    cnet_connection outbound_connection[PEERS], inbound_connection[PEERS];
    cnet_tls_client_config client_tls[PEERS];
    tr_raft_cnet_identity_policy_t client_policy[PEERS], server_policy;
    tr_raft_cnet_peer_identity_t client_server_peer[PEERS], server_peers[PEERS];
    const char *client_server_fingerprint[PEERS][1];
    const char *server_fingerprint[PEERS][1];
    char cert_paths[7][512];
    int accepted;
} quorum_fixture_t;

static int fixture_path(char *out, size_t cap, const char *file)
{
    int n = snprintf(out, cap, "%s/%s", TURBORAFT_ACE23_FIXTURE_DIR, file);
    return n > 0 && (size_t)n < cap ? SALTS_OK : SALTS_ERANGE;
}

static cnet_client_config net_config(void)
{
    return (cnet_client_config){
#if defined(_WIN32)
        .backend = NATIVE_IO_BACKEND_IOCP,
#elif defined(__linux__)
        .backend = NATIVE_IO_BACKEND_EPOLL,
#else
        .backend = NATIVE_IO_BACKEND_KQUEUE,
#endif
        .connection_capacity = PEERS,
        .command_capacity = 32U,
        .request_capacity = 32U,
        .completion_batch_capacity = 16U,
        .event_capacity = 32U,
        .max_send_bytes = 4096U,
        .receive_buffer_bytes = 4096U,
        .connect_timeout_ms = 2000U,
        /* Node1 deliberately acknowledges no Raft messages for several
         * elections while its TLS connection stays live. This is a FINITE
         * fixture lifetime bound, not an implicit reconnect or retry.
         * Heartbeats to Node3 are driven by real elapsed time below. */
        .read_timeout_ms = 15000U,
        .write_timeout_ms = 5000U,
        .tls_io_buffer_bytes = CNET_TLS_MIN_IO_BUFFER_BYTES,
        .tls_handshake_timeout_ms = 2000U
    };
}

static tr_raft_handshake_config_t hello(tr_raft_node_id_t self)
{
    tr_raft_handshake_config_t config = {0};
    size_t i;
    for (i = 0U; i < sizeof(config.cluster_id.bytes); ++i)
        config.cluster_id.bytes[i] = (uint8_t)(i + 17U);
    config.local_node_id = self;
    config.process_incarnation.bytes[0] = (uint8_t)self;
    config.config_epoch = 1U;
    config.wire_major_min = config.wire_major_max =
        TR_RAFT_HANDSHAKE_WIRE_MAJOR;
    config.wire_minor_min = config.wire_minor_max =
        TR_RAFT_HANDSHAKE_WIRE_MINOR;
    config.max_frame_size = TR_RAFT_WIRE_MAX_FRAME_SIZE;
    config.max_snapshot_chunk_size = TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES;
    return config;
}

static tr_raft_ready_t make_ready(tr_raft_message_t *messages)
{
    tr_raft_ready_t ready = {0};
    ready.messages = messages;
    ready.message_capacity = READY_MESSAGES;
    return ready;
}

static int persist_and_stage(quorum_fixture_t *f, size_t node,
                             const tr_raft_ready_t *ready)
{
    durable_values_t *d = &f->nodes[node].durable;
    size_t i;
    if (ready->hard_state_changed) {
        d->term = ready->term;
        d->vote = ready->voted_for;
    }
    if (ready->log_changed && ready->log_truncate_from != 0U) {
        if (ready->log_truncate_from > d->length + 1U)
            return SALTS_EPROTO;
        d->length = (size_t)ready->log_truncate_from - 1U;
    }
    for (i = 0U; i < ready->log_entry_count; ++i) {
        const tr_raft_entry_t *e = &ready->log_entries[i];
        if (e->index == 0U || e->index > MAX_LOG ||
            e->index > d->length + 1U)
            return SALTS_EPROTO;
        d->entries[e->index - 1U] = *e;
        if (e->index > d->length) d->length = (size_t)e->index;
    }
    if (ready->commit_changed) {
        if (ready->commit_index > d->length) return SALTS_EPROTO;
        d->committed = ready->commit_index;
    }
    if (ready->committed_entry_count != 0U) {
        tr_raft_index_t applied =
            ready->committed_entries[ready->committed_entry_count - 1U].index;
        if (applied > d->committed) return SALTS_EPROTO;
        d->applied = applied;
    }
    if (ready->message_count > PENDING_CAPACITY - f->pending_count)
        return SALTS_ENOSPC;
    for (i = 0U; i < ready->message_count; ++i)
        f->pending[f->pending_count++] = ready->messages[i];
    if (ready->message_count || ready->hard_state_changed ||
        ready->role_changed || ready->log_changed ||
        ready->commit_changed || ready->committed_entry_count ||
        ready->read_state_ready)
        return tr_raft_core_advance(f->nodes[node].core);
    return SALTS_OK;
}

static int core_step(quorum_fixture_t *f, size_t node,
                     const tr_raft_message_t *message)
{
    tr_raft_message_t outputs[READY_MESSAGES];
    tr_raft_ready_t ready = make_ready(outputs);
    int rc = tr_raft_core_step(f->nodes[node].core, message, &ready);
    return rc == SALTS_OK ? persist_and_stage(f,node,&ready) : rc;
}

static int core_tick(quorum_fixture_t *f, size_t node, uint32_t ticks)
{
    tr_raft_message_t outputs[READY_MESSAGES];
    tr_raft_ready_t ready = make_ready(outputs);
    tr_raft_tick_t tick = {ticks, 4U};
    int rc = tr_raft_core_tick(f->nodes[node].core, &tick, &ready);
    return rc == SALTS_OK ? persist_and_stage(f,node,&ready) : rc;
}

static int core_propose(quorum_fixture_t *f, uint64_t command,
                        const char *value)
{
    tr_raft_message_t outputs[READY_MESSAGES];
    tr_raft_ready_t ready = make_ready(outputs);
    tr_raft_proposal_t proposal = {command, value, strlen(value)};
    int rc = tr_raft_core_propose(f->nodes[1].core, &proposal, &ready);
    return rc == SALTS_OK ? persist_and_stage(f,1U,&ready) : rc;
}

static int server_receive(void *context,
                          const tr_raft_transport_payload_t *payload)
{
    peer_endpoint_t *endpoint = (peer_endpoint_t *)context;
    quorum_fixture_t *f = endpoint->fixture;
    tr_raft_cnet_channel_status_t status = {0};
    const tr_raft_message_t *message;
    int rc;
    if (payload == NULL || payload->kind != TR_RAFT_WIRE_PAYLOAD_RAFT ||
        payload->group_id != 77U) return SALTS_EPROTO;
    message = &payload->data.raft;
    rc = tr_raft_cnet_channel_get_status(endpoint->channel, &status);
    if (rc != SALTS_OK || status.phase != TR_RAFT_CNET_CHANNEL_ACTIVE ||
        message->from != status.authenticated_peer_node_id ||
        message->to != 2U ||
        (message->from != 1U && message->from != 3U))
        return SALTS_EPROTO;
    ++f->wire_recv;
    if (message->from == 3U &&
        (message->type == TR_RAFT_MSG_VOTE_RESPONSE ||
         message->type == TR_RAFT_MSG_PRE_VOTE_RESPONSE))
        ++f->real_votes_to_leader;
    if (message->from == 3U &&
        message->type == TR_RAFT_MSG_APPEND_RESPONSE)
        ++f->real_append_acks_from_three;
    rc = core_step(f,1U,message);
    if (rc != SALTS_OK) f->callback_error = rc;
    return rc;
}

static int client_receive(void *context,
                          const tr_raft_transport_payload_t *payload)
{
    peer_endpoint_t *endpoint = (peer_endpoint_t *)context;
    quorum_fixture_t *f = endpoint->fixture;
    const tr_raft_message_t *m;
    size_t index = (size_t)endpoint->expected_node - 1U;
    int rc;
    if (payload == NULL || payload->kind != TR_RAFT_WIRE_PAYLOAD_RAFT ||
        payload->group_id != 77U)
        return SALTS_EPROTO;
    m = &payload->data.raft;
    if (m->from != 2U || m->to != endpoint->expected_node ||
        (m->to != 1U && m->to != 3U))
        return SALTS_EPROTO;
    ++f->wire_recv;
    if (m->to == 3U && m->type == TR_RAFT_MSG_APPEND_REQUEST)
        ++f->real_appends_to_three;
#ifdef TURBORAFT_TEST_CERTIFIED_QUORUM_LOSS
    if (m->to == 3U && f->node_three_muted) {
        /* The complete certified TLS frame has been decoded, but this
         * ONE test receiver explicitly discards it BEFORE Core.step.
         * This is not a second network owner, a real TCP partition, or a
         * replay/settlement retry. All actual ACKs from Node3 must stop. */
        ++f->lost_node_three;
        if (m->type == TR_RAFT_MSG_APPEND_REQUEST)
            ++f->lost_append_three;
        return SALTS_OK;
    }
#endif
    if (m->to == 1U && f->node_one_muted) {
        ++f->lost_node_one; /* test-only receive mute, NOT a real link fault */
        return SALTS_OK;
    }
    if (m->to == 1U && m->type == TR_RAFT_MSG_APPEND_REQUEST)
        ++f->caught_up_node_one;
    rc = core_step(f,index,m);
    if (rc != SALTS_OK) f->callback_error = rc;
    return rc;
}

static int send_one(quorum_fixture_t *f)
{
    tr_raft_transport_payload_t payload = {0};
    tr_raft_cnet_channel_t *channel = NULL;
    tr_raft_message_t m;
    size_t i;
    int rc;

    if (f->pending_count == 0U) return SALTS_OK;
    m = f->pending[0];
#ifdef TURBORAFT_TEST_CERTIFIED_QUORUM_PHYSICAL_LOSS
    if (f->node_three_link_down && (m.from == 3U || m.to == 3U)) {
        /* Explicit TEST harness transport rejection while the real CNet
         * connection is TERMINAL. This Ready output was NEVER admitted by
         * CNet, so it is neither settlement retry nor network delivery.
         * The authoritative Core must recover via a NEW election/Append. */
        if (m.to == 3U && m.type == TR_RAFT_MSG_APPEND_REQUEST)
            ++f->lost_append_three;
        ++f->offline_raft_outputs;
        if (f->pending_count > 1U)
            memmove(&f->pending[0],&f->pending[1],
                    (f->pending_count - 1U) * sizeof(f->pending[0]));
        --f->pending_count;
        return SALTS_OK;
    }
#endif
    if (m.from == 2U && (m.to == 1U || m.to == 3U)) {
        for (i = 0U; i < PEERS; ++i) {
            tr_raft_cnet_channel_status_t status = {0};
            rc = tr_raft_cnet_channel_get_status(f->inbound[i], &status);
            if (rc != SALTS_OK) return rc;
            if (status.authenticated_peer_node_id == m.to &&
                status.phase == TR_RAFT_CNET_CHANNEL_ACTIVE) {
                channel = f->inbound[i];
                break;
            }
        }
    } else if (m.to == 2U && (m.from == 1U || m.from == 3U)) {
        for (i = 0U; i < PEERS; ++i)
            if (PEER_IDS[i] == m.from) channel = f->outbound[i];
    }
    if (channel == NULL) return SALTS_EPROTO;
    payload.group_id = 77U;
    payload.kind = TR_RAFT_WIRE_PAYLOAD_RAFT;
    payload.data.raft = m;
    rc = tr_raft_cnet_channel_send(channel,&payload);
    if (rc != SALTS_OK) return rc; /* never retry an uncertain accepted send */
    if (f->pending_count > 1U)
        memmove(&f->pending[0],&f->pending[1],
                (f->pending_count - 1U) * sizeof(f->pending[0]));
    --f->pending_count;
    ++f->wire_sent;
    return SALTS_OK;
}

static int network_progress(quorum_fixture_t *f)
{
    size_t events = 0U;
    int rc;
    if (f->callback_error != SALTS_OK) return f->callback_error;
    rc = send_one(f);
    if (rc != SALTS_OK) return rc;
    rc = cnet_client_poll(&f->clients,1U,&events);
    if (rc != SALTS_OK) return rc;
    rc = cnet_client_poll(&f->server,1U,&events);
    if (rc != SALTS_OK) return rc;
    return f->callback_error;
}

static int status_is(quorum_fixture_t *f, tr_raft_index_t expected,
                     bool require_node_one)
{
    tr_raft_status_t leader = {0}, n3 = {0}, n1 = {0};
    if (tr_raft_core_status(f->nodes[1].core,&leader) != SALTS_OK ||
        tr_raft_core_status(f->nodes[2].core,&n3) != SALTS_OK ||
        tr_raft_core_status(f->nodes[0].core,&n1) != SALTS_OK)
        return 0;
    return leader.role == TR_RAFT_LEADER &&
           leader.leader_id == 2U &&
           leader.commit_index >= expected && n3.commit_index >= expected &&
           (!require_node_one || (n1.commit_index >= expected &&
                                  n1.applied_index >= expected &&
                                  n1.last_log_index >= expected));
}

/* Capture only bounded non-secret counters on platform-specific failures.
 * Every gate still fails closed: diagnostics do not trigger a retry or
 * change CNet/Owner admission. */
static void report_drive_state(quorum_fixture_t *f,
                               tr_raft_index_t target,
                               int include_one, unsigned attempts, int rc)
{
    tr_raft_status_t leader = {0}, one = {0}, three = {0};
    tr_raft_cnet_channel_status_t c1 = {0}, c3 = {0};
    (void)tr_raft_core_status(f->nodes[1].core, &leader);
    (void)tr_raft_core_status(f->nodes[0].core, &one);
    (void)tr_raft_core_status(f->nodes[2].core, &three);
    (void)tr_raft_cnet_channel_get_status(f->outbound[0], &c1);
    (void)tr_raft_cnet_channel_get_status(f->outbound[1], &c3);
    fprintf(stderr,
            "TLS majority progress target=%llu require_node1=%d "
            "attempts=%u result=%d leader_role=%d leader_term=%llu "
            "leader_index=%llu leader_commit=%llu "
            "node3_index=%llu node3_commit=%llu node1_index=%llu "
            "node1_commit=%llu wire=%zu/%zu pending=%zu votes=%zu "
            "append3=%zu ack3=%zu mute1=%zu peers=%d/%d "
            "peer_err=%d/%d\n",
            (unsigned long long)target,include_one,attempts,rc,
            (int)leader.role,(unsigned long long)leader.term,
            (unsigned long long)leader.last_log_index,
            (unsigned long long)leader.commit_index,
            (unsigned long long)three.last_log_index,
            (unsigned long long)three.commit_index,
            (unsigned long long)one.last_log_index,
            (unsigned long long)one.commit_index,
            f->wire_sent,f->wire_recv,f->pending_count,
            f->real_votes_to_leader,f->real_appends_to_three,
            f->real_append_acks_from_three,f->lost_node_one,
            (int)c1.phase,(int)c3.phase,c1.last_error,c3.last_error);
}

static int drive_until(quorum_fixture_t *f, tr_raft_index_t index,
                       int include_one)
{
    const uint64_t started_ms = cmeta_monotonic_ms();
    uint64_t next_heartbeat_ms = started_ms + UINT64_C(100);
    unsigned attempts = 0U;
    int rc;
    /* An IOCP poll iteration and a Kqueue/epoll poll iteration do NOT take
     * the same wall-clock duration. Logical Raft ticks must be driven by
     * elapsed monotonic time, not 120 arbitrary loop iterations.
     * 8-s wall-clock + 11000-poll bounds stay fail-closed on every OS. */
    for (attempts = 0U; attempts < MAX_PROGRESS; ++attempts) {
        uint64_t now_ms;
        rc = network_progress(f);
        if (rc != SALTS_OK) {
            report_drive_state(f,index,include_one,attempts,rc);
            return rc;
        }
        if (status_is(f,index,include_one)) return SALTS_OK;
        now_ms = cmeta_monotonic_ms();
        if (now_ms - started_ms >= UINT64_C(8000))
            break;
        if (now_ms >= next_heartbeat_ms) {
            rc = core_tick(f,1U,1U);
            if (rc != SALTS_OK) {
                report_drive_state(f,index,include_one,attempts,rc);
                return rc;
            }
            next_heartbeat_ms = now_ms + UINT64_C(100);
        }
    }
    report_drive_state(f,index,include_one,attempts,SALTS_ETIMEDOUT);
    return SALTS_ETIMEDOUT;
}

static int setup_core(quorum_fixture_t *f)
{
    size_t i;
    for (i = 0U; i < NODES; ++i) {
        tr_raft_core_config_t cfg = {0};
        cfg.self_id = (tr_raft_node_id_t)(i + 1U);
        cfg.voters = VOTERS;
        cfg.voter_count = NODES;
        cfg.heartbeat_ticks = 1U;
        cfg.election_min_ticks = 3U;
        cfg.election_max_ticks = 6U;
        cfg.initial_election_timeout_ticks = (uint32_t)(i + 3U);
        cfg.max_log_entries = MAX_LOG;
        {
            int rc = tr_raft_core_create(&cfg,&f->nodes[i].core);
            if (rc != SALTS_OK) return rc;
        }
    }
    return SALTS_OK;
}

static int setup_tls(quorum_fixture_t *f)
{
    static const char * const files[7] = {
        "ca.pem", "three-node-client-ca.pem",
        "node1-cert.pem", "node1-key.pem",
        "node2-client-cert.pem", "node2-client-key.pem",
        "node2-cert.pem"
    };
    char server_key[512], uri[128];
    cnet_tls_server_config tls = {0};
    cnet_listener_config listener = {0};
    cnet_client_config net = net_config();
    tr_raft_cnet_channel_config_t server_cfg = {0};
    uint16_t port = 0U;
    size_t i, events = 0U;
    unsigned round;
    int rc;
    for (i = 0U; i < 7U; ++i) {
        rc = fixture_path(f->cert_paths[i],sizeof(f->cert_paths[i]),files[i]);
        if (rc != SALTS_OK) return rc;
    }
    rc = fixture_path(server_key,sizeof(server_key),"node2-key.pem");
    if (rc != SALTS_OK) return rc;

    f->server_fingerprint[0][0] = NODE1_FINGERPRINT;
    f->server_fingerprint[1][0] = NODE3_FINGERPRINT;
    f->server_peers[0] =
        (tr_raft_cnet_peer_identity_t){1U,f->server_fingerprint[0],1U};
    f->server_peers[1] =
        (tr_raft_cnet_peer_identity_t){3U,f->server_fingerprint[1],1U};
    f->server_policy =
        (tr_raft_cnet_identity_policy_t){2U,f->server_peers,PEERS};
    rc = tr_raft_cnet_identity_policy_validate(&f->server_policy);
    if (rc != SALTS_OK) return rc;

    tls.size = sizeof(tls);
    tls.ca_file = f->cert_paths[1];
    tls.cert_file = f->cert_paths[6];
    tls.key_file = server_key;
    tls.client_auth = CNET_TLS_CLIENT_AUTH_REQUIRED;
    rc = cnet_tls_server_init(&f->server_tls,&tls);
    if (rc != SALTS_OK) return rc;
    f->tls_live = 1;
    rc = cnet_client_init(&f->clients,&net);
    if (rc != SALTS_OK) return rc;
    f->clients_live = 1;
    rc = cnet_client_init(&f->server,&net);
    if (rc != SALTS_OK) return rc;
    f->server_live = 1;
    listener.backend = net.backend;
    listener.host = "127.0.0.1";
    listener.port = 0U;
    listener.backlog = PEERS;
    rc = cnet_listener_init(&f->listener,&listener);
    if (rc != SALTS_OK) return rc;
    f->listener_live = 1;
    rc = cnet_listener_port(&f->listener,&port);
    if (rc != SALTS_OK) return rc;
    if (snprintf(uri,sizeof(uri),"tls://127.0.0.1:%u",
                 (unsigned)port) <= 0) return SALTS_EINVAL;

    server_cfg.client = &f->server;
    server_cfg.identity = &f->server_policy;
    server_cfg.handshake = hello(2U);
    server_cfg.first_outbound_message_id = 1U;
    server_cfg.host_module_generation = UINT64_C(123007);
    server_cfg.on_payload = server_receive;
    for (i = 0U; i < PEERS; ++i) {
        tr_raft_cnet_channel_config_t client_cfg = {0};
        cnet_connect_options connect = {0};
        f->client_server_fingerprint[i][0] = NODE2_FINGERPRINT;
        f->client_server_peer[i] =
            (tr_raft_cnet_peer_identity_t){
                2U,f->client_server_fingerprint[i],1U};
        f->client_policy[i] =
            (tr_raft_cnet_identity_policy_t){
                PEER_IDS[i],&f->client_server_peer[i],1U};
        f->client_tls[i].size = sizeof(f->client_tls[i]);
        f->client_tls[i].ca_file = f->cert_paths[0];
        f->client_tls[i].cert_file = f->cert_paths[i == 0U ? 2U : 4U];
        f->client_tls[i].key_file = f->cert_paths[i == 0U ? 3U : 5U];
        f->client_tls[i].server_name = "node-2.mesh";
        f->client_endpoint[i].fixture = f;
        f->client_endpoint[i].expected_node = PEER_IDS[i];
        client_cfg.client = &f->clients;
        client_cfg.identity = &f->client_policy[i];
        client_cfg.handshake = hello(PEER_IDS[i]);
        client_cfg.first_outbound_message_id = 1U;
        client_cfg.host_module_generation = UINT64_C(123007);
        client_cfg.on_payload = client_receive;
        client_cfg.payload_context = &f->client_endpoint[i];
        rc = tr_raft_cnet_channel_create(&client_cfg,&f->outbound[i]);
        if (rc != SALTS_OK) return rc;
        f->client_endpoint[i].channel = f->outbound[i];
        connect.uri = uri;
        connect.tls = &f->client_tls[i];
        connect.observer = tr_raft_cnet_channel_observer(f->outbound[i]);
        rc = cnet_connect(&f->clients,&connect,&f->outbound_connection[i]);
        if (rc != SALTS_OK) return rc;
        rc = tr_raft_cnet_channel_attach(
            f->outbound[i],f->outbound_connection[i]);
        if (rc != SALTS_OK) return rc;
    }

    for (round = 0U; round < MAX_PROGRESS; ++round) {
        rc = cnet_client_poll(&f->clients,1U,&events);
        if (rc != SALTS_OK) return rc;
        if (f->accepted < PEERS) {
            int ready = 0;
            rc = cnet_listener_wait(&f->listener,0U,&ready);
            if (rc != SALTS_OK) return rc;
            if (ready) {
                size_t position = (size_t)f->accepted;
                tr_raft_cnet_channel_config_t config = server_cfg;
                cnet_observer observer = {0};
                f->server_endpoint[position].fixture = f;
                config.payload_context = &f->server_endpoint[position];
                rc = tr_raft_cnet_channel_create(
                    &config,&f->inbound[position]);
                if (rc != SALTS_OK) return rc;
                f->server_endpoint[position].channel = f->inbound[position];
                observer = tr_raft_cnet_channel_observer(f->inbound[position]);
                rc = cnet_listener_accept_tls(
                    &f->listener,&f->server,&f->server_tls,
                    &observer,&f->inbound_connection[position]);
                if (rc != SALTS_OK) return rc;
                rc = tr_raft_cnet_channel_attach(
                    f->inbound[position],f->inbound_connection[position]);
                if (rc != SALTS_OK) return rc;
                ++f->accepted;
            }
        }
        rc = cnet_client_poll(&f->server,1U,&events);
        if (rc != SALTS_OK) return rc;
        if (f->accepted == PEERS) {
            size_t ok = 0U;
            for (i = 0U; i < PEERS; ++i) {
                tr_raft_cnet_channel_status_t a = {0}, b = {0};
                rc = tr_raft_cnet_channel_get_status(f->outbound[i],&a);
                if (rc != SALTS_OK) return rc;
                rc = tr_raft_cnet_channel_get_status(f->inbound[i],&b);
                if (rc != SALTS_OK) return rc;
                if (a.phase == TR_RAFT_CNET_CHANNEL_ACTIVE &&
                    b.phase == TR_RAFT_CNET_CHANNEL_ACTIVE) ++ok;
            }
            if (ok == PEERS) return SALTS_OK;
        }
    }
    return SALTS_ETIMEDOUT;
}


#ifdef TURBORAFT_TEST_CERTIFIED_QUORUM_PHYSICAL_LOSS
/* Close the ACTUAL certified CNet Node3 connection, not only Core.step.
 * Do not reuse a connection, observer or pending send-credit ledger before
 * both physical owners report terminal and exact settlement. The listener,
 * TLS config, authenticated identity policy and all three Cores remain. */
static int physical_disconnect_node_three(quorum_fixture_t *f)
{
    tr_raft_cnet_channel_status_t in = {0}, out = {0};
    size_t socket = PEERS, i, events = 0U;
    uint64_t started_ms = cmeta_monotonic_ms();
    unsigned polls;
    int rc;
#define CHECK_PHYSICAL(x) do { rc=(x); if(rc!=SALTS_OK) return rc; } while(0)
    for (i = 0U; i < PEERS; ++i) {
        CHECK_PHYSICAL(tr_raft_cnet_channel_get_status(f->inbound[i],&in));
        if (in.authenticated_peer_node_id == 3U &&
            in.phase == TR_RAFT_CNET_CHANNEL_ACTIVE) {
            socket = i;
            break;
        }
    }
    if (socket == PEERS || f->outbound[1] == NULL) return SALTS_EPROTO;
    f->node_three_socket_slot = (unsigned)socket;
    CHECK_PHYSICAL(tr_raft_cnet_channel_capture_reply_origin(
        f->inbound[socket],77U,&f->node_three_old_origin));
    if (f->node_three_old_origin.authenticated_peer_node_id != 3U ||
        f->node_three_old_origin.group_id != 77U ||
        f->node_three_old_origin.connection_token == 0U)
        return SALTS_EPROTO;
    f->node_three_link_down = 1;
    CHECK_PHYSICAL(tr_raft_cnet_channel_stop(f->inbound[socket]));
    for (polls = 0U; polls < MAX_PROGRESS &&
         cmeta_monotonic_ms() - started_ms < UINT64_C(6000); ++polls) {
        CHECK_PHYSICAL(cnet_client_poll(&f->server,1U,&events));
        CHECK_PHYSICAL(cnet_client_poll(&f->clients,1U,&events));
        CHECK_PHYSICAL(tr_raft_cnet_channel_get_status(f->inbound[socket],&in));
        CHECK_PHYSICAL(tr_raft_cnet_channel_get_status(f->outbound[1],&out));
        if (in.terminal && out.terminal) break;
    }
    if (polls == MAX_PROGRESS || !in.terminal || !out.terminal ||
        in.payload_writes_pending != 0U ||
        out.payload_writes_pending != 0U ||
        in.payloads_admitted != in.payloads_completed + in.payloads_canceled ||
        out.payloads_admitted != out.payloads_completed + out.payloads_canceled)
        return SALTS_EPROTO;
    CHECK_PHYSICAL(tr_raft_cnet_channel_destroy(f->inbound[socket]));
    f->inbound[socket] = NULL;
    f->server_endpoint[socket].channel = NULL;
    CHECK_PHYSICAL(tr_raft_cnet_channel_destroy(f->outbound[1]));
    f->outbound[1] = NULL;
    f->client_endpoint[1].channel = NULL;
#undef CHECK_PHYSICAL
    return SALTS_OK;
}

/* Explicit caller-driven physical N+1 TLS reconnect; preserve the single
 * listener, CNet owners and exact certified identity. No network replay or
 * settlement retry is attempted, and no CNet->FlowMQ fallback exists. */
static int physical_reconnect_node_three(quorum_fixture_t *f)
{
    const size_t socket = (size_t)f->node_three_socket_slot;
    tr_raft_cnet_channel_config_t client_cfg = {0}, server_cfg = {0};
    tr_raft_cnet_channel_status_t in = {0}, out = {0};
    cnet_connect_options connect = {0};
    cnet_observer observer = {0};
    uint16_t port = 0U;
    char uri[128];
    uint64_t started_ms = cmeta_monotonic_ms();
    unsigned round;
    size_t events = 0U;
    int accepted = 0, rc;
#define CHECK_PHYSICAL(x) do { rc=(x); if(rc!=SALTS_OK) return rc; } while(0)
    if (!f->node_three_link_down || socket >= PEERS ||
        f->inbound[socket] != NULL || f->outbound[1] != NULL)
        return SALTS_EPROTO;
    CHECK_PHYSICAL(cnet_listener_port(&f->listener,&port));
    if (snprintf(uri,sizeof(uri),"tls://127.0.0.1:%u",
                 (unsigned)port) <= 0) return SALTS_EINVAL;
    client_cfg.client = &f->clients;
    client_cfg.identity = &f->client_policy[1];
    client_cfg.handshake = hello(3U);
    client_cfg.first_outbound_message_id = 1U;
    client_cfg.host_module_generation = UINT64_C(123007);
    client_cfg.on_payload = client_receive;
    client_cfg.payload_context = &f->client_endpoint[1];
    CHECK_PHYSICAL(tr_raft_cnet_channel_create(
        &client_cfg,&f->outbound[1]));
    f->client_endpoint[1].channel = f->outbound[1];
    connect.uri = uri;
    connect.tls = &f->client_tls[1];
    connect.observer = tr_raft_cnet_channel_observer(f->outbound[1]);
    CHECK_PHYSICAL(cnet_connect(&f->clients,&connect,
                                &f->outbound_connection[1]));
    CHECK_PHYSICAL(tr_raft_cnet_channel_attach(f->outbound[1],
                                               f->outbound_connection[1]));
    server_cfg.client = &f->server;
    server_cfg.identity = &f->server_policy;
    server_cfg.handshake = hello(2U);
    server_cfg.first_outbound_message_id = 1U;
    server_cfg.host_module_generation = UINT64_C(123007);
    server_cfg.on_payload = server_receive;
    server_cfg.payload_context = &f->server_endpoint[socket];
    for (round = 0U; round < MAX_PROGRESS &&
         cmeta_monotonic_ms() - started_ms < UINT64_C(8000); ++round) {
        CHECK_PHYSICAL(cnet_client_poll(&f->clients,1U,&events));
        if (!accepted) {
            int ready = 0;
            CHECK_PHYSICAL(cnet_listener_wait(&f->listener,0U,&ready));
            if (ready) {
                CHECK_PHYSICAL(tr_raft_cnet_channel_create(
                    &server_cfg,&f->inbound[socket]));
                f->server_endpoint[socket].channel = f->inbound[socket];
                observer = tr_raft_cnet_channel_observer(f->inbound[socket]);
                CHECK_PHYSICAL(cnet_listener_accept_tls(
                    &f->listener,&f->server,&f->server_tls,
                    &observer,&f->inbound_connection[socket]));
                CHECK_PHYSICAL(tr_raft_cnet_channel_attach(
                    f->inbound[socket],f->inbound_connection[socket]));
                accepted = 1;
            }
        }
        CHECK_PHYSICAL(cnet_client_poll(&f->server,1U,&events));
        if (accepted) {
            CHECK_PHYSICAL(tr_raft_cnet_channel_get_status(
                f->inbound[socket],&in));
            CHECK_PHYSICAL(tr_raft_cnet_channel_get_status(
                f->outbound[1],&out));
            if (in.phase == TR_RAFT_CNET_CHANNEL_ACTIVE &&
                out.phase == TR_RAFT_CNET_CHANNEL_ACTIVE) break;
        }
    }
    if (round == MAX_PROGRESS || !accepted ||
        in.phase != TR_RAFT_CNET_CHANNEL_ACTIVE ||
        out.phase != TR_RAFT_CNET_CHANNEL_ACTIVE ||
        in.authenticated_peer_node_id != 3U ||
        out.authenticated_peer_node_id != 2U ||
        in.payloads_admitted != 0U || out.payloads_admitted != 0U)
        return SALTS_EPROTO;
    CHECK_PHYSICAL(tr_raft_cnet_channel_capture_reply_origin(
        f->inbound[socket],77U,&f->node_three_new_origin));
    if (f->node_three_old_origin.host_module_generation !=
            f->node_three_new_origin.host_module_generation ||
        f->node_three_new_origin.authenticated_peer_node_id != 3U ||
        f->node_three_new_origin.group_id != 77U ||
        f->node_three_old_origin.channel_instance ==
            f->node_three_new_origin.channel_instance ||
        f->node_three_old_origin.connection_token ==
            f->node_three_new_origin.connection_token)
        return SALTS_EPROTO;
    f->node_three_link_down = 0;
    ++f->node_three_reconnected;
#undef CHECK_PHYSICAL
    return SALTS_OK;
}
#endif

static int cleanup(quorum_fixture_t *f)
{
    size_t i;
    int result, first = SALTS_OK;
#define STEP(x) do { result=(x); if(first==SALTS_OK && result!=SALTS_OK) \
    first=result; } while(0)
    for (i = 0U; i < PEERS; ++i) {
        if (f->outbound[i] != NULL)
            (void)tr_raft_cnet_channel_stop(f->outbound[i]);
        if (f->inbound[i] != NULL)
            (void)tr_raft_cnet_channel_stop(f->inbound[i]);
    }
    if (f->clients_live) {
        result = cnet_client_stop(&f->clients,2000U);
        if (result != SALTS_OK && result != SALTS_EALREADY) return result;
    }
    if (f->server_live) {
        result = cnet_client_stop(&f->server,2000U);
        if (result != SALTS_OK && result != SALTS_EALREADY) return result;
    }
    for (i = 0U; i < PEERS; ++i) {
        tr_raft_cnet_channel_status_t status = {0};
        if (f->outbound[i] != NULL) {
            STEP(tr_raft_cnet_channel_get_status(f->outbound[i],&status));
            if (!status.terminal ||
                status.payload_writes_pending != 0U ||
                status.payloads_admitted !=
                    status.payloads_completed + status.payloads_canceled)
                return SALTS_EPROTO;
            STEP(tr_raft_cnet_channel_destroy(f->outbound[i]));
        }
        if (f->inbound[i] != NULL) {
            status = (tr_raft_cnet_channel_status_t){0};
            STEP(tr_raft_cnet_channel_get_status(f->inbound[i],&status));
            if (!status.terminal ||
                status.payload_writes_pending != 0U ||
                status.payloads_admitted !=
                    status.payloads_completed + status.payloads_canceled)
                return SALTS_EPROTO;
            STEP(tr_raft_cnet_channel_destroy(f->inbound[i]));
        }
    }
    if (f->clients_live) STEP(cnet_client_destroy(&f->clients));
    if (f->server_live) STEP(cnet_client_destroy(&f->server));
    if (f->listener_live) {
        STEP(cnet_listener_close(&f->listener));
        STEP(cnet_listener_destroy(&f->listener));
    }
    if (f->tls_live) STEP(cnet_tls_server_destroy(&f->server_tls));
    for (i = 0U; i < NODES; ++i)
        tr_raft_core_destroy(f->nodes[i].core);
#undef STEP
    return first;
}

static int run_quorum(void)
{
    quorum_fixture_t *f = (quorum_fixture_t *)calloc(1U,sizeof(*f));
    tr_raft_status_t node2 = {0}, node1 = {0}, node3 = {0};
    size_t i;
    int rc = SALTS_OK, clean;
#define TRY(x) do { rc=(x); if(rc!=SALTS_OK) { \
    fprintf(stderr, "CNet Core majority failed %s rc=%d\n",#x,rc); \
    goto done; } } while(0)
#define REQUIRE(x,why) do { if(!(x)) { \
    fprintf(stderr, "CNet Core majority invariant: %s\n",why); \
    rc=SALTS_EPROTO; goto done; } } while(0)
    if (f == NULL) return SALTS_ENOMEM;
    TRY(setup_core(f));
    TRY(setup_tls(f));
    f->node_one_muted = 1;
    /* Real Node3 vote grants Node2 a 2-of-3 quorum while Node1's received
     * Raft packets are intentionally dropped by its test callback. */
    TRY(core_tick(f,1U,4U));
    TRY(drive_until(f,0U,0));
    TRY(tr_raft_core_status(f->nodes[1].core,&node2));
    REQUIRE(node2.role == TR_RAFT_LEADER && node2.leader_id == 2U &&
            f->real_votes_to_leader != 0U && f->lost_node_one != 0U,
            "candidate 2 elected through real Node3 TLS vote");
    for (i = 1U; i <= 3U; ++i) {
        char command[16];
        (void)snprintf(command,sizeof(command),"command-%zu",i);
        TRY(core_propose(f,(uint64_t)i,command));
        TRY(drive_until(f,(tr_raft_index_t)i,0));
        REQUIRE(f->nodes[1].durable.committed == i &&
                f->nodes[2].durable.committed == i &&
                f->nodes[0].durable.committed == 0U,
                "real Node2+Node3 Append ACK quorum while Node1 silent");
    }
#ifdef TURBORAFT_TEST_CERTIFIED_QUORUM_LOSS
    {
        uint64_t start_ms = cmeta_monotonic_ms();
        size_t acknowledgements_before =
            f->real_append_acks_from_three;
        unsigned attempts = 0U;
#ifdef TURBORAFT_TEST_CERTIFIED_QUORUM_TICKED_LOSS
        const tr_raft_term_t term_before = node2.term;
        const size_t votes_before = f->real_votes_to_leader;
#endif
        /* Both Node1 and Node3 are REAL authenticated CNet peers, but their
         * TEST callbacks decline to hand packets to Core. A successful
         * local append is NOT a committed quorum entry, even if its bytes
         * traversed a certified TLS connection. */
#ifdef TURBORAFT_TEST_CERTIFIED_QUORUM_PHYSICAL_LOSS
        TRY(physical_disconnect_node_three(f));
        /* Close can settle an older index3 ACK; only count NEW responses
         * during the actual minority interval and N+1 recovery. */
        acknowledgements_before = f->real_append_acks_from_three;
        start_ms = cmeta_monotonic_ms();
#else
        f->node_three_muted = 1;
#endif
        TRY(core_propose(f,4U,"not-yet-majority-committed"));
#ifdef TURBORAFT_TEST_CERTIFIED_QUORUM_TICKED_LOSS
        {
            uint64_t next_tick_ms = start_ms + UINT64_C(100);
            /* Network polling alone is insufficient: sustain the genuine
             * minority through EIGHT time-driven Raft ticks, including
             * required CheckQuorum demotion and subsequent pre-votes.
             * Assert after every I/O progress step, not only at the end.
             * Timed tick budget is wall-clock bounded on epoll/IOCP/Kqueue;
             * it is never based on an arbitrary count of socket polls. */
            while (attempts++ < MAX_PROGRESS &&
                   f->quorum_loss_ticks < 8U &&
                   cmeta_monotonic_ms() - start_ms < UINT64_C(4500)) {
                uint64_t now_ms;
                TRY(network_progress(f));
                now_ms = cmeta_monotonic_ms();
                if (now_ms >= next_tick_ms) {
                    TRY(core_tick(f,1U,1U));
                    ++f->quorum_loss_ticks;
                    next_tick_ms = now_ms + UINT64_C(100);
                }
                TRY(tr_raft_core_status(f->nodes[1].core,&node2));
                /* CheckQuorum is REQUIRED to demote a minority leader.
                 * A demotion must never mutate a committed/applied index. */
                if (node2.role != TR_RAFT_LEADER)
                    f->saw_check_quorum_demotion = 1;
                REQUIRE(node2.last_log_index == 4U &&
                        node2.commit_index == 3U &&
                        node2.applied_index == 3U &&
                        f->nodes[1].durable.length == 4U &&
                        f->nodes[1].durable.committed == 3U &&
                        f->nodes[1].durable.applied == 3U &&
                        f->nodes[2].durable.committed == 3U &&
                        f->nodes[0].durable.committed == 0U &&
                        f->real_append_acks_from_three ==
                            acknowledgements_before,
                        "isolated Core must not commit/apply through CheckQuorum");
            }
            REQUIRE(f->quorum_loss_ticks == 8U &&
                    f->lost_append_three > 0U &&
                    f->saw_check_quorum_demotion,
                    "ticked minority must trigger fail-closed CheckQuorum demotion");
        }
#else
        while (attempts++ < 2000U &&
               cmeta_monotonic_ms() - start_ms < UINT64_C(400)) {
            TRY(network_progress(f));
        }
#endif
        TRY(tr_raft_core_status(f->nodes[1].core,&node2));
        TRY(tr_raft_core_status(f->nodes[2].core,&node3));
        REQUIRE(node2.last_log_index == 4U &&
                node2.commit_index == 3U && node2.applied_index == 3U &&
                f->nodes[1].durable.length == 4U &&
                f->nodes[1].durable.committed == 3U &&
                f->nodes[2].durable.committed == 3U &&
                f->nodes[0].durable.committed == 0U &&
                f->lost_append_three > 0U &&
                f->real_append_acks_from_three == acknowledgements_before,
                "two silent certified peers cannot commit fourth Raft entry");
#ifdef TURBORAFT_TEST_CERTIFIED_QUORUM_PHYSICAL_LOSS
        REQUIRE(f->node_three_link_down &&
                f->offline_raft_outputs > 0U,
                "isolated real CNet socket must reject fresh Raft outputs");
        TRY(physical_reconnect_node_three(f));
        REQUIRE(f->node_three_reconnected == 1U &&
                f->node_three_old_origin.connection_token !=
                    f->node_three_new_origin.connection_token,
                "rejoined Node3 must use new certified TLS generation");
#else
        f->node_three_muted = 0;
#endif
#ifdef TURBORAFT_TEST_CERTIFIED_QUORUM_TICKED_LOSS
        /* CheckQuorum already demoted Node2. Recover by NEW authentic TLS
         * votes and a higher term. Never directly commit the index4 entry
         * from the PREVIOUS term: Raft only advances an old term once a
         * CURRENT-term entry also reaches a majority. No host send retry. */
        TRY(drive_until(f,3U,0));
        TRY(tr_raft_core_status(f->nodes[1].core,&node2));
        REQUIRE(node2.role == TR_RAFT_LEADER &&
                node2.term > term_before &&
                node2.last_log_index == 4U &&
                node2.commit_index == 3U &&
                node2.applied_index == 3U &&
                f->nodes[1].durable.committed == 3U &&
                f->real_votes_to_leader > votes_before,
                "fresh TLS re-election must not directly commit old-term index4");
        TRY(core_propose(f,5U,"new-term-quorum-barrier"));
        REQUIRE(f->nodes[1].durable.length == 5U &&
                f->nodes[1].durable.entries[3].term == term_before &&
                f->nodes[1].durable.entries[4].term == node2.term,
                "commit barrier must follow old-term index4 at current term");
        TRY(drive_until(f,5U,0));
        REQUIRE(f->nodes[1].durable.committed == 5U &&
                f->nodes[1].durable.applied == 5U &&
                f->nodes[2].durable.committed == 5U &&
                f->nodes[0].durable.committed == 0U &&
                f->real_append_acks_from_three > acknowledgements_before,
                "new-term majority ACK commits current index5 and old index4");
#else
        /* No CheckQuorum ticks in the shorter minority fixture: an in-term
         * Raft retry can obtain Node3's new certified Append ACK for index4. */
        TRY(core_tick(f,1U,1U));
        TRY(drive_until(f,4U,0));
        REQUIRE(f->nodes[1].durable.committed == 4U &&
                f->nodes[2].durable.committed == 4U &&
                f->nodes[0].durable.committed == 0U &&
                f->real_append_acks_from_three > acknowledgements_before,
                "Node3 restored real-TLS ACK advances exact 2/3 commit");
#endif
    }
#endif
    /* Destroy and reconstruct lagging independent Core1 from its persisted
     * (in-memory TEST) Ready values, while certified TLS links remain ACTIVE. */
    tr_raft_core_destroy(f->nodes[0].core);
    f->nodes[0].core = NULL;
    {
        tr_raft_core_config_t cfg = {0};
        cfg.self_id = 1U;
        cfg.voters = VOTERS;
        cfg.voter_count = NODES;
        cfg.heartbeat_ticks = 1U;
        cfg.election_min_ticks = 3U;
        cfg.election_max_ticks = 6U;
        cfg.initial_election_timeout_ticks = 3U;
        cfg.initial_term = f->nodes[0].durable.term;
        cfg.initial_vote = f->nodes[0].durable.vote;
        cfg.initial_log_entries = f->nodes[0].durable.length
                                  ? f->nodes[0].durable.entries : NULL;
        cfg.initial_log_entry_count = f->nodes[0].durable.length;
        cfg.initial_commit_index = f->nodes[0].durable.committed;
        cfg.initial_applied_index = f->nodes[0].durable.applied;
        cfg.max_log_entries = MAX_LOG;
        TRY(tr_raft_core_create(&cfg,&f->nodes[0].core));
    }
    f->node_one_muted = 0;
    {
#ifdef TURBORAFT_TEST_CERTIFIED_QUORUM_TICKED_LOSS
        const tr_raft_index_t target_commit = 5U;
#elif defined(TURBORAFT_TEST_CERTIFIED_QUORUM_LOSS)
        const tr_raft_index_t target_commit = 4U;
#else
        const tr_raft_index_t target_commit = 3U;
#endif
        TRY(drive_until(f,target_commit,1));
    }
    for (i = 0U; i < NODES; ++i) {
        const durable_values_t *d = &f->nodes[i].durable;
        size_t entry;
        tr_raft_status_t status = {0};
        TRY(tr_raft_core_status(f->nodes[i].core,&status));
#ifdef TURBORAFT_TEST_CERTIFIED_QUORUM_TICKED_LOSS
        const tr_raft_index_t expected_final = 5U;
#elif defined(TURBORAFT_TEST_CERTIFIED_QUORUM_LOSS)
        const tr_raft_index_t expected_final = 4U;
#else
        const tr_raft_index_t expected_final = 3U;
#endif
        REQUIRE(d->committed == expected_final &&
                d->applied == expected_final &&
                d->length == expected_final &&
                status.commit_index == expected_final &&
                status.applied_index == expected_final &&
                status.last_log_index == expected_final &&
                status.leader_id == 2U,
                "all three Cores converged exact committed/applied indexes");
        for (entry = 0U; entry < (size_t)expected_final; ++entry) {
            const tr_raft_entry_t *actual = &d->entries[entry];
            const tr_raft_entry_t *leader = &f->nodes[1].durable.entries[entry];
            REQUIRE(actual->index == leader->index &&
                    actual->term == leader->term &&
                    actual->command_id == leader->command_id &&
                    actual->data_length == leader->data_length &&
                    memcmp(actual->data,leader->data,actual->data_length) == 0,
                    "three durable Ready logs match byte for byte");
        }
    }
    TRY(tr_raft_core_status(f->nodes[0].core,&node1));
    TRY(tr_raft_core_status(f->nodes[2].core,&node3));
    REQUIRE(node2.role == TR_RAFT_LEADER &&
            f->real_appends_to_three >= 3U &&
            f->real_append_acks_from_three >= 3U &&
            f->caught_up_node_one != 0U &&
            f->pending_count < PENDING_CAPACITY,
            "real TLS AppendixEntries and acknowledgements traversed each peer");
    printf("certified_tls_raft_majority,wire_sent=%zu,wire_received=%zu,"
           "node3_votes=%zu,node3_appends=%zu,node3_append_acks=%zu,"
           "node1_muted=%zu,node1_rejoin_append=%zu,"
#ifdef TURBORAFT_TEST_CERTIFIED_QUORUM_LOSS
           "node3_muted=%zu,node3_append_misses=%zu,"
           "isolated_raft_ticks=%u,check_quorum_demotion=%d,"
#ifdef TURBORAFT_TEST_CERTIFIED_QUORUM_TICKED_LOSS
           "committed=5,applied=5\n",
#else
           "committed=4,applied=4\n",
#endif
#else
           "committed=3,applied=3\n",
#endif
           f->wire_sent,f->wire_recv,f->real_votes_to_leader,
           f->real_appends_to_three,f->real_append_acks_from_three,
           f->lost_node_one,f->caught_up_node_one
#ifdef TURBORAFT_TEST_CERTIFIED_QUORUM_LOSS
           , f->lost_node_three,f->lost_append_three,
           f->quorum_loss_ticks,f->saw_check_quorum_demotion
#endif
           );
done:
    clean = cleanup(f);
    if (rc == SALTS_OK && clean != SALTS_OK) rc = clean;
    free(f);
#undef TRY
#undef REQUIRE
    return rc;
}

int main(void)
{
    return run_quorum() == SALTS_OK ? 0 : 1;
}
