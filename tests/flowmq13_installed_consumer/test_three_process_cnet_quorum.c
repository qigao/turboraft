#define _POSIX_C_SOURCE 200809L

#include <turboraft/raft_core.h>
#include <turboraft/raft_cnet_channel.h>
#include <turboraft/raft_wal_storage.h>
#include <cmeta_error.h>
#include <salts/clock.h>

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

/*
 * #149 P1: three simultaneously running, independently exec'ed Core/WAL
 * processes. All Vote/Append/ACK frames use actual certified CNet TLS
 * and reciprocal Raft HELLO between Node2 and separate Node1/Node3 owners.
 *
 * Parent IPC transports ONLY scalar start/stop/propose/status barriers;
 * never a Raft message, log entry, TLS key/handle or callback/owner state.
 * Node1's TEST Core callback is deliberately muted while its verified
 * TLS Channel stays active; certified Node2+Node3 must commit indices1..3
 * to their OWN fsynced WAL writers. Each WAL is reopened independently
 * by the controller after all workers release their exclusive locks.
 *
 * This is NOT yet real partition recovery, SIGKILL of an authentic Raft
 * proposal, or full three-process distributed catch-up (#149 subsequent).
 */
enum {
    NODES = 3, LINKS = 2, MAX_LOG = 32, READY_MESSAGES = TR_RAFT_MAX_MEMBERS * 2,
    PENDING_CAP = 64, IPC_TIMEOUT_MS = 10000, TOTAL_TIMEOUT_MS = 14000
};
#define IPC_MAGIC UINT32_C(0x54525133)
#define NODE1_FP "eb571a92b33237897216c79501066b3e77391047eaeb6be084526c4769657549"
#define NODE3_FP "d696b3ab8d0596e3e8e8abcecc4ca87856eda0f5055f31a7dfb6c213c2c2b1f3"
#define NODE2_FP "44e8fe3ce37ede1c2a1b5d36efa345cb662887d4250d17a000ebea2e834aed95"
static const tr_raft_node_id_t VOTERS[NODES] = {1U,2U,3U};

typedef struct control_command {
    uint32_t op;
    uint32_t unused;
    uint64_t arg;
} control_command_t;

typedef struct control_status {
    uint32_t magic;
    uint32_t op;
    uint64_t id;
    uint64_t pid;
    uint64_t port;
    uint64_t role;
    uint64_t term;
    uint64_t vote;
    uint64_t last;
    uint64_t committed;
    uint64_t applied;
    uint64_t wal_term;
    uint64_t wal_vote;
    uint64_t wal_last;
    uint64_t wal_commit;
    uint64_t wal_hash;
    uint64_t active_tls;
    uint64_t wire_sent;
    uint64_t wire_received;
    uint64_t certified_votes_from_3;
    uint64_t certified_append_acks_from_3;
    uint64_t node1_ignored;
    int64_t error;
} control_status_t;

typedef struct child {
    pid_t pid;
    int control;
    char wal_prefix[512];
} child_t;

struct node_worker;
typedef struct peer_context {
    struct node_worker *worker;
    tr_raft_cnet_channel_t *channel;
    tr_raft_node_id_t expected_remote;
} peer_context_t;

typedef struct node_worker {
    unsigned id;
    int control;
    int error;
    uint16_t port;
    int net_live, listener_live, tls_live, campaign_started;
    uint64_t next_tick_ms;
    uint64_t applied;
    uint64_t wire_sent, wire_received;
    uint64_t votes_from_3, append_acks_from_3, node1_ignored;
    tr_raft_core_t *core;
    tr_raft_wal_storage_t *wal;
    tr_raft_storage_t storage;
    tr_raft_message_t pending[PENDING_CAP];
    size_t pending_count;
    cnet_client net;
    cnet_listener listener;
    cnet_tls_server server_tls;
    cnet_tls_client_config client_tls;
    cnet_connection connections[LINKS];
    tr_raft_cnet_channel_t *channels[LINKS];
    peer_context_t peers[LINKS];
    size_t accepted;
    tr_raft_cnet_identity_policy_t policy;
    tr_raft_cnet_peer_identity_t allowed[LINKS];
    const char *fingerprints[LINKS][1];
    char tls_paths[8][512];
} node_worker_t;

static void child_kill(child_t *child)
{
    if (child->pid > 0) {
        int status;
        (void)kill(child->pid,SIGKILL);
        while (waitpid(child->pid,&status,0) < 0 && errno == EINTR) {}
        child->pid = -1;
    }
    if (child->control >= 0) {
        (void)close(child->control);
        child->control = -1;
    }
}

static int fd_message(int fd, const void *data, size_t size)
{
    ssize_t written;
    do { written = send(fd,data,size,MSG_NOSIGNAL); }
    while (written < 0 && errno == EINTR);
    return written == (ssize_t)size ? 0 : -1;
}

static int fd_receive(int fd, void *data, size_t size)
{
    struct pollfd wait_on = {fd,POLLIN,0};
    int ready;
    ssize_t received;
    do { ready = poll(&wait_on,1,IPC_TIMEOUT_MS); }
    while (ready < 0 && errno == EINTR);
    if (ready != 1 || !(wait_on.revents & POLLIN)) return -1;
    do { received = recv(fd,data,size,0); }
    while (received < 0 && errno == EINTR);
    return received == (ssize_t)size ? 0 : -1;
}

static tr_raft_wal_storage_config_t wal_config(const char *prefix, int create)
{
    tr_raft_wal_storage_config_t cfg = {0};
    cfg.path_prefix = prefix;
    cfg.segment_bytes = TR_RAFT_WAL_MIN_SEGMENT_BYTES;
    cfg.max_transaction_bytes = 8U * 1024U;
    cfg.max_live_segments = 4U;
    cfg.max_log_entries = MAX_LOG;
    cfg.max_snapshot_bytes = 1024U;
    cfg.create_if_missing = create != 0;
    return cfg;
}

static uint64_t hash_u64(uint64_t hash, uint64_t value)
{
    size_t j;
    for (j=0U;j<8U;++j) {
        hash ^= value & UINT64_C(255);
        hash *= UINT64_C(1099511628211);
        value >>= 8U;
    }
    return hash;
}

static uint64_t entries_hash(const tr_raft_wal_recovery_t *recovered)
{
    uint64_t h = UINT64_C(14695981039346656037);
    size_t i,j;
    for (i=0U;i<recovered->entry_count;++i) {
        const tr_raft_entry_t *e = &recovered->entries[i];
        h = hash_u64(h,e->index);
        h = hash_u64(h,e->term);
        h = hash_u64(h,e->command_id);
        h = hash_u64(h,(uint64_t)e->data_length);
        for (j=0U;j<e->data_length;++j) {
            h ^= e->data[j];
            h *= UINT64_C(1099511628211);
        }
    }
    return h;
}

static tr_raft_handshake_config_t handshake(unsigned self)
{
    tr_raft_handshake_config_t c = {0};
    size_t i;
    for (i=0U;i<sizeof(c.cluster_id.bytes);++i)
        c.cluster_id.bytes[i]=(uint8_t)(i+17U);
    c.local_node_id=self;
    c.process_incarnation.bytes[0]=(uint8_t)self;
    c.config_epoch=1U;
    c.wire_major_min=c.wire_major_max=TR_RAFT_HANDSHAKE_WIRE_MAJOR;
    c.wire_minor_min=c.wire_minor_max=TR_RAFT_HANDSHAKE_WIRE_MINOR;
    c.max_frame_size=TR_RAFT_WIRE_MAX_FRAME_SIZE;
    c.max_snapshot_chunk_size=TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES;
    return c;
}

static cnet_client_config network_config(void)
{
    cnet_client_config cfg = {0};
    cfg.backend=NATIVE_IO_BACKEND_EPOLL;
    cfg.connection_capacity=LINKS;
    cfg.command_capacity=32U;
    cfg.request_capacity=32U;
    cfg.completion_batch_capacity=16U;
    cfg.event_capacity=32U;
    cfg.max_send_bytes=4096U;
    cfg.receive_buffer_bytes=4096U;
    cfg.connect_timeout_ms=2000U;
    cfg.read_timeout_ms=15000U;
    cfg.write_timeout_ms=5000U;
    cfg.tls_io_buffer_bytes=CNET_TLS_MIN_IO_BUFFER_BYTES;
    cfg.tls_handshake_timeout_ms=2000U;
    return cfg;
}

static int fixture_path(char *out, size_t cap, const char *name)
{
    int n=snprintf(out,cap,"%s/%s",TURBORAFT_ACE23_FIXTURE_DIR,name);
    return n>0 && (size_t)n<cap ? SALTS_OK : SALTS_ERANGE;
}

static int persist_ready(node_worker_t *w, const tr_raft_ready_t *ready)
{
    size_t i;
    int rc;
    int needs_storage=ready->hard_state_changed || ready->log_changed ||
                      ready->commit_changed || ready->log_entry_count!=0U;
    int has_effect=needs_storage || ready->message_count!=0U ||
                   ready->role_changed || ready->committed_entry_count!=0U ||
                   ready->read_state_ready;
    if (ready->message_count>PENDING_CAP-w->pending_count)
        return SALTS_ENOSPC;
    if (needs_storage) {
        rc=w->storage.begin(w->storage.context);
        if (rc!=SALTS_OK) return rc;
        if (ready->hard_state_changed)
            rc=w->storage.write_hard_state(
                w->storage.context,ready->term,ready->voted_for);
        if (rc==SALTS_OK && ready->log_changed &&
            ready->log_truncate_from!=0U)
            rc=w->storage.truncate_log(
                w->storage.context,ready->log_truncate_from);
        if (rc==SALTS_OK && ready->log_entry_count!=0U)
            rc=w->storage.append_log(
                w->storage.context,ready->log_entries,ready->log_entry_count);
        if (rc==SALTS_OK && ready->commit_changed)
            rc=w->storage.write_commit_index(
                w->storage.context,ready->commit_index);
        if (rc==SALTS_OK) rc=w->storage.commit(w->storage.context);
        else (void)w->storage.rollback(w->storage.context);
        if (rc!=SALTS_OK) return rc;
    }
    for (i=0U;i<ready->message_count;++i)
        w->pending[w->pending_count++]=ready->messages[i];
    if (ready->committed_entry_count!=0U)
        w->applied=ready->committed_entries[
            ready->committed_entry_count-1U].index;
    return has_effect ? tr_raft_core_advance(w->core) : SALTS_OK;
}

static tr_raft_ready_t new_ready(tr_raft_message_t *out)
{
    tr_raft_ready_t r = {0};
    r.messages=out;
    r.message_capacity=READY_MESSAGES;
    return r;
}

static int core_step(node_worker_t *w, const tr_raft_message_t *message)
{
    tr_raft_message_t buffer[READY_MESSAGES];
    tr_raft_ready_t ready=new_ready(buffer);
    int rc=tr_raft_core_step(w->core,message,&ready);
    return rc==SALTS_OK?persist_ready(w,&ready):rc;
}

static int core_tick(node_worker_t *w, uint32_t ticks)
{
    tr_raft_message_t buffer[READY_MESSAGES];
    tr_raft_ready_t ready=new_ready(buffer);
    tr_raft_tick_t tick={ticks,4U};
    int rc=tr_raft_core_tick(w->core,&tick,&ready);
    return rc==SALTS_OK?persist_ready(w,&ready):rc;
}

static int core_propose(node_worker_t *w, unsigned index)
{
    char command[32];
    tr_raft_message_t buffer[READY_MESSAGES];
    tr_raft_ready_t ready=new_ready(buffer);
    tr_raft_proposal_t proposal={0};
    int rc;
    if (snprintf(command,sizeof(command),"command-%u",index)<=0)
        return SALTS_EINVAL;
    proposal.command_id=index;
    proposal.data=command;
    proposal.data_length=strlen(command);
    rc=tr_raft_core_propose(w->core,&proposal,&ready);
    return rc==SALTS_OK?persist_ready(w,&ready):rc;
}

static int on_payload(void *context,const tr_raft_transport_payload_t *payload)
{
    peer_context_t *peer=(peer_context_t *)context;
    node_worker_t *w=peer->worker;
    tr_raft_cnet_channel_status_t channel={0};
    const tr_raft_message_t *m;
    int rc;
    if (payload==NULL || payload->group_id!=77U ||
        payload->kind!=TR_RAFT_WIRE_PAYLOAD_RAFT)
        return SALTS_EPROTO;
    m=&payload->data.raft;
    rc=tr_raft_cnet_channel_get_status(peer->channel,&channel);
    if (rc!=SALTS_OK ||
        channel.phase!=TR_RAFT_CNET_CHANNEL_ACTIVE ||
        channel.authenticated_peer_node_id!=m->from ||
        m->to!=w->id || m->from==w->id ||
        (m->from!=1U && m->from!=2U && m->from!=3U))
        return SALTS_EPROTO;
    ++w->wire_received;
    if (w->id==2U && m->from==3U &&
        (m->type==TR_RAFT_MSG_VOTE_RESPONSE ||
         m->type==TR_RAFT_MSG_PRE_VOTE_RESPONSE))
        ++w->votes_from_3;
    if (w->id==2U && m->from==3U &&
        m->type==TR_RAFT_MSG_APPEND_RESPONSE)
        ++w->append_acks_from_3;
    if (w->id==1U) {
        ++w->node1_ignored; /* intentionally silent Core1, REAL TLS */
        return SALTS_OK;
    }
    rc=core_step(w,m);
    if (rc!=SALTS_OK) w->error=rc;
    return rc;
}

static int send_pending(node_worker_t *w)
{
    tr_raft_transport_payload_t payload={0};
    tr_raft_cnet_channel_t *target=NULL;
    tr_raft_message_t message;
    size_t i;
    int rc;
    if (w->pending_count==0U) return SALTS_OK;
    message=w->pending[0];
    if (message.from!=w->id || message.to==w->id)
        return SALTS_EPROTO;
    if (w->id==2U) {
        if (message.to!=1U && message.to!=3U) return SALTS_EPROTO;
        for (i=0U;i<w->accepted;++i) {
            tr_raft_cnet_channel_status_t s={0};
            rc=tr_raft_cnet_channel_get_status(w->channels[i],&s);
            if (rc!=SALTS_OK) return rc;
            if (s.phase==TR_RAFT_CNET_CHANNEL_ACTIVE &&
                s.authenticated_peer_node_id==message.to) {
                target=w->channels[i];
                break;
            }
        }
    } else if (message.to==2U) {
        target=w->channels[0];
    } else return SALTS_EPROTO;
    if (target==NULL) return SALTS_EPROTO;
    payload.group_id=77U;
    payload.kind=TR_RAFT_WIRE_PAYLOAD_RAFT;
    payload.data.raft=message;
    rc=tr_raft_cnet_channel_send(target,&payload);
    if (rc!=SALTS_OK) return rc; /* no retry of accepted sends */
    if (w->pending_count>1U)
        memmove(&w->pending[0],&w->pending[1],
                (w->pending_count-1U)*sizeof(w->pending[0]));
    --w->pending_count;
    ++w->wire_sent;
    return SALTS_OK;
}

static int make_listener(node_worker_t *w)
{
    cnet_tls_server_config tls={0};
    cnet_listener_config listener={0};
    int rc;
    tls.size=sizeof(tls);
    tls.ca_file=w->tls_paths[1];
    tls.cert_file=w->tls_paths[6];
    tls.key_file=w->tls_paths[7];
    tls.client_auth=CNET_TLS_CLIENT_AUTH_REQUIRED;
    rc=cnet_tls_server_init(&w->server_tls,&tls);
    if (rc!=SALTS_OK) return rc;
    w->tls_live=1;
    listener.backend=NATIVE_IO_BACKEND_EPOLL;
    listener.host="127.0.0.1";
    listener.port=0U;
    listener.backlog=LINKS;
    rc=cnet_listener_init(&w->listener,&listener);
    if (rc!=SALTS_OK) return rc;
    w->listener_live=1;
    return cnet_listener_port(&w->listener,&w->port);
}

static int network_setup(node_worker_t *w)
{
    static const char *const files[8]={
        "ca.pem","three-node-client-ca.pem","node1-cert.pem",
        "node1-key.pem","node2-client-cert.pem","node2-client-key.pem",
        "node2-cert.pem","node2-key.pem"
    };
    cnet_client_config net=network_config();
    size_t i;
    int rc;
    for (i=0U;i<8U;++i) {
        rc=fixture_path(w->tls_paths[i],sizeof(w->tls_paths[i]),files[i]);
        if (rc!=SALTS_OK) return rc;
    }
    if (w->id==2U) {
        w->fingerprints[0][0]=NODE1_FP;
        w->fingerprints[1][0]=NODE3_FP;
        w->allowed[0]=(tr_raft_cnet_peer_identity_t){
            1U,w->fingerprints[0],1U};
        w->allowed[1]=(tr_raft_cnet_peer_identity_t){
            3U,w->fingerprints[1],1U};
        w->policy=(tr_raft_cnet_identity_policy_t){
            2U,w->allowed,LINKS};
    } else {
        w->fingerprints[0][0]=NODE2_FP;
        w->allowed[0]=(tr_raft_cnet_peer_identity_t){
            2U,w->fingerprints[0],1U};
        w->policy=(tr_raft_cnet_identity_policy_t){
            w->id,w->allowed,1U};
        w->client_tls.size=sizeof(w->client_tls);
        w->client_tls.ca_file=w->tls_paths[0];
        w->client_tls.cert_file=w->tls_paths[w->id==1U?2U:4U];
        w->client_tls.key_file=w->tls_paths[w->id==1U?3U:5U];
        w->client_tls.server_name="node-2.mesh";
    }
    rc=tr_raft_cnet_identity_policy_validate(&w->policy);
    if (rc!=SALTS_OK) return rc;
    rc=cnet_client_init(&w->net,&net);
    if (rc!=SALTS_OK) return rc;
    w->net_live=1;
    return w->id==2U?make_listener(w):SALTS_OK;
}

static int connect_peer(node_worker_t *w,uint16_t port)
{
    tr_raft_cnet_channel_config_t cfg={0};
    cnet_connect_options options={0};
    char uri[128];
    int rc;
    if (w->id==2U || w->channels[0]!=NULL || port==0U)
        return SALTS_EINVAL;
    if (snprintf(uri,sizeof(uri),"tls://127.0.0.1:%u",
                 (unsigned)port)<=0) return SALTS_EINVAL;
    w->peers[0].worker=w;
    w->peers[0].expected_remote=2U;
    cfg.client=&w->net;
    cfg.identity=&w->policy;
    cfg.handshake=handshake(w->id);
    cfg.first_outbound_message_id=1U;
    cfg.host_module_generation=UINT64_C(14920001);
    cfg.on_payload=on_payload;
    cfg.payload_context=&w->peers[0];
    rc=tr_raft_cnet_channel_create(&cfg,&w->channels[0]);
    if (rc!=SALTS_OK) return rc;
    w->peers[0].channel=w->channels[0];
    options.uri=uri;
    options.tls=&w->client_tls;
    options.observer=tr_raft_cnet_channel_observer(w->channels[0]);
    rc=cnet_connect(&w->net,&options,&w->connections[0]);
    if (rc!=SALTS_OK) return rc;
    return tr_raft_cnet_channel_attach(
        w->channels[0],w->connections[0]);
}

static int accept_peers(node_worker_t *w)
{
    tr_raft_cnet_channel_config_t cfg={0};
    cnet_observer observer={0};
    size_t pos;
    int ready=0,rc;
    if (w->id!=2U || w->accepted>=LINKS) return SALTS_OK;
    rc=cnet_listener_wait(&w->listener,0U,&ready);
    if (rc!=SALTS_OK || !ready) return rc;
    pos=w->accepted;
    w->peers[pos].worker=w;
    cfg.client=&w->net;
    cfg.identity=&w->policy;
    cfg.handshake=handshake(2U);
    cfg.first_outbound_message_id=1U;
    cfg.host_module_generation=UINT64_C(14920001);
    cfg.on_payload=on_payload;
    cfg.payload_context=&w->peers[pos];
    rc=tr_raft_cnet_channel_create(&cfg,&w->channels[pos]);
    if (rc!=SALTS_OK) return rc;
    w->peers[pos].channel=w->channels[pos];
    observer=tr_raft_cnet_channel_observer(w->channels[pos]);
    rc=cnet_listener_accept_tls(
        &w->listener,&w->net,&w->server_tls,
        &observer,&w->connections[pos]);
    if (rc!=SALTS_OK) return rc;
    rc=tr_raft_cnet_channel_attach(
        w->channels[pos],w->connections[pos]);
    if (rc!=SALTS_OK) return rc;
    ++w->accepted;
    return SALTS_OK;
}

static unsigned count_active(node_worker_t *w)
{
    unsigned count=0U;
    size_t i,limit=(w->id==2U?w->accepted:(w->channels[0]!=NULL?1U:0U));
    for (i=0U;i<limit;++i) {
        tr_raft_cnet_channel_status_t s={0};
        if (tr_raft_cnet_channel_get_status(w->channels[i],&s)==SALTS_OK &&
            s.phase==TR_RAFT_CNET_CHANNEL_ACTIVE &&
            ((w->id==2U && (s.authenticated_peer_node_id==1U ||
                             s.authenticated_peer_node_id==3U)) ||
             (w->id!=2U && s.authenticated_peer_node_id==2U)))
            ++count;
    }
    return count;
}

static int worker_cleanup(node_worker_t *w)
{
    size_t i;
    int rc,result=SALTS_OK;
    for (i=0U;i<LINKS;++i)
        if (w->channels[i]!=NULL)
            (void)tr_raft_cnet_channel_stop(w->channels[i]);
    if (w->net_live) {
        rc=cnet_client_stop(&w->net,2000U);
        if (rc!=SALTS_OK && rc!=SALTS_EALREADY) result=rc;
    }
    for (i=0U;i<LINKS;++i) {
        if (w->channels[i]!=NULL) {
            tr_raft_cnet_channel_status_t s={0};
            rc=tr_raft_cnet_channel_get_status(w->channels[i],&s);
            if (rc==SALTS_OK && (!s.terminal ||
                 s.payload_writes_pending!=0U ||
                 s.payloads_admitted!=s.payloads_completed+s.payloads_canceled))
                rc=SALTS_EPROTO;
            if (rc==SALTS_OK)
                rc=tr_raft_cnet_channel_destroy(w->channels[i]);
            if (result==SALTS_OK) result=rc;
        }
    }
    if (w->net_live) {
        rc=cnet_client_destroy(&w->net);
        if (result==SALTS_OK) result=rc;
    }
    if (w->listener_live) {
        rc=cnet_listener_close(&w->listener);
        if (result==SALTS_OK) result=rc;
        rc=cnet_listener_destroy(&w->listener);
        if (result==SALTS_OK) result=rc;
    }
    if (w->tls_live) {
        rc=cnet_tls_server_destroy(&w->server_tls);
        if (result==SALTS_OK) result=rc;
    }
    tr_raft_core_destroy(w->core);
    if (w->wal!=NULL) {
        rc=tr_raft_wal_storage_close(w->wal);
        if (result==SALTS_OK) result=rc;
    }
    return result;
}

static int init_core_wal(node_worker_t *w,const char *prefix)
{
    tr_raft_wal_storage_config_t wc=wal_config(prefix,1);
    tr_raft_wal_recovery_t recovered={0};
    tr_raft_core_config_t cc={0};
    int rc=tr_raft_wal_storage_open(&wc,&w->wal);
    if (rc!=SALTS_OK) return rc;
    rc=tr_raft_wal_storage_bind(w->wal,&w->storage);
    if (rc!=SALTS_OK) return rc;
    rc=tr_raft_wal_storage_load(w->wal,&recovered);
    if (rc!=SALTS_OK) return rc;
    if (recovered.term!=0U || recovered.voted_for!=0U ||
        recovered.entry_count!=0U || recovered.commit_index!=0U ||
        recovered.snapshot_index!=0U) {
        tr_raft_wal_recovery_destroy(&recovered);
        return SALTS_EPROTO;
    }
    cc.self_id=w->id;
    cc.voters=VOTERS;
    cc.voter_count=NODES;
    cc.heartbeat_ticks=1U;
    cc.election_min_ticks=3U;
    cc.election_max_ticks=6U;
    cc.initial_election_timeout_ticks=w->id+2U;
    cc.max_log_entries=MAX_LOG;
    rc=tr_raft_core_create(&cc,&w->core);
    tr_raft_wal_recovery_destroy(&recovered);
    return rc;
}

static int snapshot(node_worker_t *w,uint32_t op,control_status_t *out)
{
    tr_raft_status_t core={0};
    tr_raft_wal_recovery_t disk={0};
    int rc=tr_raft_core_status(w->core,&core);
    if (rc!=SALTS_OK) return rc;
    rc=tr_raft_wal_storage_load(w->wal,&disk);
    if (rc!=SALTS_OK) return rc;
    memset(out,0,sizeof(*out));
    out->magic=IPC_MAGIC;
    out->op=op;
    out->id=w->id;
    out->pid=(uint64_t)getpid();
    out->port=w->port;
    out->role=(uint64_t)core.role;
    out->term=core.term;
    out->vote=core.voted_for;
    out->last=core.last_log_index;
    out->committed=core.commit_index;
    out->applied=core.applied_index;
    out->wal_term=disk.term;
    out->wal_vote=disk.voted_for;
    out->wal_last=disk.snapshot_index+disk.entry_count;
    out->wal_commit=disk.commit_index;
    out->wal_hash=entries_hash(&disk);
    out->active_tls=count_active(w);
    out->wire_sent=w->wire_sent;
    out->wire_received=w->wire_received;
    out->certified_votes_from_3=w->votes_from_3;
    out->certified_append_acks_from_3=w->append_acks_from_3;
    out->node1_ignored=w->node1_ignored;
    out->error=w->error;
    tr_raft_wal_recovery_destroy(&disk);
    if (out->wal_term!=out->term || out->wal_vote!=out->vote ||
        out->wal_last!=out->last || out->wal_commit!=out->committed ||
        out->applied!=w->applied)
        return SALTS_EPROTO;
    return SALTS_OK;
}

static int handle_command(node_worker_t *w,const control_command_t *cmd,
                          int *stop)
{
    control_status_t reply={0};
    int rc=SALTS_OK;
    if (cmd->op=='C') {
        rc=connect_peer(w,(uint16_t)cmd->arg);
    } else if (cmd->op=='E') {
        if (w->id!=2U || count_active(w)!=LINKS ||
            w->campaign_started) rc=SALTS_EINVAL;
        else {
            w->campaign_started=1;
            w->next_tick_ms=cmeta_monotonic_ms()+100U;
            rc=core_tick(w,4U);
        }
    } else if (cmd->op=='P') {
        if (w->id!=2U || cmd->arg<1U || cmd->arg>3U)
            rc=SALTS_EINVAL;
        else rc=core_propose(w,(unsigned)cmd->arg);
    } else if (cmd->op!='S' && cmd->op!='Q')
        rc=SALTS_EINVAL;
    if (rc!=SALTS_OK) return rc;
    rc=snapshot(w,cmd->op,&reply);
    if (rc!=SALTS_OK) return rc;
    if (fd_message(w->control,&reply,sizeof(reply))!=0) return SALTS_EPIPE;
    if (cmd->op=='Q') *stop=1;
    return SALTS_OK;
}

static int worker_main(unsigned id,const char *prefix,int control)
{
    node_worker_t *w=(node_worker_t *)calloc(1U,sizeof(*w));
    control_status_t reply={0};
    int stop=0,rc=SALTS_OK,clean;
    const uint64_t started=cmeta_monotonic_ms();
    if (w==NULL) return 1;
    w->id=id;
    w->control=control;
    rc=init_core_wal(w,prefix);
    if (rc==SALTS_OK) rc=network_setup(w);
    if (rc==SALTS_OK) rc=snapshot(w,'R',&reply);
    if (rc==SALTS_OK && fd_message(control,&reply,sizeof(reply))!=0)
        rc=SALTS_EPIPE;
    while (rc==SALTS_OK && !stop &&
           cmeta_monotonic_ms()-started<UINT64_C(29000)) {
        control_command_t cmd={0};
        size_t events=0U;
        ssize_t got=recv(control,&cmd,sizeof(cmd),MSG_DONTWAIT);
        if (got==0) { rc=SALTS_EPIPE; break; }
        if (got>0) {
            if ((size_t)got!=sizeof(cmd)) {rc=SALTS_EPROTO;break;}
            rc=handle_command(w,&cmd,&stop);
            if (rc!=SALTS_OK || stop) break;
        } else if (errno!=EAGAIN && errno!=EWOULDBLOCK && errno!=EINTR) {
            rc=SALTS_EPIPE;
            break;
        }
        if (w->id==2U) rc=accept_peers(w);
        if (rc==SALTS_OK && w->net_live)
            rc=cnet_client_poll(&w->net,1U,&events);
        if (rc==SALTS_OK) rc=send_pending(w);
        if (rc==SALTS_OK && w->campaign_started &&
            cmeta_monotonic_ms()>=w->next_tick_ms) {
            rc=core_tick(w,1U);
            w->next_tick_ms=cmeta_monotonic_ms()+UINT64_C(100);
        }
        if (rc==SALTS_OK && w->error!=SALTS_OK) rc=w->error;
    }
    if (rc==SALTS_OK && !stop) rc=SALTS_ETIMEDOUT;
    clean=worker_cleanup(w);
    if (rc==SALTS_OK) rc=clean;
    if (rc!=SALTS_OK)
        fprintf(stderr,"#149 live TLS node%u failed rc=%d sent=%llu recv=%llu\n",
                id,rc,(unsigned long long)w->wire_sent,
                (unsigned long long)w->wire_received);
    free(w);
    (void)close(control);
    return rc==SALTS_OK?0:1;
}

static int child_command(child_t *child,unsigned id,uint32_t op,
                         uint64_t arg,control_status_t *reply)
{
    control_command_t command={0};
    command.op=op;
    command.arg=arg;
    if (fd_message(child->control,&command,sizeof(command))!=0 ||
        fd_receive(child->control,reply,sizeof(*reply))!=0)
        return -1;
    if (reply->magic!=IPC_MAGIC || reply->op!=op ||
        reply->id!=id || reply->pid!=(uint64_t)child->pid ||
        reply->error!=SALTS_OK || reply->applied>reply->committed ||
        reply->wal_last!=reply->last ||
        reply->wal_commit!=reply->committed ||
        reply->wal_term!=reply->term ||
        reply->wal_vote!=reply->vote)
        return -1;
    return 0;
}

static int start_child(const char *program,unsigned id,
                       const char *prefix,child_t *children,
                       control_status_t *hello_reply)
{
    int sockets[2]={-1,-1};
    char node_arg[16],fd_arg[16];
    pid_t pid;
    unsigned i;
    if (socketpair(AF_UNIX,SOCK_SEQPACKET,0,sockets)!=0) return -1;
    pid=fork();
    if (pid<0) {
        (void)close(sockets[0]);(void)close(sockets[1]);
        return -1;
    }
    if (pid==0) {
        for (i=0U;i<id-1U;++i)
            if (children[i].control>=0) (void)close(children[i].control);
        (void)close(sockets[0]);
        (void)snprintf(node_arg,sizeof(node_arg),"%u",id);
        (void)snprintf(fd_arg,sizeof(fd_arg),"%d",sockets[1]);
        execl(program,program,"--node",node_arg,prefix,fd_arg,(char *)NULL);
        _exit(127);
    }
    (void)close(sockets[1]);
    children[id-1U].pid=pid;
    children[id-1U].control=sockets[0];
    if (fd_receive(sockets[0],hello_reply,sizeof(*hello_reply))!=0 ||
        hello_reply->magic!=IPC_MAGIC || hello_reply->op!='R' ||
        hello_reply->id!=id || hello_reply->pid!=(uint64_t)pid ||
        hello_reply->last!=0U || hello_reply->committed!=0U ||
        hello_reply->wal_last!=0U || hello_reply->wal_commit!=0U)
        return -1;
    return 0;
}

static int snapshot_three(child_t *children,control_status_t status[NODES])
{
    unsigned i;
    for (i=0U;i<NODES;++i)
        if (child_command(&children[i],i+1U,'S',0U,&status[i])!=0)
            return -1;
    return 0;
}

static int wait_for_state(child_t *children,int target)
{
    uint64_t start=cmeta_monotonic_ms();
    control_status_t states[NODES];
    while (cmeta_monotonic_ms()-start<TOTAL_TIMEOUT_MS) {
        if (snapshot_three(children,states)!=0) return -1;
        if (target==0) {
            if (states[0].active_tls==1U &&
                states[1].active_tls==2U &&
                states[2].active_tls==1U &&
                states[0].node1_ignored==0U)
                return 0;
        } else if (target==-1) {
            if (states[1].role==TR_RAFT_LEADER &&
                states[1].term!=0U &&
                states[1].certified_votes_from_3>0U &&
                states[1].active_tls==2U &&
                states[2].active_tls==1U)
                return 0;
        } else if (states[1].role==TR_RAFT_LEADER &&
                   states[1].committed>=(uint64_t)target &&
                   states[1].applied>=(uint64_t)target &&
                   states[2].committed>=(uint64_t)target &&
                   states[2].applied>=(uint64_t)target &&
                   states[0].committed==0U &&
                   states[0].last==0U &&
                   states[0].node1_ignored!=0U &&
                   states[1].certified_append_acks_from_3 >=
                       (uint64_t)target &&
                   states[1].wal_hash==states[2].wal_hash &&
                   states[1].wal_last==(uint64_t)target &&
                   states[2].wal_last==(uint64_t)target)
            return 0;
    }
    fprintf(stderr,"#149 real TLS timeout stage=%d node2(term=%llu role=%llu commit=%llu ack3=%llu) node3(commit=%llu) node1(commit=%llu ignored=%llu)\n",
            target,(unsigned long long)states[1].term,
            (unsigned long long)states[1].role,
            (unsigned long long)states[1].committed,
            (unsigned long long)states[1].certified_append_acks_from_3,
            (unsigned long long)states[2].committed,
            (unsigned long long)states[0].committed,
            (unsigned long long)states[0].node1_ignored);
    return -1;
}

static int reopen_wal(const char *prefix,unsigned id,
                      const control_status_t *expected)
{
    tr_raft_wal_storage_t *wal=NULL;
    tr_raft_wal_recovery_t recovered={0};
    tr_raft_wal_storage_config_t cfg=wal_config(prefix,0);
    int rc=tr_raft_wal_storage_open(&cfg,&wal);
    if (rc!=SALTS_OK) return -1;
    rc=tr_raft_wal_storage_load(wal,&recovered);
    if (rc==SALTS_OK &&
        (recovered.term!=expected->wal_term ||
         recovered.voted_for!=expected->wal_vote ||
         recovered.commit_index!=expected->wal_commit ||
         recovered.entry_count!=(id==1U?0U:3U) ||
         recovered.snapshot_index!=0U ||
         entries_hash(&recovered)!=expected->wal_hash))
        rc=SALTS_EPROTO;
    if (rc==SALTS_OK && id!=1U) {
        size_t i;
        for (i=0U;i<3U;++i) {
            const tr_raft_entry_t *e=&recovered.entries[i];
            char buffer[24];
            (void)snprintf(buffer,sizeof(buffer),"command-%zu",i+1U);
            if (e->index!=i+1U || e->command_id!=i+1U ||
                e->data_length!=strlen(buffer) ||
                memcmp(e->data,buffer,e->data_length)!=0) {
                rc=SALTS_EPROTO;
                break;
            }
        }
    }
    tr_raft_wal_recovery_destroy(&recovered);
    if (tr_raft_wal_storage_close(wal)!=SALTS_OK) rc=SALTS_EIO;
    return rc==SALTS_OK?0:-1;
}

static void cleanup_root(const char *root)
{
    unsigned id;
    for (id=1U;id<=NODES;++id) {
        char path[512];
        DIR *d;
        struct dirent *e;
        if (snprintf(path,sizeof(path),"%s/node%u",root,id)<=0)
            continue;
        d=opendir(path);
        if (d==NULL) continue;
        while ((e=readdir(d))!=NULL) {
            char file[1024];
            int n;
            if (!strcmp(e->d_name,".") || !strcmp(e->d_name,".."))
                continue;
            n=snprintf(file,sizeof(file),"%s/%s",path,e->d_name);
            if (n>0 && (size_t)n<sizeof(file)) (void)unlink(file);
        }
        (void)closedir(d);
        (void)rmdir(path);
    }
    (void)rmdir(root);
}

static int controller_main(const char *program)
{
    const char *temp=getenv("TMPDIR");
    char root[512],directory[512];
    child_t children[NODES]={{0}};
    control_status_t statuses[NODES]={{0}};
    uint16_t port=0U;
    unsigned i;
    int failed=0;
    if (temp==NULL || *temp=='\0') temp="/tmp";
    if (snprintf(root,sizeof(root),"%s/turboraft-3tls-XXXXXX",
                 temp)>=(int)sizeof(root)) return 1;
    if (mkdtemp(root)==NULL) return 1;
    for (i=0U;i<NODES;++i) {
        children[i].pid=-1;
        children[i].control=-1;
        if (snprintf(directory,sizeof(directory),"%s/node%u",
                     root,i+1U)>=(int)sizeof(directory) ||
            mkdir(directory,0700)!=0 ||
            snprintf(children[i].wal_prefix,
                     sizeof(children[i].wal_prefix),"%s/raft",
                     directory)>=(int)sizeof(children[i].wal_prefix))
            {failed=1;goto done;}
    }
    /* Node2 creates its listener first, but all THREE workers keep their
     * independent event loops running concurrently before voting begins. */
    for (i=0U;i<NODES;++i)
        if (start_child(program,i+1U,children[i].wal_prefix,
                        children,&statuses[i])!=0) {
            failed=1;goto done;
        }
    port=(uint16_t)statuses[1].port;
    if (port==0U || children[0].pid==children[1].pid ||
        children[1].pid==children[2].pid ||
        child_command(&children[0],1U,'C',port,&statuses[0])!=0 ||
        child_command(&children[2],3U,'C',port,&statuses[2])!=0 ||
        wait_for_state(children,0)!=0) {
        failed=1;goto done;
    }
    if (child_command(&children[1],2U,'E',0U,&statuses[1])!=0 ||
        wait_for_state(children,-1)!=0) {
        failed=1;goto done;
    }
    for (i=1U;i<=3U;++i) {
        if (child_command(&children[1],2U,'P',i,&statuses[1])!=0 ||
            wait_for_state(children,(int)i)!=0) {
            failed=1;goto done;
        }
    }
    if (snapshot_three(children,statuses)!=0 ||
        statuses[0].committed!=0U ||
        statuses[0].wal_last!=0U ||
        statuses[1].committed!=3U ||
        statuses[2].committed!=3U ||
        statuses[1].certified_votes_from_3==0U ||
        statuses[1].certified_append_acks_from_3<3U ||
        statuses[1].wal_hash!=statuses[2].wal_hash) {
        failed=1;goto done;
    }
    /* Close all three REAL owners before independent controller WAL opens.
     * No staged Core memory or runtime handles survive the process boundary. */
    for (i=0U;i<NODES;++i) {
        control_status_t reply={0};
        int status,waited;
        if (child_command(&children[i],i+1U,'Q',0U,&reply)!=0) {
            failed=1;goto done;
        }
        do {waited=(int)waitpid(children[i].pid,&status,0);}
        while (waited<0 && errno==EINTR);
        if (waited!=(int)children[i].pid || !WIFEXITED(status) ||
            WEXITSTATUS(status)!=0) {
            failed=1;goto done;
        }
        children[i].pid=-1;
        (void)close(children[i].control);
        children[i].control=-1;
    }
    for (i=0U;i<NODES;++i)
        if (reopen_wal(children[i].wal_prefix,i+1U,&statuses[i])!=0) {
            failed=1;goto done;
        }
done:
    for (i=0U;i<NODES;++i) child_kill(&children[i]);
    cleanup_root(root);
    if (failed) {
        fputs("FAIL #149 three distinct live Core/WAL owners with authentic TLS 2/3 majority\n",stderr);
        return 1;
    }
    puts("PASS #149 three separately exec'ed Core/WAL owners; real certified TLS Node2+Node3 votes/Append ACKs; three WAL independently reopened; 3/3 majority-committed commands on Node2+3 while Node1 remains silent (NOT partition/crash recovery)");
    return 0;
}

int main(int argc,char **argv)
{
    if (argc==5 && strcmp(argv[1],"--node")==0) {
        char *end=NULL;
        unsigned long id=strtoul(argv[2],&end,10);
        int fd=atoi(argv[4]);
        if (end==argv[2] || *end!='\0' || id<1UL ||
            id>NODES || fd<3) return 2;
        return worker_main((unsigned)id,argv[3],fd);
    }
    if (argc!=1) return 2;
    return controller_main(argv[0]);
}
