#include <turboraft/raft_node_config.h>
#include <cmeta_fs.h>
#include <cmeta_error.h>
#include <tstr.h>
#include <stdlib.h>
#include <string.h>
#include "node_schema.h"

typedef struct tr_node_peer_storage {
    tr_raft_flowmq_peer_config_t peers[TR_RAFT_MAX_MEMBERS];
    const char *certificates[TR_RAFT_MAX_MEMBERS][TR_RAFT_FLOWMQ_MAX_CERTIFICATES_PER_PEER];
} tr_node_peer_storage_t;

typedef struct tr_node_members {
    tr_raft_node_id_t voters[TR_RAFT_MAX_MEMBERS];
    tr_raft_node_id_t learners[TR_RAFT_MAX_MEMBERS];
} tr_node_members_t;

struct tr_raft_node_config {
    DataBind *codec;
    DataBindRecord *record;
    tr_raft_node_settings_t settings;
    tr_raft_group_assignment_t *assignments;
    tr_raft_node_group_config_t *groups;
    tr_node_members_t *members;
    tr_raft_flowmq_peer_service_config_t *owners;
    tr_node_peer_storage_t *peers;
};

static bool tr_node_absolute(const char *path)
{
    if (path == NULL || path[0] == '\0') return false;
    if (path[0] == '/') return true;
    if (path[0] == '\\' && path[1] == '\\') return true;
    return ((path[0] >= 'A' && path[0] <= 'Z') || (path[0] >= 'a' && path[0] <= 'z')) &&
        path[1] == ':' && (path[2] == '/' || path[2] == '\\');
}

static bool tr_node_text(const char *text)
{
    return text != NULL && text[0] != '\0';
}

static bool tr_node_tls(const char *endpoint)
{
    return endpoint != NULL && strncmp(endpoint, "tls://", 6U) == 0;
}

static bool tr_node_endpoint(const char *endpoint)
{
    return endpoint != NULL &&
        (strncmp(endpoint, "tcp://", 6U) == 0 || tr_node_tls(endpoint)) && endpoint[6] != '\0';
}

static bool tr_node_tls_valid(const tr_raft_flowmq_tls_config_t *tls,
                             const char *endpoint, bool listener)
{
    if (!tr_node_endpoint(endpoint)) return false;
    if (!tr_node_tls(endpoint))
        return !tr_node_text(tls->ca_file) && !tr_node_text(tls->cert_file) &&
               !tr_node_text(tls->key_file) && !tr_node_text(tls->server_name) &&
               !tr_node_text(tls->key_password);
    return tr_node_absolute(tls->ca_file) && tr_node_absolute(tls->cert_file) &&
        tr_node_absolute(tls->key_file) &&
        (listener ? tls->require_client_certificate != 0 : tr_node_text(tls->server_name));
}

static bool tr_node_fingerprint(const char *s)
{
    size_t i;
    if (s == NULL || strlen(s) != 71U || strncmp(s, "sha256:", 7U) != 0) return false;
    for (i = 7U; i < 71U; ++i)
        if (!((s[i] >= '0' && s[i] <= '9') || (s[i] >= 'a' && s[i] <= 'f'))) return false;
    return true;
}

int tr_raft_node_settings_validate(const tr_raft_node_settings_t *s)
{
    const tr_raft_cluster_id_t zero = {{0}};
    size_t i, j, k;
    int result;
    if (s == NULL || s->node_id == 0U || s->groups == NULL ||
        memcmp(&s->cluster_id, &zero, sizeof(zero)) == 0) return SALTS_EINVAL;
    result = tr_raft_multicore_config_validate(&s->runtime);
    if (result != SALTS_OK) return result;
    if (s->network_enabled != (s->owners != NULL)) return SALTS_EINVAL;
    if (s->network_enabled) {
        for (i = 0; i < s->runtime.owner_count; ++i) {
            const tr_raft_flowmq_peer_service_config_t *o = &s->owners[i];
            if (!tr_node_tls_valid(&o->tls, o->bind_endpoint, true) ||
                !tr_node_text(o->local_identity) || strlen(o->local_identity) > TR_RAFT_FLOWMQ_MAX_IDENTITY_SIZE ||
                o->protocol.local_node_id != s->node_id ||
                memcmp(&o->protocol.cluster_id, &s->cluster_id, sizeof(s->cluster_id)) != 0 ||
                o->peer_count == 0U || o->peer_count >= TR_RAFT_MAX_MEMBERS || o->peers == NULL ||
                o->max_send_batch_items == 0U || o->max_receive_batch_items == 0U ||
                o->max_send_batch_items > TR_RAFT_FLOWMQ_MAX_OUTBOUND_QUEUE_CAPACITY ||
                o->max_receive_batch_items > TR_RAFT_FLOWMQ_MAX_OUTBOUND_QUEUE_CAPACITY ||
                o->send_hwm_messages == 0U || o->receive_hwm_messages == 0U ||
                o->send_hwm_messages > TR_RAFT_FLOWMQ_MAX_OUTBOUND_QUEUE_CAPACITY ||
                o->receive_hwm_messages > TR_RAFT_FLOWMQ_MAX_OUTBOUND_QUEUE_CAPACITY ||
                o->send_hwm_bytes < TR_RAFT_WIRE_MAX_FRAME_SIZE || o->receive_hwm_bytes < TR_RAFT_WIRE_MAX_FRAME_SIZE ||
                o->reconnect_initial_ms == 0U || o->reconnect_max_ms < o->reconnect_initial_ms ||
                o->heartbeat_interval_ms == 0U || o->heartbeat_timeout_ms <= o->heartbeat_interval_ms ||
                o->outbound_limits.total_item_capacity == 0U ||
                o->outbound_limits.total_item_capacity > TR_RAFT_FLOWMQ_MAX_OUTBOUND_QUEUE_CAPACITY ||
                o->outbound_limits.per_group_item_capacity == 0U ||
                o->outbound_limits.per_group_item_capacity > o->outbound_limits.total_item_capacity ||
                o->outbound_limits.max_active_groups == 0U ||
                o->outbound_limits.per_group_data_bytes == 0U ||
                o->outbound_limits.per_group_data_bytes > o->outbound_limits.total_data_bytes)
                return SALTS_EINVAL;
            for (j = 0; j < i; ++j)
                if (strcmp(o->bind_endpoint, s->owners[j].bind_endpoint) == 0) return SALTS_EINVAL;
            for (j = 0; j < o->peer_count; ++j) {
                const tr_raft_flowmq_peer_config_t *p = &o->peers[j];
                if (p->node_id == 0U || p->node_id == s->node_id ||
                    !tr_node_text(p->identity) || strlen(p->identity) > TR_RAFT_FLOWMQ_MAX_IDENTITY_SIZE ||
                    !tr_node_tls_valid(&p->tls, p->endpoint, false) ||
                    tr_node_tls(o->bind_endpoint) != tr_node_tls(p->endpoint) ||
                    (tr_node_tls(o->bind_endpoint) ?
                        (p->client_certificate_sha256_count == 0U ||
                         p->client_certificate_sha256_count > TR_RAFT_FLOWMQ_MAX_CERTIFICATES_PER_PEER ||
                         p->client_certificate_sha256 == NULL) : p->client_certificate_sha256_count != 0U))
                    return SALTS_EINVAL;
                for (k = 0; k < p->client_certificate_sha256_count; ++k)
                    if (!tr_node_fingerprint(p->client_certificate_sha256[k])) return SALTS_EINVAL;
                for (k = 0; k < j; ++k)
                    if (p->node_id == o->peers[k].node_id || strcmp(p->identity, o->peers[k].identity) == 0)
                        return SALTS_EINVAL;
            }
        }
    }
    for (i = 0; i < s->runtime.group_count; ++i) {
        const tr_raft_node_group_config_t *g = &s->groups[i];
        const tr_raft_core_config_t *c = &g->core;
        const tr_raft_group_assignment_t *a = &s->runtime.groups[i];
        bool self = false;
        if (g->group_id != a->group_id || !tr_node_absolute(g->storage_path) ||
            c->self_id != s->node_id || c->voters == NULL || c->voter_count == 0U ||
            c->voter_count > TR_RAFT_MAX_MEMBERS || c->learner_count > TR_RAFT_MAX_MEMBERS - c->voter_count ||
            (c->learner_count != 0U && c->learners == NULL) || c->heartbeat_ticks == 0U ||
            c->election_min_ticks <= c->heartbeat_ticks || c->election_max_ticks < c->election_min_ticks ||
            c->election_min_ticks != a->election_min_ticks || c->election_max_ticks != a->election_max_ticks ||
            c->initial_election_timeout_ticks < c->election_min_ticks ||
            c->initial_election_timeout_ticks > c->election_max_ticks || c->max_log_entries == 0U)
            return SALTS_EINVAL;
        for (j = 0; j < i; ++j)
            if (strcmp(g->storage_path, s->groups[j].storage_path) == 0) return SALTS_EINVAL;
        for (j = 0; j < c->voter_count + c->learner_count; ++j) {
            tr_raft_node_id_t id = j < c->voter_count ? c->voters[j] : c->learners[j - c->voter_count];
            if (id == 0U) return SALTS_EINVAL;
            if (id == s->node_id) self = true;
            for (k = 0; k < j; ++k)
                if (id == (k < c->voter_count ? c->voters[k] : c->learners[k - c->voter_count]))
                    return SALTS_EINVAL;
            if (id != s->node_id) {
                const tr_raft_flowmq_peer_service_config_t *o;
                if (!s->network_enabled) return SALTS_EINVAL;
                o = &s->owners[a->owner_index];
                for (k = 0; k < o->peer_count && o->peers[k].node_id != id; ++k) {}
                if (k == o->peer_count) return SALTS_EINVAL;
            }
        }
        if (!self) return SALTS_EINVAL;
    }
    return SALTS_OK;
}

static const DataBindValue *tr_node_field(const tr_raft_node_config_t *c, const char *name)
{
    DataBindRecordField field = DATA_BIND_RECORD_FIELD_INIT;
    if (data_bind_record_find_field(c->record, name, &field, NULL) != DATA_BIND_OK) return NULL;
    return data_bind_record_field_value(&field);
}

static int tr_node_u32(const DataBindValue *v, uint32_t *out)
{
    uint64_t value;
    if (data_bind_value_get_uint64(v, &value) != DATA_BIND_OK || value > UINT32_MAX) return SALTS_EINVAL;
    *out = (uint32_t)value;
    return SALTS_OK;
}

static const char *tr_node_string(const DataBindValue *v)
{
    const char *s;
    size_t length;
    if (data_bind_value_get_string(v, &s, &length) != DATA_BIND_OK || s == NULL ||
        memchr(s, 0, length) != NULL) return NULL;
    return s; /* DataBind owns a NUL-terminated string, stable until record_free. */
}

static int tr_node_parse_tls(const DataBindValue *v, tr_raft_flowmq_tls_config_t *tls)
{
    tls->ca_file = tr_node_string(data_bind_value_get(v, "tls_ca_file"));
    tls->cert_file = tr_node_string(data_bind_value_get(v, "tls_cert_file"));
    tls->key_file = tr_node_string(data_bind_value_get(v, "tls_key_file"));
    tls->server_name = tr_node_string(data_bind_value_get(v, "tls_server_name"));
    if ((data_bind_value_get(v, "tls_ca_file") != NULL && tls->ca_file == NULL) ||
        (data_bind_value_get(v, "tls_cert_file") != NULL && tls->cert_file == NULL) ||
        (data_bind_value_get(v, "tls_key_file") != NULL && tls->key_file == NULL) ||
        (data_bind_value_get(v, "tls_server_name") != NULL && tls->server_name == NULL) ||
        data_bind_value_get_bool(data_bind_value_get(v, "tls_require_client_certificate"),
                                 &tls->require_client_certificate) != DATA_BIND_OK) return SALTS_EINVAL;
    return SALTS_OK;
}

static int tr_node_convert(tr_raft_node_config_t *c)
{
    tr_raft_multicore_config_t *r = &c->settings.runtime;
    const DataBindValue *groups = tr_node_field(c, "groups");
    const DataBindValue *owners = tr_node_field(c, "owners");
    uint32_t batch, items, bytes, initial, maximum, interval, timeout;
    uint64_t epoch;
    int enabled;
    size_t i, j, k;
#define TR_NODE_U32(FIELD, OUT) do { if (tr_node_u32(tr_node_field(c, FIELD), &(OUT)) != SALTS_OK) return SALTS_EINVAL; } while (0)
    TR_NODE_U32("version", r->version);
    TR_NODE_U32("owner_count", r->owner_count);
    TR_NODE_U32("capacity", r->capacity);
    TR_NODE_U32("work_budget", r->work_budget);
    TR_NODE_U32("tick_ms", r->tick_ms);
    TR_NODE_U32("idle_ms", r->idle_ms);
    TR_NODE_U32("network_batch", batch);
    TR_NODE_U32("network_queue_items", items);
    TR_NODE_U32("network_queue_bytes", bytes);
    TR_NODE_U32("reconnect_initial_ms", initial);
    TR_NODE_U32("reconnect_max_ms", maximum);
    TR_NODE_U32("heartbeat_interval_ms", interval);
    TR_NODE_U32("heartbeat_timeout_ms", timeout);
#undef TR_NODE_U32
    if (data_bind_value_get_uint64(tr_node_field(c, "node_id"), &c->settings.node_id) != DATA_BIND_OK ||
        data_bind_value_get_uuid(tr_node_field(c, "cluster_id"), c->settings.cluster_id.bytes) != DATA_BIND_OK ||
        data_bind_value_get_uint64(tr_node_field(c, "config_epoch"), &epoch) != DATA_BIND_OK ||
        data_bind_value_get_bool(tr_node_field(c, "network_enabled"), &enabled) != DATA_BIND_OK)
        return SALTS_EINVAL;
    c->settings.network_enabled = enabled != 0;
    r->group_count = data_bind_value_count(groups);
    if (r->owner_count == 0U || r->owner_count > TR_RAFT_MULTICORE_MAX_OWNERS ||
        r->group_count == 0U || r->group_count > TR_RAFT_MULTICORE_MAX_GROUPS ||
        data_bind_value_count(owners) != (enabled ? r->owner_count : 0U)) return SALTS_EINVAL;
    c->groups = calloc(r->group_count, sizeof(*c->groups));
    c->members = calloc(r->group_count, sizeof(*c->members));
    c->assignments = calloc(r->group_count, sizeof(*c->assignments));
    if (c->groups == NULL || c->members == NULL || c->assignments == NULL) return SALTS_ENOMEM;
    c->settings.groups = c->groups;
    r->groups = c->assignments;
    for (i = 0; i < r->group_count; ++i) {
        const DataBindValue *v = data_bind_value_at(groups, i);
        const DataBindValue *voters = data_bind_value_get(v, "voters");
        const DataBindValue *learners = data_bind_value_get(v, "learners");
        tr_raft_core_config_t *core = &c->groups[i].core;
        tr_raft_group_assignment_t *a = &c->assignments[i];
        if (data_bind_value_get_uint64(data_bind_value_get(v, "group_id"), &a->group_id) != DATA_BIND_OK ||
            tr_node_u32(data_bind_value_get(v, "owner_index"), &a->owner_index) != SALTS_OK ||
            tr_node_u32(data_bind_value_get(v, "heartbeat_ticks"), &core->heartbeat_ticks) != SALTS_OK ||
            tr_node_u32(data_bind_value_get(v, "election_min_ticks"), &a->election_min_ticks) != SALTS_OK ||
            tr_node_u32(data_bind_value_get(v, "election_max_ticks"), &a->election_max_ticks) != SALTS_OK)
            return SALTS_EINVAL;
        {
            uint32_t max_log;
            if (tr_node_u32(data_bind_value_get(v, "max_log_entries"), &max_log) != SALTS_OK) return SALTS_EINVAL;
            core->max_log_entries = max_log;
        }
        c->groups[i].group_id = a->group_id;
        c->groups[i].storage_path = tr_node_string(data_bind_value_get(v, "storage_path"));
        core->self_id = c->settings.node_id;
        core->election_min_ticks = a->election_min_ticks;
        core->election_max_ticks = a->election_max_ticks;
        core->initial_election_timeout_ticks = a->election_min_ticks;
        core->voter_count = data_bind_value_count(voters);
        core->learner_count = data_bind_value_count(learners);
        if (core->voter_count > TR_RAFT_MAX_MEMBERS || core->learner_count > TR_RAFT_MAX_MEMBERS) return SALTS_EINVAL;
        core->voters = c->members[i].voters;
        core->learners = c->members[i].learners;
        for (j = 0; j < core->voter_count; ++j)
            if (data_bind_value_get_uint64(data_bind_value_at(voters, j), &c->members[i].voters[j]) != DATA_BIND_OK) return SALTS_EINVAL;
        for (j = 0; j < core->learner_count; ++j)
            if (data_bind_value_get_uint64(data_bind_value_at(learners, j), &c->members[i].learners[j]) != DATA_BIND_OK) return SALTS_EINVAL;
    }
    if (!enabled) return tr_raft_node_settings_validate(&c->settings);
    c->owners = calloc(r->owner_count, sizeof(*c->owners));
    c->peers = calloc(r->owner_count, sizeof(*c->peers));
    if (c->owners == NULL || c->peers == NULL) return SALTS_ENOMEM;
    c->settings.owners = c->owners;
    for (i = 0; i < r->owner_count; ++i) {
        const DataBindValue *v = data_bind_value_at(owners, i);
        const DataBindValue *peers = data_bind_value_get(v, "peers");
        tr_raft_flowmq_peer_service_config_t *o = &c->owners[i];
        o->protocol.cluster_id = c->settings.cluster_id;
        o->protocol.local_node_id = c->settings.node_id;
        o->protocol.config_epoch = epoch;
        o->protocol.wire_major_min = o->protocol.wire_major_max = TR_RAFT_HANDSHAKE_WIRE_MAJOR;
        o->protocol.wire_minor_min = o->protocol.wire_minor_max = TR_RAFT_HANDSHAKE_WIRE_MINOR;
        o->protocol.max_frame_size = TR_RAFT_WIRE_MAX_FRAME_SIZE;
        o->protocol.max_snapshot_chunk_size = TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES;
        o->bind_endpoint = tr_node_string(data_bind_value_get(v, "bind_endpoint"));
        o->local_identity = tr_node_string(data_bind_value_get(v, "identity"));
        if (tr_node_parse_tls(v, &o->tls) != SALTS_OK) return SALTS_EINVAL;
        o->peer_count = data_bind_value_count(peers);
        if (o->peer_count >= TR_RAFT_MAX_MEMBERS) return SALTS_EINVAL;
        o->peers = c->peers[i].peers;
        for (j = 0; j < o->peer_count; ++j) {
            const DataBindValue *p = data_bind_value_at(peers, j);
            const DataBindValue *certs = data_bind_value_get(p, "client_certificate_sha256");
            tr_raft_flowmq_peer_config_t *peer = &c->peers[i].peers[j];
            if (data_bind_value_get_uint64(data_bind_value_get(p, "node_id"), &peer->node_id) != DATA_BIND_OK ||
                tr_node_parse_tls(p, &peer->tls) != SALTS_OK) return SALTS_EINVAL;
            peer->endpoint = tr_node_string(data_bind_value_get(p, "endpoint"));
            peer->identity = tr_node_string(data_bind_value_get(p, "identity"));
            peer->client_certificate_sha256_count = data_bind_value_count(certs);
            if (peer->client_certificate_sha256_count > TR_RAFT_FLOWMQ_MAX_CERTIFICATES_PER_PEER) return SALTS_EINVAL;
            if (peer->client_certificate_sha256_count != 0U) peer->client_certificate_sha256 = c->peers[i].certificates[j];
            for (k = 0; k < peer->client_certificate_sha256_count; ++k)
                c->peers[i].certificates[j][k] = tr_node_string(data_bind_value_at(certs, k));
        }
        o->outbound_limits.total_item_capacity = items;
        o->outbound_limits.total_data_bytes = bytes;
        for (j = 0; j < r->group_count; ++j)
            if (c->assignments[j].owner_index == i) ++o->outbound_limits.max_active_groups;
        o->outbound_limits.per_group_item_capacity = r->capacity;
        o->outbound_limits.per_group_data_bytes = bytes;
        o->max_send_batch_items = o->max_receive_batch_items = batch;
        o->send_hwm_messages = o->receive_hwm_messages = items;
        o->send_hwm_bytes = o->receive_hwm_bytes = bytes;
        o->reconnect_initial_ms = initial;
        o->reconnect_max_ms = maximum;
        o->heartbeat_interval_ms = interval;
        o->heartbeat_timeout_ms = timeout;
    }
    return tr_raft_node_settings_validate(&c->settings);
}

int tr_raft_node_config_parse(const char *json, size_t size,
                             tr_raft_node_config_t **out, DataBindError *error)
{
    tr_raft_node_config_t *c;
    DataBindStatus status;
    DataBindJsonOptions options = DATA_BIND_JSON_OPTIONS_INIT;
    int result;
    if (out == NULL) return SALTS_EINVAL;
    *out = NULL;
    if (json == NULL || size == 0U || size > TR_RAFT_NODE_CONFIG_MAX_BYTES) return SALTS_EINVAL;
    c = calloc(1U, sizeof(*c));
    if (c == NULL) return SALTS_ENOMEM;
    options.flags = DATA_BIND_JSON_BIND_EXACT_SCALAR_TOKENS | DATA_BIND_JSON_BIND_REJECT_UNKNOWN_FIELDS;
    status = data_bind_create_from_text(tr_node_schema, sizeof(tr_node_schema) - 1U, &c->codec, error);
    if (status == DATA_BIND_OK)
        status = data_bind_record_from_json_ex(c->codec, "RaftNodeConfig", json, size, &options, &c->record, error);
    result = status == DATA_BIND_OK ? tr_node_convert(c) :
        status == DATA_BIND_ERR_OOM ? SALTS_ENOMEM : SALTS_EINVAL;
    if (result != SALTS_OK) { tr_raft_node_config_destroy(c); return result; }
    *out = c;
    return SALTS_OK;
}

int tr_raft_node_config_load(const char *path, tr_raft_node_config_t **out, DataBindError *error)
{
    cmeta_file_t file;
    tstr buffer;
    size_t used = 0U;
    int count = 0, result;
    if (out == NULL) return SALTS_EINVAL;
    *out = NULL;
    if (path == NULL) return SALTS_EINVAL;
    file = cmeta_fs_open(path, SALTS_FS_O_RDONLY, 0);
    if (file < 0) return file;
    buffer = tstr_new_len(NULL, TR_RAFT_NODE_CONFIG_MAX_BYTES + 1U);
    if (buffer == NULL) { (void)cmeta_fs_close(file); return SALTS_ENOMEM; }
    while (used < TR_RAFT_NODE_CONFIG_MAX_BYTES + 1U) {
        count = cmeta_fs_read(file, buffer + used, TR_RAFT_NODE_CONFIG_MAX_BYTES + 1U - used);
        if (count <= 0) break;
        used += (size_t)count;
    }
    result = cmeta_fs_close(file);
    if (count < 0) result = count;
    if (result == SALTS_OK) result = tr_raft_node_config_parse(buffer, used, out, error);
    tstr_free(buffer);
    return result;
}

const tr_raft_node_settings_t *tr_raft_node_config_settings(const tr_raft_node_config_t *c)
{
    return c != NULL ? &c->settings : NULL;
}

void tr_raft_node_config_destroy(tr_raft_node_config_t *c)
{
    if (c == NULL) return;
    free(c->assignments);
    free(c->groups);
    free(c->members);
    free(c->owners);
    free(c->peers);
    data_bind_record_free(c->record);
    data_bind_free(c->codec);
    free(c);
}
