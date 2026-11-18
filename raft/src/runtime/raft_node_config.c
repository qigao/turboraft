#include <turboraft/raft_node_config.h>
#include <file/fs.h>
#include <core/error.h>
#include <core/tstr.h>
#include <stdlib.h>
#include <string.h>
#include "node_schema.h"

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
};

static bool tr_node_absolute(const char *path)
{
    if (path == NULL || path[0] == '\0') return false;
    if (path[0] == '/') return true;
    if (path[0] == '\\' && path[1] == '\\') return true;
    return ((path[0] >= 'A' && path[0] <= 'Z') || (path[0] >= 'a' && path[0] <= 'z')) &&
        path[1] == ':' && (path[2] == '/' || path[2] == '\\');
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

static int tr_node_convert(tr_raft_node_config_t *c)
{
    tr_raft_multicore_config_t *r = &c->settings.runtime;
    const DataBindValue *groups = tr_node_field(c, "groups");
    size_t i, j;
#define TR_NODE_U32(FIELD, OUT) do { if (tr_node_u32(tr_node_field(c, FIELD), &(OUT)) != SALTS_OK) return SALTS_EINVAL; } while (0)
    TR_NODE_U32("version", r->version);
    TR_NODE_U32("owner_count", r->owner_count);
    TR_NODE_U32("capacity", r->capacity);
    TR_NODE_U32("work_budget", r->work_budget);
    TR_NODE_U32("tick_ms", r->tick_ms);
    TR_NODE_U32("idle_ms", r->idle_ms);
#undef TR_NODE_U32
    if (data_bind_value_get_uint64(tr_node_field(c, "node_id"), &c->settings.node_id) != DATA_BIND_OK ||
        data_bind_value_get_uuid(tr_node_field(c, "cluster_id"), c->settings.cluster_id.bytes) != DATA_BIND_OK)
        return SALTS_EINVAL;
    r->group_count = data_bind_value_count(groups);
    if (r->owner_count == 0U || r->owner_count > TR_RAFT_MULTICORE_MAX_OWNERS ||
        r->group_count == 0U || r->group_count > TR_RAFT_MULTICORE_MAX_GROUPS) return SALTS_EINVAL;
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
    file_t file;
    tstr buffer;
    size_t used = 0U;
    int count = 0, result;
    if (out == NULL) return SALTS_EINVAL;
    *out = NULL;
    if (path == NULL) return SALTS_EINVAL;
    file = fs_open(path, SALTS_FS_O_RDONLY, 0);
    if (file < 0) return file;
    buffer = tstr_new_len(NULL, TR_RAFT_NODE_CONFIG_MAX_BYTES + 1U);
    if (buffer == NULL) { (void)fs_close(file); return SALTS_ENOMEM; }
    while (used < TR_RAFT_NODE_CONFIG_MAX_BYTES + 1U) {
        count = fs_read(file, buffer + used, TR_RAFT_NODE_CONFIG_MAX_BYTES + 1U - used);
        if (count <= 0) break;
        used += (size_t)count;
    }
    result = fs_close(file);
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
    data_bind_record_free(c->record);
    data_bind_free(c->codec);
    free(c);
}
