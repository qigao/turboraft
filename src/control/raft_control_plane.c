#include <turboraft/raft_control_plane.h>

#include <salts_error.h>

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum { TR_RAFT_CONTROL_RENDER_CAPACITY = 8192 };

struct tr_raft_control_plane {
    crpc_server server;
    tr_raft_control_status_provider_fn status_provider;
    void *status_context;
    uint32_t stop_timeout_ms;
    int started;
};

static const char *tr_control_role_name(tr_raft_role_t role)
{
    switch (role) {
    case TR_RAFT_FOLLOWER:
        return "follower";
    case TR_RAFT_PRE_CANDIDATE:
        return "pre-candidate";
    case TR_RAFT_CANDIDATE:
        return "candidate";
    case TR_RAFT_LEADER:
        return "leader";
    default:
        return "invalid";
    }
}

static int tr_control_status(const tr_raft_control_plane_t *plane,
                             tr_raft_service_status_t *out_status)
{
    if (plane == NULL || out_status == NULL) {
        return SALTS_EINVAL;
    }
    memset(out_status, 0, sizeof(*out_status));
    return plane->status_provider(plane->status_context, out_status);
}

static int tr_control_finish_render(int written,
                                    size_t capacity,
                                    size_t *out_size)
{
    if (written < 0 || (size_t)written >= capacity) {
        return SALTS_ENOSPC;
    }
    *out_size = (size_t)written;
    return SALTS_OK;
}

int tr_raft_control_plane_render_status_json(
    const tr_raft_control_plane_t *plane,
    char *output,
    size_t capacity,
    size_t *out_size)
{
    tr_raft_service_status_t status;
    int result;
    int written;

    if (plane == NULL || output == NULL || capacity == 0U ||
        out_size == NULL) {
        return SALTS_EINVAL;
    }
    *out_size = 0U;
    result = tr_control_status(plane, &status);
    if (result != SALTS_OK) {
        return result;
    }
    written = snprintf(
        output, capacity,
        "{\"node_id\":%" PRIu64 ",\"leader_id\":%" PRIu64
        ",\"role\":\"%s\",\"term\":%" PRIu64
        ",\"last_log_index\":%" PRIu64
        ",\"log_base_index\":%" PRIu64
        ",\"log_entry_count\":%zu,\"commit_index\":%" PRIu64
        ",\"applied_index\":%" PRIu64
        ",\"voter_count\":%zu,\"learner_count\":%zu"
        ",\"peer_count\":%zu,\"joint_configuration\":%s"
        ",\"faulted\":%s,\"cause\":%d}",
        status.core.self_id, status.core.leader_id,
        tr_control_role_name(status.core.role), status.core.term,
        status.core.last_log_index, status.core.log_base_index,
        status.core.log_entry_count, status.core.commit_index,
        status.core.applied_index, status.core.voter_count,
        status.core.learner_count, status.core.peer_count,
        status.core.joint_configuration ? "true" : "false",
        status.faulted ? "true" : "false", status.cause);
    return tr_control_finish_render(written, capacity, out_size);
}

int tr_raft_control_plane_render_status_html(
    const tr_raft_control_plane_t *plane,
    char *output,
    size_t capacity,
    size_t *out_size)
{
    tr_raft_service_status_t status;
    int result;
    int written;

    if (plane == NULL || output == NULL || capacity == 0U ||
        out_size == NULL) {
        return SALTS_EINVAL;
    }
    *out_size = 0U;
    result = tr_control_status(plane, &status);
    if (result != SALTS_OK) {
        return result;
    }
    written = snprintf(
        output, capacity,
        "<div class=\"metric hero\"><span>ROLE</span><strong>%s</strong>"
        "</div><div class=\"metric\"><span>NODE</span><strong>%" PRIu64
        "</strong></div><div class=\"metric\"><span>LEADER</span>"
        "<strong>%" PRIu64 "</strong></div><div class=\"metric\">"
        "<span>TERM</span><strong>%" PRIu64 "</strong></div>"
        "<div class=\"metric\"><span>COMMIT</span><strong>%" PRIu64
        "</strong></div><div class=\"metric\"><span>APPLIED</span>"
        "<strong>%" PRIu64 "</strong></div><div class=\"health %s\">%s"
        " &middot; cause %d</div>",
        tr_control_role_name(status.core.role), status.core.self_id,
        status.core.leader_id, status.core.term, status.core.commit_index,
        status.core.applied_index, status.faulted ? "bad" : "good",
        status.faulted ? "FAULTED" : "HEALTHY", status.cause);
    return tr_control_finish_render(written, capacity, out_size);
}

static cserde_status tr_control_write_token(cserde_writer *writer,
                                            cserde_token_kind kind)
{
    cserde_token token;

    memset(&token, 0, sizeof(token));
    token.kind = kind;
    return cserde_writer_write(writer, &token);
}

static cserde_status tr_control_write_string(cserde_writer *writer,
                                             const char *value)
{
    cserde_token token;

    memset(&token, 0, sizeof(token));
    token.kind = CSERDE_STRING;
    token.value.slice.data = (const unsigned char *)value;
    token.value.slice.size = strlen(value);
    token.value.slice.lifetime = CSERDE_VIEW_STABLE;
    return cserde_writer_write(writer, &token);
}

static cserde_status tr_control_write_uint(cserde_writer *writer,
                                           uint64_t value)
{
    cserde_token token;

    memset(&token, 0, sizeof(token));
    token.kind = CSERDE_UINT;
    token.value.uint = value;
    return cserde_writer_write(writer, &token);
}

static cserde_status tr_control_write_bool(cserde_writer *writer, bool value)
{
    cserde_token token;

    memset(&token, 0, sizeof(token));
    token.kind = CSERDE_BOOL;
    token.value.boolean = value;
    return cserde_writer_write(writer, &token);
}

static cserde_status tr_control_encode_status(void *user,
                                              cserde_writer *writer)
{
    const tr_raft_service_status_t *status =
        (const tr_raft_service_status_t *)user;

#define TR_CONTROL_WRITE(expression)                                           \
    do {                                                                       \
        cserde_status write_status = (expression);                             \
        if (write_status != CSERDE_OK) {                                       \
            return write_status;                                               \
        }                                                                      \
    } while (0)

    TR_CONTROL_WRITE(tr_control_write_token(writer, CSERDE_MAP_BEGIN));
    TR_CONTROL_WRITE(tr_control_write_string(writer, "node_id"));
    TR_CONTROL_WRITE(tr_control_write_uint(writer, status->core.self_id));
    TR_CONTROL_WRITE(tr_control_write_string(writer, "leader_id"));
    TR_CONTROL_WRITE(tr_control_write_uint(writer, status->core.leader_id));
    TR_CONTROL_WRITE(tr_control_write_string(writer, "role"));
    TR_CONTROL_WRITE(tr_control_write_string(
        writer, tr_control_role_name(status->core.role)));
    TR_CONTROL_WRITE(tr_control_write_string(writer, "term"));
    TR_CONTROL_WRITE(tr_control_write_uint(writer, status->core.term));
    TR_CONTROL_WRITE(tr_control_write_string(writer, "commit_index"));
    TR_CONTROL_WRITE(tr_control_write_uint(writer, status->core.commit_index));
    TR_CONTROL_WRITE(tr_control_write_string(writer, "applied_index"));
    TR_CONTROL_WRITE(tr_control_write_uint(writer,
                                           status->core.applied_index));
    TR_CONTROL_WRITE(tr_control_write_string(writer, "faulted"));
    TR_CONTROL_WRITE(tr_control_write_bool(writer, status->faulted));
    TR_CONTROL_WRITE(tr_control_write_token(writer, CSERDE_MAP_END));
#undef TR_CONTROL_WRITE
    return CSERDE_OK;
}

static int tr_control_rpc_status(void *user,
                                 const crpc_server_request_view *request,
                                 crpc_server_response *response)
{
    tr_raft_control_plane_t *plane = (tr_raft_control_plane_t *)user;
    tr_raft_service_status_t status;
    int result;

    (void)request;
    result = tr_control_status(plane, &status);
    if (result != SALTS_OK) {
        return crpc_server_response_error(response, -32001,
                                          "Raft status unavailable", NULL,
                                          NULL);
    }
    return crpc_server_response_result(response, tr_control_encode_status,
                                       &status);
}

static int tr_control_http_status(void *user,
                                  const chttp_server_request_view *request,
                                  chttp_server_response *response)
{
    tr_raft_control_plane_t *plane = (tr_raft_control_plane_t *)user;
    char output[TR_RAFT_CONTROL_RENDER_CAPACITY];
    size_t size = 0U;
    int result;

    (void)request;
    result = tr_raft_control_plane_render_status_json(
        plane, output, sizeof(output), &size);
    if (result != SALTS_OK) {
        static const char unavailable[] =
            "{\"error\":\"Raft status unavailable\"}";
        return chttp_server_reply(response, 503U, "application/json",
                                  unavailable, sizeof(unavailable) - 1U);
    }
    return chttp_server_reply(response, 200U, "application/json", output,
                              size);
}

int tr_raft_control_plane_create(
    const tr_raft_control_plane_config_t *config,
    tr_raft_control_plane_t **out_plane)
{
    tr_raft_control_plane_t *plane;
    crpc_method method;
    chttp_server *http;
    int result;

    if (config == NULL || out_plane == NULL ||
        config->status_provider == NULL || config->stop_timeout_ms == 0U) {
        return SALTS_EINVAL;
    }
    *out_plane = NULL;
    plane = (tr_raft_control_plane_t *)calloc(1U, sizeof(*plane));
    if (plane == NULL) {
        return SALTS_ENOMEM;
    }
    plane->status_provider = config->status_provider;
    plane->status_context = config->status_context;
    plane->stop_timeout_ms = config->stop_timeout_ms;
    result = crpc_server_init(&plane->server, &config->server);
    if (result != SALTS_OK) {
        free(plane);
        return result;
    }
    memset(&method, 0, sizeof(method));
    method.service = "raft";
    method.name = "status";
    result = crpc_server_register(&plane->server,
                                  TR_RAFT_CONTROL_RPC_ENDPOINT, &method,
                                  tr_control_rpc_status, plane);
    http = crpc_server_http(&plane->server);
    if (result == SALTS_OK && http != NULL) {
        result = chttp_server_get(http, TR_RAFT_CONTROL_STATUS_PATH,
                                  tr_control_http_status, plane);
    }
    if (result != SALTS_OK) {
        (void)crpc_server_destroy(&plane->server);
        free(plane);
        return result;
    }
    *out_plane = plane;
    return SALTS_OK;
}

int tr_raft_control_plane_start(tr_raft_control_plane_t *plane)
{
    int result;

    if (plane == NULL) {
        return SALTS_EINVAL;
    }
    if (plane->started) {
        return SALTS_EALREADY;
    }
    result = crpc_server_start(&plane->server);
    if (result == SALTS_OK) {
        plane->started = 1;
    }
    return result;
}

int tr_raft_control_plane_port(const tr_raft_control_plane_t *plane,
                               uint16_t *out_port)
{
    if (plane == NULL || out_port == NULL || !plane->started) {
        return SALTS_EINVAL;
    }
    return crpc_server_port(&plane->server, out_port);
}

int tr_raft_control_plane_stop(tr_raft_control_plane_t *plane)
{
    int result;

    if (plane == NULL) {
        return SALTS_EINVAL;
    }
    if (!plane->started) {
        return SALTS_OK;
    }
    result = crpc_server_stop(&plane->server, plane->stop_timeout_ms);
    if (result == SALTS_OK) {
        plane->started = 0;
    }
    return result;
}

int tr_raft_control_plane_destroy(tr_raft_control_plane_t *plane)
{
    int result;

    if (plane == NULL) {
        return SALTS_OK;
    }
    if (plane->started) {
        return SALTS_EBUSY;
    }
    result = crpc_server_destroy(&plane->server);
    if (result == SALTS_OK) {
        free(plane);
    }
    return result;
}

chttp_server *tr_raft_control_plane_http(tr_raft_control_plane_t *plane)
{
    return plane == NULL ? NULL : crpc_server_http(&plane->server);
}
