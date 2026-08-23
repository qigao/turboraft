#include <turboraft/raft_control_plane.h>
#include <turboraft/raft_service_owner.h>

#include <CoroNet.h>
#include <iris/error_recovery.h>
#include <iris/iris_app.h>
#include <iris/router.h>
#include <iris/security.h>
#include <iris/server.h>
#include <tinytest.h>
#include <turbo_http.h>
#include <turbo_error.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int control_http_response_contains(const char *response,
                                          size_t response_size,
                                          const char *text)
{
    size_t text_size;
    size_t offset;

    if (response == NULL || text == NULL) {
        return 0;
    }
    text_size = strlen(text);
    if (text_size == 0U) {
        return 1;
    }
    if (text_size > response_size) {
        return 0;
    }
    for (offset = 0U; offset <= response_size - text_size; ++offset) {
        if (memcmp(response + offset, text, text_size) == 0) {
            return 1;
        }
    }
    return 0;
}

enum {
    CONTROL_HTTP_TEST_PORT = 19941,
    CONTROL_HTTP_AUTH_TEST_PORT = 19942,
    CONTROL_HTTP_RPC_MAX_REQUEST_BYTES = 16 * 1024,
    CONTROL_HTTP_REQUEST_CAPACITY = 20 * 1024,
    CONTROL_HTTP_URL_CAPACITY = 128,
    CONTROL_HTTP_DRAIN_TIMEOUT_MS = 1000
};

typedef struct control_http_state {
    coro_context_t *context;
    coro_socket_t *server;
    iris_app_t *app;
    tr_raft_service_t *service;
    tr_raft_control_plane_t *plane;
    tr_raft_service_owner_t *owner;
    tr_raft_control_audit_t *audit;
    size_t apply_count;
    size_t audit_authorization_count;
    size_t audit_completion_count;
    size_t audit_receipt_count;
    int audit_valid;
    int audit_sink_result;
    int audit_failure_rejected;
    int audit_failure_sticky;
    int audit_status_ok;
    int server_stopped;
    int page_ok;
    int controls_ok;
    int management_ok;
    int operation_control_ok;
    int async_control_ok;
    int fragment_ok;
    int h2_status_ok;
    int websocket_status_ok;
    int status_ok;
    int members_ok;
    int members_filter_ok;
    int progress_ok;
    int progress_filter_ok;
    int storage_ok;
    int batch_boundary_ok;
    int batch_limit_ok;
    int request_boundary_ok;
    int request_limit_ok;
    int tick_ok;
    int leader_ok;
    int propose_ok;
    int operation_ok;
    int disconnect_ok;
    int membership_ok;
    int read_index_ok;
    int take_read_state_ok;
    int auth_rejected;
    int auth_allowed;
    int leader_hint_ok;
    int token_redaction_ok;
    int command_redaction_ok;
    int certificate_redaction_ok;
    char authorization[1024];
    char authorization_with_certificate[1024];
    char bearer_token[1024];
} control_http_state_t;

static int control_http_audit_sink(
    void *context,
    const tr_raft_control_audit_event_t *event)
{
    control_http_state_t *state = (control_http_state_t *)context;

    if (state == NULL || event == NULL) {
        return TURBO_EINVAL;
    }
    state->audit_valid = state->audit_valid &&
                         event->version == TR_RAFT_CONTROL_AUDIT_VERSION &&
                         event->size == sizeof(*event) &&
                         event->local_node_id == 7U;
    if (event->phase == TR_RAFT_CONTROL_AUDIT_AUTHORIZATION) {
        state->audit_authorization_count += 1U;
    } else if (event->phase == TR_RAFT_CONTROL_AUDIT_COMPLETION) {
        state->audit_completion_count += 1U;
        if ((event->method == TR_RAFT_CONTROL_AUDIT_PROPOSE ||
             event->method == TR_RAFT_CONTROL_AUDIT_ADD_LEARNER) &&
            event->term != 0U && event->index != 0U) {
            state->audit_receipt_count += 1U;
        }
    }
    return state->audit_sink_result;
}

static char control_http_oversized_rpc[
    CONTROL_HTTP_RPC_MAX_REQUEST_BYTES + 2U];
static char control_http_boundary_rpc[
    CONTROL_HTTP_RPC_MAX_REQUEST_BYTES + 1U];

static const char control_http_boundary_batch[] =
    "["
    "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"raft.status\",\"params\":{}},"
    "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"raft.status\",\"params\":{}},"
    "{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"raft.status\",\"params\":{}},"
    "{\"jsonrpc\":\"2.0\",\"id\":4,\"method\":\"raft.status\",\"params\":{}},"
    "{\"jsonrpc\":\"2.0\",\"id\":5,\"method\":\"raft.status\",\"params\":{}},"
    "{\"jsonrpc\":\"2.0\",\"id\":6,\"method\":\"raft.status\",\"params\":{}},"
    "{\"jsonrpc\":\"2.0\",\"id\":7,\"method\":\"raft.status\",\"params\":{}},"
    "{\"jsonrpc\":\"2.0\",\"id\":8,\"method\":\"raft.status\",\"params\":{}}"
    "]";

static const char control_http_oversized_batch[] =
    "["
    "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"raft.status\",\"params\":{}},"
    "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"raft.status\",\"params\":{}},"
    "{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"raft.status\",\"params\":{}},"
    "{\"jsonrpc\":\"2.0\",\"id\":4,\"method\":\"raft.status\",\"params\":{}},"
    "{\"jsonrpc\":\"2.0\",\"id\":5,\"method\":\"raft.status\",\"params\":{}},"
    "{\"jsonrpc\":\"2.0\",\"id\":6,\"method\":\"raft.status\",\"params\":{}},"
    "{\"jsonrpc\":\"2.0\",\"id\":7,\"method\":\"raft.status\",\"params\":{}},"
    "{\"jsonrpc\":\"2.0\",\"id\":8,\"method\":\"raft.status\",\"params\":{}},"
    "{\"jsonrpc\":\"2.0\",\"id\":9,\"method\":\"raft.status\",\"params\":{}}"
    "]";

static int control_http_timeout(void *context, uint32_t *timeout)
{
    (void)context;
    *timeout = 4U;
    return TURBO_OK;
}

static int control_http_storage_ok(void *context)
{
    (void) context;
    return TURBO_OK;
}

static int control_http_hard_state(void *context,
                                   tr_raft_term_t term,
                                   tr_raft_node_id_t voted_for)
{
    (void) context;
    (void) term;
    (void) voted_for;
    return TURBO_OK;
}

static int control_http_truncate(void *context, tr_raft_index_t index)
{
    (void) context;
    (void) index;
    return TURBO_OK;
}

static int control_http_append(void *context,
                               const tr_raft_entry_t *entries,
                               size_t count)
{
    (void) context;
    return entries != NULL && count != 0U ? TURBO_OK : TURBO_EINVAL;
}

static int control_http_commit_index(void *context,
                                     tr_raft_index_t index)
{
    (void) context;
    (void) index;
    return TURBO_OK;
}

static int control_http_enqueue(void *context,
                                const tr_raft_message_t *message)
{
    (void) context;
    return message == NULL ? TURBO_EINVAL : TURBO_OK;
}

static int control_http_apply(void *context,
                              const tr_raft_entry_t *entries,
                              size_t count)
{
    control_http_state_t *state = (control_http_state_t *) context;

    if (entries == NULL || count == 0U) {
        return TURBO_EINVAL;
    }
    state->apply_count += count;
    return TURBO_OK;
}

static int control_http_create_service(control_http_state_t *state)
{
    static const tr_raft_node_id_t voters[] = {7U};
    tr_raft_service_config_t config;

    memset(&config, 0, sizeof(config));
    config.core.self_id = 7U;
    config.core.voters = voters;
    config.core.voter_count = 1U;
    config.core.heartbeat_ticks = 1U;
    config.core.election_min_ticks = 3U;
    config.core.election_max_ticks = 5U;
    config.core.initial_election_timeout_ticks = 3U;
    config.core.max_log_entries = 16U;
    config.storage.context = state;
    config.storage.begin = control_http_storage_ok;
    config.storage.write_hard_state = control_http_hard_state;
    config.storage.truncate_log = control_http_truncate;
    config.storage.append_log = control_http_append;
    config.storage.write_commit_index = control_http_commit_index;
    config.storage.commit = control_http_storage_ok;
    config.storage.rollback = control_http_storage_ok;
    config.transport.context = state;
    config.transport.enqueue = control_http_enqueue;
    config.state_machine.context = state;
    config.state_machine.apply_batch = control_http_apply;
    return tr_raft_service_create(&config, &state->service);
}

enum {
    CONTROL_HTTP_RESPONSE_MISMATCH = 0,
    CONTROL_HTTP_RESPONSE_MATCH = 1,
    CONTROL_HTTP_RESPONSE_FORBIDDEN_A = 2,
    CONTROL_HTTP_RESPONSE_FORBIDDEN_B = 3,
    CONTROL_HTTP_RESPONSE_FORBIDDEN_C = 4
};

static int control_http_exchange_auth(coro_context_t *context,
                                      unsigned short port,
                                      const char *method,
                                      const char *path,
                                      const char *body,
                                      const char *authorization,
                                      const char *expected_status,
                                      const char *expected,
                                      const char *forbidden_a,
                                      const char *forbidden_b,
                                      const char *forbidden_c)
{
    coro_socket_t *client;
    char request[CONTROL_HTTP_REQUEST_CAPACITY];
    char *response = NULL;
    size_t response_size = 0U;
    size_t body_size = body == NULL ? 0U : strlen(body);
    int written;
    int matched = 0;

    client = coro_socket_create(context, CORO_SOCKET_TCP_V4);
    if (client == NULL) {
        return 0;
    }
    if (coro_socket_connect(client, "127.0.0.1", port) != TURBO_OK) {
        coro_socket_destroy(client);
        return 0;
    }
    written = snprintf(
        request, sizeof(request),
        "%s %s HTTP/1.1\r\nHost: localhost\r\n"
        "%s%sContent-Length: %zu\r\nConnection: close\r\n\r\n%s",
        method, path,
        body == NULL ? "" : "Content-Type: application/json\r\n",
        authorization == NULL ? "" : authorization,
        body_size, body == NULL ? "" : body);
    if (written <= 0 || (size_t) written >= sizeof(request) ||
        coro_socket_send(client, request, (size_t) written) != TURBO_OK) {
        coro_socket_destroy(client);
        return 0;
    }
    if (coro_socket_recv(client, &response, &response_size) == TURBO_OK &&
        response != NULL && response_size != 0U &&
        control_http_response_contains(response, response_size,
                                       expected_status) &&
        control_http_response_contains(response, response_size, expected)) {
        matched = CONTROL_HTTP_RESPONSE_MATCH;
        if (forbidden_a != NULL &&
            control_http_response_contains(response, response_size,
                                           forbidden_a)) {
            matched = CONTROL_HTTP_RESPONSE_FORBIDDEN_A;
        } else if (forbidden_b != NULL &&
                   control_http_response_contains(response, response_size,
                                                  forbidden_b)) {
            matched = CONTROL_HTTP_RESPONSE_FORBIDDEN_B;
        } else if (forbidden_c != NULL &&
                   control_http_response_contains(response, response_size,
                                                  forbidden_c)) {
            matched = CONTROL_HTTP_RESPONSE_FORBIDDEN_C;
        }
    }
    if (response != NULL) {
        coro_socket_free_recv(response);
    }
    coro_socket_destroy(client);
    return matched;
}

static int control_http_exchange(coro_context_t *context,
                                 const char *method,
                                 const char *path,
                                 const char *body,
                                 const char *expected)
{
    return control_http_exchange_auth(context, CONTROL_HTTP_TEST_PORT,
                                      method, path, body, NULL,
                                      "HTTP/1.1 200 OK", expected,
                                      NULL, NULL, NULL) ==
           CONTROL_HTTP_RESPONSE_MATCH;
}

static int control_websocket_status(coro_context_t *context)
{
    static const char refresh[] = "refresh";
    coro_socket_t *client;
    char *data = NULL;
    size_t size = 0U;
    int is_text = 0;
    int initial_ok = 0;
    int refresh_ok = 0;

    client = coro_socket_create(context, CORO_SOCKET_TCP_V4);
    if (client == NULL) {
        return 0;
    }
    if (coro_socket_connect_ws(client, "127.0.0.1",
                               CONTROL_HTTP_TEST_PORT,
                               TR_RAFT_CONTROL_WS_PATH, 0) != TURBO_OK) {
        coro_socket_destroy(client);
        return 0;
    }
    if (coro_socket_recv_ws(client, &data, &size, &is_text) == TURBO_OK) {
        initial_ok = is_text &&
                     control_http_response_contains(data, size,
                                                    "\"node_id\":7");
        coro_socket_free_recv(data);
        data = NULL;
    }
    if (initial_ok &&
        coro_socket_send_ws_text(client, refresh,
                                 sizeof(refresh) - 1U) == TURBO_OK &&
        coro_socket_recv_ws(client, &data, &size, &is_text) == TURBO_OK) {
        refresh_ok = is_text &&
                     control_http_response_contains(data, size,
                                                    "\"node_id\":7");
    }
    if (data != NULL) {
        coro_socket_free_recv(data);
    }
    coro_socket_destroy(client);
    return initial_ok && refresh_ok;
}

static int control_h2_status(coro_context_t *context)
{
    turbo_http_options_t options;
    turbo_http_t *client = NULL;
    http_response_t *response = NULL;
    char url[CONTROL_HTTP_URL_CAPACITY];
    int written;
    int matched = 0;

    if (turbo_http_options_init(&options, sizeof(options)) != TURBO_OK) {
        return 0;
    }
    options.transport = TURBO_HTTP_TRANSPORT_H2;
    options.follow_redirects = 0;
    if (turbo_http_create(context, &options, &client) != TURBO_OK) {
        return 0;
    }
    written = snprintf(url, sizeof(url), "http://127.0.0.1:%u%s",
                       CONTROL_HTTP_TEST_PORT,
                       TR_RAFT_CONTROL_STATUS_PATH);
    if (written > 0 && (size_t) written < sizeof(url)) {
        response = turbo_http_get(client, url);
        matched = response != NULL && response->status_code == 200 &&
                  response->body != NULL &&
                  control_http_response_contains(response->body,
                                                 response->body_len,
                                                 "STABLE");
    }
    http_response_free(response);
    turbo_http_destroy(client);
    return matched;
}

static int control_http_rpc(coro_context_t *context,
                            const char *body,
                            const char *expected)
{
    return control_http_exchange(context, "POST",
                                 TR_RAFT_CONTROL_RPC_ENDPOINT,
                                 body, expected);
}

static int control_http_send_and_disconnect(coro_context_t *context,
                                            const char *body)
{
    coro_socket_t *client;
    char request[1024];
    size_t body_size;
    int written;
    int result;

    if (context == NULL || body == NULL) {
        return 0;
    }
    client = coro_socket_create(context, CORO_SOCKET_TCP_V4);
    if (client == NULL) {
        return 0;
    }
    if (coro_socket_connect(client, "127.0.0.1",
                            CONTROL_HTTP_TEST_PORT) != TURBO_OK) {
        coro_socket_destroy(client);
        return 0;
    }
    body_size = strlen(body);
    written = snprintf(
        request, sizeof(request),
        "POST %s HTTP/1.1\r\nHost: localhost\r\n"
        "Content-Type: application/json\r\nContent-Length: %zu\r\n"
        "Connection: close\r\n\r\n%s",
        TR_RAFT_CONTROL_RPC_ENDPOINT, body_size, body);
    if (written <= 0 || (size_t)written >= sizeof(request)) {
        coro_socket_destroy(client);
        return 0;
    }
    result = coro_socket_send(client, request, (size_t)written);
    coro_socket_destroy(client);
    return result == TURBO_OK;
}

static int control_http_rpc_auth(coro_context_t *context,
                                 const char *authorization,
                                 const char *expected_status,
                                 const char *body,
                                 const char *expected)
{
    return control_http_exchange_auth(
               context, CONTROL_HTTP_AUTH_TEST_PORT, "POST",
               TR_RAFT_CONTROL_RPC_ENDPOINT, body, authorization,
               expected_status, expected, NULL, NULL, NULL) ==
           CONTROL_HTTP_RESPONSE_MATCH;
}

static int control_http_rpc_auth_redacted(
    coro_context_t *context,
    const char *authorization,
    const char *expected_status,
    const char *body,
    const char *expected,
    const char *forbidden_token,
    const char *forbidden_command,
    const char *forbidden_certificate)
{
    return control_http_exchange_auth(
        context, CONTROL_HTTP_AUTH_TEST_PORT, "POST",
        TR_RAFT_CONTROL_RPC_ENDPOINT, body, authorization,
        expected_status, expected, forbidden_token, forbidden_command,
        forbidden_certificate);
}

static void control_http_coro(coro_t *coroutine, void *argument)
{
    control_http_state_t *state = (control_http_state_t *) argument;

    (void) coroutine;
    state->server = iris_server_start(
        tr_raft_control_plane_app(state->plane), state->context,
        CONTROL_HTTP_TEST_PORT);
    if (state->server == NULL) {
        return;
    }
    coro_yield();
    coro_sleep(state->context, 50U);

    state->page_ok = control_http_exchange(
        state->context, "GET", TR_RAFT_CONTROL_UI_PATH, NULL,
        "hx-trigger=\"load, refresh, every 1s\"");
    state->controls_ok = control_http_exchange(
        state->context, "GET", TR_RAFT_CONTROL_UI_PATH, NULL,
        "hx-ext=\"raft-rpc\"");
    state->management_ok = control_http_exchange(
        state->context, "GET", TR_RAFT_CONTROL_UI_PATH, NULL,
        "data-method=\"raft.snapshot.trigger\"");
    state->operation_control_ok = control_http_exchange(
        state->context, "GET", TR_RAFT_CONTROL_UI_PATH, NULL,
        "data-method=\"raft.operation.status\"");
    state->async_control_ok = control_http_exchange(
        state->context, "GET", TR_RAFT_CONTROL_UI_PATH, NULL,
        "data-method=\"raft.propose_async\"");
    state->fragment_ok = control_http_exchange(
        state->context, "GET", TR_RAFT_CONTROL_STATUS_PATH, NULL,
        "STABLE");
    state->h2_status_ok = control_h2_status(state->context);
    state->websocket_status_ok = control_websocket_status(state->context);
    state->status_ok = control_http_rpc(
        state->context,
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"raft.status\",\"params\":{}}",
        "\"owner\":{\"configured\":true");
    state->members_ok = control_http_rpc(
        state->context,
        "{\"jsonrpc\":\"2.0\",\"id\":8,\"method\":\"raft.members\",\"params\":{}}",
        "\"node_id\":7");
    state->members_filter_ok = control_http_rpc(
        state->context,
        "{\"jsonrpc\":\"2.0\",\"id\":15,\"method\":\"raft.members\","
        "\"params\":{\"role\":\"learner\"}}",
        "\"members\":[]");
    state->progress_ok = control_http_rpc(
        state->context,
        "{\"jsonrpc\":\"2.0\",\"id\":9,\"method\":\"raft.progress\",\"params\":{}}",
        "\"recent_active\":true");
    state->progress_filter_ok = control_http_rpc(
        state->context,
        "{\"jsonrpc\":\"2.0\",\"id\":16,\"method\":\"raft.progress\","
        "\"params\":{\"node_id\":999}}",
        "\"error\"");
    state->storage_ok = control_http_rpc(
        state->context,
        "{\"jsonrpc\":\"2.0\",\"id\":10,\"method\":\"raft.storage.status\",\"params\":{}}",
        "\"audit\":{\"configured\":true,\"faulted\":false,\"dropped\":0}");
    state->batch_boundary_ok = control_http_rpc(
        state->context, control_http_boundary_batch, "\"id\":8");
    state->batch_limit_ok = control_http_rpc(
        state->context, control_http_oversized_batch,
        "Invalid batch request");
    {
        static const char boundary_request[] =
            "{\"jsonrpc\":\"2.0\",\"id\":12,"
            "\"method\":\"raft.status\",\"params\":{}}";
        size_t prefix_size = sizeof(boundary_request) - 1U;

        memcpy(control_http_boundary_rpc, boundary_request, prefix_size);
        memset(control_http_boundary_rpc + prefix_size, ' ',
               CONTROL_HTTP_RPC_MAX_REQUEST_BYTES - prefix_size);
        control_http_boundary_rpc[CONTROL_HTTP_RPC_MAX_REQUEST_BYTES] = '\0';
    }
    state->request_boundary_ok = control_http_rpc(
        state->context, control_http_boundary_rpc, "\"node_id\":7");
    memset(control_http_oversized_rpc, 'x',
           CONTROL_HTTP_RPC_MAX_REQUEST_BYTES + 1U);
    control_http_oversized_rpc[CONTROL_HTTP_RPC_MAX_REQUEST_BYTES + 1U] =
        '\0';
    state->request_limit_ok = control_http_rpc(
        state->context, control_http_oversized_rpc,
        "Request too large");
    state->tick_ok = control_http_rpc(
        state->context,
        "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"raft.tick\","
        "\"params\":{\"elapsed_ticks\":3,\"next_timeout_ticks\":4}}",
        "\"accepted\":true");
    state->leader_ok = control_http_rpc(
        state->context,
        "{\"jsonrpc\":\"2.0\",\"id\":3,\"method\":\"raft.status\",\"params\":{}}",
        "\"role\":\"leader\"");
    state->propose_ok = control_http_rpc(
        state->context,
        "{\"jsonrpc\":\"2.0\",\"id\":4,\"method\":\"raft.propose_async\","
        "\"params\":{\"command_id\":91,\"data\":\"mesh-job\"}}",
        "\"state\":\"APPLIED\"");
    state->operation_ok = control_http_rpc(
        state->context,
        "{\"jsonrpc\":\"2.0\",\"id\":11,"
        "\"method\":\"raft.operation.status\","
        "\"params\":{\"term\":1,\"index\":1}}",
        "\"state\":\"APPLIED\"");
    state->disconnect_ok = control_http_send_and_disconnect(
        state->context,
        "{\"jsonrpc\":\"2.0\",\"id\":13,"
        "\"method\":\"raft.propose_async\","
        "\"params\":{\"command_id\":92,\"data\":\"detached-job\"}}" );
    coro_sleep(state->context, 10U);
    state->disconnect_ok = state->disconnect_ok && control_http_rpc(
        state->context,
        "{\"jsonrpc\":\"2.0\",\"id\":14,"
        "\"method\":\"raft.operation.status\","
        "\"params\":{\"term\":1,\"index\":2}}",
        "\"state\":\"APPLIED\"");
    state->membership_ok = control_http_rpc(
        state->context,
        "{\"jsonrpc\":\"2.0\",\"id\":5,\"method\":\"raft.member.add_learner\","
        "\"params\":{\"transition_id\":101,\"node_id\":8}}",
        "\"state\":\"APPLIED\"");
    state->read_index_ok = control_http_rpc(
        state->context,
        "{\"jsonrpc\":\"2.0\",\"id\":6,\"method\":\"raft.read_index\","
        "\"params\":{\"context_id\":33}}",
        "\"accepted\":true");
    state->take_read_state_ok = control_http_rpc(
        state->context,
        "{\"jsonrpc\":\"2.0\",\"id\":7,\"method\":\"raft.takeReadState\","
        "\"params\":{}}",
        "\"context_id\":33");

    state->server_stopped = 1;
    (void)tr_raft_service_owner_stop(state->owner);
    coro_socket_destroy(state->server);
    state->server = NULL;
}

static void control_http_auth_coro(coro_t *coroutine, void *argument)
{
    static const char tick_request[] =
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"raft.tick\","
        "\"params\":{\"elapsed_ticks\":3,\"next_timeout_ticks\":4}}";
    static const char propose_request[] =
        "{\"jsonrpc\":\"2.0\",\"id\":2,"
        "\"method\":\"raft.propose_async\","
        "\"params\":{\"command_id\":1,\"data\":\"before-election\"}}";
    control_http_state_t *state = (control_http_state_t *) argument;

    (void) coroutine;
    state->server = iris_server_start(
        tr_raft_control_plane_app(state->plane), state->context,
        CONTROL_HTTP_AUTH_TEST_PORT);
    if (state->server == NULL) {
        return;
    }
    coro_yield();
    coro_sleep(state->context, 50U);
    state->auth_rejected = control_http_rpc_auth(
        state->context, NULL, "HTTP/1.1 401 Unauthorized",
        tick_request, "Authentication required");
    state->leader_hint_ok = control_http_rpc_auth(
        state->context, state->authorization, "HTTP/1.1 200 OK",
        propose_request, "NOT_LEADER; leader_id=0");
    state->token_redaction_ok = control_http_rpc_auth_redacted(
        state->context, state->authorization, "HTTP/1.1 200 OK",
        propose_request, "NOT_LEADER; leader_id=0", state->bearer_token,
        NULL, NULL);
    state->command_redaction_ok = control_http_rpc_auth_redacted(
        state->context, state->authorization, "HTTP/1.1 200 OK",
        propose_request, "NOT_LEADER; leader_id=0", NULL,
        "before-election", NULL);
    state->certificate_redaction_ok = control_http_rpc_auth_redacted(
        state->context, state->authorization_with_certificate,
        "HTTP/1.1 200 OK", propose_request,
        "NOT_LEADER; leader_id=0", NULL, NULL,
        "sensitive-certificate");
    state->auth_allowed = control_http_rpc_auth(
        state->context, state->authorization, "HTTP/1.1 200 OK",
        tick_request, "\"accepted\":true");
    state->server_stopped = 1;
    coro_socket_destroy(state->server);
    state->server = NULL;
}

static void control_http_audit_failure_coro(coro_t *coroutine, void *argument)
{
    static const char tick_request[] =
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"raft.tick\","
        "\"params\":{\"elapsed_ticks\":3,\"next_timeout_ticks\":4}}";
    control_http_state_t *state = (control_http_state_t *)argument;

    (void)coroutine;
    state->server = iris_server_start(
        tr_raft_control_plane_app(state->plane), state->context,
        CONTROL_HTTP_TEST_PORT);
    if (state->server == NULL) {
        return;
    }
    coro_yield();
    coro_sleep(state->context, 50U);
    state->audit_failure_rejected = control_http_rpc(
        state->context, tick_request, "AUDIT_FAULTED");
    state->audit_failure_sticky = control_http_rpc(
        state->context, tick_request, "AUDIT_FAULTED");
    state->audit_status_ok = control_http_rpc(
        state->context,
        "{\"jsonrpc\":\"2.0\",\"id\":3,"
        "\"method\":\"raft.storage.status\",\"params\":{}}",
        "\"audit\":{\"configured\":true,\"faulted\":true");
    state->server_stopped = 1;
    coro_socket_destroy(state->server);
    state->server = NULL;
}

static void control_http_drain(coro_context_t *context)
{
    uint64_t deadline;

    if (context == NULL) {
        return;
    }
    deadline = turbo_monotonic_ms() + CONTROL_HTTP_DRAIN_TIMEOUT_MS;
    while (coro_context_alive(context) &&
           turbo_monotonic_ms() < deadline) {
        coro_context_run(context, TURBO_RUN_ONCE);
    }
}

spec("raft control plane HTTP integration")
{
    it("serves HTMX status and executes JSON-RPC commands on one owner-loop")
    {
        control_http_state_t state = {0};
        tr_raft_control_plane_config_t config;
        tr_raft_control_audit_config_t audit_config =
            TR_RAFT_CONTROL_AUDIT_CONFIG_INIT;
        tr_raft_service_owner_config_t owner_config;
        coro_object_pool_config_t pool_config;

        iris_app_reset_default();
        reset_router();
        iris_error_recovery_init();
        check_equal(control_http_create_service(&state), TURBO_OK);
        memset(&pool_config, 0, sizeof(pool_config));
        pool_config.initial_capacity = 8U;
        state.context = coro_context_create_ex(NULL, &pool_config);
        check_not_null(state.context);
        memset(&owner_config, 0, sizeof(owner_config));
        owner_config.service = state.service;
        owner_config.context = state.context;
        owner_config.tick_interval_ms = 10U;
        owner_config.elapsed_ticks = 1U;
        owner_config.max_pending_commands = 8U;
        owner_config.max_command_bytes = 1024U;
        owner_config.next_election_timeout = control_http_timeout;
        check_equal(tr_raft_service_owner_create(&owner_config, &state.owner),
                     TURBO_OK);
        check_equal(tr_raft_service_owner_start(state.owner), TURBO_OK);
        state.audit_valid = 1;
        audit_config.sink = control_http_audit_sink;
        audit_config.context = &state;
        audit_config.required = 1U;
        check_equal(tr_raft_control_audit_create(&audit_config, &state.audit),
                     TURBO_OK);
        memset(&config, 0, sizeof(config));
        config.service = state.service;
        config.owner = state.owner;
        config.audit = state.audit;
        config.allow_unauthenticated_mutations = true;
        state.app = iris_app_create();
        check_not_null(state.app);
        config.app = state.app;
        check_equal(tr_raft_control_plane_create(&config, &state.plane),
                     TURBO_OK);
        init_router();
        check_equal(coro_context_spawn(state.context, control_http_coro,
                                        &state), TURBO_OK);
        coro_context_run(state.context, TURBO_RUN_DEFAULT);

        check(state.server_stopped);
        check(state.page_ok);
        check(state.controls_ok);
        check(state.management_ok);
        check(state.operation_control_ok);
        check(state.async_control_ok);
        check(state.fragment_ok);
        check(state.h2_status_ok);
        check(state.websocket_status_ok);
        check(state.status_ok);
        check(state.members_ok);
        check(state.members_filter_ok);
        check(state.progress_ok);
        check(state.progress_filter_ok);
        check(state.storage_ok);
        check(state.batch_boundary_ok);
        check(state.batch_limit_ok);
        check(state.request_boundary_ok);
        check(state.request_limit_ok);
        check(state.tick_ok);
        check(state.leader_ok);
        check(state.propose_ok);
        check(state.operation_ok);
        check(state.disconnect_ok);
        check(state.membership_ok);
        check(state.read_index_ok);
        check(state.take_read_state_ok);
        check_equal(state.apply_count, 2U);
        check(state.audit_valid);
        check_equal(state.audit_authorization_count, 5U);
        check_equal(state.audit_completion_count, 5U);
        check_equal(state.audit_receipt_count, 3U);

        if (!state.server_stopped && state.server != NULL) {
            coro_socket_destroy(state.server);
        }
        tr_raft_control_plane_destroy(state.plane);
        iris_app_destroy(state.app);
        tr_raft_control_audit_destroy(state.audit);
        check_equal(tr_raft_service_owner_close(state.owner), TURBO_OK);
        tr_raft_service_destroy(state.service);
        control_http_drain(state.context);
        coro_context_destroy(state.context);
        reset_router();
        iris_app_reset_default();
        iris_error_recovery_cleanup();
    }

    it("rejects unauthenticated mutation RPC and accepts a valid JWT")
    {
        static const char secret[] = "turboraft-control-test-secret";
        control_http_state_t state = {0};
        tr_raft_control_plane_config_t config;
        char *token;

        iris_app_reset_default();
        reset_router();
        iris_error_recovery_init();
        check_equal(control_http_create_service(&state), TURBO_OK);
        memset(&config, 0, sizeof(config));
        config.service = state.service;
        check_equal(tr_raft_control_plane_create(&config, &state.plane),
                     TURBO_OK);
        token = iris_jwt_encode(secret, "{\"sub\":\"raft-admin\"}");
        check_not_null(token);
        check(snprintf(state.bearer_token, sizeof(state.bearer_token),
                       "%s", token) > 0);
        check(snprintf(state.authorization, sizeof(state.authorization),
                       "Authorization: Bearer %s\r\n", token) > 0);
        check(snprintf(state.authorization_with_certificate,
                       sizeof(state.authorization_with_certificate),
                       "Authorization: Bearer %s\r\n"
                       "X-Test-Client-Certificate: sensitive-certificate\r\n",
                       token) > 0);
        iris_jwt_set_secret(secret);
        iris_app_hook(tr_raft_control_plane_app(state.plane),
                      iris_jwt_context_middleware);
        state.context = coro_context_create(NULL);
        check_not_null(state.context);
        init_router();
        check_equal(coro_context_spawn(state.context,
                                        control_http_auth_coro, &state),
                     TURBO_OK);
        coro_context_run(state.context, TURBO_RUN_DEFAULT);

        check(state.server_stopped);
        check(state.auth_rejected);
        check(state.leader_hint_ok);
        check_equal(state.token_redaction_ok, CONTROL_HTTP_RESPONSE_MATCH);
        check_equal(state.command_redaction_ok, CONTROL_HTTP_RESPONSE_MATCH);
        check_equal(state.certificate_redaction_ok,
                     CONTROL_HTTP_RESPONSE_MATCH);
        check(state.auth_allowed);

        free(token);
        iris_jwt_set_secret(NULL);
        tr_raft_control_plane_destroy(state.plane);
        tr_raft_service_destroy(state.service);
        control_http_drain(state.context);
        coro_context_destroy(state.context);
        reset_router();
        iris_app_reset_default();
        iris_error_recovery_cleanup();
    }

    it("fails closed and latches a required audit sink failure")
    {
        control_http_state_t state = {0};
        tr_raft_control_plane_config_t config;
        tr_raft_control_audit_config_t audit_config =
            TR_RAFT_CONTROL_AUDIT_CONFIG_INIT;
        tr_raft_service_status_t status;

        iris_app_reset_default();
        reset_router();
        iris_error_recovery_init();
        check_equal(control_http_create_service(&state), TURBO_OK);
        state.audit_valid = 1;
        state.audit_sink_result = TURBO_EINVAL;
        audit_config.sink = control_http_audit_sink;
        audit_config.context = &state;
        audit_config.required = 1U;
        check_equal(tr_raft_control_audit_create(&audit_config, &state.audit),
                     TURBO_OK);
        memset(&config, 0, sizeof(config));
        config.service = state.service;
        config.audit = state.audit;
        config.allow_unauthenticated_mutations = true;
        check_equal(tr_raft_control_plane_create(&config, &state.plane),
                     TURBO_OK);
        state.context = coro_context_create(NULL);
        check_not_null(state.context);
        init_router();
        check_equal(coro_context_spawn(state.context,
                                        control_http_audit_failure_coro,
                                        &state), TURBO_OK);
        coro_context_run(state.context, TURBO_RUN_DEFAULT);

        check(state.server_stopped);
        check(state.audit_failure_rejected);
        check(state.audit_failure_sticky);
        check(state.audit_status_ok);
        check(state.audit_valid);
        check_equal(state.audit_authorization_count, 1U);
        check_equal(state.audit_completion_count, 0U);
        check_equal(state.apply_count, 0U);
        check_equal(tr_raft_service_status(state.service, &status), TURBO_OK);
        check_equal(status.core.role, TR_RAFT_FOLLOWER);
        check_equal(status.core.term, 0U);

        tr_raft_control_plane_destroy(state.plane);
        tr_raft_control_audit_destroy(state.audit);
        tr_raft_service_destroy(state.service);
        control_http_drain(state.context);
        coro_context_destroy(state.context);
        reset_router();
        iris_app_reset_default();
        iris_error_recovery_cleanup();
    }
}
