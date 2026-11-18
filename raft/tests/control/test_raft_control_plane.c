#include <turboraft/raft_control_plane.h>

#include <core/error.h>
#include <http_client/http.h>
#include <json_parser.h>
#include <tinytest.h>

#include <stdatomic.h>
#include <stdio.h>
#include <string.h>

static native_io_backend_kind control_backend(void)
{
#if defined(_WIN32)
    return NATIVE_IO_BACKEND_IOCP;
#elif defined(__linux__)
    return NATIVE_IO_BACKEND_EPOLL;
#else
    return NATIVE_IO_BACKEND_KQUEUE;
#endif
}

static chttp_application_config control_server_config(void)
{
    chttp_application_config config;

    config = chttp_application_config_default();
    config.http.host = "127.0.0.1";
    config.http.port = 0U;
    config.http.backlog = 8U;
    config.http.network.backend = control_backend();
    config.http.network.connection_capacity = 8U;
    config.http.network.command_capacity = 32U;
    config.http.network.request_capacity = 16U;
    config.http.network.completion_batch_capacity = 8U;
    config.http.network.event_capacity = 32U;
    config.http.network.max_send_bytes = 64U * 1024U;
    config.http.network.receive_buffer_bytes = 4096U;
    config.http.network.connect_timeout_ms = 1000U;
    config.http.network.read_timeout_ms = 1000U;
    config.http.network.write_timeout_ms = 1000U;
    config.http.route_capacity = 8U;
    config.http.middleware_capacity = 4U;
    config.http.max_route_middleware_count = 4U;
    config.http.max_route_param_count = 4U;
    config.http.max_route_param_bytes = 128U;
    config.http.max_target_bytes = 256U;
    config.http.max_header_count = 16U;
    config.http.max_header_bytes = 4096U;
    config.http.max_request_body_bytes = 8192U;
    config.http.max_response_header_count = 16U;
    config.http.max_response_header_bytes = 4096U;
    config.http.max_response_body_bytes = 8192U;
    config.http.poll_slice_ms = 2U;
    config.http.max_buffered_response_body_bytes = 8192U;
    config.rpc.method_capacity = 8U;
    config.rpc.max_method_bytes = 64U;
    config.rpc.max_json_depth = 8U;
    config.rpc.max_batch_items = 4U;
    config.bindings.method_capacity = 1U;
    return config;
}

typedef struct control_provider {
    tr_raft_service_status_t status;
    atomic_bool unavailable;
} control_provider_t;

static struct {
    tr_raft_control_plane_config_t config[2];
    tr_raft_control_plane_t *planes[2];
    control_provider_t providers[2];
    chttp_client client;
    chttp_response response;
} fixture;

static int control_status(void *context, tr_raft_service_status_t *out_status)
{
    control_provider_t *provider = (control_provider_t *)context;
    if (atomic_load(&provider->unavailable)) return SALTS_EIO;
    *out_status = provider->status;
    return SALTS_OK;
}

static int control_start(size_t index)
{
    int result = tr_raft_control_plane_create(
        &fixture.config[index], &fixture.planes[index]);
    if (result != SALTS_OK) return result;
    return tr_raft_control_plane_start(fixture.planes[index]);
}

static int control_request(size_t index, const char *json)
{
    static const chttp_header headers[] = {
        {"Content-Type", "application/json"}};
    chttp_options options = {0};
    chttp_error error = {0};
    char uri[64];
    uint16_t port = 0U;
    int result = tr_raft_control_plane_port(fixture.planes[index], &port);
    if (result != SALTS_OK) return result;
    chttp_response_destroy(&fixture.response);
    snprintf(uri, sizeof(uri), "tcp://127.0.0.1:%u", (unsigned)port);
    options.connection_uri = uri;
    options.authority = "127.0.0.1";
    options.target = json != NULL ? TR_RAFT_CONTROL_RPC_ENDPOINT
                                  : TR_RAFT_CONTROL_STATUS_PATH;
    options.timeout_ms = 5000U;
    if (json == NULL) {
        return chttp_get(&fixture.client, &options, &fixture.response, &error);
    }
    options.headers = headers;
    options.header_count = sizeof(headers) / sizeof(headers[0]);
    options.body = json;
    options.body_size = strlen(json);
    return chttp_post(&fixture.client, &options, &fixture.response, &error);
}

static void control_body_contains(const char *expected)
{
    char text[8192];
    check(fixture.response.body_size < sizeof(text));
    memcpy(text, fixture.response.body, fixture.response.body_size);
    text[fixture.response.body_size] = '\0';
    check_not_null(strstr(text, expected));
}

spec("Raft CHTTP Application control plane")
{
    before_each()
    {
        chttp_client_config client = {0};
        size_t i;
        memset(&fixture, 0, sizeof(fixture));
        for (i = 0U; i < 2U; ++i) {
            tr_raft_service_status_t *status = &fixture.providers[i].status;
            atomic_init(&fixture.providers[i].unavailable, false);
            status->core.self_id = 7U + i;
            status->core.leader_id = status->core.self_id;
            status->core.role = TR_RAFT_LEADER;
            status->core.term = 3U;
            status->core.commit_index = UINT64_MAX;
            status->core.applied_index = UINT64_MAX - 1U;
            status->core.voter_count = 3U;
            fixture.config[i].server = control_server_config();
            fixture.config[i].status_provider = control_status;
            fixture.config[i].status_context = &fixture.providers[i];
            fixture.config[i].stop_timeout_ms = 5000U;
        }
        client.network = fixture.config[0].server.http.network;
        client.request_capacity = 2U;
        client.max_start_line_bytes = 256U;
        client.max_header_count = 16U;
        client.max_header_bytes = 4096U;
        client.max_request_body_bytes = 8192U;
        client.max_response_body_bytes = 8192U;
        client.max_informational_responses = 4U;
        check_equal(chttp_client_init(&fixture.client, &client), SALTS_OK);
    }

    after_each()
    {
        size_t i;
        chttp_response_destroy(&fixture.response);
        for (i = 0U; i < 2U; ++i) {
            if (fixture.planes[i] != NULL) {
                check_warn(tr_raft_control_plane_stop(fixture.planes[i]) == SALTS_OK);
                check_warn(tr_raft_control_plane_destroy(fixture.planes[i]) == SALTS_OK);
            }
        }
        check_equal(chttp_client_destroy(&fixture.client, 5000U), SALTS_OK);
    }

    it("renders status and preserves explicit stop, restart and destroy")
    {
        char output[4096];
        size_t size = 0U;
        uint16_t port = 0U;
        check_equal(tr_raft_control_plane_create(
            &fixture.config[0], &fixture.planes[0]), SALTS_OK);
        check_equal(tr_raft_control_plane_port(fixture.planes[0], &port), SALTS_EINVAL);
        check_equal(tr_raft_control_plane_render_status_json(
            fixture.planes[0], output, sizeof(output), &size), SALTS_OK);
        check_not_null(strstr(output, "\"node_id\":7"));
        check_not_null(strstr(output, "\"role\":\"leader\""));
        check_equal(tr_raft_control_plane_render_status_html(
            fixture.planes[0], output, sizeof(output), &size), SALTS_OK);
        check_not_null(strstr(output, "LEADER"));
        check_equal(tr_raft_control_plane_render_status_json(
            fixture.planes[0], output, 1U, &size), SALTS_ENOSPC);
        check_equal(size, 0U);
        check_equal(tr_raft_control_plane_start(fixture.planes[0]), SALTS_OK);
        check_equal(tr_raft_control_plane_start(fixture.planes[0]), SALTS_EALREADY);
        check_equal(tr_raft_control_plane_destroy(fixture.planes[0]), SALTS_EBUSY);
        check_equal(tr_raft_control_plane_stop(fixture.planes[0]), SALTS_OK);
        check_equal(tr_raft_control_plane_start(fixture.planes[0]), SALTS_OK);
        check_equal(control_request(0U, NULL), SALTS_OK);
        check_equal(fixture.response.status_code, 200U);
    }

    it("serves HTTP and JSON-RPC with exact 64-bit status values")
    {
        check_equal(control_start(0U), SALTS_OK);
        check_equal(control_request(0U, NULL), SALTS_OK);
        check_equal(fixture.response.status_code, 200U);
        control_body_contains("\"node_id\":7");
        control_body_contains("\"commit_index\":18446744073709551615");
        check_equal(control_request(0U,
            "{\"jsonrpc\":\"2.0\",\"method\":\"raft.status\",\"id\":42}"), SALTS_OK);
        check_equal(fixture.response.status_code, 200U);
        control_body_contains("\"id\":42");
        json_value_t *reply = json_parse(fixture.response.body, fixture.response.body_size);
        check_not_null(reply);
        const json_value_t *result = json_object_get(reply, "result");
        check_equal(json_object_size(result), 7U);
        check_equal(json_string(json_object_get(result, "role")), "leader");
        json_free(reply);
        control_body_contains("\"applied_index\":18446744073709551614");
    }

    it("retains protocol errors, notifications and status-unavailable responses")
    {
        check_equal(control_start(0U), SALTS_OK);
        check_equal(control_request(0U,
            "{\"jsonrpc\":\"2.0\",\"method\":\"missing\",\"id\":1}"), SALTS_OK);
        control_body_contains("\"code\":-32601");
        check_equal(control_request(0U, "{"), SALTS_OK);
        control_body_contains("\"code\":-32700");
        check_equal(control_request(0U,
            "{\"jsonrpc\":\"2.0\",\"method\":\"raft.status\"}"), SALTS_OK);
        check_equal(fixture.response.status_code, 204U);
        atomic_store(&fixture.providers[0].unavailable, true);
        check_equal(control_request(0U, NULL), SALTS_OK);
        check_equal(fixture.response.status_code, 503U);
        control_body_contains("Raft status unavailable");
        check_equal(control_request(0U,
            "{\"jsonrpc\":\"2.0\",\"method\":\"raft.status\",\"id\":2}"), SALTS_OK);
        control_body_contains("\"code\":-32001");
        control_body_contains("Raft status unavailable");
    }

    it("binds each Application to its own status provider")
    {
        check_equal(control_start(0U), SALTS_OK);
        check_equal(control_start(1U), SALTS_OK);
        check_equal(control_request(0U,
            "{\"jsonrpc\":\"2.0\",\"method\":\"raft.status\",\"id\":1}"), SALTS_OK);
        control_body_contains("\"node_id\":7");
        check_equal(control_request(1U,
            "{\"jsonrpc\":\"2.0\",\"method\":\"raft.status\",\"id\":2}"), SALTS_OK);
        control_body_contains("\"node_id\":8");
    }
}
