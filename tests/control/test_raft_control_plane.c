#include <turboraft/raft_control_plane.h>

#include <salts_error.h>
#include <tinytest.h>

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

static crpc_server_config control_server_config(void)
{
    crpc_server_config config;

    memset(&config, 0, sizeof(config));
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
    config.method_capacity = 8U;
    config.max_method_bytes = 64U;
    config.max_json_depth = 8U;
    config.max_batch_items = 4U;
    return config;
}

static int control_status(void *context,
                          tr_raft_service_status_t *out_status)
{
    const tr_raft_service_status_t *status =
        (const tr_raft_service_status_t *)context;

    *out_status = *status;
    return SALTS_OK;
}

spec("Raft CHTTP/CRPC control plane")
{
    it("renders status and owns an explicit server lifecycle")
    {
        tr_raft_service_status_t status;
        tr_raft_control_plane_config_t config;
        tr_raft_control_plane_t *plane = NULL;
        char output[4096];
        size_t size = 0U;
        uint16_t port = 0U;

        memset(&status, 0, sizeof(status));
        memset(&config, 0, sizeof(config));
        status.core.self_id = 7U;
        status.core.leader_id = 7U;
        status.core.role = TR_RAFT_LEADER;
        status.core.term = 3U;
        status.core.commit_index = 11U;
        status.core.applied_index = 10U;
        status.core.voter_count = 3U;
        config.server = control_server_config();
        config.status_provider = control_status;
        config.status_context = &status;
        config.stop_timeout_ms = 5000U;

        check_equal(tr_raft_control_plane_create(&config, &plane), SALTS_OK);
        check_not_null(tr_raft_control_plane_http(plane));
        check_equal(tr_raft_control_plane_render_status_json(
                        plane, output, sizeof(output), &size),
                    SALTS_OK);
        check_not_null(strstr(output, "\"node_id\":7"));
        check_not_null(strstr(output, "\"role\":\"leader\""));
        check_equal(tr_raft_control_plane_render_status_html(
                        plane, output, sizeof(output), &size),
                    SALTS_OK);
        check_not_null(strstr(output, "LEADER"));

        check_equal(tr_raft_control_plane_start(plane), SALTS_OK);
        check_equal(tr_raft_control_plane_port(plane, &port), SALTS_OK);
        check(port != 0U);
        check_equal(tr_raft_control_plane_destroy(plane), SALTS_EBUSY);
        check_equal(tr_raft_control_plane_stop(plane), SALTS_OK);
        check_equal(tr_raft_control_plane_destroy(plane), SALTS_OK);
    }
}
