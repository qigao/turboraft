#include <turboraft/raft_control_plane.h>

#include <iris/iris_app.h>
#include <iris/rpc_server.h>
#include <tinytest.h>
#include <turbo_error.h>

#include <string.h>

static int control_ok(void *context)
{
    (void) context;
    return TURBO_OK;
}

static int control_hard_state(void *context,
                              tr_raft_term_t term,
                              tr_raft_node_id_t voted_for)
{
    (void) context;
    (void) term;
    (void) voted_for;
    return TURBO_OK;
}

static int control_truncate(void *context, tr_raft_index_t index)
{
    (void) context;
    (void) index;
    return TURBO_OK;
}

static int control_append(void *context,
                          const tr_raft_entry_t *entries,
                          size_t count)
{
    (void) context;
    return entries != NULL && count != 0U ? TURBO_OK : TURBO_EINVAL;
}

static int control_commit_index(void *context, tr_raft_index_t index)
{
    (void) context;
    (void) index;
    return TURBO_OK;
}

static int control_enqueue(void *context, const tr_raft_message_t *message)
{
    (void) context;
    return message == NULL ? TURBO_EINVAL : TURBO_OK;
}

static int control_apply(void *context,
                         const tr_raft_entry_t *entries,
                         size_t count)
{
    (void) context;
    return entries != NULL && count != 0U ? TURBO_OK : TURBO_EINVAL;
}

static tr_raft_service_t *control_create_service(void)
{
    static const tr_raft_node_id_t voters[] = {7U};
    tr_raft_service_config_t config;
    tr_raft_service_t *service = NULL;

    memset(&config, 0, sizeof(config));
    config.core.self_id = 7U;
    config.core.voters = voters;
    config.core.voter_count = 1U;
    config.core.heartbeat_ticks = 1U;
    config.core.election_min_ticks = 3U;
    config.core.election_max_ticks = 5U;
    config.core.initial_election_timeout_ticks = 3U;
    config.core.max_log_entries = 16U;
    config.storage.begin = control_ok;
    config.storage.write_hard_state = control_hard_state;
    config.storage.truncate_log = control_truncate;
    config.storage.append_log = control_append;
    config.storage.write_commit_index = control_commit_index;
    config.storage.commit = control_ok;
    config.storage.rollback = control_ok;
    config.transport.enqueue = control_enqueue;
    config.state_machine.apply_batch = control_apply;
    check_equal(tr_raft_service_create(&config, &service), TURBO_OK);
    return service;
}

spec("raft TurboHTTP control plane")
{
    it("binds typed RPC methods and renders bounded HTMX status")
    {
        tr_raft_service_t *service;
        tr_raft_control_plane_config_t config;
        tr_raft_control_plane_t *plane = NULL;
        iris_app_t *app;
        rpc_context_t *rpc;
        char output[4096];
        size_t size;

        app = iris_app_create();
        check_not_null(app);
        service = control_create_service();
        memset(&config, 0, sizeof(config));
        config.service = service;
        config.app = app;
        check_equal(tr_raft_control_plane_create(&config, &plane), TURBO_OK);
        check_not_null(plane);
        check(tr_raft_control_plane_app(plane) == app);
        rpc = (rpc_context_t *) iris_app_lookup_rpc_context(
            app, TR_RAFT_CONTROL_RPC_ENDPOINT);
        check_not_null(rpc);
        check_equal(rpc->method_count, 19U);
        check_equal(rpc->config.max_response_size, 64U * 1024U);
        check(!rpc->methods[0].requires_auth);
        check(!rpc->methods[1].requires_auth);
        check(!rpc->methods[2].requires_auth);
        check(!rpc->methods[3].requires_auth);
        {
            size_t method_index;

            for (method_index = 4U; method_index < 18U; ++method_index) {
                check(rpc->methods[method_index].requires_auth);
            }
        }

        check_equal(tr_raft_control_plane_render_status_json(
                         plane, output, sizeof(output), &size), TURBO_OK);
        check(size > 0U);
        check_not_null(strstr(output, "\"node_id\":7"));
        check_not_null(strstr(output, "\"role\":\"follower\""));
        check_not_null(strstr(output, "\"log_base_index\":0"));
        check_not_null(strstr(output,
                              "\"snapshot_required_peer_count\":0"));
        check_not_null(strstr(output, "\"owner\":{\"configured\":false"));
        check_equal(tr_raft_control_plane_render_status_html(
                         plane, output, sizeof(output), &size), TURBO_OK);
        check(size > 0U);
        check_not_null(strstr(output, "7"));
        check_not_null(strstr(output, "STABLE"));
        check_not_null(strstr(output, "LEGACY"));

        tr_raft_control_plane_destroy(plane);
        iris_app_destroy(app);
        tr_raft_service_destroy(service);
    }
}
