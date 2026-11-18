#include <turboraft/raft_control_plane.h>

#include "turboraft_control.h"

#include <cmeta/data_reflect.h>
#include <core/error.h>
#include <core/tlog.h>
#include <core/tstr.h>

#include <inttypes.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum { TR_RAFT_CONTROL_RENDER_CAPACITY = 8192 };

struct tr_raft_control_plane {
    chttp_application application;
    chttp_application_config configuration;
    salts_configurator_provider_binding provider;
    salts_configurator_deployment *components;
    chttp_controller *controllers;
    DataBind *codec;
    DataBindRpcMethodPlan *rpc_plan;
    DataBindNativeTypeBinding request_binding;
    DataBindNativeTypeBinding response_binding;
    DataBindServiceNativeBinding native_binding;
    DataBindNativeExecution execution;
    chttp_rpc_method rpc_method;
    tr_raft_control_status_provider_fn status_provider;
    void *status_context;
    uint32_t stop_timeout_ms;
    int started;
};

cmeta_reflect_data(tr_raft_control_plane_t, "turboraft.ControlPlane",
                   cmeta_field(int, started));
cmeta_component_empty(TurboRaftControlComponent);

typedef struct tr_control_rpc_error {
    uint32_t kind;
    StatusUnavailable_t payload;
} tr_control_rpc_error_t;

static const cmeta_type_desc tr_control_request_pointer = {
    "StatusRequest_t *", sizeof(StatusRequest_t *),
    _Alignof(StatusRequest_t *), CMETA_T_POINTER,
    &StatusRequest_CMETA_TYPE, NULL, NULL};
static const cmeta_type_desc tr_control_response_pointer = {
    "StatusResponse_t *", sizeof(StatusResponse_t *),
    _Alignof(StatusResponse_t *), CMETA_T_POINTER,
    &StatusResponse_CMETA_TYPE, NULL, NULL};
static const cmeta_type_identity tr_control_error_identity =
    CMETA_TYPE_ID_ATOM_INIT("turboraft.ControlStatusError");
static const cmeta_type_desc tr_control_error_type = {
    "tr_control_rpc_error_t", sizeof(tr_control_rpc_error_t),
    _Alignof(tr_control_rpc_error_t), CMETA_T_OBJECT,
    NULL, NULL, &tr_control_error_identity};
static const cmeta_type_desc tr_control_error_pointer = {
    "tr_control_rpc_error_t *", sizeof(tr_control_rpc_error_t *),
    _Alignof(tr_control_rpc_error_t *), CMETA_T_POINTER,
    &tr_control_error_type, NULL, NULL};

CMETA_FUNCTION_METADATA_AS_ABI_RESULT(
    tr_control_rpc, "raft.status", fallible, &cmeta_type_int,
    CMETA_ABI_SCALAR, CMETA_RESULT_VALUE,
    (StatusRequest_t *, request, CMETA_PARAM_IN | CMETA_PARAM_BORROWED,
     &tr_control_request_pointer, CMETA_ABI_OBJECT_POINTER),
    (StatusResponse_t *, response, CMETA_PARAM_OUT | CMETA_PARAM_BORROWED,
     &tr_control_response_pointer, CMETA_ABI_OBJECT_POINTER),
    (tr_control_rpc_error_t *, error, CMETA_PARAM_OUT | CMETA_PARAM_BORROWED,
     &tr_control_error_pointer, CMETA_ABI_OBJECT_POINTER));

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

static cmeta_status tr_control_controller_create(
    void *context, const cmeta_data_desc *data, const void *configuration,
    const salts_configurator_dependency *dependencies, size_t count,
    cmeta_object_ref *out)
{
    (void)data;
    (void)configuration;
    (void)dependencies;
    (void)count;
    return cmeta_object_borrow(
        out, context, cmeta_reflected_data(tr_raft_control_plane_t), NULL);
}

/* App binds context to the canonical Controller; DTO slots remain callback-local. */
static bool DATA_BIND_NATIVE_CALL tr_control_rpc_invoke(
    void *context, void *return_storage, void *const *params, size_t count)
{
    tr_raft_control_plane_t *plane = (tr_raft_control_plane_t *)context;
    RaftStatus_t *response;
    tr_control_rpc_error_t *error;
    tr_raft_service_status_t status;
    int result;
    (void)count;
    response = &((StatusResponse_t *)params[1])->status;
    error = (tr_control_rpc_error_t *)params[2];
    result = tr_control_status(plane, &status);
    if (result != SALTS_OK) {
        error->payload.reason = tstr_dup("Raft status unavailable");
        if (error->payload.reason == NULL) {
            *(int *)return_storage = SALTS_ENOMEM;
        } else {
            error->kind = 1U;
            *(int *)return_storage = SALTS_OK;
        }
        return true;
    }
    response->node_id = status.core.self_id;
    response->leader_id = status.core.leader_id;
    response->term = status.core.term;
    response->commit_index = status.core.commit_index;
    response->applied_index = status.core.applied_index;
    response->faulted = status.faulted;
    response->role = tstr_dup(tr_control_role_name(status.core.role));
    *(int *)return_storage =
        response->role != NULL ? SALTS_OK : SALTS_ENOMEM;
    return true;
}

CHTTP_ACTION(tr_control_http_status);
int tr_control_http_status(void *self, const chttp_invocation *invocation)
{
    tr_raft_control_plane_t *plane = (tr_raft_control_plane_t *)self;
    char output[TR_RAFT_CONTROL_RENDER_CAPACITY];
    size_t size = 0U;
    int result = tr_raft_control_plane_render_status_json(
        plane, output, sizeof(output), &size);
    if (result != SALTS_OK) {
        static const char unavailable[] = "{\"error\":\"Raft status unavailable\"}";
        return chttp_invocation_respond(invocation, 503U, "application/json",
                                       unavailable, sizeof(unavailable) - 1U);
    }
    return chttp_invocation_respond(
        invocation, 200U, "application/json", output, size);
}

static int tr_control_bind_failure(const char *phase, DataBindStatus status,
                                   const char *message)
{
    TLOG_ERRORF("ControlPlane RPC {} failed: DataBind {}, {}",
                phase, (int)status, message);
    if (status == DATA_BIND_ERR_OOM) return SALTS_ENOMEM;
    if (status == DATA_BIND_ERR_LIMIT) return SALTS_ENOBUFS;
    return SALTS_EPROTO;
}

static int tr_control_prepare_rpc(tr_raft_control_plane_t *plane)
{
    static const DataBindRpcFieldProjection status_result = {
        sizeof(DataBindRpcFieldProjection), DATA_BIND_BINDING_EGRESS,
        "status", "status", 0U};
    static const DataBindRpcErrorMapping errors[] = {
        {sizeof(DataBindRpcErrorMapping), "StatusUnavailable", -32001}};
    static const DataBindNativeErrorBinding native_errors[] = {
        {sizeof(DataBindNativeErrorBinding), "StatusUnavailable", 1U,
         StatusUnavailable_cmeta_data, offsetof(tr_control_rpc_error_t, payload)}};
    DataBindError error = DATA_BIND_ERROR_INIT;
    DataBindBindingPlanDiagnostic diagnostic =
        DATA_BIND_BINDING_PLAN_DIAGNOSTIC_INIT;
    DataBindRpcProjectionConfig projection = DATA_BIND_RPC_PROJECTION_CONFIG_INIT;
    DataBindStatus result = RaftControl_codec_create(&plane->codec, &error);
    if (result != DATA_BIND_OK) {
        return tr_control_bind_failure("codec", result, error.message);
    }
    result = StatusRequest_native_artifact()->native_binding(
        &plane->request_binding, &error);
    if (result == DATA_BIND_OK) {
        result = StatusResponse_native_artifact()->native_binding(
            &plane->response_binding, &error);
    }
    if (result != DATA_BIND_OK) {
        return tr_control_bind_failure("DTO binding", result, error.message);
    }
    plane->native_binding = (DataBindServiceNativeBinding)
        DATA_BIND_SERVICE_NATIVE_BINDING_INIT(
            &tr_control_rpc__function_meta,
            &plane->request_binding, &plane->response_binding);
    plane->native_binding.errors = native_errors;
    plane->native_binding.error_count = 1U;
    plane->native_binding.error_param_index = 2U;
    plane->native_binding.error_envelope_bytes = sizeof(tr_control_rpc_error_t);
    plane->native_binding.error_kind_offset = offsetof(tr_control_rpc_error_t, kind);
    plane->native_binding.error_kind_bytes = sizeof(uint32_t);
    plane->execution = (DataBindNativeExecution){
        sizeof(DataBindNativeExecution), DATA_BIND_NATIVE_EXECUTION_ABI_VERSION,
        &tr_control_rpc__function_meta, &tr_control_rpc__function_abi_meta,
        NULL, tr_control_rpc_invoke};
    projection.wire_method = "raft.status";
    projection.fields = &status_result;
    projection.field_count = 1U;
    projection.errors = errors;
    projection.error_count = 1U;
    result = data_bind_rpc_method_plan_compile_service(
        plane->codec, "raft", "status", &projection, &plane->native_binding,
        &plane->rpc_plan, &diagnostic);
    if (result != DATA_BIND_OK) {
        return tr_control_bind_failure("method plan", result, diagnostic.message);
    }
    plane->rpc_method = (chttp_rpc_method){
        .path = TR_RAFT_CONTROL_RPC_ENDPOINT,
        .plan = plane->rpc_plan,
        .binding = &plane->native_binding,
        .execution = &plane->execution,
        .codec = plane->codec};
    return SALTS_OK;
}

static void tr_control_release(tr_raft_control_plane_t *plane)
{
    data_bind_rpc_method_plan_free(plane->rpc_plan);
    data_bind_free(plane->codec);
    free(plane->controllers);
    free(plane->components);
    free(plane);
}

int tr_raft_control_plane_create(
    const tr_raft_control_plane_config_t *config,
    tr_raft_control_plane_t **out_plane)
{
    static const chttp_action actions[] = {
        CHTTP_GET(TR_RAFT_CONTROL_STATUS_PATH, tr_control_http_status)};
    tr_raft_control_plane_t *plane;
    size_t component_count, controller_count;
    int result;
    if (out_plane == NULL) return SALTS_EINVAL;
    *out_plane = NULL;
    if (config == NULL || config->status_provider == NULL ||
        config->stop_timeout_ms == 0U ||
        (config->server.component_count != 0U && config->server.components == NULL) ||
        (config->server.controller_count != 0U && config->server.controllers == NULL)) {
        return SALTS_EINVAL;
    }
    if (config->server.component_count >= SIZE_MAX / sizeof(*plane->components) ||
        config->server.controller_count >= SIZE_MAX / sizeof(*plane->controllers)) {
        return SALTS_ERANGE;
    }
    component_count = config->server.component_count + 1U;
    controller_count = config->server.controller_count + 1U;
    plane = (tr_raft_control_plane_t *)calloc(1U, sizeof(*plane));
    if (plane == NULL) return SALTS_ENOMEM;
    plane->components = (salts_configurator_deployment *)calloc(
        component_count, sizeof(*plane->components));
    plane->controllers = (chttp_controller *)calloc(
        controller_count, sizeof(*plane->controllers));
    if (plane->components == NULL || plane->controllers == NULL) {
        tr_control_release(plane);
        return SALTS_ENOMEM;
    }
    plane->status_provider = config->status_provider;
    plane->status_context = config->status_context;
    plane->stop_timeout_ms = config->stop_timeout_ms;
    result = tr_control_prepare_rpc(plane);
    if (result != SALTS_OK) {
        tr_control_release(plane);
        return result;
    }
    plane->provider = (salts_configurator_provider_binding){
        .struct_size = sizeof(salts_configurator_provider_binding),
        .abi_version = SALTS_CONFIGURATOR_PROVIDER_BINDING_ABI_VERSION,
        .component = cmeta_component_meta(TurboRaftControlComponent),
        .provider_context = plane,
        .create = tr_control_controller_create};
    if (config->server.component_count != 0U) {
        memcpy(plane->components, config->server.components,
               config->server.component_count * sizeof(*plane->components));
    }
    if (config->server.controller_count != 0U) {
        memcpy(plane->controllers, config->server.controllers,
               config->server.controller_count * sizeof(*plane->controllers));
    }
    plane->components[component_count - 1U] = (salts_configurator_deployment){
        &plane->provider, NULL, NULL};
    plane->controllers[controller_count - 1U] = (chttp_controller){
        .component_id = "TurboRaftControlComponent",
        .receiver = cmeta_reflected_data(tr_raft_control_plane_t),
        .actions = actions,
        .action_count = sizeof(actions) / sizeof(actions[0]),
        .rpc_methods = &plane->rpc_method,
        .rpc_method_count = 1U};
    plane->configuration = config->server;
    plane->configuration.components = plane->components;
    plane->configuration.component_count = component_count;
    plane->configuration.controllers = plane->controllers;
    plane->configuration.controller_count = controller_count;
    result = chttp_application_init(&plane->application, &plane->configuration);
    if (result != SALTS_OK && plane->application.impl == NULL) {
        tr_control_release(plane);
        return result;
    }
    *out_plane = plane;
    return result;
}

int tr_raft_control_plane_start(tr_raft_control_plane_t *plane)
{
    int result;
    if (plane == NULL) return SALTS_EINVAL;
    if (plane->started) return SALTS_EALREADY;
    if (plane->application.impl == NULL) {
        result = chttp_application_init(
            &plane->application, &plane->configuration);
        if (result != SALTS_OK) return result;
    }
    result = chttp_application_start(&plane->application);
    if (result == SALTS_OK) plane->started = 1;
    return result;
}

int tr_raft_control_plane_port(const tr_raft_control_plane_t *plane,
                               uint16_t *out_port)
{
    if (plane == NULL || out_port == NULL || !plane->started) {
        return SALTS_EINVAL;
    }
    return chttp_application_port(&plane->application, out_port);
}

int tr_raft_control_plane_stop(tr_raft_control_plane_t *plane)
{
    int result;
    if (plane == NULL) return SALTS_EINVAL;
    if (!plane->started) return SALTS_OK;
    result = chttp_application_close(
        &plane->application, plane->stop_timeout_ms);
    if (plane->application.impl == NULL) plane->started = 0;
    return result;
}

int tr_raft_control_plane_destroy(tr_raft_control_plane_t *plane)
{
    int result;
    if (plane == NULL) return SALTS_OK;
    if (plane->started) return SALTS_EBUSY;
    result = chttp_application_close(
        &plane->application, plane->stop_timeout_ms);
    if (result == SALTS_OK) tr_control_release(plane);
    return result;
}
