#include <turboraft/raft_control_plane.h>
#include <turboraft/raft_service_owner.h>

#include "raft_control_owner_bridge.h"

#include <iris/iris.h>
#include <iris/iris_app.h>
#include <iris/rpc_server.h>
#include <turbo_parser.h>
#include <turbo_error.h>

#include <inttypes.h>
#include <limits.h>
#include <stdarg.h>

#include "raft_control_operation.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum {
    TR_RAFT_CONTROL_MAX_REQUEST_BYTES = 16 * 1024,
    TR_RAFT_CONTROL_MAX_RESPONSE_BYTES = 64 * 1024,
    TR_RAFT_CONTROL_MAX_BATCH_SIZE = 8,
    TR_RAFT_CONTROL_JSON_CAPACITY = 8192,
    TR_RAFT_CONTROL_HTML_CAPACITY = 4096,
    TR_RAFT_CONTROL_RPC_SERVICE_ERROR = -32001,
    TR_RAFT_CONTROL_RPC_NOT_LEADER = -32002,
    TR_RAFT_CONTROL_RPC_AUDIT_FAULTED = -32003
};

#define TR_RAFT_CONTROL_HTMX_RPC_ATTRS                                  \
    " hx-post=\"" TR_RAFT_CONTROL_RPC_ENDPOINT                         \
    "\" hx-ext=\"raft-rpc\" hx-target=\"#rpc-result\""              \
    " hx-swap=\"innerHTML\" hx-disabled-elt=\"button\""

static const char tr_control_binding_path[] =
    "/__turboraft/control-plane";

struct tr_raft_control_plane {
    tr_raft_service_t *service;
    tr_raft_service_owner_t *owner;
    tr_raft_control_audit_t *audit;
    bool audit_faulted;
    uint64_t owner_bridge_timeout_ms;
    tr_raft_node_id_t local_node_id;
    iris_app_t *app;
    rpc_context_t *rpc;
};

typedef struct tr_control_service_status_command {
    tr_raft_service_status_t *output;
} tr_control_service_status_command_t;

typedef struct tr_control_configuration_command {
    tr_raft_conf_t *output;
} tr_control_configuration_command_t;

typedef struct tr_control_progress_command {
    tr_raft_progress_view_t *output;
} tr_control_progress_command_t;

typedef struct tr_control_read_state_command {
    tr_raft_read_state_t *output;
} tr_control_read_state_command_t;

static int tr_control_execute(
    const tr_raft_control_plane_t *plane,
    tr_raft_service_owner_command_fn command,
    const void *payload,
    size_t payload_size);
static int tr_control_command_service_status(tr_raft_service_t *service,
                                             const void *payload,
                                             size_t payload_size);

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

static int tr_control_finish_render(int written,
                                    size_t capacity,
                                    size_t *out_size)
{
    if (written < 0 || (size_t) written >= capacity) {
        return TURBO_EINVAL;
    }
    *out_size = (size_t) written;
    return TURBO_OK;
}

int tr_raft_control_plane_render_status_json(
    const tr_raft_control_plane_t *plane,
    char *output,
    size_t capacity,
    size_t *out_size)
{
    tr_raft_service_status_t status;
    tr_control_service_status_command_t status_command = {&status};
#if defined(TURBORAFT_HAS_SERVICE_OWNER)
    tr_raft_service_owner_status_t owner_status;
#endif
    bool owner_configured = false;
    bool owner_running = false;
    bool owner_faulted = false;
    size_t owner_pending = 0U;
    uint64_t owner_completed = 0U;
    int result;
    int written;

    if (plane == NULL || output == NULL || capacity == 0U ||
        out_size == NULL) {
        return TURBO_EINVAL;
    }
    *out_size = 0U;
    result = tr_control_execute(plane, tr_control_command_service_status,
                                &status_command, sizeof(status_command));
    if (result != TURBO_OK) {
        return result;
    }
#if defined(TURBORAFT_HAS_SERVICE_OWNER)
    if (plane->owner != NULL) {
        result = tr_raft_service_owner_status(plane->owner, &owner_status);
        if (result != TURBO_OK) {
            return result;
        }
        owner_configured = true;
        owner_running = owner_status.running;
        owner_faulted = owner_status.faulted;
        owner_pending = owner_status.pending_commands;
        owner_completed = owner_status.completed_commands;
    }
#endif
    written = snprintf(
        output, capacity,
        "{\"node_id\":%" PRIu64 ",\"leader_id\":%" PRIu64
        ",\"role\":\"%s\",\"term\":%" PRIu64
        ",\"last_log_index\":%" PRIu64
        ",\"log_base_index\":%" PRIu64
        ",\"log_entry_count\":%zu"
        ",\"commit_index\":%" PRIu64
        ",\"applied_index\":%" PRIu64
        ",\"voter_count\":%zu,\"learner_count\":%zu"
        ",\"peer_count\":%zu,\"joint_configuration\":%s"
        ",\"membership_transition_id\":%" PRIu64
        ",\"inflight_append_count\":%zu"
        ",\"snapshot_required_peer_count\":%zu,\"faulted\":%s"
        ",\"cause\":%d,\"owner\":{\"configured\":%s"
        ",\"running\":%s,\"faulted\":%s,\"pending\":%zu"
        ",\"completed\":%" PRIu64 "}}",
        status.core.self_id, status.core.leader_id,
        tr_control_role_name(status.core.role), status.core.term,
        status.core.last_log_index, status.core.log_base_index,
        status.core.log_entry_count, status.core.commit_index,
        status.core.applied_index, status.core.voter_count,
        status.core.learner_count, status.core.peer_count,
        status.core.joint_configuration ? "true" : "false",
        status.core.membership_transition_id,
        status.core.inflight_append_count,
        status.core.snapshot_required_peer_count,
        status.faulted ? "true" : "false", status.cause,
        owner_configured ? "true" : "false",
        owner_running ? "true" : "false",
        owner_faulted ? "true" : "false", owner_pending,
        owner_completed);
    return tr_control_finish_render(written, capacity, out_size);
}

int tr_raft_control_plane_render_status_html(
    const tr_raft_control_plane_t *plane,
    char *output,
    size_t capacity,
    size_t *out_size)
{
    tr_raft_service_status_t status;
    tr_control_service_status_command_t status_command = {&status};
#if defined(TURBORAFT_HAS_SERVICE_OWNER)
    tr_raft_service_owner_status_t owner_status;
#endif
    const char *owner_state = "LEGACY";
    size_t owner_pending = 0U;
    uint64_t owner_completed = 0U;
    bool owner_faulted = false;
    int result;
    int written;

    if (plane == NULL || output == NULL || capacity == 0U ||
        out_size == NULL) {
        return TURBO_EINVAL;
    }
    *out_size = 0U;
    result = tr_control_execute(plane, tr_control_command_service_status,
                                &status_command, sizeof(status_command));
    if (result != TURBO_OK) {
        return result;
    }
#if defined(TURBORAFT_HAS_SERVICE_OWNER)
    if (plane->owner != NULL) {
        result = tr_raft_service_owner_status(plane->owner, &owner_status);
        if (result != TURBO_OK) {
            return result;
        }
        owner_state = owner_status.faulted
                          ? "FAULTED"
                          : (owner_status.running ? "RUNNING" : "STOPPED");
        owner_pending = owner_status.pending_commands;
        owner_completed = owner_status.completed_commands;
        owner_faulted = owner_status.faulted;
    }
#endif
    written = snprintf(
        output, capacity,
        "<div class=\"metric hero\"><span>ROLE</span><strong>%s</strong>"
        "</div><div class=\"metric\"><span>NODE</span><strong>%" PRIu64
        "</strong></div><div class=\"metric\"><span>LEADER</span>"
        "<strong>%" PRIu64 "</strong></div><div class=\"metric\">"
        "<span>TERM</span><strong>%" PRIu64 "</strong></div>"
        "<div class=\"metric\"><span>COMMIT</span><strong>%" PRIu64
        "</strong></div><div class=\"metric\"><span>APPLIED</span>"
        "<strong>%" PRIu64 "</strong></div><div class=\"metric\">"
        "<span>LOG TAIL</span><strong>%" PRIu64 "</strong></div>"
        "<div class=\"metric\"><span>LOG BASE</span><strong>%" PRIu64
        "</strong></div><div class=\"metric\"><span>RETAINED</span>"
        "<strong>%zu</strong></div><div class=\"metric\">"
        "<span>SNAPSHOT WAIT</span><strong>%zu</strong></div>"
        "<div class=\"metric\"><span>MEMBERS</span><strong>%zuV / %zuL"
        "</strong></div><div class=\"metric\"><span>CONFIG</span>"
        "<strong>%s</strong></div><div class=\"metric\"><span>OWNER</span>"
        "<strong>%s / %zuQ</strong></div><div class=\"metric\">"
        "<span>OWNER CMDS</span><strong>%" PRIu64 "</strong></div>"
        "<div class=\"health %s\">%s"
        " &middot; cause %d</div>",
        tr_control_role_name(status.core.role), status.core.self_id,
        status.core.leader_id, status.core.term, status.core.commit_index,
        status.core.applied_index, status.core.last_log_index,
        status.core.log_base_index, status.core.log_entry_count,
        status.core.snapshot_required_peer_count,
        status.core.voter_count, status.core.learner_count,
        status.core.joint_configuration ? "JOINT" : "STABLE",
        owner_state, owner_pending, owner_completed,
        status.faulted || owner_faulted ? "bad" : "good",
        status.faulted || owner_faulted ? "FAULTED" : "HEALTHY",
        status.cause);
    return tr_control_finish_render(written, capacity, out_size);
}

static tr_raft_control_plane_t *tr_control_from_request(const Req *request)
{
    if (request == NULL || request->app == NULL) {
        return NULL;
    }
    return (tr_raft_control_plane_t *) iris_app_lookup_rpc_context(
        request->app, tr_control_binding_path);
}

static int tr_control_rpc_failure(rpc_response_t *response,
                                  int cause,
                                  const char *operation)
{
    char message[128];

    snprintf(message, sizeof(message), "%s failed with Turbo error %d",
             operation, cause);
    rpc_set_error(response, TR_RAFT_CONTROL_RPC_SERVICE_ERROR, message);
    return TR_RAFT_CONTROL_RPC_SERVICE_ERROR;
}

static int tr_control_rpc_failure_with_leader(
    tr_raft_control_plane_t *plane,
    rpc_response_t *response,
    int cause,
    const char *operation)
{
    tr_raft_service_status_t status;
    tr_control_service_status_command_t status_command = {&status};
    char message[160];

    if (plane != NULL && plane->audit_faulted) {
        snprintf(message, sizeof(message), "%s failed: AUDIT_FAULTED",
                 operation);
        rpc_set_error(response, TR_RAFT_CONTROL_RPC_AUDIT_FAULTED, message);
        return TR_RAFT_CONTROL_RPC_AUDIT_FAULTED;
    }
    if (plane != NULL && (cause == TURBO_EPROTO || cause == TURBO_EPERM) &&
        tr_control_execute(plane, tr_control_command_service_status,
                           &status_command, sizeof(status_command)) ==
            TURBO_OK &&
        !status.faulted && status.core.role != TR_RAFT_LEADER) {
        snprintf(message, sizeof(message),
                 "%s failed: NOT_LEADER; leader_id=%" PRIu64,
                 operation, status.core.leader_id);
        rpc_set_error(response, TR_RAFT_CONTROL_RPC_NOT_LEADER, message);
        return TR_RAFT_CONTROL_RPC_NOT_LEADER;
    }
    return tr_control_rpc_failure(response, cause, operation);
}

static int tr_control_command_service_status(tr_raft_service_t *service,
                                             const void *payload,
                                             size_t payload_size)
{
    const tr_control_service_status_command_t *command =
        (const tr_control_service_status_command_t *)payload;

    return command != NULL && payload_size == sizeof(*command) &&
                   command->output != NULL
               ? tr_raft_service_status(service, command->output)
               : TURBO_EINVAL;
}

static int tr_control_command_configuration(tr_raft_service_t *service,
                                            const void *payload,
                                            size_t payload_size)
{
    const tr_control_configuration_command_t *command =
        (const tr_control_configuration_command_t *)payload;

    return command != NULL && payload_size == sizeof(*command) &&
                   command->output != NULL
               ? tr_raft_service_configuration(service, command->output)
               : TURBO_EINVAL;
}

static int tr_control_command_progress(tr_raft_service_t *service,
                                       const void *payload,
                                       size_t payload_size)
{
    const tr_control_progress_command_t *command =
        (const tr_control_progress_command_t *)payload;

    return command != NULL && payload_size == sizeof(*command) &&
                   command->output != NULL
               ? tr_raft_service_progress(service, command->output)
               : TURBO_EINVAL;
}

static int tr_control_command_take_read_state(tr_raft_service_t *service,
                                              const void *payload,
                                              size_t payload_size)
{
    const tr_control_read_state_command_t *command =
        (const tr_control_read_state_command_t *)payload;

    return command != NULL && payload_size == sizeof(*command) &&
                   command->output != NULL
               ? tr_raft_service_take_read_state(service, command->output)
               : TURBO_EINVAL;
}

static int tr_control_command_tick(tr_raft_service_t *service,
                                   const void *payload,
                                   size_t payload_size)
{
    return payload_size == sizeof(tr_raft_tick_t)
               ? tr_raft_service_tick(service,
                                      (const tr_raft_tick_t *)payload)
               : TURBO_EINVAL;
}

static int tr_control_command_propose(tr_raft_service_t *service,
                                      const void *payload,
                                      size_t payload_size)
{
    return payload_size == sizeof(tr_raft_proposal_t)
               ? tr_raft_service_propose(
                     service, (const tr_raft_proposal_t *)payload)
               : TURBO_EINVAL;
}

typedef struct tr_control_propose_receipt_command {
    tr_raft_proposal_t proposal;
    tr_raft_operation_status_t *out_receipt;
} tr_control_propose_receipt_command_t;

static int tr_control_command_propose_receipt(tr_raft_service_t *service,
                                              const void *payload,
                                              size_t payload_size)
{
    const tr_control_propose_receipt_command_t *command =
        (const tr_control_propose_receipt_command_t *)payload;

    return command != NULL && payload_size == sizeof(*command) &&
                   command->out_receipt != NULL
               ? tr_raft_service_propose_with_receipt(
                     service, &command->proposal, command->out_receipt)
               : TURBO_EINVAL;
}

static int tr_control_command_read_index(tr_raft_service_t *service,
                                         const void *payload,
                                         size_t payload_size)
{
    return payload_size == sizeof(uint64_t)
               ? tr_raft_service_read_index(service,
                                            *(const uint64_t *)payload)
               : TURBO_EINVAL;
}

static int tr_control_command_transfer(tr_raft_service_t *service,
                                       const void *payload,
                                       size_t payload_size)
{
    return payload_size == sizeof(tr_raft_node_id_t)
               ? tr_raft_service_transfer_leadership(
                     service, *(const tr_raft_node_id_t *)payload)
               : TURBO_EINVAL;
}

static int tr_control_command_membership(tr_raft_service_t *service,
                                         const void *payload,
                                         size_t payload_size)
{
    return payload_size == sizeof(tr_raft_membership_change_t)
               ? tr_raft_service_change_membership(
                     service, (const tr_raft_membership_change_t *)payload)
               : TURBO_EINVAL;
}

static int tr_control_rpc_operation_status(Req *request,
                                           Res *response,
                                           rpc_request_t *rpc_request,
                                           rpc_response_t *rpc_response);

typedef enum tr_control_member_action {
    TR_CONTROL_MEMBER_ADD_LEARNER = 1,
    TR_CONTROL_MEMBER_PROMOTE = 2,
    TR_CONTROL_MEMBER_REMOVE = 3
} tr_control_member_action_t;

typedef struct tr_control_member_command {
    tr_control_member_action_t action;
    tr_raft_node_id_t node_id;
    uint64_t transition_id;
    tr_raft_operation_status_t *out_receipt;
} tr_control_member_command_t;

static void tr_control_sort_nodes(tr_raft_node_id_t *nodes, size_t count)
{
    size_t index;

    for (index = 1U; index < count; ++index) {
        tr_raft_node_id_t value = nodes[index];
        size_t position = index;

        while (position != 0U && nodes[position - 1U] > value) {
            nodes[position] = nodes[position - 1U];
            position--;
        }
        nodes[position] = value;
    }
}

static int tr_control_command_member_action(tr_raft_service_t *service,
                                            const void *payload,
                                            size_t payload_size)
{
    const tr_control_member_command_t *command =
        (const tr_control_member_command_t *) payload;
    tr_raft_node_id_t voters[TR_RAFT_MAX_MEMBERS];
    tr_raft_node_id_t learners[TR_RAFT_MAX_MEMBERS];
    tr_raft_membership_change_t change;
    tr_raft_conf_t configuration;
    size_t voter_count = 0U;
    size_t learner_count = 0U;
    bool found = false;
    size_t index;
    int result;

    if (service == NULL || command == NULL ||
        payload_size != sizeof(*command) || command->node_id == 0U ||
        command->transition_id == 0U || command->out_receipt == NULL) {
        return TURBO_EINVAL;
    }
    result = tr_raft_service_configuration(service, &configuration);
    if (result != TURBO_OK) {
        return result;
    }
    if (configuration.phase != TR_RAFT_CONF_FINAL) {
        return TURBO_EBUSY;
    }
    for (index = 0U; index < configuration.member_count; ++index) {
        const tr_raft_conf_member_t *member = &configuration.members[index];
        bool target = member->node_id == command->node_id;
        bool voter = (member->roles & (TR_RAFT_CONF_OLD_VOTER |
                                       TR_RAFT_CONF_NEW_VOTER)) != 0U;

        found = found || target;
        if (target && command->action == TR_CONTROL_MEMBER_REMOVE) {
            continue;
        }
        if (target && command->action == TR_CONTROL_MEMBER_ADD_LEARNER) {
            return TURBO_EINVAL;
        }
        if (target && command->action == TR_CONTROL_MEMBER_PROMOTE) {
            if (voter) {
                return TURBO_EINVAL;
            }
            voters[voter_count++] = member->node_id;
        } else if (voter) {
            voters[voter_count++] = member->node_id;
        } else {
            learners[learner_count++] = member->node_id;
        }
    }
    if (command->action == TR_CONTROL_MEMBER_ADD_LEARNER) {
        if (found || voter_count + learner_count >= TR_RAFT_MAX_MEMBERS) {
            return TURBO_EINVAL;
        }
        learners[learner_count++] = command->node_id;
    } else if (!found) {
        return TURBO_EINVAL;
    }
    if (voter_count == 0U) {
        return TURBO_EINVAL;
    }
    tr_control_sort_nodes(voters, voter_count);
    tr_control_sort_nodes(learners, learner_count);
    memset(&change, 0, sizeof(change));
    change.transition_id = command->transition_id;
    change.voters = voters;
    change.voter_count = voter_count;
    change.learners = learner_count == 0U ? NULL : learners;
    change.learner_count = learner_count;
    return tr_raft_service_change_membership_with_receipt(
        service, &change, command->out_receipt);
}

static int tr_control_command_snapshot(tr_raft_service_t *service,
                                       const void *payload,
                                       size_t payload_size)
{
    (void) payload;
    return payload_size == 0U
               ? tr_raft_service_trigger_snapshot(service)
               : TURBO_EINVAL;
}

static int tr_control_execute(const tr_raft_control_plane_t *plane,
                              tr_raft_service_owner_command_fn command,
                              const void *payload,
                              size_t payload_size)
{
#if defined(TURBORAFT_HAS_SERVICE_OWNER)
    if (plane->owner != NULL) {
        return tr_control_owner_bridge_execute(
            plane->owner, command, payload, payload_size,
            plane->owner_bridge_timeout_ms);
    }
#endif
    return command(plane->service, payload, payload_size);
}

typedef struct tr_control_audit_scope {
    uint32_t method;
    tr_raft_node_id_t target_node_id;
    const tr_raft_operation_status_t *receipt;
} tr_control_audit_scope_t;

static void tr_control_fill_audit_event(
    tr_raft_control_plane_t *plane,
    const tr_control_audit_scope_t *scope,
    uint32_t phase,
    int outcome,
    tr_raft_control_audit_event_t *event)
{
    memset(event, 0, sizeof(*event));
    event->method = scope->method;
    event->phase = phase;
    event->outcome = outcome;
    event->target_node_id = scope->target_node_id;
    event->local_node_id = plane->local_node_id;
    if (scope->receipt != NULL) {
        event->term = scope->receipt->term;
        event->index = scope->receipt->index;
    }
}

static uint32_t tr_control_audit_rpc_method(const char *method)
{
    if (method == NULL) {
        return 0U;
    }
    if (strcmp(method, "raft.tick") == 0) {
        return TR_RAFT_CONTROL_AUDIT_TICK;
    }
    if (strcmp(method, "raft.propose") == 0 ||
        strcmp(method, "raft.propose_async") == 0) {
        return TR_RAFT_CONTROL_AUDIT_PROPOSE;
    }
    if (strcmp(method, "raft.readIndex") == 0 ||
        strcmp(method, "raft.read_index") == 0 ||
        strcmp(method, "raft.takeReadState") == 0) {
        return TR_RAFT_CONTROL_AUDIT_READ_INDEX;
    }
    if (strcmp(method, "raft.transferLeadership") == 0 ||
        strcmp(method, "raft.leader.transfer") == 0) {
        return TR_RAFT_CONTROL_AUDIT_LEADER_TRANSFER;
    }
    if (strcmp(method, "raft.changeMembership") == 0) {
        return TR_RAFT_CONTROL_AUDIT_CHANGE_MEMBERSHIP;
    }
    if (strcmp(method, "raft.member.add_learner") == 0) {
        return TR_RAFT_CONTROL_AUDIT_ADD_LEARNER;
    }
    if (strcmp(method, "raft.member.promote") == 0) {
        return TR_RAFT_CONTROL_AUDIT_PROMOTE;
    }
    if (strcmp(method, "raft.member.remove") == 0) {
        return TR_RAFT_CONTROL_AUDIT_REMOVE;
    }
    if (strcmp(method, "raft.snapshot.trigger") == 0) {
        return TR_RAFT_CONTROL_AUDIT_SNAPSHOT;
    }
    if (strcmp(method, "raft.operation.status") == 0) {
        return TR_RAFT_CONTROL_AUDIT_OPERATION_STATUS;
    }
    return 0U;
}

static void tr_control_rpc_auth_decision(
    void *context,
    const rpc_auth_decision_t *decision)
{
    tr_raft_control_plane_t *plane = (tr_raft_control_plane_t *)context;
    tr_control_audit_scope_t scope;
    tr_raft_control_audit_event_t event;
    uint32_t method;

    if (plane == NULL || decision == NULL || plane->audit == NULL ||
        plane->audit_faulted ||
        decision->version != RPC_AUTH_DECISION_VERSION ||
        decision->size < sizeof(*decision) || !decision->requires_auth ||
        decision->allowed) {
        return;
    }
    method = tr_control_audit_rpc_method(decision->method);
    if (method == 0U) {
        return;
    }
    memset(&scope, 0, sizeof(scope));
    scope.method = method;
    tr_control_fill_audit_event(
        plane, &scope, TR_RAFT_CONTROL_AUDIT_AUTHORIZATION, TURBO_EPERM,
        &event);
    if (tr_raft_control_audit_emit(plane->audit, &event) != TURBO_OK) {
        plane->audit_faulted = true;
    }
}

static int tr_control_execute_audited(
    tr_raft_control_plane_t *plane,
    const tr_control_audit_scope_t *scope,
    tr_raft_service_owner_command_fn command,
    const void *payload,
    size_t payload_size)
{
    tr_raft_control_audit_event_t event;
    int audit_result;
    int result;

    if (plane == NULL || scope == NULL || command == NULL) {
        return TURBO_EINVAL;
    }
    if (plane->audit == NULL) {
        return tr_control_execute(plane, command, payload, payload_size);
    }
    if (plane->audit_faulted) {
        return TURBO_EPROTO;
    }

    tr_control_fill_audit_event(
        plane, scope, TR_RAFT_CONTROL_AUDIT_AUTHORIZATION, TURBO_OK, &event);
    audit_result = tr_raft_control_audit_emit(plane->audit, &event);
    if (audit_result != TURBO_OK) {
        plane->audit_faulted = true;
        return audit_result;
    }

    result = tr_control_execute(plane, command, payload, payload_size);
    tr_control_fill_audit_event(
        plane, scope, TR_RAFT_CONTROL_AUDIT_COMPLETION, result, &event);
    audit_result = tr_raft_control_audit_emit(plane->audit, &event);
    if (audit_result != TURBO_OK) {
        plane->audit_faulted = true;
    }
    return result;
}

static int tr_control_rpc_status(Req *request,
                                 Res *http_response,
                                 rpc_request_t *rpc_request,
                                 rpc_response_t *rpc_response)
{
    tr_raft_control_plane_t *plane = tr_control_from_request(request);
    char json[TR_RAFT_CONTROL_JSON_CAPACITY];
    size_t size;
    int result;

    (void) http_response;
    (void) rpc_request;
    if (plane == NULL) {
        rpc_set_error(rpc_response, RPC_ERROR_INTERNAL,
                      "Raft control plane is not bound");
        return RPC_ERROR_INTERNAL;
    }
    result = tr_raft_control_plane_render_status_json(
        plane, json, sizeof(json), &size);
    if (result != TURBO_OK) {
        return tr_control_rpc_failure(rpc_response, result, "status");
    }
    (void) size;
    rpc_set_result(rpc_response, json);
    return 0;
}

static int tr_control_get_positive_u64(rpc_request_t *request,
                                       const char *key,
                                       uint64_t *output)
{
    int64_t value;

    if (rpc_get_param_int(request, key, &value) != 0 || value <= 0) {
        return TURBO_EINVAL;
    }
    *output = (uint64_t) value;
    return TURBO_OK;
}

typedef enum tr_control_query_role {
    TR_CONTROL_QUERY_ROLE_ANY = 0,
    TR_CONTROL_QUERY_ROLE_VOTER,
    TR_CONTROL_QUERY_ROLE_LEARNER
} tr_control_query_role_t;

static int tr_control_json_uint64(const json_value_t *value,
                                  uint64_t *output);

static int tr_control_parse_query_filter(
    const rpc_request_t *rpc_request,
    tr_control_query_role_t *out_role,
    bool *out_has_node_id,
    tr_raft_node_id_t *out_node_id)
{
    turbo_json_doc_t *params = NULL;
    json_value_t *value;
    const char *role;
    int result = TURBO_EINVAL;

    if (out_role == NULL || out_has_node_id == NULL || out_node_id == NULL) {
        return TURBO_EINVAL;
    }
    *out_role = TR_CONTROL_QUERY_ROLE_ANY;
    *out_has_node_id = false;
    *out_node_id = 0U;
    if (rpc_request == NULL || rpc_request->params == NULL) {
        return TURBO_OK;
    }
    if (turbo_parse_json((const uint8_t *) rpc_request->params,
                         strlen(rpc_request->params), &params) != TURBO_OK ||
        params == NULL || turbo_json_type(params) != TURBO_JSON_OBJECT) {
        goto done;
    }

    value = turbo_json_object_get(params, "role");
    if (value != NULL) {
        if (turbo_json_type(value) != TURBO_JSON_STRING) {
            goto done;
        }
        role = turbo_json_string(value);
        if (strcmp(role, "voter") == 0) {
            *out_role = TR_CONTROL_QUERY_ROLE_VOTER;
        } else if (strcmp(role, "learner") == 0) {
            *out_role = TR_CONTROL_QUERY_ROLE_LEARNER;
        } else {
            goto done;
        }
    }

    value = turbo_json_object_get(params, "node_id");
    if (value != NULL) {
        if (tr_control_json_uint64(value, out_node_id) != TURBO_OK ||
            *out_node_id == 0U) {
            goto done;
        }
        *out_has_node_id = true;
    }
    result = TURBO_OK;

done:
    turbo_free_json(&params);
    return result;
}

static bool tr_control_member_matches_role(
    tr_control_query_role_t role,
    const tr_raft_conf_member_t *member)
{
    if (role == TR_CONTROL_QUERY_ROLE_ANY) {
        return true;
    }
    if (role == TR_CONTROL_QUERY_ROLE_VOTER) {
        return (member->roles &
                (TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER)) != 0U;
    }
    return (member->roles & TR_RAFT_CONF_LEARNER) != 0U;
}

static int tr_control_json_append(char *output,
                                  size_t capacity,
                                  size_t *used,
                                  const char *format,
                                  ...)
{
    va_list arguments;
    int written;

    if (output == NULL || used == NULL || format == NULL ||
        *used >= capacity) {
        return TURBO_ENOSPC;
    }
    va_start(arguments, format);
    written = vsnprintf(output + *used, capacity - *used, format, arguments);
    va_end(arguments);
    if (written < 0 || (size_t) written >= capacity - *used) {
        return TURBO_ENOSPC;
    }
    *used += (size_t) written;
    return TURBO_OK;
}

static int tr_control_rpc_members(Req *request,
                                  Res *http_response,
                                  rpc_request_t *rpc_request,
                                  rpc_response_t *rpc_response)
{
    tr_raft_control_plane_t *plane = tr_control_from_request(request);
    tr_raft_conf_t configuration = {0};
    tr_control_configuration_command_t command = {&configuration};
    char json[TR_RAFT_CONTROL_JSON_CAPACITY];
    size_t used = 0U;
    size_t index;
    size_t emitted = 0U;
    tr_control_query_role_t role;
    bool has_node_id;
    tr_raft_node_id_t node_id;
    int result;

    (void) http_response;
    if (plane == NULL) {
        return tr_control_rpc_failure(rpc_response, TURBO_EPROTO,
                                      "members");
    }
    if (tr_control_parse_query_filter(rpc_request, &role, &has_node_id,
                                      &node_id) != TURBO_OK || has_node_id) {
        rpc_set_error(rpc_response, RPC_ERROR_INVALID_PARAMS,
                      "role must be voter or learner");
        return RPC_ERROR_INVALID_PARAMS;
    }
    result = tr_control_execute(plane, tr_control_command_configuration,
                                &command, sizeof(command));
    if (result == TURBO_OK) {
        result = tr_control_json_append(
            json, sizeof(json), &used,
            "{\"phase\":\"%s\",\"transition_id\":%" PRIu64
            ",\"members\":[",
            configuration.phase == TR_RAFT_CONF_JOINT ? "joint" : "stable",
            configuration.transition_id);
    }
    for (index = 0U; result == TURBO_OK &&
                     index < configuration.member_count; ++index) {
        const tr_raft_conf_member_t *member = &configuration.members[index];

        if (!tr_control_member_matches_role(role, member)) {
            continue;
        }
        result = tr_control_json_append(
            json, sizeof(json), &used,
            "%s{\"node_id\":%" PRIu64 ",\"roles\":%u}",
            emitted == 0U ? "" : ",", member->node_id,
            (unsigned int) member->roles);
        ++emitted;
    }
    if (result == TURBO_OK) {
        result = tr_control_json_append(json, sizeof(json), &used, "]}");
    }
    if (result != TURBO_OK) {
        return tr_control_rpc_failure(rpc_response, result, "members");
    }
    rpc_set_result(rpc_response, json);
    return 0;
}

static int tr_control_rpc_progress(Req *request,
                                   Res *http_response,
                                   rpc_request_t *rpc_request,
                                   rpc_response_t *rpc_response)
{
    tr_raft_control_plane_t *plane = tr_control_from_request(request);
    tr_raft_progress_view_t progress = {0};
    tr_control_progress_command_t command = {&progress};
    char json[TR_RAFT_CONTROL_JSON_CAPACITY];
    size_t used = 0U;
    size_t index;
    size_t emitted = 0U;
    tr_control_query_role_t role;
    bool has_node_id;
    tr_raft_node_id_t node_id;
    int result;

    (void) http_response;
    if (plane == NULL) {
        return tr_control_rpc_failure(rpc_response, TURBO_EPROTO,
                                      "progress");
    }
    if (tr_control_parse_query_filter(rpc_request, &role, &has_node_id,
                                      &node_id) != TURBO_OK ||
        role != TR_CONTROL_QUERY_ROLE_ANY) {
        rpc_set_error(rpc_response, RPC_ERROR_INVALID_PARAMS,
                      "node_id must be a positive integer");
        return RPC_ERROR_INVALID_PARAMS;
    }
    result = tr_control_execute(plane, tr_control_command_progress,
                                &command, sizeof(command));
    if (result == TURBO_OK) {
        result = tr_control_json_append(json, sizeof(json), &used,
                                        "{\"peers\":[");
    }
    for (index = 0U; result == TURBO_OK && index < progress.peer_count;
         ++index) {
        const tr_raft_peer_progress_t *peer = &progress.peers[index];

        if (has_node_id && peer->node_id != node_id) {
            continue;
        }
        result = tr_control_json_append(
            json, sizeof(json), &used,
            "%s{\"node_id\":%" PRIu64 ",\"match_index\":%" PRIu64
            ",\"next_index\":%" PRIu64
            ",\"recent_active\":%s,\"append_inflight\":%s"
            ",\"snapshot_required\":%s}",
            emitted == 0U ? "" : ",", peer->node_id, peer->match_index,
            peer->next_index, peer->recent_active ? "true" : "false",
            peer->append_inflight ? "true" : "false",
            peer->snapshot_required ? "true" : "false");
        ++emitted;
    }
    if (result == TURBO_OK && has_node_id && emitted == 0U) {
        result = TURBO_ENOENT;
    }
    if (result == TURBO_OK) {
        result = tr_control_json_append(json, sizeof(json), &used, "]}");
    }
    if (result != TURBO_OK) {
        return tr_control_rpc_failure(rpc_response, result, "progress");
    }
    rpc_set_result(rpc_response, json);
    return 0;
}

static int tr_control_rpc_storage_status(Req *request,
                                         Res *http_response,
                                         rpc_request_t *rpc_request,
                                         rpc_response_t *rpc_response)
{
    tr_raft_control_plane_t *plane = tr_control_from_request(request);
    tr_raft_service_status_t status = {0};
    tr_control_service_status_command_t command = {&status};
    char json[512];
    int result;

    (void) http_response;
    (void) rpc_request;
    if (plane == NULL) {
        return tr_control_rpc_failure(rpc_response, TURBO_EPROTO,
                                      "storage.status");
    }
    result = tr_control_execute(plane, tr_control_command_service_status,
                                &command, sizeof(command));
    if (result != TURBO_OK) {
        return tr_control_rpc_failure(rpc_response, result,
                                      "storage.status");
    }
    if (snprintf(
            json, sizeof(json),
            "{\"stage\":%d,\"cause\":%d,\"rollback_error\":%d"
            ",\"durable\":%s,\"messages_enqueued\":%zu"
            ",\"snapshots_requested\":%zu,\"applied_through\":%" PRIu64
            ",\"audit\":{\"configured\":%s,\"faulted\":%s"
            ",\"dropped\":%" PRIu64 "}}",
            (int) status.runtime.stage, status.runtime.cause,
            status.runtime.rollback_error,
            status.runtime.durable ? "true" : "false",
            status.runtime.messages_enqueued,
            status.runtime.snapshots_requested,
            status.runtime.applied_through,
            plane->audit != NULL ? "true" : "false",
            plane->audit_faulted ? "true" : "false",
            plane->audit == NULL
                ? 0U
                : tr_raft_control_audit_dropped(plane->audit)) < 0) {
        return tr_control_rpc_failure(rpc_response, TURBO_EINVAL,
                                      "storage.status");
    }
    rpc_set_result(rpc_response, json);
    return 0;
}

static int tr_control_rpc_tick(Req *request,
                               Res *http_response,
                               rpc_request_t *rpc_request,
                               rpc_response_t *rpc_response)
{
    tr_raft_control_plane_t *plane = tr_control_from_request(request);
    uint64_t elapsed;
    uint64_t timeout;
    tr_raft_tick_t tick;
    int result;

    (void) http_response;
    if (plane == NULL) {
        return tr_control_rpc_failure(rpc_response, TURBO_EPROTO, "tick");
    }
    if (tr_control_get_positive_u64(rpc_request, "elapsed_ticks", &elapsed) !=
            TURBO_OK ||
        tr_control_get_positive_u64(rpc_request, "next_timeout_ticks",
                                    &timeout) != TURBO_OK ||
        elapsed > UINT32_MAX || timeout > UINT32_MAX) {
        rpc_set_error(rpc_response, RPC_ERROR_INVALID_PARAMS,
                      "elapsed_ticks and next_timeout_ticks must be positive uint32 values");
        return RPC_ERROR_INVALID_PARAMS;
    }
    tick.elapsed_ticks = (uint32_t) elapsed;
    tick.next_election_timeout_ticks = (uint32_t) timeout;
    result = tr_control_execute_audited(
        plane,
        &(tr_control_audit_scope_t){TR_RAFT_CONTROL_AUDIT_TICK, 0U, NULL},
        tr_control_command_tick, &tick, sizeof(tick));
    if (result != TURBO_OK) {
        return tr_control_rpc_failure_with_leader(
            plane, rpc_response, result, "tick");
    }
    rpc_set_result(rpc_response, "{\"accepted\":true}");
    return 0;
}

static int tr_control_rpc_propose(Req *request,
                                  Res *http_response,
                                  rpc_request_t *rpc_request,
                                  rpc_response_t *rpc_response)
{
    tr_raft_control_plane_t *plane = tr_control_from_request(request);
    const char *data;
    uint64_t command_id;
    tr_raft_proposal_t proposal;
    int result;

    (void) http_response;
    data = rpc_get_param_string(rpc_request, "data");
    if (plane == NULL || data == NULL ||
        tr_control_get_positive_u64(rpc_request, "command_id", &command_id) !=
            TURBO_OK ||
        strlen(data) > TR_RAFT_MAX_ENTRY_BYTES) {
        rpc_set_error(rpc_response, RPC_ERROR_INVALID_PARAMS,
                      "command_id must be positive and data must fit one Raft entry");
        return RPC_ERROR_INVALID_PARAMS;
    }
    proposal.command_id = command_id;
    proposal.data = data;
    proposal.data_length = strlen(data);
    result = tr_control_execute_audited(
        plane,
        &(tr_control_audit_scope_t){TR_RAFT_CONTROL_AUDIT_PROPOSE, 0U, NULL},
        tr_control_command_propose, &proposal, sizeof(proposal));
    if (result != TURBO_OK) {
        return tr_control_rpc_failure_with_leader(
            plane, rpc_response, result, "propose");
    }
    rpc_set_result(rpc_response, "{\"accepted\":true}");
    return 0;
}

static int tr_control_rpc_propose_async(Req *request,
                                        Res *http_response,
                                        rpc_request_t *rpc_request,
                                        rpc_response_t *rpc_response)
{
    tr_raft_control_plane_t *plane = tr_control_from_request(request);
    tr_control_propose_receipt_command_t command;
    tr_raft_operation_status_t receipt;
    const char *data;
    uint64_t command_id;
    char json[256];
    size_t json_size;
    int result;

    (void)http_response;
    data = rpc_get_param_string(rpc_request, "data");
    if (plane == NULL || data == NULL ||
        tr_control_get_positive_u64(rpc_request, "command_id", &command_id) !=
            TURBO_OK ||
        strlen(data) > TR_RAFT_MAX_ENTRY_BYTES) {
        rpc_set_error(rpc_response, RPC_ERROR_INVALID_PARAMS,
                      "command_id must be positive and data must fit one Raft entry");
        return RPC_ERROR_INVALID_PARAMS;
    }

    memset(&receipt, 0, sizeof(receipt));
    memset(&command, 0, sizeof(command));
    command.proposal.command_id = command_id;
    command.proposal.data = data;
    command.proposal.data_length = strlen(data);
    command.out_receipt = &receipt;
    result = tr_control_execute_audited(
        plane,
        &(tr_control_audit_scope_t){TR_RAFT_CONTROL_AUDIT_PROPOSE, 0U,
                                    &receipt},
        tr_control_command_propose_receipt, &command, sizeof(command));
    if (result != TURBO_OK) {
        return tr_control_rpc_failure_with_leader(
            plane, rpc_response, result, "propose_async");
    }
    result = tr_control_operation_status_json(&receipt, json, sizeof(json),
                                              &json_size);
    if (result != TURBO_OK) {
        return tr_control_rpc_failure(rpc_response, result,
                                      "propose_async");
    }
    rpc_set_result(rpc_response, json);
    return 0;
}

static int tr_control_rpc_read_index(Req *request,
                                     Res *http_response,
                                     rpc_request_t *rpc_request,
                                     rpc_response_t *rpc_response)
{
    tr_raft_control_plane_t *plane = tr_control_from_request(request);
    uint64_t context_id;
    int result;

    (void) http_response;
    if (plane == NULL ||
        tr_control_get_positive_u64(rpc_request, "context_id", &context_id) !=
            TURBO_OK) {
        rpc_set_error(rpc_response, RPC_ERROR_INVALID_PARAMS,
                      "context_id must be a positive integer");
        return RPC_ERROR_INVALID_PARAMS;
    }
    result = tr_control_execute_audited(
        plane,
        &(tr_control_audit_scope_t){TR_RAFT_CONTROL_AUDIT_READ_INDEX, 0U,
                                    NULL},
        tr_control_command_read_index, &context_id, sizeof(context_id));
    if (result != TURBO_OK) {
        return tr_control_rpc_failure_with_leader(
            plane, rpc_response, result, "readIndex");
    }
    rpc_set_result(rpc_response, "{\"accepted\":true}");
    return 0;
}

static int tr_control_rpc_take_read_state(Req *request,
                                          Res *http_response,
                                          rpc_request_t *rpc_request,
                                          rpc_response_t *rpc_response)
{
    tr_raft_control_plane_t *plane = tr_control_from_request(request);
    tr_raft_read_state_t state;
    tr_control_read_state_command_t command = {&state};
    char json[128];
    int result;

    (void) http_response;
    (void) rpc_request;
    if (plane == NULL) {
        return tr_control_rpc_failure(rpc_response, TURBO_EPROTO,
                                      "takeReadState");
    }
    result = tr_control_execute(plane, tr_control_command_take_read_state,
                                &command, sizeof(command));
    if (result != TURBO_OK) {
        return tr_control_rpc_failure(rpc_response, result,
                                      "takeReadState");
    }
    if (snprintf(json, sizeof(json),
                 "{\"context_id\":%" PRIu64 ",\"index\":%" PRIu64 "}",
                 state.context_id, state.index) < 0) {
        return tr_control_rpc_failure(rpc_response, TURBO_EINVAL,
                                      "takeReadState");
    }
    rpc_set_result(rpc_response, json);
    return 0;
}

static int tr_control_rpc_transfer(Req *request,
                                   Res *http_response,
                                   rpc_request_t *rpc_request,
                                   rpc_response_t *rpc_response)
{
    tr_raft_control_plane_t *plane = tr_control_from_request(request);
    uint64_t node_id;
    int result;

    (void) http_response;
    if (plane == NULL ||
        tr_control_get_positive_u64(rpc_request, "node_id", &node_id) !=
            TURBO_OK) {
        rpc_set_error(rpc_response, RPC_ERROR_INVALID_PARAMS,
                      "node_id must be a positive integer");
        return RPC_ERROR_INVALID_PARAMS;
    }
    {
        tr_raft_node_id_t target = (tr_raft_node_id_t)node_id;
        result = tr_control_execute_audited(
            plane,
            &(tr_control_audit_scope_t){
                TR_RAFT_CONTROL_AUDIT_LEADER_TRANSFER, target, NULL},
            tr_control_command_transfer, &target, sizeof(target));
    }
    if (result != TURBO_OK) {
        return tr_control_rpc_failure_with_leader(
            plane, rpc_response, result, "transferLeadership");
    }
    rpc_set_result(rpc_response, "{\"accepted\":true}");
    return 0;
}

static int tr_control_json_uint64(const json_value_t *value,
                                  uint64_t *output)
{
    const char *text;
    size_t length;
    size_t index;
    uint64_t result = 0U;

    if (value == NULL || output == NULL ||
        turbo_json_type(value) != TURBO_JSON_NUMBER) {
        return TURBO_EINVAL;
    }
    text = turbo_json_number_text(value, &length);
    if (text == NULL || length == 0U || length > 20U) {
        return TURBO_EINVAL;
    }
    for (index = 0U; index < length; ++index) {
        uint64_t digit;

        if (text[index] < '0' || text[index] > '9') {
            return TURBO_EINVAL;
        }
        digit = (uint64_t) (text[index] - '0');
        if (result > (UINT64_MAX - digit) / 10U) {
            return TURBO_EINVAL;
        }
        result = result * 10U + digit;
    }
    if (result == 0U) {
        return TURBO_EINVAL;
    }
    *output = result;
    return TURBO_OK;
}

static int tr_control_json_node_array(const json_value_t *value,
                                      bool required,
                                      tr_raft_node_id_t *nodes,
                                      size_t *out_count)
{
    size_t count;
    size_t index;

    if (nodes == NULL || out_count == NULL) {
        return TURBO_EINVAL;
    }
    *out_count = 0U;
    if (value == NULL) {
        return required ? TURBO_EINVAL : TURBO_OK;
    }
    if (turbo_json_type(value) != TURBO_JSON_ARRAY) {
        return TURBO_EINVAL;
    }
    count = turbo_json_array_size(value);
    if ((required && count == 0U) || count > TR_RAFT_MAX_MEMBERS) {
        return TURBO_EINVAL;
    }
    for (index = 0U; index < count; ++index) {
        if (tr_control_json_uint64(turbo_json_array_get(value, index),
                                  &nodes[index]) != TURBO_OK) {
            return TURBO_EINVAL;
        }
    }
    *out_count = count;
    return TURBO_OK;
}

static int tr_control_parse_membership_change(
    const rpc_request_t *rpc_request,
    tr_raft_membership_change_t *change,
    tr_raft_node_id_t voters[TR_RAFT_MAX_MEMBERS],
    tr_raft_node_id_t learners[TR_RAFT_MAX_MEMBERS])
{
    turbo_json_doc_t *params = NULL;
    json_value_t *transition;
    int result = TURBO_EINVAL;

    if (rpc_request == NULL || rpc_request->params == NULL ||
        change == NULL || voters == NULL || learners == NULL) {
        return TURBO_EINVAL;
    }
    if (turbo_parse_json((const uint8_t *) rpc_request->params,
                         strlen(rpc_request->params), &params) != TURBO_OK ||
        params == NULL || turbo_json_type(params) != TURBO_JSON_OBJECT) {
        turbo_free_json(&params);
        return TURBO_EINVAL;
    }
    memset(change, 0, sizeof(*change));
    transition = turbo_json_object_get(params, "transition_id");
    if (tr_control_json_uint64(transition, &change->transition_id) !=
            TURBO_OK ||
        tr_control_json_node_array(turbo_json_object_get(params, "voters"),
                                   true,
                                   voters, &change->voter_count) != TURBO_OK ||
        tr_control_json_node_array(turbo_json_object_get(params, "learners"),
                                   false,
                                   learners, &change->learner_count) !=
            TURBO_OK) {
        goto done;
    }
    change->voters = voters;
    change->learners = change->learner_count == 0U ? NULL : learners;
    result = TURBO_OK;

done:
    turbo_free_json(&params);
    return result;
}

static int tr_control_rpc_change_membership(
    Req *request,
    Res *http_response,
    rpc_request_t *rpc_request,
    rpc_response_t *rpc_response)
{
    tr_raft_control_plane_t *plane = tr_control_from_request(request);
    tr_raft_node_id_t voters[TR_RAFT_MAX_MEMBERS];
    tr_raft_node_id_t learners[TR_RAFT_MAX_MEMBERS];
    tr_raft_membership_change_t change;
    int result;

    (void) http_response;
    if (plane == NULL ||
        tr_control_parse_membership_change(rpc_request, &change, voters,
                                           learners) != TURBO_OK) {
        rpc_set_error(rpc_response, RPC_ERROR_INVALID_PARAMS,
                      "transition_id and bounded uint64 voters/learners arrays are required");
        return RPC_ERROR_INVALID_PARAMS;
    }
    result = tr_control_execute_audited(
        plane,
        &(tr_control_audit_scope_t){
            TR_RAFT_CONTROL_AUDIT_CHANGE_MEMBERSHIP, 0U, NULL},
        tr_control_command_membership, &change, sizeof(change));
    if (result == TURBO_EINVAL) {
        rpc_set_error(rpc_response, RPC_ERROR_INVALID_PARAMS,
                      "membership violates ordering or role invariants");
        return RPC_ERROR_INVALID_PARAMS;
    }
    if (result != TURBO_OK) {
        return tr_control_rpc_failure_with_leader(
            plane, rpc_response, result, "changeMembership");
    }
    rpc_set_result(rpc_response, "{\"accepted\":true}");
    return 0;
}

static int tr_control_rpc_member_action(
    Req *request,
    Res *http_response,
    rpc_request_t *rpc_request,
    rpc_response_t *rpc_response,
    tr_control_member_action_t action,
    const char *operation)
{
    tr_raft_control_plane_t *plane = tr_control_from_request(request);
    tr_control_member_command_t command;
    tr_raft_operation_status_t receipt;
    uint64_t node_id;
    char json[256];
    size_t json_size;
    int result;

    (void) http_response;
    memset(&command, 0, sizeof(command));
    memset(&receipt, 0, sizeof(receipt));
    if (plane == NULL ||
        tr_control_get_positive_u64(rpc_request, "node_id", &node_id) !=
            TURBO_OK ||
        tr_control_get_positive_u64(rpc_request, "transition_id",
                                    &command.transition_id) != TURBO_OK) {
        rpc_set_error(rpc_response, RPC_ERROR_INVALID_PARAMS,
                      "node_id and transition_id must be positive integers");
        return RPC_ERROR_INVALID_PARAMS;
    }
    command.action = action;
    command.node_id = (tr_raft_node_id_t) node_id;
    command.out_receipt = &receipt;
    result = tr_control_execute_audited(
        plane,
        &(tr_control_audit_scope_t){
            action == TR_CONTROL_MEMBER_ADD_LEARNER
                ? TR_RAFT_CONTROL_AUDIT_ADD_LEARNER
                : action == TR_CONTROL_MEMBER_PROMOTE
                      ? TR_RAFT_CONTROL_AUDIT_PROMOTE
                      : TR_RAFT_CONTROL_AUDIT_REMOVE,
            command.node_id, &receipt},
        tr_control_command_member_action, &command, sizeof(command));
    if (result == TURBO_EINVAL) {
        rpc_set_error(rpc_response, RPC_ERROR_INVALID_PARAMS,
                      "member action is invalid for the current stable configuration");
        return RPC_ERROR_INVALID_PARAMS;
    }
    if (result != TURBO_OK) {
        return tr_control_rpc_failure_with_leader(
            plane, rpc_response, result, operation);
    }
    result = tr_control_operation_status_json(&receipt, json, sizeof(json),
                                              &json_size);
    if (result != TURBO_OK) {
        return tr_control_rpc_failure(rpc_response, result, operation);
    }
    rpc_set_result(rpc_response, json);
    return 0;
}

static int tr_control_rpc_add_learner(Req *request,
                                      Res *http_response,
                                      rpc_request_t *rpc_request,
                                      rpc_response_t *rpc_response)
{
    return tr_control_rpc_member_action(
        request, http_response, rpc_request, rpc_response,
        TR_CONTROL_MEMBER_ADD_LEARNER, "member.add_learner");
}

static int tr_control_rpc_promote(Req *request,
                                  Res *http_response,
                                  rpc_request_t *rpc_request,
                                  rpc_response_t *rpc_response)
{
    return tr_control_rpc_member_action(
        request, http_response, rpc_request, rpc_response,
        TR_CONTROL_MEMBER_PROMOTE, "member.promote");
}

static int tr_control_rpc_remove(Req *request,
                                 Res *http_response,
                                 rpc_request_t *rpc_request,
                                 rpc_response_t *rpc_response)
{
    return tr_control_rpc_member_action(
        request, http_response, rpc_request, rpc_response,
        TR_CONTROL_MEMBER_REMOVE, "member.remove");
}

static int tr_control_rpc_snapshot_trigger(
    Req *request,
    Res *http_response,
    rpc_request_t *rpc_request,
    rpc_response_t *rpc_response)
{
    tr_raft_control_plane_t *plane = tr_control_from_request(request);
    int result;

    (void) http_response;
    (void) rpc_request;
    if (plane == NULL) {
        return tr_control_rpc_failure(rpc_response, TURBO_EPROTO,
                                      "snapshot.trigger");
    }
    result = tr_control_execute_audited(
        plane,
        &(tr_control_audit_scope_t){TR_RAFT_CONTROL_AUDIT_SNAPSHOT, 0U,
                                    NULL},
        tr_control_command_snapshot, NULL, 0U);
    if (result != TURBO_OK) {
        return tr_control_rpc_failure_with_leader(
            plane, rpc_response, result, "snapshot.trigger");
    }
    rpc_set_result(rpc_response, "{\"accepted\":true}");
    return 0;
}

static void tr_control_status_route(Req *request, Res *response)
{
    tr_raft_control_plane_t *plane = tr_control_from_request(request);
    char html[TR_RAFT_CONTROL_HTML_CAPACITY];
    size_t size;

    if (plane == NULL ||
        tr_raft_control_plane_render_status_html(
            plane, html, sizeof(html), &size) != TURBO_OK) {
        send_html(response, 500,
                  "<div class=\"health bad\">STATUS UNAVAILABLE</div>");
        return;
    }
    reply(response, 200, "text/html; charset=utf-8", html, size);
}

static void tr_control_ui_route(Req *request, Res *response)
{
    static const char page[] =
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">"
        "<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">"
        "<title>TurboRaft Control</title>"
        "<script src=\"https://cdn.jsdelivr.net/npm/htmx.org@2.0.10/dist/htmx.min.js\" "
        "integrity=\"sha384-H5SrcfygHmAuTDZphMHqBJLc3FhssKjG7w/CeCpFReSfwBWDTKpkzPP8c+cLsK+V\" "
        "crossorigin=\"anonymous\"></script>"
        "<style>:root{--ink:#14211d;--paper:#ece9df;--signal:#ff5b35;"
        "--line:#93a399}*{box-sizing:border-box}body{margin:0;color:var(--ink);"
        "background:linear-gradient(120deg,rgba(255,91,53,.14),transparent 42%),"
        "repeating-linear-gradient(90deg,transparent 0 39px,rgba(20,33,29,.06) 40px),"
        "var(--paper);font-family:'IBM Plex Mono','Cascadia Code',monospace}"
        "main{max-width:1100px;margin:auto;padding:clamp(24px,6vw,72px)}"
        "header{border-top:8px solid var(--ink);display:flex;justify-content:space-between;"
        "align-items:end;padding:18px 0 38px}h1{font-size:clamp(2rem,7vw,5.5rem);"
        "line-height:.84;letter-spacing:-.09em;margin:0}header p{max-width:28ch;text-align:right}"
        ".grid{display:grid;grid-template-columns:repeat(4,1fr);gap:10px}"
        ".metric{background:rgba(255,255,255,.58);border:1px solid var(--line);"
        "min-height:112px;padding:16px;display:flex;flex-direction:column;justify-content:space-between}"
        ".metric.hero{grid-column:span 2;background:var(--ink);color:var(--paper)}"
        ".metric span{font-size:.7rem;letter-spacing:.14em}.metric strong{font-size:1.45rem}"
        ".health{grid-column:1/-1;padding:14px 16px;font-weight:700;border-left:8px solid}"
        ".health.good{border-color:#168b62}.health.bad{border-color:var(--signal)}"
        ".controls{margin-top:34px;border-top:8px solid var(--ink);padding-top:18px}"
        ".controls h2{font-size:clamp(1.5rem,4vw,3rem);letter-spacing:-.06em;margin:0 0 18px}"
        ".auth,.action{display:grid;grid-template-columns:1fr 1fr auto;gap:10px;margin:10px 0}"
        ".action.wide{grid-template-columns:1fr 1fr 1fr auto}label{display:flex;flex-direction:column;"
        "gap:5px;font-size:.68rem;letter-spacing:.1em}input{width:100%;border:1px solid var(--line);"
        "background:rgba(255,255,255,.7);padding:11px;color:var(--ink);font:inherit}"
        "button{align-self:end;border:0;background:var(--signal);color:#fff;padding:12px 18px;"
        "font:700 .72rem 'IBM Plex Mono','Cascadia Code',monospace;letter-spacing:.08em;cursor:pointer}"
        "button:hover{background:var(--ink)}#rpc-result{min-height:74px;white-space:pre-wrap;"
        "background:var(--ink);color:#b9f5d8;padding:15px;overflow:auto}"
        "footer{margin-top:34px;border-top:1px solid var(--line);padding-top:12px;font-size:.75rem}"
        "@media(max-width:720px){header{display:block}header p{text-align:left}.grid{grid-template-columns:repeat(2,1fr)}"
        ".auth,.action,.action.wide{grid-template-columns:1fr}button{width:100%}}"
        "</style></head><body><main><header><h1>TURBO<br>RAFT</h1>"
        "<p>LIVE CONSENSUS CONTROL SURFACE<br>JSON-RPC "
        TR_RAFT_CONTROL_RPC_ENDPOINT
        "</p></header><section id=\"raft-status\" class=\"grid\" hx-get=\""
        TR_RAFT_CONTROL_STATUS_PATH
        "\" hx-trigger=\"load, refresh, every 1s\" hx-swap=\"innerHTML\">"
        "<div class=\"health\">CONNECTING TO NODE</div></section>"
        "<section class=\"controls\"><h2>DIAGNOSTICS</h2>"
        "<form class=\"action\" data-method=\"raft.members\""
        TR_RAFT_CONTROL_HTMX_RPC_ATTRS "><button>MEMBERS</button></form>"
        "<form class=\"action\" data-method=\"raft.progress\""
        TR_RAFT_CONTROL_HTMX_RPC_ATTRS "><button>PEER PROGRESS</button></form>"
        "<form class=\"action\" data-method=\"raft.storage.status\""
        TR_RAFT_CONTROL_HTMX_RPC_ATTRS "><button>DURABILITY STATUS</button></form>"
        "<h2>NODE COMMANDS</h2>"
        "<div class=\"auth\"><label>BEARER TOKEN<input id=\"rpc-token\" type=\"password\" "
        "autocomplete=\"off\" placeholder=\"required for mutation RPC\"></label></div>"
        "<form class=\"action\" data-method=\"raft.tick\""
        TR_RAFT_CONTROL_HTMX_RPC_ATTRS "><label>ELAPSED TICKS"
        "<input name=\"elapsed_ticks\" type=\"number\" min=\"1\" value=\"1\" required></label>"
        "<label>NEXT TIMEOUT<input name=\"next_timeout_ticks\" type=\"number\" min=\"1\" value=\"3\" required>"
        "</label><button>ADVANCE CLOCK</button></form>"
        "<form class=\"action\" data-method=\"raft.propose_async\""
        TR_RAFT_CONTROL_HTMX_RPC_ATTRS "><label>COMMAND ID"
        "<input name=\"command_id\" type=\"number\" min=\"1\" required></label>"
        "<label>COMMAND DATA<input name=\"data\" maxlength=\"512\" required></label>"
        "<button>PROPOSE</button></form>"
        "<form class=\"action\" data-method=\"raft.read_index\""
        TR_RAFT_CONTROL_HTMX_RPC_ATTRS "><label>READ CONTEXT"
        "<input name=\"context_id\" type=\"number\" min=\"1\" required></label>"
        "<button>READ BARRIER</button></form>"
        "<form class=\"action\" data-method=\"raft.takeReadState\""
        TR_RAFT_CONTROL_HTMX_RPC_ATTRS "><button>TAKE READ STATE</button></form>"
        "<form class=\"action\" data-method=\"raft.leader.transfer\""
        TR_RAFT_CONTROL_HTMX_RPC_ATTRS "><label>TARGET NODE"
        "<input name=\"node_id\" type=\"number\" min=\"1\" required></label>"
        "<button>TRANSFER LEADER</button></form>"
        "<form class=\"action wide\" data-method=\"raft.changeMembership\""
        TR_RAFT_CONTROL_HTMX_RPC_ATTRS "><label>TRANSITION ID"
        "<input name=\"transition_id\" type=\"number\" min=\"1\" required></label>"
        "<label>VOTERS JSON<input name=\"voters\" data-json value=\"[]\" required></label>"
        "<label>LEARNERS JSON<input name=\"learners\" data-json value=\"[]\" required></label>"
        "<button>CHANGE MEMBERS</button></form>"
        "<form class=\"action wide\" data-method=\"raft.member.add_learner\""
        TR_RAFT_CONTROL_HTMX_RPC_ATTRS "><label>TRANSITION ID"
        "<input name=\"transition_id\" type=\"number\" min=\"1\" required></label>"
        "<label>NODE ID<input name=\"node_id\" type=\"number\" min=\"1\" required></label>"
        "<button>ADD LEARNER</button></form>"
        "<form class=\"action wide\" data-method=\"raft.member.promote\""
        TR_RAFT_CONTROL_HTMX_RPC_ATTRS "><label>TRANSITION ID"
        "<input name=\"transition_id\" type=\"number\" min=\"1\" required></label>"
        "<label>NODE ID<input name=\"node_id\" type=\"number\" min=\"1\" required></label>"
        "<button>PROMOTE</button></form>"
        "<form class=\"action wide\" data-method=\"raft.member.remove\""
        TR_RAFT_CONTROL_HTMX_RPC_ATTRS "><label>TRANSITION ID"
        "<input name=\"transition_id\" type=\"number\" min=\"1\" required></label>"
        "<label>NODE ID<input name=\"node_id\" type=\"number\" min=\"1\" required></label>"
        "<button>REMOVE MEMBER</button></form>"
        "<form id=\"operation-tracker\" class=\"action wide\" data-method=\"raft.operation.status\""
        TR_RAFT_CONTROL_HTMX_RPC_ATTRS
        " hx-trigger=\"submit, receipt-poll\"><label>RECEIPT TERM"
        "<input name=\"term\" type=\"number\" min=\"1\" required></label>"
        "<label>RECEIPT INDEX<input name=\"index\" type=\"number\" min=\"1\" required></label>"
        "<button>TRACK OPERATION</button></form>"
        "<form class=\"action\" data-method=\"raft.snapshot.trigger\""
        TR_RAFT_CONTROL_HTMX_RPC_ATTRS "><button>CREATE SNAPSHOT</button></form>"
        "<pre id=\"rpc-result\">READY</pre></section>"
        "<script>let raftRpcId=1;htmx.defineExtension('raft-rpc',{"
        "encodeParameters:function(xhr,_parameters,element){const form=element.closest('form[data-method]'),params={};"
        "for(const input of form.elements){if(!input.name)continue;params[input.name]=input.hasAttribute('data-json')?"
        "JSON.parse(input.value):input.type==='number'?Number(input.value):input.value}"
        "xhr.setRequestHeader('Content-Type','application/json');const token=document.querySelector('#rpc-token').value.trim();"
        "if(token)xhr.setRequestHeader('Authorization','Bearer '+token);return JSON.stringify({jsonrpc:'2.0',"
        "id:raftRpcId++,method:form.dataset.method,params:params})}});"
        "document.body.addEventListener('htmx:afterRequest',function(event){const form=event.detail.elt.closest"
        "('form[data-method]');if(!form)return;try{const payload=JSON.parse(event.detail.xhr.responseText);"
        "if(!payload.error){htmx.trigger('#raft-status','refresh');const receipt=payload.result||{},"
        "tracker=document.querySelector('#operation-tracker'),state=receipt.state;"
        "if(form.dataset.method!=='raft.operation.status'&&receipt.term&&receipt.index){"
        "tracker.elements.term.value=receipt.term;tracker.elements.index.value=receipt.index;"
        "if(state==='PENDING'||state==='COMMITTED')setTimeout(function(){htmx.trigger(tracker,'receipt-poll')},1000)}"
        "else if(form.dataset.method==='raft.operation.status'&&(state==='PENDING'||state==='COMMITTED'))"
        "setTimeout(function(){htmx.trigger(form,'receipt-poll')},1000)}}catch(_error){}});"
        "for(const form of document.querySelectorAll('form[data-method]'))form.addEventListener('submit',function(event){"
        "try{for(const input of form.querySelectorAll('[data-json]'))JSON.parse(input.value)}catch(error){event.preventDefault();"
        "document.querySelector('#rpc-result').textContent='INVALID JSON: '+error.message}});"
        "</script>"
        "<footer>HTMX POLL 1S / OWNER-CONTEXT GUARDED</footer></main></body></html>";

    (void) request;
    reply(response, 200, "text/html; charset=utf-8", page,
          sizeof(page) - 1U);
}

static int tr_control_register_rpc_methods(
    rpc_context_t *rpc,
    bool allow_unauthenticated_mutations)
{
    rpc_method_t methods[] = {
        {"raft.status", tr_control_rpc_status,
         "Return the current Raft node status", 0},
        {"raft.members", tr_control_rpc_members,
         "Return the committed membership configuration", 0},
        {"raft.progress", tr_control_rpc_progress,
         "Return bounded per-peer replication progress", 0},
        {"raft.storage.status", tr_control_rpc_storage_status,
         "Return the last durability barrier result", 0},
        {"raft.operation.status", tr_control_rpc_operation_status,
         "Query an accepted Raft operation receipt", 0},
        {"raft.tick", tr_control_rpc_tick,
         "Advance the Raft logical clock", 0},
        {"raft.propose", tr_control_rpc_propose,
         "Propose one bounded command", 0},
        {"raft.propose_async", tr_control_rpc_propose_async,
         "Propose one bounded command and return a receipt", 0},
        {"raft.readIndex", tr_control_rpc_read_index,
         "Start a linearizable read barrier", 0},
        {"raft.read_index", tr_control_rpc_read_index,
         "Start a linearizable read barrier", 0},
        {"raft.takeReadState", tr_control_rpc_take_read_state,
         "Take one completed linearizable read state", 0},
        {"raft.transferLeadership", tr_control_rpc_transfer,
         "Transfer leadership to a voting peer", 0},
        {"raft.leader.transfer", tr_control_rpc_transfer,
         "Transfer leadership to a voting peer", 0},
        {"raft.changeMembership", tr_control_rpc_change_membership,
         "Start a Joint Consensus membership transition", 0},
        {"raft.member.add_learner", tr_control_rpc_add_learner,
         "Add one learner through Joint Consensus", 0},
        {"raft.member.promote", tr_control_rpc_promote,
         "Promote one learner through Joint Consensus", 0},
        {"raft.member.remove", tr_control_rpc_remove,
         "Remove one member through Joint Consensus", 0},
        {"raft.snapshot.trigger", tr_control_rpc_snapshot_trigger,
         "Create and durably store one application snapshot", 0}
    };
    size_t index;

    if (!allow_unauthenticated_mutations) {
        for (index = 4U; index < sizeof(methods) / sizeof(methods[0]);
             ++index) {
            methods[index].requires_auth = 1;
        }
    }
    for (index = 0U; index < sizeof(methods) / sizeof(methods[0]); ++index) {
        if (rpc_register_method(rpc, &methods[index]) != 0) {
            return TURBO_EPROTO;
        }
    }
    return TURBO_OK;
}

int tr_raft_control_plane_create(
    const tr_raft_control_plane_config_t *config,
    tr_raft_control_plane_t **out_plane)
{
    tr_raft_control_plane_t *plane;
    tr_raft_service_status_t service_status;
    rpc_config_t rpc_config;
    int result;

    if (config == NULL || config->service == NULL || out_plane == NULL ||
        config->owner_bridge_timeout_ms >
            TR_RAFT_CONTROL_MAX_OWNER_BRIDGE_TIMEOUT_MS) {
        return TURBO_EINVAL;
    }
#if defined(TURBORAFT_HAS_SERVICE_OWNER)
    if (config->owner != NULL &&
        tr_raft_service_owner_service(config->owner) != config->service) {
        return TURBO_EINVAL;
    }
#else
    if (config->owner != NULL) {
        return TURBO_EINVAL;
    }
#endif
    *out_plane = NULL;
    plane = (tr_raft_control_plane_t *) calloc(1U, sizeof(*plane));
    if (plane == NULL) {
        return TURBO_ENOMEM;
    }
    plane->service = config->service;
    plane->owner = config->owner;
    plane->audit = config->audit;
    plane->owner_bridge_timeout_ms =
        config->owner_bridge_timeout_ms == 0U
            ? TR_RAFT_CONTROL_DEFAULT_OWNER_BRIDGE_TIMEOUT_MS
            : config->owner_bridge_timeout_ms;
    plane->local_node_id = config->local_node_id;
    if (plane->local_node_id == 0U) {
        result = tr_raft_service_status(config->service, &service_status);
        if (result != TURBO_OK) {
            free(plane);
            return result;
        }
        plane->local_node_id = service_status.core.self_id;
    }
    plane->app = config->app != NULL ? config->app : iris_app_default();
    if (plane->app == NULL ||
        iris_app_lookup_rpc_context(plane->app, tr_control_binding_path) !=
            NULL ||
        iris_app_bind_rpc_context(plane->app, tr_control_binding_path,
                                  plane) != 0) {
        free(plane);
        return TURBO_EBUSY;
    }

    memset(&rpc_config, 0, sizeof(rpc_config));
    rpc_config.endpoint = TR_RAFT_CONTROL_RPC_ENDPOINT;
    rpc_config.default_protocol = RPC_PROTOCOL_JSON;
    rpc_config.enable_introspection = 1;
    rpc_config.enable_batch = 1;
    rpc_config.max_batch_size = TR_RAFT_CONTROL_MAX_BATCH_SIZE;
    rpc_config.auth_decision = tr_control_rpc_auth_decision;
    rpc_config.auth_decision_context = plane;
    rpc_config.max_request_size = TR_RAFT_CONTROL_MAX_REQUEST_BYTES;
    rpc_config.max_response_size = TR_RAFT_CONTROL_MAX_RESPONSE_BYTES;
    plane->rpc = rpc_init(&rpc_config);
    if (plane->rpc == NULL) {
        result = TURBO_ENOMEM;
        goto fail;
    }
    result = tr_control_register_rpc_methods(
        plane->rpc, config->allow_unauthenticated_mutations);
    if (result != TURBO_OK ||
        rpc_setup_endpoint_on_app(plane->rpc, plane->app) != 0) {
        result = TURBO_EPROTO;
        goto fail;
    }
    iris_app_get(plane->app, TR_RAFT_CONTROL_UI_PATH, tr_control_ui_route);
    iris_app_get(plane->app, TR_RAFT_CONTROL_STATUS_PATH,
                 tr_control_status_route);
    *out_plane = plane;
    return TURBO_OK;

fail:
    rpc_destroy(plane->rpc);
    iris_app_unbind_rpc_context(plane->app, tr_control_binding_path, plane);
    free(plane);
    return result;
}

void tr_raft_control_plane_destroy(tr_raft_control_plane_t *plane)
{
    if (plane == NULL) {
        return;
    }
    iris_app_unbind_rpc_context(plane->app, tr_control_binding_path, plane);
    rpc_destroy(plane->rpc);
    free(plane);
}

struct iris_app *tr_raft_control_plane_app(
    const tr_raft_control_plane_t *plane)
{
    return plane == NULL ? NULL : plane->app;
}

typedef struct tr_control_operation_query {
    tr_raft_term_t term;
    tr_raft_index_t index;
    tr_raft_operation_status_t *out_status;
} tr_control_operation_query_t;

static int tr_control_command_operation_status(tr_raft_service_t *service,
                                                const void *payload,
                                                size_t payload_size)
{
    const tr_control_operation_query_t *query =
        (const tr_control_operation_query_t *)payload;

    if (query == NULL || payload_size != sizeof(*query) ||
        query->out_status == NULL) {
        return TURBO_EINVAL;
    }
    return tr_raft_service_operation_status(service, query->term, query->index,
                                            query->out_status);
}

static int tr_control_rpc_operation_status(Req *request,
                                           Res *response,
                                           rpc_request_t *rpc_request,
                                           rpc_response_t *rpc_response)
{
    tr_raft_control_plane_t *plane = tr_control_from_request(request);
    tr_control_operation_query_t query;
    tr_raft_operation_status_t status;
    int64_t term;
    int64_t index;
    char json[256];
    size_t json_size;
    int result;

    (void)response;
    if (plane == NULL) {
        rpc_set_error(rpc_response, RPC_ERROR_INTERNAL,
                      "control plane context unavailable");
        return TURBO_OK;
    }
    if (rpc_get_param_int(rpc_request, "term", &term) != 0 || term <= 0 ||
        rpc_get_param_int(rpc_request, "index", &index) != 0 || index <= 0) {
        rpc_set_error(rpc_response, RPC_ERROR_INVALID_PARAMS,
                      "term and index must be positive integers");
        return TURBO_OK;
    }

    memset(&status, 0, sizeof(status));
    query.term = (tr_raft_term_t)term;
    query.index = (tr_raft_index_t)index;
    query.out_status = &status;
    result = tr_control_execute(plane, tr_control_command_operation_status,
                                &query, sizeof(query));
    if (result != TURBO_OK) {
        return tr_control_rpc_failure(rpc_response, result,
                                      "operation.status");
    }

    result = tr_control_operation_status_json(&status, json, sizeof(json),
                                              &json_size);
    if (result != TURBO_OK) {
        return tr_control_rpc_failure(rpc_response, result,
                                      "operation.status");
    }
    rpc_set_result(rpc_response, json);
    return TURBO_OK;
}
