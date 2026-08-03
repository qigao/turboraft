#ifndef TURBORAFT_RAFT_CONTROL_PLANE_H
#define TURBORAFT_RAFT_CONTROL_PLANE_H

#include <turboraft/raft_service.h>
#include <turboraft/raft_control_audit.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TR_RAFT_CONTROL_RPC_ENDPOINT "/raft/rpc"
#define TR_RAFT_CONTROL_UI_PATH "/raft"
#define TR_RAFT_CONTROL_STATUS_PATH "/raft/status"

struct iris_app;

typedef struct tr_raft_control_plane tr_raft_control_plane_t;

#define TR_RAFT_CONTROL_DEFAULT_OWNER_BRIDGE_TIMEOUT_MS 5000U
#define TR_RAFT_CONTROL_MAX_OWNER_BRIDGE_TIMEOUT_MS 60000U
typedef struct tr_raft_service_owner tr_raft_service_owner_t;

typedef struct tr_raft_control_plane_config {
    tr_raft_service_t *service;
    /* Optional owner; RPC may run on another concurrently driven context. */
    tr_raft_service_owner_t *owner;
    /* Optional borrowed audit handle; it must outlive the plane. */
    tr_raft_control_audit_t *audit;
    /* Development-only escape hatch; production mutation RPCs require auth. */
    bool allow_unauthenticated_mutations;
    /* Zero selects TR_RAFT_CONTROL_DEFAULT_OWNER_BRIDGE_TIMEOUT_MS. */
    uint64_t owner_bridge_timeout_ms;
    /* Optional immutable audit identity; zero derives it during creation. */
    tr_raft_node_id_t local_node_id;
    /* Borrowed Iris app; production callers should inject an explicit app. */
    struct iris_app *app;
} tr_raft_control_plane_config_t;

/*
 * Binds JSON-RPC and HTMX routes to config.app. A null app retains the legacy
 * default-app behavior. The caller owns app and service, and both must outlive
 * the plane. Raft operations execute on the service's single owner-loop.
 */
int tr_raft_control_plane_create(
    const tr_raft_control_plane_config_t *config,
    tr_raft_control_plane_t **out_plane);

/* Call only after the Iris listener has stopped processing requests. */
void tr_raft_control_plane_destroy(tr_raft_control_plane_t *plane);

/* Returns the borrowed Iris app used by the plane. */
struct iris_app *tr_raft_control_plane_app(
    const tr_raft_control_plane_t *plane);

/* Render bounded status representations used by RPC and HTMX handlers. */
int tr_raft_control_plane_render_status_json(
    const tr_raft_control_plane_t *plane,
    char *output,
    size_t capacity,
    size_t *out_size);

int tr_raft_control_plane_render_status_html(
    const tr_raft_control_plane_t *plane,
    char *output,
    size_t capacity,
    size_t *out_size);

#ifdef __cplusplus
}
#endif

#endif
