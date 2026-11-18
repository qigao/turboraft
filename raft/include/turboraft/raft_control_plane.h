#ifndef TURBORAFT_RAFT_CONTROL_PLANE_H
#define TURBORAFT_RAFT_CONTROL_PLANE_H

#include <turboraft/raft_service.h>

#include <chttp_app/application.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TR_RAFT_CONTROL_RPC_ENDPOINT "/raft/rpc"
#define TR_RAFT_CONTROL_STATUS_PATH "/raft/status"
typedef struct tr_raft_control_plane tr_raft_control_plane_t;

/**
 * Copies one status snapshot for the control plane. Application invokes this
 * callback on its worker pool, so implementations must use an
 * executor/mailbox when the Raft service belongs to another owner.
 */
typedef int (*tr_raft_control_status_provider_fn)(
    void *context, tr_raft_service_status_t *out_status);

typedef struct tr_raft_control_plane_config {
    /** Explicit Application bounds and optional application-owned Controllers. */
    chttp_application_config server;
    tr_raft_control_status_provider_fn status_provider;
    void *status_context;
    /** Required bounded shutdown timeout; zero is rejected. */
    uint32_t stop_timeout_ms;
} tr_raft_control_plane_config_t;

/**
 * Initializes an owned Application with raft.status and GET /raft/status.
 * Configuration metadata and provider context borrow the plane lifetime.
 * On initialization failure, a non-NULL output retains cleanup ownership when
 * Application rollback could not settle; destroy that plane explicitly.
 */
int tr_raft_control_plane_create(
    const tr_raft_control_plane_config_t *config,
    tr_raft_control_plane_t **out_plane);
int tr_raft_control_plane_start(tr_raft_control_plane_t *plane);
int tr_raft_control_plane_port(const tr_raft_control_plane_t *plane,
                               uint16_t *out_port);
int tr_raft_control_plane_stop(tr_raft_control_plane_t *plane);
int tr_raft_control_plane_destroy(tr_raft_control_plane_t *plane);

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
