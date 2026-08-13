#ifndef TURBORAFT_RAFT_SNAPSHOT_MANAGER_H
#define TURBORAFT_RAFT_SNAPSHOT_MANAGER_H

#include <turboraft/raft_coronet_peer_service.h>
#include <turboraft/raft_core.h>
#include <turboraft/raft_snapshot_sender.h>

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct tr_raft_snapshot_manager tr_raft_snapshot_manager_t;

typedef int (*tr_raft_snapshot_manager_payload_enqueue_fn)(
    void *context,
    const tr_raft_coronet_payload_t *payload);

typedef int (*tr_raft_snapshot_provider_fn)(
    void *context,
    tr_raft_index_t required_index,
    tr_raft_term_t required_term,
    tr_raft_snapshot_point_t *out_point,
    uint8_t *buffer,
    size_t capacity,
    size_t *out_size);

typedef int (*tr_raft_snapshot_complete_fn)(void *context,
                                            tr_raft_node_id_t peer_id,
                                            tr_raft_index_t snapshot_index);

typedef struct tr_raft_snapshot_manager_config {
    tr_raft_node_id_t self_id;
    /* Strictly ascending, unique, non-zero peer IDs excluding self_id. */
    const tr_raft_node_id_t *peer_node_ids;
    size_t peer_count;
    size_t max_snapshot_bytes;
    /* Zero values preserve V4 512-byte stop-and-wait transfers. */
    size_t snapshot_chunk_size;
    size_t snapshot_max_inflight_chunks;
    /* Optional transport seam; leave null when bind_peer_service is used. */
    tr_raft_snapshot_manager_payload_enqueue_fn enqueue;
    void *enqueue_context;
    /* Required when enqueue_request is used. */
    tr_raft_snapshot_provider_fn provider;
    void *provider_context;
    /* Called after the remote peer acknowledges the complete snapshot. */
    tr_raft_snapshot_complete_fn complete;
    void *complete_context;
} tr_raft_snapshot_manager_config_t;

int tr_raft_snapshot_manager_create(
    const tr_raft_snapshot_manager_config_t *config,
    tr_raft_snapshot_manager_t **out_manager);

void tr_raft_snapshot_manager_destroy(tr_raft_snapshot_manager_t *manager);

int tr_raft_snapshot_manager_bind_peer_service(
    tr_raft_snapshot_manager_t *manager,
    tr_raft_coronet_peer_service_t *peer_service);

int tr_raft_snapshot_manager_begin(
    tr_raft_snapshot_manager_t *manager,
    tr_raft_node_id_t peer_id,
    tr_raft_term_t leader_term,
    tr_raft_index_t snapshot_index,
    tr_raft_term_t snapshot_term,
    const tr_raft_conf_t *configuration,
    const uint8_t *data,
    size_t size);

int tr_raft_snapshot_manager_enqueue_request(
    void *context,
    const tr_raft_snapshot_request_t *request);

int tr_raft_snapshot_manager_handle_payload(
    void *context,
    const tr_raft_coronet_payload_t *payload);

int tr_raft_snapshot_manager_resume(
    tr_raft_snapshot_manager_t *manager,
    tr_raft_node_id_t peer_id);

int tr_raft_snapshot_manager_get_status(
    const tr_raft_snapshot_manager_t *manager,
    tr_raft_node_id_t peer_id,
    tr_raft_snapshot_sender_status_t *out_status);

#ifdef __cplusplus
}
#endif

#endif
