#ifndef TURBORAFT_RAFT_CORONET_TRANSPORT_INTERNAL_H
#define TURBORAFT_RAFT_CORONET_TRANSPORT_INTERNAL_H

#include <turboraft/raft_coronet_transport.h>

int tr_raft_coronet_inbound_service_admit_internal(
    tr_raft_coronet_inbound_service_t *service,
    coro_socket_t *socket,
    tr_raft_node_id_t *out_peer_node_id);

int tr_raft_coronet_peer_manager_receive_peer(
    tr_raft_coronet_peer_manager_t *manager,
    tr_raft_node_id_t peer_node_id);

int tr_raft_coronet_peer_manager_release_peer(
    tr_raft_coronet_peer_manager_t *manager,
    tr_raft_node_id_t peer_node_id);

int tr_raft_coronet_peer_manager_close_peer(
    tr_raft_coronet_peer_manager_t *manager,
    tr_raft_node_id_t peer_node_id);

int tr_raft_coronet_peer_manager_close_all(
    tr_raft_coronet_peer_manager_t *manager);

int tr_raft_coronet_v2_append_part(
    const tr_raft_message_t *message,
    size_t entry_index,
    tr_raft_message_t *out_message);

#endif
