#ifndef TURBORAFT_RAFT_MEMBERSHIP_H
#define TURBORAFT_RAFT_MEMBERSHIP_H

#include "raft_configuration.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

typedef tr_raft_conf_t tr_raft_membership_t;

int tr_raft_membership_stable(const tr_raft_node_id_t *voters,
                              size_t voter_count,
                              const tr_raft_node_id_t *learners,
                              size_t learner_count,
                              tr_raft_membership_t *membership);

int tr_raft_membership_joint(const tr_raft_membership_t *current,
                             const tr_raft_node_id_t *target_voters,
                             size_t target_voter_count,
                             const tr_raft_node_id_t *target_learners,
                             size_t target_learner_count,
                             uint64_t transition_id,
                             tr_raft_membership_t *joint);

int tr_raft_membership_final(const tr_raft_membership_t *joint,
                             tr_raft_membership_t *final_membership);

int tr_raft_membership_index(const tr_raft_membership_t *membership,
                             tr_raft_node_id_t node_id);

uint8_t tr_raft_membership_roles(const tr_raft_membership_t *membership,
                                 tr_raft_node_id_t node_id);

bool tr_raft_membership_is_voter(const tr_raft_membership_t *membership,
                                 tr_raft_node_id_t node_id);

bool tr_raft_membership_has_quorum(const tr_raft_membership_t *membership,
                                   uint32_t acknowledgements);

bool tr_raft_membership_match_quorum(
    const tr_raft_membership_t *membership,
    const tr_raft_index_t *match_indices,
    tr_raft_index_t candidate_index);

#endif
