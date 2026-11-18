#ifndef TURBORAFT_RAFT_MEMBERSHIP_TRANSITION_H
#define TURBORAFT_RAFT_MEMBERSHIP_TRANSITION_H

#include "raft_membership.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#define TR_RAFT_MEMBERSHIP_MAX_PENDING 2U

typedef struct tr_raft_membership_transition {
    tr_raft_membership_t committed;
    tr_raft_membership_t pending[TR_RAFT_MEMBERSHIP_MAX_PENDING];
    tr_raft_index_t pending_indices[TR_RAFT_MEMBERSHIP_MAX_PENDING];
    size_t pending_count;
} tr_raft_membership_transition_t;

int tr_raft_membership_transition_init(
    tr_raft_membership_transition_t *transition,
    const tr_raft_node_id_t *voters,
    size_t voter_count,
    const tr_raft_node_id_t *learners,
    size_t learner_count);

int tr_raft_membership_transition_init_configuration(
    tr_raft_membership_transition_t *transition,
    const tr_raft_conf_t *configuration);

int tr_raft_membership_transition_propose(
    const tr_raft_membership_transition_t *transition,
    const tr_raft_node_id_t *target_voters,
    size_t target_voter_count,
    const tr_raft_node_id_t *target_learners,
    size_t target_learner_count,
    uint64_t transition_id,
    tr_raft_membership_t *joint);

int tr_raft_membership_transition_stage(
    tr_raft_membership_transition_t *transition,
    const tr_raft_membership_t *configuration,
    tr_raft_index_t log_index);

int tr_raft_membership_transition_stage_entry(
    tr_raft_membership_transition_t *transition,
    const tr_raft_entry_t *entry);

tr_raft_index_t tr_raft_membership_transition_commit_limit(
    const tr_raft_membership_transition_t *transition,
    tr_raft_index_t candidate_index);

int tr_raft_membership_transition_apply(
    tr_raft_membership_transition_t *transition,
    tr_raft_index_t commit_index);

bool tr_raft_membership_transition_needs_final(
    const tr_raft_membership_transition_t *transition);

int tr_raft_membership_transition_final(
    const tr_raft_membership_transition_t *transition,
    tr_raft_membership_t *final_membership);

const tr_raft_membership_t *tr_raft_membership_transition_committed(
    const tr_raft_membership_transition_t *transition);

const tr_raft_membership_t *tr_raft_membership_transition_next(
    const tr_raft_membership_transition_t *transition);

#endif
