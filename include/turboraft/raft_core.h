#ifndef TURBORAFT_RAFT_CORE_H
#define TURBORAFT_RAFT_CORE_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TR_RAFT_MAX_VOTERS 31U
#define TR_RAFT_MAX_MEMBERS TR_RAFT_MAX_VOTERS
#define TR_RAFT_MAX_ENTRY_BYTES 512U
#define TR_RAFT_MAX_APPEND_ENTRIES 8U
#define TR_RAFT_DEFAULT_MAX_INFLIGHT_APPEND_REQUESTS 1U
#define TR_RAFT_MAX_INFLIGHT_APPEND_REQUESTS 64U
#define TR_RAFT_DEFAULT_MAX_LOG_ENTRIES 1024U
#define TR_RAFT_CONF_CODEC_VERSION 1U
#define TR_RAFT_CONF_HEADER_SIZE 16U
#define TR_RAFT_CONF_MEMBER_SIZE 9U
#define TR_RAFT_CONF_MAX_ENCODED_SIZE                                      \
    (TR_RAFT_CONF_HEADER_SIZE +                                            \
     TR_RAFT_CONF_MEMBER_SIZE * TR_RAFT_MAX_MEMBERS)

typedef uint64_t tr_raft_node_id_t;
typedef uint64_t tr_raft_term_t;
typedef uint64_t tr_raft_index_t;

typedef struct tr_raft_core tr_raft_core_t;

typedef enum tr_raft_role {
    TR_RAFT_FOLLOWER = 0,
    TR_RAFT_PRE_CANDIDATE,
    TR_RAFT_CANDIDATE,
    TR_RAFT_LEADER
} tr_raft_role_t;

typedef enum tr_raft_message_type {
    TR_RAFT_MSG_PRE_VOTE_REQUEST = 0,
    TR_RAFT_MSG_PRE_VOTE_RESPONSE,
    TR_RAFT_MSG_VOTE_REQUEST,
    TR_RAFT_MSG_VOTE_RESPONSE,
    TR_RAFT_MSG_HEARTBEAT_REQUEST,
    TR_RAFT_MSG_HEARTBEAT_RESPONSE,
    TR_RAFT_MSG_APPEND_REQUEST,
    TR_RAFT_MSG_APPEND_RESPONSE,
    TR_RAFT_MSG_TIMEOUT_NOW,
    TR_RAFT_MSG_READ_INDEX_REQUEST,
    TR_RAFT_MSG_READ_INDEX_RESPONSE
} tr_raft_message_type_t;

typedef struct tr_raft_entry {
    tr_raft_index_t index;
    tr_raft_term_t term;
    uint64_t command_id;
    size_t data_length;
    uint8_t data[TR_RAFT_MAX_ENTRY_BYTES];
} tr_raft_entry_t;

typedef enum tr_raft_conf_phase {
    TR_RAFT_CONF_JOINT = 1,
    TR_RAFT_CONF_FINAL = 2
} tr_raft_conf_phase_t;

typedef enum tr_raft_conf_role {
    TR_RAFT_CONF_OLD_VOTER = 0x01,
    TR_RAFT_CONF_NEW_VOTER = 0x02,
    TR_RAFT_CONF_LEARNER = 0x04
} tr_raft_conf_role_t;

typedef struct tr_raft_conf_member {
    tr_raft_node_id_t node_id;
    uint8_t roles;
} tr_raft_conf_member_t;

/** Canonical committed membership stored separately from application bytes. */
typedef struct tr_raft_conf {
    tr_raft_conf_phase_t phase;
    uint64_t transition_id;
    size_t member_count;
    tr_raft_conf_member_t members[TR_RAFT_MAX_MEMBERS];
} tr_raft_conf_t;

int tr_raft_conf_validate(const tr_raft_conf_t *configuration);
int tr_raft_conf_encode(const tr_raft_conf_t *configuration,
                        uint8_t *output,
                        size_t output_capacity,
                        size_t *output_length);
int tr_raft_conf_decode(const uint8_t *input,
                        size_t input_length,
                        tr_raft_conf_t *configuration);

typedef struct tr_raft_message {
    tr_raft_message_type_t type;
    tr_raft_node_id_t from;
    tr_raft_node_id_t to;
    tr_raft_term_t term;
    tr_raft_term_t campaign_term;
    tr_raft_index_t last_log_index;
    tr_raft_term_t last_log_term;
    tr_raft_index_t leader_commit;
    tr_raft_index_t previous_log_index;
    tr_raft_term_t previous_log_term;
    tr_raft_index_t match_index;
    tr_raft_index_t reject_hint;
    size_t entry_count;
    union {
        tr_raft_entry_t entry;
        tr_raft_entry_t entries[TR_RAFT_MAX_APPEND_ENTRIES];
    };
    bool granted;
    uint64_t context_id;
} tr_raft_message_t;

typedef struct tr_raft_read_state {
    uint64_t context_id;
    tr_raft_index_t index;
} tr_raft_read_state_t;

typedef struct tr_raft_snapshot_request {
    tr_raft_node_id_t peer_id;
    tr_raft_term_t leader_term;
    tr_raft_index_t snapshot_index;
    tr_raft_term_t snapshot_term;
} tr_raft_snapshot_request_t;

typedef struct tr_raft_core_config {
    tr_raft_node_id_t self_id;
    const tr_raft_node_id_t *voters;
    size_t voter_count;
    const tr_raft_node_id_t *learners;
    size_t learner_count;
    /* Optional snapshot configuration; when set it supersedes bootstrap sets. */
    const tr_raft_conf_t *initial_configuration;
    uint32_t heartbeat_ticks;
    uint32_t election_min_ticks;
    uint32_t election_max_ticks;
    uint32_t initial_election_timeout_ticks;
    tr_raft_term_t initial_term;
    tr_raft_node_id_t initial_vote;
    tr_raft_index_t initial_last_log_index;
    tr_raft_term_t initial_last_log_term;
    const tr_raft_entry_t *initial_log_entries;
    size_t initial_log_entry_count;
    tr_raft_index_t initial_commit_index;
    tr_raft_index_t initial_applied_index;
    size_t max_log_entries;
    /**
     * Per-peer AppendEntries window. Zero preserves the historical single
     * in-flight request behavior.
     */
    size_t max_inflight_append_requests;
} tr_raft_core_config_t;

typedef struct tr_raft_proposal {
    uint64_t command_id;
    const void *data;
    size_t data_length;
} tr_raft_proposal_t;

typedef struct tr_raft_membership_change {
    uint64_t transition_id;
    const tr_raft_node_id_t *voters;
    size_t voter_count;
    const tr_raft_node_id_t *learners;
    size_t learner_count;
} tr_raft_membership_change_t;

typedef struct tr_raft_tick {
    uint32_t elapsed_ticks;
    uint32_t next_election_timeout_ticks;
} tr_raft_tick_t;

typedef struct tr_raft_ready {
    tr_raft_message_t *messages;
    size_t message_capacity;
    size_t message_count;
    bool hard_state_changed;
    tr_raft_term_t term;
    tr_raft_node_id_t voted_for;
    bool role_changed;
    tr_raft_role_t role;
    bool log_changed;
    tr_raft_index_t log_truncate_from;
    const tr_raft_entry_t *log_entries;
    size_t log_entry_count;
    bool commit_changed;
    tr_raft_index_t commit_index;
    const tr_raft_entry_t *committed_entries;
    size_t committed_entry_count;
    bool read_state_ready;
    tr_raft_read_state_t read_state;
    tr_raft_snapshot_request_t snapshot_requests[TR_RAFT_MAX_MEMBERS];
    size_t snapshot_request_count;
} tr_raft_ready_t;

typedef struct tr_raft_status {
    tr_raft_node_id_t self_id;
    tr_raft_node_id_t leader_id;
    tr_raft_role_t role;
    tr_raft_term_t term;
    tr_raft_node_id_t voted_for;
    tr_raft_index_t last_log_index;
    tr_raft_term_t last_log_term;
    tr_raft_index_t log_base_index;
    tr_raft_term_t log_base_term;
    size_t log_entry_count;
    tr_raft_index_t commit_index;
    tr_raft_index_t applied_index;
    uint32_t election_elapsed_ticks;
    uint32_t election_timeout_ticks;
    uint32_t heartbeat_elapsed_ticks;
    bool ready_outstanding;
    tr_raft_node_id_t leadership_transfer_target;
    uint32_t leadership_transfer_elapsed_ticks;
    uint64_t pending_read_context_id;
    size_t inflight_append_count;
    bool self_is_voter;
    size_t voter_count;
    size_t learner_count;
    bool joint_configuration;
    uint64_t membership_transition_id;
    size_t pending_configuration_count;
    size_t peer_count;
    size_t snapshot_required_peer_count;
} tr_raft_status_t;

typedef struct tr_raft_snapshot_point {
    tr_raft_index_t index;
    tr_raft_term_t term;
    tr_raft_conf_t configuration;
} tr_raft_snapshot_point_t;

typedef struct tr_raft_peer_progress {
    tr_raft_node_id_t node_id;
    tr_raft_index_t match_index;
    tr_raft_index_t next_index;
    uint32_t append_inflight_elapsed_ticks;
    size_t inflight_append_count;
    size_t max_inflight_append_requests;
    bool recent_active;
    bool append_inflight;
    bool append_probe;
    bool snapshot_required;
} tr_raft_peer_progress_t;

typedef struct tr_raft_progress_view {
    size_t peer_count;
    tr_raft_peer_progress_t peers[TR_RAFT_MAX_MEMBERS];
} tr_raft_progress_view_t;

typedef enum tr_raft_operation_state {
    TR_RAFT_OPERATION_PENDING = 1,
    TR_RAFT_OPERATION_COMMITTED = 2,
    TR_RAFT_OPERATION_APPLIED = 3,
    TR_RAFT_OPERATION_LOST = 4,
    TR_RAFT_OPERATION_EXPIRED = 5
} tr_raft_operation_state_t;

typedef struct tr_raft_operation_status {
    tr_raft_operation_state_t state;
    tr_raft_term_t term;
    tr_raft_index_t index;
    tr_raft_index_t commit_index;
    tr_raft_index_t applied_index;
} tr_raft_operation_status_t;

/**
 * Creates a single-owner deterministic Raft election core.
 *
 * Voters and learners are copied, strictly ascending, non-zero, and disjoint.
 * Their combined count is bounded by TR_RAFT_MAX_MEMBERS. self_id must occur
 * exactly once across both sets. Learners replicate but never vote, campaign,
 * contribute to quorum, serve ReadIndex, or receive leadership transfer.
 */
int tr_raft_core_create(const tr_raft_core_config_t *config,
                        tr_raft_core_t **out_core);

void tr_raft_core_destroy(tr_raft_core_t *core);

/**
 * Advances logical time. Randomness remains outside the core: every tick input
 * supplies the timeout to use for the next election cycle.
 *
 * Returns SALTS_ENOSPC without changing state when ready cannot hold all
 * messages. On success, persist changed hard state before transmitting messages.
 */
int tr_raft_core_tick(tr_raft_core_t *core,
                      const tr_raft_tick_t *tick,
                      tr_raft_ready_t *ready);

/** Processes one validated peer message and produces bounded effects. */
int tr_raft_core_step(tr_raft_core_t *core,
                      const tr_raft_message_t *message,
                      tr_raft_ready_t *ready);

/** Appends one bounded command on the leader and starts replication. */
int tr_raft_core_propose(tr_raft_core_t *core,
                         const tr_raft_proposal_t *proposal,
                         tr_raft_ready_t *ready);

/** Starts one leader-only Joint Consensus membership transition. */
int tr_raft_core_change_membership(
    tr_raft_core_t *core,
    const tr_raft_membership_change_t *change,
    tr_raft_ready_t *ready);

/**
 * Starts transfer to a voting peer. A caught-up peer receives TimeoutNow;
 * otherwise one replication message is emitted first. Proposals return
 * SALTS_EBUSY until transfer succeeds, is cancelled by a role change, or times
 * out after the current election timeout.
 */
int tr_raft_core_transfer_leadership(tr_raft_core_t *core,
                                     tr_raft_node_id_t transferee_id,
                                     tr_raft_ready_t *ready);

/**
 * Starts one linearizable read barrier identified by a non-zero context.
 * The leader must already have committed an entry in its current term. Only one
 * read may be pending; its Ready result captures commit_index at request time.
 */
int tr_raft_core_read_index(tr_raft_core_t *core,
                            uint64_t context_id,
                            tr_raft_ready_t *ready);

/**
 * Returns startup/committed work and, on a leader, fills available bounded
 * AppendEntries window slots up to Ready.message_capacity.
 */
int tr_raft_core_poll(tr_raft_core_t *core, tr_raft_ready_t *ready);

/**
 * Acknowledges that persistence, transmission, and committed-entry application
 * for the last non-empty Ready all completed successfully.
 */
int tr_raft_core_advance(tr_raft_core_t *core);

/**
 * Captures the exact applied boundary for application snapshot creation.
 * The Core must have no outstanding Ready and applied_index must equal
 * commit_index, so the returned ConfState describes the same boundary.
 */
int tr_raft_core_snapshot_point(const tr_raft_core_t *core,
                                tr_raft_snapshot_point_t *out_point);

/**
 * Discards the in-memory log prefix covered by a durably stored snapshot.
 * The point must equal the Core's current applied boundary and identity.
 */
int tr_raft_core_compact(tr_raft_core_t *core,
                         const tr_raft_snapshot_point_t *point);

/**
 * Acknowledges that a peer durably installed a requested snapshot. The leader
 * advances that peer's replication progress and emits the retained suffix, or
 * requests a newer snapshot when compaction advanced during transfer.
 */
int tr_raft_core_snapshot_completed(tr_raft_core_t *core,
                                    tr_raft_node_id_t peer_id,
                                    tr_raft_index_t snapshot_index,
                                    tr_raft_ready_t *ready);

/** Copies a read-only diagnostic snapshot. */
int tr_raft_core_status(const tr_raft_core_t *core, tr_raft_status_t *status);

/** Copies the authoritative committed membership view. */
int tr_raft_core_configuration(const tr_raft_core_t *core,
                               tr_raft_conf_t *out_configuration);

/** Copies bounded per-peer replication progress. */
int tr_raft_core_progress(const tr_raft_core_t *core,
                          tr_raft_progress_view_t *out_progress);

/** Derives receipt progress without mutating or retaining operation state. */
int tr_raft_core_operation_status(const tr_raft_core_t *core,
                                  tr_raft_term_t term,
                                  tr_raft_index_t index,
                                  tr_raft_operation_status_t *out_status);

#ifdef __cplusplus
}
#endif

#endif
