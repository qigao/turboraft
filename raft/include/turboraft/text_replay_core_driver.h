#ifndef TURBORAFT_TEXT_REPLAY_CORE_DRIVER_H
#define TURBORAFT_TEXT_REPLAY_CORE_DRIVER_H

#include <turboraft/raft_core.h>
#include <turboraft/text_syntax.h>

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Deterministic replay driver over native tr_raft_core_t nodes.
 *
 * The driver owns a fixed set of cores and a simulated message network. It
 * executes every tr_text_replay_action_t, including the fault-injection and
 * expectation actions that the generic tr_text_replay_execute() adapter
 * leaves unimplemented (they return SALTS_ENOTSUP there).
 *
 * Semantics:
 * - NODE validates that the referenced node exists in the driver; nodes are
 *   created up front from the configuration and are never created lazily.
 * - TICK advances every node by the given number of ticks and then pumps all
 *   due (delay == 0) messages to their destinations.
 * - SEND injects one heartbeat request from the source node to the target
 *   node using the source's current term, last log index/term, and commit
 *   index. The injected message is subject to the same filters and partition
 *   rules as any other message.
 * - DROP_NEXT / DELAY_NEXT / DUPLICATE_NEXT install a single-shot filter that
 *   applies to the next in-flight message of the named kind (message type
 *   names are the wire names used by the protocol-debug DSL, e.g.
 *   "append_request"). DELAY_NEXT holds the message for the given number of
 *   ticks; DUPLICATE_NEXT enqueues the message twice. Up to
 *   TR_REPLAY_DRIVER_MAX_FILTERS filters may be pending; adding more fails
 *   with SALTS_ENOSPC.
 * - PARTITION removes the directed link from->to; HEAL restores it. Messages
 *   on a partitioned link are dropped at delivery time.
 * - SUBMIT proposes an entry on the target node (which must be the leader)
 *   with command_id = request_id and data = the decoded payload, and stores a
 *   (term, index) receipt keyed by request_id. Duplicate request ids fail
 *   with SALTS_EALREADY.
 * - POLL checks the stored receipt against the operation state. The driver
 *   advances time internally (ticking all nodes) up to timeout_ticks until
 *   the target is reached; SALTS_ETIMEDOUT is returned if it is not. This is
 *   a driver-specific behavior: the generic tr_text_replay_execute() adapter
 *   never advances time.
 * - EXPECT_ROLE and EXPECT_COMMIT_INDEX assert on the node's core status;
 *   a failed expectation returns SALTS_EPROTO.
 *
 * All queues and delivery loops are bounded; exceeding a bound fails fast
 * with SALTS_ENOSPC or SALTS_EPROTO.
 */

#define TR_REPLAY_DRIVER_MAX_NODES TR_RAFT_MAX_MEMBERS
#define TR_REPLAY_DRIVER_MAX_FILTERS 8U
#define TR_REPLAY_DRIVER_DEFAULT_QUEUE_PER_NODE 64U

typedef int (*tr_replay_driver_timeout_fn)(void *context,
                                           uint32_t *out_ticks);

/* Optional application callback invoked for committed entries in index
 * order after the node's Ready has been fully processed. */
typedef int (*tr_replay_driver_apply_fn)(
    void *context,
    tr_raft_node_id_t node_id,
    const tr_raft_entry_t *entries,
    size_t entry_count);

typedef struct tr_replay_driver_config {
    /* One core configuration per node; self_id identifies the node and must
     * be unique and non-zero. */
    const tr_raft_core_config_t *nodes;
    size_t node_count;
    /* Optional committed-entry observer. */
    tr_replay_driver_apply_fn apply;
    void *apply_context;
    /* Optional deterministic next-election-timeout provider. When NULL the
     * driver uses default_election_timeout_ticks, or the node's
     * initial_election_timeout_ticks when that is zero. */
    tr_replay_driver_timeout_fn next_election_timeout;
    void *timeout_context;
    uint32_t default_election_timeout_ticks;
    /* Zero selects TR_REPLAY_DRIVER_DEFAULT_QUEUE_PER_NODE * node_count. */
    size_t max_queued;
    /* Zero selects max_queued * 16. */
    size_t max_deliveries;
} tr_replay_driver_config_t;

typedef struct tr_replay_driver tr_replay_driver_t;

int tr_replay_driver_create(const tr_replay_driver_config_t *config,
                            tr_replay_driver_t **out_driver);

void tr_replay_driver_destroy(tr_replay_driver_t *driver);

/* Executes one parsed replay plan in order. */
int tr_replay_driver_run(tr_replay_driver_t *driver,
                         const tr_text_replay_plan_t *plan);

/* Executes a single action (used by the REPL and incremental drivers). */
int tr_replay_driver_step(tr_replay_driver_t *driver,
                          const tr_text_replay_action_t *action);

/* Copies a node's core status for inspection/expectations. */
int tr_replay_driver_status(const tr_replay_driver_t *driver,
                            tr_raft_node_id_t node_id,
                            tr_raft_status_t *out_status);

/* Total in-flight (queued, not yet delivered) message count. */
size_t tr_replay_driver_queued(const tr_replay_driver_t *driver);

/* Counters for observability: dropped by partition, dropped by filter,
 * duplicated, delivered. */
typedef struct tr_replay_driver_counters {
    size_t ticks;
    size_t delivered;
    size_t dropped_partition;
    size_t dropped_filter;
    size_t duplicated;
} tr_replay_driver_counters_t;

void tr_replay_driver_counters(const tr_replay_driver_t *driver,
                               tr_replay_driver_counters_t *out_counters);

#ifdef __cplusplus
}
#endif

#endif
