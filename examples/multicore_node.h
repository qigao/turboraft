#ifndef TURBORAFT_MULTICORE_NODE_EXAMPLE_H
#define TURBORAFT_MULTICORE_NODE_EXAMPLE_H
#include <turboraft/raft_node_config.h>

/* Single-node durable counter example. Each group appends one increment.
 * Results include replayed increments. Caller supplies group_count outputs.
 * Network and snapshot recovery are deliberately rejected by this example. */
int multicore_node_run(const tr_raft_node_settings_t *settings,
                        uint64_t *out_counts, size_t count);
#endif
