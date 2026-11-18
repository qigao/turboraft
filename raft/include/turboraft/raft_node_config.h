#ifndef TURBORAFT_RAFT_NODE_CONFIG_H
#define TURBORAFT_RAFT_NODE_CONFIG_H

#include <turboraft/raft_multicore.h>
#include <data_bind.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TR_RAFT_NODE_CONFIG_MAX_BYTES (1024U * 1024U)

typedef struct tr_raft_node_group_config {
    uint64_t group_id;
    /* Absolute WAL path prefix; never expanded or opened by the loader. */
    const char *storage_path;
    /* Bootstrap template. Host MUST replace recovery fields from durable state. */
    tr_raft_core_config_t core;
} tr_raft_node_group_config_t;

typedef struct tr_raft_node_settings {
    tr_raft_multicore_config_t runtime;
    tr_raft_cluster_id_t cluster_id;
    tr_raft_node_id_t node_id;
    const tr_raft_node_group_config_t *groups; /* runtime.group_count elements */
} tr_raft_node_settings_t;

typedef struct tr_raft_node_config tr_raft_node_config_t;

/* Same semantic validation for parsed and programmatic settings.
 * Transport configuration and peer authentication belong to the host factory. */
int tr_raft_node_settings_validate(const tr_raft_node_settings_t *settings);
/**
 * Strict version-1 JSON: unknown fields and scalar coercion are rejected.
 * Schema defaults apply only to omitted fields. Error details belong to the
 * caller; the loader never logs configuration. Output is NULL on failure.
 * All strings/arrays in settings are borrowed until config_destroy.
 */
int tr_raft_node_config_parse(const char *json, size_t size,
                             tr_raft_node_config_t **out_config, DataBindError *error);
/* Reads at most MAX_BYTES + 1, independent of file size changes during read. */
int tr_raft_node_config_load(const char *path, tr_raft_node_config_t **out_config,
                            DataBindError *error);
const tr_raft_node_settings_t *tr_raft_node_config_settings(const tr_raft_node_config_t *config);
void tr_raft_node_config_destroy(tr_raft_node_config_t *config);

#ifdef __cplusplus
}
#endif
#endif
