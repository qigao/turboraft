#ifndef TURBORAFT_RAFT_SNAPSHOT_INSTALLER_H
#define TURBORAFT_RAFT_SNAPSHOT_INSTALLER_H

#include <turboraft/raft_wal_storage.h>

#include <stdbool.h>

typedef struct tr_raft_snapshot_installer tr_raft_snapshot_installer_t;

typedef int (*tr_raft_snapshot_restore_application_fn)(
    void *context,
    tr_raft_index_t snapshot_index,
    tr_raft_term_t snapshot_term,
    const uint8_t *data,
    size_t size);

typedef int (*tr_raft_snapshot_reload_runtime_fn)(
    void *context,
    tr_raft_index_t snapshot_index,
    tr_raft_term_t snapshot_term);

typedef enum tr_raft_snapshot_install_stage {
    TR_RAFT_SNAPSHOT_INSTALL_IDLE = 0,
    TR_RAFT_SNAPSHOT_INSTALL_DURABLE = 1,
    TR_RAFT_SNAPSHOT_INSTALL_APPLICATION = 2,
    TR_RAFT_SNAPSHOT_INSTALL_RUNTIME = 3,
    TR_RAFT_SNAPSHOT_INSTALL_COMPLETE = 4
} tr_raft_snapshot_install_stage_t;

typedef struct tr_raft_snapshot_installer_config {
    /* Borrowed and must outlive the installer. */
    tr_raft_wal_storage_t *storage;
    tr_raft_snapshot_restore_application_fn restore_application;
    void *application_context;
    tr_raft_snapshot_reload_runtime_fn reload_runtime;
    void *runtime_context;
} tr_raft_snapshot_installer_config_t;

typedef struct tr_raft_snapshot_installer_status {
    bool faulted;
    tr_raft_snapshot_install_stage_t stage;
    int cause;
    tr_raft_index_t durable_index;
    tr_raft_index_t restored_index;
    tr_raft_index_t active_index;
} tr_raft_snapshot_installer_status_t;

int tr_raft_snapshot_installer_create(
    const tr_raft_snapshot_installer_config_t *config,
    tr_raft_snapshot_installer_t **out_installer);

void tr_raft_snapshot_installer_destroy(
    tr_raft_snapshot_installer_t *installer);

/* Signature-compatible with tr_raft_snapshot_install_fn. */
int tr_raft_snapshot_installer_install(
    void *context,
    tr_raft_term_t leader_term,
    tr_raft_index_t snapshot_index,
    tr_raft_term_t snapshot_term,
    const tr_raft_conf_t *configuration,
    const uint8_t *data,
    size_t size);

int tr_raft_snapshot_installer_get_status(
    const tr_raft_snapshot_installer_t *installer,
    tr_raft_snapshot_installer_status_t *out_status);

#endif
