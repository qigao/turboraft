#include "raft_snapshot_installer.h"

#include <turbo_error.h>

#include <stdlib.h>

struct tr_raft_snapshot_installer {
    tr_raft_snapshot_installer_config_t config;
    tr_raft_snapshot_installer_status_t status;
};

static int tr_snapshot_installer_fail(
    tr_raft_snapshot_installer_t *installer,
    tr_raft_snapshot_install_stage_t stage,
    int cause)
{
    installer->status.faulted = true;
    installer->status.stage = stage;
    installer->status.cause = cause;
    return cause;
}

int tr_raft_snapshot_installer_create(
    const tr_raft_snapshot_installer_config_t *config,
    tr_raft_snapshot_installer_t **out_installer)
{
    tr_raft_snapshot_installer_t *installer;

    if (config == NULL || out_installer == NULL || config->storage == NULL ||
        config->restore_application == NULL || config->reload_runtime == NULL) {
        return TURBO_EINVAL;
    }
    *out_installer = NULL;
    installer = (tr_raft_snapshot_installer_t *) calloc(
        1U, sizeof(*installer));
    if (installer == NULL) {
        return TURBO_ENOMEM;
    }
    installer->config = *config;
    *out_installer = installer;
    return TURBO_OK;
}

void tr_raft_snapshot_installer_destroy(
    tr_raft_snapshot_installer_t *installer)
{
    free(installer);
}

int tr_raft_snapshot_installer_install(
    void *context,
    tr_raft_term_t leader_term,
    tr_raft_index_t snapshot_index,
    tr_raft_term_t snapshot_term,
    const tr_raft_conf_t *configuration,
    const uint8_t *data,
    size_t size)
{
    tr_raft_snapshot_installer_t *installer =
        (tr_raft_snapshot_installer_t *) context;
    int result;

    if (installer == NULL || configuration == NULL || leader_term == 0U ||
        snapshot_index == 0U ||
        snapshot_term == 0U || snapshot_term > leader_term ||
        (size > 0U && data == NULL)) {
        return TURBO_EINVAL;
    }
    if (installer->status.faulted) {
        return TURBO_EPROTO;
    }

    installer->status.stage = TR_RAFT_SNAPSHOT_INSTALL_DURABLE;
    result = tr_raft_sqlite_storage_install_snapshot(
        installer->config.storage, leader_term, snapshot_index,
        snapshot_term, configuration, data, size);
    if (result != TURBO_OK) {
        return tr_snapshot_installer_fail(
            installer, TR_RAFT_SNAPSHOT_INSTALL_DURABLE, result);
    }
    installer->status.durable_index = snapshot_index;

    installer->status.stage = TR_RAFT_SNAPSHOT_INSTALL_APPLICATION;
    result = installer->config.restore_application(
        installer->config.application_context, snapshot_index,
        snapshot_term, data, size);
    if (result != TURBO_OK) {
        return tr_snapshot_installer_fail(
            installer, TR_RAFT_SNAPSHOT_INSTALL_APPLICATION, result);
    }
    installer->status.restored_index = snapshot_index;

    installer->status.stage = TR_RAFT_SNAPSHOT_INSTALL_RUNTIME;
    result = installer->config.reload_runtime(
        installer->config.runtime_context, snapshot_index, snapshot_term);
    if (result != TURBO_OK) {
        return tr_snapshot_installer_fail(
            installer, TR_RAFT_SNAPSHOT_INSTALL_RUNTIME, result);
    }
    installer->status.active_index = snapshot_index;
    installer->status.stage = TR_RAFT_SNAPSHOT_INSTALL_COMPLETE;
    installer->status.cause = TURBO_OK;
    return TURBO_OK;
}

int tr_raft_snapshot_installer_get_status(
    const tr_raft_snapshot_installer_t *installer,
    tr_raft_snapshot_installer_status_t *out_status)
{
    if (installer == NULL || out_status == NULL) {
        return TURBO_EINVAL;
    }
    *out_status = installer->status;
    return TURBO_OK;
}
