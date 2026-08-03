#include "raft_service_sqlite_reload.h"
#include "raft_snapshot_installer.h"

#include <tinytest.h>
#include <turbo_error.h>

#include <string.h>

typedef struct reload_application_capture {
    uint8_t data[8];
    size_t size;
    size_t restore_count;
} reload_application_capture_t;

static int reload_apply(
    void *context,
    const tr_raft_entry_t *entries,
    size_t entry_count)
{
    (void) context;
    (void) entries;
    (void) entry_count;
    return TURBO_OK;
}

static int reload_restore(
    void *context,
    tr_raft_index_t snapshot_index,
    tr_raft_term_t snapshot_term,
    const uint8_t *data,
    size_t size)
{
    reload_application_capture_t *capture =
        (reload_application_capture_t *) context;

    if (capture == NULL || snapshot_index != 9U || snapshot_term != 6U ||
        data == NULL || size > sizeof(capture->data)) {
        return TURBO_EPROTO;
    }
    memcpy(capture->data, data, size);
    capture->size = size;
    ++capture->restore_count;
    return TURBO_OK;
}

spec("raft service sqlite reload")
{
    it("rebuilds Service from an installed durable snapshot")
    {
        const tr_raft_node_id_t voters[] = {1U};
        const tr_raft_conf_t snapshot_configuration = {
            TR_RAFT_CONF_FINAL,
            44U,
            2U,
            {
                {1U, TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER},
                {2U, TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER}
            }
        };
        const uint8_t snapshot[] = {0x61U, 0x62U, 0x63U};
        tr_raft_sqlite_storage_config_t storage_config;
        tr_raft_sqlite_storage_t *storage = NULL;
        tr_raft_storage_t storage_adapter;
        tr_raft_service_config_t service_config;
        tr_raft_service_t *service = NULL;
        tr_raft_service_sqlite_reload_config_t reload_config;
        tr_raft_service_sqlite_reload_t *reload = NULL;
        tr_raft_snapshot_installer_config_t installer_config;
        tr_raft_snapshot_installer_t *installer = NULL;
        tr_raft_service_status_t service_status;
        tr_raft_snapshot_installer_status_t installer_status;
        reload_application_capture_t application;

        memset(&storage_config, 0, sizeof(storage_config));
        memset(&storage_adapter, 0, sizeof(storage_adapter));
        memset(&service_config, 0, sizeof(service_config));
        memset(&reload_config, 0, sizeof(reload_config));
        memset(&installer_config, 0, sizeof(installer_config));
        memset(&application, 0, sizeof(application));

        storage_config.path = ":memory:";
        storage_config.busy_timeout_ms = 1000;
        storage_config.create_if_missing = true;
        storage_config.max_snapshot_bytes = 1024U;
        check_int_eq(tr_raft_sqlite_storage_open(
                         &storage_config, &storage), TURBO_OK);
        check_int_eq(tr_raft_sqlite_storage_bind(storage, &storage_adapter),
                     TURBO_OK);

        service_config.core.self_id = 1U;
        service_config.core.voters = voters;
        service_config.core.voter_count = 1U;
        service_config.core.heartbeat_ticks = 1U;
        service_config.core.election_min_ticks = 3U;
        service_config.core.election_max_ticks = 5U;
        service_config.core.initial_election_timeout_ticks = 3U;
        service_config.core.max_log_entries = 16U;
        service_config.storage = storage_adapter;
        service_config.state_machine.context = &application;
        service_config.state_machine.apply_batch = reload_apply;
        check_int_eq(tr_raft_service_create(&service_config, &service),
                     TURBO_OK);

        reload_config.service = service;
        reload_config.storage = storage;
        reload_config.self_id = 1U;
        reload_config.voters = voters;
        reload_config.voter_count = 1U;
        reload_config.heartbeat_ticks = 1U;
        reload_config.election_min_ticks = 3U;
        reload_config.election_max_ticks = 5U;
        reload_config.initial_election_timeout_ticks = 3U;
        reload_config.max_log_entries = 16U;
        check_int_eq(tr_raft_service_sqlite_reload_create(
                         &reload_config, &reload), TURBO_OK);

        installer_config.storage = storage;
        installer_config.restore_application = reload_restore;
        installer_config.application_context = &application;
        installer_config.reload_runtime =
            tr_raft_service_sqlite_reload_runtime;
        installer_config.runtime_context = reload;
        check_int_eq(tr_raft_snapshot_installer_create(
                         &installer_config, &installer), TURBO_OK);
        check_int_eq(tr_raft_snapshot_installer_install(
                         installer, 7U, 9U, 6U, &snapshot_configuration,
                         snapshot, sizeof(snapshot)), TURBO_OK);

        check_size_eq(application.restore_count, 1U);
        check_size_eq(application.size, sizeof(snapshot));
        check_mem_eq(application.data, snapshot, sizeof(snapshot));
        check_int_eq(tr_raft_service_status(service, &service_status),
                     TURBO_OK);
        check(!service_status.faulted);
        check_long_eq(service_status.core.term, 7U);
        check_long_eq(service_status.core.last_log_index, 9U);
        check_long_eq(service_status.core.commit_index, 9U);
        check_long_eq(service_status.core.applied_index, 9U);
        check_size_eq(service_status.core.voter_count, 2U);
        check_long_eq(service_status.core.membership_transition_id, 44U);
        check_int_eq(tr_raft_snapshot_installer_get_status(
                         installer, &installer_status), TURBO_OK);
        check_int_eq(installer_status.stage,
                     TR_RAFT_SNAPSHOT_INSTALL_COMPLETE);

        tr_raft_snapshot_installer_destroy(installer);
        tr_raft_service_sqlite_reload_destroy(reload);
        tr_raft_service_destroy(service);
        check_int_eq(tr_raft_sqlite_storage_close(storage), TURBO_OK);
    }
}
