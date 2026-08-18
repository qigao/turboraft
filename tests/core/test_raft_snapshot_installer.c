#include "raft_snapshot_installer.h"

#include <tinytest.h>
#include <turbo_error.h>
#include <turbo_fs.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static const tr_raft_conf_t installer_configuration = {
    TR_RAFT_CONF_FINAL,
    0U,
    1U,
    {{1U, TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER}}
};

typedef struct installer_capture {
    int sequence;
    int restore_order;
    int reload_order;
    int restore_result;
    int reload_result;
    uint8_t data[4];
    size_t size;
} installer_capture_t;

static char *installer_path_prefix;

static tr_raft_wal_storage_t *installer_open_storage(void)
{
    tr_raft_wal_storage_config_t config;
    tr_raft_wal_storage_t *storage = NULL;

    memset(&config, 0, sizeof(config));
    installer_path_prefix = tt_make_temp_file("turboraft-installer", ".data");
    config.path_prefix = installer_path_prefix;
    config.segment_bytes = TR_RAFT_WAL_MIN_SEGMENT_BYTES;
    config.max_transaction_bytes = 8192U;
    config.max_segments = 2U;
    config.max_log_entries = 16U;
    config.create_if_missing = true;
    config.max_snapshot_bytes = 1024U;
    check_int_eq(tr_raft_wal_storage_open(&config, &storage), TURBO_OK);
    return storage;
}

static void installer_close_storage(tr_raft_wal_storage_t *storage)
{
    char path[TURBO_FS_MAX_PATH];
    check_int_eq(tr_raft_wal_storage_close(storage), TURBO_OK);
    snprintf(path, sizeof(path), "%s.snapshot.9.6", installer_path_prefix);
    check_int_eq(tt_remove_file(path), 0);
    snprintf(path, sizeof(path), "%s.00000001.wal", installer_path_prefix);
    check_int_eq(tt_remove_file(path), 0);
    snprintf(path, sizeof(path), "%s.lock", installer_path_prefix);
    check_int_eq(tt_remove_file(path), 0);
    check_int_eq(tt_remove_file(installer_path_prefix), 0);
    free(installer_path_prefix);
    installer_path_prefix = NULL;
}

static int installer_restore(
    void *context,
    tr_raft_index_t snapshot_index,
    tr_raft_term_t snapshot_term,
    const uint8_t *data,
    size_t size)
{
    installer_capture_t *capture = (installer_capture_t *) context;

    if (capture == NULL || snapshot_index != 9U || snapshot_term != 6U ||
        data == NULL || size > sizeof(capture->data)) {
        return TURBO_EPROTO;
    }
    capture->restore_order = ++capture->sequence;
    memcpy(capture->data, data, size);
    capture->size = size;
    return capture->restore_result;
}

static int installer_reload(
    void *context,
    tr_raft_index_t snapshot_index,
    tr_raft_term_t snapshot_term)
{
    installer_capture_t *capture = (installer_capture_t *) context;

    if (capture == NULL || snapshot_index != 9U || snapshot_term != 6U) {
        return TURBO_EPROTO;
    }
    capture->reload_order = ++capture->sequence;
    return capture->reload_result;
}

static tr_raft_snapshot_installer_t *installer_create(
    tr_raft_wal_storage_t *storage,
    installer_capture_t *capture)
{
    tr_raft_snapshot_installer_config_t config;
    tr_raft_snapshot_installer_t *installer = NULL;

    memset(&config, 0, sizeof(config));
    config.storage = storage;
    config.restore_application = installer_restore;
    config.application_context = capture;
    config.reload_runtime = installer_reload;
    config.runtime_context = capture;
    check_int_eq(tr_raft_snapshot_installer_create(&config, &installer),
                 TURBO_OK);
    return installer;
}

spec("raft snapshot installer")
{
    it("installs durable state before restore and runtime reload")
    {
        const uint8_t snapshot[] = {0x10U, 0x20U, 0x30U};
        tr_raft_wal_storage_t *storage = installer_open_storage();
        tr_raft_snapshot_installer_t *installer;
        tr_raft_snapshot_installer_status_t status;
        installer_capture_t capture;

        memset(&capture, 0, sizeof(capture));
        installer = installer_create(storage, &capture);
        check_int_eq(tr_raft_snapshot_installer_install(
                         installer, 7U, 9U, 6U, &installer_configuration,
                         snapshot, sizeof(snapshot)), TURBO_OK);
        check_int_eq(capture.restore_order, 1);
        check_int_eq(capture.reload_order, 2);
        check_size_eq(capture.size, sizeof(snapshot));
        check_mem_eq(capture.data, snapshot, sizeof(snapshot));
        check_int_eq(tr_raft_snapshot_installer_get_status(installer, &status),
                     TURBO_OK);
        check(!status.faulted);
        check_int_eq(status.stage, TR_RAFT_SNAPSHOT_INSTALL_COMPLETE);
        check_long_eq(status.durable_index, 9U);
        check_long_eq(status.restored_index, 9U);
        check_long_eq(status.active_index, 9U);

        tr_raft_snapshot_installer_destroy(installer);
        installer_close_storage(storage);
    }

    it("faults after durable install when application restore fails")
    {
        const uint8_t snapshot[] = {0x40U, 0x50U};
        tr_raft_wal_storage_t *storage = installer_open_storage();
        tr_raft_snapshot_installer_t *installer;
        tr_raft_snapshot_installer_status_t status;
        installer_capture_t capture;

        memset(&capture, 0, sizeof(capture));
        capture.restore_result = TURBO_EPIPE;
        installer = installer_create(storage, &capture);
        check_int_eq(tr_raft_snapshot_installer_install(
                         installer, 7U, 9U, 6U, &installer_configuration,
                         snapshot, sizeof(snapshot)), TURBO_EPIPE);
        check_int_eq(tr_raft_snapshot_installer_get_status(installer, &status),
                     TURBO_OK);
        check(status.faulted);
        check_int_eq(status.stage, TR_RAFT_SNAPSHOT_INSTALL_APPLICATION);
        check_int_eq(status.cause, TURBO_EPIPE);
        check_long_eq(status.durable_index, 9U);
        check_long_eq(status.restored_index, 0U);
        check_long_eq(status.active_index, 0U);
        check_int_eq(capture.reload_order, 0);
        check_int_eq(tr_raft_snapshot_installer_install(
                         installer, 8U, 10U, 7U, &installer_configuration,
                         snapshot, sizeof(snapshot)), TURBO_EPROTO);

        tr_raft_snapshot_installer_destroy(installer);
        installer_close_storage(storage);
    }
}
