#include "../../examples/multicore_node.h"
#include <cmeta_fs.h>
#include <salts/clock.h>
#include <cmeta_error.h>
#include <fmt.h>
#include <tinytest.h>
#include <string.h>

spec("multicore durable counter example")
{
    static tstr directory;
    static tstr prefixes[4];
    static tr_raft_node_settings_t settings;
    static tr_raft_node_group_config_t groups[4];
    static tr_raft_group_assignment_t assignments[4];
    static tr_raft_node_id_t voter;
    static uint64_t counts[4];
    before_each() {
        char temp[4096];
        memset(&settings, 0, sizeof(settings));
        memset(groups, 0, sizeof(groups));
        memset(assignments, 0, sizeof(assignments));
        memset(prefixes, 0, sizeof(prefixes));
        check_equal(cmeta_fs_get_tmpdir(temp, sizeof(temp)), SALTS_OK);
        directory = tstr_format("{}/turboraft-multicore-{}", temp, cmeta_hrtime());
        check_not_null(directory);
        check_equal(cmeta_fs_mkdir(directory, 0700), SALTS_OK);
        voter = 1U;
        settings.node_id = 1U;
        settings.cluster_id.bytes[0] = 1U;
        settings.groups = groups;
        settings.runtime.version = 1U;
        settings.runtime.owner_count = 2U;
        settings.runtime.capacity = 4U;
        settings.runtime.work_budget = 2U;
        settings.runtime.tick_ms = 1U;
        settings.runtime.idle_ms = 1U;
        settings.runtime.groups = assignments;
        settings.runtime.group_count = 4U;
        for (size_t i = 0; i < 4U; ++i) {
            prefixes[i] = tstr_format("{}/group{}", directory, i);
            check_not_null(prefixes[i]);
            assignments[i].group_id = i + 1U;
            assignments[i].owner_index = (uint32_t)i % 2U;
            assignments[i].election_min_ticks = 3U;
            assignments[i].election_max_ticks = 5U;
            groups[i].group_id = i + 1U;
            groups[i].storage_path = prefixes[i];
            groups[i].core.self_id = 1U;
            groups[i].core.voters = &voter;
            groups[i].core.voter_count = 1U;
            groups[i].core.heartbeat_ticks = 1U;
            groups[i].core.election_min_ticks = 3U;
            groups[i].core.election_max_ticks = 5U;
            groups[i].core.initial_election_timeout_ticks = 3U;
            groups[i].core.max_log_entries = 128U;
        }
    }
    after_each() {
        cmeta_fs_dir_t *dir = NULL;
        cmeta_fs_dirent_t entry;
        if (directory != NULL && cmeta_fs_opendir(directory, &dir) == SALTS_OK) {
            while (cmeta_fs_readdir(dir, &entry) > 0) {
                if (strcmp(entry.name, ".") != 0 && strcmp(entry.name, "..") != 0) {
                    tstr file = tstr_format("{}/{}", directory, entry.name);
                    if (file != NULL) { (void)cmeta_fs_unlink(file); tstr_free(file); }
                }
            }
            (void)cmeta_fs_closedir(dir);
            (void)cmeta_fs_rmdir(directory);
        }
        for (size_t i = 0; i < 4U; ++i) tstr_freep(&prefixes[i]);
        tstr_freep(&directory);
    }
    it("replays separate WALs after stopping and changing owner count") {
        check_equal(multicore_node_run(&settings, counts, 4U), SALTS_OK);
        for (size_t i = 0; i < 4U; ++i) check_equal(counts[i], 1U);
        settings.runtime.owner_count = 4U;
        for (size_t i = 0; i < 4U; ++i) assignments[i].owner_index = (uint32_t)i;
        check_equal(multicore_node_run(&settings, counts, 4U), SALTS_OK);
        for (size_t i = 0; i < 4U; ++i) check_equal(counts[i], 2U);
    }
    it("rolls back opened WALs when a sibling path cannot be opened") {
        tstr bad = tstr_format("{}/missing/raft", directory);
        check_not_null(bad);
        groups[3].storage_path = bad;
        int result = multicore_node_run(&settings, counts, 4U);
        groups[3].storage_path = prefixes[3];
        tstr_free(bad);
        check_not_equal(result, SALTS_OK);
        /* A second start proves rollback released all acquired WAL locks. */
        check_equal(multicore_node_run(&settings, counts, 4U), SALTS_OK);
        for (size_t i = 0; i < 4U; ++i) check_equal(counts[i], 1U);
    }
}
