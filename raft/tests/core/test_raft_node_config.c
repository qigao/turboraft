#include <turboraft/raft_node_config.h>
#include <core/error.h>
#include <file/fs.h>
#include <core/clock.h>
#include <core/fmt.h>
#include <tinytest.h>
#include <string.h>

#define NODE_PREFIX "\"version\":1,\"node_id\":1,\"cluster_id\":\"12345678-1234-1234-1234-123456789abc\","
#define GROUP_ONE "{\"group_id\":9007199254740993,\"owner_index\":0,\"voters\":[1],\"learners\":[],\"storage_path\":\"/var/lib/raft/one\"}"
#define GROUP_TWO "{\"group_id\":2,\"owner_index\":1,\"voters\":[1],\"learners\":[],\"storage_path\":\"/var/lib/raft/two\"}"
#define VALID_JSON "{" NODE_PREFIX "\"groups\":[" GROUP_ONE "]}"

spec("versioned node configuration")
{
    static tr_raft_node_config_t *config;
    static DataBindError error;
    static tstr file_path;
    before_each() { config = NULL; file_path = NULL; memset(&error, 0, sizeof(error)); error.size = sizeof(error); }
    after_each() {
        tr_raft_node_config_destroy(config);
        if (file_path != NULL) { (void)fs_unlink(file_path); tstr_freep(&file_path); }
    }

    it("loads schema defaults and preserves exact group IDs above 2^53") {
        const tr_raft_node_settings_t *settings;
        check_equal(tr_raft_node_config_parse(VALID_JSON, sizeof(VALID_JSON) - 1U, &config, &error), SALTS_OK);
        settings = tr_raft_node_config_settings(config);
        check_equal(settings->runtime.groups[0].group_id, UINT64_C(9007199254740993));
        check_equal(settings->runtime.capacity, 64U);
        check_equal(settings->runtime.work_budget, 8U);
        check_equal(settings->groups[0].core.election_min_ticks, 10U);
        check_equal(settings->groups[0].storage_path, "/var/lib/raft/one");
        check_equal(tr_raft_node_settings_validate(settings), SALTS_OK);
    }
    it("maps multiple groups to explicit owners") {
        const char json[] = "{" NODE_PREFIX "\"owner_count\":2,\"groups\":[" GROUP_ONE "," GROUP_TWO "]}";
        check_equal(tr_raft_node_config_parse(json, sizeof(json) - 1U, &config, &error), SALTS_OK);
        check_equal(tr_raft_node_config_settings(config)->runtime.groups[1].owner_index, 1U);
    }
    it("rejects unknown fields including misspelled scheduling options") {
        const char json[] = "{" NODE_PREFIX "\"owner_cout\":4,\"groups\":[" GROUP_ONE "]}";
        check_equal(tr_raft_node_config_parse(json, sizeof(json) - 1U, &config, &error), SALTS_EINVAL);
        check_null(config);
    }
    it("rejects wrong scalar token kinds without coercion") {
        const char json[] = "{" NODE_PREFIX "\"owner_count\":\"2\",\"groups\":[" GROUP_ONE "]}";
        check_equal(tr_raft_node_config_parse(json, sizeof(json) - 1U, &config, &error), SALTS_EINVAL);
    }
    it("rejects a group mapped outside the owner set") {
        const char json[] = "{" NODE_PREFIX "\"groups\":[" GROUP_TWO "]}";
        check_equal(tr_raft_node_config_parse(json, sizeof(json) - 1U, &config, &error), SALTS_EINVAL);
    }
    it("rejects duplicate group IDs and storage prefixes") {
        const char json[] = "{" NODE_PREFIX "\"groups\":[" GROUP_ONE "," GROUP_ONE "]}";
        check_equal(tr_raft_node_config_parse(json, sizeof(json) - 1U, &config, &error), SALTS_EINVAL);
    }
    it("rejects capacity overflow before allocating runtime queues") {
        const char json[] = "{" NODE_PREFIX "\"capacity\":65537,\"groups\":[" GROUP_ONE "]}";
        check_equal(tr_raft_node_config_parse(json, sizeof(json) - 1U, &config, &error), SALTS_EINVAL);
    }
    it("rejects unsupported versions and oversized input") {
        const char json[] = "{\"version\":2,\"node_id\":1,\"cluster_id\":\"12345678-1234-1234-1234-123456789abc\",\"groups\":[" GROUP_ONE "]}";
        check_equal(tr_raft_node_config_parse(json, sizeof(json) - 1U, &config, &error), SALTS_EINVAL);
        check_equal(tr_raft_node_config_parse(json, TR_RAFT_NODE_CONFIG_MAX_BYTES + 1U, &config, &error), SALTS_EINVAL);
    }
    it("rejects removed transport configuration fields") {
        const char *fields[] = {
            "\"network_enabled\":false", "\"owners\":[]", "\"config_epoch\":1",
            "\"network_batch\":16", "\"network_queue_items\":256",
            "\"network_queue_bytes\":4194304", "\"reconnect_initial_ms\":10",
            "\"reconnect_max_ms\":1000", "\"heartbeat_interval_ms\":1000",
            "\"heartbeat_timeout_ms\":5000"
        };
        for (size_t i = 0; i < sizeof(fields) / sizeof(fields[0]); ++i) {
            tstr json = tstr_format("{{{}{},\"groups\":[{}]}}", NODE_PREFIX, fields[i], GROUP_ONE);
            check_not_null(json);
            int result = tr_raft_node_config_parse(json, tstr_len(json), &config, &error);
            tstr_freep(&json);
            check_equal(result, SALTS_EINVAL);
            check_null(config);
        }
    }
    it("validates remote membership independently of the host transport") {
        const char json[] = "{" NODE_PREFIX "\"groups\":[{\"group_id\":1,\"owner_index\":0,"
            "\"voters\":[1,2],\"learners\":[3],\"storage_path\":\"/var/lib/raft/one\"}]}";
        check_equal(tr_raft_node_config_parse(json, sizeof(json) - 1U, &config, &error), SALTS_OK);
        const tr_raft_node_settings_t *settings = tr_raft_node_config_settings(config);
        check_equal(settings->groups[0].core.voter_count, 2U);
        check_equal(settings->groups[0].core.voters[1], 2U);
        check_equal(settings->groups[0].core.learners[0], 3U);
    }
    it("shares validation with programmatic settings") {
        tr_raft_node_settings_t settings;
        check_equal(tr_raft_node_config_parse(VALID_JSON, sizeof(VALID_JSON) - 1U, &config, &error), SALTS_OK);
        settings = *tr_raft_node_config_settings(config);
        settings.runtime.idle_ms = settings.runtime.tick_ms + 1U;
        check_equal(tr_raft_node_settings_validate(&settings), SALTS_EINVAL);
        settings = *tr_raft_node_config_settings(config);
        settings.node_id = 2U;
        check_equal(tr_raft_node_settings_validate(&settings), SALTS_EINVAL);
    }
    it("loads the same schema and defaults from a bounded file read") {
        char temporary[4096];
        fs_buf_t buffer = {(char *)VALID_JSON, sizeof(VALID_JSON) - 1U};
        check_equal(fs_get_tmpdir(temporary, sizeof(temporary)), SALTS_OK);
        file_path = tstr_format("{}/turboraft-node-{}.json", temporary, hrtime());
        check_not_null(file_path);
        check_equal(fs_write_file(file_path, &buffer), SALTS_OK);
        check_equal(tr_raft_node_config_load(file_path, &config, &error), SALTS_OK);
        check_equal(tr_raft_node_config_settings(config)->runtime.groups[0].group_id,
                    UINT64_C(9007199254740993));
    }
}
