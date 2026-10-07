#include <turboraft/raft_node_config.h>
#include <cmeta_error.h>
#include <cmeta_fs.h>
#include <salts/clock.h>
#include <fmt.h>
#include <tinytest.h>
#include <string.h>

#define NODE_PREFIX "\"version\":1,\"node_id\":1,\"cluster_id\":\"12345678-1234-1234-1234-123456789abc\","
#define GROUP_ONE "{\"group_id\":9007199254740993,\"owner_index\":0,\"voters\":[1],\"learners\":[],\"storage_path\":\"/var/lib/raft/one\"}"
#define GROUP_TWO "{\"group_id\":2,\"owner_index\":1,\"voters\":[1],\"learners\":[],\"storage_path\":\"/var/lib/raft/two\"}"
#define VALID_JSON "{" NODE_PREFIX "\"owners\":[],\"groups\":[" GROUP_ONE "]}"

spec("versioned node configuration")
{
    static tr_raft_node_config_t *config;
    static DataBindError error;
    static tstr file_path;
    before_each() { config = NULL; file_path = NULL; memset(&error, 0, sizeof(error)); error.size = sizeof(error); }
    after_each() {
        tr_raft_node_config_destroy(config);
        if (file_path != NULL) { (void)cmeta_fs_unlink(file_path); tstr_freep(&file_path); }
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
        const char json[] = "{" NODE_PREFIX "\"owner_count\":2,\"owners\":[],\"groups\":[" GROUP_ONE "," GROUP_TWO "]}";
        check_equal(tr_raft_node_config_parse(json, sizeof(json) - 1U, &config, &error), SALTS_OK);
        check_equal(tr_raft_node_config_settings(config)->runtime.groups[1].owner_index, 1U);
    }
    it("rejects unknown fields including misspelled scheduling options") {
        const char json[] = "{" NODE_PREFIX "\"owner_cout\":4,\"owners\":[],\"groups\":[" GROUP_ONE "]}";
        check_equal(tr_raft_node_config_parse(json, sizeof(json) - 1U, &config, &error), SALTS_EINVAL);
        check_null(config);
    }
    it("rejects wrong scalar token kinds without coercion") {
        const char json[] = "{" NODE_PREFIX "\"owner_count\":\"2\",\"owners\":[],\"groups\":[" GROUP_ONE "]}";
        check_equal(tr_raft_node_config_parse(json, sizeof(json) - 1U, &config, &error), SALTS_EINVAL);
    }
    it("rejects a group mapped outside the owner set") {
        const char json[] = "{" NODE_PREFIX "\"owners\":[],\"groups\":[" GROUP_TWO "]}";
        check_equal(tr_raft_node_config_parse(json, sizeof(json) - 1U, &config, &error), SALTS_EINVAL);
    }
    it("rejects duplicate group IDs and storage prefixes") {
        const char json[] = "{" NODE_PREFIX "\"owners\":[],\"groups\":[" GROUP_ONE "," GROUP_ONE "]}";
        check_equal(tr_raft_node_config_parse(json, sizeof(json) - 1U, &config, &error), SALTS_EINVAL);
    }
    it("rejects capacity overflow before allocating runtime queues") {
        const char json[] = "{" NODE_PREFIX "\"capacity\":65537,\"owners\":[],\"groups\":[" GROUP_ONE "]}";
        check_equal(tr_raft_node_config_parse(json, sizeof(json) - 1U, &config, &error), SALTS_EINVAL);
    }
    it("rejects unsupported versions and oversized input") {
        const char json[] = "{\"version\":2,\"node_id\":1,\"cluster_id\":\"12345678-1234-1234-1234-123456789abc\",\"owners\":[],\"groups\":[" GROUP_ONE "]}";
        check_equal(tr_raft_node_config_parse(json, sizeof(json) - 1U, &config, &error), SALTS_EINVAL);
        check_equal(tr_raft_node_config_parse(json, TR_RAFT_NODE_CONFIG_MAX_BYTES + 1U, &config, &error), SALTS_EINVAL);
    }
    it("rejects incomplete TLS settings before opening any socket") {
        const char json[] = "{" NODE_PREFIX "\"network_enabled\":true,\"owners\":[{\"bind_endpoint\":\"tls://127.0.0.1:19001\",\"identity\":\"node1\",\"peers\":[{\"node_id\":2,\"identity\":\"node2\",\"endpoint\":\"tls://127.0.0.1:19002\",\"client_certificate_sha256\":[]}]}],\"groups\":[" GROUP_ONE "]}";
        check_equal(tr_raft_node_config_parse(json, sizeof(json) - 1U, &config, &error), SALTS_EINVAL);
    }
    it("parses a network plan without manufacturing authenticated handshakes") {
        const char json[] = "{" NODE_PREFIX "\"network_enabled\":true,\"owners\":[{\"bind_endpoint\":\"tcp://127.0.0.1:19001\",\"identity\":\"node1\",\"peers\":[{\"node_id\":2,\"identity\":\"node2\",\"endpoint\":\"tcp://127.0.0.1:19002\",\"client_certificate_sha256\":[]}]}],\"groups\":[" GROUP_ONE "]}";
        int result = tr_raft_node_config_parse(json, sizeof(json) - 1U, &config, &error);
        info("parse result %d, path %s, error %s", result, error.path, error.message);
        check_equal(result, SALTS_OK);
        check_null(tr_raft_node_config_settings(config)->owners[0].peers[0].handshake);
        check_equal(tr_raft_node_config_settings(config)->owners[0].outbound_limits.max_active_groups, 1U);
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
        cmeta_fs_buf_t buffer = {(char *)VALID_JSON, sizeof(VALID_JSON) - 1U};
        check_equal(cmeta_fs_get_tmpdir(temporary, sizeof(temporary)), SALTS_OK);
        file_path = tstr_format("{}/turboraft-node-{}.json", temporary, cmeta_hrtime());
        check_not_null(file_path);
        check_equal(cmeta_fs_write_file(file_path, &buffer), SALTS_OK);
        check_equal(tr_raft_node_config_load(file_path, &config, &error), SALTS_OK);
        check_equal(tr_raft_node_config_settings(config)->runtime.groups[0].group_id,
                    UINT64_C(9007199254740993));
    }
}
