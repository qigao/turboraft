#include <turboraft/turbodb_redis_state_machine.h>

#include <tinytest.h>
#include <salts_error.h>

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#define TR_TURBODB_REDIS_TEST_WAIT_TIMEOUT_NS UINT64_C(5000000000)
#define TR_TURBODB_REDIS_TEST_MAX_STEPS 16U
#define TR_TURBODB_REDIS_TEST_MAX_COMMAND_BYTES 8192U

static cflow_io_native_backend_kind tr_turbodb_redis_test_backend(void)
{
#if defined(_WIN32)
    return CFLOW_IO_NATIVE_IOCP;
#elif defined(__linux__)
    return CFLOW_IO_NATIVE_EPOLL;
#elif defined(__APPLE__)
    return CFLOW_IO_NATIVE_KQUEUE;
#else
    return CFLOW_IO_NATIVE_POLL;
#endif
}

static redis_reply_t *tr_turbodb_redis_test_command(
    redis_cflow_connection *connection, redis_io_runtime *runtime,
    int argc, const char **argv)
{
    redis_cflow_stream stream = {0};
    redis_cflow_stream_step step;
    size_t attempt;

    if (redis_cflow_command_open(connection, argc, argv, NULL, 4096U,
                                 &stream) != SALTS_OK)
        return NULL;
    for (attempt = 0U; attempt < TR_TURBODB_REDIS_TEST_MAX_STEPS; ++attempt) {
        step = redis_cflow_stream_next(&stream);
        if (step.kind == REDIS_CFLOW_STREAM_WAIT) {
            if (redis_io_runtime_wait_idle(
                    runtime, TR_TURBODB_REDIS_TEST_WAIT_TIMEOUT_NS) != SALTS_OK)
                break;
            continue;
        }
        if (step.kind == REDIS_CFLOW_STREAM_ITEM) {
            redis_reply_t *reply = step.item;
            step = redis_cflow_stream_next(&stream);
            if (step.kind == REDIS_CFLOW_STREAM_DONE) {
                (void)redis_cflow_stream_destroy(&stream);
                return reply;
            }
            redis_reply_free(reply);
        }
        redis_reply_free(step.item);
        break;
    }
    (void)redis_cflow_stream_destroy(&stream);
    return NULL;
}

static int tr_turbodb_redis_test_open_connection(
    redis_io_runtime *runtime, redis_cflow_connection *connection,
    const char *port_text)
{
    redis_io_runtime_config runtime_config = {
        tr_turbodb_redis_test_backend(), 1U, 1U};
    redis_cflow_open_config connection_config;
    redis_cflow_connect_step connect_step;
    char *port_end = NULL;
    unsigned long port;

    if (port_text == NULL || port_text[0] == '\0') return SALTS_EINVAL;
    port = strtoul(port_text, &port_end, 10);
    if (port_end == port_text || *port_end != '\0' || port == 0U ||
        port > UINT16_MAX)
        return SALTS_EINVAL;
    if (redis_io_runtime_init(runtime, &runtime_config) != SALTS_OK)
        return SALTS_EIO;
    connection_config = (redis_cflow_open_config){
        runtime, "127.0.0.1", (uint16_t)port, 1U,
        TR_TURBODB_REDIS_TEST_MAX_COMMAND_BYTES, 64U, 4096U, 64U,
        TR_TURBODB_REDIS_TEST_WAIT_TIMEOUT_NS};
    if (redis_cflow_connection_open(connection, &connection_config) != SALTS_OK)
        return SALTS_EIO;
    connect_step = redis_cflow_connection_connect_next(connection);
    if (connect_step.kind != REDIS_CFLOW_CONNECT_WAIT ||
        redis_io_runtime_wait_idle(runtime,
                                   TR_TURBODB_REDIS_TEST_WAIT_TIMEOUT_NS) != SALTS_OK)
        return SALTS_EIO;
    connect_step = redis_cflow_connection_connect_next(connection);
    return connect_step.kind == REDIS_CFLOW_CONNECT_DONE ? SALTS_OK : SALTS_EIO;
}

static tr_turbodb_redis_state_machine_config_t
tr_turbodb_redis_test_config(redis_cflow_connection *connection,
                             redis_io_runtime *runtime)
{
    return (tr_turbodb_redis_state_machine_config_t){
        connection, runtime, TR_TURBODB_REDIS_TEST_WAIT_TIMEOUT_NS, 16U, 2U,
        "raft:{orders}:meta", "raft:{orders}:journal",
        "raft:{orders}:identity", "raft:{orders}:outbox"};
}

spec("TurboDB Redis state machine")
{
    it("rejects an incomplete configuration before constructing an adapter")
    {
        tr_turbodb_redis_state_machine_config_t config;
        tr_turbodb_redis_state_machine_t *adapter = NULL;

        memset(&config, 0, sizeof(config));
        check_equal(tr_turbodb_redis_state_machine_open(&config, &adapter),
                    SALTS_EINVAL);
        check_null(adapter);
    }

    it("rejects a configuration entry before submitting a Redis command")
    {
        redis_cflow_connection connection = {0};
        redis_io_runtime io_runtime = {0};
        tr_turbodb_redis_state_machine_config_t config = {
            &connection, &io_runtime, UINT64_C(1), 1U, 1U,
            "raft:{orders}:meta", "raft:{orders}:journal",
            "raft:{orders}:identity", "raft:{orders}:outbox"};
        tr_turbodb_redis_state_machine_t *adapter = NULL;
        tr_raft_state_machine_t state_machine = {0};
        tr_raft_entry_t entry = {0};

        entry.index = 1U;
        entry.term = 1U;
        check_equal(tr_turbodb_redis_state_machine_open(&config, &adapter),
                    SALTS_OK);
        check_equal(tr_turbodb_redis_state_machine_bind(
                        adapter, &state_machine), SALTS_OK);
        check_equal(tr_turbodb_redis_state_machine_reconcile_batch(
                        adapter, &entry, 1U, NULL), SALTS_EINVAL);
        check_equal(state_machine.apply_batch(state_machine.context, &entry, 1U),
                    SALTS_EINVAL);
        check_equal(tr_turbodb_redis_state_machine_close(adapter), SALTS_OK);
    }

    it("rejects an invalid snapshot compaction request before I/O")
    {
        tr_turbodb_redis_state_machine_t *adapter = NULL;

        check_equal(tr_turbodb_redis_state_machine_compact_snapshot(
                        adapter, 0U, 1U, NULL), SALTS_EINVAL);
    }

    it("commits, replays, and rejects a conflicting Raft batch against Redis")
    {
        const char *port_text = getenv("TURBODB_REDIS_TEST_PORT");
        static const char *delete_command[] = {
            "DEL", "raft:{orders}:meta", "raft:{orders}:journal",
            "raft:{orders}:identity", "raft:{orders}:outbox"};
        static const char *meta_command[] = {
            "HGET", "raft:{orders}:meta", "applied_index"};
        static const char *outbox_command[] = {
            "XLEN", "raft:{orders}:outbox"};
        redis_io_runtime runtime = {0};
        redis_cflow_connection connection = {0};
        tr_turbodb_redis_state_machine_config_t config;
        tr_turbodb_redis_state_machine_t *adapter = NULL;
        tr_raft_state_machine_t state_machine = {0};
        tr_raft_entry_t entries[2] = {{0}};
        tr_turbodb_redis_reconcile_result_t reconcile_result;
        redis_reply_t *reply;

        if (port_text == NULL || port_text[0] == '\0') return;
        check_equal(tr_turbodb_redis_test_open_connection(
                        &runtime, &connection, port_text), SALTS_OK);
        reply = tr_turbodb_redis_test_command(&connection, &runtime, 5,
                                              delete_command);
        check_not_null(reply);
        redis_reply_free(reply);
        config = tr_turbodb_redis_test_config(&connection, &runtime);
        entries[0].index = 1U;
        entries[0].term = 1U;
        entries[0].command_id = 101U;
        entries[0].data_length = 3U;
        memcpy(entries[0].data, "one", 3U);
        entries[1].index = 2U;
        entries[1].term = 1U;
        entries[1].command_id = 102U;
        entries[1].data_length = 3U;
        memcpy(entries[1].data, "two", 3U);
        check_equal(tr_turbodb_redis_state_machine_open(&config, &adapter),
                    SALTS_OK);
        check_equal(tr_turbodb_redis_state_machine_bind(adapter, &state_machine),
                    SALTS_OK);
        check_equal(tr_turbodb_redis_state_machine_reconcile_batch(
                        adapter, entries, 2U, &reconcile_result), SALTS_OK);
        check_equal(reconcile_result, TR_TURBODB_REDIS_RECONCILE_PENDING);
        check_equal(state_machine.apply_batch(state_machine.context, entries, 2U),
                    SALTS_OK);
        check_equal(state_machine.apply_batch(state_machine.context, entries, 2U),
                    SALTS_OK);
        check_equal(tr_turbodb_redis_state_machine_reconcile_batch(
                        adapter, entries, 2U, &reconcile_result), SALTS_OK);
        check_equal(reconcile_result, TR_TURBODB_REDIS_RECONCILE_REPLAYED);
        reply = tr_turbodb_redis_test_command(&connection, &runtime, 3,
                                              meta_command);
        check_not_null(reply);
        check_equal(reply->str, "2", 1U);
        redis_reply_free(reply);
        reply = tr_turbodb_redis_test_command(&connection, &runtime, 2,
                                              outbox_command);
        check_not_null(reply);
        check_equal(reply->integer, 2);
        redis_reply_free(reply);
        memcpy(entries[0].data, "bad", 3U);
        check_equal(state_machine.apply_batch(state_machine.context, entries, 2U),
                    SALTS_EPROTO);
        check_equal(tr_turbodb_redis_state_machine_reconcile_batch(
                        adapter, entries, 2U, &reconcile_result), SALTS_EPROTO);
        reply = tr_turbodb_redis_test_command(&connection, &runtime, 2,
                                              outbox_command);
        check_not_null(reply);
        check_equal(reply->integer, 2);
        redis_reply_free(reply);
        check_equal(tr_turbodb_redis_state_machine_close(adapter), SALTS_OK);
        check_equal(redis_cflow_connection_destroy(&connection), SALTS_OK);
        check_equal(redis_io_runtime_close(&runtime), SALTS_OK);
        check_equal(redis_io_runtime_destroy(&runtime), SALTS_OK);
    }

    it("compacts a durable journal and replays the same snapshot request")
    {
        const char *port_text = getenv("TURBODB_REDIS_TEST_PORT");
        static const char *delete_command[] = {
            "DEL", "raft:{orders}:meta", "raft:{orders}:journal",
            "raft:{orders}:identity", "raft:{orders}:outbox"};
        static const char *seed_command[] = {
            "HSET", "raft:{orders}:meta", "applied_index", "41", "term", "7",
            "command_id", "4101", "journal_floor", "41"};
        static const char *high_seed_command[] = {
            "HSET", "raft:{orders}:meta", "applied_index", "18446744073709551613",
            "term", "8", "command_id", "seed-high", "journal_floor",
            "18446744073709551613"};
        static const char *floor_command[] = {
            "HGET", "raft:{orders}:meta", "journal_floor"};
        redis_io_runtime runtime = {0};
        redis_cflow_connection connection = {0};
        tr_turbodb_redis_state_machine_config_t config;
        tr_turbodb_redis_state_machine_t *adapter = NULL;
        tr_raft_state_machine_t state_machine = {0};
        tr_raft_entry_t entries[2] = {{0}};
        tr_raft_entry_t high_entries[2] = {{0}};
        tr_turbodb_redis_compact_result_t compact_result;
        redis_reply_t *reply;

        if (port_text == NULL || port_text[0] == '\0') return;
        check_equal(tr_turbodb_redis_test_open_connection(
                        &runtime, &connection, port_text), SALTS_OK);
        reply = tr_turbodb_redis_test_command(&connection, &runtime, 5,
                                              delete_command);
        check_not_null(reply);
        redis_reply_free(reply);
        reply = tr_turbodb_redis_test_command(&connection, &runtime, 10,
                                              seed_command);
        check_not_null(reply);
        redis_reply_free(reply);
        config = tr_turbodb_redis_test_config(&connection, &runtime);
        entries[0].index = 42U;
        entries[0].term = 123456U;
        entries[0].command_id = 4201U;
        entries[0].data_length = 5U;
        memcpy(entries[0].data, "first", 5U);
        entries[1].index = 43U;
        entries[1].term = 123456U;
        entries[1].command_id = 4301U;
        entries[1].data_length = 6U;
        memcpy(entries[1].data, "second", 6U);
        check_equal(tr_turbodb_redis_state_machine_open(&config, &adapter),
                    SALTS_OK);
        check_equal(tr_turbodb_redis_state_machine_bind(adapter, &state_machine),
                    SALTS_OK);
        check_equal(state_machine.apply_batch(state_machine.context, entries, 2U),
                    SALTS_OK);
        check_equal(tr_turbodb_redis_state_machine_compact_snapshot(
                        adapter, 43U, 123455U, &compact_result), SALTS_EPROTO);
        reply = tr_turbodb_redis_test_command(&connection, &runtime, 3,
                                              floor_command);
        check_not_null(reply);
        check_equal(reply->str, "41", 2U);
        redis_reply_free(reply);
        check_equal(tr_turbodb_redis_state_machine_compact_snapshot(
                        adapter, 43U, 123456U, &compact_result), SALTS_OK);
        check_equal(compact_result, TR_TURBODB_REDIS_COMPACTED);
        reply = tr_turbodb_redis_test_command(&connection, &runtime, 3,
                                              floor_command);
        check_not_null(reply);
        check_equal(reply->str, "43", 2U);
        redis_reply_free(reply);
        check_equal(tr_turbodb_redis_state_machine_compact_snapshot(
                        adapter, 43U, 123456U, &compact_result), SALTS_OK);
        check_equal(compact_result, TR_TURBODB_REDIS_COMPACT_REPLAYED);

        reply = tr_turbodb_redis_test_command(&connection, &runtime, 5,
                                              delete_command);
        check_not_null(reply);
        redis_reply_free(reply);
        reply = tr_turbodb_redis_test_command(&connection, &runtime, 10,
                                              high_seed_command);
        check_not_null(reply);
        redis_reply_free(reply);
        high_entries[0].index = UINT64_MAX - UINT64_C(1);
        high_entries[0].term = 8U;
        high_entries[0].command_id = 18446744073709551613ULL;
        high_entries[0].data_length = 10U;
        memcpy(high_entries[0].data, "high-first", 10U);
        high_entries[1].index = UINT64_MAX;
        high_entries[1].term = 8U;
        high_entries[1].command_id = 18446744073709551614ULL;
        high_entries[1].data_length = 9U;
        memcpy(high_entries[1].data, "high-last", 9U);
        check_equal(state_machine.apply_batch(state_machine.context,
                                              high_entries, 2U),
                    SALTS_OK);
        check_equal(tr_turbodb_redis_state_machine_compact_snapshot(
                        adapter, UINT64_MAX, 8U, &compact_result), SALTS_OK);
        check_equal(compact_result, TR_TURBODB_REDIS_COMPACTED);
        reply = tr_turbodb_redis_test_command(&connection, &runtime, 3,
                                              floor_command);
        check_not_null(reply);
        check_equal(reply->str, "18446744073709551615", 20U);
        redis_reply_free(reply);
        check_equal(tr_turbodb_redis_state_machine_compact_snapshot(
                        adapter, UINT64_MAX, 8U, &compact_result), SALTS_OK);
        check_equal(compact_result, TR_TURBODB_REDIS_COMPACT_REPLAYED);
        check_equal(tr_turbodb_redis_state_machine_close(adapter), SALTS_OK);
        check_equal(redis_cflow_connection_destroy(&connection), SALTS_OK);
        check_equal(redis_io_runtime_close(&runtime), SALTS_OK);
        check_equal(redis_io_runtime_destroy(&runtime), SALTS_OK);
    }
}
