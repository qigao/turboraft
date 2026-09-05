#include <turboraft/turbodb_redis_state_machine.h>

#include <tinytest.h>
#include <salts_error.h>

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#define TR_TURBODB_REDIS_TEST_WAIT_TIMEOUT_NS UINT64_C(5000000000)
#define TR_TURBODB_REDIS_TEST_MAX_STEPS 16U

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
        check_equal(state_machine.apply_batch(state_machine.context, &entry, 1U),
                    SALTS_EINVAL);
        check_equal(tr_turbodb_redis_state_machine_close(adapter), SALTS_OK);
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
        redis_io_runtime_config runtime_config = {
            tr_turbodb_redis_test_backend(), 1U, 1U};
        redis_cflow_connection connection = {0};
        redis_cflow_open_config connection_config;
        redis_cflow_connect_step connect_step;
        tr_turbodb_redis_state_machine_config_t config;
        tr_turbodb_redis_state_machine_t *adapter = NULL;
        tr_raft_state_machine_t state_machine = {0};
        tr_raft_entry_t entries[2] = {{0}};
        redis_reply_t *reply;
        char *port_end = NULL;
        unsigned long port;

        if (port_text == NULL || port_text[0] == '\0') return;
        port = strtoul(port_text, &port_end, 10);
        check_true(port_end != port_text && *port_end == '\0' && port > 0U &&
                   port <= UINT16_MAX);
        check_equal(redis_io_runtime_init(&runtime, &runtime_config), SALTS_OK);
        connection_config = (redis_cflow_open_config){
            &runtime, "127.0.0.1", (uint16_t)port, 1U, 4096U, 64U, 4096U, 64U,
            TR_TURBODB_REDIS_TEST_WAIT_TIMEOUT_NS};
        check_equal(redis_cflow_connection_open(&connection, &connection_config),
                    SALTS_OK);
        connect_step = redis_cflow_connection_connect_next(&connection);
        check_equal(connect_step.kind, REDIS_CFLOW_CONNECT_WAIT);
        check_equal(redis_io_runtime_wait_idle(
                        &runtime, TR_TURBODB_REDIS_TEST_WAIT_TIMEOUT_NS),
                    SALTS_OK);
        connect_step = redis_cflow_connection_connect_next(&connection);
        check_equal(connect_step.kind, REDIS_CFLOW_CONNECT_DONE);
        reply = tr_turbodb_redis_test_command(&connection, &runtime, 5,
                                              delete_command);
        check_not_null(reply);
        redis_reply_free(reply);

        config = (tr_turbodb_redis_state_machine_config_t){
            &connection, &runtime, TR_TURBODB_REDIS_TEST_WAIT_TIMEOUT_NS, 16U,
            2U, "raft:{orders}:meta", "raft:{orders}:journal",
            "raft:{orders}:identity", "raft:{orders}:outbox"};
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
        check_equal(state_machine.apply_batch(state_machine.context, entries, 2U),
                    SALTS_OK);
        check_equal(state_machine.apply_batch(state_machine.context, entries, 2U),
                    SALTS_OK);
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
}
