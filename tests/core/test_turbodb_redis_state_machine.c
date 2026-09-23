#include <turboraft/turbodb_redis_state_machine.h>

#include <tinytest.h>
#include <salts_error.h>

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#define TR_TURBODB_REDIS_TEST_WAIT_TIMEOUT_NS UINT64_C(5000000000)
#define TR_TURBODB_REDIS_TEST_MAX_STEPS 16U
#define TR_TURBODB_REDIS_TEST_MAX_COMMAND_BYTES 8192U
#define TR_TURBODB_REDIS_TEST_PROXY_POLL_ATTEMPTS 5000U
#define TR_TURBODB_REDIS_TEST_PROXY_JOIN_TIMEOUT_MS 6000U
#define TR_TURBODB_REDIS_TEST_PROXY_MAX_ARGUMENTS 16U
#define TR_TURBODB_REDIS_TEST_RESP_INCOMPLETE 1

#if defined(_WIN32)
  #include <winsock2.h>
  #include <windows.h>
typedef SOCKET tr_turbodb_redis_test_socket_t;
typedef HANDLE tr_turbodb_redis_test_thread_t;
  #define TR_TURBODB_REDIS_TEST_INVALID_SOCKET INVALID_SOCKET
#else
  #include <arpa/inet.h>
  #include <errno.h>
  #include <fcntl.h>
  #include <netinet/in.h>
  #include <pthread.h>
  #include <sys/socket.h>
  #include <unistd.h>
typedef int tr_turbodb_redis_test_socket_t;
typedef pthread_t tr_turbodb_redis_test_thread_t;
  #define TR_TURBODB_REDIS_TEST_INVALID_SOCKET (-1)
#endif

typedef struct tr_turbodb_redis_test_unknown_commit_peer {
    tr_turbodb_redis_test_socket_t socket_value;
    const char *state_reply;
    size_t state_reply_length;
    int close_after_compact_command;
    int result;
} tr_turbodb_redis_test_unknown_commit_peer_t;

typedef struct tr_turbodb_redis_test_resp_command {
    char buffer[TR_TURBODB_REDIS_TEST_MAX_COMMAND_BYTES + 1U];
    const char *arguments[TR_TURBODB_REDIS_TEST_PROXY_MAX_ARGUMENTS];
    size_t argument_lengths[TR_TURBODB_REDIS_TEST_PROXY_MAX_ARGUMENTS];
    size_t argument_count;
} tr_turbodb_redis_test_resp_command_t;

static int tr_turbodb_redis_test_socket_error(void)
{
#if defined(_WIN32)
    return -WSAGetLastError();
#else
    return -errno;
#endif
}

static void tr_turbodb_redis_test_close_socket(
    tr_turbodb_redis_test_socket_t socket_value)
{
    if (socket_value == TR_TURBODB_REDIS_TEST_INVALID_SOCKET) return;
#if defined(_WIN32)
    (void)closesocket(socket_value);
#else
    (void)close(socket_value);
#endif
}

static int tr_turbodb_redis_test_set_nonblocking(
    tr_turbodb_redis_test_socket_t socket_value)
{
#if defined(_WIN32)
    u_long enabled = 1U;
    return ioctlsocket(socket_value, FIONBIO, &enabled) == 0
               ? SALTS_OK
               : tr_turbodb_redis_test_socket_error();
#else
    int flags = fcntl(socket_value, F_GETFL);

    if (flags < 0) return -errno;
    return fcntl(socket_value, F_SETFL, flags | O_NONBLOCK) == 0
               ? SALTS_OK
               : -errno;
#endif
}

static int tr_turbodb_redis_test_socket_pair(
    tr_turbodb_redis_test_socket_t sockets[2])
{
    tr_turbodb_redis_test_socket_t listener =
        TR_TURBODB_REDIS_TEST_INVALID_SOCKET;
    struct sockaddr_in address;
#if defined(_WIN32)
    int address_size = (int)sizeof(address);
#else
    socklen_t address_size = (socklen_t)sizeof(address);
#endif
    int result = SALTS_OK;

    sockets[0] = TR_TURBODB_REDIS_TEST_INVALID_SOCKET;
    sockets[1] = TR_TURBODB_REDIS_TEST_INVALID_SOCKET;
    listener = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (listener == TR_TURBODB_REDIS_TEST_INVALID_SOCKET)
        return tr_turbodb_redis_test_socket_error();
    memset(&address, 0, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (bind(listener, (const struct sockaddr *)&address, (int)sizeof(address)) != 0 ||
        getsockname(listener, (struct sockaddr *)&address, &address_size) != 0 ||
        listen(listener, 1) != 0)
        result = tr_turbodb_redis_test_socket_error();
    if (result == SALTS_OK) {
        sockets[0] = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        if (sockets[0] == TR_TURBODB_REDIS_TEST_INVALID_SOCKET)
            result = tr_turbodb_redis_test_socket_error();
    }
    if (result == SALTS_OK &&
        connect(sockets[0], (const struct sockaddr *)&address,
                (int)sizeof(address)) != 0)
        result = tr_turbodb_redis_test_socket_error();
    if (result == SALTS_OK) {
        sockets[1] = accept(listener, NULL, NULL);
        if (sockets[1] == TR_TURBODB_REDIS_TEST_INVALID_SOCKET)
            result = tr_turbodb_redis_test_socket_error();
    }
    tr_turbodb_redis_test_close_socket(listener);
    if (result != SALTS_OK) {
        tr_turbodb_redis_test_close_socket(sockets[0]);
        tr_turbodb_redis_test_close_socket(sockets[1]);
        sockets[0] = TR_TURBODB_REDIS_TEST_INVALID_SOCKET;
        sockets[1] = TR_TURBODB_REDIS_TEST_INVALID_SOCKET;
        return result;
    }
    result = tr_turbodb_redis_test_set_nonblocking(sockets[0]);
    if (result == SALTS_OK)
        result = tr_turbodb_redis_test_set_nonblocking(sockets[1]);
    if (result != SALTS_OK) {
        tr_turbodb_redis_test_close_socket(sockets[0]);
        tr_turbodb_redis_test_close_socket(sockets[1]);
        sockets[0] = TR_TURBODB_REDIS_TEST_INVALID_SOCKET;
        sockets[1] = TR_TURBODB_REDIS_TEST_INVALID_SOCKET;
    }
    return result;
}

static int tr_turbodb_redis_test_resp_parse_decimal(
    const char *buffer, size_t buffer_length, size_t *inout_offset,
    size_t *out_value)
{
    size_t offset = *inout_offset;
    size_t value = 0U;

    if (buffer == NULL || inout_offset == NULL || out_value == NULL)
        return SALTS_EINVAL;
    while (offset < buffer_length) {
        unsigned char character = (unsigned char)buffer[offset];

        if (character == '\r') {
            if (offset + 1U >= buffer_length)
                return TR_TURBODB_REDIS_TEST_RESP_INCOMPLETE;
            if (buffer[offset + 1U] != '\n' || offset == *inout_offset)
                return SALTS_EPROTO;
            *inout_offset = offset + 2U;
            *out_value = value;
            return SALTS_OK;
        }
        if (character < (unsigned char)'0' || character > (unsigned char)'9')
            return SALTS_EPROTO;
        if (value > (SIZE_MAX - (size_t)(character - (unsigned char)'0')) / 10U)
            return SALTS_ERANGE;
        value = value * 10U + (size_t)(character - (unsigned char)'0');
        ++offset;
    }
    return TR_TURBODB_REDIS_TEST_RESP_INCOMPLETE;
}

static int tr_turbodb_redis_test_resp_parse_command(
    tr_turbodb_redis_test_resp_command_t *command, size_t buffer_length)
{
    size_t offset = 0U;
    size_t argument_count;
    size_t index;
    int result;

    if (command == NULL || buffer_length == 0U)
        return TR_TURBODB_REDIS_TEST_RESP_INCOMPLETE;
    if (command->buffer[offset++] != '*') return SALTS_EPROTO;
    result = tr_turbodb_redis_test_resp_parse_decimal(
        command->buffer, buffer_length, &offset, &argument_count);
    if (result != SALTS_OK) return result;
    if (argument_count == 0U ||
        argument_count > TR_TURBODB_REDIS_TEST_PROXY_MAX_ARGUMENTS)
        return SALTS_EPROTO;
    for (index = 0U; index < argument_count; ++index) {
        size_t argument_length;

        if (offset >= buffer_length)
            return TR_TURBODB_REDIS_TEST_RESP_INCOMPLETE;
        if (command->buffer[offset++] != '$') return SALTS_EPROTO;
        result = tr_turbodb_redis_test_resp_parse_decimal(
            command->buffer, buffer_length, &offset, &argument_length);
        if (result != SALTS_OK) return result;
        if (argument_length > buffer_length - offset ||
            buffer_length - offset - argument_length < 2U)
            return TR_TURBODB_REDIS_TEST_RESP_INCOMPLETE;
        command->arguments[index] = command->buffer + offset;
        command->argument_lengths[index] = argument_length;
        offset += argument_length;
        if (command->buffer[offset] != '\r' || command->buffer[offset + 1U] != '\n')
            return SALTS_EPROTO;
        offset += 2U;
    }
    if (offset != buffer_length) return SALTS_EPROTO;
    command->argument_count = argument_count;
    return SALTS_OK;
}

static int tr_turbodb_redis_test_peer_read_command(
    tr_turbodb_redis_test_socket_t socket_value,
    tr_turbodb_redis_test_resp_command_t *out_command)
{
    size_t received_total = 0U;
    size_t attempt;

    if (out_command == NULL) return SALTS_EINVAL;
    memset(out_command, 0, sizeof(*out_command));
    for (attempt = 0U; attempt < TR_TURBODB_REDIS_TEST_PROXY_POLL_ATTEMPTS;
         ++attempt) {
        int received = recv(socket_value, out_command->buffer + received_total,
                            (int)(sizeof(out_command->buffer) - received_total - 1U), 0);

        if (received > 0) {
            received_total += (size_t)received;
            out_command->buffer[received_total] = '\0';
            {
                int result = tr_turbodb_redis_test_resp_parse_command(
                    out_command, received_total);

                if (result == SALTS_OK) return SALTS_OK;
                if (result != TR_TURBODB_REDIS_TEST_RESP_INCOMPLETE)
                    return result;
            }
            if (received_total + 1U == sizeof(out_command->buffer))
                return SALTS_ENOBUFS;
            continue;
        }
        if (received == 0) return SALTS_EIO;
#if defined(_WIN32)
        if (WSAGetLastError() != WSAEWOULDBLOCK)
            return tr_turbodb_redis_test_socket_error();
        Sleep(1U);
#else
        if (errno != EAGAIN && errno != EWOULDBLOCK) return -errno;
        usleep(1000U);
#endif
    }
    return SALTS_ETIMEDOUT;
}

static int tr_turbodb_redis_test_resp_argument_equals(
    const tr_turbodb_redis_test_resp_command_t *command, size_t index,
    const char *expected)
{
    size_t expected_length;

    if (command == NULL || expected == NULL || index >= command->argument_count)
        return 0;
    expected_length = strlen(expected);
    return command->argument_lengths[index] == expected_length &&
           memcmp(command->arguments[index], expected, expected_length) == 0;
}

static int tr_turbodb_redis_test_peer_validate_state_command(
    const tr_turbodb_redis_test_resp_command_t *command)
{
    return command != NULL && command->argument_count == 4U &&
           tr_turbodb_redis_test_resp_argument_equals(command, 0U, "EVAL") &&
           tr_turbodb_redis_test_resp_argument_equals(command, 2U, "1") &&
           tr_turbodb_redis_test_resp_argument_equals(
               command, 3U, "raft:{orders}:meta")
               ? SALTS_OK
               : SALTS_EPROTO;
}

static int tr_turbodb_redis_test_peer_validate_compact_command(
    const tr_turbodb_redis_test_resp_command_t *command)
{
    return command != NULL && command->argument_count == 12U &&
           tr_turbodb_redis_test_resp_argument_equals(command, 0U, "EVAL") &&
           tr_turbodb_redis_test_resp_argument_equals(command, 2U, "4") &&
           tr_turbodb_redis_test_resp_argument_equals(command, 7U, "43") &&
           tr_turbodb_redis_test_resp_argument_equals(command, 8U, "43") &&
           tr_turbodb_redis_test_resp_argument_equals(command, 9U, "43") &&
           tr_turbodb_redis_test_resp_argument_equals(command, 10U, "123456")
               ? SALTS_OK
               : SALTS_EPROTO;
}

static int tr_turbodb_redis_test_peer_send_all(
    tr_turbodb_redis_test_socket_t socket_value, const char *data,
    size_t data_length)
{
    size_t written = 0U;

    while (written < data_length) {
        int result = send(socket_value, data + written,
                          (int)(data_length - written), 0);

        if (result > 0) {
            written += (size_t)result;
            continue;
        }
#if defined(_WIN32)
        if (result < 0 && WSAGetLastError() == WSAEWOULDBLOCK) {
            Sleep(1U);
            continue;
        }
#else
        if (result < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            usleep(1000U);
            continue;
        }
#endif
        return tr_turbodb_redis_test_socket_error();
    }
    return SALTS_OK;
}

#if defined(_WIN32)
static DWORD WINAPI tr_turbodb_redis_test_unknown_commit_peer_main(
    LPVOID context)
#else
static void *tr_turbodb_redis_test_unknown_commit_peer_main(void *context)
#endif
{
    tr_turbodb_redis_test_unknown_commit_peer_t *peer = context;
    tr_turbodb_redis_test_resp_command_t command;

    peer->result = tr_turbodb_redis_test_peer_read_command(peer->socket_value,
                                                            &command);
    if (peer->result == SALTS_OK)
        peer->result = tr_turbodb_redis_test_peer_validate_state_command(&command);
    if (peer->result == SALTS_OK)
        peer->result = tr_turbodb_redis_test_peer_send_all(
            peer->socket_value, peer->state_reply, peer->state_reply_length);
    if (peer->result == SALTS_OK && peer->close_after_compact_command)
        peer->result = tr_turbodb_redis_test_peer_read_command(peer->socket_value,
                                                                &command);
    if (peer->result == SALTS_OK && peer->close_after_compact_command)
        peer->result = tr_turbodb_redis_test_peer_validate_compact_command(&command);
    if (peer->close_after_compact_command) {
        tr_turbodb_redis_test_close_socket(peer->socket_value);
        peer->socket_value = TR_TURBODB_REDIS_TEST_INVALID_SOCKET;
    }
#if defined(_WIN32)
    return 0U;
#else
    return NULL;
#endif
}

static int tr_turbodb_redis_test_unknown_commit_peer_start(
    tr_turbodb_redis_test_unknown_commit_peer_t *peer,
    tr_turbodb_redis_test_thread_t *out_thread)
{
#if defined(_WIN32)
    *out_thread = CreateThread(NULL, 0U,
                               tr_turbodb_redis_test_unknown_commit_peer_main,
                               peer, 0U, NULL);
    return *out_thread == NULL ? SALTS_EIO : SALTS_OK;
#else
    return pthread_create(out_thread, NULL,
                          tr_turbodb_redis_test_unknown_commit_peer_main,
                          peer) == 0
               ? SALTS_OK
               : SALTS_EIO;
#endif
}

static int tr_turbodb_redis_test_unknown_commit_peer_join(
    tr_turbodb_redis_test_thread_t thread)
{
#if defined(_WIN32)
    DWORD wait_result = WaitForSingleObject(
        thread, TR_TURBODB_REDIS_TEST_PROXY_JOIN_TIMEOUT_MS);
    BOOL close_result = CloseHandle(thread);

    return wait_result == WAIT_OBJECT_0 && close_result ? SALTS_OK : SALTS_EIO;
#else
    return pthread_join(thread, NULL) == 0 ? SALTS_OK : SALTS_EIO;
#endif
}

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
    do {
        connect_step = redis_cflow_connection_connect_next(connection);
        if (connect_step.kind == REDIS_CFLOW_CONNECT_WAIT) {
            if (redis_io_runtime_wait_idle(
                    runtime, TR_TURBODB_REDIS_TEST_WAIT_TIMEOUT_NS) !=
                SALTS_OK) {
                return SALTS_EIO;
            }
        }
    } while (connect_step.kind == REDIS_CFLOW_CONNECT_WAIT);
    return connect_step.kind == REDIS_CFLOW_CONNECT_DONE
               ? SALTS_OK
               : connect_step.status != SALTS_OK
                     ? connect_step.status
                     : SALTS_EIO;
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

    it("retries the same snapshot after a sent compaction loses its reply")
    {
        static const char state_at_42_reply[] =
            "*2\r\n+APPLIED\r\n$2\r\n42\r\n";
        static const char state_at_43_reply[] =
            "*2\r\n+APPLIED\r\n$2\r\n43\r\n";
        redis_io_runtime runtime = {0};
        redis_io_runtime_config runtime_config = {
            tr_turbodb_redis_test_backend(), 1U, 1U};
        tr_turbodb_redis_test_socket_t sockets[2];
        redis_cflow_connection connection = {0};
        redis_cflow_connection_config connection_config;
        tr_turbodb_redis_state_machine_config_t config;
        tr_turbodb_redis_state_machine_t *adapter = NULL;
        tr_turbodb_redis_compact_result_t compact_result;
        tr_turbodb_redis_test_unknown_commit_peer_t peer;
        tr_turbodb_redis_test_thread_t peer_thread;
        redis_io_runtime retry_runtime = {0};
        tr_turbodb_redis_test_socket_t retry_sockets[2];
        redis_cflow_connection retry_connection = {0};
        tr_turbodb_redis_state_machine_config_t retry_config;
        tr_turbodb_redis_state_machine_t *retry_adapter = NULL;
        tr_turbodb_redis_test_unknown_commit_peer_t retry_peer;
        tr_turbodb_redis_test_thread_t retry_peer_thread;

        sockets[0] = TR_TURBODB_REDIS_TEST_INVALID_SOCKET;
        sockets[1] = TR_TURBODB_REDIS_TEST_INVALID_SOCKET;
        check_equal(redis_io_runtime_init(&runtime, &runtime_config), SALTS_OK);
        check_equal(tr_turbodb_redis_test_socket_pair(sockets), SALTS_OK);
        connection_config = (redis_cflow_connection_config){
            &runtime, (uintptr_t)sockets[0], TR_TURBODB_REDIS_TEST_MAX_COMMAND_BYTES,
            64U, 4096U, 64U, TR_TURBODB_REDIS_TEST_WAIT_TIMEOUT_NS, 1};
        check_equal(redis_cflow_connection_init_attached(&connection,
                                                         &connection_config),
                    SALTS_OK);
        config = tr_turbodb_redis_test_config(&connection, &runtime);
        check_equal(tr_turbodb_redis_state_machine_open(&config, &adapter),
                    SALTS_OK);
        peer = (tr_turbodb_redis_test_unknown_commit_peer_t){
            sockets[1], state_at_42_reply, sizeof(state_at_42_reply) - 1U, 1,
            SALTS_EIO};
        sockets[1] = TR_TURBODB_REDIS_TEST_INVALID_SOCKET;
        check_equal(tr_turbodb_redis_test_unknown_commit_peer_start(
                        &peer, &peer_thread),
                    SALTS_OK);
        check_equal(tr_turbodb_redis_state_machine_compact_snapshot(
                        adapter, 43U, 123456U, &compact_result),
                    SALTS_EIO);
        check_equal(tr_turbodb_redis_test_unknown_commit_peer_join(peer_thread),
                    SALTS_OK);
        check_equal(peer.result, SALTS_OK);
        check_equal(tr_turbodb_redis_state_machine_close(adapter), SALTS_OK);
        check_equal(redis_cflow_connection_destroy(&connection), SALTS_OK);
        check_equal(redis_io_runtime_close(&runtime), SALTS_OK);
        check_equal(redis_io_runtime_destroy(&runtime), SALTS_OK);

        retry_sockets[0] = TR_TURBODB_REDIS_TEST_INVALID_SOCKET;
        retry_sockets[1] = TR_TURBODB_REDIS_TEST_INVALID_SOCKET;
        check_equal(redis_io_runtime_init(&retry_runtime, &runtime_config),
                    SALTS_OK);
        check_equal(tr_turbodb_redis_test_socket_pair(retry_sockets), SALTS_OK);
        connection_config = (redis_cflow_connection_config){
            &retry_runtime, (uintptr_t)retry_sockets[0],
            TR_TURBODB_REDIS_TEST_MAX_COMMAND_BYTES, 64U, 4096U, 64U,
            TR_TURBODB_REDIS_TEST_WAIT_TIMEOUT_NS, 1};
        check_equal(redis_cflow_connection_init_attached(&retry_connection,
                                                         &connection_config),
                    SALTS_OK);
        retry_config = tr_turbodb_redis_test_config(&retry_connection,
                                                     &retry_runtime);
        check_equal(tr_turbodb_redis_state_machine_open(&retry_config,
                                                        &retry_adapter),
                    SALTS_OK);
        retry_peer = (tr_turbodb_redis_test_unknown_commit_peer_t){
            retry_sockets[1], state_at_43_reply, sizeof(state_at_43_reply) - 1U,
            0, SALTS_EIO};
        retry_sockets[1] = TR_TURBODB_REDIS_TEST_INVALID_SOCKET;
        check_equal(tr_turbodb_redis_test_unknown_commit_peer_start(
                        &retry_peer, &retry_peer_thread),
                    SALTS_OK);
        check_equal(tr_turbodb_redis_state_machine_compact_snapshot(
                        retry_adapter, 43U, 123456U, &compact_result),
                    SALTS_OK);
        check_equal(compact_result, TR_TURBODB_REDIS_COMPACT_REPLAYED);
        tr_turbodb_redis_test_close_socket(retry_peer.socket_value);
        retry_peer.socket_value = TR_TURBODB_REDIS_TEST_INVALID_SOCKET;
        check_equal(tr_turbodb_redis_test_unknown_commit_peer_join(
                        retry_peer_thread),
                    SALTS_OK);
        check_equal(retry_peer.result, SALTS_OK);
        check_equal(tr_turbodb_redis_state_machine_close(retry_adapter),
                    SALTS_OK);
        check_equal(redis_cflow_connection_destroy(&retry_connection),
                    SALTS_OK);
        check_equal(redis_io_runtime_close(&retry_runtime), SALTS_OK);
        check_equal(redis_io_runtime_destroy(&retry_runtime), SALTS_OK);
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
