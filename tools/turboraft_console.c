#include <turboraft/raft_control_plane.h>
#include <turboraft/text_syntax.h>

#include <chttp/chttp.h>
#include <salts_error.h>

#include <stdio.h>
#include <string.h>

enum { TR_CONSOLE_URI_CAPACITY = 512, TR_CONSOLE_TIMEOUT_MS = 5000 };

typedef struct tr_console_endpoint {
    char connection_uri[TR_CONSOLE_URI_CAPACITY];
    char authority[TR_CONSOLE_URI_CAPACITY];
    char target[TR_CONSOLE_URI_CAPACITY];
} tr_console_endpoint_t;

static char tr_console_input[TR_TEXT_MAX_INPUT_BYTES + 1U];

static native_io_backend_kind tr_console_backend(void)
{
#if defined(_WIN32)
    return NATIVE_IO_BACKEND_IOCP;
#elif defined(__linux__)
    return NATIVE_IO_BACKEND_EPOLL;
#else
    return NATIVE_IO_BACKEND_KQUEUE;
#endif
}

static int tr_console_copy(char *output,
                           size_t capacity,
                           const char *input,
                           size_t size)
{
    if (output == NULL || input == NULL || size == 0U || size >= capacity) {
        return SALTS_ERANGE;
    }
    memcpy(output, input, size);
    output[size] = '\0';
    return SALTS_OK;
}

static int tr_console_parse_endpoint(const char *url,
                                     tr_console_endpoint_t *out_endpoint)
{
    const char *authority;
    const char *target;
    const char *transport;
    size_t authority_size;
    int written;

    if (url == NULL || out_endpoint == NULL) {
        return SALTS_EINVAL;
    }
    if (strncmp(url, "http://", 7U) == 0) {
        authority = url + 7U;
        transport = "tcp";
    } else if (strncmp(url, "https://", 8U) == 0) {
        authority = url + 8U;
        transport = "tls";
    } else {
        return SALTS_EINVAL;
    }
    target = strchr(authority, '/');
    authority_size = target == NULL ? strlen(authority)
                                    : (size_t)(target - authority);
    memset(out_endpoint, 0, sizeof(*out_endpoint));
    if (tr_console_copy(out_endpoint->authority,
                        sizeof(out_endpoint->authority), authority,
                        authority_size) != SALTS_OK) {
        return SALTS_ERANGE;
    }
    written = snprintf(out_endpoint->connection_uri,
                       sizeof(out_endpoint->connection_uri), "%s://%s",
                       transport, out_endpoint->authority);
    if (written <= 0 || (size_t)written >=
                            sizeof(out_endpoint->connection_uri)) {
        return SALTS_ERANGE;
    }
    if (target == NULL) {
        memcpy(out_endpoint->target, TR_RAFT_CONTROL_STATUS_PATH,
               sizeof(TR_RAFT_CONTROL_STATUS_PATH));
    } else if (tr_console_copy(out_endpoint->target,
                               sizeof(out_endpoint->target), target,
                               strlen(target)) != SALTS_OK) {
        return SALTS_ERANGE;
    }
    return SALTS_OK;
}

static chttp_client_config tr_console_client_config(void)
{
    chttp_client_config config;

    memset(&config, 0, sizeof(config));
    config.network.backend = tr_console_backend();
    config.network.connection_capacity = 2U;
    config.network.command_capacity = 16U;
    config.network.request_capacity = 8U;
    config.network.completion_batch_capacity = 8U;
    config.network.event_capacity = 16U;
    config.network.max_send_bytes = 64U * 1024U;
    config.network.receive_buffer_bytes = 16U * 1024U;
    config.network.connect_timeout_ms = TR_CONSOLE_TIMEOUT_MS;
    config.network.read_timeout_ms = TR_CONSOLE_TIMEOUT_MS;
    config.network.write_timeout_ms = TR_CONSOLE_TIMEOUT_MS;
    config.network.tls_io_buffer_bytes = CNET_TLS_MIN_IO_BUFFER_BYTES;
    config.network.tls_handshake_timeout_ms = TR_CONSOLE_TIMEOUT_MS;
    config.request_capacity = 2U;
    config.max_start_line_bytes = 1024U;
    config.max_header_count = 32U;
    config.max_header_bytes = 8192U;
    config.max_request_body_bytes = 8192U;
    config.max_response_body_bytes = 64U * 1024U;
    config.max_informational_responses = 2U;
    return config;
}

static int tr_console_status(chttp_client *client,
                             const tr_console_endpoint_t *endpoint)
{
    chttp_options options;
    chttp_response response;
    chttp_error error;
    int result;

    memset(&options, 0, sizeof(options));
    memset(&response, 0, sizeof(response));
    memset(&error, 0, sizeof(error));
    options.connection_uri = endpoint->connection_uri;
    options.authority = endpoint->authority;
    options.target = endpoint->target;
    options.timeout_ms = TR_CONSOLE_TIMEOUT_MS;
    result = chttp_get(client, &options, &response, &error);
    if (result != SALTS_OK) {
        fprintf(stderr, "HTTP status request failed at %s: %d\n",
                error.stage == NULL ? "unknown" : error.stage, result);
        return 1;
    }
    if (response.status_code != 200U) {
        fprintf(stderr, "HTTP status request returned %u\n",
                response.status_code);
        chttp_response_destroy(&response);
        return 1;
    }
    if (response.body_size != 0U) {
        (void)fwrite(response.body, 1U, response.body_size, stdout);
    }
    fputc('\n', stdout);
    chttp_response_destroy(&response);
    return 0;
}

static int tr_console_execute_query(chttp_client *client,
                                    const tr_console_endpoint_t *endpoint,
                                    const char *input,
                                    size_t input_length)
{
    tr_text_query_plan_t plan;
    tr_text_diagnostic_t diagnostic;
    size_t index;
    int result;

    result = tr_text_query_parse(input, input_length, NULL, &plan,
                                 &diagnostic);
    if (result != SALTS_OK) {
        fprintf(stderr, "query parse failed at %zu:%zu: %s\n",
                diagnostic.line, diagnostic.column,
                diagnostic.message == NULL ? "invalid input"
                                           : diagnostic.message);
        return 1;
    }
    for (index = 0U; index < plan.command_count; ++index) {
        if (plan.commands[index].kind != TR_TEXT_QUERY_SHOW_STATUS) {
            fputs("this control plane currently exposes SHOW STATUS only\n",
                  stderr);
            return 1;
        }
        if (tr_console_status(client, endpoint) != 0) {
            return 1;
        }
    }
    return 0;
}

static void tr_console_print_usage(const char *program)
{
    fprintf(stderr,
            "Usage: %s --endpoint <http[s]://host:port/raft/status> "
            "[--query <query DSL>]\n",
            program);
}

int main(int argc, char **argv)
{
    tr_console_endpoint_t endpoint;
    chttp_client_config client_config;
    chttp_client client;
    const char *query = NULL;
    int exit_code = 0;

    if ((argc != 3 && argc != 5) || strcmp(argv[1], "--endpoint") != 0 ||
        argv[2][0] == '\0') {
        tr_console_print_usage(argv[0]);
        return 2;
    }
    if (argc == 5) {
        if (strcmp(argv[3], "--query") != 0 || argv[4][0] == '\0') {
            tr_console_print_usage(argv[0]);
            return 2;
        }
        query = argv[4];
    }
    if (tr_console_parse_endpoint(argv[2], &endpoint) != SALTS_OK) {
        tr_console_print_usage(argv[0]);
        return 2;
    }
    memset(&client, 0, sizeof(client));
    client_config = tr_console_client_config();
    if (chttp_client_init(&client, &client_config) != SALTS_OK) {
        fputs("cannot initialize CHTTP client\n", stderr);
        return 1;
    }
    if (query != NULL) {
        exit_code = tr_console_execute_query(&client, &endpoint, query,
                                             strlen(query));
    } else {
        while (fputs("turboraft> ", stdout),
               fgets(tr_console_input, sizeof(tr_console_input), stdin) !=
                   NULL) {
            if (strcmp(tr_console_input, "exit\n") == 0 ||
                strcmp(tr_console_input, "quit\n") == 0) {
                break;
            }
            if (tr_console_execute_query(&client, &endpoint,
                                         tr_console_input,
                                         strlen(tr_console_input)) != 0) {
                exit_code = 1;
            }
        }
    }
    if (chttp_client_destroy(&client, TR_CONSOLE_TIMEOUT_MS) != SALTS_OK) {
        exit_code = 1;
    }
    return exit_code;
}
