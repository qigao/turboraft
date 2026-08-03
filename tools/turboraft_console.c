#include <rpc_client.h>

#include <turboraft/text_syntax.h>

#include <stdio.h>
#include <string.h>

static char tr_console_input[TR_TEXT_MAX_INPUT_BYTES + 1u];

static int tr_console_call(rpc_client_t *client,
                           const char *method,
                           const char *params)
{
    rpc_call_result_t result = {0};
    int call_result;

    call_result = rpc_client_call(client, method, params, &result);
    if (call_result != 0) {
        fprintf(stderr, "RPC transport failed for %s\n", method);
        return 1;
    }
    if (!result.success) {
        fprintf(stderr, "RPC %s failed: %d %s\n", method,
                result.error_code,
                result.error_message == NULL ? "" : result.error_message);
        rpc_result_free(&result);
        return 1;
    }
    puts(result.result == NULL ? "null" : result.result);
    rpc_result_free(&result);
    return 0;
}

static int tr_console_execute_query(rpc_client_t *client,
                                    const char *input,
                                    size_t input_length)
{
    tr_text_query_plan_t plan;
    tr_text_diagnostic_t diagnostic;
    size_t index;
    int result;

    result = tr_text_query_parse(input, input_length, NULL, &plan,
                                 &diagnostic);
    if (result != TURBO_OK) {
        fprintf(stderr, "query parse failed at %zu:%zu: %s\n",
                diagnostic.line, diagnostic.column,
                diagnostic.message == NULL ? "invalid input" :
                                             diagnostic.message);
        return 1;
    }
    for (index = 0u; index < plan.command_count; ++index) {
        const tr_text_query_command_t *command = &plan.commands[index];

        switch (command->kind) {
            case TR_TEXT_QUERY_SHOW_STATUS:
                result = tr_console_call(client, "raft.status", "{}");
                break;
            case TR_TEXT_QUERY_SHOW_MEMBERS:
                if (command->role == TR_TEXT_QUERY_ROLE_ANY) {
                    result = tr_console_call(client, "raft.members", "{}");
                } else {
                    result = tr_console_call(
                        client, "raft.members",
                        command->role == TR_TEXT_QUERY_ROLE_VOTER
                            ? "{\"role\":\"voter\"}"
                            : "{\"role\":\"learner\"}");
                }
                break;
            case TR_TEXT_QUERY_SHOW_PROGRESS: {
                char params[64];
                int written = snprintf(params, sizeof(params),
                                       "{\"node_id\":%llu}",
                                       (unsigned long long)command->node_id);

                if (written <= 0 || (size_t)written >= sizeof(params)) {
                    return 1;
                }
                result = tr_console_call(client, "raft.progress", params);
                break;
            }
            default:
                return 1;
        }
        if (result != 0) {
            return result;
        }
    }
    return 0;
}

static void tr_console_print_usage(const char *program)
{
    fprintf(stderr,
            "Usage: %s --endpoint <http[s]://host:port/raft/rpc> "
            "[--query <query DSL>]\n",
            program);
}

int main(int argc, char **argv)
{
    const char *endpoint;
    const char *query = NULL;
    rpc_client_t *client;
    int exit_code = 0;

    if (argc != 3 && argc != 5) {
        tr_console_print_usage(argv[0]);
        return 2;
    }
    if (strcmp(argv[1], "--endpoint") != 0 || argv[2][0] == '\0') {
        tr_console_print_usage(argv[0]);
        return 2;
    }
    endpoint = argv[2];
    if (argc == 5) {
        if (strcmp(argv[3], "--query") != 0 || argv[4][0] == '\0') {
            tr_console_print_usage(argv[0]);
            return 2;
        }
        query = argv[4];
    }

    client = rpc_client_create_simple(endpoint);
    if (client == NULL || rpc_client_connect(client) != 0) {
        fprintf(stderr, "cannot connect to %s\n", endpoint);
        rpc_client_destroy(client);
        return 1;
    }
    if (query != NULL) {
        exit_code = tr_console_execute_query(client, query, strlen(query));
    } else {
        while (fputs("turboraft> ", stdout),
               fgets(tr_console_input, sizeof(tr_console_input), stdin) !=
                   NULL) {
            if (strcmp(tr_console_input, "exit\n") == 0 ||
                strcmp(tr_console_input, "quit\n") == 0) {
                break;
            }
            if (tr_console_execute_query(client, tr_console_input,
                                         strlen(tr_console_input)) != 0) {
                exit_code = 1;
            }
        }
    }
    rpc_client_disconnect(client);
    rpc_client_destroy(client);
    return exit_code;
}
