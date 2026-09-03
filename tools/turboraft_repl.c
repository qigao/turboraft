#include <turboraft/text_replay_core_driver.h>
#include <turboraft/text_syntax.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#include <io.h>
#define repl_isatty _isatty
#else
#include <unistd.h>
#define repl_isatty isatty
#endif

#define REPL_MAX_ARGS 32u
#define REPL_MAX_ARG_BYTES 256u
#define REPL_MAX_LINE_BYTES (REPL_MAX_ARGS * REPL_MAX_ARG_BYTES)

typedef struct repl_state {
    tr_replay_driver_t *driver;
    size_t node_count;
} repl_state_t;

/* Parsed command arguments, borrowed from the current token buffer. */
static char *v_arg1;
static char *v_arg2;
static char *v_arg3;
static char *v_arg4;
static char *v_arg5;
/* Optional argument outputs for expect/status. */
static int64_t v_node;
static char *v_erole;
static char *v_commit;

static const char repl_help[] =
    "commands (interactive REPL):\n"
    "  cluster <n>                 (re)create an n-node cluster\n"
    "  node <id>                   reference/validate a node\n"
    "  tick <ticks>                advance all nodes and pump the network\n"
    "  send <from> <to>            inject a heartbeat request from->to\n"
    "  drop <message>              drop the next <message> (e.g. append_request)\n"
    "  delay <message> <ticks>     hold the next <message> for <ticks>\n"
    "  duplicate <message>         duplicate the next <message>\n"
    "  partition <a> <b>           cut the directed link a->b\n"
    "  heal <a> <b>                restore the directed link a->b\n"
    "  submit <request> <node> <client> <sequence> <payloadhex>\n"
    "  poll <request> <accepted|committed|applied> <timeout_ticks>\n"
    "  expect --node <id> --role <follower|pre-candidate|candidate|leader>\n"
    "  expect --node <id> --commit <index>\n"
    "  status [--node <id>]        print node state (all nodes by default)\n"
    "  run <file>                  execute a replay DSL script file\n"
    "  help                        this help\n"
    "  exit | quit                 leave the REPL\n"
    "\n"
    "Batch modes: turboraft_repl --nodes N --script FILE, or pipe lines to stdin.\n"
    "Arguments are validated before dispatch. Use `run <file>` for robust "
    "batch execution.\n";

static int repl_stoi64(const char *text, int64_t *out)
{
    char *end = NULL;
    long long value;

    if (text == NULL || text[0] == '\0') {
        return -1;
    }
    value = strtoll(text, &end, 10);
    if (end == text || *end != '\0') {
        return -1;
    }
    *out = (int64_t)value;
    return 0;
}

static int repl_tokenize(const char *line,
                         char tokens[][REPL_MAX_ARG_BYTES],
                         int *out_count)
{
    int count = 0;
    size_t index = 0u;
    size_t length = strlen(line);

    while (index < length) {
        char *out;

        while (index < length &&
               (line[index] == ' ' || line[index] == '\t' ||
                line[index] == '\r' || line[index] == '\n')) {
            ++index;
        }
        if (index >= length) {
            break;
        }
        if (count >= (int)REPL_MAX_ARGS) {
            return -1;
        }
        out = tokens[count];
        if (line[index] == '"') {
            size_t written = 0u;
            ++index;
            while (index < length && line[index] != '"' &&
                   written + 1u < REPL_MAX_ARG_BYTES) {
                out[written++] = line[index++];
            }
            if (index < length && line[index] == '"') {
                ++index;
            }
            out[written] = '\0';
        } else {
            size_t written = 0u;
            while (index < length &&
                   line[index] != ' ' && line[index] != '\t' &&
                   line[index] != '\r' && line[index] != '\n' &&
                   written + 1u < REPL_MAX_ARG_BYTES) {
                out[written++] = line[index++];
            }
            out[written] = '\0';
        }
        ++count;
    }
    *out_count = count;
    return 0;
}

static const char *const repl_known_commands[] = {
    "cluster", "node",   "tick",  "send",   "drop",   "delay",
    "duplicate", "partition", "heal", "submit", "poll", "expect",
    "status", "run", "help", "exit", "quit"};

static int repl_known_command(const char *name)
{
    size_t index;

    for (index = 0u;
         index < sizeof(repl_known_commands) / sizeof(repl_known_commands[0]);
         ++index) {
        if (strcmp(repl_known_commands[index], name) == 0) {
            return 1;
        }
    }
    return 0;
}

static const char *const repl_options_expect[] = {"--node", "--role",
                                                  "--commit"};
static const char *const repl_options_status[] = {"--node"};

static int repl_option_allowed(const char *command, const char *token)
{
    const char *const *options = NULL;
    size_t option_count = 0u;
    size_t index;

    if (strcmp(command, "expect") == 0) {
        options = repl_options_expect;
        option_count =
            sizeof(repl_options_expect) / sizeof(repl_options_expect[0]);
    } else if (strcmp(command, "status") == 0) {
        options = repl_options_status;
        option_count =
            sizeof(repl_options_status) / sizeof(repl_options_status[0]);
    }
    if (options == NULL) {
        return 0;
    }
    for (index = 0u; index < option_count; ++index) {
        size_t option_length = strlen(options[index]);

        if (strcmp(token, options[index]) == 0 ||
            (strncmp(token, options[index], option_length) == 0 &&
             token[option_length] == '=')) {
            return 1;
        }
    }
    return 0;
}

static int repl_positional_count(const char *command)
{
    if (strcmp(command, "cluster") == 0 || strcmp(command, "node") == 0 ||
        strcmp(command, "tick") == 0 || strcmp(command, "drop") == 0 ||
        strcmp(command, "duplicate") == 0 || strcmp(command, "run") == 0) {
        return 1;
    }
    if (strcmp(command, "send") == 0 || strcmp(command, "delay") == 0 ||
        strcmp(command, "partition") == 0 || strcmp(command, "heal") == 0) {
        return 2;
    }
    if (strcmp(command, "submit") == 0) {
        return 5;
    }
    if (strcmp(command, "poll") == 0) {
        return 3;
    }
    return 0;
}

/* Returns 1 when all integer positionals parse, 0 otherwise. */
static int repl_positionals_are_integers(const char *command)
{
    if (strcmp(command, "cluster") == 0 || strcmp(command, "node") == 0 ||
        strcmp(command, "tick") == 0 || strcmp(command, "send") == 0 ||
        strcmp(command, "delay") == 0 || strcmp(command, "partition") == 0 ||
        strcmp(command, "heal") == 0 || strcmp(command, "submit") == 0) {
        return 1;
    }
    return strcmp(command, "poll") == 0;
}

static int repl_pre_validate(const char *command,
                             char tokens[][REPL_MAX_ARG_BYTES],
                             int token_count)
{
    int positional_index = 0;
    int expect_value = 0;
    int index;

    for (index = 1; index < token_count; ++index) {
        const char *token = tokens[index];

        if (strcmp(token, "--help") == 0) {
            return 2; /* caller prints help and continues */
        }
        if (expect_value) {
            expect_value = 0; /* value of the previous option */
            continue;
        }
        if (token[0] == '-') {
            if (!repl_option_allowed(command, token)) {
                fprintf(stderr, "unknown option '%s' for '%s'\n", token,
                        command);
                return -1;
            }
            if (strchr(token, '=') == NULL) {
                expect_value = 1; /* option takes a separate value */
            }
        } else {
            ++positional_index;
            if (repl_positionals_are_integers(command) &&
                !(strcmp(command, "submit") == 0 && positional_index == 5) &&
                !(strcmp(command, "poll") == 0 && positional_index == 2)) {
                int64_t ignored;

                if (repl_stoi64(token, &ignored) != 0) {
                    fprintf(stderr, "expected an integer, got '%s'\n",
                            token);
                    return -1;
                }
            }
        }
    }
    if (expect_value) {
        fprintf(stderr, "option requires a value\n");
        return -1;
    }
    if (positional_index != repl_positional_count(command)) {
        fprintf(stderr, "usage: %s takes %d positional argument(s)\n",
                command, repl_positional_count(command));
        return -1;
    }
    return 0;
}

static void repl_print_error(int result)
{
    fprintf(stderr, "error: result=%d (%s)\n", result,
            result == SALTS_ETIMEDOUT ? "timed out"
            : result == SALTS_EPROTO  ? "protocol/expectation failure"
            : result == SALTS_ENOENT  ? "not found"
            : result == SALTS_EALREADY ? "already exists"
            : result == SALTS_ENOSPC  ? "capacity exceeded"
            : result == SALTS_ERANGE  ? "out of range"
            : result == SALTS_EINVAL  ? "invalid argument"
                                      : "error");
}

static tr_text_replay_action_t repl_action(void)
{
    tr_text_replay_action_t action;

    memset(&action, 0, sizeof(action));
    return action;
}

static int repl_dispatch(repl_state_t *state, const char *command)
{
    tr_replay_driver_t *driver = state->driver;
    tr_text_replay_action_t action = repl_action();
    int result;

    if (driver == NULL) {
        fprintf(stderr, "no cluster; use `cluster <n>` first\n");
        return 1;
    }
    if (strcmp(command, "tick") == 0) {
        int64_t ticks;

        if (repl_stoi64(v_arg1, &ticks) != 0) {
            return 1;
        }
        action.kind = TR_TEXT_REPLAY_TICK;
        action.value = (uint64_t)ticks;
    } else if (strcmp(command, "send") == 0 ||
               strcmp(command, "partition") == 0 ||
               strcmp(command, "heal") == 0) {
        int64_t a;
        int64_t b;

        if (repl_stoi64(v_arg1, &a) != 0 || repl_stoi64(v_arg2, &b) != 0) {
            return 1;
        }
        action.node_id = (uint64_t)a;
        action.peer_id = (uint64_t)b;
        if (strcmp(command, "send") == 0) {
            action.kind = TR_TEXT_REPLAY_SEND;
        } else if (strcmp(command, "partition") == 0) {
            action.kind = TR_TEXT_REPLAY_PARTITION;
        } else {
            action.kind = TR_TEXT_REPLAY_HEAL;
        }
    } else if (strcmp(command, "drop") == 0 ||
               strcmp(command, "duplicate") == 0) {
        action.kind = strcmp(command, "drop") == 0
                          ? TR_TEXT_REPLAY_DROP_NEXT
                          : TR_TEXT_REPLAY_DUPLICATE_NEXT;
        action.name.data = v_arg1;
        action.name.len = strlen(v_arg1);
    } else if (strcmp(command, "delay") == 0) {
        int64_t ticks;

        if (repl_stoi64(v_arg2, &ticks) != 0) {
            return 1;
        }
        action.kind = TR_TEXT_REPLAY_DELAY_NEXT;
        action.name.data = v_arg1;
        action.name.len = strlen(v_arg1);
        action.value = (uint64_t)ticks;
    } else if (strcmp(command, "submit") == 0) {
        int64_t request;
        int64_t node;
        int64_t client;
        int64_t sequence;

        if (repl_stoi64(v_arg1, &request) != 0 ||
            repl_stoi64(v_arg2, &node) != 0 ||
            repl_stoi64(v_arg3, &client) != 0 ||
            repl_stoi64(v_arg4, &sequence) != 0) {
            return 1;
        }
        action.kind = TR_TEXT_REPLAY_SUBMIT;
        action.request_id = (uint64_t)request;
        action.node_id = (uint64_t)node;
        action.client_id = (uint64_t)client;
        action.sequence = (uint64_t)sequence;
        action.payload_hex.data = v_arg5;
        action.payload_hex.len = strlen(v_arg5);
    } else if (strcmp(command, "poll") == 0) {
        int64_t request;
        int64_t timeout;
        tr_text_replay_poll_target_t target;

        if (repl_stoi64(v_arg1, &request) != 0 ||
            repl_stoi64(v_arg3, &timeout) != 0) {
            return 1;
        }
        if (strcmp(v_arg2, "accepted") == 0) {
            target = TR_TEXT_REPLAY_POLL_ACCEPTED;
        } else if (strcmp(v_arg2, "committed") == 0) {
            target = TR_TEXT_REPLAY_POLL_COMMITTED;
        } else if (strcmp(v_arg2, "applied") == 0) {
            target = TR_TEXT_REPLAY_POLL_APPLIED;
        } else {
            fprintf(stderr, "unknown poll target '%s'\n", v_arg2);
            return 1;
        }
        action.kind = TR_TEXT_REPLAY_POLL;
        action.request_id = (uint64_t)request;
        action.poll_target = target;
        action.timeout_ticks = (uint64_t)timeout;
    } else {
        fprintf(stderr, "unsupported command '%s'\n", command);
        return 1;
    }
    result = tr_replay_driver_step(driver, &action);
    if (result != SALTS_OK) {
        repl_print_error(result);
        return 1;
    }
    return 0;
}

static int repl_print_status(repl_state_t *state, int64_t node)
{
    tr_replay_driver_t *driver = state->driver;
    tr_raft_status_t status;
    int result;

    if (driver == NULL) {
        fprintf(stderr, "no cluster\n");
        return 1;
    }
    if (node == 0) {
        size_t index;

        for (index = 0u; index < state->node_count; ++index) {
            if (repl_print_status(state, (int64_t)(index + 1u)) != 0) {
                return 1;
            }
        }
        return 0;
    }
    result = tr_replay_driver_status(driver, (tr_raft_node_id_t)node,
                                     &status);
    if (result != SALTS_OK) {
        fprintf(stderr, "node %lld: %s\n", (long long)node,
                result == SALTS_ENOENT ? "not found" : "error");
        return 1;
    }
    printf("node %llu role=%d term=%llu leader=%llu commit=%llu "
           "applied=%llu last_log=%llu\n",
           (unsigned long long)status.self_id, (int)status.role,
           (unsigned long long)status.term,
           (unsigned long long)status.leader_id,
           (unsigned long long)status.commit_index,
           (unsigned long long)status.applied_index,
           (unsigned long long)status.last_log_index);
    return 0;
}

static int repl_run_script(repl_state_t *state, const char *path)
{
    static char script_buffer[TR_TEXT_MAX_INPUT_BYTES + 1u];
    FILE *file = fopen(path, "rb");
    tr_text_replay_plan_t plan;
    tr_text_diagnostic_t diagnostic;
    size_t length;
    int result;

    if (file == NULL) {
        fprintf(stderr, "cannot open %s\n", path);
        return 1;
    }
    length = fread(script_buffer, 1u, sizeof(script_buffer) - 1u, file);
    fclose(file);
    if (length == sizeof(script_buffer) - 1u) {
        fprintf(stderr, "script exceeds %u bytes\n", TR_TEXT_MAX_INPUT_BYTES);
        return 1;
    }
    script_buffer[length] = '\0';
    result = tr_text_replay_parse(script_buffer, length, NULL, &plan,
                                  &diagnostic);
    if (result != SALTS_OK) {
        fprintf(stderr, "script parse failed at %zu:%zu: %s\n",
                diagnostic.line, diagnostic.column,
                diagnostic.message == NULL ? "invalid input"
                                           : diagnostic.message);
        return 1;
    }
    if (state->driver == NULL) {
        fprintf(stderr, "no cluster; add `cluster <n>` to the script\n");
        return 1;
    }
    result = tr_replay_driver_run(state->driver, &plan);
    if (result != SALTS_OK) {
        repl_print_error(result);
        return 1;
    }
    return 0;
}

static int repl_build_driver(repl_state_t *state, size_t count)
{
    static tr_raft_node_id_t voters[TR_REPLAY_DRIVER_MAX_NODES];
    static tr_raft_core_config_t nodes[TR_REPLAY_DRIVER_MAX_NODES];
    tr_replay_driver_config_t config;
    tr_replay_driver_t *driver = NULL;
    size_t index;
    int result;

    if (count == 0u || count > TR_REPLAY_DRIVER_MAX_NODES) {
        fprintf(stderr, "node count must be in [1, %u]\n",
                TR_REPLAY_DRIVER_MAX_NODES);
        return 1;
    }
    for (index = 0u; index < count; ++index) {
        voters[index] = (tr_raft_node_id_t)(index + 1u);
        memset(&nodes[index], 0, sizeof(nodes[index]));
        nodes[index].self_id = voters[index];
        nodes[index].voters = voters;
        nodes[index].voter_count = count;
        nodes[index].heartbeat_ticks = 1u;
        nodes[index].election_min_ticks = 3u;
        nodes[index].election_max_ticks = 8u;
        nodes[index].initial_election_timeout_ticks =
            4u + (uint32_t)(index % 3u);
        nodes[index].max_log_entries = 1024u;
    }
    memset(&config, 0, sizeof(config));
    config.nodes = nodes;
    config.node_count = count;
    result = tr_replay_driver_create(&config, &driver);
    if (result != SALTS_OK) {
        fprintf(stderr, "driver create failed: %d\n", result);
        return 1;
    }
    if (state->driver != NULL) {
        tr_replay_driver_destroy(state->driver);
    }
    state->driver = driver;
    state->node_count = count;
    return 0;
}

static void repl_zero_outputs(void)
{
    v_arg1 = NULL;
    v_arg2 = NULL;
    v_arg3 = NULL;
    v_arg4 = NULL;
    v_arg5 = NULL;
    v_node = 0;
    v_erole = NULL;
    v_commit = NULL;
}

static int repl_handle_line(repl_state_t *state, char *line)
{
    char tokens[REPL_MAX_ARGS][REPL_MAX_ARG_BYTES];
    const char *command;
    int token_count = 0;
    int index;
    int validate;

    if (repl_tokenize(line, tokens, &token_count) != 0 || token_count == 0) {
        return 0;
    }
    command = tokens[0];
    if (strcmp(command, "exit") == 0 || strcmp(command, "quit") == 0) {
        return -1;
    }
    if (strcmp(command, "help") == 0) {
        fputs(repl_help, stdout);
        return 0;
    }
    if (!repl_known_command(command)) {
        fprintf(stderr, "unknown command '%s'; type `help`\n", command);
        return 0;
    }
    validate = repl_pre_validate(command, tokens, token_count);
    if (validate == 2) {
        fputs(repl_help, stdout);
        return 0;
    }
    if (validate != 0) {
        return 0;
    }
    repl_zero_outputs();
    if (strcmp(command, "expect") == 0 || strcmp(command, "status") == 0) {
        for (index = 1; index + 1 < token_count; index += 2) {
            if (strcmp(tokens[index], "--node") == 0 ||
                strcmp(tokens[index], "-n") == 0) {
                if (repl_stoi64(tokens[index + 1], &v_node) != 0) {
                    return 0;
                }
            } else if (strcmp(tokens[index], "--role") == 0 ||
                       strcmp(tokens[index], "-r") == 0) {
                v_erole = tokens[index + 1];
            } else if (strcmp(tokens[index], "--commit") == 0 ||
                       strcmp(tokens[index], "-c") == 0) {
                v_commit = tokens[index + 1];
            }
        }
    } else {
        v_arg1 = token_count > 1 ? tokens[1] : NULL;
        v_arg2 = token_count > 2 ? tokens[2] : NULL;
        v_arg3 = token_count > 3 ? tokens[3] : NULL;
        v_arg4 = token_count > 4 ? tokens[4] : NULL;
        v_arg5 = token_count > 5 ? tokens[5] : NULL;
    }
    if (strcmp(command, "cluster") == 0) {
        int64_t count;

        if (repl_stoi64(v_arg1, &count) != 0) {
            return 0;
        }
        return repl_build_driver(state, (size_t)count) == 0 ? 0 : 1;
    }
    if (strcmp(command, "node") == 0) {
        int64_t id;

        if (repl_stoi64(v_arg1, &id) != 0) {
            return 0;
        }
        {
            tr_text_replay_action_t action = repl_action();

            action.kind = TR_TEXT_REPLAY_NODE;
            action.node_id = (uint64_t)id;
            return tr_replay_driver_step(state->driver, &action) == SALTS_OK
                       ? 0
                       : 1;
        }
    }
    if (strcmp(command, "expect") == 0) {
        tr_text_replay_action_t action = repl_action();

        if (v_node <= 0) {
            fprintf(stderr, "expect requires --node <id>\n");
            return 0;
        }
        action.node_id = (uint64_t)v_node;
        if (v_erole != NULL && v_commit == NULL) {
            action.kind = TR_TEXT_REPLAY_EXPECT_ROLE;
            action.comparison = TR_TEXT_REPLAY_COMPARE_EQ;
            action.name.data = v_erole;
            action.name.len = strlen(v_erole);
        } else if (v_commit != NULL && v_erole == NULL) {
            int64_t commit;

            if (repl_stoi64(v_commit, &commit) != 0) {
                return 0;
            }
            action.kind = TR_TEXT_REPLAY_EXPECT_COMMIT_INDEX;
            action.comparison = TR_TEXT_REPLAY_COMPARE_GE;
            action.value = (uint64_t)commit;
        } else {
            fprintf(stderr, "expect requires exactly one of --role/--commit\n");
            return 0;
        }
        {
            int result = tr_replay_driver_step(state->driver, &action);

            if (result != SALTS_OK) {
                repl_print_error(result);
                return 1;
            }
        }
        return 0;
    }
    if (strcmp(command, "status") == 0) {
        return repl_print_status(state, v_node);
    }
    if (strcmp(command, "run") == 0) {
        return repl_run_script(state, v_arg1);
    }
    return repl_dispatch(state, command) == 0 ? 0 : 1;
}

int main(int argc, char **argv)
{
    int64_t nodes = 3;
    const char *script = NULL;
    repl_state_t state;
    int interactive;

    memset(&state, 0, sizeof(state));
    for (int index = 1; index < argc; ++index) {
        if (strcmp(argv[index], "--nodes") == 0 && index + 1 < argc) {
            if (repl_stoi64(argv[++index], &nodes) != 0) {
                fprintf(stderr, "invalid --nodes value\n");
                return 2;
            }
        } else if (strcmp(argv[index], "--script") == 0 && index + 1 < argc) {
            script = argv[++index];
        } else if (strcmp(argv[index], "--help") == 0 ||
                   strcmp(argv[index], "-h") == 0) {
            fputs(repl_help, stdout);
            return 0;
        } else {
            fprintf(stderr, "unknown or incomplete option: %s\n", argv[index]);
            return 2;
        }
    }
    if (nodes < 1 || nodes > TR_REPLAY_DRIVER_MAX_NODES) {
        fprintf(stderr, "--nodes must be in [1, %u]\n",
                TR_REPLAY_DRIVER_MAX_NODES);
        return 2;
    }
    if (repl_build_driver(&state, (size_t)nodes) != 0) {
        return 1;
    }
    if (script != NULL && repl_run_script(&state, script) != 0) {
        tr_replay_driver_destroy(state.driver);
        return 1;
    }
    interactive = repl_isatty(0) != 0;
    for (;;) {
        char line[REPL_MAX_LINE_BYTES];

        if (interactive) {
            fputs("repl> ", stdout);
            fflush(stdout);
        }
        if (fgets(line, sizeof(line), stdin) == NULL) {
            break;
        }
        if (repl_handle_line(&state, line) < 0) {
            break; /* exit/quit */
        }
    }
    tr_replay_driver_destroy(state.driver);
    return 0;
}
