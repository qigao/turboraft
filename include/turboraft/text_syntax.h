#ifndef TURBORAFT_TEXT_SYNTAX_H
#define TURBORAFT_TEXT_SYNTAX_H

#include <salts_error.h>
#include <salts_vstr.h>

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TR_TEXT_MAX_INPUT_BYTES (1024u * 1024u)
#define TR_TEXT_MAX_STATEMENTS 64u

typedef struct {
  size_t max_input_bytes;
  size_t max_statements;
} tr_text_parse_options_t;

typedef enum {
  TR_TEXT_DIAGNOSTIC_NONE = 0,
  TR_TEXT_DIAGNOSTIC_ARGUMENT,
  TR_TEXT_DIAGNOSTIC_LIMIT,
  TR_TEXT_DIAGNOSTIC_LEXICAL,
  TR_TEXT_DIAGNOSTIC_SYNTAX,
  TR_TEXT_DIAGNOSTIC_SEMANTIC
} tr_text_diagnostic_kind_t;

typedef struct {
  int code;
  tr_text_diagnostic_kind_t kind;
  size_t offset;
  size_t line;
  size_t column;
  const char *message;
} tr_text_diagnostic_t;

typedef enum {
  TR_TEXT_QUERY_SHOW_STATUS = 0,
  TR_TEXT_QUERY_SHOW_MEMBERS,
  TR_TEXT_QUERY_SHOW_PROGRESS
} tr_text_query_command_kind_t;

typedef enum {
  TR_TEXT_QUERY_ROLE_ANY = 0,
  TR_TEXT_QUERY_ROLE_VOTER,
  TR_TEXT_QUERY_ROLE_LEARNER
} tr_text_query_role_t;

typedef struct {
  tr_text_query_command_kind_t kind;
  tr_text_query_role_t role;
  uint64_t node_id;
} tr_text_query_command_t;

typedef struct {
  tr_text_query_command_t commands[TR_TEXT_MAX_STATEMENTS];
  size_t command_count;
} tr_text_query_plan_t;

typedef struct {
  uint64_t version;
  vstr kind;
  uint64_t from;
  uint64_t to;
  uint64_t term;
  vstr message;
  vstr payload_hex;
} tr_text_protocol_frame_t;

typedef struct {
  tr_text_protocol_frame_t frames[TR_TEXT_MAX_STATEMENTS];
  size_t frame_count;
} tr_text_protocol_debug_plan_t;

typedef enum {
  TR_TEXT_REPLAY_NODE = 0,
  TR_TEXT_REPLAY_TICK,
  TR_TEXT_REPLAY_SEND,
  TR_TEXT_REPLAY_DROP_NEXT,
  TR_TEXT_REPLAY_EXPECT_ROLE,
  TR_TEXT_REPLAY_EXPECT_COMMIT_INDEX,
  TR_TEXT_REPLAY_PARTITION,
  TR_TEXT_REPLAY_HEAL,
  TR_TEXT_REPLAY_DELAY_NEXT,
  TR_TEXT_REPLAY_DUPLICATE_NEXT,
  TR_TEXT_REPLAY_SUBMIT,
  TR_TEXT_REPLAY_POLL
} tr_text_replay_action_kind_t;

typedef enum {
  TR_TEXT_REPLAY_COMPARE_EQ = 0,
  TR_TEXT_REPLAY_COMPARE_GE
} tr_text_replay_comparison_t;

typedef enum {
  TR_TEXT_REPLAY_POLL_ACCEPTED = 0,
  TR_TEXT_REPLAY_POLL_COMMITTED,
  TR_TEXT_REPLAY_POLL_APPLIED
} tr_text_replay_poll_target_t;

typedef struct {
  tr_text_replay_action_kind_t kind;
  uint64_t node_id;
  uint64_t peer_id;
  uint64_t value;
  tr_text_replay_comparison_t comparison;
  vstr name;
  uint64_t client_id;
  uint64_t request_id;
  uint64_t sequence;
  uint64_t timeout_ticks;
  tr_text_replay_poll_target_t poll_target;
  vstr payload_hex;
} tr_text_replay_action_t;

typedef struct {
  tr_text_replay_action_t actions[TR_TEXT_MAX_STATEMENTS];
  size_t action_count;
} tr_text_replay_plan_t;

/*
 * Initializes parser options with the hard-limit defaults.
 * Zero-valued individual limits have the same defaulting behavior when passed
 * directly to a parse function.
 */
void tr_text_parse_options_init(tr_text_parse_options_t *options);

/*
 * Parses query text into a fixed-capacity typed plan.
 *
 * The parser only constructs a plan; it does not read or mutate Raft state.
 * Any string views in the resulting plan borrow the input buffer, which must
 * remain alive and unchanged while the plan is used. The diagnostic argument
 * may be NULL and is reset on entry when provided.
 */
int tr_text_query_parse(const char *input,
                        size_t input_length,
                        const tr_text_parse_options_t *options,
                        tr_text_query_plan_t *plan,
                        tr_text_diagnostic_t *diagnostic);

/*
 * Parses a protocol-debug frame description into a typed plan.
 * The input-lifetime, diagnostic, limit, and state-isolation rules of
 * tr_text_query_parse apply here as well.
 */
int tr_text_protocol_debug_parse(
    const char *input,
    size_t input_length,
    const tr_text_parse_options_t *options,
    tr_text_protocol_debug_plan_t *plan,
    tr_text_diagnostic_t *diagnostic);

/*
 * Parses replay actions into a typed plan.
 * The input-lifetime, diagnostic, limit, and state-isolation rules of
 * tr_text_query_parse apply here as well.
 */
int tr_text_replay_parse(const char *input,
                         size_t input_length,
                         const tr_text_parse_options_t *options,
                         tr_text_replay_plan_t *plan,
                         tr_text_diagnostic_t *diagnostic);

#ifdef __cplusplus
}
#endif

#endif
