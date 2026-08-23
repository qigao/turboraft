#ifndef TURBORAFT_TEXT_SYNTAX_INTERNAL_H
#define TURBORAFT_TEXT_SYNTAX_INTERNAL_H

#include <turboraft/text_syntax.h>

#include <stdint.h>

typedef struct {
  int type;
  vstr text;
  uint64_t number;
  size_t offset;
  size_t line;
  size_t column;
} tr_text_token_t;

typedef struct {
  const char *input;
  const char *cursor;
  const char *limit;
  size_t line;
  size_t column;
  int error_code;
  const char *error_message;
} tr_text_lexer_t;

typedef struct {
  int status;
  tr_text_diagnostic_t *diagnostic;
  size_t max_statements;
  tr_text_token_t last_token;
} tr_text_parse_context_base_t;

typedef struct {
  tr_text_parse_context_base_t base;
  tr_text_query_plan_t *plan;
} tr_text_query_parse_context_t;

typedef struct {
  tr_text_parse_context_base_t base;
  tr_text_protocol_debug_plan_t *plan;
  tr_text_protocol_frame_t current_frame;
  uint32_t current_fields;
  int frame_active;
} tr_text_protocol_debug_parse_context_t;

typedef struct {
  tr_text_parse_context_base_t base;
  tr_text_replay_plan_t *plan;
} tr_text_replay_parse_context_t;

void *tr_text_query_parserAlloc(void *(*malloc_proc)(size_t));
void tr_text_query_parser(void *parser,
                          int token_type,
                          tr_text_token_t token,
                          tr_text_query_parse_context_t *ctx);
void tr_text_query_parserFree(void *parser, void (*free_proc)(void *));

void *tr_text_protocol_debug_parserAlloc(void *(*malloc_proc)(size_t));
void tr_text_protocol_debug_parser(
    void *parser,
    int token_type,
    tr_text_token_t token,
    tr_text_protocol_debug_parse_context_t *ctx);
void tr_text_protocol_debug_parserFree(void *parser,
                                       void (*free_proc)(void *));

void *tr_text_replay_parserAlloc(void *(*malloc_proc)(size_t));
void tr_text_replay_parser(void *parser,
                           int token_type,
                           tr_text_token_t token,
                           tr_text_replay_parse_context_t *ctx);
void tr_text_replay_parserFree(void *parser, void (*free_proc)(void *));

enum {
  TR_TEXT_PROTOCOL_FIELD_VERSION = 1u << 0,
  TR_TEXT_PROTOCOL_FIELD_KIND = 1u << 1,
  TR_TEXT_PROTOCOL_FIELD_FROM = 1u << 2,
  TR_TEXT_PROTOCOL_FIELD_TO = 1u << 3,
  TR_TEXT_PROTOCOL_FIELD_TERM = 1u << 4,
  TR_TEXT_PROTOCOL_FIELD_MESSAGE = 1u << 5,
  TR_TEXT_PROTOCOL_FIELD_PAYLOAD = 1u << 6
};

typedef enum {
  TR_TEXT_PROTOCOL_VALUE_FROM = 0,
  TR_TEXT_PROTOCOL_VALUE_TO,
  TR_TEXT_PROTOCOL_VALUE_TERM
} tr_text_protocol_value_kind_t;

void tr_text_lexer_init(tr_text_lexer_t *lexer,
                        const char *input,
                        size_t input_length);
void tr_text_lexer_advance(tr_text_lexer_t *lexer,
                           const char *begin,
                           const char *end);
void tr_text_lexer_set_error(tr_text_lexer_t *lexer,
                             int code,
                             const char *message);
int tr_text_parse_uint64(const char *data, size_t length, uint64_t *value);
void tr_text_token_set(tr_text_token_t *token,
                       const tr_text_lexer_t *lexer,
                       const char *begin,
                       const char *end);

int tr_text_parse_context_init(tr_text_parse_context_base_t *base,
                               const char *input,
                               size_t input_length,
                               const tr_text_parse_options_t *options,
                               tr_text_diagnostic_t *diagnostic);
void tr_text_parse_context_fail(tr_text_parse_context_base_t *base,
                                int code,
                                tr_text_diagnostic_kind_t kind,
                                const char *message);
void tr_text_parse_context_fail_at(tr_text_parse_context_base_t *base,
                                   int code,
                                   tr_text_diagnostic_kind_t kind,
                                   size_t offset,
                                   size_t line,
                                   size_t column,
                                   const char *message);
int tr_text_parse_context_reserve_statement(
    tr_text_parse_context_base_t *base);
int tr_text_parse_context_finish(const tr_text_parse_context_base_t *base);

void tr_text_query_append_status(tr_text_query_parse_context_t *ctx);
void tr_text_query_append_members(tr_text_query_parse_context_t *ctx,
                                  tr_text_query_role_t role);
void tr_text_query_append_progress(tr_text_query_parse_context_t *ctx,
                                   uint64_t node_id);

void tr_text_protocol_begin_frame(tr_text_protocol_debug_parse_context_t *ctx,
                                  tr_text_token_t version,
                                  tr_text_token_t kind);
void tr_text_protocol_set_value(tr_text_protocol_debug_parse_context_t *ctx,
                                tr_text_protocol_value_kind_t kind,
                                tr_text_token_t value);
void tr_text_protocol_set_message(
    tr_text_protocol_debug_parse_context_t *ctx,
    tr_text_token_t message);
void tr_text_protocol_set_payload(
    tr_text_protocol_debug_parse_context_t *ctx,
    tr_text_token_t payload);
void tr_text_protocol_finish_frame(
    tr_text_protocol_debug_parse_context_t *ctx);

void tr_text_replay_append_node(tr_text_replay_parse_context_t *ctx,
                                uint64_t node_id);
void tr_text_replay_append_tick(tr_text_replay_parse_context_t *ctx,
                                uint64_t ticks);
void tr_text_replay_append_send(tr_text_replay_parse_context_t *ctx,
                                uint64_t from,
                                uint64_t to);
void tr_text_replay_append_drop_next(tr_text_replay_parse_context_t *ctx,
                                     tr_text_token_t message);
void tr_text_replay_append_partition(tr_text_replay_parse_context_t *ctx,
                                     uint64_t node_id,
                                     uint64_t peer_id);
void tr_text_replay_append_heal(tr_text_replay_parse_context_t *ctx,
                                uint64_t node_id,
                                uint64_t peer_id);
void tr_text_replay_append_delay_next(tr_text_replay_parse_context_t *ctx,
                                      tr_text_token_t message,
                                      uint64_t ticks);
void tr_text_replay_append_duplicate_next(
    tr_text_replay_parse_context_t *ctx,
    tr_text_token_t message);
void tr_text_replay_append_submit(tr_text_replay_parse_context_t *ctx,
                                  uint64_t request_id,
                                  uint64_t node_id,
                                  uint64_t client_id,
                                  uint64_t sequence,
                                  tr_text_token_t payload);
void tr_text_replay_append_poll(tr_text_replay_parse_context_t *ctx,
                                uint64_t request_id,
                                tr_text_replay_poll_target_t target,
                                uint64_t timeout_ticks);
void tr_text_replay_append_expect_role(tr_text_replay_parse_context_t *ctx,
                                       uint64_t node_id,
                                       tr_text_token_t role);
void tr_text_replay_append_expect_commit_index(
    tr_text_replay_parse_context_t *ctx,
    uint64_t node_id,
    uint64_t commit_index);

void tr_text_query_lexer_init(tr_text_lexer_t *lexer,
                              const char *input,
                              size_t input_length);
int tr_text_query_lexer_next(tr_text_lexer_t *lexer, tr_text_token_t *token);
void tr_text_protocol_debug_lexer_init(tr_text_lexer_t *lexer,
                                       const char *input,
                                       size_t input_length);
int tr_text_protocol_debug_lexer_next(tr_text_lexer_t *lexer,
                                      tr_text_token_t *token);
void tr_text_replay_lexer_init(tr_text_lexer_t *lexer,
                               const char *input,
                               size_t input_length);
int tr_text_replay_lexer_next(tr_text_lexer_t *lexer,
                              tr_text_token_t *token);

#endif
