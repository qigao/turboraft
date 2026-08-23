#include "text_syntax_internal.h"
#include <turboraft/raft_wire_codec.h>

#include "turboraft_text_protocol_debug_grammar_gen.h"
#include "turboraft_text_query_grammar_gen.h"
#include "turboraft_text_replay_grammar_gen.h"

#include <limits.h>
#include <stdlib.h>
#include <string.h>

static const char *const TR_TEXT_DEFAULT_SYNTAX_ERROR =
    "text syntax error";

static void tr_text_diagnostic_reset(tr_text_diagnostic_t *diagnostic) {
  if (diagnostic != NULL) {
    memset(diagnostic, 0, sizeof(*diagnostic));
  }
}

void tr_text_parse_options_init(tr_text_parse_options_t *options) {
  if (options == NULL) {
    return;
  }
  options->max_input_bytes = TR_TEXT_MAX_INPUT_BYTES;
  options->max_statements = TR_TEXT_MAX_STATEMENTS;
}

static int tr_text_normalize_options(const tr_text_parse_options_t *options,
                                     size_t *max_input_bytes,
                                     size_t *max_statements) {
  tr_text_parse_options_t defaults;
  if (options == NULL) {
    tr_text_parse_options_init(&defaults);
    options = &defaults;
  }

  *max_input_bytes = options->max_input_bytes == 0u
                         ? TR_TEXT_MAX_INPUT_BYTES
                         : options->max_input_bytes;
  *max_statements = options->max_statements == 0u
                        ? TR_TEXT_MAX_STATEMENTS
                        : options->max_statements;
  if (*max_input_bytes > TR_TEXT_MAX_INPUT_BYTES ||
      *max_statements > TR_TEXT_MAX_STATEMENTS) {
    return TURBO_EINVAL;
  }
  return TURBO_OK;
}

int tr_text_parse_context_init(tr_text_parse_context_base_t *base,
                               const char *input,
                               size_t input_length,
                               const tr_text_parse_options_t *options,
                               tr_text_diagnostic_t *diagnostic) {
  size_t max_input_bytes;
  size_t max_statements;
  if (base == NULL) {
    return TURBO_EINVAL;
  }
  memset(base, 0, sizeof(*base));
  tr_text_diagnostic_reset(diagnostic);
  base->status = TURBO_OK;
  base->diagnostic = diagnostic;
  if (tr_text_normalize_options(options, &max_input_bytes, &max_statements) !=
      TURBO_OK) {
    tr_text_parse_context_fail_at(base, TURBO_EINVAL,
                                  TR_TEXT_DIAGNOSTIC_ARGUMENT, 0u, 1u, 1u,
                                  "text parse options exceed hard limits");
    return base->status;
  }
  base->max_statements = max_statements;
  if (input == NULL || input_length > max_input_bytes) {
    tr_text_parse_context_fail_at(
        base, input == NULL ? TURBO_EINVAL : TURBO_ENOSPC,
        input == NULL ? TR_TEXT_DIAGNOSTIC_ARGUMENT : TR_TEXT_DIAGNOSTIC_LIMIT,
        0u, 1u, 1u,
        input == NULL ? "text input is null" : "text input exceeds hard limit");
    return base->status;
  }
  return TURBO_OK;
}

void tr_text_parse_context_fail_at(tr_text_parse_context_base_t *base,
                                   int code,
                                   tr_text_diagnostic_kind_t kind,
                                   size_t offset,
                                   size_t line,
                                   size_t column,
                                   const char *message) {
  if (base == NULL || base->status != TURBO_OK) {
    return;
  }
  base->status = code;
  if (base->diagnostic != NULL) {
    base->diagnostic->code = code;
    base->diagnostic->kind = kind;
    base->diagnostic->offset = offset;
    base->diagnostic->line = line;
    base->diagnostic->column = column;
    base->diagnostic->message = message;
  }
}

void tr_text_parse_context_fail(tr_text_parse_context_base_t *base,
                                int code,
                                tr_text_diagnostic_kind_t kind,
                                const char *message) {
  if (base == NULL) {
    return;
  }
  tr_text_parse_context_fail_at(base, code, kind, base->last_token.offset,
                                base->last_token.line, base->last_token.column,
                                message);
}

int tr_text_parse_context_reserve_statement(
    tr_text_parse_context_base_t *base) {
  if (base == NULL) {
    return TURBO_EINVAL;
  }
  if (base->status != TURBO_OK) {
    return base->status;
  }
  if (base->max_statements == 0u) {
    tr_text_parse_context_fail(base, TURBO_ENOSPC, TR_TEXT_DIAGNOSTIC_LIMIT,
                               "text statement limit is zero");
    return base->status;
  }
  return TURBO_OK;
}

int tr_text_parse_context_finish(const tr_text_parse_context_base_t *base) {
  return base == NULL ? TURBO_EINVAL : base->status;
}

void tr_text_lexer_init(tr_text_lexer_t *lexer,
                        const char *input,
                        size_t input_length) {
  if (lexer == NULL) {
    return;
  }
  memset(lexer, 0, sizeof(*lexer));
  lexer->input = input;
  lexer->cursor = input;
  lexer->limit = input + input_length;
  lexer->line = 1u;
  lexer->column = 1u;
}

void tr_text_lexer_advance(tr_text_lexer_t *lexer,
                           const char *begin,
                           const char *end) {
  const char *cursor;
  if (lexer == NULL || begin == NULL || end == NULL || end < begin) {
    return;
  }
  cursor = begin;
  while (cursor < end) {
    if (*cursor == '\r') {
      if (cursor + 1 < end && cursor[1] == '\n') {
        ++cursor;
      }
      ++lexer->line;
      lexer->column = 1u;
    } else if (*cursor == '\n') {
      ++lexer->line;
      lexer->column = 1u;
    } else {
      ++lexer->column;
    }
    ++cursor;
  }
}

void tr_text_lexer_set_error(tr_text_lexer_t *lexer,
                             int code,
                             const char *message) {
  if (lexer == NULL) {
    return;
  }
  lexer->error_code = code;
  lexer->error_message = message;
}

int tr_text_parse_uint64(const char *data, size_t length, uint64_t *value) {
  size_t index;
  uint64_t result = 0u;
  if (data == NULL || value == NULL || length == 0u) {
    return TURBO_EINVAL;
  }
  for (index = 0u; index < length; ++index) {
    uint64_t digit;
    if (data[index] < '0' || data[index] > '9') {
      return TURBO_EPROTO;
    }
    digit = (uint64_t)(data[index] - '0');
    if (result > (UINT64_MAX - digit) / 10u) {
      return TURBO_ERANGE;
    }
    result = result * 10u + digit;
  }
  *value = result;
  return TURBO_OK;
}

void tr_text_token_set(tr_text_token_t *token,
                       const tr_text_lexer_t *lexer,
                       const char *begin,
                       const char *end) {
  if (token == NULL || lexer == NULL || begin == NULL || end == NULL ||
      end < begin) {
    return;
  }
  token->text.data = begin;
  token->text.len = (size_t)(end - begin);
  token->offset = (size_t)(begin - lexer->input);
  token->line = lexer->line;
  token->column = lexer->column;
}

static int tr_text_parser_run_query(const char *input,
                                    size_t input_length,
                                    const tr_text_parse_options_t *options,
                                    tr_text_query_plan_t *plan,
                                    tr_text_diagnostic_t *diagnostic) {
  tr_text_query_parse_context_t ctx;
  tr_text_lexer_t lexer;
  tr_text_token_t token;
  void *parser;
  int token_type;
  int status;

  if (plan == NULL) {
    return TURBO_EINVAL;
  }
  memset(&ctx, 0, sizeof(ctx));
  memset(plan, 0, sizeof(*plan));
  if (tr_text_parse_context_init(&ctx.base, input, input_length, options,
                                diagnostic) != TURBO_OK) {
    return ctx.base.status;
  }
  ctx.plan = plan;
  tr_text_query_lexer_init(&lexer, input, input_length);
  parser = tr_text_query_parserAlloc(malloc);
  if (parser == NULL) {
    tr_text_parse_context_fail(&ctx.base, TURBO_ENOMEM,
                               TR_TEXT_DIAGNOSTIC_LIMIT,
                               "query parser allocation failed");
    return ctx.base.status;
  }

  for (;;) {
    memset(&token, 0, sizeof(token));
    token_type = tr_text_query_lexer_next(&lexer, &token);
    if (token_type < 0) {
      tr_text_parse_context_fail_at(
          &ctx.base, lexer.error_code == 0 ? TURBO_EPROTO : lexer.error_code,
          TR_TEXT_DIAGNOSTIC_LEXICAL, token.offset, token.line, token.column,
          lexer.error_message == NULL ? "query lexical error"
                                       : lexer.error_message);
      break;
    }
    ctx.base.last_token = token;
    tr_text_query_parser(parser, token_type, token, &ctx);
    if (ctx.base.status != TURBO_OK || token_type == 0) {
      break;
    }
  }
  status = tr_text_parse_context_finish(&ctx.base);
  tr_text_query_parserFree(parser, free);
  if (status != TURBO_OK) {
    memset(plan, 0, sizeof(*plan));
  }
  return status;
}

static int tr_text_parser_run_protocol(
    const char *input,
    size_t input_length,
    const tr_text_parse_options_t *options,
    tr_text_protocol_debug_plan_t *plan,
    tr_text_diagnostic_t *diagnostic) {
  tr_text_protocol_debug_parse_context_t ctx;
  tr_text_lexer_t lexer;
  tr_text_token_t token;
  void *parser;
  int token_type;
  int status;

  if (plan == NULL) {
    return TURBO_EINVAL;
  }
  memset(&ctx, 0, sizeof(ctx));
  memset(plan, 0, sizeof(*plan));
  if (tr_text_parse_context_init(&ctx.base, input, input_length, options,
                                diagnostic) != TURBO_OK) {
    return ctx.base.status;
  }
  ctx.plan = plan;
  tr_text_protocol_debug_lexer_init(&lexer, input, input_length);
  parser = tr_text_protocol_debug_parserAlloc(malloc);
  if (parser == NULL) {
    tr_text_parse_context_fail(&ctx.base, TURBO_ENOMEM,
                               TR_TEXT_DIAGNOSTIC_LIMIT,
                               "protocol parser allocation failed");
    return ctx.base.status;
  }

  for (;;) {
    memset(&token, 0, sizeof(token));
    token_type = tr_text_protocol_debug_lexer_next(&lexer, &token);
    if (token_type < 0) {
      tr_text_parse_context_fail_at(
          &ctx.base, lexer.error_code == 0 ? TURBO_EPROTO : lexer.error_code,
          TR_TEXT_DIAGNOSTIC_LEXICAL, token.offset, token.line, token.column,
          lexer.error_message == NULL ? "protocol lexical error"
                                       : lexer.error_message);
      break;
    }
    ctx.base.last_token = token;
    tr_text_protocol_debug_parser(parser, token_type, token, &ctx);
    if (ctx.base.status != TURBO_OK || token_type == 0) {
      break;
    }
  }
  status = tr_text_parse_context_finish(&ctx.base);
  tr_text_protocol_debug_parserFree(parser, free);
  if (status != TURBO_OK) {
    memset(plan, 0, sizeof(*plan));
  }
  return status;
}

static int tr_text_parser_run_replay(const char *input,
                                     size_t input_length,
                                     const tr_text_parse_options_t *options,
                                     tr_text_replay_plan_t *plan,
                                     tr_text_diagnostic_t *diagnostic) {
  tr_text_replay_parse_context_t ctx;
  tr_text_lexer_t lexer;
  tr_text_token_t token;
  void *parser;
  int token_type;
  int status;

  if (plan == NULL) {
    return TURBO_EINVAL;
  }
  memset(&ctx, 0, sizeof(ctx));
  memset(plan, 0, sizeof(*plan));
  if (tr_text_parse_context_init(&ctx.base, input, input_length, options,
                                diagnostic) != TURBO_OK) {
    return ctx.base.status;
  }
  ctx.plan = plan;
  tr_text_replay_lexer_init(&lexer, input, input_length);
  parser = tr_text_replay_parserAlloc(malloc);
  if (parser == NULL) {
    tr_text_parse_context_fail(&ctx.base, TURBO_ENOMEM,
                               TR_TEXT_DIAGNOSTIC_LIMIT,
                               "replay parser allocation failed");
    return ctx.base.status;
  }

  for (;;) {
    memset(&token, 0, sizeof(token));
    token_type = tr_text_replay_lexer_next(&lexer, &token);
    if (token_type < 0) {
      tr_text_parse_context_fail_at(
          &ctx.base, lexer.error_code == 0 ? TURBO_EPROTO : lexer.error_code,
          TR_TEXT_DIAGNOSTIC_LEXICAL, token.offset, token.line, token.column,
          lexer.error_message == NULL ? "replay lexical error"
                                       : lexer.error_message);
      break;
    }
    ctx.base.last_token = token;
    tr_text_replay_parser(parser, token_type, token, &ctx);
    if (ctx.base.status != TURBO_OK || token_type == 0) {
      break;
    }
  }
  status = tr_text_parse_context_finish(&ctx.base);
  tr_text_replay_parserFree(parser, free);
  if (status != TURBO_OK) {
    memset(plan, 0, sizeof(*plan));
  }
  return status;
}

void tr_text_query_append_status(tr_text_query_parse_context_t *ctx) {
  tr_text_query_command_t *command;
  if (ctx == NULL ||
      tr_text_parse_context_reserve_statement(&ctx->base) != TURBO_OK) {
    return;
  }
  if (ctx->plan->command_count >= ctx->base.max_statements) {
    tr_text_parse_context_fail(&ctx->base, TURBO_ENOSPC,
                               TR_TEXT_DIAGNOSTIC_LIMIT,
                               "query statement limit exceeded");
    return;
  }
  command = &ctx->plan->commands[ctx->plan->command_count++];
  memset(command, 0, sizeof(*command));
  command->kind = TR_TEXT_QUERY_SHOW_STATUS;
  command->role = TR_TEXT_QUERY_ROLE_ANY;
}

void tr_text_query_append_members(tr_text_query_parse_context_t *ctx,
                                  tr_text_query_role_t role) {
  tr_text_query_command_t *command;
  if (ctx == NULL ||
      tr_text_parse_context_reserve_statement(&ctx->base) != TURBO_OK) {
    return;
  }
  if (ctx->plan->command_count >= ctx->base.max_statements) {
    tr_text_parse_context_fail(&ctx->base, TURBO_ENOSPC,
                               TR_TEXT_DIAGNOSTIC_LIMIT,
                               "query statement limit exceeded");
    return;
  }
  command = &ctx->plan->commands[ctx->plan->command_count++];
  memset(command, 0, sizeof(*command));
  command->kind = TR_TEXT_QUERY_SHOW_MEMBERS;
  command->role = role;
}

void tr_text_query_append_progress(tr_text_query_parse_context_t *ctx,
                                   uint64_t node_id) {
  tr_text_query_command_t *command;
  if (ctx == NULL ||
      tr_text_parse_context_reserve_statement(&ctx->base) != TURBO_OK) {
    return;
  }
  if (ctx->plan->command_count >= ctx->base.max_statements) {
    tr_text_parse_context_fail(&ctx->base, TURBO_ENOSPC,
                               TR_TEXT_DIAGNOSTIC_LIMIT,
                               "query statement limit exceeded");
    return;
  }
  command = &ctx->plan->commands[ctx->plan->command_count++];
  memset(command, 0, sizeof(*command));
  command->kind = TR_TEXT_QUERY_SHOW_PROGRESS;
  command->node_id = node_id;
}

void tr_text_protocol_begin_frame(tr_text_protocol_debug_parse_context_t *ctx,
                                  tr_text_token_t version,
                                  tr_text_token_t kind) {
  if (ctx == NULL || ctx->base.status != TURBO_OK) {
    return;
  }
  if (ctx->frame_active) {
    tr_text_parse_context_fail(&ctx->base, TURBO_EPROTO,
                               TR_TEXT_DIAGNOSTIC_SEMANTIC,
                               "nested protocol frame");
    return;
  }
  if (ctx->plan->frame_count >= ctx->base.max_statements) {
    tr_text_parse_context_fail(&ctx->base, TURBO_ENOSPC,
                               TR_TEXT_DIAGNOSTIC_LIMIT,
                               "protocol frame limit exceeded");
    return;
  }
  memset(&ctx->current_frame, 0, sizeof(ctx->current_frame));
  ctx->current_frame.version = version.number;
  ctx->current_frame.kind = kind.text;
  ctx->current_fields = TR_TEXT_PROTOCOL_FIELD_VERSION |
                        TR_TEXT_PROTOCOL_FIELD_KIND;
  ctx->frame_active = 1;
}

static int tr_text_protocol_claim_field(
    tr_text_protocol_debug_parse_context_t *ctx,
    uint32_t field) {
  if (ctx == NULL || ctx->base.status != TURBO_OK) {
    return 0;
  }
  if (!ctx->frame_active) {
    tr_text_parse_context_fail(&ctx->base, TURBO_EPROTO,
                               TR_TEXT_DIAGNOSTIC_SEMANTIC,
                               "protocol field is outside a frame");
    return 0;
  }
  if ((ctx->current_fields & field) != 0u) {
    tr_text_parse_context_fail(&ctx->base, TURBO_EPROTO,
                               TR_TEXT_DIAGNOSTIC_SEMANTIC,
                               "duplicate protocol frame field");
    return 0;
  }
  ctx->current_fields |= field;
  return 1;
}

void tr_text_protocol_set_value(tr_text_protocol_debug_parse_context_t *ctx,
                                tr_text_protocol_value_kind_t kind,
                                tr_text_token_t value) {
  uint32_t field;
  uint64_t *destination;
  if (ctx == NULL || ctx->base.status != TURBO_OK) {
    return;
  }
  switch (kind) {
    case TR_TEXT_PROTOCOL_VALUE_FROM:
      field = TR_TEXT_PROTOCOL_FIELD_FROM;
      destination = &ctx->current_frame.from;
      break;
    case TR_TEXT_PROTOCOL_VALUE_TO:
      field = TR_TEXT_PROTOCOL_FIELD_TO;
      destination = &ctx->current_frame.to;
      break;
    case TR_TEXT_PROTOCOL_VALUE_TERM:
      field = TR_TEXT_PROTOCOL_FIELD_TERM;
      destination = &ctx->current_frame.term;
      break;
    default:
      tr_text_parse_context_fail(&ctx->base, TURBO_EINVAL,
                                 TR_TEXT_DIAGNOSTIC_SEMANTIC,
                                 "unknown protocol field");
      return;
  }
  if (!tr_text_protocol_claim_field(ctx, field)) {
    return;
  }
  *destination = value.number;
}

void tr_text_protocol_set_message(
    tr_text_protocol_debug_parse_context_t *ctx,
    tr_text_token_t message) {
  if (ctx == NULL || ctx->base.status != TURBO_OK) {
    return;
  }
  if (!tr_text_protocol_claim_field(ctx, TR_TEXT_PROTOCOL_FIELD_MESSAGE)) {
    return;
  }
  ctx->current_frame.message = message.text;
}

void tr_text_protocol_set_payload(
    tr_text_protocol_debug_parse_context_t *ctx,
    tr_text_token_t payload) {
  size_t hex_digits;
  if (ctx == NULL || ctx->base.status != TURBO_OK) {
    return;
  }
  if (!tr_text_protocol_claim_field(ctx, TR_TEXT_PROTOCOL_FIELD_PAYLOAD)) {
    return;
  }
  if (payload.text.len < 3u) {
    tr_text_parse_context_fail(&ctx->base, TURBO_EPROTO,
                               TR_TEXT_DIAGNOSTIC_SEMANTIC,
                               "protocol payload is empty");
    return;
  }
  hex_digits = payload.text.len - 2u;
  if ((hex_digits & 1u) != 0u) {
    tr_text_parse_context_fail(
        &ctx->base, TURBO_EPROTO, TR_TEXT_DIAGNOSTIC_SEMANTIC,
        "protocol payload must contain an even number of hex digits");
    return;
  }
  if (hex_digits / 2u > TR_RAFT_WIRE_MAX_FRAME_SIZE) {
    tr_text_parse_context_fail(
        &ctx->base, TURBO_ENOSPC, TR_TEXT_DIAGNOSTIC_LIMIT,
        "protocol payload exceeds the maximum wire frame size");
    return;
  }
  ctx->current_frame.payload_hex = payload.text;
}

void tr_text_protocol_finish_frame(
    tr_text_protocol_debug_parse_context_t *ctx) {
  const uint32_t required = TR_TEXT_PROTOCOL_FIELD_VERSION |
                            TR_TEXT_PROTOCOL_FIELD_KIND |
                            TR_TEXT_PROTOCOL_FIELD_FROM |
                            TR_TEXT_PROTOCOL_FIELD_TO |
                            TR_TEXT_PROTOCOL_FIELD_TERM |
                            TR_TEXT_PROTOCOL_FIELD_MESSAGE;
  if (ctx == NULL || ctx->base.status != TURBO_OK) {
    return;
  }
  if (!ctx->frame_active || (ctx->current_fields & required) != required) {
    tr_text_parse_context_fail(&ctx->base, TURBO_EPROTO,
                               TR_TEXT_DIAGNOSTIC_SEMANTIC,
                               "protocol frame is missing a required field");
    return;
  }
  ctx->plan->frames[ctx->plan->frame_count++] = ctx->current_frame;
  ctx->frame_active = 0;
}

static tr_text_replay_action_t *tr_text_replay_append(
    tr_text_replay_parse_context_t *ctx) {
  tr_text_replay_action_t *action;
  if (ctx == NULL ||
      tr_text_parse_context_reserve_statement(&ctx->base) != TURBO_OK) {
    return NULL;
  }
  if (ctx->plan->action_count >= ctx->base.max_statements) {
    tr_text_parse_context_fail(&ctx->base, TURBO_ENOSPC,
                               TR_TEXT_DIAGNOSTIC_LIMIT,
                               "replay action limit exceeded");
    return NULL;
  }
  action = &ctx->plan->actions[ctx->plan->action_count++];
  memset(action, 0, sizeof(*action));
  return action;
}

void tr_text_replay_append_node(tr_text_replay_parse_context_t *ctx,
                                uint64_t node_id) {
  tr_text_replay_action_t *action = tr_text_replay_append(ctx);
  if (action == NULL) {
    return;
  }
  action->kind = TR_TEXT_REPLAY_NODE;
  action->node_id = node_id;
}

void tr_text_replay_append_tick(tr_text_replay_parse_context_t *ctx,
                                uint64_t ticks) {
  tr_text_replay_action_t *action = tr_text_replay_append(ctx);
  if (action == NULL) {
    return;
  }
  action->kind = TR_TEXT_REPLAY_TICK;
  action->value = ticks;
}

void tr_text_replay_append_send(tr_text_replay_parse_context_t *ctx,
                                uint64_t from,
                                uint64_t to) {
  tr_text_replay_action_t *action = tr_text_replay_append(ctx);
  if (action == NULL) {
    return;
  }
  action->kind = TR_TEXT_REPLAY_SEND;
  action->node_id = from;
  action->peer_id = to;
}

void tr_text_replay_append_drop_next(tr_text_replay_parse_context_t *ctx,
                                     tr_text_token_t message) {
  tr_text_replay_action_t *action = tr_text_replay_append(ctx);
  if (action == NULL) {
    return;
  }
  action->kind = TR_TEXT_REPLAY_DROP_NEXT;
  action->name = message.text;
}

void tr_text_replay_append_partition(tr_text_replay_parse_context_t *ctx,
                                     uint64_t node_id,
                                     uint64_t peer_id) {
  tr_text_replay_action_t *action = tr_text_replay_append(ctx);
  if (action == NULL) {
    return;
  }
  action->kind = TR_TEXT_REPLAY_PARTITION;
  action->node_id = node_id;
  action->peer_id = peer_id;
}

void tr_text_replay_append_heal(tr_text_replay_parse_context_t *ctx,
                                uint64_t node_id,
                                uint64_t peer_id) {
  tr_text_replay_action_t *action = tr_text_replay_append(ctx);
  if (action == NULL) {
    return;
  }
  action->kind = TR_TEXT_REPLAY_HEAL;
  action->node_id = node_id;
  action->peer_id = peer_id;
}

void tr_text_replay_append_delay_next(tr_text_replay_parse_context_t *ctx,
                                      tr_text_token_t message,
                                      uint64_t ticks) {
  tr_text_replay_action_t *action = tr_text_replay_append(ctx);
  if (action == NULL) {
    return;
  }
  action->kind = TR_TEXT_REPLAY_DELAY_NEXT;
  action->name = message.text;
  action->value = ticks;
}

void tr_text_replay_append_duplicate_next(
    tr_text_replay_parse_context_t *ctx,
    tr_text_token_t message) {
  tr_text_replay_action_t *action = tr_text_replay_append(ctx);
  if (action == NULL) {
    return;
  }
  action->kind = TR_TEXT_REPLAY_DUPLICATE_NEXT;
  action->name = message.text;
}

static int tr_text_validate_hex_payload(tr_text_parse_context_base_t *base,
                                        vstr payload,
                                        size_t max_decoded_bytes,
                                        const char *empty_message,
                                        const char *odd_length_message,
                                        const char *oversize_message) {
  size_t hex_digits;
  if (payload.len < 3u) {
    tr_text_parse_context_fail(base, TURBO_EPROTO,
                               TR_TEXT_DIAGNOSTIC_SEMANTIC, empty_message);
    return 0;
  }
  hex_digits = payload.len - 2u;
  if ((hex_digits & 1u) != 0u) {
    tr_text_parse_context_fail(base, TURBO_EPROTO,
                               TR_TEXT_DIAGNOSTIC_SEMANTIC,
                               odd_length_message);
    return 0;
  }
  if (hex_digits / 2u > max_decoded_bytes) {
    tr_text_parse_context_fail(base, TURBO_ENOSPC,
                               TR_TEXT_DIAGNOSTIC_LIMIT, oversize_message);
    return 0;
  }
  return 1;
}

void tr_text_replay_append_submit(tr_text_replay_parse_context_t *ctx,
                                  uint64_t request_id,
                                  uint64_t node_id,
                                  uint64_t client_id,
                                  uint64_t sequence,
                                  tr_text_token_t payload) {
  tr_text_replay_action_t *action = tr_text_replay_append(ctx);
  if (action == NULL) {
    return;
  }
  if (!tr_text_validate_hex_payload(
          &ctx->base, payload.text, TR_RAFT_MAX_ENTRY_BYTES,
          "replay submit payload is empty",
          "replay submit payload must contain an even number of hex digits",
          "replay submit payload exceeds a Raft entry")) {
    return;
  }
  action->kind = TR_TEXT_REPLAY_SUBMIT;
  action->node_id = node_id;
  action->client_id = client_id;
  action->request_id = request_id;
  action->sequence = sequence;
  action->payload_hex = payload.text;
}

void tr_text_replay_append_poll(tr_text_replay_parse_context_t *ctx,
                                uint64_t request_id,
                                tr_text_replay_poll_target_t target,
                                uint64_t timeout_ticks) {
  tr_text_replay_action_t *action = tr_text_replay_append(ctx);
  if (action == NULL) {
    return;
  }
  action->kind = TR_TEXT_REPLAY_POLL;
  action->request_id = request_id;
  action->poll_target = target;
  action->timeout_ticks = timeout_ticks;
}

void tr_text_replay_append_expect_role(tr_text_replay_parse_context_t *ctx,
                                       uint64_t node_id,
                                       tr_text_token_t role) {
  tr_text_replay_action_t *action = tr_text_replay_append(ctx);
  if (action == NULL) {
    return;
  }
  action->kind = TR_TEXT_REPLAY_EXPECT_ROLE;
  action->node_id = node_id;
  action->comparison = TR_TEXT_REPLAY_COMPARE_EQ;
  action->name = role.text;
}

void tr_text_replay_append_expect_commit_index(
    tr_text_replay_parse_context_t *ctx,
    uint64_t node_id,
    uint64_t commit_index) {
  tr_text_replay_action_t *action = tr_text_replay_append(ctx);
  if (action == NULL) {
    return;
  }
  action->kind = TR_TEXT_REPLAY_EXPECT_COMMIT_INDEX;
  action->node_id = node_id;
  action->value = commit_index;
  action->comparison = TR_TEXT_REPLAY_COMPARE_GE;
}

int tr_text_query_parse(const char *input,
                        size_t input_length,
                        const tr_text_parse_options_t *options,
                        tr_text_query_plan_t *plan,
                        tr_text_diagnostic_t *diagnostic) {
  return tr_text_parser_run_query(input, input_length, options, plan,
                                  diagnostic);
}

int tr_text_protocol_debug_parse(
    const char *input,
    size_t input_length,
    const tr_text_parse_options_t *options,
    tr_text_protocol_debug_plan_t *plan,
    tr_text_diagnostic_t *diagnostic) {
  return tr_text_parser_run_protocol(input, input_length, options, plan,
                                     diagnostic);
}

int tr_text_replay_parse(const char *input,
                         size_t input_length,
                         const tr_text_parse_options_t *options,
                         tr_text_replay_plan_t *plan,
                         tr_text_diagnostic_t *diagnostic) {
  return tr_text_parser_run_replay(input, input_length, options, plan,
                                   diagnostic);
}
