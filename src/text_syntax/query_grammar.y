%name tr_text_query_parser
%token_prefix TR_TEXT_QUERY_TOKEN_
%token_type {tr_text_token_t}
%default_type {tr_text_token_t}
%stack_size 128
%extra_argument {tr_text_query_parse_context_t *ctx}

%include {
#include "text_syntax_internal.h"
}

%token SHOW STATUS MEMBERS WHERE ROLE VOTER LEARNER PROGRESS FOR NODE.
%token IDENTIFIER INTEGER EQ SEMICOLON.

%start_symbol input

input ::= statements.

statements ::= statement.
statements ::= statements statement.

statement ::= SHOW STATUS SEMICOLON. {
  tr_text_query_append_status(ctx);
}
statement ::= SHOW MEMBERS SEMICOLON. {
  tr_text_query_append_members(ctx, TR_TEXT_QUERY_ROLE_ANY);
}
statement ::= SHOW MEMBERS WHERE ROLE EQ VOTER SEMICOLON. {
  tr_text_query_append_members(ctx, TR_TEXT_QUERY_ROLE_VOTER);
}
statement ::= SHOW MEMBERS WHERE ROLE EQ LEARNER SEMICOLON. {
  tr_text_query_append_members(ctx, TR_TEXT_QUERY_ROLE_LEARNER);
}
statement ::= SHOW PROGRESS FOR NODE INTEGER(N) SEMICOLON. {
  tr_text_query_append_progress(ctx, N.number);
}

%syntax_error {
  tr_text_parse_context_fail(&ctx->base, SALTS_EPROTO,
                             TR_TEXT_DIAGNOSTIC_SYNTAX,
                             "query syntax error");
}

%parse_failure {
  tr_text_parse_context_fail(&ctx->base, SALTS_EPROTO,
                             TR_TEXT_DIAGNOSTIC_SYNTAX,
                             "query parse failure");
}
