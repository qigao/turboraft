%name tr_text_replay_parser
%token_prefix TR_TEXT_REPLAY_TOKEN_
%token_type {tr_text_token_t}
%default_type {tr_text_token_t}
%stack_size 128
%extra_argument {tr_text_replay_parse_context_t *ctx}

%include {
#include "text_syntax_internal.h"
}

%token NODE TICK SEND DROP PARTITION HEAL WITH DELAY BY DUPLICATE TICKS
       NEXT EXPECT ROLE COMMIT_INDEX SUBMIT REQUEST TO CLIENT SEQUENCE PAYLOAD
       POLL UNTIL ACCEPTED COMMITTED APPLIED TIMEOUT.
%token IDENTIFIER INTEGER HEX_LITERAL EQ GE ARROW SEMICOLON.

%start_symbol input

input ::= statements.

statements ::= statement.
statements ::= statements statement.

statement ::= NODE INTEGER(N) SEMICOLON. {
  tr_text_replay_append_node(ctx, N.number);
}
statement ::= TICK INTEGER(N) SEMICOLON. {
  tr_text_replay_append_tick(ctx, N.number);
}
statement ::= SEND NODE INTEGER(F) ARROW NODE INTEGER(T) SEMICOLON. {
  tr_text_replay_append_send(ctx, F.number, T.number);
}
statement ::= DROP NEXT IDENTIFIER(M) SEMICOLON. {
  tr_text_replay_append_drop_next(ctx, M);
}
statement ::= PARTITION NODE INTEGER(N) WITH NODE INTEGER(P) SEMICOLON. {
  tr_text_replay_append_partition(ctx, N.number, P.number);
}
statement ::= HEAL NODE INTEGER(N) WITH NODE INTEGER(P) SEMICOLON. {
  tr_text_replay_append_heal(ctx, N.number, P.number);
}
statement ::= DELAY NEXT IDENTIFIER(M) BY INTEGER(T) TICKS SEMICOLON. {
  tr_text_replay_append_delay_next(ctx, M, T.number);
}
statement ::= DUPLICATE NEXT IDENTIFIER(M) SEMICOLON. {
  tr_text_replay_append_duplicate_next(ctx, M);
}
statement ::= SUBMIT REQUEST INTEGER(R) TO NODE INTEGER(N)
              CLIENT INTEGER(C) SEQUENCE INTEGER(S)
              PAYLOAD HEX_LITERAL(P) SEMICOLON. {
  tr_text_replay_append_submit(ctx, R.number, N.number, C.number, S.number,
                               P);
}
statement ::= POLL REQUEST INTEGER(R) UNTIL poll_target(T)
              TIMEOUT INTEGER(TICKS) TICKS SEMICOLON. {
  tr_text_replay_append_poll(
      ctx, R.number, (tr_text_replay_poll_target_t)T.number, TICKS.number);
}
poll_target(A) ::= ACCEPTED. {
  A.number = TR_TEXT_REPLAY_POLL_ACCEPTED;
}
poll_target(A) ::= COMMITTED. {
  A.number = TR_TEXT_REPLAY_POLL_COMMITTED;
}
poll_target(A) ::= APPLIED. {
  A.number = TR_TEXT_REPLAY_POLL_APPLIED;
}
statement ::= EXPECT NODE INTEGER(N) ROLE EQ IDENTIFIER(R) SEMICOLON. {
  tr_text_replay_append_expect_role(ctx, N.number, R);
}
statement ::= EXPECT NODE INTEGER(N) COMMIT_INDEX GE INTEGER(V) SEMICOLON. {
  tr_text_replay_append_expect_commit_index(ctx, N.number, V.number);
}

%syntax_error {
  tr_text_parse_context_fail(&ctx->base, SALTS_EPROTO,
                             TR_TEXT_DIAGNOSTIC_SYNTAX,
                             "replay syntax error");
}

%parse_failure {
  tr_text_parse_context_fail(&ctx->base, SALTS_EPROTO,
                             TR_TEXT_DIAGNOSTIC_SYNTAX,
                             "replay parse failure");
}
