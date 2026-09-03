%name tr_text_protocol_debug_parser
%token_prefix TR_TEXT_PROTOCOL_DEBUG_TOKEN_
%token_type {tr_text_token_t}
%default_type {tr_text_token_t}
%stack_size 128
%extra_argument {tr_text_protocol_debug_parse_context_t *ctx}

%include {
#include "text_syntax_internal.h"
}

%token FRAME VERSION KIND FROM TO TERM MESSAGE PAYLOAD
       LBRACE RBRACE ASSIGN SEMICOLON.
%token IDENTIFIER INTEGER HEX_LITERAL.

%start_symbol input

input ::= frames.

frames ::= frame.
frames ::= frames frame.

frame ::= frame_header LBRACE frame_fields RBRACE. {
  tr_text_protocol_finish_frame(ctx);
}

frame_header(A) ::= FRAME VERSION INTEGER(V) KIND IDENTIFIER(K). {
  A = V;
  tr_text_protocol_begin_frame(ctx, V, K);
}

frame_fields ::= .
frame_fields ::= frame_fields frame_field.

frame_field ::= FROM ASSIGN INTEGER(V) SEMICOLON. {
  tr_text_protocol_set_value(ctx, TR_TEXT_PROTOCOL_VALUE_FROM, V);
}
frame_field ::= TO ASSIGN INTEGER(V) SEMICOLON. {
  tr_text_protocol_set_value(ctx, TR_TEXT_PROTOCOL_VALUE_TO, V);
}
frame_field ::= TERM ASSIGN INTEGER(V) SEMICOLON. {
  tr_text_protocol_set_value(ctx, TR_TEXT_PROTOCOL_VALUE_TERM, V);
}
frame_field ::= MESSAGE ASSIGN IDENTIFIER(M) SEMICOLON. {
  tr_text_protocol_set_message(ctx, M);
}
frame_field ::= PAYLOAD ASSIGN HEX_LITERAL(P) SEMICOLON. {
  tr_text_protocol_set_payload(ctx, P);
}

%syntax_error {
  tr_text_parse_context_fail(&ctx->base, SALTS_EPROTO,
                             TR_TEXT_DIAGNOSTIC_SYNTAX,
                             "protocol debug syntax error");
}

%parse_failure {
  tr_text_parse_context_fail(&ctx->base, SALTS_EPROTO,
                             TR_TEXT_DIAGNOSTIC_SYNTAX,
                             "protocol debug parse failure");
}
