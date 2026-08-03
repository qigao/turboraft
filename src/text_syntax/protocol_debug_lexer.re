// re2c $INPUT -o $OUTPUT
#include "text_syntax_internal.h"
#include "turboraft_text_protocol_debug_grammar_gen.h"

#include <string.h>

static int tr_text_protocol_keyword(const char *value, size_t length) {
#define TR_TEXT_PROTOCOL_KW(text, token)                                   \
  if (length == sizeof(text) - 1u &&                                       \
      memcmp(value, text, sizeof(text) - 1u) == 0) {                       \
    return token;                                                          \
  }
  TR_TEXT_PROTOCOL_KW("frame", TR_TEXT_PROTOCOL_DEBUG_TOKEN_FRAME)
  TR_TEXT_PROTOCOL_KW("version", TR_TEXT_PROTOCOL_DEBUG_TOKEN_VERSION)
  TR_TEXT_PROTOCOL_KW("kind", TR_TEXT_PROTOCOL_DEBUG_TOKEN_KIND)
  TR_TEXT_PROTOCOL_KW("from", TR_TEXT_PROTOCOL_DEBUG_TOKEN_FROM)
  TR_TEXT_PROTOCOL_KW("to", TR_TEXT_PROTOCOL_DEBUG_TOKEN_TO)
  TR_TEXT_PROTOCOL_KW("term", TR_TEXT_PROTOCOL_DEBUG_TOKEN_TERM)
  TR_TEXT_PROTOCOL_KW("message", TR_TEXT_PROTOCOL_DEBUG_TOKEN_MESSAGE)
  TR_TEXT_PROTOCOL_KW("payload", TR_TEXT_PROTOCOL_DEBUG_TOKEN_PAYLOAD)
#undef TR_TEXT_PROTOCOL_KW
  return TR_TEXT_PROTOCOL_DEBUG_TOKEN_IDENTIFIER;
}

void tr_text_protocol_debug_lexer_init(tr_text_lexer_t *lexer,
                                       const char *input,
                                       size_t input_length) {
  tr_text_lexer_init(lexer, input, input_length);
}

int tr_text_protocol_debug_lexer_next(tr_text_lexer_t *lexer,
                                      tr_text_token_t *token) {
  const char *YYCURSOR;
  const char *YYMARKER;
  const char *YYLIMIT;
  const char *start;
  if (lexer == NULL || token == NULL) {
    return -1;
  }
  YYCURSOR = lexer->cursor;
  YYMARKER = YYCURSOR;
  YYLIMIT = lexer->limit;
again:
  memset(token, 0, sizeof(*token));
  start = YYCURSOR;
  token->offset = (size_t)(start - lexer->input);
  token->line = lexer->line;
  token->column = lexer->column;
  /*!re2c
    re2c:define:YYCTYPE = "unsigned char";
    re2c:yyfill:enable = 0;
    re2c:eof = 0;

    ws = [\x09\x0a\x0d\x20]+;
    line_comment = "#" [^\x0a\x0d]*;
    ident = [A-Za-z_][A-Za-z0-9_]*;
    hex_literal = "0" [xX] [0-9A-Fa-f]+;
    number = [0-9]+;

    $ {
      lexer->cursor = YYCURSOR;
      return 0;
    }
    ws {
      tr_text_lexer_advance(lexer, start, YYCURSOR);
      lexer->cursor = YYCURSOR;
      goto again;
    }
    line_comment {
      tr_text_lexer_advance(lexer, start, YYCURSOR);
      lexer->cursor = YYCURSOR;
      goto again;
    }
    "=" {
      tr_text_token_set(token, lexer, start, YYCURSOR);
      lexer->cursor = YYCURSOR;
      tr_text_lexer_advance(lexer, start, YYCURSOR);
      return TR_TEXT_PROTOCOL_DEBUG_TOKEN_ASSIGN;
    }
    "{" {
      tr_text_token_set(token, lexer, start, YYCURSOR);
      lexer->cursor = YYCURSOR;
      tr_text_lexer_advance(lexer, start, YYCURSOR);
      return TR_TEXT_PROTOCOL_DEBUG_TOKEN_LBRACE;
    }
    "}" {
      tr_text_token_set(token, lexer, start, YYCURSOR);
      lexer->cursor = YYCURSOR;
      tr_text_lexer_advance(lexer, start, YYCURSOR);
      return TR_TEXT_PROTOCOL_DEBUG_TOKEN_RBRACE;
    }
    ";" {
      tr_text_token_set(token, lexer, start, YYCURSOR);
      lexer->cursor = YYCURSOR;
      tr_text_lexer_advance(lexer, start, YYCURSOR);
      return TR_TEXT_PROTOCOL_DEBUG_TOKEN_SEMICOLON;
    }
    hex_literal {
      tr_text_token_set(token, lexer, start, YYCURSOR);
      lexer->cursor = YYCURSOR;
      tr_text_lexer_advance(lexer, start, YYCURSOR);
      return TR_TEXT_PROTOCOL_DEBUG_TOKEN_HEX_LITERAL;
    }
    number {
      tr_text_token_set(token, lexer, start, YYCURSOR);
      if (tr_text_parse_uint64(token->text.data, token->text.len,
                               &token->number) != TURBO_OK) {
        tr_text_lexer_set_error(lexer, TURBO_ERANGE,
                                "integer literal out of range");
        lexer->cursor = YYCURSOR;
        return -1;
      }
      lexer->cursor = YYCURSOR;
      tr_text_lexer_advance(lexer, start, YYCURSOR);
      return TR_TEXT_PROTOCOL_DEBUG_TOKEN_INTEGER;
    }
    ident {
      tr_text_token_set(token, lexer, start, YYCURSOR);
      lexer->cursor = YYCURSOR;
      tr_text_lexer_advance(lexer, start, YYCURSOR);
      return tr_text_protocol_keyword(token->text.data, token->text.len);
    }
    * {
      tr_text_token_set(token, lexer, start, YYCURSOR);
      tr_text_lexer_set_error(lexer, TURBO_EPROTO,
                              "invalid protocol debug character");
      lexer->cursor = YYCURSOR;
      tr_text_lexer_advance(lexer, start, YYCURSOR);
      return -1;
    }
  */
}
