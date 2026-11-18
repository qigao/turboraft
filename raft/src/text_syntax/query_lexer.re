// re2c $INPUT -o $OUTPUT
#include "text_syntax_internal.h"
#include "turboraft_text_query_grammar_gen.h"

#include <string.h>

static int tr_text_query_keyword(const char *value, size_t length) {
#define TR_TEXT_QUERY_KW(text, token)                                      \
  if (length == sizeof(text) - 1u &&                                       \
      memcmp(value, text, sizeof(text) - 1u) == 0) {                       \
    return token;                                                          \
  }
  TR_TEXT_QUERY_KW("show", TR_TEXT_QUERY_TOKEN_SHOW)
  TR_TEXT_QUERY_KW("status", TR_TEXT_QUERY_TOKEN_STATUS)
  TR_TEXT_QUERY_KW("members", TR_TEXT_QUERY_TOKEN_MEMBERS)
  TR_TEXT_QUERY_KW("where", TR_TEXT_QUERY_TOKEN_WHERE)
  TR_TEXT_QUERY_KW("role", TR_TEXT_QUERY_TOKEN_ROLE)
  TR_TEXT_QUERY_KW("voter", TR_TEXT_QUERY_TOKEN_VOTER)
  TR_TEXT_QUERY_KW("learner", TR_TEXT_QUERY_TOKEN_LEARNER)
  TR_TEXT_QUERY_KW("progress", TR_TEXT_QUERY_TOKEN_PROGRESS)
  TR_TEXT_QUERY_KW("for", TR_TEXT_QUERY_TOKEN_FOR)
  TR_TEXT_QUERY_KW("node", TR_TEXT_QUERY_TOKEN_NODE)
#undef TR_TEXT_QUERY_KW
  return TR_TEXT_QUERY_TOKEN_IDENTIFIER;
}

void tr_text_query_lexer_init(tr_text_lexer_t *lexer,
                              const char *input,
                              size_t input_length) {
  tr_text_lexer_init(lexer, input, input_length);
}

int tr_text_query_lexer_next(tr_text_lexer_t *lexer, tr_text_token_t *token) {
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
    "==" {
      tr_text_token_set(token, lexer, start, YYCURSOR);
      lexer->cursor = YYCURSOR;
      tr_text_lexer_advance(lexer, start, YYCURSOR);
      return TR_TEXT_QUERY_TOKEN_EQ;
    }
    ";" {
      tr_text_token_set(token, lexer, start, YYCURSOR);
      lexer->cursor = YYCURSOR;
      tr_text_lexer_advance(lexer, start, YYCURSOR);
      return TR_TEXT_QUERY_TOKEN_SEMICOLON;
    }
    number {
      tr_text_token_set(token, lexer, start, YYCURSOR);
      if (tr_text_parse_uint64(token->text.data, token->text.len,
                               &token->number) != SALTS_OK) {
        tr_text_lexer_set_error(lexer, SALTS_ERANGE,
                                "integer literal out of range");
        lexer->cursor = YYCURSOR;
        return -1;
      }
      lexer->cursor = YYCURSOR;
      tr_text_lexer_advance(lexer, start, YYCURSOR);
      return TR_TEXT_QUERY_TOKEN_INTEGER;
    }
    ident {
      tr_text_token_set(token, lexer, start, YYCURSOR);
      lexer->cursor = YYCURSOR;
      tr_text_lexer_advance(lexer, start, YYCURSOR);
      return tr_text_query_keyword(token->text.data, token->text.len);
    }
    * {
      tr_text_token_set(token, lexer, start, YYCURSOR);
      tr_text_lexer_set_error(lexer, SALTS_EPROTO,
                              "invalid query character");
      lexer->cursor = YYCURSOR;
      tr_text_lexer_advance(lexer, start, YYCURSOR);
      return -1;
    }
  */
}
