#ifndef TURBORAFT_TEXT_QUERY_EXECUTOR_H
#define TURBORAFT_TEXT_QUERY_EXECUTOR_H

#include <turboraft/text_syntax.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef int (*tr_text_query_status_fn)(
    void *context,
    const tr_text_query_command_t *command);

typedef int (*tr_text_query_members_fn)(
    void *context,
    const tr_text_query_command_t *command);

typedef int (*tr_text_query_progress_fn)(
    void *context,
    const tr_text_query_command_t *command);

typedef struct tr_text_query_executor_ops {
  tr_text_query_status_fn status;
  tr_text_query_members_fn members;
  tr_text_query_progress_fn progress;
} tr_text_query_executor_ops_t;

/*
 * Executes a parsed query plan synchronously in plan order.
 *
 * The executor only dispatches typed commands. A runtime-specific callback
 * owns the read-only snapshot, role filtering, node lookup, and output
 * formatting. The callback must not retain the command view after returning.
 * The first callback error is returned unchanged and stops execution.
 * A command without a corresponding callback returns TURBO_ENOTSUP.
 */
int tr_text_query_execute(
    const tr_text_query_plan_t *plan,
    const tr_text_query_executor_ops_t *ops,
    void *context);

#ifdef __cplusplus
}
#endif

#endif
