#ifndef TURBORAFT_TEXT_QUERY_SERVICE_H
#define TURBORAFT_TEXT_QUERY_SERVICE_H

#include <turboraft/raft_service.h>
#include <turboraft/text_syntax.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef int (*tr_text_query_service_status_fn)(
    void *context,
    const tr_text_query_command_t *command,
    const tr_raft_service_status_t *status);

typedef int (*tr_text_query_service_member_fn)(
    void *context,
    const tr_text_query_command_t *command,
    const tr_raft_conf_t *configuration,
    const tr_raft_conf_member_t *member);

typedef int (*tr_text_query_service_progress_fn)(
    void *context,
    const tr_text_query_command_t *command,
    const tr_raft_progress_view_t *progress,
    const tr_raft_peer_progress_t *peer);

typedef struct tr_text_query_service_sink {
  tr_text_query_service_status_fn status;
  tr_text_query_service_member_fn member;
  tr_text_query_service_progress_fn progress;
} tr_text_query_service_sink_t;

/*
 * Executes a query plan against a single-owner Raft Service.
 *
 * `status` receives one service status snapshot. `member` receives one call
 * for each configuration member selected by the command role. `progress`
 * receives one call for the requested node, or SALTS_ENOENT when that node is
 * not present in the progress view.
 *
 * Snapshots and their nested views are borrowed until the sink callback
 * returns. The service must be quiescent with respect to other callers for
 * the duration of this synchronous operation. No time, Raft state, or read
 * barrier is advanced by this function.
 */
int tr_text_query_execute_service(
    tr_raft_service_t *service,
    const tr_text_query_plan_t *plan,
    const tr_text_query_service_sink_t *sink,
    void *context);

#ifdef __cplusplus
}
#endif

#endif
