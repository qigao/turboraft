#ifndef TURBORAFT_TEXT_REPLAY_EXECUTOR_H
#define TURBORAFT_TEXT_REPLAY_EXECUTOR_H

#include <turboraft/text_syntax.h>

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  uint64_t term;
  uint64_t index;
} tr_text_replay_receipt_t;

/*
 * The action and payload views are borrowed for the duration of the callback.
 * A callback that needs them after returning must copy them itself.
 */
typedef int (*tr_text_replay_submit_fn)(
    void *context,
    const tr_text_replay_action_t *action,
    const uint8_t *payload,
    size_t payload_size,
    tr_text_replay_receipt_t *out_receipt);

/*
 * The receipt is owned by the executor and remains valid only for this
 * callback. The callback receives the poll timeout exactly as written in the
 * action; the executor never advances time while polling.
 */
typedef int (*tr_text_replay_poll_fn)(
    void *context,
    const tr_text_replay_action_t *action,
    const tr_text_replay_receipt_t *receipt);

/*
 * Tick is the only executor operation that may advance a replay clock.
 */
typedef int (*tr_text_replay_tick_fn)(void *context, uint64_t ticks);

typedef struct {
  tr_text_replay_submit_fn submit;
  tr_text_replay_poll_fn poll;
  tr_text_replay_tick_fn tick;
} tr_text_replay_executor_ops_t;

/*
 * Executes submit, poll, and tick actions synchronously in plan order.
 *
 * Request ids are unique within one execution and are mapped to the receipt
 * returned by submit. Duplicate request ids return SALTS_EALREADY and polls
 * for unknown request ids return SALTS_ENOENT. The first callback failure is
 * returned unchanged and stops execution. Other replay action kinds are
 * rejected with SALTS_ENOTSUP until a runtime-specific driver handles them.
 *
 * The function is single-threaded with respect to the supplied callbacks.
 * Payloads are decoded into bounded temporary storage and are not retained by
 * the executor after submit returns.
 */
int tr_text_replay_execute(
    const tr_text_replay_plan_t *plan,
    const tr_text_replay_executor_ops_t *ops,
    void *context);

#ifdef __cplusplus
}
#endif

#endif
