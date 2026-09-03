#include <turboraft/raft_core.h>
#include <turboraft/text_replay_executor.h>

#include <stdbool.h>
#include <string.h>

typedef struct {
  bool occupied;
  uint64_t request_id;
  tr_text_replay_receipt_t receipt;
} tr_text_replay_receipt_slot_t;

static int tr_text_replay_hex_value(char value)
{
  if (value >= '0' && value <= '9') {
    return (int)(value - '0');
  }
  if (value >= 'a' && value <= 'f') {
    return (int)(value - 'a') + 10;
  }
  if (value >= 'A' && value <= 'F') {
    return (int)(value - 'A') + 10;
  }
  return -1;
}

static int tr_text_replay_decode_payload(
    vstr payload,
    uint8_t *output,
    size_t output_capacity,
    size_t *output_size)
{
  size_t hex_digits;
  size_t decoded_size;
  size_t index;

  if (output == NULL || output_size == NULL || payload.data == NULL ||
      payload.len < 3u || payload.data[0] != '0' ||
      (payload.data[1] != 'x' && payload.data[1] != 'X')) {
    return SALTS_EPROTO;
  }
  hex_digits = payload.len - 2u;
  if ((hex_digits & 1u) != 0u) {
    return SALTS_EPROTO;
  }
  decoded_size = hex_digits / 2u;
  if (decoded_size > output_capacity) {
    return SALTS_ENOSPC;
  }
  for (index = 0u; index < decoded_size; ++index) {
    int high = tr_text_replay_hex_value(payload.data[2u + index * 2u]);
    int low = tr_text_replay_hex_value(payload.data[3u + index * 2u]);
    if (high < 0 || low < 0) {
      return SALTS_EPROTO;
    }
    output[index] = (uint8_t)((high << 4) | low);
  }
  *output_size = decoded_size;
  return SALTS_OK;
}

static bool tr_text_replay_find_receipt(
    const tr_text_replay_receipt_slot_t *slots,
    size_t slot_count,
    uint64_t request_id,
    size_t *slot_index)
{
  size_t index;

  for (index = 0u; index < slot_count; ++index) {
    if (slots[index].occupied && slots[index].request_id == request_id) {
      if (slot_index != NULL) {
        *slot_index = index;
      }
      return true;
    }
  }
  return false;
}

static bool tr_text_replay_find_free_slot(
    const tr_text_replay_receipt_slot_t *slots,
    size_t slot_count,
    size_t *slot_index)
{
  size_t index;

  for (index = 0u; index < slot_count; ++index) {
    if (!slots[index].occupied) {
      *slot_index = index;
      return true;
    }
  }
  return false;
}

int tr_text_replay_execute(
    const tr_text_replay_plan_t *plan,
    const tr_text_replay_executor_ops_t *ops,
    void *context)
{
  tr_text_replay_receipt_slot_t receipts[TR_TEXT_MAX_STATEMENTS];
  uint8_t payload[TR_RAFT_MAX_ENTRY_BYTES];
  size_t action_index;

  if (plan == NULL || ops == NULL ||
      plan->action_count > TR_TEXT_MAX_STATEMENTS) {
    return SALTS_EINVAL;
  }
  memset(receipts, 0, sizeof(receipts));

  for (action_index = 0u; action_index < plan->action_count;
       ++action_index) {
    const tr_text_replay_action_t *action = &plan->actions[action_index];
    int result;

    switch (action->kind) {
      case TR_TEXT_REPLAY_SUBMIT: {
        tr_text_replay_receipt_t receipt;
        size_t payload_size;
        size_t slot_index;

        if (ops->submit == NULL) {
          return SALTS_ENOTSUP;
        }
        if (tr_text_replay_find_receipt(
                receipts, TR_TEXT_MAX_STATEMENTS, action->request_id, NULL)) {
          return SALTS_EALREADY;
        }
        result = tr_text_replay_decode_payload(
            action->payload_hex, payload, sizeof(payload), &payload_size);
        if (result != SALTS_OK) {
          return result;
        }
        memset(&receipt, 0, sizeof(receipt));
        result = ops->submit(context, action, payload, payload_size, &receipt);
        if (result != SALTS_OK) {
          return result;
        }
        if (!tr_text_replay_find_free_slot(
                receipts, TR_TEXT_MAX_STATEMENTS, &slot_index)) {
          return SALTS_ENOSPC;
        }
        receipts[slot_index].occupied = true;
        receipts[slot_index].request_id = action->request_id;
        receipts[slot_index].receipt = receipt;
        break;
      }
      case TR_TEXT_REPLAY_POLL: {
        size_t slot_index;

        if (ops->poll == NULL) {
          return SALTS_ENOTSUP;
        }
        if (!tr_text_replay_find_receipt(
                receipts, TR_TEXT_MAX_STATEMENTS, action->request_id,
                &slot_index)) {
          return SALTS_ENOENT;
        }
        result = ops->poll(context, action,
                           &receipts[slot_index].receipt);
        if (result != SALTS_OK) {
          return result;
        }
        break;
      }
      case TR_TEXT_REPLAY_TICK:
        if (ops->tick == NULL) {
          return SALTS_ENOTSUP;
        }
        result = ops->tick(context, action->value);
        if (result != SALTS_OK) {
          return result;
        }
        break;
      default:
        return SALTS_ENOTSUP;
    }
  }
  return SALTS_OK;
}
