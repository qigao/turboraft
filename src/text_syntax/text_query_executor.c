#include <turboraft/text_query_executor.h>

int tr_text_query_execute(
    const tr_text_query_plan_t *plan,
    const tr_text_query_executor_ops_t *ops,
    void *context)
{
  size_t command_index;

  if (plan == NULL || ops == NULL ||
      plan->command_count > TR_TEXT_MAX_STATEMENTS) {
    return TURBO_EINVAL;
  }

  for (command_index = 0u; command_index < plan->command_count;
       ++command_index) {
    const tr_text_query_command_t *command = &plan->commands[command_index];
    int result;

    switch (command->kind) {
      case TR_TEXT_QUERY_SHOW_STATUS:
        if (ops->status == NULL) {
          return TURBO_ENOTSUP;
        }
        result = ops->status(context, command);
        break;
      case TR_TEXT_QUERY_SHOW_MEMBERS:
        if (ops->members == NULL) {
          return TURBO_ENOTSUP;
        }
        result = ops->members(context, command);
        break;
      case TR_TEXT_QUERY_SHOW_PROGRESS:
        if (ops->progress == NULL) {
          return TURBO_ENOTSUP;
        }
        result = ops->progress(context, command);
        break;
      default:
        return TURBO_ENOTSUP;
    }

    if (result != TURBO_OK) {
      return result;
    }
  }

  return TURBO_OK;
}
