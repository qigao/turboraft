#include <turboraft/text_query_service.h>
#include <turboraft/text_query_executor.h>

#include <stdbool.h>

typedef struct tr_text_query_service_context {
  tr_raft_service_t *service;
  const tr_text_query_service_sink_t *sink;
  void *sink_context;
} tr_text_query_service_context_t;

static int tr_text_query_service_status(
    void *context,
    const tr_text_query_command_t *command)
{
  tr_text_query_service_context_t *driver =
      (tr_text_query_service_context_t *)context;
  tr_raft_service_status_t status;
  int result;

  result = tr_raft_service_status(driver->service, &status);
  if (result != TURBO_OK) {
    return result;
  }
  return driver->sink->status(driver->sink_context, command, &status);
}

static bool tr_text_query_service_member_matches(
    tr_text_query_role_t role,
    const tr_raft_conf_member_t *member)
{
  switch (role) {
    case TR_TEXT_QUERY_ROLE_ANY:
      return true;
    case TR_TEXT_QUERY_ROLE_VOTER:
      return (member->roles &
              (TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER)) != 0u;
    case TR_TEXT_QUERY_ROLE_LEARNER:
      return (member->roles & TR_RAFT_CONF_LEARNER) != 0u;
    default:
      return false;
  }
}

static int tr_text_query_service_members(
    void *context,
    const tr_text_query_command_t *command)
{
  tr_text_query_service_context_t *driver =
      (tr_text_query_service_context_t *)context;
  tr_raft_conf_t configuration;
  size_t member_index;
  int result;

  result = tr_raft_service_configuration(driver->service, &configuration);
  if (result != TURBO_OK) {
    return result;
  }
  if (configuration.member_count > TR_RAFT_MAX_MEMBERS) {
    return TURBO_EPROTO;
  }

  for (member_index = 0u; member_index < configuration.member_count;
       ++member_index) {
    const tr_raft_conf_member_t *member = &configuration.members[member_index];

    if (!tr_text_query_service_member_matches(command->role, member)) {
      continue;
    }
    result = driver->sink->member(
        driver->sink_context, command, &configuration, member);
    if (result != TURBO_OK) {
      return result;
    }
  }

  return TURBO_OK;
}

static int tr_text_query_service_progress(
    void *context,
    const tr_text_query_command_t *command)
{
  tr_text_query_service_context_t *driver =
      (tr_text_query_service_context_t *)context;
  tr_raft_progress_view_t progress;
  size_t peer_index;
  int result;

  result = tr_raft_service_progress(driver->service, &progress);
  if (result != TURBO_OK) {
    return result;
  }
  if (progress.peer_count > TR_RAFT_MAX_MEMBERS) {
    return TURBO_EPROTO;
  }

  for (peer_index = 0u; peer_index < progress.peer_count; ++peer_index) {
    const tr_raft_peer_progress_t *peer = &progress.peers[peer_index];

    if (peer->node_id != command->node_id) {
      continue;
    }
    return driver->sink->progress(
        driver->sink_context, command, &progress, peer);
  }

  return TURBO_ENOENT;
}

int tr_text_query_execute_service(
    tr_raft_service_t *service,
    const tr_text_query_plan_t *plan,
    const tr_text_query_service_sink_t *sink,
    void *context)
{
  tr_text_query_service_context_t driver;
  tr_text_query_executor_ops_t ops;

  if (service == NULL || plan == NULL || sink == NULL) {
    return TURBO_EINVAL;
  }
  if (sink->status == NULL || sink->member == NULL ||
      sink->progress == NULL) {
    return TURBO_ENOTSUP;
  }

  driver.service = service;
  driver.sink = sink;
  driver.sink_context = context;

  ops.status = tr_text_query_service_status;
  ops.members = tr_text_query_service_members;
  ops.progress = tr_text_query_service_progress;
  return tr_text_query_execute(plan, &ops, &driver);
}
