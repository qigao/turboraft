#include <turboraft/raft_coronet_transport.h>

#include <turbo_error.h>

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

static const char tr_raft_coronet_validation_host[] = "scheduler.invalid";

struct tr_raft_coronet_dial_scheduler {
    tr_raft_coronet_dial_scheduler_config_t config;
    tr_raft_coronet_dial_status_t status;
    uint64_t retry_delay_ms;
};

static int tr_raft_coronet_endpoint_validate(
    const tr_raft_coronet_endpoint_t *endpoint)
{
    if (endpoint == NULL || endpoint->connect_host[0] == '\0' ||
        endpoint->request_host[0] == '\0' || endpoint->port <= 0 ||
        endpoint->port > 65535 ||
        memchr(endpoint->connect_host, '\0',
               sizeof(endpoint->connect_host)) == NULL ||
        memchr(endpoint->request_host, '\0',
               sizeof(endpoint->request_host)) == NULL) {
        return TURBO_EINVAL;
    }
    return TURBO_OK;
}

static int tr_raft_coronet_dial_config_validate(
    const tr_raft_coronet_dial_scheduler_config_t *config)
{
    tr_raft_coronet_outbound_config_t outbound;
    int result;

    if (config == NULL || config->manager == NULL ||
        config->resolve_endpoint == NULL || config->connect_outbound == NULL ||
        config->is_retryable == NULL || config->initial_retry_delay_ms == 0U ||
        config->max_retry_delay_ms < config->initial_retry_delay_ms ||
        config->max_attempts == 0U) {
        return TURBO_EINVAL;
    }

    outbound = config->outbound;
    outbound.connect_host = tr_raft_coronet_validation_host;
    outbound.request_host = tr_raft_coronet_validation_host;
    outbound.port = 1;
    result = tr_raft_coronet_outbound_config_validate(&outbound);
    return result;
}

static int tr_raft_coronet_dial_terminal(
    tr_raft_coronet_dial_scheduler_t *scheduler,
    int error_code)
{
    scheduler->status.state = TR_RAFT_CORONET_DIAL_EXHAUSTED;
    scheduler->status.next_attempt_ms = 0U;
    scheduler->status.last_error = error_code;
    return error_code;
}

static int tr_raft_coronet_dial_record_failure(
    tr_raft_coronet_dial_scheduler_t *scheduler,
    uint64_t now_ms,
    int error_code)
{
    if (error_code == TURBO_EINVAL || error_code == TURBO_EPROTO ||
        scheduler->status.attempt_count >= scheduler->config.max_attempts ||
        !scheduler->config.is_retryable(scheduler->config.retry_context,
                                        error_code)) {
        return tr_raft_coronet_dial_terminal(scheduler, error_code);
    }
    if (now_ms > UINT64_MAX - scheduler->retry_delay_ms) {
        return tr_raft_coronet_dial_terminal(scheduler, TURBO_EINVAL);
    }

    scheduler->status.state = TR_RAFT_CORONET_DIAL_WAITING;
    scheduler->status.next_attempt_ms = now_ms + scheduler->retry_delay_ms;
    scheduler->status.last_error = error_code;
    if (scheduler->retry_delay_ms >
        scheduler->config.max_retry_delay_ms / 2U) {
        scheduler->retry_delay_ms = scheduler->config.max_retry_delay_ms;
    } else {
        scheduler->retry_delay_ms *= 2U;
    }
    return error_code;
}

int tr_raft_coronet_dial_scheduler_create(
    const tr_raft_coronet_dial_scheduler_config_t *config,
    tr_raft_coronet_dial_scheduler_t **out_scheduler)
{
    tr_raft_coronet_dial_scheduler_t *scheduler;
    int result;

    if (out_scheduler == NULL) {
        return TURBO_EINVAL;
    }
    *out_scheduler = NULL;
    result = tr_raft_coronet_dial_config_validate(config);
    if (result != TURBO_OK) {
        return result;
    }

    scheduler = (tr_raft_coronet_dial_scheduler_t *) calloc(
        1U, sizeof(*scheduler));
    if (scheduler == NULL) {
        return TURBO_ENOMEM;
    }
    scheduler->config = *config;
    scheduler->config.outbound.connect_host = NULL;
    scheduler->config.outbound.request_host = NULL;
    scheduler->config.outbound.port = 0;
    scheduler->status.state = TR_RAFT_CORONET_DIAL_WAITING;
    scheduler->retry_delay_ms = config->initial_retry_delay_ms;
    *out_scheduler = scheduler;
    return TURBO_OK;
}

void tr_raft_coronet_dial_scheduler_destroy(
    tr_raft_coronet_dial_scheduler_t *scheduler)
{
    free(scheduler);
}

int tr_raft_coronet_dial_scheduler_step(
    tr_raft_coronet_dial_scheduler_t *scheduler,
    uint64_t now_ms)
{
    tr_raft_coronet_outbound_config_t outbound;
    tr_raft_coronet_endpoint_t endpoint;
    tr_raft_node_id_t connected_peer_node_id = 0U;
    tr_raft_node_id_t expected_peer_node_id;
    int result;

    if (scheduler == NULL) {
        return TURBO_EINVAL;
    }
    if (scheduler->status.state != TR_RAFT_CORONET_DIAL_WAITING ||
        now_ms < scheduler->status.next_attempt_ms) {
        return TURBO_EBUSY;
    }

    ++scheduler->status.attempt_count;
    expected_peer_node_id =
        scheduler->config.outbound.admission.expected_peer_node_id;
    memset(&endpoint, 0, sizeof(endpoint));
    result = scheduler->config.resolve_endpoint(
        scheduler->config.resolve_context, expected_peer_node_id, &endpoint);
    if (result != TURBO_OK) {
        return tr_raft_coronet_dial_record_failure(scheduler, now_ms, result);
    }
    result = tr_raft_coronet_endpoint_validate(&endpoint);
    if (result != TURBO_OK) {
        return tr_raft_coronet_dial_terminal(scheduler, result);
    }

    outbound = scheduler->config.outbound;
    outbound.connect_host = endpoint.connect_host;
    outbound.request_host = endpoint.request_host;
    outbound.port = endpoint.port;
    result = scheduler->config.connect_outbound(
        scheduler->config.manager, &outbound, &connected_peer_node_id);
    if (result != TURBO_OK) {
        return tr_raft_coronet_dial_record_failure(scheduler, now_ms, result);
    }
    if (connected_peer_node_id != expected_peer_node_id) {
        return tr_raft_coronet_dial_terminal(scheduler, TURBO_EPROTO);
    }

    scheduler->status.state = TR_RAFT_CORONET_DIAL_CONNECTED;
    scheduler->status.next_attempt_ms = 0U;
    scheduler->status.last_error = TURBO_OK;
    scheduler->retry_delay_ms = scheduler->config.initial_retry_delay_ms;
    return TURBO_OK;
}

int tr_raft_coronet_dial_scheduler_reset(
    tr_raft_coronet_dial_scheduler_t *scheduler,
    uint64_t now_ms)
{
    if (scheduler == NULL) {
        return TURBO_EINVAL;
    }
    scheduler->status.state = TR_RAFT_CORONET_DIAL_WAITING;
    scheduler->status.attempt_count = 0U;
    scheduler->status.next_attempt_ms = now_ms;
    scheduler->status.last_error = TURBO_OK;
    scheduler->retry_delay_ms = scheduler->config.initial_retry_delay_ms;
    return TURBO_OK;
}

int tr_raft_coronet_dial_scheduler_get_status(
    const tr_raft_coronet_dial_scheduler_t *scheduler,
    tr_raft_coronet_dial_status_t *out_status)
{
    if (scheduler == NULL || out_status == NULL) {
        return TURBO_EINVAL;
    }
    *out_status = scheduler->status;
    return TURBO_OK;
}
