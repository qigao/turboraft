#include <turboraft/raft_coronet_peer_service.h>

#include <tinytest.h>
#include <turbo_error.h>

#include <stdint.h>
#include <string.h>

typedef struct peer_service_test_context {
    tr_raft_coronet_peer_service_t *service;
    int inbound_result_count;
    int destroy_result;
    tr_raft_node_id_t inbound_peer_node_id;
    tr_raft_node_id_t outbound_identity_node_id;
} peer_service_test_context_t;

static tr_raft_coronet_identity_entry_t service_identity(char hex_digit,
                                                         tr_raft_node_id_t node_id)
{
    tr_raft_coronet_identity_entry_t entry;

    memset(&entry, 0, sizeof(entry));
    memcpy(entry.certificate_sha256, "sha256:", 7U);
    memset(entry.certificate_sha256 + 7U, hex_digit, 64U);
    entry.node_id = node_id;
    return entry;
}

static int service_ignore_message(void *context,
                                  const tr_raft_message_t *message)
{
    (void) context;
    (void) message;
    return TURBO_OK;
}

static int service_fake_admit(
    tr_raft_coronet_peer_manager_t *manager,
    coro_socket_t *socket,
    const tr_raft_coronet_owned_socket_admission_config_t *config,
    tr_raft_node_id_t *out_peer_node_id)
{
    static const char fingerprint[] =
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    (void) manager;
    (void) socket;
    return config->handshake.resolve_peer_identity(
        config->handshake.identity_context, fingerprint, out_peer_node_id);
}

static void service_collect_inbound(void *context,
                                    int result,
                                    tr_raft_node_id_t peer_node_id)
{
    peer_service_test_context_t *test =
        (peer_service_test_context_t *) context;

    ++test->inbound_result_count;
    test->inbound_peer_node_id = peer_node_id;
    test->destroy_result = tr_raft_coronet_peer_service_destroy(test->service);
    check_equal(result, TURBO_OK);
}

static int service_resolve_endpoint(
    void *context,
    tr_raft_node_id_t peer_node_id,
    tr_raft_coronet_endpoint_t *out_endpoint)
{
    static const char connect_host[] = "100.64.0.3";
    static const char request_host[] = "node-3.mesh";

    (void) context;
    if (peer_node_id != 3U) {
        return TURBO_EPROTO;
    }
    memcpy(out_endpoint->connect_host, connect_host, sizeof(connect_host));
    memcpy(out_endpoint->request_host, request_host, sizeof(request_host));
    out_endpoint->port = 7443;
    return TURBO_OK;
}

static int service_fake_connect(
    tr_raft_coronet_peer_manager_t *manager,
    const tr_raft_coronet_outbound_config_t *config,
    tr_raft_node_id_t *out_peer_node_id)
{
    static const char fingerprint[] =
        "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    peer_service_test_context_t *test =
        (peer_service_test_context_t *) config->admission.message_context;
    int result;

    (void) manager;
    result = config->admission.handshake.resolve_peer_identity(
        config->admission.handshake.identity_context, fingerprint,
        &test->outbound_identity_node_id);
    if (result != TURBO_OK) {
        return result;
    }
    *out_peer_node_id = config->admission.expected_peer_node_id;
    return TURBO_OK;
}

static int service_retry_io(void *context, int error_code)
{
    (void) context;
    return error_code == TURBO_EIO;
}

spec("raft CoroNet peer service")
{
    it("orchestrates identity, inbound, outbound, and quiescent shutdown")
    {
        const tr_raft_node_id_t peer_node_ids[] = {1U, 3U};
        tr_raft_coronet_identity_entry_t identities[2];
        tr_raft_coronet_peer_service_config_t service_config;
        tr_raft_coronet_inbound_service_config_t inbound_config;
        tr_raft_coronet_dial_scheduler_config_t dial_config;
        tr_raft_coronet_peer_service_step_result_t step_result;
        tr_raft_coronet_peer_service_status_t status;
        tr_raft_coronet_peer_service_t *service = NULL;
        coro_context_t *coro_context = NULL;
        peer_service_test_context_t test;

        memset(&service_config, 0, sizeof(service_config));
        memset(&inbound_config, 0, sizeof(inbound_config));
        memset(&dial_config, 0, sizeof(dial_config));
        memset(&step_result, 0, sizeof(step_result));
        memset(&status, 0, sizeof(status));
        memset(&test, 0, sizeof(test));
        identities[0] = service_identity('a', 1U);
        identities[1] = service_identity('b', 3U);
        coro_context = coro_context_create(NULL);
        check_not_null(coro_context);
        service_config.context = coro_context;
        service_config.outbound_queue_capacity = 4U;
        service_config.manager.local_node_id = 2U;
        service_config.manager.peer_node_ids = peer_node_ids;
        service_config.manager.peer_count = 2U;
        memset(service_config.manager.cluster_id.bytes, 1,
               sizeof(service_config.manager.cluster_id.bytes));
        service_config.identity_entries = identities;
        service_config.identity_entry_count = 2U;
        service_config.admit_owned_socket = service_fake_admit;
        service_config.connect_outbound = service_fake_connect;
        check_equal(tr_raft_coronet_peer_service_create(&service_config,
                                                          &service),
                     TURBO_OK);
        test.service = service;

        inbound_config.admission.handshake.handshake =
            (const tr_raft_handshake_config_t *) (uintptr_t) 1;
        inbound_config.admission.handshake.timeout_ms = 3000U;
        inbound_config.admission.direction =
            TR_RAFT_CORONET_CONNECTION_INBOUND;
        inbound_config.admission.first_outbound_message_id = 1U;
        inbound_config.admission.peer_idle_timeout_ms = 10000U;
        inbound_config.admission.on_message = service_ignore_message;
        inbound_config.admission.message_context = &test;
        inbound_config.on_result = service_collect_inbound;
        inbound_config.result_context = &test;
        check_equal(tr_raft_coronet_peer_service_configure_inbound(
                         service, &inbound_config),
                     TURBO_OK);
        tr_raft_coronet_peer_service_handle_inbound(
            (coro_socket_t *) (uintptr_t) 1, service);
        check_equal(test.inbound_result_count, 1);
        check_equal(test.inbound_peer_node_id, 1);
        check_equal(test.destroy_result, TURBO_EBUSY);

        {
            tr_raft_message_t queued_message;
            size_t queue_index;

            check_equal(tr_raft_coronet_peer_service_get_status(
                             service, &status),
                         TURBO_OK);
            memset(&queued_message, 0, sizeof(queued_message));
            queued_message.type = TR_RAFT_MSG_HEARTBEAT_REQUEST;
            queued_message.from = 2U;
            queued_message.to = 3U;
            for (queue_index = 0U;
                 queue_index < status.outbound_queue_capacity;
                 ++queue_index) {
                queued_message.term = queue_index + 1U;
                check_equal(tr_raft_coronet_peer_service_enqueue(
                                 service, &queued_message),
                             TURBO_OK);
            }
            queued_message.term = status.outbound_queue_capacity + 1U;
            check_equal(tr_raft_coronet_peer_service_enqueue(
                         service, &queued_message),
                     TURBO_ENOSPC);
            {
                tr_raft_coronet_payload_t snapshot_ack;

                memset(&snapshot_ack, 0, sizeof(snapshot_ack));
                snapshot_ack.kind = TR_RAFT_WIRE_PAYLOAD_SNAPSHOT_ACK;
                snapshot_ack.data.snapshot_ack.from = 2U;
                snapshot_ack.data.snapshot_ack.to = 3U;
                snapshot_ack.data.snapshot_ack.term = 1U;
                snapshot_ack.data.snapshot_ack.snapshot_index = 1U;
                check_equal(tr_raft_coronet_peer_service_enqueue_payload(
                                 service, &snapshot_ack),
                             TURBO_ENOSPC);
            }
            check_equal(tr_raft_coronet_peer_service_get_status(
                             service, &status),
                         TURBO_OK);
            check_equal(status.queued_message_count,
                          status.outbound_queue_capacity);
            check_equal(status.queued_payload_count,
                          status.outbound_queue_capacity);
            coro_context_run(coro_context, TURBO_RUN_NOWAIT);
            check_equal(tr_raft_coronet_peer_service_get_status(
                             service, &status),
                         TURBO_OK);
            check_equal(status.queued_message_count,
                          status.outbound_queue_capacity);
        }

        dial_config.outbound.context = (coro_context_t *) (uintptr_t) 1;
        dial_config.outbound.connect_timeout_ms = 5000U;
        dial_config.outbound.tls.verify_peer = 1;
        dial_config.outbound.admission.handshake.handshake =
            (const tr_raft_handshake_config_t *) (uintptr_t) 1;
        dial_config.outbound.admission.handshake.timeout_ms = 3000U;
        dial_config.outbound.admission.direction =
            TR_RAFT_CORONET_CONNECTION_OUTBOUND;
        dial_config.outbound.admission.expected_peer_node_id = 3U;
        dial_config.outbound.admission.first_outbound_message_id = 1U;
        dial_config.outbound.admission.peer_idle_timeout_ms = 10000U;
        dial_config.outbound.admission.on_message = service_ignore_message;
        dial_config.outbound.admission.message_context = &test;
        dial_config.resolve_endpoint = service_resolve_endpoint;
        dial_config.connect_outbound = NULL;
        dial_config.is_retryable = service_retry_io;
        dial_config.initial_retry_delay_ms = 10U;
        dial_config.max_retry_delay_ms = 20U;
        dial_config.max_attempts = 3U;
        check_equal(tr_raft_coronet_peer_service_add_outbound(
                         service, &dial_config),
                     TURBO_OK);
        check_equal(tr_raft_coronet_peer_service_step(service, 100U,
                                                        &step_result),
                     TURBO_OK);
        check_equal(step_result.scheduler_count, 1);
        check_equal(step_result.attempted_count, 1);
        check_equal(step_result.newly_connected_count, 1);
        check_equal(step_result.failed_count, 0);
        check_equal(test.outbound_identity_node_id, 3);
        coro_context_run(coro_context, TURBO_RUN_NOWAIT);

        identities[0] = service_identity('a', 3U);
        identities[1] = service_identity('b', 1U);
        check_equal(tr_raft_coronet_peer_service_update_identities(
                         service, identities, 2U),
                     TURBO_OK);
        tr_raft_coronet_peer_service_handle_inbound(
            (coro_socket_t *) (uintptr_t) 1, service);
        check_equal(test.inbound_peer_node_id, 3);
        check_equal(tr_raft_coronet_peer_service_get_status(service, &status),
                     TURBO_OK);
        check_equal(status.peer_count, 2);
        check_equal(status.scheduler_count, 1);
        check_equal(status.identity_generation, 2U);
        check_equal(status.inbound_configured, 1);
        coro_context_run(coro_context, TURBO_RUN_NOWAIT);
        check_equal(tr_raft_coronet_peer_service_destroy(service), TURBO_OK);
        coro_context_destroy(coro_context);
    }
}
