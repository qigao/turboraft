#include <turboraft/raft_control_audit.h>

#include <tinytest.h>
#include <turbo_error.h>

#include <string.h>

typedef struct audit_capture {
    int result;
    size_t calls;
    tr_raft_control_audit_event_t event;
} audit_capture_t;

static int capture_event(void *context,
                         const tr_raft_control_audit_event_t *event)
{
    audit_capture_t *capture = (audit_capture_t *)context;

    capture->calls += 1U;
    capture->event = *event;
    return capture->result;
}

spec("raft control audit boundary")
{
    it("emits fixed metadata without accepting raw payload fields")
    {
        tr_raft_control_audit_t *audit = NULL;
        audit_capture_t capture;
        tr_raft_control_audit_config_t config;
        tr_raft_control_audit_event_t event;

        memset(&capture, 0, sizeof(capture));
        memset(&config, 0, sizeof(config));
        config.version = TR_RAFT_CONTROL_AUDIT_VERSION;
        config.size = sizeof(config);
        config.sink = capture_event;
        config.context = &capture;
        config.required = true;
        check_int_eq(tr_raft_control_audit_create(&config, &audit), TURBO_OK);

        memset(&event, 0, sizeof(event));
        event.method = TR_RAFT_CONTROL_AUDIT_ADD_LEARNER;
        event.phase = TR_RAFT_CONTROL_AUDIT_ACCEPTANCE;
        event.local_node_id = 1U;
        event.target_node_id = 4U;
        event.term = 7U;
        event.index = 19U;
        event.principal_fingerprint[0] = 0xA5U;
        check_int_eq(tr_raft_control_audit_emit(audit, &event), TURBO_OK);

        check_size_eq(capture.calls, 1U);
        check_uint_eq(capture.event.version,
                      TR_RAFT_CONTROL_AUDIT_VERSION);
        check_uint_eq(capture.event.size, sizeof(capture.event));
        check_long_eq(capture.event.sequence, 1U);
        check_long_eq(capture.event.target_node_id, 4U);
        check_uint_eq(capture.event.principal_fingerprint[0], 0xA5U);
        check_long_eq(tr_raft_control_audit_dropped(audit), 0U);
        tr_raft_control_audit_destroy(audit);
    }

    it("fails closed when required and counts optional drops")
    {
        tr_raft_control_audit_t *audit = NULL;
        audit_capture_t capture;
        tr_raft_control_audit_config_t config;
        tr_raft_control_audit_event_t event;

        memset(&config, 0, sizeof(config));
        config.version = TR_RAFT_CONTROL_AUDIT_VERSION;
        config.size = sizeof(config);
        config.required = true;
        check_int_eq(tr_raft_control_audit_create(&config, &audit),
                     TURBO_EINVAL);
        check_null(audit);

        config.required = false;
        config.reserved[0] = 1U;
        check_int_eq(tr_raft_control_audit_create(&config, &audit),
                     TURBO_EINVAL);
        check_null(audit);
        config.reserved[0] = 0U;

        memset(&capture, 0, sizeof(capture));
        capture.result = TURBO_EINVAL;
        config.sink = capture_event;
        config.context = &capture;
        config.required = false;
        check_int_eq(tr_raft_control_audit_create(&config, &audit), TURBO_OK);
        memset(&event, 0, sizeof(event));
        event.method = TR_RAFT_CONTROL_AUDIT_PROPOSE;
        event.phase = TR_RAFT_CONTROL_AUDIT_ACCEPTANCE;
        check_int_eq(tr_raft_control_audit_emit(audit, &event), TURBO_OK);
        check_long_eq(tr_raft_control_audit_dropped(audit), 1U);
        tr_raft_control_audit_destroy(audit);
        audit = NULL;

        config.required = true;
        check_int_eq(tr_raft_control_audit_create(&config, &audit), TURBO_OK);
        check_int_eq(tr_raft_control_audit_emit(audit, &event),
                     TURBO_EINVAL);
        check_long_eq(tr_raft_control_audit_dropped(audit), 0U);
        tr_raft_control_audit_destroy(audit);
    }

    it("validates typed method and phase values")
    {
        tr_raft_control_audit_t *audit = NULL;
        tr_raft_control_audit_config_t config;
        tr_raft_control_audit_event_t event;

        memset(&config, 0, sizeof(config));
        config.version = TR_RAFT_CONTROL_AUDIT_VERSION;
        config.size = sizeof(config);
        check_int_eq(tr_raft_control_audit_create(&config, &audit), TURBO_OK);
        memset(&event, 0, sizeof(event));
        event.method = 0U;
        event.phase = TR_RAFT_CONTROL_AUDIT_AUTHORIZATION;
        check_int_eq(tr_raft_control_audit_emit(audit, &event),
                     TURBO_EINVAL);
        event.method = TR_RAFT_CONTROL_AUDIT_TICK;
        event.phase = 0U;
        check_int_eq(tr_raft_control_audit_emit(audit, &event),
                     TURBO_EINVAL);
        tr_raft_control_audit_destroy(audit);
    }
}
