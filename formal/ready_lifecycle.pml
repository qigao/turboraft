/*
 * Ready lifecycle contract model.
 *
 * Core state may move ahead of durable state while a Ready is outstanding.
 * The only legal resolution is successful completion or a faulted owner;
 * there is no retry of a partially published external effect.
 */

#define MAX_READY_COMMIT 3
#define READY_MESSAGES 2

#define READY_IDLE 0
#define READY_PREPARED 1
#define READY_DURABLE 2
#define READY_SENT 3
#define READY_APPLIED 4
#define READY_FAULTED 5

byte ready_phase = READY_IDLE;
byte ready_has_storage;
byte ready_has_apply;
byte ready_has_read;
byte ready_message_count;
byte ready_sent_count;

byte core_commit;
byte core_applied;
byte core_configuration;
byte durable_commit;
byte durable_configuration;
byte visible_read;
byte visible_read_index;
byte faulted;

byte snapshot_point;
byte snapshot_point_configuration;
byte snapshot_stored;
byte snapshot_base;

inline check_ready_invariants()
{
    assert(core_applied <= core_commit);
    assert(durable_commit <= core_commit);
    assert(ready_sent_count <= ready_message_count);
    assert(snapshot_base <= core_applied);

    if
    :: ready_phase == READY_IDLE ->
        assert(ready_sent_count == 0);
        assert(ready_message_count == 0)
    :: ready_phase == READY_PREPARED ->
        assert(ready_has_storage || ready_message_count != 0 ||
               ready_has_apply || ready_has_read)
    :: ready_phase == READY_DURABLE ->
        assert(durable_commit == core_commit)
    :: ready_phase == READY_SENT ->
        assert(ready_sent_count == ready_message_count)
    :: ready_phase == READY_APPLIED ->
        assert(core_applied == core_commit)
    :: ready_phase == READY_FAULTED ->
        assert(faulted)
    :: else -> assert(false)
    fi;

    if
    :: visible_read != 0 ->
        assert(ready_phase == READY_IDLE || ready_phase == READY_FAULTED);
        assert(visible_read_index <= core_applied)
    :: else -> skip
    fi;

    if
    :: snapshot_point != 0 ->
        assert(ready_phase == READY_IDLE || ready_phase == READY_FAULTED);
        assert(core_applied == core_commit);
        assert(snapshot_point == core_applied);
        assert(snapshot_point_configuration == core_configuration)
    :: else -> skip
    fi
}

inline prepare_ready()
{
    if
    :: !faulted && ready_phase == READY_IDLE &&
       snapshot_point == 0 &&
       core_commit < MAX_READY_COMMIT ->
        core_commit++;
        core_configuration = core_commit;
        ready_phase = READY_PREPARED;
        ready_has_storage = 1;
        ready_has_apply = 1;
        ready_has_read = 1;
        ready_message_count = READY_MESSAGES;
        ready_sent_count = 0;
        visible_read = 0;
        check_ready_invariants()
    fi
}

inline persist_ready()
{
    if
    :: !faulted && ready_phase == READY_PREPARED && ready_has_storage ->
        durable_commit = core_commit;
        durable_configuration = core_configuration;
        ready_phase = READY_DURABLE;
        check_ready_invariants()
    fi
}

inline fail_persist()
{
    if
    :: !faulted && ready_phase == READY_PREPARED && ready_has_storage ->
        faulted = 1;
        ready_phase = READY_FAULTED;
        assert(core_applied < core_commit || durable_commit < core_commit);
        check_ready_invariants()
    fi
}

inline send_one()
{
    if
    :: !faulted && ready_phase == READY_DURABLE &&
       ready_sent_count < ready_message_count ->
        ready_sent_count++;
        if
        :: ready_sent_count == ready_message_count ->
            ready_phase = READY_SENT
        :: else -> skip
        fi;
        check_ready_invariants()
    fi
}

inline fail_send()
{
    if
    :: !faulted && ready_phase == READY_DURABLE &&
       ready_sent_count < ready_message_count ->
        faulted = 1;
        ready_phase = READY_FAULTED;
        /* Failure may occur before the first message or after a prefix. */
        assert(ready_sent_count < ready_message_count);
        check_ready_invariants()
    fi
}

inline apply_ready()
{
    if
    :: !faulted && ready_phase == READY_SENT && ready_has_apply ->
        core_applied = core_commit;
        ready_phase = READY_APPLIED;
        check_ready_invariants()
    fi
}

inline fail_apply()
{
    if
    :: !faulted && ready_phase == READY_SENT && ready_has_apply ->
        faulted = 1;
        ready_phase = READY_FAULTED;
        assert(core_applied < core_commit);
        check_ready_invariants()
    fi
}

inline advance_ready()
{
    if
    :: !faulted && ready_phase == READY_APPLIED ->
        if
        :: ready_has_read ->
            visible_read = 1;
            visible_read_index = core_commit
        :: else -> skip
        fi;
        ready_phase = READY_IDLE;
        ready_has_storage = 0;
        ready_has_apply = 0;
        ready_has_read = 0;
        ready_message_count = 0;
        ready_sent_count = 0;
        check_ready_invariants()
    fi
}

inline create_snapshot_point()
{
    if
    :: !faulted && ready_phase == READY_IDLE && snapshot_point == 0 &&
       core_applied == core_commit && core_applied > snapshot_base ->
        snapshot_point = core_applied;
        snapshot_point_configuration = core_configuration;
        snapshot_stored = 0;
        check_ready_invariants()
    fi
}

inline store_snapshot()
{
    if
    :: !faulted && snapshot_point != 0 && !snapshot_stored ->
        snapshot_stored = 1;
        check_ready_invariants()
    fi
}

inline fail_snapshot_store()
{
    if
    :: !faulted && snapshot_point != 0 && !snapshot_stored ->
        faulted = 1;
        ready_phase = READY_FAULTED;
        check_ready_invariants()
    fi
}

inline compact_snapshot()
{
    if
    :: !faulted && snapshot_point != 0 && snapshot_stored ->
        snapshot_base = snapshot_point;
        snapshot_point = 0;
        snapshot_stored = 0;
        check_ready_invariants()
    fi
}

active proctype ReadyLifecycle()
{
    do
    :: prepare_ready()
    :: persist_ready()
    :: fail_persist()
    :: send_one()
    :: fail_send()
    :: apply_ready()
    :: fail_apply()
    :: advance_ready()
    :: create_snapshot_point()
    :: store_snapshot()
    :: fail_snapshot_store()
    :: compact_snapshot()
    :: break
    od
}
