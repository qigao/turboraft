/*
 * Snapshot metadata contract model.
 *
 * Snapshot bytes and transport are deliberately absent.  The model checks
 * that a snapshot boundary is taken from the applied boundary, carries the
 * exact committed configuration, and becomes the log base only after durable
 * storage succeeds.
 */

#define READY_IDLE 0
#define READY_OUTSTANDING 1
#define SNAPSHOT_NONE 0
#define SNAPSHOT_POINT 1
#define SNAPSHOT_STORED 2

byte ready_state = READY_IDLE;
byte log_base;
byte log_last = 3;
byte commit_index;
byte applied_index;
byte configuration = 1;
byte durable_configuration = 1;
byte snapshot_state = SNAPSHOT_NONE;
byte point_index;
byte point_term;
byte point_configuration;
byte snapshot_base_configuration = 1;
byte faulted;

inline check_snapshot_invariants()
{
    assert(log_base <= applied_index);
    assert(applied_index <= commit_index);
    assert(commit_index <= log_last);
    assert(snapshot_base_configuration != 0);
    if
    :: snapshot_state != SNAPSHOT_NONE ->
        assert(point_index > log_base);
        assert(point_index == applied_index);
        assert(point_term == point_index);
        assert(point_configuration == configuration)
    :: else -> skip
    fi;
    if
    :: snapshot_state == SNAPSHOT_STORED ->
        assert(ready_state == READY_IDLE)
    :: else -> skip
    fi
}

inline start_ready()
{
    if
    :: !faulted && ready_state == READY_IDLE && snapshot_state == SNAPSHOT_NONE ->
        ready_state = READY_OUTSTANDING;
        check_snapshot_invariants()
    fi
}

inline finish_ready()
{
    if
    :: !faulted && ready_state == READY_OUTSTANDING ->
        ready_state = READY_IDLE;
        check_snapshot_invariants()
    fi
}

inline commit_entry()
{
    if
    :: !faulted && ready_state == READY_OUTSTANDING &&
       commit_index < log_last ->
        commit_index++;
        if
        :: commit_index == 2 -> configuration = 2
        :: else -> skip
        fi;
        check_snapshot_invariants()
    fi
}

inline apply_entry()
{
    if
    :: !faulted && ready_state == READY_OUTSTANDING &&
       applied_index < commit_index ->
        applied_index++;
        check_snapshot_invariants()
    fi
}

inline create_snapshot_point()
{
    if
    :: !faulted && ready_state == READY_IDLE &&
       snapshot_state == SNAPSHOT_NONE &&
       applied_index == commit_index && applied_index > log_base ->
        point_index = applied_index;
        point_term = point_index;
        point_configuration = configuration;
        snapshot_state = SNAPSHOT_POINT;
        check_snapshot_invariants()
    fi
}

inline store_snapshot()
{
    if
    :: !faulted && snapshot_state == SNAPSHOT_POINT &&
       ready_state == READY_IDLE ->
        snapshot_base_configuration = point_configuration;
        durable_configuration = point_configuration;
        snapshot_state = SNAPSHOT_STORED;
        check_snapshot_invariants()
    fi
}

inline fail_snapshot_store()
{
    if
    :: !faulted && snapshot_state == SNAPSHOT_POINT ->
        faulted = 1;
        check_snapshot_invariants()
    fi
}

inline compact()
{
    if
    :: !faulted && snapshot_state == SNAPSHOT_STORED &&
       ready_state == READY_IDLE ->
        log_base = point_index;
        snapshot_state = SNAPSHOT_NONE;
        point_index = 0;
        point_term = 0;
        point_configuration = 0;
        check_snapshot_invariants()
    fi
}

inline install_snapshot()
{
    if
    :: !faulted && ready_state == READY_IDLE &&
       snapshot_state == SNAPSHOT_STORED ->
        log_base = point_index;
        commit_index = point_index;
        applied_index = point_index;
        configuration = point_configuration;
        durable_configuration = point_configuration;
        snapshot_state = SNAPSHOT_NONE;
        point_index = 0;
        point_term = 0;
        point_configuration = 0;
        check_snapshot_invariants()
    fi
}

active proctype SnapshotMetadata()
{
    do
    :: start_ready()
    :: finish_ready()
    :: commit_entry()
    :: apply_entry()
    :: create_snapshot_point()
    :: store_snapshot()
    :: fail_snapshot_store()
    :: compact()
    :: install_snapshot()
    :: break
    od
}
