/*
 * Joint-consensus contract model.
 *
 * The log and transport are intentionally abstracted to acknowledgements.
 * The model checks that configuration activation, not mere staging, changes
 * the quorum fact source and that the final configuration needs both voters.
 */

#define STABLE 0
#define JOINT 1

#define OLD_MASK 7
#define NEW_MASK 14

#define QUORUM3(ack, mask) \
    (((mask == 7) && (((ack & 3) == 3) || \
                      ((ack & 5) == 5) || \
                      ((ack & 6) == 6))) || \
     ((mask == 14) && (((ack & 6) == 6) || \
                       ((ack & 10) == 10) || \
                       ((ack & 12) == 12))))

#define JOINT_QUORUM(ack) (QUORUM3(ack, OLD_MASK) && QUORUM3(ack, NEW_MASK))

byte phase = STABLE;
byte pending_kind;
byte committed_index;
byte pending_index;
byte acknowledgements;
byte transition_id;
byte committed_old_mask = OLD_MASK;
byte committed_new_mask = OLD_MASK;

inline check_membership_invariants()
{
    assert(transition_id == 0 || transition_id == 1);
    assert(committed_index <= 2);
    assert(committed_old_mask != 0);
    assert(committed_new_mask != 0);
    if
    :: phase == STABLE ->
        assert(committed_old_mask == committed_new_mask);
        assert(pending_kind == 0 || pending_kind == 1)
    :: phase == JOINT ->
        assert(committed_old_mask == OLD_MASK);
        assert(committed_new_mask == NEW_MASK);
        assert(pending_kind == 2)
    :: else -> assert(false)
    fi;
    if
    :: pending_kind != 0 -> assert(pending_index == committed_index + 1)
    :: else -> assert(pending_index == 0)
    fi
}

inline propose_joint()
{
    if
    :: phase == STABLE && pending_kind == 0 && transition_id == 0 ->
        transition_id = 1;
        pending_kind = 1;
        pending_index = committed_index + 1;
        acknowledgements = 0;
        check_membership_invariants()
    fi
}

inline acknowledge(node)
{
    if
    :: pending_kind != 0 && node < 4 ->
        acknowledgements = acknowledgements | (1 << node);
        check_membership_invariants()
    fi
}

inline commit_joint()
{
    if
    :: phase == STABLE && pending_kind == 1 &&
       QUORUM3(acknowledgements, OLD_MASK) ->
        committed_index++;
        phase = JOINT;
        pending_kind = 2;
        pending_index = committed_index + 1;
        acknowledgements = 0;
        committed_new_mask = NEW_MASK;
        check_membership_invariants()
    fi
}

inline commit_final()
{
    if
    :: phase == JOINT && pending_kind == 2 &&
       JOINT_QUORUM(acknowledgements) ->
        committed_index++;
        phase = STABLE;
        pending_kind = 0;
        pending_index = 0;
        acknowledgements = 0;
        committed_old_mask = NEW_MASK;
        committed_new_mask = NEW_MASK;
        check_membership_invariants()
    fi
}

active proctype JointMembership()
{
    do
    :: propose_joint()
    :: acknowledge(0)
    :: acknowledge(1)
    :: acknowledge(2)
    :: acknowledge(3)
    :: commit_joint()
    :: commit_final()
    :: break
    od
}
