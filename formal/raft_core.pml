/*
 * Bounded static-core model.
 *
 * This is an event-level model of the deterministic Core contract.  It keeps
 * the network and Ready protocol out of this file so that election, vote
 * restriction, leader completeness, log matching, and current-term commit
 * can be checked without the Ready/storage state-space product.
 */

#define NODES 3
#define MAX_LOG 2
#define MAX_TERM 2

#define FOLLOWER 0
#define CANDIDATE 1
#define LEADER 2

#define QUORUM3(ack) \
    (((ack & 3) == 3) || ((ack & 5) == 5) || ((ack & 6) == 6))

#define LOG_TERM(n, i) log_term[((n) * MAX_LOG) + (i)]
#define LOG_CMD(n, i) log_cmd[((n) * MAX_LOG) + (i)]

#define HAS_COMMITTED_LOG(n) \
    ((committed_index == 0) || \
     ((log_len[n] >= committed_index) && \
      (LOG_TERM(n, committed_index - 1) == committed_term) && \
      (LOG_CMD(n, committed_index - 1) == committed_command)))

byte up[NODES] = {1, 1, 1};
byte role[NODES] = {FOLLOWER, FOLLOWER, FOLLOWER};
byte term[NODES];
byte vote_for[NODES];
byte votes[NODES];
byte match[NODES];

byte log_len[NODES];
byte log_term[NODES * MAX_LOG];
byte log_cmd[NODES * MAX_LOG];

byte commit[NODES];
byte applied[NODES];
byte global_term;
byte committed_index;
byte committed_term;
byte committed_command;
byte loop_i;
byte loop_j;

inline check_core_invariants()
{
    assert(committed_index <= MAX_LOG);
    if
    :: committed_index != 0 ->
        assert(committed_term != 0);
        assert(committed_command != 0)
    :: else -> skip
    fi;

    loop_i = 0;
    do
    :: loop_i < NODES ->
        assert(log_len[loop_i] <= MAX_LOG);
        assert(applied[loop_i] <= commit[loop_i]);
        assert(commit[loop_i] <= log_len[loop_i]);
        if
        :: committed_index != 0 && commit[loop_i] >= committed_index ->
            assert(HAS_COMMITTED_LOG(loop_i))
        :: else -> skip
        fi;
        loop_j = loop_i + 1;
        do
        :: loop_j < NODES ->
            if
            :: committed_index != 0 && commit[loop_i] >= committed_index &&
               commit[loop_j] >= committed_index ->
                assert(LOG_TERM(loop_i, committed_index - 1) ==
                       LOG_TERM(loop_j, committed_index - 1));
                assert(LOG_CMD(loop_i, committed_index - 1) ==
                       LOG_CMD(loop_j, committed_index - 1))
            :: else -> skip
            fi;
            loop_j++
        :: else -> break
        od;
        loop_i++
    :: else -> break
    od
}

inline start_election(candidate)
{
    if
    :: up[candidate] && role[candidate] != LEADER && global_term < MAX_TERM ->
        global_term++;
        loop_i = 0;
        do
        :: loop_i < NODES ->
            if
            :: loop_i != candidate ->
                /* A new election epoch invalidates unfinished elections and
                 * replication evidence from the previous epoch. */
                role[loop_i] = FOLLOWER;
                match[loop_i] = 0;
                votes[loop_i] = 0
            :: else -> skip
            fi;
            loop_i++
        :: else -> break
        od;
        term[candidate] = global_term;
        role[candidate] = CANDIDATE;
        vote_for[candidate] = candidate + 1;
        votes[candidate] = 1 << candidate;
        check_core_invariants()
    fi
}

inline grant_vote(voter, candidate)
{
    if
    :: up[voter] && up[candidate] && voter != candidate &&
       role[candidate] == CANDIDATE && term[candidate] >= term[voter] &&
       HAS_COMMITTED_LOG(candidate) &&
       (vote_for[voter] == 0 || vote_for[voter] == candidate + 1) ->
        if
        :: term[candidate] > term[voter] ->
            term[voter] = term[candidate];
            role[voter] = FOLLOWER;
            vote_for[voter] = 0;
            match[voter] = 0
        :: else -> skip
        fi;
        vote_for[voter] = candidate + 1;
        votes[candidate] = votes[candidate] | (1 << voter);
        check_core_invariants()
    fi
}

inline become_leader(candidate)
{
    if
    :: up[candidate] && role[candidate] == CANDIDATE &&
       QUORUM3(votes[candidate]) && HAS_COMMITTED_LOG(candidate) ->
        role[candidate] = LEADER;
        match[candidate] = 1 << candidate;
        loop_i = 0;
        do
        :: loop_i < NODES ->
            if
            :: loop_i != candidate ->
                assert(!(role[loop_i] == LEADER &&
                         term[loop_i] == term[candidate]))
            :: else -> skip
            fi;
            loop_i++
        :: else -> break
        od;
        check_core_invariants()
    fi
}

inline append_command(leader, command)
{
    if
    :: up[leader] && role[leader] == LEADER && command != 0 &&
       log_len[leader] < MAX_LOG ->
        LOG_TERM(leader, log_len[leader]) = term[leader];
        LOG_CMD(leader, log_len[leader]) = command;
        log_len[leader]++;
        match[leader] = 1 << leader;
        check_core_invariants()
    fi
}

inline replicate(leader, follower)
{
    if
    :: up[leader] && up[follower] && leader != follower &&
       role[leader] == LEADER && log_len[leader] != 0 ->
        if
        :: term[follower] < term[leader] ->
            term[follower] = term[leader];
            role[follower] = FOLLOWER;
            vote_for[follower] = 0
        :: else -> skip
        fi;
        loop_i = 0;
        do
        :: loop_i < log_len[leader] ->
            LOG_TERM(follower, loop_i) = LOG_TERM(leader, loop_i);
            LOG_CMD(follower, loop_i) = LOG_CMD(leader, loop_i);
            loop_i++
        :: else -> break
        od;
        log_len[follower] = log_len[leader];
        match[leader] = match[leader] | (1 << follower);
        check_core_invariants()
    fi
}

inline commit_current_term(leader)
{
    if
    :: up[leader] && role[leader] == LEADER && log_len[leader] != 0 &&
       LOG_TERM(leader, log_len[leader] - 1) == term[leader] &&
       log_len[leader] > committed_index &&
       QUORUM3(match[leader]) ->
        committed_index = log_len[leader];
        committed_term = LOG_TERM(leader, committed_index - 1);
        committed_command = LOG_CMD(leader, committed_index - 1);
        loop_i = 0;
        do
        :: loop_i < NODES ->
            if
            :: (match[leader] & (1 << loop_i)) != 0 ->
                assert(LOG_TERM(loop_i, committed_index - 1) == committed_term);
                assert(LOG_CMD(loop_i, committed_index - 1) == committed_command)
            :: else -> skip
            fi;
            loop_i++
        :: else -> break
        od;
        check_core_invariants()
    fi
}

inline apply_commit(node)
{
    if
    :: up[node] && committed_index != 0 &&
       log_len[node] >= committed_index &&
       HAS_COMMITTED_LOG(node) ->
        commit[node] = committed_index;
        applied[node] = commit[node];
        check_core_invariants()
    fi
}

active proctype RaftCore()
{
    do
    :: start_election(0)
    :: start_election(1)
    :: start_election(2)
    :: grant_vote(0, 1)
    :: grant_vote(0, 2)
    :: grant_vote(1, 0)
    :: grant_vote(1, 2)
    :: grant_vote(2, 0)
    :: grant_vote(2, 1)
    :: become_leader(0)
    :: become_leader(1)
    :: become_leader(2)
    :: append_command(0, 1)
    :: append_command(0, 2)
    :: append_command(1, 1)
    :: append_command(1, 2)
    :: append_command(2, 1)
    :: append_command(2, 2)
    :: replicate(0, 1)
    :: replicate(0, 2)
    :: replicate(1, 0)
    :: replicate(1, 2)
    :: replicate(2, 0)
    :: replicate(2, 1)
    :: commit_current_term(0)
    :: commit_current_term(1)
    :: commit_current_term(2)
    :: apply_commit(0)
    :: apply_commit(1)
    :: apply_commit(2)
    :: break
    od
}
