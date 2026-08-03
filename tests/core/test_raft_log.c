#include "raft_log.h"

#include <tinytest.h>
#include <turbo_error.h>

#include <string.h>

static tr_raft_log_entry_t test_entry(tr_raft_log_index_t index,
                                      tr_raft_log_term_t term,
                                      uint64_t command_id,
                                      const char *data)
{
    tr_raft_log_entry_t entry;

    memset(&entry, 0, sizeof(entry));
    entry.index = index;
    entry.term = term;
    entry.command_id = command_id;
    entry.data_length = strlen(data);
    memcpy(entry.data, data, entry.data_length);
    return entry;
}

spec("bounded raft log")
{
    it("appends local entries with contiguous indexes")
    {
        tr_raft_log_t log;
        const tr_raft_log_entry_t *entry = NULL;

        check_int_eq(tr_raft_log_init(&log, 4U, 0U, 0U), TURBO_OK);
        check_int_eq(tr_raft_log_append_local(&log, 1U, 7U, "one", 3U,
                                              &entry),
                     TURBO_OK);
        check_not_null(entry);
        check_long_eq(entry->index, 1U);
        check_long_eq(entry->term, 1U);
        check_size_eq(tr_raft_log_count(&log), 1U);
        check_long_eq(tr_raft_log_last_index(&log), 1U);
        check_long_eq(tr_raft_log_last_term(&log), 1U);
        tr_raft_log_destroy(&log);
    }

    it("returns a conflict hint without mutating the log")
    {
        tr_raft_log_t log;
        tr_raft_log_entry_t entries[2];
        tr_raft_log_reconcile_result_t result;

        entries[0] = test_entry(1U, 1U, 1U, "a");
        entries[1] = test_entry(2U, 1U, 2U, "b");
        check_int_eq(tr_raft_log_init(&log, 4U, 0U, 0U), TURBO_OK);
        check_int_eq(tr_raft_log_reconcile(&log, 0U, 0U, entries, 2U, 0U,
                                            &result),
                     TURBO_OK);
        check_true(result.matched);
        check_int_eq(tr_raft_log_reconcile(&log, 2U, 2U, NULL, 0U, 0U,
                                            &result),
                     TURBO_OK);
        check_false(result.matched);
        check_long_eq(result.reject_hint, 1U);
        check_size_eq(tr_raft_log_count(&log), 2U);
        tr_raft_log_destroy(&log);
    }

    it("replaces the conflicting suffix as one bounded mutation")
    {
        tr_raft_log_t log;
        tr_raft_log_entry_t original[3];
        tr_raft_log_entry_t replacement[2];
        tr_raft_log_reconcile_result_t result;
        const tr_raft_log_entry_t *entry;

        original[0] = test_entry(1U, 1U, 1U, "a");
        original[1] = test_entry(2U, 1U, 2U, "b");
        original[2] = test_entry(3U, 2U, 3U, "old");
        replacement[0] = test_entry(3U, 3U, 4U, "new");
        replacement[1] = test_entry(4U, 3U, 5U, "tail");

        check_int_eq(tr_raft_log_init(&log, 5U, 0U, 0U), TURBO_OK);
        check_int_eq(tr_raft_log_reconcile(&log, 0U, 0U, original, 3U, 0U,
                                            &result),
                     TURBO_OK);
        check_int_eq(tr_raft_log_reconcile(&log, 2U, 1U, replacement, 2U, 0U,
                                            &result),
                     TURBO_OK);
        check_true(result.changed);
        check_long_eq(result.truncate_from, 3U);
        check_long_eq(result.append_from, 3U);
        check_size_eq(result.append_count, 2U);
        entry = tr_raft_log_get(&log, 3U);
        check_not_null(entry);
        check_long_eq(entry->term, 3U);
        check_long_eq(tr_raft_log_last_index(&log), 4U);
        tr_raft_log_destroy(&log);
    }

    it("rejects divergent payload at the same term and index")
    {
        tr_raft_log_t log;
        tr_raft_log_entry_t first = test_entry(1U, 1U, 1U, "a");
        tr_raft_log_entry_t divergent = test_entry(1U, 1U, 1U, "b");
        tr_raft_log_reconcile_result_t result;

        check_int_eq(tr_raft_log_init(&log, 2U, 0U, 0U), TURBO_OK);
        check_int_eq(tr_raft_log_reconcile(&log, 0U, 0U, &first, 1U, 0U,
                                            &result),
                     TURBO_OK);
        check_int_eq(tr_raft_log_reconcile(&log, 0U, 0U, &divergent, 1U, 0U,
                                            &result),
                     TURBO_EPROTO);
        check_size_eq(tr_raft_log_count(&log), 1U);
        check_mem_eq(tr_raft_log_get(&log, 1U)->data, "a", 1U);
        tr_raft_log_destroy(&log);
    }

    it("compacts a durable prefix and releases bounded capacity")
    {
        tr_raft_log_t log;
        tr_raft_log_entry_t entries[3];
        tr_raft_log_reconcile_result_t result;

        entries[0] = test_entry(1U, 1U, 1U, "a");
        entries[1] = test_entry(2U, 1U, 2U, "b");
        entries[2] = test_entry(3U, 2U, 3U, "c");
        check_int_eq(tr_raft_log_init(&log, 3U, 0U, 0U), TURBO_OK);
        check_int_eq(tr_raft_log_reconcile(&log, 0U, 0U, entries, 3U, 0U,
                                            &result), TURBO_OK);

        check_int_eq(tr_raft_log_compact(&log, 2U, 1U), TURBO_OK);
        check_long_eq(log.base_index, 2U);
        check_long_eq(log.base_term, 1U);
        check_size_eq(tr_raft_log_count(&log), 1U);
        check_null(tr_raft_log_get(&log, 2U));
        check_long_eq(tr_raft_log_get(&log, 3U)->term, 2U);
        check_int_eq(tr_raft_log_append_local(&log, 2U, 4U, "d", 1U, NULL),
                     TURBO_OK);
        check_long_eq(tr_raft_log_last_index(&log), 4U);
        tr_raft_log_destroy(&log);
    }

    it("rejects a mismatched compaction identity without mutation")
    {
        tr_raft_log_t log;

        check_int_eq(tr_raft_log_init(&log, 2U, 0U, 0U), TURBO_OK);
        check_int_eq(tr_raft_log_append_local(&log, 1U, 1U, "a", 1U, NULL),
                     TURBO_OK);
        check_int_eq(tr_raft_log_compact(&log, 1U, 2U), TURBO_EPROTO);
        check_long_eq(log.base_index, 0U);
        check_size_eq(tr_raft_log_count(&log), 1U);
        tr_raft_log_destroy(&log);
    }

    it("rejects append on a null log without dereferencing it")
    {
        check_int_eq(tr_raft_log_append_local(
                         NULL, 1U, 1U, "a", 1U, NULL),
                     TURBO_EINVAL);
    }
}
