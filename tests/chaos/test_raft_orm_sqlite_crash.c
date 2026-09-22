#include <salts_error.h>
#include <salts_process.h>
#include "tinytest.h"

#include <stdlib.h>
#include <string.h>

enum {
    APP_CRASH_BEFORE_COMMIT = 41,
    APP_CRASH_AFTER_COMMIT = 42
};

static int app_run(const char *program, const char *mode, const char *path,
                   int expected_exit)
{
    const char *args[] = {mode, path, NULL};
    salts_process_options_t options;
    salts_process_result_t result;
    salts_process_t *process = NULL;
    int status;

    salts_process_options_init(&options);
    options.program = program;
    options.args = args;
    options.timeout_ms = 10000U;
    status = salts_process_spawn(&options, &process);
    if (status != SALTS_OK) {
        return status;
    }
    memset(&result, 0, sizeof(result));
    status = salts_process_wait(process, &result);
    salts_process_destroy(process);
    if (status != SALTS_OK || result.state != SALTS_PROCESS_EXITED ||
        result.exit_code != expected_exit) {
        return SALTS_EIO;
    }
    return SALTS_OK;
}

spec("SQLite ORM process crash recovery")
{
    it("classifies exact durable identity around the commit cut-point")
    {
        const char *program = getenv("TURBORAFT_ORM_SQLITE_CRASH_NODE");
        char *before_path = tt_make_temp_file(
            "turboraft-orm-before-commit", ".sqlite");
        char *after_path = tt_make_temp_file(
            "turboraft-orm-after-commit", ".sqlite");

        check_not_null(program);
        check_not_null(before_path);
        check_not_null(after_path);
        if (program != NULL && before_path != NULL && after_path != NULL) {
            check_equal(app_run(program, "init", before_path, EXIT_SUCCESS),
                        SALTS_OK);
            check_equal(app_run(program, "crash-before-commit", before_path,
                                APP_CRASH_BEFORE_COMMIT),
                        SALTS_OK);
            check_equal(app_run(program, "verify-pending", before_path,
                                EXIT_SUCCESS),
                        SALTS_OK);

            check_equal(app_run(program, "init", after_path, EXIT_SUCCESS),
                        SALTS_OK);
            check_equal(app_run(program, "crash-after-commit", after_path,
                                APP_CRASH_AFTER_COMMIT),
                        SALTS_OK);
            check_equal(app_run(program, "verify-applied", after_path,
                                EXIT_SUCCESS),
                        SALTS_OK);
        }
        if (before_path != NULL) {
            check_equal(tt_remove_file(before_path), 0);
        }
        if (after_path != NULL) {
            check_equal(tt_remove_file(after_path), 0);
        }
        free(before_path);
        free(after_path);
    }
}
