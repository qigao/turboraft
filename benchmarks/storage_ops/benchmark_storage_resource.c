#include <turboraft/raft_wal_storage.h>

#include <salts_error.h>

#include <dirent.h>
#include <glob.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

enum {
    BENCH_ENTRY_COUNT = 1024U,
    BENCH_ENTRY_BATCH = 16U,
    BENCH_SNAPSHOT_BYTES = 4U * 1024U * 1024U,
    BENCH_MAX_SNAPSHOT_BYTES = 8U * 1024U * 1024U
};

typedef struct bench_resource_sample {
    uint64_t user_ns;
    uint64_t sys_ns;
    long max_rss_kb;
    size_t fd_count;
} bench_resource_sample_t;

typedef struct bench_file_sample {
    size_t wal_segments;
    uint64_t wal_bytes;
    uint64_t durable_bytes;
} bench_file_sample_t;

static uint64_t bench_now_ns(void)
{
    struct timespec ts;

    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        return 0U;
    }
    return (uint64_t)ts.tv_sec * UINT64_C(1000000000) +
           (uint64_t)ts.tv_nsec;
}

static uint64_t bench_timeval_ns(const struct timeval *value)
{
    return (uint64_t)value->tv_sec * UINT64_C(1000000000) +
           (uint64_t)value->tv_usec * UINT64_C(1000);
}

static size_t bench_fd_count(void)
{
    DIR *directory = opendir("/proc/self/fd");
    struct dirent *entry;
    size_t count = 0U;

    if (directory == NULL) {
        return 0U;
    }
    while ((entry = readdir(directory)) != NULL) {
        if (strcmp(entry->d_name, ".") != 0 &&
            strcmp(entry->d_name, "..") != 0) {
            ++count;
        }
    }
    (void)closedir(directory);
    /* Exclude the descriptor used to inspect /proc/self/fd itself. */
    return count == 0U ? 0U : count - 1U;
}

static int bench_resource_sample(bench_resource_sample_t *out)
{
    struct rusage usage;

    if (out == NULL) {
        return SALTS_EINVAL;
    }
    memset(out, 0, sizeof(*out));
    if (getrusage(RUSAGE_SELF, &usage) != 0) {
        return SALTS_EIO;
    }
    out->user_ns = bench_timeval_ns(&usage.ru_utime);
    out->sys_ns = bench_timeval_ns(&usage.ru_stime);
    out->max_rss_kb = usage.ru_maxrss;
    out->fd_count = bench_fd_count();
    return SALTS_OK;
}

static int bench_glob_bytes(const char *pattern,
                            size_t *out_count,
                            uint64_t *out_bytes)
{
    glob_t matches;
    size_t index;
    int status;

    if (pattern == NULL || out_count == NULL || out_bytes == NULL) {
        return SALTS_EINVAL;
    }
    *out_count = 0U;
    *out_bytes = 0U;
    memset(&matches, 0, sizeof(matches));
    status = glob(pattern, 0, NULL, &matches);
    if (status == GLOB_NOMATCH) {
        return SALTS_OK;
    }
    if (status != 0) {
        return SALTS_EIO;
    }
    for (index = 0U; index < matches.gl_pathc; ++index) {
        struct stat info;

        if (stat(matches.gl_pathv[index], &info) != 0) {
            globfree(&matches);
            return SALTS_EIO;
        }
        if (S_ISREG(info.st_mode)) {
            ++*out_count;
            *out_bytes += (uint64_t)info.st_size;
        }
    }
    globfree(&matches);
    return SALTS_OK;
}

static int bench_file_sample(const char *prefix,
                             bench_file_sample_t *out)
{
    char pattern[512];
    size_t durable_files = 0U;
    int written;
    int result;

    if (prefix == NULL || out == NULL) {
        return SALTS_EINVAL;
    }
    memset(out, 0, sizeof(*out));

    written = snprintf(pattern, sizeof(pattern), "%s.*.wal", prefix);
    if (written < 0 || (size_t)written >= sizeof(pattern)) {
        return SALTS_ENAMETOOLONG;
    }
    result = bench_glob_bytes(
        pattern, &out->wal_segments, &out->wal_bytes);
    if (result != SALTS_OK) {
        return result;
    }

    written = snprintf(pattern, sizeof(pattern), "%s*", prefix);
    if (written < 0 || (size_t)written >= sizeof(pattern)) {
        return SALTS_ENAMETOOLONG;
    }
    result = bench_glob_bytes(
        pattern, &durable_files, &out->durable_bytes);
    (void)durable_files;
    return result;
}

static void bench_cleanup(const char *prefix)
{
    char pattern[512];
    glob_t matches;
    size_t index;
    int written;

    if (prefix == NULL) {
        return;
    }
    written = snprintf(pattern, sizeof(pattern), "%s*", prefix);
    if (written < 0 || (size_t)written >= sizeof(pattern)) {
        return;
    }
    memset(&matches, 0, sizeof(matches));
    if (glob(pattern, 0, NULL, &matches) != 0) {
        return;
    }
    for (index = 0U; index < matches.gl_pathc; ++index) {
        (void)unlink(matches.gl_pathv[index]);
    }
    globfree(&matches);
}

static tr_raft_wal_storage_config_t bench_storage_config(
    const char *prefix)
{
    tr_raft_wal_storage_config_t config;

    memset(&config, 0, sizeof(config));
    config.path_prefix = prefix;
    config.segment_bytes = TR_RAFT_WAL_MIN_SEGMENT_BYTES;
    config.max_transaction_bytes = 32U * 1024U;
    config.max_live_segments = 32U;
    config.max_log_entries = BENCH_ENTRY_COUNT + 32U;
    config.max_snapshot_bytes = BENCH_MAX_SNAPSHOT_BYTES;
    config.create_if_missing = true;
    return config;
}

static int bench_append_entries(tr_raft_storage_t *storage)
{
    tr_raft_index_t next_index = 1U;
    size_t batch;

    for (batch = 0U;
         batch < BENCH_ENTRY_COUNT / BENCH_ENTRY_BATCH;
         ++batch) {
        tr_raft_entry_t entries[BENCH_ENTRY_BATCH];
        size_t index;
        int result;

        memset(entries, 0, sizeof(entries));
        for (index = 0U; index < BENCH_ENTRY_BATCH; ++index) {
            tr_raft_entry_t *entry = &entries[index];

            entry->index = next_index++;
            entry->term = 1U;
            entry->command_id = entry->index;
            entry->data_length = TR_RAFT_MAX_ENTRY_BYTES;
            memset(entry->data, (int)(entry->index & 0xffU),
                   entry->data_length);
        }

        result = storage->begin(storage->context);
        if (result == SALTS_OK) {
            result = storage->write_hard_state(
                storage->context, 1U, 1U);
        }
        if (result == SALTS_OK) {
            result = storage->append_log(
                storage->context, entries, BENCH_ENTRY_BATCH);
        }
        if (result == SALTS_OK) {
            result = storage->write_commit_index(
                storage->context,
                entries[BENCH_ENTRY_BATCH - 1U].index);
        }
        if (result == SALTS_OK) {
            result = storage->commit(storage->context);
        } else {
            (void)storage->rollback(storage->context);
        }
        if (result != SALTS_OK) {
            return result;
        }
    }
    return SALTS_OK;
}

static void bench_print_header(void)
{
    puts("phase,payload_bytes,duration_ns,throughput_mib_s,"
         "wal_segments_before,wal_segments_after,"
         "wal_bytes_before,wal_bytes_after,"
         "durable_bytes_before,durable_bytes_after,"
         "user_cpu_ns,sys_cpu_ns,max_rss_kb,fd_count");
}

static void bench_print_phase(const char *phase,
                              uint64_t payload_bytes,
                              uint64_t duration_ns,
                              const bench_file_sample_t *before_files,
                              const bench_file_sample_t *after_files,
                              const bench_resource_sample_t *before_resource,
                              const bench_resource_sample_t *after_resource)
{
    double throughput = 0.0;

    if (payload_bytes != 0U && duration_ns != 0U) {
        throughput =
            ((double)payload_bytes / (1024.0 * 1024.0)) /
            ((double)duration_ns / 1000000000.0);
    }
    printf("%s,%" PRIu64 ",%" PRIu64 ",%.2f,%zu,%zu,%" PRIu64
           ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%" PRIu64
           ",%" PRIu64 ",%ld,%zu\n",
           phase, payload_bytes, duration_ns, throughput,
           before_files->wal_segments, after_files->wal_segments,
           before_files->wal_bytes, after_files->wal_bytes,
           before_files->durable_bytes, after_files->durable_bytes,
           after_resource->user_ns - before_resource->user_ns,
           after_resource->sys_ns - before_resource->sys_ns,
           after_resource->max_rss_kb, after_resource->fd_count);
}

static int bench_capture_before(const char *prefix,
                                bench_file_sample_t *files,
                                bench_resource_sample_t *resource)
{
    int result = bench_file_sample(prefix, files);

    return result == SALTS_OK
               ? bench_resource_sample(resource)
               : result;
}

static int bench_capture_after(const char *prefix,
                               bench_file_sample_t *files,
                               bench_resource_sample_t *resource)
{
    int result = bench_resource_sample(resource);

    return result == SALTS_OK
               ? bench_file_sample(prefix, files)
               : result;
}

int main(void)
{
    static const tr_raft_conf_t configuration = {
        TR_RAFT_CONF_FINAL, 1U, 1U,
        {{1U, TR_RAFT_CONF_OLD_VOTER | TR_RAFT_CONF_NEW_VOTER}}};
    char primary_prefix[256];
    char install_prefix[256];
    tr_raft_wal_storage_config_t config;
    tr_raft_wal_storage_t *storage = NULL;
    tr_raft_storage_t adapter;
    bench_file_sample_t before_files;
    bench_file_sample_t after_files;
    bench_resource_sample_t before_resource;
    bench_resource_sample_t after_resource;
    uint8_t *snapshot = NULL;
    uint64_t start;
    uint64_t end;
    size_t index;
    int result;

    snprintf(primary_prefix, sizeof(primary_prefix),
             "/tmp/turboraft-storage-resource-%ld", (long)getpid());
    snprintf(install_prefix, sizeof(install_prefix),
             "/tmp/turboraft-storage-install-%ld", (long)getpid());
    bench_cleanup(primary_prefix);
    bench_cleanup(install_prefix);

    snapshot = (uint8_t *)malloc(BENCH_SNAPSHOT_BYTES);
    if (snapshot == NULL) {
        return 10;
    }
    for (index = 0U; index < BENCH_SNAPSHOT_BYTES; ++index) {
        snapshot[index] = (uint8_t)(index & 0xffU);
    }

    config = bench_storage_config(primary_prefix);
    result = tr_raft_wal_storage_open(&config, &storage);
    if (result == SALTS_OK) {
        result = tr_raft_wal_storage_bind(storage, &adapter);
    }
    if (result != SALTS_OK) {
        fprintf(stderr, "cannot open primary WAL: %d\n", result);
        free(snapshot);
        bench_cleanup(primary_prefix);
        bench_cleanup(install_prefix);
        return 11;
    }

    bench_print_header();

    result = bench_capture_before(
        primary_prefix, &before_files, &before_resource);
    start = bench_now_ns();
    if (result == SALTS_OK) {
        result = bench_append_entries(&adapter);
    }
    end = bench_now_ns();
    if (result == SALTS_OK) {
        result = bench_capture_after(
            primary_prefix, &after_files, &after_resource);
    }
    if (result != SALTS_OK) {
        fprintf(stderr, "append/rotation benchmark failed: %d\n", result);
        tr_raft_wal_storage_close(storage);
        free(snapshot);
        bench_cleanup(primary_prefix);
        bench_cleanup(install_prefix);
        return 12;
    }
    bench_print_phase(
        "wal_append_rotate",
        (uint64_t)BENCH_ENTRY_COUNT * TR_RAFT_MAX_ENTRY_BYTES,
        end - start, &before_files, &after_files,
        &before_resource, &after_resource);

    result = bench_capture_before(
        primary_prefix, &before_files, &before_resource);
    start = bench_now_ns();
    if (result == SALTS_OK) {
        result = tr_raft_wal_storage_store_snapshot(
            storage, 768U, 1U, &configuration,
            snapshot, BENCH_SNAPSHOT_BYTES);
    }
    end = bench_now_ns();
    if (result == SALTS_OK) {
        result = bench_capture_after(
            primary_prefix, &after_files, &after_resource);
    }
    if (result != SALTS_OK) {
        fprintf(stderr, "snapshot store benchmark failed: %d\n", result);
        tr_raft_wal_storage_close(storage);
        free(snapshot);
        bench_cleanup(primary_prefix);
        bench_cleanup(install_prefix);
        return 13;
    }
    bench_print_phase(
        "snapshot_store_retained_suffix",
        BENCH_SNAPSHOT_BYTES, end - start,
        &before_files, &after_files,
        &before_resource, &after_resource);

    result = bench_capture_before(
        primary_prefix, &before_files, &before_resource);
    start = bench_now_ns();
    if (result == SALTS_OK) {
        result = tr_raft_wal_storage_store_snapshot(
            storage, BENCH_ENTRY_COUNT, 1U, &configuration, NULL, 0U);
    }
    end = bench_now_ns();
    if (result == SALTS_OK) {
        result = bench_capture_after(
            primary_prefix, &after_files, &after_resource);
    }
    if (result != SALTS_OK) {
        fprintf(stderr, "checkpoint compaction benchmark failed: %d\n",
                result);
        tr_raft_wal_storage_close(storage);
        free(snapshot);
        bench_cleanup(primary_prefix);
        bench_cleanup(install_prefix);
        return 14;
    }
    bench_print_phase(
        "wal_checkpoint_compaction", 0U, end - start,
        &before_files, &after_files,
        &before_resource, &after_resource);

    result = tr_raft_wal_storage_close(storage);
    storage = NULL;
    if (result != SALTS_OK) {
        fprintf(stderr, "cannot close primary WAL: %d\n", result);
        free(snapshot);
        bench_cleanup(primary_prefix);
        bench_cleanup(install_prefix);
        return 15;
    }

    config = bench_storage_config(install_prefix);
    result = tr_raft_wal_storage_open(&config, &storage);
    if (result != SALTS_OK) {
        fprintf(stderr, "cannot open install WAL: %d\n", result);
        free(snapshot);
        bench_cleanup(primary_prefix);
        bench_cleanup(install_prefix);
        return 16;
    }
    result = bench_capture_before(
        install_prefix, &before_files, &before_resource);
    start = bench_now_ns();
    if (result == SALTS_OK) {
        result = tr_raft_wal_storage_install_snapshot(
            storage, 1U, BENCH_ENTRY_COUNT, 1U,
            &configuration, snapshot, BENCH_SNAPSHOT_BYTES);
    }
    end = bench_now_ns();
    if (result == SALTS_OK) {
        result = bench_capture_after(
            install_prefix, &after_files, &after_resource);
    }
    if (result != SALTS_OK) {
        fprintf(stderr, "snapshot install benchmark failed: %d\n", result);
        tr_raft_wal_storage_close(storage);
        free(snapshot);
        bench_cleanup(primary_prefix);
        bench_cleanup(install_prefix);
        return 17;
    }
    bench_print_phase(
        "snapshot_install",
        BENCH_SNAPSHOT_BYTES, end - start,
        &before_files, &after_files,
        &before_resource, &after_resource);

    result = tr_raft_wal_storage_close(storage);
    free(snapshot);
    bench_cleanup(primary_prefix);
    bench_cleanup(install_prefix);
    return result == SALTS_OK ? 0 : 18;
}
