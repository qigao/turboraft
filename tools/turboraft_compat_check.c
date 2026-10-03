#include <salts_error.h>
#include <salts_fs.h>

#include <xxhash.h>

#include <errno.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define TR_COMPAT_WAL_FORMAT_VERSION 1U
#define TR_COMPAT_SEGMENT_HEADER_SIZE 32U
#define TR_COMPAT_MANIFEST_HEADER_SIZE 40U
#define TR_COMPAT_SEGMENT_MAGIC "TRSEG001"
#define TR_COMPAT_MANIFEST_MAGIC "TRMAN001"
#define TR_COMPAT_V020_MAX_SEGMENTS 65535U

typedef struct tr_compat_limits {
    uint64_t max_segments;
    uint64_t segment_bytes;
    uint64_t max_transaction_bytes;
    uint64_t max_log_entries;
    uint64_t max_snapshot_bytes;
} tr_compat_limits_t;

typedef struct tr_compat_result {
    int compatible;
    const char *reason;
    int manifest_present;
    uint64_t authoritative_first;
    uint64_t authoritative_last;
    uint64_t visible_first;
    uint64_t visible_last;
    tr_compat_limits_t limits;
} tr_compat_result_t;

static uint32_t tr_get_u32(const uint8_t *input)
{
    return (uint32_t)input[0] |
           (uint32_t)input[1] << 8U |
           (uint32_t)input[2] << 16U |
           (uint32_t)input[3] << 24U;
}

static uint64_t tr_get_u64(const uint8_t *input)
{
    uint64_t value = 0U;
    size_t index;

    for (index = 0U; index < 8U; ++index) {
        value |= (uint64_t)input[index] << (index * 8U);
    }
    return value;
}

static int tr_parse_u64(const char *value, uint64_t *out)
{
    char *end = NULL;
    unsigned long long parsed;

    if (value == NULL || value[0] == '\0' || out == NULL ||
        value[0] == '-') {
        return SALTS_EINVAL;
    }
    errno = 0;
    parsed = strtoull(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0') {
        return SALTS_EINVAL;
    }
    *out = (uint64_t)parsed;
    return SALTS_OK;
}

static int tr_read_exact(const char *path, uint8_t *data, size_t size)
{
    salts_file_t file;
    size_t used = 0U;
    int result = SALTS_OK;

    file = salts_fs_open(path, SALTS_FS_O_RDONLY, 0);
    if (file == SALTS_INVALID_FILE) {
        return SALTS_EIO;
    }
    while (used < size) {
        int count = salts_fs_pread(
            file, (char *)data + used, size - used, (int64_t)used);
        if (count <= 0) {
            result = count < 0 ? count : SALTS_EIO;
            break;
        }
        used += (size_t)count;
    }
    {
        int close_result = salts_fs_close(file);
        if (result == SALTS_OK) {
            result = close_result;
        }
    }
    return result;
}

static int tr_manifest_path(const char *prefix, char *path, size_t size)
{
    int written = snprintf(path, size, "%s.manifest", prefix);
    return written < 0 || (size_t)written >= size
               ? SALTS_ENAMETOOLONG
               : SALTS_OK;
}

static int tr_segment_path(const char *prefix, uint64_t sequence,
                           char *path, size_t size)
{
    int written = snprintf(path, size, "%s.%08" PRIu64 ".wal",
                           prefix, sequence);
    return written < 0 || (size_t)written >= size
               ? SALTS_ENAMETOOLONG
               : SALTS_OK;
}

static int tr_read_manifest(const char *prefix, tr_compat_result_t *result)
{
    uint8_t header[TR_COMPAT_MANIFEST_HEADER_SIZE];
    char path[SALTS_FS_MAX_PATH];
    salts_fs_stat_t stat;
    uint32_t version;
    int rc;

    rc = tr_manifest_path(prefix, path, sizeof(path));
    if (rc != SALTS_OK) {
        result->reason = "path_too_long";
        return rc;
    }
    rc = salts_fs_access(path, SALTS_FS_ACCESS_EXISTS);
    if (rc == -ENOENT) {
        result->manifest_present = 0;
        return SALTS_OK;
    }
    if (rc != SALTS_OK) {
        result->reason = "manifest_io_error";
        return rc;
    }
    result->manifest_present = 1;
    rc = salts_fs_stat(path, &stat);
    if (rc != SALTS_OK || !stat.is_file ||
        stat.size != TR_COMPAT_MANIFEST_HEADER_SIZE) {
        result->reason = "manifest_corrupt";
        return SALTS_EPROTO;
    }
    rc = tr_read_exact(path, header, sizeof(header));
    if (rc != SALTS_OK) {
        result->reason = "manifest_io_error";
        return rc;
    }
    if (memcmp(header, TR_COMPAT_MANIFEST_MAGIC, 8U) != 0 ||
        tr_get_u32(header + 12U) != TR_COMPAT_MANIFEST_HEADER_SIZE) {
        result->reason = "manifest_corrupt";
        return SALTS_EPROTO;
    }
    version = tr_get_u32(header + 8U);
    if (version != TR_COMPAT_WAL_FORMAT_VERSION) {
        result->reason = "manifest_version_unsupported";
        return SALTS_EPROTO;
    }
    if (tr_get_u64(header + 32U) != XXH3_64bits(header, 32U)) {
        result->reason = "manifest_checksum_mismatch";
        return SALTS_EPROTO;
    }
    result->authoritative_first = tr_get_u64(header + 16U);
    result->authoritative_last = tr_get_u64(header + 24U);
    if (result->authoritative_first == 0U ||
        result->authoritative_last < result->authoritative_first) {
        result->reason = "manifest_range_invalid";
        return SALTS_EPROTO;
    }
    if (result->authoritative_last > result->limits.max_segments) {
        result->reason = "range_outside_legacy_window";
        return SALTS_EPROTO;
    }
    return SALTS_OK;
}

static int tr_validate_segment(const char *prefix, uint64_t sequence,
                               const tr_compat_limits_t *limits,
                               const char **out_reason)
{
    uint8_t header[TR_COMPAT_SEGMENT_HEADER_SIZE];
    char path[SALTS_FS_MAX_PATH];
    salts_fs_stat_t stat;
    uint32_t version;
    int rc;

    rc = tr_segment_path(prefix, sequence, path, sizeof(path));
    if (rc != SALTS_OK) {
        *out_reason = "path_too_long";
        return rc;
    }
    rc = salts_fs_stat(path, &stat);
    if (rc != SALTS_OK || !stat.is_file ||
        stat.size < TR_COMPAT_SEGMENT_HEADER_SIZE) {
        *out_reason = "segment_corrupt";
        return SALTS_EPROTO;
    }
    if (stat.size > limits->segment_bytes) {
        *out_reason = "segment_bytes_exceeded";
        return SALTS_EPROTO;
    }
    rc = tr_read_exact(path, header, sizeof(header));
    if (rc != SALTS_OK) {
        *out_reason = "segment_io_error";
        return rc;
    }
    if (memcmp(header, TR_COMPAT_SEGMENT_MAGIC, 8U) != 0 ||
        tr_get_u32(header + 12U) != TR_COMPAT_SEGMENT_HEADER_SIZE) {
        *out_reason = "segment_corrupt";
        return SALTS_EPROTO;
    }
    version = tr_get_u32(header + 8U);
    if (version != TR_COMPAT_WAL_FORMAT_VERSION) {
        *out_reason = "segment_version_unsupported";
        return SALTS_EPROTO;
    }
    if (tr_get_u64(header + 16U) != sequence ||
        tr_get_u64(header + 24U) != XXH3_64bits(header, 24U)) {
        *out_reason = "segment_header_mismatch";
        return SALTS_EPROTO;
    }
    return SALTS_OK;
}

static int tr_scan_legacy_window(const char *prefix, tr_compat_result_t *result)
{
    uint64_t sequence;
    int in_range = 0;
    int ended = 0;

    for (sequence = 1U; sequence <= result->limits.max_segments; ++sequence) {
        char path[SALTS_FS_MAX_PATH];
        int exists;
        int rc = tr_segment_path(prefix, sequence, path, sizeof(path));

        if (rc != SALTS_OK) {
            result->reason = "path_too_long";
            return rc;
        }
        rc = salts_fs_access(path, SALTS_FS_ACCESS_EXISTS);
        if (rc == -ENOENT) {
            if (in_range) {
                ended = 1;
            }
            continue;
        }
        if (rc != SALTS_OK) {
            result->reason = "segment_io_error";
            return rc;
        }
        exists = 1;
        if (ended) {
            result->reason = "legacy_visible_range_not_contiguous";
            return SALTS_EPROTO;
        }
        if (!in_range) {
            result->visible_first = sequence;
            in_range = 1;
        }
        result->visible_last = sequence;
        rc = tr_validate_segment(
            prefix, sequence, &result->limits, &result->reason);
        if (rc != SALTS_OK) {
            return rc;
        }
    }

    if (!in_range) {
        result->reason = "no_legacy_visible_segments";
        return SALTS_ENOENT;
    }
    if (result->manifest_present) {
        if (result->visible_first != result->authoritative_first ||
            result->visible_last != result->authoritative_last) {
            result->reason = "manifest_legacy_namespace_mismatch";
            return SALTS_EPROTO;
        }
    } else {
        result->authoritative_first = result->visible_first;
        result->authoritative_last = result->visible_last;
    }
    return SALTS_OK;
}

static void tr_print_json(const tr_compat_result_t *result)
{
    printf(
        "{\"target\":\"v0.2.0\","
        "\"compatible\":%s,"
        "\"reason\":\"%s\","
        "\"manifest_present\":%s,"
        "\"authoritative_first\":%" PRIu64 ","
        "\"authoritative_last\":%" PRIu64 ","
        "\"legacy_visible_first\":%" PRIu64 ","
        "\"legacy_visible_last\":%" PRIu64 ","
        "\"legacy_max_segments\":%" PRIu64 ","
        "\"legacy_segment_bytes\":%" PRIu64 ","
        "\"legacy_max_transaction_bytes\":%" PRIu64 ","
        "\"legacy_max_log_entries\":%" PRIu64 ","
        "\"legacy_max_snapshot_bytes\":%" PRIu64 "}\n",
        result->compatible ? "true" : "false",
        result->reason == NULL ? "ok" : result->reason,
        result->manifest_present ? "true" : "false",
        result->authoritative_first, result->authoritative_last,
        result->visible_first, result->visible_last,
        result->limits.max_segments, result->limits.segment_bytes,
        result->limits.max_transaction_bytes,
        result->limits.max_log_entries,
        result->limits.max_snapshot_bytes);
}

static void tr_usage(const char *program)
{
    fprintf(stderr,
            "Usage: %s downgrade --target v0.2.0 "
            "--path-prefix PREFIX "
            "--legacy-max-segments N "
            "--legacy-segment-bytes N "
            "--legacy-max-transaction-bytes N "
            "--legacy-max-log-entries N "
            "--legacy-max-snapshot-bytes N\n",
            program);
}

int main(int argc, char **argv)
{
    tr_compat_result_t result;
    const char *prefix = NULL;
    int index;
    int rc;

    memset(&result, 0, sizeof(result));
    result.reason = "invalid_arguments";

    if (argc < 3 || strcmp(argv[1], "downgrade") != 0) {
        tr_usage(argv[0]);
        return 2;
    }
    for (index = 2; index < argc; ++index) {
        const char *name = argv[index];
        const char *value;

        if (++index >= argc) {
            tr_usage(argv[0]);
            return 2;
        }
        value = argv[index];
        if (strcmp(name, "--target") == 0) {
            if (strcmp(value, "v0.2.0") != 0) {
                tr_print_json(&result);
                return 1;
            }
        } else if (strcmp(name, "--path-prefix") == 0) {
            prefix = value;
        } else if (strcmp(name, "--legacy-max-segments") == 0) {
            if (tr_parse_u64(value, &result.limits.max_segments) != SALTS_OK) {
                tr_print_json(&result);
                return 1;
            }
        } else if (strcmp(name, "--legacy-segment-bytes") == 0) {
            if (tr_parse_u64(value, &result.limits.segment_bytes) != SALTS_OK) {
                tr_print_json(&result);
                return 1;
            }
        } else if (strcmp(name, "--legacy-max-transaction-bytes") == 0) {
            if (tr_parse_u64(
                    value, &result.limits.max_transaction_bytes) != SALTS_OK) {
                tr_print_json(&result);
                return 1;
            }
        } else if (strcmp(name, "--legacy-max-log-entries") == 0) {
            if (tr_parse_u64(value, &result.limits.max_log_entries) != SALTS_OK) {
                tr_print_json(&result);
                return 1;
            }
        } else if (strcmp(name, "--legacy-max-snapshot-bytes") == 0) {
            if (tr_parse_u64(
                    value, &result.limits.max_snapshot_bytes) != SALTS_OK) {
                tr_print_json(&result);
                return 1;
            }
        } else {
            tr_usage(argv[0]);
            return 2;
        }
    }

    if (prefix == NULL || prefix[0] == '\0' ||
        result.limits.max_segments == 0U ||
        result.limits.max_segments > TR_COMPAT_V020_MAX_SEGMENTS ||
        result.limits.segment_bytes < TR_COMPAT_SEGMENT_HEADER_SIZE ||
        result.limits.max_transaction_bytes == 0U ||
        result.limits.max_transaction_bytes > result.limits.segment_bytes ||
        result.limits.max_log_entries == 0U ||
        result.limits.max_snapshot_bytes == 0U) {
        tr_print_json(&result);
        return 1;
    }

    result.reason = "ok";
    rc = tr_read_manifest(prefix, &result);
    if (rc == SALTS_OK) {
        rc = tr_scan_legacy_window(prefix, &result);
    }
    if (rc == SALTS_OK) {
        result.compatible = 1;
        result.reason = "ok";
        tr_print_json(&result);
        return 0;
    }

    result.compatible = 0;
    if (result.reason == NULL || strcmp(result.reason, "ok") == 0) {
        result.reason = "io_or_format_error";
    }
    tr_print_json(&result);
    return 1;
}
