#include <turboraft/raft_core.h>

#include <salts_error.h>
#include <salts_fs.h>

#include <openssl/sha.h>
#include <xxhash.h>

#include <errno.h>
#include <inttypes.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define TR_COMPAT_WAL_FORMAT_VERSION 1U
#define TR_COMPAT_SEGMENT_HEADER_SIZE 32U
#define TR_COMPAT_TRANSACTION_HEADER_SIZE 48U
#define TR_COMPAT_OPERATION_HEADER_SIZE 8U
#define TR_COMPAT_SNAPSHOT_HEADER_SIZE 48U
#define TR_COMPAT_MANIFEST_HEADER_SIZE 40U
#define TR_COMPAT_SEGMENT_MAGIC "TRSEG001"
#define TR_COMPAT_TRANSACTION_MAGIC "TRWAL001"
#define TR_COMPAT_SNAPSHOT_MAGIC "TRSNP001"
#define TR_COMPAT_MANIFEST_MAGIC "TRMAN001"
#define TR_COMPAT_V020_MIN_SEGMENT_BYTES (64U * 1024U)
#define TR_COMPAT_V020_MAX_SEGMENTS 65535U
#define TR_COMPAT_V020_MAX_ENTRY_BYTES 512U
#define TR_COMPAT_SNAPSHOT_DIGEST_SIZE 32U

enum tr_compat_operation_type {
    TR_COMPAT_OP_HARD_STATE = 1,
    TR_COMPAT_OP_TRUNCATE = 2,
    TR_COMPAT_OP_ENTRY = 3,
    TR_COMPAT_OP_COMMIT_INDEX = 4,
    TR_COMPAT_OP_SNAPSHOT = 5
};

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

typedef struct tr_compat_replay_state {
    uint64_t term;
    uint64_t commit_index;
    uint64_t snapshot_index;
    uint64_t last_transaction_id;
    uint64_t *entry_terms;
    size_t entry_count;
    size_t entry_capacity;
} tr_compat_replay_state_t;

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

static int tr_read_at_exact(salts_file_t file,
                            uint64_t offset,
                            uint8_t *data,
                            size_t size)
{
    size_t used = 0U;

    while (used < size) {
        int count = salts_fs_pread(
            file, (char *)data + used, size - used,
            (int64_t)(offset + used));
        if (count <= 0) {
            return count < 0 ? count : SALTS_EIO;
        }
        used += (size_t)count;
    }
    return SALTS_OK;
}

static int tr_read_exact(const char *path, uint8_t *data, size_t size)
{
    salts_file_t file;
    int result;

    file = salts_fs_open(path, SALTS_FS_O_RDONLY, 0);
    if (file == SALTS_INVALID_FILE) {
        return SALTS_EIO;
    }
    result = tr_read_at_exact(file, 0U, data, size);
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

static int tr_snapshot_path(const char *prefix,
                            uint64_t index,
                            uint64_t term,
                            char *path,
                            size_t size)
{
    int written = snprintf(path, size, "%s.snapshot.%" PRIu64 ".%" PRIu64,
                           prefix, index, term);
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

static int tr_replay_reserve_terms(tr_compat_replay_state_t *state,
                                   uint64_t max_log_entries,
                                   size_t required,
                                   const char **out_reason)
{
    size_t capacity;
    uint64_t *next;

    if ((uint64_t)required > max_log_entries) {
        *out_reason = "max_log_entries_exceeded";
        return SALTS_EPROTO;
    }
    if (required <= state->entry_capacity) {
        return SALTS_OK;
    }
    capacity = state->entry_capacity == 0U ? 8U : state->entry_capacity;
    while (capacity < required) {
        if (capacity > SIZE_MAX / 2U) {
            *out_reason = "preflight_memory_error";
            return SALTS_ENOMEM;
        }
        capacity *= 2U;
    }
    if ((uint64_t)capacity > max_log_entries) {
        capacity = (size_t)max_log_entries;
    }
    if (capacity > SIZE_MAX / sizeof(*next)) {
        *out_reason = "preflight_memory_error";
        return SALTS_ENOMEM;
    }
    next = (uint64_t *)realloc(
        state->entry_terms, capacity * sizeof(*next));
    if (next == NULL) {
        *out_reason = "preflight_memory_error";
        return SALTS_ENOMEM;
    }
    state->entry_terms = next;
    state->entry_capacity = capacity;
    return SALTS_OK;
}

static int tr_validate_snapshot_file(
    const char *prefix,
    uint64_t index,
    uint64_t term,
    uint64_t expected_size,
    uint64_t expected_checksum,
    const uint8_t *expected_digest,
    size_t expected_digest_size,
    const char **out_reason)
{
    uint8_t header[TR_COMPAT_SNAPSHOT_HEADER_SIZE];
    uint8_t buffer[8192];
    char path[SALTS_FS_MAX_PATH];
    salts_fs_stat_t stat;
    salts_file_t file = SALTS_INVALID_FILE;
    XXH3_state_t *xxh = NULL;
    SHA256_CTX sha;
    uint8_t digest[TR_COMPAT_SNAPSHOT_DIGEST_SIZE];
    uint64_t offset = 0U;
    int rc;

    rc = tr_snapshot_path(prefix, index, term, path, sizeof(path));
    if (rc != SALTS_OK) {
        *out_reason = "path_too_long";
        return rc;
    }
    rc = salts_fs_stat(path, &stat);
    if (rc == -ENOENT) {
        *out_reason = "snapshot_missing";
        return SALTS_ENOENT;
    }
    if (rc != SALTS_OK) {
        *out_reason = "snapshot_io_error";
        return rc;
    }
    if (!stat.is_file ||
        expected_size > UINT64_MAX - TR_COMPAT_SNAPSHOT_HEADER_SIZE ||
        stat.size != TR_COMPAT_SNAPSHOT_HEADER_SIZE + expected_size) {
        *out_reason = "snapshot_corrupt";
        return SALTS_EPROTO;
    }
    file = salts_fs_open(path, SALTS_FS_O_RDONLY, 0);
    if (file == SALTS_INVALID_FILE) {
        *out_reason = "snapshot_io_error";
        return SALTS_EIO;
    }
    rc = tr_read_at_exact(file, 0U, header, sizeof(header));
    if (rc != SALTS_OK) {
        *out_reason = "snapshot_io_error";
        (void)salts_fs_close(file);
        return rc;
    }
    if (memcmp(header, TR_COMPAT_SNAPSHOT_MAGIC, 8U) != 0 ||
        tr_get_u32(header + 8U) != TR_COMPAT_WAL_FORMAT_VERSION ||
        tr_get_u32(header + 12U) != TR_COMPAT_SNAPSHOT_HEADER_SIZE ||
        tr_get_u64(header + 16U) != index ||
        tr_get_u64(header + 24U) != term ||
        tr_get_u64(header + 32U) != expected_size ||
        tr_get_u64(header + 40U) != expected_checksum) {
        *out_reason = "snapshot_corrupt";
        (void)salts_fs_close(file);
        return SALTS_EPROTO;
    }

    xxh = XXH3_createState();
    if (xxh == NULL || XXH3_64bits_reset(xxh) != XXH_OK ||
        SHA256_Init(&sha) != 1) {
        if (xxh != NULL) {
            XXH3_freeState(xxh);
        }
        (void)salts_fs_close(file);
        *out_reason = "preflight_memory_error";
        return SALTS_ENOMEM;
    }
    while (offset < expected_size) {
        uint64_t remaining = expected_size - offset;
        size_t request = remaining > sizeof(buffer)
                             ? sizeof(buffer)
                             : (size_t)remaining;

        rc = tr_read_at_exact(
            file, TR_COMPAT_SNAPSHOT_HEADER_SIZE + offset,
            buffer, request);
        if (rc != SALTS_OK) {
            *out_reason = "snapshot_io_error";
            break;
        }
        if (XXH3_64bits_update(xxh, buffer, request) != XXH_OK ||
            SHA256_Update(&sha, buffer, request) != 1) {
            rc = SALTS_EIO;
            *out_reason = "snapshot_io_error";
            break;
        }
        offset += request;
    }
    if (rc == SALTS_OK &&
        XXH3_64bits_digest(xxh) != expected_checksum) {
        rc = SALTS_EPROTO;
        *out_reason = "snapshot_checksum_mismatch";
    }
    if (rc == SALTS_OK) {
        if (SHA256_Final(digest, &sha) != 1) {
            rc = SALTS_EIO;
            *out_reason = "snapshot_io_error";
        } else if (expected_digest_size != 0U &&
                   memcmp(digest, expected_digest,
                          expected_digest_size) != 0) {
            rc = SALTS_EPROTO;
            *out_reason = "snapshot_digest_mismatch";
        }
    }
    XXH3_freeState(xxh);
    {
        int close_result = salts_fs_close(file);
        if (rc == SALTS_OK && close_result != SALTS_OK) {
            rc = close_result;
            *out_reason = "snapshot_io_error";
        }
    }
    return rc;
}

static int tr_apply_payload(const char *prefix,
                            const uint8_t *payload,
                            size_t payload_size,
                            tr_compat_replay_state_t *state,
                            const tr_compat_limits_t *limits,
                            const char **out_reason)
{
    size_t offset = 0U;

    while (offset < payload_size) {
        uint8_t type;
        uint32_t size;
        const uint8_t *data;
        uint64_t last_index;

        if (payload_size - offset < TR_COMPAT_OPERATION_HEADER_SIZE) {
            *out_reason = "transaction_corrupt";
            return SALTS_EPROTO;
        }
        type = payload[offset];
        if (payload[offset + 1U] != 0U ||
            payload[offset + 2U] != 0U ||
            payload[offset + 3U] != 0U) {
            *out_reason = "transaction_corrupt";
            return SALTS_EPROTO;
        }
        size = tr_get_u32(payload + offset + 4U);
        offset += TR_COMPAT_OPERATION_HEADER_SIZE;
        if ((size_t)size > payload_size - offset) {
            *out_reason = "transaction_corrupt";
            return SALTS_EPROTO;
        }
        data = payload + offset;
        if (state->snapshot_index >
            UINT64_MAX - (uint64_t)state->entry_count) {
            *out_reason = "wal_replay_incompatible";
            return SALTS_EPROTO;
        }
        last_index = state->snapshot_index +
                     (uint64_t)state->entry_count;

        if (type == TR_COMPAT_OP_HARD_STATE) {
            uint64_t term;

            if (size != 16U) {
                *out_reason = "transaction_corrupt";
                return SALTS_EPROTO;
            }
            term = tr_get_u64(data);
            if (term < state->term) {
                *out_reason = "wal_replay_incompatible";
                return SALTS_EPROTO;
            }
            state->term = term;
        } else if (type == TR_COMPAT_OP_TRUNCATE) {
            uint64_t from_index;

            if (size != 8U) {
                *out_reason = "transaction_corrupt";
                return SALTS_EPROTO;
            }
            from_index = tr_get_u64(data);
            if (from_index == 0U ||
                from_index <= state->commit_index ||
                from_index <= state->snapshot_index ||
                from_index > last_index + 1U) {
                *out_reason = "wal_replay_incompatible";
                return SALTS_EPROTO;
            }
            state->entry_count =
                (size_t)(from_index - state->snapshot_index - 1U);
        } else if (type == TR_COMPAT_OP_ENTRY) {
            uint32_t data_length;
            uint64_t entry_index;
            uint64_t entry_term;
            int rc;

            if (size < 32U) {
                *out_reason = "transaction_corrupt";
                return SALTS_EPROTO;
            }
            data_length = tr_get_u32(data + 24U);
            if (tr_get_u32(data + 28U) != 0U ||
                data_length > TR_COMPAT_V020_MAX_ENTRY_BYTES ||
                size != 32U + data_length) {
                *out_reason = "transaction_corrupt";
                return SALTS_EPROTO;
            }
            if ((uint64_t)state->entry_count >=
                limits->max_log_entries) {
                *out_reason = "max_log_entries_exceeded";
                return SALTS_EPROTO;
            }
            rc = tr_replay_reserve_terms(
                state, limits->max_log_entries,
                state->entry_count + 1U, out_reason);
            if (rc != SALTS_OK) {
                return rc;
            }
            entry_index = tr_get_u64(data);
            entry_term = tr_get_u64(data + 8U);
            if (entry_index != last_index + 1U ||
                entry_term == 0U) {
                *out_reason = "wal_replay_incompatible";
                return SALTS_EPROTO;
            }
            state->entry_terms[state->entry_count++] = entry_term;
        } else if (type == TR_COMPAT_OP_COMMIT_INDEX) {
            uint64_t commit_index;

            if (size != 8U) {
                *out_reason = "transaction_corrupt";
                return SALTS_EPROTO;
            }
            commit_index = tr_get_u64(data);
            if (commit_index < state->commit_index ||
                commit_index > last_index) {
                *out_reason = "wal_replay_incompatible";
                return SALTS_EPROTO;
            }
            state->commit_index = commit_index;
        } else if (type == TR_COMPAT_OP_SNAPSHOT) {
            uint64_t leader_term;
            uint64_t snapshot_index;
            uint64_t snapshot_term;
            uint64_t snapshot_size;
            uint64_t snapshot_checksum;
            uint64_t commit_index;
            uint32_t configuration_size;
            uint32_t digest_size;
            const uint8_t *snapshot_digest = NULL;
            size_t suffix_offset = 0U;
            size_t suffix_count = 0U;
            tr_raft_conf_t configuration;
            int rc;

            if (size < 64U) {
                *out_reason = "transaction_corrupt";
                return SALTS_EPROTO;
            }
            leader_term = tr_get_u64(data);
            snapshot_index = tr_get_u64(data + 8U);
            snapshot_term = tr_get_u64(data + 16U);
            snapshot_size = tr_get_u64(data + 24U);
            snapshot_checksum = tr_get_u64(data + 32U);
            commit_index = tr_get_u64(data + 40U);
            configuration_size = tr_get_u32(data + 56U);
            digest_size = tr_get_u32(data + 60U);
            if ((digest_size != 0U &&
                 digest_size != TR_COMPAT_SNAPSHOT_DIGEST_SIZE) ||
                configuration_size > size - 64U ||
                digest_size > size - 64U - configuration_size ||
                size != 64U + configuration_size + digest_size ||
                leader_term == 0U ||
                snapshot_index == 0U ||
                snapshot_term == 0U ||
                snapshot_term > leader_term ||
                leader_term < state->term ||
                snapshot_index <= state->snapshot_index ||
                commit_index < snapshot_index ||
                (commit_index != snapshot_index &&
                 commit_index > last_index)) {
                *out_reason = "wal_replay_incompatible";
                return SALTS_EPROTO;
            }
            if (snapshot_size > limits->max_snapshot_bytes) {
                *out_reason = "max_snapshot_bytes_exceeded";
                return SALTS_EPROTO;
            }
            if (digest_size != 0U) {
                snapshot_digest =
                    data + 64U + configuration_size;
            }
            memset(&configuration, 0, sizeof(configuration));
            if (tr_raft_conf_decode(
                    data + 64U, configuration_size,
                    &configuration) != SALTS_OK) {
                *out_reason = "configuration_incompatible";
                return SALTS_EPROTO;
            }
            if (snapshot_index > state->snapshot_index &&
                snapshot_index <= last_index) {
                size_t match = (size_t)(
                    snapshot_index - state->snapshot_index - 1U);

                if (state->entry_terms[match] == snapshot_term) {
                    suffix_offset = match + 1U;
                    suffix_count =
                        state->entry_count - suffix_offset;
                }
            }
            rc = tr_validate_snapshot_file(
                prefix, snapshot_index, snapshot_term,
                snapshot_size, snapshot_checksum,
                snapshot_digest, digest_size, out_reason);
            if (rc != SALTS_OK) {
                return rc;
            }
            if (suffix_count != 0U) {
                memmove(
                    state->entry_terms,
                    state->entry_terms + suffix_offset,
                    suffix_count * sizeof(state->entry_terms[0]));
            }
            state->entry_count = suffix_count;
            state->snapshot_index = snapshot_index;
            state->term = leader_term;
            state->commit_index = commit_index;
        } else {
            *out_reason = "wal_operation_unsupported";
            return SALTS_EPROTO;
        }
        offset += size;
    }
    return SALTS_OK;
}

static uint64_t tr_transaction_checksum(const uint8_t *header,
                                        const uint8_t *payload,
                                        size_t payload_size)
{
    uint64_t seed = tr_get_u64(header + 16U) ^
                    tr_get_u64(header + 24U) ^
                    tr_get_u64(header + 32U);

    return XXH3_64bits_withSeed(payload, payload_size, seed);
}

static int tr_replay_segment(const char *prefix,
                             uint64_t sequence,
                             int is_first,
                             int is_last,
                             tr_compat_replay_state_t *state,
                             const tr_compat_limits_t *limits,
                             const char **out_reason)
{
    uint8_t segment_header[TR_COMPAT_SEGMENT_HEADER_SIZE];
    char path[SALTS_FS_MAX_PATH];
    salts_fs_stat_t stat;
    salts_file_t file = SALTS_INVALID_FILE;
    uint64_t offset = TR_COMPAT_SEGMENT_HEADER_SIZE;
    int rc;

    rc = tr_segment_path(prefix, sequence, path, sizeof(path));
    if (rc != SALTS_OK) {
        *out_reason = "path_too_long";
        return rc;
    }
    rc = salts_fs_stat(path, &stat);
    if (rc != SALTS_OK || !stat.is_file ||
        stat.size < TR_COMPAT_SEGMENT_HEADER_SIZE ||
        stat.size > limits->segment_bytes) {
        *out_reason = "segment_corrupt";
        return SALTS_EPROTO;
    }
    file = salts_fs_open(path, SALTS_FS_O_RDONLY, 0);
    if (file == SALTS_INVALID_FILE) {
        *out_reason = "segment_io_error";
        return SALTS_EIO;
    }
    rc = tr_read_at_exact(
        file, 0U, segment_header, sizeof(segment_header));
    if (rc != SALTS_OK) {
        *out_reason = "segment_io_error";
        (void)salts_fs_close(file);
        return rc;
    }
    while (rc == SALTS_OK && offset < stat.size) {
        uint8_t header[TR_COMPAT_TRANSACTION_HEADER_SIZE];
        uint64_t transaction_id;
        uint64_t previous_transaction_id;
        uint64_t payload_size;
        uint8_t *payload = NULL;
        uint64_t remaining = stat.size - offset;

        if (remaining < sizeof(header)) {
            if (is_last) {
                break;
            }
            *out_reason = "transaction_corrupt";
            rc = SALTS_EPROTO;
            break;
        }
        rc = tr_read_at_exact(file, offset, header, sizeof(header));
        if (rc != SALTS_OK) {
            *out_reason = "segment_io_error";
            break;
        }
        if (memcmp(header, TR_COMPAT_TRANSACTION_MAGIC, 8U) != 0 ||
            tr_get_u32(header + 8U) != TR_COMPAT_WAL_FORMAT_VERSION ||
            tr_get_u32(header + 12U) !=
                TR_COMPAT_TRANSACTION_HEADER_SIZE) {
            *out_reason = "transaction_corrupt";
            rc = SALTS_EPROTO;
            break;
        }
        transaction_id = tr_get_u64(header + 16U);
        previous_transaction_id = tr_get_u64(header + 24U);
        payload_size = tr_get_u64(header + 32U);
        if (is_first &&
            offset == TR_COMPAT_SEGMENT_HEADER_SIZE &&
            state->last_transaction_id == 0U &&
            previous_transaction_id != 0U) {
            state->last_transaction_id =
                previous_transaction_id;
        }
        if (transaction_id != state->last_transaction_id + 1U ||
            previous_transaction_id != state->last_transaction_id) {
            *out_reason = "transaction_sequence_mismatch";
            rc = SALTS_EPROTO;
            break;
        }
        if (payload_size >
            limits->max_transaction_bytes -
                TR_COMPAT_TRANSACTION_HEADER_SIZE) {
            *out_reason = "transaction_bytes_exceeded";
            rc = SALTS_EPROTO;
            break;
        }
        if (payload_size >
            stat.size - offset - sizeof(header)) {
            if (is_last) {
                break;
            }
            *out_reason = "transaction_corrupt";
            rc = SALTS_EPROTO;
            break;
        }
        payload = (uint8_t *)malloc(
            payload_size == 0U ? 1U : (size_t)payload_size);
        if (payload == NULL) {
            *out_reason = "preflight_memory_error";
            rc = SALTS_ENOMEM;
            break;
        }
        rc = tr_read_at_exact(
            file, offset + sizeof(header),
            payload, (size_t)payload_size);
        if (rc != SALTS_OK) {
            *out_reason = "segment_io_error";
            free(payload);
            break;
        }
        if (tr_get_u64(header + 40U) !=
            tr_transaction_checksum(
                header, payload, (size_t)payload_size)) {
            *out_reason = "transaction_checksum_mismatch";
            free(payload);
            rc = SALTS_EPROTO;
            break;
        }
        rc = tr_apply_payload(
            prefix, payload, (size_t)payload_size,
            state, limits, out_reason);
        free(payload);
        if (rc != SALTS_OK) {
            break;
        }
        if (is_first &&
            offset == TR_COMPAT_SEGMENT_HEADER_SIZE &&
            previous_transaction_id != 0U &&
            state->snapshot_index == 0U) {
            *out_reason = "compacted_prefix_missing_snapshot";
            rc = SALTS_EPROTO;
            break;
        }
        state->last_transaction_id = transaction_id;
        offset += sizeof(header) + payload_size;
    }
    (void)salts_fs_close(file);
    return rc;
}

static int tr_validate_legacy_replay(
    const char *prefix,
    tr_compat_result_t *result)
{
    tr_compat_replay_state_t state;
    uint64_t sequence;
    int rc = SALTS_OK;

    memset(&state, 0, sizeof(state));
    for (sequence = result->visible_first;
         sequence <= result->visible_last;
         ++sequence) {
        rc = tr_replay_segment(
            prefix, sequence,
            sequence == result->visible_first,
            sequence == result->visible_last,
            &state, &result->limits, &result->reason);
        if (rc != SALTS_OK) {
            break;
        }
    }
    free(state.entry_terms);
    return rc;
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
        result.limits.segment_bytes < TR_COMPAT_V020_MIN_SEGMENT_BYTES ||
        result.limits.segment_bytes > INT_MAX ||
        result.limits.max_transaction_bytes <
            TR_COMPAT_TRANSACTION_HEADER_SIZE ||
        result.limits.max_transaction_bytes >
            result.limits.segment_bytes -
                TR_COMPAT_SEGMENT_HEADER_SIZE ||
        result.limits.max_log_entries == 0U ||
        result.limits.max_log_entries > SIZE_MAX ||
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
        rc = tr_validate_legacy_replay(prefix, &result);
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
