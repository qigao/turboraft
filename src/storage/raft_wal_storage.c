#include <turboraft/raft_wal_storage.h>

#include <salts_error.h>
#include <salts_fs.h>
#include <openssl/sha.h>
#include <xxhash.h>

#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define TR_WAL_FORMAT_VERSION 1U
#define TR_WAL_SEGMENT_HEADER_SIZE 32U
#define TR_WAL_TRANSACTION_HEADER_SIZE 48U
#define TR_WAL_OPERATION_HEADER_SIZE 8U
#define TR_WAL_SEGMENT_MAGIC "TRSEG001"
#define TR_WAL_TRANSACTION_MAGIC "TRWAL001"
#define TR_WAL_SNAPSHOT_MAGIC "TRSNP001"
#define TR_WAL_SNAPSHOT_HEADER_SIZE 48U

enum tr_wal_operation_type {
    TR_WAL_OP_HARD_STATE = 1,
    TR_WAL_OP_TRUNCATE = 2,
    TR_WAL_OP_ENTRY = 3,
    TR_WAL_OP_COMMIT_INDEX = 4,
    TR_WAL_OP_SNAPSHOT = 5
};

struct tr_raft_wal_storage {
    char path_prefix[SALTS_FS_MAX_PATH];
    char current_path[SALTS_FS_MAX_PATH];
    salts_file_t lock_file;
    salts_file_t current_file;
    uint8_t *transaction;
    size_t transaction_capacity;
    size_t transaction_used;
    size_t segment_bytes;
    size_t max_segments;
    size_t max_log_entries;
    uint64_t max_snapshot_bytes;
    size_t current_segment;
    uint64_t current_offset;
    uint64_t last_transaction_id;
    tr_raft_term_t term;
    tr_raft_node_id_t voted_for;
    tr_raft_index_t commit_index;
    tr_raft_index_t last_log_index;
    tr_raft_index_t snapshot_index;
    tr_raft_term_t snapshot_term;
    tr_raft_term_t pending_term;
    tr_raft_node_id_t pending_voted_for;
    tr_raft_index_t pending_commit_index;
    tr_raft_index_t pending_last_log_index;
    int transaction_active;
    int faulted;
};

static void tr_wal_put_u32(uint8_t *output, uint32_t value)
{
    output[0] = (uint8_t)value;
    output[1] = (uint8_t)(value >> 8U);
    output[2] = (uint8_t)(value >> 16U);
    output[3] = (uint8_t)(value >> 24U);
}

static void tr_wal_put_u64(uint8_t *output, uint64_t value)
{
    size_t index;
    for (index = 0U; index < 8U; ++index) {
        output[index] = (uint8_t)(value >> (index * 8U));
    }
}

static uint32_t tr_wal_get_u32(const uint8_t *input)
{
    return (uint32_t)input[0] | (uint32_t)input[1] << 8U |
           (uint32_t)input[2] << 16U | (uint32_t)input[3] << 24U;
}

static uint64_t tr_wal_get_u64(const uint8_t *input)
{
    uint64_t value = 0U;
    size_t index;
    for (index = 0U; index < 8U; ++index) {
        value |= (uint64_t)input[index] << (index * 8U);
    }
    return value;
}

static int tr_wal_segment_path(const tr_raft_wal_storage_t *storage,
                               size_t sequence, char *path, size_t path_size)
{
    int length = snprintf(path, path_size, "%s.%08zu.wal",
                          storage->path_prefix, sequence);
    return length < 0 || (size_t)length >= path_size ? SALTS_ENAMETOOLONG
                                                     : SALTS_OK;
}

static int tr_wal_snapshot_path(const tr_raft_wal_storage_t *storage,
                                tr_raft_index_t index,
                                tr_raft_term_t term,
                                const char *suffix,
                                char *path, size_t path_size)
{
    int length = snprintf(path, path_size, "%s.snapshot.%llu.%llu%s",
                          storage->path_prefix,
                          (unsigned long long)index,
                          (unsigned long long)term, suffix);
    return length < 0 || (size_t)length >= path_size ? SALTS_ENAMETOOLONG
                                                     : SALTS_OK;
}

static int tr_wal_write_all(salts_file_t file, const uint8_t *data, size_t size)
{
    size_t offset = 0U;
    while (offset < size) {
        int written = salts_fs_write(file, (const char *)data + offset,
                                     size - offset);
        if (written <= 0) return written < 0 ? written : SALTS_EIO;
        offset += (size_t)written;
    }
    return SALTS_OK;
}

static int tr_wal_read_exact(salts_file_t file, uint64_t offset,
                             uint8_t *data, size_t size)
{
    size_t used = 0U;
    while (used < size) {
        int count = salts_fs_pread(file, (char *)data + used, size - used,
                                   (int64_t)(offset + used));
        if (count <= 0) return count < 0 ? count : SALTS_EIO;
        used += (size_t)count;
    }
    return SALTS_OK;
}

typedef struct tr_wal_snapshot_file_source {
    char path[SALTS_FS_MAX_PATH];
    uint64_t size;
} tr_wal_snapshot_file_source_t;

typedef struct tr_wal_memory_snapshot_source {
    const uint8_t *data;
    size_t size;
} tr_wal_memory_snapshot_source_t;

static int tr_wal_memory_snapshot_read(
    void *context,
    uint64_t offset,
    uint8_t *buffer,
    size_t capacity,
    size_t *out_size)
{
    tr_wal_memory_snapshot_source_t *source =
        (tr_wal_memory_snapshot_source_t *)context;

    if (source == NULL || out_size == NULL ||
        offset > source->size ||
        capacity > source->size - (size_t)offset ||
        (capacity != 0U && buffer == NULL)) {
        return SALTS_EINVAL;
    }
    if (capacity != 0U) {
        memcpy(buffer, source->data + (size_t)offset, capacity);
    }
    *out_size = capacity;
    return SALTS_OK;
}

static int tr_wal_snapshot_file_source_read(
    void *context,
    uint64_t offset,
    uint8_t *buffer,
    size_t capacity,
    size_t *out_size)
{
    tr_wal_snapshot_file_source_t *source =
        (tr_wal_snapshot_file_source_t *)context;
    salts_file_t file;
    int result;

    if (source == NULL || out_size == NULL ||
        offset > source->size ||
        capacity > source->size - offset ||
        (capacity != 0U && buffer == NULL)) {
        return SALTS_EINVAL;
    }
    *out_size = 0U;
    if (capacity == 0U) {
        return SALTS_OK;
    }

    file = salts_fs_open(source->path, SALTS_FS_O_RDONLY, 0);
    if (file == SALTS_INVALID_FILE) {
        return SALTS_EIO;
    }
    result = tr_wal_read_exact(
        file, TR_WAL_SNAPSHOT_HEADER_SIZE + offset, buffer, capacity);
    {
        int close_result = salts_fs_close(file);
        if (result == SALTS_OK) {
            result = close_result;
        }
    }
    if (result == SALTS_OK) {
        *out_size = capacity;
    }
    return result;
}

static void tr_wal_snapshot_file_source_release(void *context)
{
    free(context);
}

static int tr_wal_snapshot_file_header(
    tr_raft_wal_storage_t *storage,
    tr_raft_index_t index,
    tr_raft_term_t term,
    uint64_t expected_size,
    uint64_t *out_checksum,
    char *out_path,
    size_t out_path_size)
{
    uint8_t header[TR_WAL_SNAPSHOT_HEADER_SIZE];
    salts_fs_stat_t stat;
    salts_file_t file;
    int result;

    if (storage == NULL || out_checksum == NULL || out_path == NULL) {
        return SALTS_EINVAL;
    }
    result = tr_wal_snapshot_path(
        storage, index, term, "", out_path, out_path_size);
    if (result != SALTS_OK) {
        return result;
    }
    result = salts_fs_stat(out_path, &stat);
    if (result != SALTS_OK || !stat.is_file ||
        stat.size != TR_WAL_SNAPSHOT_HEADER_SIZE + expected_size) {
        return SALTS_EPROTO;
    }
    file = salts_fs_open(out_path, SALTS_FS_O_RDONLY, 0);
    if (file == SALTS_INVALID_FILE) {
        return SALTS_EIO;
    }
    result = tr_wal_read_exact(file, 0U, header, sizeof(header));
    {
        int close_result = salts_fs_close(file);
        if (result == SALTS_OK) {
            result = close_result;
        }
    }
    if (result != SALTS_OK) {
        return result;
    }
    if (memcmp(header, TR_WAL_SNAPSHOT_MAGIC, 8U) != 0 ||
        tr_wal_get_u32(header + 8U) != TR_WAL_FORMAT_VERSION ||
        tr_wal_get_u32(header + 12U) != TR_WAL_SNAPSHOT_HEADER_SIZE ||
        tr_wal_get_u64(header + 16U) != index ||
        tr_wal_get_u64(header + 24U) != term ||
        tr_wal_get_u64(header + 32U) != expected_size) {
        return SALTS_EPROTO;
    }
    *out_checksum = tr_wal_get_u64(header + 40U);
    return SALTS_OK;
}

static int tr_wal_read_snapshot_file(
    tr_raft_wal_storage_t *storage,
    tr_raft_wal_recovery_t *recovery,
    uint64_t expected_checksum,
    const uint8_t *expected_digest,
    size_t expected_digest_size)
{
    tr_wal_snapshot_file_source_t *source_context = NULL;
    XXH3_state_t *xxh_state = NULL;
    SHA256_CTX sha;
    uint8_t digest[TR_RAFT_WIRE_SNAPSHOT_DIGEST_SIZE];
    uint8_t *buffer = NULL;
    char path[SALTS_FS_MAX_PATH];
    uint64_t header_checksum = 0U;
    uint64_t offset = 0U;
    int result;

    if (storage == NULL || recovery == NULL ||
        (expected_digest_size != 0U &&
         expected_digest_size != TR_RAFT_WIRE_SNAPSHOT_DIGEST_SIZE) ||
        (expected_digest_size != 0U && expected_digest == NULL)) {
        return SALTS_EINVAL;
    }

    result = tr_wal_snapshot_file_header(
        storage, recovery->snapshot_index, recovery->snapshot_term,
        recovery->snapshot_size, &header_checksum, path, sizeof(path));
    if (result != SALTS_OK || header_checksum != expected_checksum) {
        return result == SALTS_OK ? SALTS_EPROTO : result;
    }

    buffer = (uint8_t *)malloc(TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES);
    xxh_state = XXH3_createState();
    if (buffer == NULL || xxh_state == NULL) {
        result = SALTS_ENOMEM;
        goto cleanup;
    }
    if (XXH3_64bits_reset(xxh_state) != XXH_OK ||
        SHA256_Init(&sha) != 1) {
        result = SALTS_EIO;
        goto cleanup;
    }

    while (offset < recovery->snapshot_size) {
        size_t request = TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES;
        uint64_t remaining64 = recovery->snapshot_size - offset;
        size_t remaining =
            remaining64 > (uint64_t)SIZE_MAX ? SIZE_MAX : (size_t)remaining64;
        salts_file_t file;

        if (request > remaining) {
            request = remaining;
        }
        file = salts_fs_open(path, SALTS_FS_O_RDONLY, 0);
        if (file == SALTS_INVALID_FILE) {
            result = SALTS_EIO;
            goto cleanup;
        }
        result = tr_wal_read_exact(
            file, TR_WAL_SNAPSHOT_HEADER_SIZE + offset, buffer, request);
        {
            int close_result = salts_fs_close(file);
            if (result == SALTS_OK) {
                result = close_result;
            }
        }
        if (result != SALTS_OK) {
            goto cleanup;
        }
        if (XXH3_64bits_update(xxh_state, buffer, request) != XXH_OK ||
            SHA256_Update(&sha, buffer, request) != 1) {
            result = SALTS_EIO;
            goto cleanup;
        }
        offset += request;
    }

    if (XXH3_64bits_digest(xxh_state) != expected_checksum ||
        SHA256_Final(digest, &sha) != 1) {
        result = SALTS_EPROTO;
        goto cleanup;
    }
    if (expected_digest_size != 0U &&
        memcmp(digest, expected_digest, expected_digest_size) != 0) {
        result = SALTS_EPROTO;
        goto cleanup;
    }

    source_context =
        (tr_wal_snapshot_file_source_t *)calloc(1U, sizeof(*source_context));
    if (source_context == NULL) {
        result = SALTS_ENOMEM;
        goto cleanup;
    }
    memcpy(source_context->path, path, strlen(path) + 1U);
    source_context->size = recovery->snapshot_size;

    memset(&recovery->snapshot_source, 0, sizeof(recovery->snapshot_source));
    recovery->snapshot_source.context = source_context;
    recovery->snapshot_source.size = recovery->snapshot_size;
    memcpy(recovery->snapshot_source.digest, digest, sizeof(digest));
    recovery->snapshot_source.read_at = tr_wal_snapshot_file_source_read;
    recovery->snapshot_source.release = tr_wal_snapshot_file_source_release;
    source_context = NULL;
    result = SALTS_OK;

cleanup:
    free(source_context);
    if (xxh_state != NULL) {
        XXH3_freeState(xxh_state);
    }
    free(buffer);
    return result;
}

static int tr_wal_write_snapshot_source_file(
    tr_raft_wal_storage_t *storage,
    tr_raft_index_t index,
    tr_raft_term_t term,
    const tr_raft_snapshot_source_t *source,
    uint64_t *out_checksum)
{
    uint8_t header[TR_WAL_SNAPSHOT_HEADER_SIZE] = {0};
    char temporary_path[SALTS_FS_MAX_PATH];
    char final_path[SALTS_FS_MAX_PATH];
    XXH3_state_t *xxh_state = NULL;
    SHA256_CTX sha;
    uint8_t digest[TR_RAFT_WIRE_SNAPSHOT_DIGEST_SIZE];
    uint8_t *buffer = NULL;
    salts_file_t file = SALTS_INVALID_FILE;
    uint64_t offset = 0U;
    uint64_t checksum = 0U;
    int result;

    if (storage == NULL || source == NULL || source->read_at == NULL ||
        out_checksum == NULL ||
        source->size > storage->max_snapshot_bytes) {
        return SALTS_EINVAL;
    }

    result = tr_wal_snapshot_path(
        storage, index, term, ".tmp", temporary_path, sizeof(temporary_path));
    if (result == SALTS_OK) {
        result = tr_wal_snapshot_path(
            storage, index, term, "", final_path, sizeof(final_path));
    }
    if (result != SALTS_OK) {
        return result;
    }

    if (salts_fs_access(final_path, SALTS_FS_ACCESS_EXISTS) == SALTS_OK) {
        tr_raft_wal_recovery_t existing;
        uint64_t existing_checksum = 0U;

        memset(&existing, 0, sizeof(existing));
        existing.snapshot_index = index;
        existing.snapshot_term = term;
        existing.snapshot_size = source->size;
        result = tr_wal_snapshot_file_header(
            storage, index, term, source->size, &existing_checksum,
            final_path, sizeof(final_path));
        if (result == SALTS_OK) {
            result = tr_wal_read_snapshot_file(
                storage, &existing, existing_checksum,
                source->digest, sizeof(source->digest));
        }
        if (existing.snapshot_source.release != NULL) {
            existing.snapshot_source.release(existing.snapshot_source.context);
        }
        if (result == SALTS_OK) {
            *out_checksum = existing_checksum;
        }
        return result;
    }

    file = salts_fs_open(
        temporary_path,
        SALTS_FS_O_RDWR | SALTS_FS_O_CREAT | SALTS_FS_O_TRUNC,
        SALTS_FS_DEFAULT_MODE);
    if (file == SALTS_INVALID_FILE) {
        return SALTS_EIO;
    }

    buffer = (uint8_t *)malloc(TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES);
    xxh_state = XXH3_createState();
    if (buffer == NULL || xxh_state == NULL ||
        XXH3_64bits_reset(xxh_state) != XXH_OK ||
        SHA256_Init(&sha) != 1) {
        result = SALTS_ENOMEM;
        goto cleanup_write;
    }

    result = tr_wal_write_all(file, header, sizeof(header));
    while (result == SALTS_OK && offset < source->size) {
        size_t request = TR_RAFT_WIRE_MAX_SNAPSHOT_CHUNK_BYTES;
        uint64_t remaining64 = source->size - offset;
        size_t read_size = 0U;

        if (remaining64 < request) {
            request = (size_t)remaining64;
        }
        result = source->read_at(
            source->context, offset, buffer, request, &read_size);
        if (result != SALTS_OK) {
            break;
        }
        if (read_size == 0U || read_size > request) {
            result = SALTS_EPROTO;
            break;
        }
        if (XXH3_64bits_update(xxh_state, buffer, read_size) != XXH_OK ||
            SHA256_Update(&sha, buffer, read_size) != 1) {
            result = SALTS_EIO;
            break;
        }
        result = tr_wal_write_all(file, buffer, read_size);
        offset += read_size;
    }

    if (result == SALTS_OK && offset != source->size) {
        result = SALTS_EPROTO;
    }
    if (result == SALTS_OK) {
        checksum = XXH3_64bits_digest(xxh_state);
        if (SHA256_Final(digest, &sha) != 1 ||
            memcmp(digest, source->digest, sizeof(digest)) != 0) {
            result = SALTS_EPROTO;
        }
    }
    if (result == SALTS_OK) {
        memcpy(header, TR_WAL_SNAPSHOT_MAGIC, 8U);
        tr_wal_put_u32(header + 8U, TR_WAL_FORMAT_VERSION);
        tr_wal_put_u32(header + 12U, TR_WAL_SNAPSHOT_HEADER_SIZE);
        tr_wal_put_u64(header + 16U, index);
        tr_wal_put_u64(header + 24U, term);
        tr_wal_put_u64(header + 32U, source->size);
        tr_wal_put_u64(header + 40U, checksum);
        if (salts_fs_pwrite(
                file, (const char *)header, sizeof(header), 0) !=
            (int)sizeof(header)) {
            result = SALTS_EIO;
        }
    }
    if (result == SALTS_OK) {
        result = salts_fs_fsync(file);
    }

cleanup_write:
    {
        int close_result = salts_fs_close(file);
        if (result == SALTS_OK) {
            result = close_result;
        }
    }
    if (xxh_state != NULL) {
        XXH3_freeState(xxh_state);
    }
    free(buffer);

    if (result == SALTS_OK) {
        result = salts_fs_rename(temporary_path, final_path);
    }
    if (result != SALTS_OK) {
        (void)salts_fs_unlink(temporary_path);
        return result;
    }
    *out_checksum = checksum;
    return SALTS_OK;
}

static uint64_t tr_wal_segment_checksum(const uint8_t *header)
{
    return XXH3_64bits(header, 24U);
}

static uint64_t tr_wal_transaction_checksum(const uint8_t *frame,
                                            size_t payload_size)
{
    uint64_t seed = tr_wal_get_u64(frame + 16U) ^
                    tr_wal_get_u64(frame + 24U) ^
                    tr_wal_get_u64(frame + 32U);
    return XXH3_64bits_withSeed(frame + TR_WAL_TRANSACTION_HEADER_SIZE,
                               payload_size, seed);
}

static int tr_wal_create_segment(tr_raft_wal_storage_t *storage,
                                 size_t sequence)
{
    uint8_t header[TR_WAL_SEGMENT_HEADER_SIZE] = {0};
    salts_file_t file;
    int result = tr_wal_segment_path(storage, sequence, storage->current_path,
                                     sizeof(storage->current_path));
    if (result != SALTS_OK) return result;
    file = salts_fs_open(storage->current_path,
                         SALTS_FS_O_RDWR | SALTS_FS_O_CREAT |
                             SALTS_FS_O_TRUNC,
                         SALTS_FS_DEFAULT_MODE);
    if (file == SALTS_INVALID_FILE) return SALTS_EIO;
    memcpy(header, TR_WAL_SEGMENT_MAGIC, 8U);
    tr_wal_put_u32(header + 8U, TR_WAL_FORMAT_VERSION);
    tr_wal_put_u32(header + 12U, TR_WAL_SEGMENT_HEADER_SIZE);
    tr_wal_put_u64(header + 16U, sequence);
    tr_wal_put_u64(header + 24U, tr_wal_segment_checksum(header));
    result = tr_wal_write_all(file, header, sizeof(header));
    if (result == SALTS_OK) result = salts_fs_fsync(file);
    if (result != SALTS_OK) {
        salts_fs_close(file);
        return result;
    }
    storage->current_file = file;
    storage->current_segment = sequence;
    storage->current_offset = sizeof(header);
    return SALTS_OK;
}

static int tr_wal_append_operation(tr_raft_wal_storage_t *storage,
                                   uint8_t type, const uint8_t *payload,
                                   size_t payload_size)
{
    size_t required;
    uint8_t *operation;
    if (!storage->transaction_active || storage->faulted) return SALTS_EPROTO;
    if (payload_size > UINT32_MAX ||
        payload_size > SIZE_MAX - TR_WAL_OPERATION_HEADER_SIZE ||
        storage->transaction_used >
            storage->transaction_capacity - TR_WAL_OPERATION_HEADER_SIZE ||
        payload_size > storage->transaction_capacity -
                           storage->transaction_used -
                           TR_WAL_OPERATION_HEADER_SIZE) {
        return SALTS_ENOSPC;
    }
    required = TR_WAL_OPERATION_HEADER_SIZE + payload_size;
    operation = storage->transaction + storage->transaction_used;
    memset(operation, 0, TR_WAL_OPERATION_HEADER_SIZE);
    operation[0] = type;
    tr_wal_put_u32(operation + 4U, (uint32_t)payload_size);
    if (payload_size != 0U) {
        memcpy(operation + TR_WAL_OPERATION_HEADER_SIZE, payload,
               payload_size);
    }
    storage->transaction_used += required;
    return SALTS_OK;
}

static int tr_wal_begin(void *context)
{
    tr_raft_wal_storage_t *storage = (tr_raft_wal_storage_t *)context;
    if (storage == NULL) return SALTS_EINVAL;
    if (storage->faulted) return SALTS_EIO;
    if (storage->transaction_active) return SALTS_EBUSY;
    storage->transaction_used = TR_WAL_TRANSACTION_HEADER_SIZE;
    storage->pending_term = storage->term;
    storage->pending_voted_for = storage->voted_for;
    storage->pending_commit_index = storage->commit_index;
    storage->pending_last_log_index = storage->last_log_index;
    storage->transaction_active = 1;
    return SALTS_OK;
}

static int tr_wal_write_hard_state(void *context, tr_raft_term_t term,
                                   tr_raft_node_id_t voted_for)
{
    tr_raft_wal_storage_t *storage = (tr_raft_wal_storage_t *)context;
    uint8_t payload[16U];
    int result;
    if (storage == NULL || term < storage->pending_term) return SALTS_EINVAL;
    tr_wal_put_u64(payload, term);
    tr_wal_put_u64(payload + 8U, voted_for);
    result = tr_wal_append_operation(storage, TR_WAL_OP_HARD_STATE,
                                     payload, sizeof(payload));
    if (result == SALTS_OK) {
        storage->pending_term = term;
        storage->pending_voted_for = voted_for;
    }
    return result;
}

static int tr_wal_truncate_log(void *context, tr_raft_index_t from_index)
{
    tr_raft_wal_storage_t *storage = (tr_raft_wal_storage_t *)context;
    uint8_t payload[8U];
    int result;
    if (storage == NULL || from_index == 0U ||
        from_index > storage->pending_last_log_index + 1U ||
        from_index <= storage->pending_commit_index ||
        from_index <= storage->snapshot_index) {
        return SALTS_EPROTO;
    }
    tr_wal_put_u64(payload, from_index);
    result = tr_wal_append_operation(storage, TR_WAL_OP_TRUNCATE,
                                     payload, sizeof(payload));
    if (result == SALTS_OK) storage->pending_last_log_index = from_index - 1U;
    return result;
}

static int tr_wal_append_log(void *context, const tr_raft_entry_t *entries,
                             size_t entry_count)
{
    tr_raft_wal_storage_t *storage = (tr_raft_wal_storage_t *)context;
    size_t index;
    if (storage == NULL || (entry_count != 0U && entries == NULL))
        return SALTS_EINVAL;
    for (index = 0U; index < entry_count; ++index) {
        const tr_raft_entry_t *entry = &entries[index];
        uint8_t payload[32U + TR_RAFT_MAX_ENTRY_BYTES];
        int result;
        if (entry->index != storage->pending_last_log_index + 1U ||
            entry->term == 0U || entry->data_length > TR_RAFT_MAX_ENTRY_BYTES ||
            storage->pending_last_log_index - storage->snapshot_index >=
                storage->max_log_entries) {
            return SALTS_EPROTO;
        }
        tr_wal_put_u64(payload, entry->index);
        tr_wal_put_u64(payload + 8U, entry->term);
        tr_wal_put_u64(payload + 16U, entry->command_id);
        tr_wal_put_u32(payload + 24U, (uint32_t)entry->data_length);
        tr_wal_put_u32(payload + 28U, 0U);
        if (entry->data_length != 0U) {
            memcpy(payload + 32U, entry->data, entry->data_length);
        }
        result = tr_wal_append_operation(storage, TR_WAL_OP_ENTRY, payload,
                                         32U + entry->data_length);
        if (result != SALTS_OK) return result;
        storage->pending_last_log_index = entry->index;
    }
    return SALTS_OK;
}

static int tr_wal_write_commit_index(void *context,
                                     tr_raft_index_t commit_index)
{
    tr_raft_wal_storage_t *storage = (tr_raft_wal_storage_t *)context;
    uint8_t payload[8U];
    int result;
    if (storage == NULL || commit_index < storage->pending_commit_index ||
        commit_index > storage->pending_last_log_index) return SALTS_EPROTO;
    tr_wal_put_u64(payload, commit_index);
    result = tr_wal_append_operation(storage, TR_WAL_OP_COMMIT_INDEX,
                                     payload, sizeof(payload));
    if (result == SALTS_OK) storage->pending_commit_index = commit_index;
    return result;
}

static int tr_wal_commit(void *context)
{
    tr_raft_wal_storage_t *storage = (tr_raft_wal_storage_t *)context;
    size_t frame_size;
    size_t payload_size;
    uint64_t transaction_id;
    int result;
    if (storage == NULL || !storage->transaction_active) return SALTS_EINVAL;
    if (storage->faulted) return SALTS_EIO;
    frame_size = storage->transaction_used;
    payload_size = frame_size - TR_WAL_TRANSACTION_HEADER_SIZE;
    transaction_id = storage->last_transaction_id + 1U;
    memset(storage->transaction, 0, TR_WAL_TRANSACTION_HEADER_SIZE);
    memcpy(storage->transaction, TR_WAL_TRANSACTION_MAGIC, 8U);
    tr_wal_put_u32(storage->transaction + 8U, TR_WAL_FORMAT_VERSION);
    tr_wal_put_u32(storage->transaction + 12U,
                   TR_WAL_TRANSACTION_HEADER_SIZE);
    tr_wal_put_u64(storage->transaction + 16U, transaction_id);
    tr_wal_put_u64(storage->transaction + 24U,
                   storage->last_transaction_id);
    tr_wal_put_u64(storage->transaction + 32U, payload_size);
    tr_wal_put_u64(storage->transaction + 40U,
                   tr_wal_transaction_checksum(storage->transaction,
                                               payload_size));
    if (frame_size > storage->segment_bytes - TR_WAL_SEGMENT_HEADER_SIZE) {
        return SALTS_ENOSPC;
    }
    if (frame_size > storage->segment_bytes - storage->current_offset) {
        if (storage->current_segment >= storage->max_segments) {
            return SALTS_ENOSPC;
        }
        result = salts_fs_close(storage->current_file);
        storage->current_file = SALTS_INVALID_FILE;
        if (result == SALTS_OK) {
            result = tr_wal_create_segment(storage,
                                           storage->current_segment + 1U);
        }
        if (result != SALTS_OK) {
            storage->faulted = 1;
            return result;
        }
    }
    result = tr_wal_write_all(storage->current_file, storage->transaction,
                              frame_size);
    if (result == SALTS_OK) result = salts_fs_fsync(storage->current_file);
    if (result != SALTS_OK) {
        storage->faulted = 1;
        return result;
    }
    storage->current_offset += frame_size;
    storage->last_transaction_id = transaction_id;
    storage->term = storage->pending_term;
    storage->voted_for = storage->pending_voted_for;
    storage->commit_index = storage->pending_commit_index;
    storage->last_log_index = storage->pending_last_log_index;
    storage->transaction_active = 0;
    storage->transaction_used = 0U;
    return SALTS_OK;
}

static int tr_wal_rollback(void *context)
{
    tr_raft_wal_storage_t *storage = (tr_raft_wal_storage_t *)context;
    if (storage == NULL) return SALTS_EINVAL;
    storage->transaction_active = 0;
    storage->transaction_used = 0U;
    return storage->faulted ? SALTS_EIO : SALTS_OK;
}

static int tr_wal_apply_payload(tr_raft_wal_storage_t *storage,
                                const uint8_t *payload, size_t payload_size,
                                tr_raft_wal_recovery_t *recovery,
                                size_t max_log_entries)
{
    size_t offset = 0U;
    while (offset < payload_size) {
        uint8_t type;
        uint32_t size;
        const uint8_t *data;
        if (payload_size - offset < TR_WAL_OPERATION_HEADER_SIZE)
            return SALTS_EPROTO;
        type = payload[offset];
        if (payload[offset + 1U] != 0U || payload[offset + 2U] != 0U ||
            payload[offset + 3U] != 0U) return SALTS_EPROTO;
        size = tr_wal_get_u32(payload + offset + 4U);
        offset += TR_WAL_OPERATION_HEADER_SIZE;
        if ((size_t)size > payload_size - offset) return SALTS_EPROTO;
        data = payload + offset;
        if (type == TR_WAL_OP_HARD_STATE) {
            tr_raft_term_t term;
            if (size != 16U) return SALTS_EPROTO;
            term = tr_wal_get_u64(data);
            if (term < recovery->term) return SALTS_EPROTO;
            recovery->term = term;
            recovery->voted_for = tr_wal_get_u64(data + 8U);
        } else if (type == TR_WAL_OP_TRUNCATE) {
            tr_raft_index_t from_index;
            if (size != 8U) return SALTS_EPROTO;
            from_index = tr_wal_get_u64(data);
            if (from_index == 0U || from_index <= recovery->commit_index ||
                from_index <= recovery->snapshot_index ||
                from_index > recovery->snapshot_index +
                                 recovery->entry_count + 1U)
                return SALTS_EPROTO;
            recovery->entry_count =
                (size_t)(from_index - recovery->snapshot_index - 1U);
        } else if (type == TR_WAL_OP_ENTRY) {
            tr_raft_entry_t *entry;
            uint32_t data_length;
            if (size < 32U) return SALTS_EPROTO;
            data_length = tr_wal_get_u32(data + 24U);
            if (tr_wal_get_u32(data + 28U) != 0U ||
                data_length > TR_RAFT_MAX_ENTRY_BYTES ||
                size != 32U + data_length ||
                recovery->entry_count >= max_log_entries)
                return SALTS_EPROTO;
            entry = &recovery->entries[recovery->entry_count];
            memset(entry, 0, sizeof(*entry));
            entry->index = tr_wal_get_u64(data);
            entry->term = tr_wal_get_u64(data + 8U);
            entry->command_id = tr_wal_get_u64(data + 16U);
            entry->data_length = data_length;
            if (entry->index != recovery->snapshot_index +
                                    recovery->entry_count + 1U ||
                entry->term == 0U) return SALTS_EPROTO;
            if (data_length != 0U)
                memcpy(entry->data, data + 32U, data_length);
            ++recovery->entry_count;
        } else if (type == TR_WAL_OP_COMMIT_INDEX) {
            tr_raft_index_t commit_index;
            if (size != 8U) return SALTS_EPROTO;
            commit_index = tr_wal_get_u64(data);
            if (commit_index < recovery->commit_index ||
                commit_index > recovery->snapshot_index +
                                   recovery->entry_count)
                return SALTS_EPROTO;
            recovery->commit_index = commit_index;
        } else if (type == TR_WAL_OP_SNAPSHOT) {
            tr_raft_term_t leader_term;
            tr_raft_index_t snapshot_index;
            tr_raft_term_t snapshot_term;
            uint64_t snapshot_size;
            uint64_t snapshot_checksum;
            tr_raft_index_t commit_index;
            tr_raft_node_id_t voted_for;
            uint32_t configuration_size;
            uint32_t digest_size;
            const uint8_t *snapshot_digest = NULL;
            size_t suffix_offset = 0U;
            size_t suffix_count = 0U;
            int result;
            if (size < 64U) return SALTS_EPROTO;
            leader_term = tr_wal_get_u64(data);
            snapshot_index = tr_wal_get_u64(data + 8U);
            snapshot_term = tr_wal_get_u64(data + 16U);
            snapshot_size = tr_wal_get_u64(data + 24U);
            snapshot_checksum = tr_wal_get_u64(data + 32U);
            commit_index = tr_wal_get_u64(data + 40U);
            voted_for = tr_wal_get_u64(data + 48U);
            configuration_size = tr_wal_get_u32(data + 56U);
            digest_size = tr_wal_get_u32(data + 60U);
            if ((digest_size != 0U &&
                 digest_size != TR_RAFT_WIRE_SNAPSHOT_DIGEST_SIZE) ||
                configuration_size > size - 64U ||
                digest_size > size - 64U - configuration_size ||
                size != 64U + configuration_size + digest_size ||
                leader_term == 0U ||
                snapshot_index == 0U || snapshot_term == 0U ||
                snapshot_term > leader_term || leader_term < recovery->term ||
                snapshot_index <= recovery->snapshot_index ||
                snapshot_size > storage->max_snapshot_bytes ||
                commit_index < snapshot_index ||
                (commit_index != snapshot_index &&
                 commit_index > recovery->snapshot_index +
                                    recovery->entry_count))
                return SALTS_EPROTO;
            if (digest_size != 0U) {
                snapshot_digest = data + 64U + configuration_size;
            }
            if (snapshot_index > recovery->snapshot_index &&
                snapshot_index <= recovery->snapshot_index +
                                      recovery->entry_count) {
                size_t match = (size_t)(snapshot_index -
                                        recovery->snapshot_index - 1U);
                if (recovery->entries[match].term == snapshot_term) {
                    suffix_offset = match + 1U;
                    suffix_count = recovery->entry_count - suffix_offset;
                }
            }
            result = tr_raft_conf_decode(data + 64U, configuration_size,
                                         &recovery->snapshot_configuration);
            if (result != SALTS_OK) return SALTS_EPROTO;
            if (suffix_count != 0U)
                memmove(recovery->entries,
                        recovery->entries + suffix_offset,
                        suffix_count * sizeof(recovery->entries[0]));
            recovery->entry_count = suffix_count;
            free(recovery->snapshot_data);
            recovery->snapshot_data = NULL;
            if (recovery->snapshot_source.release != NULL) {
                recovery->snapshot_source.release(
                    recovery->snapshot_source.context);
            }
            memset(&recovery->snapshot_source, 0,
                   sizeof(recovery->snapshot_source));
            recovery->snapshot_index = snapshot_index;
            recovery->snapshot_term = snapshot_term;
            recovery->snapshot_size = snapshot_size;
            recovery->has_snapshot_configuration = true;
            recovery->voted_for = voted_for;
            recovery->term = leader_term;
            recovery->commit_index = commit_index;
            result = tr_wal_read_snapshot_file(
                storage, recovery, snapshot_checksum,
                snapshot_digest, digest_size);
            if (result != SALTS_OK) return result;
        } else {
            return SALTS_EPROTO;
        }
        offset += size;
    }
    return SALTS_OK;
}

static int tr_wal_replay_segment(tr_raft_wal_storage_t *storage,
                                 size_t sequence, int is_first, int is_last,
                                 tr_raft_wal_recovery_t *recovery,
                                 uint64_t *last_transaction_id,
                                 uint64_t *out_valid_size)
{
    char path[SALTS_FS_MAX_PATH];
    salts_fs_stat_t stat;
    salts_file_t file;
    uint8_t segment_header[TR_WAL_SEGMENT_HEADER_SIZE];
    uint64_t offset = TR_WAL_SEGMENT_HEADER_SIZE;
    int result = tr_wal_segment_path(storage, sequence, path, sizeof(path));
    if (result != SALTS_OK) return result;
    result = salts_fs_stat(path, &stat);
    if (result != SALTS_OK || !stat.is_file ||
        stat.size < TR_WAL_SEGMENT_HEADER_SIZE ||
        stat.size > storage->segment_bytes) return SALTS_EPROTO;
    file = salts_fs_open(path, SALTS_FS_O_RDONLY, 0);
    if (file == SALTS_INVALID_FILE) return SALTS_EIO;
    result = tr_wal_read_exact(file, 0U, segment_header,
                               sizeof(segment_header));
    if (result == SALTS_OK &&
        (memcmp(segment_header, TR_WAL_SEGMENT_MAGIC, 8U) != 0 ||
         tr_wal_get_u32(segment_header + 8U) != TR_WAL_FORMAT_VERSION ||
         tr_wal_get_u32(segment_header + 12U) !=
             TR_WAL_SEGMENT_HEADER_SIZE ||
         tr_wal_get_u64(segment_header + 16U) != sequence ||
         tr_wal_get_u64(segment_header + 24U) !=
             tr_wal_segment_checksum(segment_header))) {
        result = SALTS_EPROTO;
    }
    while (result == SALTS_OK && offset < stat.size) {
        uint8_t header[TR_WAL_TRANSACTION_HEADER_SIZE];
        uint64_t transaction_id;
        uint64_t previous_transaction_id;
        uint64_t payload_size;
        uint8_t *frame;
        if (stat.size - offset < sizeof(header)) {
            result = is_last ? SALTS_OK : SALTS_EPROTO;
            break;
        }
        result = tr_wal_read_exact(file, offset, header, sizeof(header));
        if (result != SALTS_OK) break;
        if (memcmp(header, TR_WAL_TRANSACTION_MAGIC, 8U) != 0 ||
            tr_wal_get_u32(header + 8U) != TR_WAL_FORMAT_VERSION ||
            tr_wal_get_u32(header + 12U) !=
                TR_WAL_TRANSACTION_HEADER_SIZE) {
            result = SALTS_EPROTO;
            break;
        }
        transaction_id = tr_wal_get_u64(header + 16U);
        previous_transaction_id = tr_wal_get_u64(header + 24U);
        payload_size = tr_wal_get_u64(header + 32U);
        if (is_first && offset == TR_WAL_SEGMENT_HEADER_SIZE &&
            *last_transaction_id == 0U && previous_transaction_id != 0U)
            *last_transaction_id = previous_transaction_id;
        if (transaction_id != *last_transaction_id + 1U ||
            previous_transaction_id != *last_transaction_id ||
            payload_size > storage->transaction_capacity -
                               TR_WAL_TRANSACTION_HEADER_SIZE) {
            result = SALTS_EPROTO;
            break;
        }
        if (payload_size > stat.size - offset - sizeof(header)) {
            result = is_last ? SALTS_OK : SALTS_EPROTO;
            break;
        }
        frame = storage->transaction;
        memcpy(frame, header, sizeof(header));
        result = tr_wal_read_exact(file, offset + sizeof(header),
                                   frame + sizeof(header),
                                   (size_t)payload_size);
        if (result != SALTS_OK) break;
        if (tr_wal_get_u64(header + 40U) !=
            tr_wal_transaction_checksum(frame, (size_t)payload_size)) {
            result = SALTS_EPROTO;
            break;
        }
        result = tr_wal_apply_payload(storage, frame + sizeof(header),
                                      (size_t)payload_size, recovery,
                                      storage->max_log_entries);
        if (result != SALTS_OK) break;
        if (is_first && offset == TR_WAL_SEGMENT_HEADER_SIZE &&
            previous_transaction_id != 0U && recovery->snapshot_index == 0U) {
            result = SALTS_EPROTO;
            break;
        }
        *last_transaction_id = transaction_id;
        offset += sizeof(header) + payload_size;
    }
    salts_fs_close(file);
    *out_valid_size = offset;
    return result;
}

static int tr_wal_replay(tr_raft_wal_storage_t *storage,
                         tr_raft_wal_recovery_t *recovery,
                         size_t *out_last_segment, uint64_t *out_valid_size,
                         uint64_t *out_last_transaction_id)
{
    size_t first_segment = 0U;
    size_t last_segment = 0U;
    size_t sequence;
    char path[SALTS_FS_MAX_PATH];
    int result;
    memset(recovery, 0, sizeof(*recovery));
    recovery->entries = (tr_raft_entry_t *)calloc(
        storage->max_log_entries, sizeof(*recovery->entries));
    if (recovery->entries == NULL) return SALTS_ENOMEM;
    for (sequence = 1U; sequence <= storage->max_segments; ++sequence) {
        result = tr_wal_segment_path(storage, sequence, path, sizeof(path));
        if (result != SALTS_OK) goto fail;
        if (salts_fs_access(path, SALTS_FS_ACCESS_EXISTS) == SALTS_OK) {
            first_segment = sequence;
            break;
        }
    }
    if (first_segment == 0U) {
        *out_last_segment = 0U;
        *out_valid_size = 0U;
        *out_last_transaction_id = 0U;
        return SALTS_OK;
    }
    last_segment = first_segment;
    for (sequence = first_segment + 1U; sequence <= storage->max_segments;
         ++sequence) {
        result = tr_wal_segment_path(storage, sequence, path, sizeof(path));
        if (result != SALTS_OK) goto fail;
        if (salts_fs_access(path, SALTS_FS_ACCESS_EXISTS) != SALTS_OK) break;
        last_segment = sequence;
    }
    for (sequence = first_segment; sequence <= last_segment; ++sequence) {
        result = tr_wal_replay_segment(storage, sequence,
                                       sequence == first_segment,
                                       sequence == last_segment, recovery,
                                       out_last_transaction_id,
                                       out_valid_size);
        if (result != SALTS_OK) goto fail;
    }
    *out_last_segment = last_segment;
    return SALTS_OK;
fail:
    tr_raft_wal_recovery_destroy(recovery);
    return result;
}

int tr_raft_wal_storage_open(const tr_raft_wal_storage_config_t *config,
                             tr_raft_wal_storage_t **out_storage)
{
    tr_raft_wal_storage_t *storage;
    tr_raft_wal_recovery_t recovery;
    char lock_path[SALTS_FS_MAX_PATH];
    size_t prefix_length;
    size_t segment_bytes;
    size_t transaction_bytes;
    size_t last_segment = 0U;
    uint64_t valid_size = 0U;
    uint64_t last_transaction_id = 0U;
    int result;
    if (out_storage == NULL) return SALTS_EINVAL;
    *out_storage = NULL;
    if (config == NULL || config->path_prefix == NULL ||
        config->path_prefix[0] == '\0' || config->max_log_entries == 0U ||
        config->max_segments == 0U ||
        config->max_segments > TR_RAFT_WAL_MAX_SEGMENTS) return SALTS_EINVAL;
    segment_bytes = config->segment_bytes == 0U
                        ? TR_RAFT_WAL_DEFAULT_SEGMENT_BYTES
                        : config->segment_bytes;
    transaction_bytes = config->max_transaction_bytes == 0U
                            ? TR_RAFT_WAL_DEFAULT_TRANSACTION_BYTES
                            : config->max_transaction_bytes;
    if (segment_bytes < TR_RAFT_WAL_MIN_SEGMENT_BYTES ||
        segment_bytes > INT_MAX ||
        transaction_bytes < TR_WAL_TRANSACTION_HEADER_SIZE ||
        transaction_bytes > segment_bytes - TR_WAL_SEGMENT_HEADER_SIZE)
        return SALTS_EINVAL;
    prefix_length = strlen(config->path_prefix);
    if (prefix_length + 64U >= SALTS_FS_MAX_PATH) return SALTS_ENAMETOOLONG;
    storage = (tr_raft_wal_storage_t *)calloc(1U, sizeof(*storage));
    if (storage == NULL) return SALTS_ENOMEM;
    memcpy(storage->path_prefix, config->path_prefix, prefix_length + 1U);
    storage->lock_file = SALTS_INVALID_FILE;
    storage->current_file = SALTS_INVALID_FILE;
    storage->segment_bytes = segment_bytes;
    storage->transaction_capacity = transaction_bytes;
    storage->max_segments = config->max_segments;
    storage->max_log_entries = config->max_log_entries;
    storage->max_snapshot_bytes = config->max_snapshot_bytes == 0U
                                      ? TR_RAFT_WAL_DEFAULT_MAX_SNAPSHOT_BYTES
                                      : config->max_snapshot_bytes;
    storage->transaction = (uint8_t *)malloc(transaction_bytes);
    if (storage->transaction == NULL) {
        free(storage);
        return SALTS_ENOMEM;
    }
    snprintf(lock_path, sizeof(lock_path), "%s.lock", storage->path_prefix);
    storage->lock_file = salts_fs_open(lock_path,
                                       SALTS_FS_O_RDWR | SALTS_FS_O_CREAT,
                                       SALTS_FS_DEFAULT_MODE);
    if (storage->lock_file == SALTS_INVALID_FILE ||
        salts_fs_lock(storage->lock_file,
                      SALTS_FS_LOCK_EXCLUSIVE | SALTS_FS_LOCK_NONBLOCK,
                      0, 0U) != SALTS_OK) {
        tr_raft_wal_storage_close(storage);
        return SALTS_EBUSY;
    }
    result = tr_wal_replay(storage, &recovery, &last_segment, &valid_size,
                           &last_transaction_id);
    if (result != SALTS_OK) {
        tr_raft_wal_storage_close(storage);
        return result;
    }
    storage->term = recovery.term;
    storage->voted_for = recovery.voted_for;
    storage->commit_index = recovery.commit_index;
    storage->snapshot_index = recovery.snapshot_index;
    storage->snapshot_term = recovery.snapshot_term;
    storage->last_log_index = recovery.snapshot_index + recovery.entry_count;
    storage->last_transaction_id = last_transaction_id;
    tr_raft_wal_recovery_destroy(&recovery);
    if (last_segment == 0U) {
        if (!config->create_if_missing) {
            tr_raft_wal_storage_close(storage);
            return SALTS_ENOENT;
        }
        result = tr_wal_create_segment(storage, 1U);
    } else {
        result = tr_wal_segment_path(storage, last_segment,
                                     storage->current_path,
                                     sizeof(storage->current_path));
        if (result == SALTS_OK) {
            storage->current_file = salts_fs_open(
                storage->current_path, SALTS_FS_O_RDWR, 0);
            if (storage->current_file == SALTS_INVALID_FILE) result = SALTS_EIO;
        }
        if (result == SALTS_OK) {
            result = salts_fs_ftruncate(storage->current_file,
                                        (int64_t)valid_size);
        }
        if (result == SALTS_OK &&
            salts_fs_seek(storage->current_file, (int64_t)valid_size,
                          SEEK_SET) < 0) result = SALTS_EIO;
        storage->current_segment = last_segment;
        storage->current_offset = valid_size;
    }
    if (result != SALTS_OK) {
        tr_raft_wal_storage_close(storage);
        return result;
    }
    *out_storage = storage;
    return SALTS_OK;
}

int tr_raft_wal_storage_close(tr_raft_wal_storage_t *storage)
{
    int result = SALTS_OK;
    if (storage == NULL) return SALTS_OK;
    storage->transaction_active = 0;
    if (storage->current_file != SALTS_INVALID_FILE) {
        result = salts_fs_close(storage->current_file);
    }
    if (storage->lock_file != SALTS_INVALID_FILE) {
        int unlock_result = salts_fs_unlock(storage->lock_file, 0, 0U);
        int close_result = salts_fs_close(storage->lock_file);
        if (result == SALTS_OK) result = unlock_result;
        if (result == SALTS_OK) result = close_result;
    }
    free(storage->transaction);
    free(storage);
    return result;
}

int tr_raft_wal_storage_bind(tr_raft_wal_storage_t *storage,
                             tr_raft_storage_t *out_storage)
{
    if (storage == NULL || out_storage == NULL) return SALTS_EINVAL;
    memset(out_storage, 0, sizeof(*out_storage));
    out_storage->context = storage;
    out_storage->begin = tr_wal_begin;
    out_storage->write_hard_state = tr_wal_write_hard_state;
    out_storage->truncate_log = tr_wal_truncate_log;
    out_storage->append_log = tr_wal_append_log;
    out_storage->write_commit_index = tr_wal_write_commit_index;
    out_storage->commit = tr_wal_commit;
    out_storage->rollback = tr_wal_rollback;
    return SALTS_OK;
}

int tr_raft_wal_storage_load(tr_raft_wal_storage_t *storage,
                             tr_raft_wal_recovery_t *out_recovery)
{
    size_t last_segment = 0U;
    uint64_t valid_size = 0U;
    uint64_t last_transaction_id = 0U;
    if (storage == NULL || out_recovery == NULL) return SALTS_EINVAL;
    if (storage->transaction_active) return SALTS_EBUSY;
    return tr_wal_replay(storage, out_recovery, &last_segment, &valid_size,
                         &last_transaction_id);
}

static int tr_wal_persist_snapshot_source(
    tr_raft_wal_storage_t *storage,
    tr_raft_term_t leader_term,
    tr_raft_index_t snapshot_index,
    tr_raft_term_t snapshot_term,
    const tr_raft_conf_t *configuration,
    const tr_raft_snapshot_source_t *source,
    int remote_install)
{
    tr_raft_wal_recovery_t recovery;
    uint8_t payload[
        64U + TR_RAFT_CONF_MAX_ENCODED_SIZE +
        TR_RAFT_WIRE_SNAPSHOT_DIGEST_SIZE];
    size_t configuration_size = 0U;
    tr_raft_index_t recovered_last_index;
    tr_raft_index_t committed_index;
    tr_raft_index_t retained_last_index;
    tr_raft_index_t previous_snapshot_index;
    tr_raft_term_t previous_snapshot_term;
    uint64_t checksum = 0U;
    size_t checkpoint_segment;
    int matching_entry = 0;
    int checkpoint_compaction;
    int result;

    if (storage == NULL || configuration == NULL || source == NULL ||
        source->read_at == NULL || leader_term == 0U ||
        snapshot_index == 0U || snapshot_term == 0U ||
        snapshot_term > leader_term) {
        return SALTS_EINVAL;
    }
    if (storage->faulted) {
        return SALTS_EIO;
    }
    if (storage->transaction_active) {
        return SALTS_EBUSY;
    }
    if (source->size > storage->max_snapshot_bytes) {
        return SALTS_ERANGE;
    }

    memset(&recovery, 0, sizeof(recovery));
    previous_snapshot_index = storage->snapshot_index;
    previous_snapshot_term = storage->snapshot_term;
    result = tr_raft_wal_storage_load(storage, &recovery);
    if (result != SALTS_OK) {
        return result;
    }

    recovered_last_index = recovery.snapshot_index + recovery.entry_count;
    if (leader_term < recovery.term ||
        snapshot_index <= recovery.snapshot_index ||
        (remote_install && snapshot_index < recovery.commit_index) ||
        (!remote_install && snapshot_index > recovery.commit_index)) {
        result = SALTS_EPROTO;
        goto cleanup;
    }

    if (snapshot_index > recovery.snapshot_index &&
        snapshot_index <= recovered_last_index) {
        size_t entry_offset =
            (size_t)(snapshot_index - recovery.snapshot_index - 1U);
        matching_entry =
            recovery.entries[entry_offset].term == snapshot_term;
    }
    if (!remote_install && !matching_entry) {
        result = SALTS_EPROTO;
        goto cleanup;
    }

    committed_index = remote_install ? snapshot_index : recovery.commit_index;
    retained_last_index = matching_entry ? recovered_last_index
                                         : snapshot_index;
    checkpoint_compaction = retained_last_index == snapshot_index;

    result = tr_raft_conf_encode(
        configuration, payload + 64U, TR_RAFT_CONF_MAX_ENCODED_SIZE,
        &configuration_size);
    if (result != SALTS_OK) {
        goto cleanup;
    }

    result = tr_wal_write_snapshot_source_file(
        storage, snapshot_index, snapshot_term, source, &checksum);
    if (result != SALTS_OK) {
        goto cleanup;
    }

    if (checkpoint_compaction &&
        storage->current_offset != TR_WAL_SEGMENT_HEADER_SIZE) {
        if (storage->current_segment >= storage->max_segments) {
            result = SALTS_ENOSPC;
            goto cleanup;
        }
        result = salts_fs_close(storage->current_file);
        storage->current_file = SALTS_INVALID_FILE;
        if (result == SALTS_OK) {
            result = tr_wal_create_segment(
                storage, storage->current_segment + 1U);
        }
        if (result != SALTS_OK) {
            storage->faulted = 1;
            goto cleanup;
        }
    }

    checkpoint_segment = storage->current_segment;
    memset(payload, 0, 64U);
    tr_wal_put_u64(payload, leader_term);
    tr_wal_put_u64(payload + 8U, snapshot_index);
    tr_wal_put_u64(payload + 16U, snapshot_term);
    tr_wal_put_u64(payload + 24U, source->size);
    tr_wal_put_u64(payload + 32U, checksum);
    tr_wal_put_u64(payload + 40U, committed_index);
    tr_wal_put_u64(
        payload + 48U,
        leader_term > storage->term ? 0U : storage->voted_for);
    tr_wal_put_u32(payload + 56U, (uint32_t)configuration_size);
    tr_wal_put_u32(
        payload + 60U, TR_RAFT_WIRE_SNAPSHOT_DIGEST_SIZE);
    memcpy(
        payload + 64U + configuration_size, source->digest,
        TR_RAFT_WIRE_SNAPSHOT_DIGEST_SIZE);

    result = tr_wal_begin(storage);
    if (result == SALTS_OK) {
        result = tr_wal_append_operation(
            storage, TR_WAL_OP_SNAPSHOT, payload,
            64U + configuration_size +
                TR_RAFT_WIRE_SNAPSHOT_DIGEST_SIZE);
    }
    if (result == SALTS_OK) {
        storage->pending_term = leader_term;
        if (leader_term > storage->term) {
            storage->pending_voted_for = 0U;
        }
        storage->pending_commit_index = committed_index;
        storage->pending_last_log_index = retained_last_index;
        result = tr_wal_commit(storage);
    } else if (storage->transaction_active) {
        tr_wal_rollback(storage);
    }

    if (result == SALTS_OK) {
        size_t sequence;

        storage->snapshot_index = snapshot_index;
        storage->snapshot_term = snapshot_term;
        for (sequence = 1U;
             checkpoint_compaction && sequence < checkpoint_segment;
             ++sequence) {
            char path[SALTS_FS_MAX_PATH];

            result = tr_wal_segment_path(
                storage, sequence, path, sizeof(path));
            if (result == SALTS_OK &&
                salts_fs_access(path, SALTS_FS_ACCESS_EXISTS) == SALTS_OK) {
                result = salts_fs_unlink(path);
            }
            if (result != SALTS_OK) {
                storage->faulted = 1;
                break;
            }
        }
        if (result == SALTS_OK && previous_snapshot_index != 0U) {
            char path[SALTS_FS_MAX_PATH];

            result = tr_wal_snapshot_path(
                storage, previous_snapshot_index, previous_snapshot_term,
                "", path, sizeof(path));
            if (result == SALTS_OK) {
                result = salts_fs_unlink(path);
            }
            if (result != SALTS_OK) {
                storage->faulted = 1;
            }
        }
    }

cleanup:
    tr_raft_wal_recovery_destroy(&recovery);
    return result;
}

int tr_raft_wal_storage_store_snapshot_source(
    tr_raft_wal_storage_t *storage,
    tr_raft_index_t last_included_index,
    tr_raft_term_t last_included_term,
    const tr_raft_conf_t *configuration,
    const tr_raft_snapshot_source_t *source)
{
    return tr_wal_persist_snapshot_source(
        storage, storage != NULL ? storage->term : 0U,
        last_included_index, last_included_term, configuration, source, 0);
}

int tr_raft_wal_storage_store_snapshot(
    tr_raft_wal_storage_t *storage,
    tr_raft_index_t last_included_index,
    tr_raft_term_t last_included_term,
    const tr_raft_conf_t *configuration,
    const void *data,
    size_t size)
{
    tr_wal_memory_snapshot_source_t memory;
    tr_raft_snapshot_source_t source;

    if (size != 0U && data == NULL) {
        return SALTS_EINVAL;
    }
    memset(&memory, 0, sizeof(memory));
    memset(&source, 0, sizeof(source));
    memory.data = (const uint8_t *)data;
    memory.size = size;
    source.context = &memory;
    source.size = size;
    SHA256(
        size != 0U ? (const uint8_t *)data : (const uint8_t *)"",
        size, source.digest);
    source.read_at = tr_wal_memory_snapshot_read;
    return tr_raft_wal_storage_store_snapshot_source(
        storage, last_included_index, last_included_term,
        configuration, &source);
}

int tr_raft_wal_storage_install_snapshot_source(
    tr_raft_wal_storage_t *storage,
    tr_raft_term_t leader_term,
    tr_raft_index_t last_included_index,
    tr_raft_term_t last_included_term,
    const tr_raft_conf_t *configuration,
    const tr_raft_snapshot_source_t *source)
{
    return tr_wal_persist_snapshot_source(
        storage, leader_term, last_included_index, last_included_term,
        configuration, source, 1);
}

int tr_raft_wal_storage_install_snapshot(
    tr_raft_wal_storage_t *storage,
    tr_raft_term_t leader_term,
    tr_raft_index_t last_included_index,
    tr_raft_term_t last_included_term,
    const tr_raft_conf_t *configuration,
    const void *data,
    size_t size)
{
    tr_wal_memory_snapshot_source_t memory;
    tr_raft_snapshot_source_t source;

    if (size != 0U && data == NULL) {
        return SALTS_EINVAL;
    }
    memset(&memory, 0, sizeof(memory));
    memset(&source, 0, sizeof(source));
    memory.data = (const uint8_t *)data;
    memory.size = size;
    source.context = &memory;
    source.size = size;
    SHA256(
        size != 0U ? (const uint8_t *)data : (const uint8_t *)"",
        size, source.digest);
    source.read_at = tr_wal_memory_snapshot_read;
    return tr_raft_wal_storage_install_snapshot_source(
        storage, leader_term, last_included_index, last_included_term,
        configuration, &source);
}

void tr_raft_wal_recovery_destroy(tr_raft_wal_recovery_t *recovery)
{
    if (recovery == NULL) return;
    if (recovery->snapshot_source.release != NULL) {
        recovery->snapshot_source.release(recovery->snapshot_source.context);
    }
    free(recovery->snapshot_data);
    free(recovery->entries);
    memset(recovery, 0, sizeof(*recovery));
}
