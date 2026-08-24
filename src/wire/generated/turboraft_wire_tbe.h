#ifndef TurboRaftWire_GENERATED_H
#define TurboRaftWire_GENERATED_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifndef TBE_GENERATED_API
#if defined(_WIN32) && defined(TBE_GENERATED_BUILD_SHARED)
#define TBE_GENERATED_API __declspec(dllexport)
#elif defined(_WIN32) && defined(TBE_GENERATED_USE_SHARED)
#define TBE_GENERATED_API __declspec(dllimport)
#elif defined(__GNUC__) && defined(TBE_GENERATED_BUILD_SHARED)
#define TBE_GENERATED_API __attribute__((visibility("default")))
#else
#define TBE_GENERATED_API
#endif
#endif

#include "tbe_wire.h"
#if defined(TBE_WASM_GUEST)
static inline void *tbe_generated_memcpy(void *destination, const void *source, size_t size) {
    unsigned char *out = (unsigned char *)destination;
    const unsigned char *in = (const unsigned char *)source;
    size_t index;
    for (index = 0; index < size; ++index) out[index] = in[index];
    return destination;
}

static inline int tbe_generated_strcmp(const char *left, const char *right) {
    while (*left != '\0' && *left == *right) {
        ++left;
        ++right;
    }
    return (int)(unsigned char)*left - (int)(unsigned char)*right;
}

static inline size_t tbe_generated_strlen(const char *text) {
    const char *end = text;
    while (*end != '\0') ++end;
    return (size_t)(end - text);
}

#define TBE_GENERATED_MEMCPY tbe_generated_memcpy
#define TBE_GENERATED_STRCMP tbe_generated_strcmp
#define TBE_GENERATED_STRLEN tbe_generated_strlen
#ifndef TURBO_UUID_H
#define TURBO_UUID_H
#define TURBO_UUID_SIZE 16U
typedef struct turbo_uuid_s {
    uint8_t bytes[TURBO_UUID_SIZE];
} turbo_uuid_t;
#endif
#else
#include <string.h>
#include "turbo_uuid.h"
#define TBE_GENERATED_MEMCPY memcpy
#define TBE_GENERATED_STRCMP strcmp
#define TBE_GENERATED_STRLEN strlen
#endif

#include "tbe_typed.h"


#ifndef TBE_SCHEMA_CODEC_V1_DEFINED
#define TBE_SCHEMA_CODEC_V1_DEFINED

enum { TBE_SCHEMA_CODEC_ABI_VERSION = 1 };

typedef enum tbe_schema_format_e {
    TBE_SCHEMA_FORMAT_JSON = DATA_BIND_FORMAT_JSON,
    TBE_SCHEMA_FORMAT_YAML = DATA_BIND_FORMAT_YAML,
    TBE_SCHEMA_FORMAT_CSV = DATA_BIND_FORMAT_CSV,
    TBE_SCHEMA_FORMAT_XML = DATA_BIND_FORMAT_XML
} tbe_schema_format_t;

/**
 * @brief Schema-specific native codec used behind a trusted host provider.
 *
 * text_to_binary_into writes only to caller-owned bounded storage. Text output
 * is allocated and transferred to the caller, which releases it with
 * free_output. A codec instance is not assumed to be thread-safe; the host
 * provider owns synchronization and resource quotas.
 */
typedef struct tbe_schema_codec_v1_s {
    size_t struct_size;
    uint32_t abi_version;
    const char *schema_id;
    DataBindStatus (*create)(DataBind **out_codec, DataBindError *error);
    DataBindStatus (*text_to_binary_into)(DataBind *codec, const char *type_name,
                                          uint32_t format, const void *input,
                                          size_t input_len, size_t csv_row,
                                          uint8_t *output, size_t output_capacity,
                                          size_t *out_len, DataBindError *error);
    DataBindStatus (*binary_to_text)(DataBind *codec, const char *type_name,
                                     uint32_t format, const void *input,
                                     size_t input_len, char **out,
                                     size_t *out_len, DataBindError *error);
    void (*free_output)(void *output);
} tbe_schema_codec_v1_t;

#endif /* TBE_SCHEMA_CODEC_V1_DEFINED */

#ifdef __cplusplus
extern "C" {
#endif

/* Schema TurboRaftWire  */
enum { TurboRaftWire_WIRE_BIG_ENDIAN = 0 };
TBE_GENERATED_API DataBindStatus TurboRaftWire_codec_create(DataBind **out_codec, DataBindError *error);
TBE_GENERATED_API const char *TurboRaftWire_schema_text(void);
TBE_GENERATED_API const tbe_schema_codec_v1_t *TurboRaftWire_schema_codec(void);

/* ========================================================================= */
/* Enums                                                                     */
/* ========================================================================= */





/* ========================================================================= */
/* Records                                                                   */
/* ========================================================================= */

/**
 * @brief message RaftWireMessage
 * Attributes: 
 */
typedef struct RaftWireMessage_s {
    /* size: 1 unsigned */
    uint8_t message_type;
    /* size: 1 unsigned */
    uint8_t granted;
    /* size: 2 unsigned */
    uint16_t reserved;
    /* size: 4 unsigned */
    uint32_t entry_count;
    /* size: 8 unsigned */
    uint64_t from_node;
    /* size: 8 unsigned */
    uint64_t to_node;
    /* size: 8 unsigned */
    uint64_t term;
    /* size: 8 unsigned */
    uint64_t campaign_term;
    /* size: 8 unsigned */
    uint64_t last_log_index;
    /* size: 8 unsigned */
    uint64_t last_log_term;
    /* size: 8 unsigned */
    uint64_t leader_commit;
    /* size: 8 unsigned */
    uint64_t previous_log_index;
    /* size: 8 unsigned */
    uint64_t previous_log_term;
    /* size: 8 unsigned */
    uint64_t match_index;
    /* size: 8 unsigned */
    uint64_t reject_hint;
    /* size: 8 unsigned */
    uint64_t entry_index;
    /* size: 8 unsigned */
    uint64_t entry_term;
    /* size: 8 unsigned */
    uint64_t entry_command_id;
    /* size:   */
    tbe_bytes_t entry_data;
} RaftWireMessage_t;

/** Owning lifecycle and schema serialization API; serialized buffers use tbe_typed_serialized_free. */
TBE_GENERATED_API void RaftWireMessage_init(RaftWireMessage_t *object);
TBE_GENERATED_API void RaftWireMessage_clear(RaftWireMessage_t *object);
TBE_GENERATED_API DataBindStatus RaftWireMessage_from_bin(DataBind *codec, RaftWireMessage_t *object, const void *data, size_t len, DataBindError *error);
TBE_GENERATED_API DataBindStatus RaftWireMessage_from_json(DataBind *codec, RaftWireMessage_t *object, const char *data, size_t len, DataBindError *error);
TBE_GENERATED_API DataBindStatus RaftWireMessage_from_yaml(DataBind *codec, RaftWireMessage_t *object, const char *data, size_t len, DataBindError *error);
TBE_GENERATED_API DataBindStatus RaftWireMessage_from_csv(DataBind *codec, RaftWireMessage_t *object, const char *data, size_t len, size_t row, DataBindError *error);
TBE_GENERATED_API DataBindStatus RaftWireMessage_from_xml(DataBind *codec, RaftWireMessage_t *object, const char *data, size_t len, DataBindError *error);
TBE_GENERATED_API DataBindStatus RaftWireMessage_to_bin(const RaftWireMessage_t *object, uint8_t **out, size_t *out_len, DataBindError *error);
TBE_GENERATED_API DataBindStatus RaftWireMessage_to_bin_into(const RaftWireMessage_t *object, uint8_t *output, size_t output_capacity, size_t *out_len, DataBindError *error);
TBE_GENERATED_API DataBindStatus RaftWireMessage_to_json(DataBind *codec, const RaftWireMessage_t *object, char **out, size_t *out_len, DataBindError *error);
TBE_GENERATED_API DataBindStatus RaftWireMessage_to_yaml(DataBind *codec, const RaftWireMessage_t *object, char **out, size_t *out_len, DataBindError *error);
TBE_GENERATED_API DataBindStatus RaftWireMessage_to_csv(DataBind *codec, const RaftWireMessage_t *object, char **out, size_t *out_len, DataBindError *error);
TBE_GENERATED_API DataBindStatus RaftWireMessage_to_xml(DataBind *codec, const RaftWireMessage_t *object, char **out, size_t *out_len, DataBindError *error);

typedef struct RaftWireMessage_view_s {
    const uint8_t *data;
    size_t size;
} RaftWireMessage_view_t;

typedef struct RaftWireMessage_builder_s {
    uint8_t *data;
    size_t size;
} RaftWireMessage_builder_t;


enum { RaftWireMessage_BLOCK_LENGTH = 120 };

enum { RaftWireMessage_message_type_OFFSET = 0 };

enum { RaftWireMessage_granted_OFFSET = 1 };

enum { RaftWireMessage_reserved_OFFSET = 2 };

enum { RaftWireMessage_entry_count_OFFSET = 4 };

enum { RaftWireMessage_from_node_OFFSET = 8 };

enum { RaftWireMessage_to_node_OFFSET = 16 };

enum { RaftWireMessage_term_OFFSET = 24 };

enum { RaftWireMessage_campaign_term_OFFSET = 32 };

enum { RaftWireMessage_last_log_index_OFFSET = 40 };

enum { RaftWireMessage_last_log_term_OFFSET = 48 };

enum { RaftWireMessage_leader_commit_OFFSET = 56 };

enum { RaftWireMessage_previous_log_index_OFFSET = 64 };

enum { RaftWireMessage_previous_log_term_OFFSET = 72 };

enum { RaftWireMessage_match_index_OFFSET = 80 };

enum { RaftWireMessage_reject_hint_OFFSET = 88 };

enum { RaftWireMessage_entry_index_OFFSET = 96 };

enum { RaftWireMessage_entry_term_OFFSET = 104 };

enum { RaftWireMessage_entry_command_id_OFFSET = 112 };


static inline bool RaftWireMessage_view_bind(RaftWireMessage_view_t *view, const void *data, size_t size) {
    if (!view || !data || size < 120) {
        return false;
    }

    view->data = (const uint8_t *)data;
    view->size = size;
    return true;
}

static inline bool RaftWireMessage_builder_bind(RaftWireMessage_builder_t *view, void *data, size_t size) {
    if (!view || !data || size < 120) {
        return false;
    }

    view->data = (uint8_t *)data;
    view->size = size;
    return true;
}



static inline bool RaftWireMessage_entry_data_set(RaftWireMessage_builder_t *view,
                                               const void *data,
                                               size_t size) {
    size_t payload_offset = RaftWireMessage_BLOCK_LENGTH;

    if (!view || !view->data || view->size < payload_offset) {
        return false;
    }

    return tbe_wire_write_var_data(view->data + payload_offset,
                                   view->size - payload_offset,
                                   TurboRaftWire_WIRE_BIG_ENDIAN,
                                   data,
                                   size);
}

static inline bool RaftWireMessage_entry_data(const RaftWireMessage_view_t *view,
                                           tbe_var_data_t *value) {
    size_t payload_offset = RaftWireMessage_BLOCK_LENGTH;

    if (!view || !value || view->size < payload_offset) {
        return false;
    }

    return tbe_wire_read_var_data(view->data + payload_offset,
                                  view->size - payload_offset,
                                  TurboRaftWire_WIRE_BIG_ENDIAN,
                                  value);
}



static inline bool RaftWireMessage_message_type_set(RaftWireMessage_builder_t *view,
                                                uint8_t value) {
    if (!view || !view->data || view->size < 0 + 1) {
        return false;
    }

    tbe_wire_write_u8(view->data + 0, TurboRaftWire_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint8_t RaftWireMessage_message_type_get(const RaftWireMessage_view_t *view) {
    return tbe_wire_read_u8(view->data + 0, TurboRaftWire_WIRE_BIG_ENDIAN);
}




static inline bool RaftWireMessage_granted_set(RaftWireMessage_builder_t *view,
                                                uint8_t value) {
    if (!view || !view->data || view->size < 1 + 1) {
        return false;
    }

    tbe_wire_write_u8(view->data + 1, TurboRaftWire_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint8_t RaftWireMessage_granted_get(const RaftWireMessage_view_t *view) {
    return tbe_wire_read_u8(view->data + 1, TurboRaftWire_WIRE_BIG_ENDIAN);
}




static inline bool RaftWireMessage_reserved_set(RaftWireMessage_builder_t *view,
                                                uint16_t value) {
    if (!view || !view->data || view->size < 2 + 2) {
        return false;
    }

    tbe_wire_write_u16(view->data + 2, TurboRaftWire_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint16_t RaftWireMessage_reserved_get(const RaftWireMessage_view_t *view) {
    return tbe_wire_read_u16(view->data + 2, TurboRaftWire_WIRE_BIG_ENDIAN);
}




static inline bool RaftWireMessage_entry_count_set(RaftWireMessage_builder_t *view,
                                                uint32_t value) {
    if (!view || !view->data || view->size < 4 + 4) {
        return false;
    }

    tbe_wire_write_u32(view->data + 4, TurboRaftWire_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint32_t RaftWireMessage_entry_count_get(const RaftWireMessage_view_t *view) {
    return tbe_wire_read_u32(view->data + 4, TurboRaftWire_WIRE_BIG_ENDIAN);
}




static inline bool RaftWireMessage_from_node_set(RaftWireMessage_builder_t *view,
                                                uint64_t value) {
    if (!view || !view->data || view->size < 8 + 8) {
        return false;
    }

    tbe_wire_write_u64(view->data + 8, TurboRaftWire_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint64_t RaftWireMessage_from_node_get(const RaftWireMessage_view_t *view) {
    return tbe_wire_read_u64(view->data + 8, TurboRaftWire_WIRE_BIG_ENDIAN);
}




static inline bool RaftWireMessage_to_node_set(RaftWireMessage_builder_t *view,
                                                uint64_t value) {
    if (!view || !view->data || view->size < 16 + 8) {
        return false;
    }

    tbe_wire_write_u64(view->data + 16, TurboRaftWire_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint64_t RaftWireMessage_to_node_get(const RaftWireMessage_view_t *view) {
    return tbe_wire_read_u64(view->data + 16, TurboRaftWire_WIRE_BIG_ENDIAN);
}




static inline bool RaftWireMessage_term_set(RaftWireMessage_builder_t *view,
                                                uint64_t value) {
    if (!view || !view->data || view->size < 24 + 8) {
        return false;
    }

    tbe_wire_write_u64(view->data + 24, TurboRaftWire_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint64_t RaftWireMessage_term_get(const RaftWireMessage_view_t *view) {
    return tbe_wire_read_u64(view->data + 24, TurboRaftWire_WIRE_BIG_ENDIAN);
}




static inline bool RaftWireMessage_campaign_term_set(RaftWireMessage_builder_t *view,
                                                uint64_t value) {
    if (!view || !view->data || view->size < 32 + 8) {
        return false;
    }

    tbe_wire_write_u64(view->data + 32, TurboRaftWire_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint64_t RaftWireMessage_campaign_term_get(const RaftWireMessage_view_t *view) {
    return tbe_wire_read_u64(view->data + 32, TurboRaftWire_WIRE_BIG_ENDIAN);
}




static inline bool RaftWireMessage_last_log_index_set(RaftWireMessage_builder_t *view,
                                                uint64_t value) {
    if (!view || !view->data || view->size < 40 + 8) {
        return false;
    }

    tbe_wire_write_u64(view->data + 40, TurboRaftWire_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint64_t RaftWireMessage_last_log_index_get(const RaftWireMessage_view_t *view) {
    return tbe_wire_read_u64(view->data + 40, TurboRaftWire_WIRE_BIG_ENDIAN);
}




static inline bool RaftWireMessage_last_log_term_set(RaftWireMessage_builder_t *view,
                                                uint64_t value) {
    if (!view || !view->data || view->size < 48 + 8) {
        return false;
    }

    tbe_wire_write_u64(view->data + 48, TurboRaftWire_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint64_t RaftWireMessage_last_log_term_get(const RaftWireMessage_view_t *view) {
    return tbe_wire_read_u64(view->data + 48, TurboRaftWire_WIRE_BIG_ENDIAN);
}




static inline bool RaftWireMessage_leader_commit_set(RaftWireMessage_builder_t *view,
                                                uint64_t value) {
    if (!view || !view->data || view->size < 56 + 8) {
        return false;
    }

    tbe_wire_write_u64(view->data + 56, TurboRaftWire_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint64_t RaftWireMessage_leader_commit_get(const RaftWireMessage_view_t *view) {
    return tbe_wire_read_u64(view->data + 56, TurboRaftWire_WIRE_BIG_ENDIAN);
}




static inline bool RaftWireMessage_previous_log_index_set(RaftWireMessage_builder_t *view,
                                                uint64_t value) {
    if (!view || !view->data || view->size < 64 + 8) {
        return false;
    }

    tbe_wire_write_u64(view->data + 64, TurboRaftWire_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint64_t RaftWireMessage_previous_log_index_get(const RaftWireMessage_view_t *view) {
    return tbe_wire_read_u64(view->data + 64, TurboRaftWire_WIRE_BIG_ENDIAN);
}




static inline bool RaftWireMessage_previous_log_term_set(RaftWireMessage_builder_t *view,
                                                uint64_t value) {
    if (!view || !view->data || view->size < 72 + 8) {
        return false;
    }

    tbe_wire_write_u64(view->data + 72, TurboRaftWire_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint64_t RaftWireMessage_previous_log_term_get(const RaftWireMessage_view_t *view) {
    return tbe_wire_read_u64(view->data + 72, TurboRaftWire_WIRE_BIG_ENDIAN);
}




static inline bool RaftWireMessage_match_index_set(RaftWireMessage_builder_t *view,
                                                uint64_t value) {
    if (!view || !view->data || view->size < 80 + 8) {
        return false;
    }

    tbe_wire_write_u64(view->data + 80, TurboRaftWire_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint64_t RaftWireMessage_match_index_get(const RaftWireMessage_view_t *view) {
    return tbe_wire_read_u64(view->data + 80, TurboRaftWire_WIRE_BIG_ENDIAN);
}




static inline bool RaftWireMessage_reject_hint_set(RaftWireMessage_builder_t *view,
                                                uint64_t value) {
    if (!view || !view->data || view->size < 88 + 8) {
        return false;
    }

    tbe_wire_write_u64(view->data + 88, TurboRaftWire_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint64_t RaftWireMessage_reject_hint_get(const RaftWireMessage_view_t *view) {
    return tbe_wire_read_u64(view->data + 88, TurboRaftWire_WIRE_BIG_ENDIAN);
}




static inline bool RaftWireMessage_entry_index_set(RaftWireMessage_builder_t *view,
                                                uint64_t value) {
    if (!view || !view->data || view->size < 96 + 8) {
        return false;
    }

    tbe_wire_write_u64(view->data + 96, TurboRaftWire_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint64_t RaftWireMessage_entry_index_get(const RaftWireMessage_view_t *view) {
    return tbe_wire_read_u64(view->data + 96, TurboRaftWire_WIRE_BIG_ENDIAN);
}




static inline bool RaftWireMessage_entry_term_set(RaftWireMessage_builder_t *view,
                                                uint64_t value) {
    if (!view || !view->data || view->size < 104 + 8) {
        return false;
    }

    tbe_wire_write_u64(view->data + 104, TurboRaftWire_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint64_t RaftWireMessage_entry_term_get(const RaftWireMessage_view_t *view) {
    return tbe_wire_read_u64(view->data + 104, TurboRaftWire_WIRE_BIG_ENDIAN);
}




static inline bool RaftWireMessage_entry_command_id_set(RaftWireMessage_builder_t *view,
                                                uint64_t value) {
    if (!view || !view->data || view->size < 112 + 8) {
        return false;
    }

    tbe_wire_write_u64(view->data + 112, TurboRaftWire_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint64_t RaftWireMessage_entry_command_id_get(const RaftWireMessage_view_t *view) {
    return tbe_wire_read_u64(view->data + 112, TurboRaftWire_WIRE_BIG_ENDIAN);
}





/**
 * @brief message InstallSnapshotChunk
 * Attributes: 
 */
typedef struct InstallSnapshotChunk_s {
    /* size: 8 unsigned */
    uint64_t from_node;
    /* size: 8 unsigned */
    uint64_t to_node;
    /* size: 8 unsigned */
    uint64_t term;
    /* size: 8 unsigned */
    uint64_t snapshot_index;
    /* size: 8 unsigned */
    uint64_t snapshot_term;
    /* size: 8 unsigned */
    uint64_t snapshot_offset;
    /* size: 8 unsigned */
    uint64_t snapshot_size;
    /* size: 1 unsigned */
    uint8_t done;
    /* size:   */
    tbe_bytes_t snapshot_configuration;
    /* size:   */
    tbe_bytes_t snapshot_digest;
    /* size:   */
    tbe_bytes_t chunk_data;
} InstallSnapshotChunk_t;

/** Owning lifecycle and schema serialization API; serialized buffers use tbe_typed_serialized_free. */
TBE_GENERATED_API void InstallSnapshotChunk_init(InstallSnapshotChunk_t *object);
TBE_GENERATED_API void InstallSnapshotChunk_clear(InstallSnapshotChunk_t *object);
TBE_GENERATED_API DataBindStatus InstallSnapshotChunk_from_bin(DataBind *codec, InstallSnapshotChunk_t *object, const void *data, size_t len, DataBindError *error);
TBE_GENERATED_API DataBindStatus InstallSnapshotChunk_from_json(DataBind *codec, InstallSnapshotChunk_t *object, const char *data, size_t len, DataBindError *error);
TBE_GENERATED_API DataBindStatus InstallSnapshotChunk_from_yaml(DataBind *codec, InstallSnapshotChunk_t *object, const char *data, size_t len, DataBindError *error);
TBE_GENERATED_API DataBindStatus InstallSnapshotChunk_from_csv(DataBind *codec, InstallSnapshotChunk_t *object, const char *data, size_t len, size_t row, DataBindError *error);
TBE_GENERATED_API DataBindStatus InstallSnapshotChunk_from_xml(DataBind *codec, InstallSnapshotChunk_t *object, const char *data, size_t len, DataBindError *error);
TBE_GENERATED_API DataBindStatus InstallSnapshotChunk_to_bin(const InstallSnapshotChunk_t *object, uint8_t **out, size_t *out_len, DataBindError *error);
TBE_GENERATED_API DataBindStatus InstallSnapshotChunk_to_bin_into(const InstallSnapshotChunk_t *object, uint8_t *output, size_t output_capacity, size_t *out_len, DataBindError *error);
TBE_GENERATED_API DataBindStatus InstallSnapshotChunk_to_json(DataBind *codec, const InstallSnapshotChunk_t *object, char **out, size_t *out_len, DataBindError *error);
TBE_GENERATED_API DataBindStatus InstallSnapshotChunk_to_yaml(DataBind *codec, const InstallSnapshotChunk_t *object, char **out, size_t *out_len, DataBindError *error);
TBE_GENERATED_API DataBindStatus InstallSnapshotChunk_to_csv(DataBind *codec, const InstallSnapshotChunk_t *object, char **out, size_t *out_len, DataBindError *error);
TBE_GENERATED_API DataBindStatus InstallSnapshotChunk_to_xml(DataBind *codec, const InstallSnapshotChunk_t *object, char **out, size_t *out_len, DataBindError *error);

typedef struct InstallSnapshotChunk_view_s {
    const uint8_t *data;
    size_t size;
} InstallSnapshotChunk_view_t;

typedef struct InstallSnapshotChunk_builder_s {
    uint8_t *data;
    size_t size;
} InstallSnapshotChunk_builder_t;


enum { InstallSnapshotChunk_BLOCK_LENGTH = 57 };

enum { InstallSnapshotChunk_from_node_OFFSET = 0 };

enum { InstallSnapshotChunk_to_node_OFFSET = 8 };

enum { InstallSnapshotChunk_term_OFFSET = 16 };

enum { InstallSnapshotChunk_snapshot_index_OFFSET = 24 };

enum { InstallSnapshotChunk_snapshot_term_OFFSET = 32 };

enum { InstallSnapshotChunk_snapshot_offset_OFFSET = 40 };

enum { InstallSnapshotChunk_snapshot_size_OFFSET = 48 };

enum { InstallSnapshotChunk_done_OFFSET = 56 };


static inline bool InstallSnapshotChunk_view_bind(InstallSnapshotChunk_view_t *view, const void *data, size_t size) {
    if (!view || !data || size < 57) {
        return false;
    }

    view->data = (const uint8_t *)data;
    view->size = size;
    return true;
}

static inline bool InstallSnapshotChunk_builder_bind(InstallSnapshotChunk_builder_t *view, void *data, size_t size) {
    if (!view || !data || size < 57) {
        return false;
    }

    view->data = (uint8_t *)data;
    view->size = size;
    return true;
}



static inline bool InstallSnapshotChunk_snapshot_configuration_set(InstallSnapshotChunk_builder_t *view,
                                               const void *data,
                                               size_t size) {
    size_t payload_offset = InstallSnapshotChunk_BLOCK_LENGTH;

    if (!view || !view->data || view->size < payload_offset) {
        return false;
    }

    return tbe_wire_write_var_data(view->data + payload_offset,
                                   view->size - payload_offset,
                                   TurboRaftWire_WIRE_BIG_ENDIAN,
                                   data,
                                   size);
}

static inline bool InstallSnapshotChunk_snapshot_configuration(const InstallSnapshotChunk_view_t *view,
                                           tbe_var_data_t *value) {
    size_t payload_offset = InstallSnapshotChunk_BLOCK_LENGTH;

    if (!view || !value || view->size < payload_offset) {
        return false;
    }

    return tbe_wire_read_var_data(view->data + payload_offset,
                                  view->size - payload_offset,
                                  TurboRaftWire_WIRE_BIG_ENDIAN,
                                  value);
}


static inline bool InstallSnapshotChunk_snapshot_digest_set(InstallSnapshotChunk_builder_t *view,
                                               const void *data,
                                               size_t size) {
    InstallSnapshotChunk_view_t read_view;
    tbe_var_data_t previous;
    const uint8_t *payload_data;
    size_t payload_offset;

    if (!view || !view->data) {
        return false;
    }

    read_view.data = view->data;
    read_view.size = view->size;
    if (!InstallSnapshotChunk_snapshot_configuration(&read_view, &previous)) {
        return false;
    }

    payload_data = tbe_wire_var_data_end(&previous);
    payload_offset = (size_t)(payload_data - view->data);
    if (payload_offset > view->size) {
        return false;
    }

    return tbe_wire_write_var_data(view->data + payload_offset,
                                   view->size - payload_offset,
                                   TurboRaftWire_WIRE_BIG_ENDIAN,
                                   data,
                                   size);
}

static inline bool InstallSnapshotChunk_snapshot_digest(const InstallSnapshotChunk_view_t *view,
                                           tbe_var_data_t *value) {
    tbe_var_data_t previous;
    const uint8_t *payload_data;
    size_t payload_offset;

    if (!view || !value) {
        return false;
    }

    if (!InstallSnapshotChunk_snapshot_configuration(view, &previous)) {
        return false;
    }

    payload_data = tbe_wire_var_data_end(&previous);
    payload_offset = (size_t)(payload_data - view->data);
    if (payload_offset > view->size) {
        return false;
    }

    return tbe_wire_read_var_data(payload_data,
                                  view->size - payload_offset,
                                  TurboRaftWire_WIRE_BIG_ENDIAN,
                                  value);
}


static inline bool InstallSnapshotChunk_chunk_data_set(InstallSnapshotChunk_builder_t *view,
                                               const void *data,
                                               size_t size) {
    InstallSnapshotChunk_view_t read_view;
    tbe_var_data_t previous;
    const uint8_t *payload_data;
    size_t payload_offset;

    if (!view || !view->data) {
        return false;
    }

    read_view.data = view->data;
    read_view.size = view->size;
    if (!InstallSnapshotChunk_snapshot_digest(&read_view, &previous)) {
        return false;
    }

    payload_data = tbe_wire_var_data_end(&previous);
    payload_offset = (size_t)(payload_data - view->data);
    if (payload_offset > view->size) {
        return false;
    }

    return tbe_wire_write_var_data(view->data + payload_offset,
                                   view->size - payload_offset,
                                   TurboRaftWire_WIRE_BIG_ENDIAN,
                                   data,
                                   size);
}

static inline bool InstallSnapshotChunk_chunk_data(const InstallSnapshotChunk_view_t *view,
                                           tbe_var_data_t *value) {
    tbe_var_data_t previous;
    const uint8_t *payload_data;
    size_t payload_offset;

    if (!view || !value) {
        return false;
    }

    if (!InstallSnapshotChunk_snapshot_digest(view, &previous)) {
        return false;
    }

    payload_data = tbe_wire_var_data_end(&previous);
    payload_offset = (size_t)(payload_data - view->data);
    if (payload_offset > view->size) {
        return false;
    }

    return tbe_wire_read_var_data(payload_data,
                                  view->size - payload_offset,
                                  TurboRaftWire_WIRE_BIG_ENDIAN,
                                  value);
}



static inline bool InstallSnapshotChunk_from_node_set(InstallSnapshotChunk_builder_t *view,
                                                uint64_t value) {
    if (!view || !view->data || view->size < 0 + 8) {
        return false;
    }

    tbe_wire_write_u64(view->data + 0, TurboRaftWire_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint64_t InstallSnapshotChunk_from_node_get(const InstallSnapshotChunk_view_t *view) {
    return tbe_wire_read_u64(view->data + 0, TurboRaftWire_WIRE_BIG_ENDIAN);
}




static inline bool InstallSnapshotChunk_to_node_set(InstallSnapshotChunk_builder_t *view,
                                                uint64_t value) {
    if (!view || !view->data || view->size < 8 + 8) {
        return false;
    }

    tbe_wire_write_u64(view->data + 8, TurboRaftWire_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint64_t InstallSnapshotChunk_to_node_get(const InstallSnapshotChunk_view_t *view) {
    return tbe_wire_read_u64(view->data + 8, TurboRaftWire_WIRE_BIG_ENDIAN);
}




static inline bool InstallSnapshotChunk_term_set(InstallSnapshotChunk_builder_t *view,
                                                uint64_t value) {
    if (!view || !view->data || view->size < 16 + 8) {
        return false;
    }

    tbe_wire_write_u64(view->data + 16, TurboRaftWire_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint64_t InstallSnapshotChunk_term_get(const InstallSnapshotChunk_view_t *view) {
    return tbe_wire_read_u64(view->data + 16, TurboRaftWire_WIRE_BIG_ENDIAN);
}




static inline bool InstallSnapshotChunk_snapshot_index_set(InstallSnapshotChunk_builder_t *view,
                                                uint64_t value) {
    if (!view || !view->data || view->size < 24 + 8) {
        return false;
    }

    tbe_wire_write_u64(view->data + 24, TurboRaftWire_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint64_t InstallSnapshotChunk_snapshot_index_get(const InstallSnapshotChunk_view_t *view) {
    return tbe_wire_read_u64(view->data + 24, TurboRaftWire_WIRE_BIG_ENDIAN);
}




static inline bool InstallSnapshotChunk_snapshot_term_set(InstallSnapshotChunk_builder_t *view,
                                                uint64_t value) {
    if (!view || !view->data || view->size < 32 + 8) {
        return false;
    }

    tbe_wire_write_u64(view->data + 32, TurboRaftWire_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint64_t InstallSnapshotChunk_snapshot_term_get(const InstallSnapshotChunk_view_t *view) {
    return tbe_wire_read_u64(view->data + 32, TurboRaftWire_WIRE_BIG_ENDIAN);
}




static inline bool InstallSnapshotChunk_snapshot_offset_set(InstallSnapshotChunk_builder_t *view,
                                                uint64_t value) {
    if (!view || !view->data || view->size < 40 + 8) {
        return false;
    }

    tbe_wire_write_u64(view->data + 40, TurboRaftWire_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint64_t InstallSnapshotChunk_snapshot_offset_get(const InstallSnapshotChunk_view_t *view) {
    return tbe_wire_read_u64(view->data + 40, TurboRaftWire_WIRE_BIG_ENDIAN);
}




static inline bool InstallSnapshotChunk_snapshot_size_set(InstallSnapshotChunk_builder_t *view,
                                                uint64_t value) {
    if (!view || !view->data || view->size < 48 + 8) {
        return false;
    }

    tbe_wire_write_u64(view->data + 48, TurboRaftWire_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint64_t InstallSnapshotChunk_snapshot_size_get(const InstallSnapshotChunk_view_t *view) {
    return tbe_wire_read_u64(view->data + 48, TurboRaftWire_WIRE_BIG_ENDIAN);
}




static inline bool InstallSnapshotChunk_done_set(InstallSnapshotChunk_builder_t *view,
                                                uint8_t value) {
    if (!view || !view->data || view->size < 56 + 1) {
        return false;
    }

    tbe_wire_write_u8(view->data + 56, TurboRaftWire_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint8_t InstallSnapshotChunk_done_get(const InstallSnapshotChunk_view_t *view) {
    return tbe_wire_read_u8(view->data + 56, TurboRaftWire_WIRE_BIG_ENDIAN);
}





/**
 * @brief message InstallSnapshotAck
 * Attributes: 
 */
typedef struct InstallSnapshotAck_s {
    /* size: 8 unsigned */
    uint64_t from_node;
    /* size: 8 unsigned */
    uint64_t to_node;
    /* size: 8 unsigned */
    uint64_t term;
    /* size: 8 unsigned */
    uint64_t snapshot_index;
    /* size: 8 unsigned */
    uint64_t snapshot_size;
    /* size: 8 unsigned */
    uint64_t next_offset;
    /* size: 1 unsigned */
    uint8_t accepted;
    /* size:   */
    tbe_bytes_t snapshot_digest;
} InstallSnapshotAck_t;

/** Owning lifecycle and schema serialization API; serialized buffers use tbe_typed_serialized_free. */
TBE_GENERATED_API void InstallSnapshotAck_init(InstallSnapshotAck_t *object);
TBE_GENERATED_API void InstallSnapshotAck_clear(InstallSnapshotAck_t *object);
TBE_GENERATED_API DataBindStatus InstallSnapshotAck_from_bin(DataBind *codec, InstallSnapshotAck_t *object, const void *data, size_t len, DataBindError *error);
TBE_GENERATED_API DataBindStatus InstallSnapshotAck_from_json(DataBind *codec, InstallSnapshotAck_t *object, const char *data, size_t len, DataBindError *error);
TBE_GENERATED_API DataBindStatus InstallSnapshotAck_from_yaml(DataBind *codec, InstallSnapshotAck_t *object, const char *data, size_t len, DataBindError *error);
TBE_GENERATED_API DataBindStatus InstallSnapshotAck_from_csv(DataBind *codec, InstallSnapshotAck_t *object, const char *data, size_t len, size_t row, DataBindError *error);
TBE_GENERATED_API DataBindStatus InstallSnapshotAck_from_xml(DataBind *codec, InstallSnapshotAck_t *object, const char *data, size_t len, DataBindError *error);
TBE_GENERATED_API DataBindStatus InstallSnapshotAck_to_bin(const InstallSnapshotAck_t *object, uint8_t **out, size_t *out_len, DataBindError *error);
TBE_GENERATED_API DataBindStatus InstallSnapshotAck_to_bin_into(const InstallSnapshotAck_t *object, uint8_t *output, size_t output_capacity, size_t *out_len, DataBindError *error);
TBE_GENERATED_API DataBindStatus InstallSnapshotAck_to_json(DataBind *codec, const InstallSnapshotAck_t *object, char **out, size_t *out_len, DataBindError *error);
TBE_GENERATED_API DataBindStatus InstallSnapshotAck_to_yaml(DataBind *codec, const InstallSnapshotAck_t *object, char **out, size_t *out_len, DataBindError *error);
TBE_GENERATED_API DataBindStatus InstallSnapshotAck_to_csv(DataBind *codec, const InstallSnapshotAck_t *object, char **out, size_t *out_len, DataBindError *error);
TBE_GENERATED_API DataBindStatus InstallSnapshotAck_to_xml(DataBind *codec, const InstallSnapshotAck_t *object, char **out, size_t *out_len, DataBindError *error);

typedef struct InstallSnapshotAck_view_s {
    const uint8_t *data;
    size_t size;
} InstallSnapshotAck_view_t;

typedef struct InstallSnapshotAck_builder_s {
    uint8_t *data;
    size_t size;
} InstallSnapshotAck_builder_t;


enum { InstallSnapshotAck_BLOCK_LENGTH = 49 };

enum { InstallSnapshotAck_from_node_OFFSET = 0 };

enum { InstallSnapshotAck_to_node_OFFSET = 8 };

enum { InstallSnapshotAck_term_OFFSET = 16 };

enum { InstallSnapshotAck_snapshot_index_OFFSET = 24 };

enum { InstallSnapshotAck_snapshot_size_OFFSET = 32 };

enum { InstallSnapshotAck_next_offset_OFFSET = 40 };

enum { InstallSnapshotAck_accepted_OFFSET = 48 };


static inline bool InstallSnapshotAck_view_bind(InstallSnapshotAck_view_t *view, const void *data, size_t size) {
    if (!view || !data || size < 49) {
        return false;
    }

    view->data = (const uint8_t *)data;
    view->size = size;
    return true;
}

static inline bool InstallSnapshotAck_builder_bind(InstallSnapshotAck_builder_t *view, void *data, size_t size) {
    if (!view || !data || size < 49) {
        return false;
    }

    view->data = (uint8_t *)data;
    view->size = size;
    return true;
}



static inline bool InstallSnapshotAck_snapshot_digest_set(InstallSnapshotAck_builder_t *view,
                                               const void *data,
                                               size_t size) {
    size_t payload_offset = InstallSnapshotAck_BLOCK_LENGTH;

    if (!view || !view->data || view->size < payload_offset) {
        return false;
    }

    return tbe_wire_write_var_data(view->data + payload_offset,
                                   view->size - payload_offset,
                                   TurboRaftWire_WIRE_BIG_ENDIAN,
                                   data,
                                   size);
}

static inline bool InstallSnapshotAck_snapshot_digest(const InstallSnapshotAck_view_t *view,
                                           tbe_var_data_t *value) {
    size_t payload_offset = InstallSnapshotAck_BLOCK_LENGTH;

    if (!view || !value || view->size < payload_offset) {
        return false;
    }

    return tbe_wire_read_var_data(view->data + payload_offset,
                                  view->size - payload_offset,
                                  TurboRaftWire_WIRE_BIG_ENDIAN,
                                  value);
}



static inline bool InstallSnapshotAck_from_node_set(InstallSnapshotAck_builder_t *view,
                                                uint64_t value) {
    if (!view || !view->data || view->size < 0 + 8) {
        return false;
    }

    tbe_wire_write_u64(view->data + 0, TurboRaftWire_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint64_t InstallSnapshotAck_from_node_get(const InstallSnapshotAck_view_t *view) {
    return tbe_wire_read_u64(view->data + 0, TurboRaftWire_WIRE_BIG_ENDIAN);
}




static inline bool InstallSnapshotAck_to_node_set(InstallSnapshotAck_builder_t *view,
                                                uint64_t value) {
    if (!view || !view->data || view->size < 8 + 8) {
        return false;
    }

    tbe_wire_write_u64(view->data + 8, TurboRaftWire_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint64_t InstallSnapshotAck_to_node_get(const InstallSnapshotAck_view_t *view) {
    return tbe_wire_read_u64(view->data + 8, TurboRaftWire_WIRE_BIG_ENDIAN);
}




static inline bool InstallSnapshotAck_term_set(InstallSnapshotAck_builder_t *view,
                                                uint64_t value) {
    if (!view || !view->data || view->size < 16 + 8) {
        return false;
    }

    tbe_wire_write_u64(view->data + 16, TurboRaftWire_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint64_t InstallSnapshotAck_term_get(const InstallSnapshotAck_view_t *view) {
    return tbe_wire_read_u64(view->data + 16, TurboRaftWire_WIRE_BIG_ENDIAN);
}




static inline bool InstallSnapshotAck_snapshot_index_set(InstallSnapshotAck_builder_t *view,
                                                uint64_t value) {
    if (!view || !view->data || view->size < 24 + 8) {
        return false;
    }

    tbe_wire_write_u64(view->data + 24, TurboRaftWire_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint64_t InstallSnapshotAck_snapshot_index_get(const InstallSnapshotAck_view_t *view) {
    return tbe_wire_read_u64(view->data + 24, TurboRaftWire_WIRE_BIG_ENDIAN);
}




static inline bool InstallSnapshotAck_snapshot_size_set(InstallSnapshotAck_builder_t *view,
                                                uint64_t value) {
    if (!view || !view->data || view->size < 32 + 8) {
        return false;
    }

    tbe_wire_write_u64(view->data + 32, TurboRaftWire_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint64_t InstallSnapshotAck_snapshot_size_get(const InstallSnapshotAck_view_t *view) {
    return tbe_wire_read_u64(view->data + 32, TurboRaftWire_WIRE_BIG_ENDIAN);
}




static inline bool InstallSnapshotAck_next_offset_set(InstallSnapshotAck_builder_t *view,
                                                uint64_t value) {
    if (!view || !view->data || view->size < 40 + 8) {
        return false;
    }

    tbe_wire_write_u64(view->data + 40, TurboRaftWire_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint64_t InstallSnapshotAck_next_offset_get(const InstallSnapshotAck_view_t *view) {
    return tbe_wire_read_u64(view->data + 40, TurboRaftWire_WIRE_BIG_ENDIAN);
}




static inline bool InstallSnapshotAck_accepted_set(InstallSnapshotAck_builder_t *view,
                                                uint8_t value) {
    if (!view || !view->data || view->size < 48 + 1) {
        return false;
    }

    tbe_wire_write_u8(view->data + 48, TurboRaftWire_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint8_t InstallSnapshotAck_accepted_get(const InstallSnapshotAck_view_t *view) {
    return tbe_wire_read_u8(view->data + 48, TurboRaftWire_WIRE_BIG_ENDIAN);
}










/* ========================================================================= */
/* Union Types                                                               */
/* ========================================================================= */


#ifdef __cplusplus
} /* extern "C" */

namespace TurboRaftWire_typed {
template <typename Record,
          void (*Init)(Record *),
          void (*Clear)(Record *),
          DataBindStatus (*FromBin)(DataBind *, Record *, const void *, size_t, DataBindError *),
          DataBindStatus (*FromJson)(DataBind *, Record *, const char *, size_t, DataBindError *),
          DataBindStatus (*FromYaml)(DataBind *, Record *, const char *, size_t, DataBindError *),
          DataBindStatus (*FromCsv)(DataBind *, Record *, const char *, size_t, size_t,
                                    DataBindError *),
          DataBindStatus (*FromXml)(DataBind *, Record *, const char *, size_t, DataBindError *),
          DataBindStatus (*ToBin)(const Record *, uint8_t **, size_t *, DataBindError *),
          DataBindStatus (*ToJson)(DataBind *, const Record *, char **, size_t *, DataBindError *),
          DataBindStatus (*ToYaml)(DataBind *, const Record *, char **, size_t *, DataBindError *),
          DataBindStatus (*ToCsv)(DataBind *, const Record *, char **, size_t *, DataBindError *),
          DataBindStatus (*ToXml)(DataBind *, const Record *, char **, size_t *, DataBindError *)>
class Owner final {
public:
    Owner() noexcept { Init(&value_); }
    ~Owner() { Clear(&value_); }
    Owner(const Owner &) = delete;
    Owner &operator=(const Owner &) = delete;
    Owner(Owner &&) = delete;
    Owner &operator=(Owner &&) = delete;

    Record *get() noexcept { return &value_; }
    const Record *get() const noexcept { return &value_; }
    Record *operator->() noexcept { return &value_; }
    const Record *operator->() const noexcept { return &value_; }

    DataBindStatus from_bin(DataBind *codec, const void *data, size_t len,
                            DataBindError *error) noexcept {
        return FromBin(codec, &value_, data, len, error);
    }
    DataBindStatus from_json(DataBind *codec, const char *data, size_t len,
                             DataBindError *error) noexcept {
        return FromJson(codec, &value_, data, len, error);
    }
    DataBindStatus from_yaml(DataBind *codec, const char *data, size_t len,
                             DataBindError *error) noexcept {
        return FromYaml(codec, &value_, data, len, error);
    }
    DataBindStatus from_csv(DataBind *codec, const char *data, size_t len, size_t row,
                            DataBindError *error) noexcept {
        return FromCsv(codec, &value_, data, len, row, error);
    }
    DataBindStatus from_xml(DataBind *codec, const char *data, size_t len,
                            DataBindError *error) noexcept {
        return FromXml(codec, &value_, data, len, error);
    }
    DataBindStatus to_bin(uint8_t **out, size_t *out_len, DataBindError *error) const noexcept {
        return ToBin(&value_, out, out_len, error);
    }
    DataBindStatus to_json(DataBind *codec, char **out, size_t *out_len,
                           DataBindError *error) const noexcept {
        return ToJson(codec, &value_, out, out_len, error);
    }
    DataBindStatus to_yaml(DataBind *codec, char **out, size_t *out_len,
                           DataBindError *error) const noexcept {
        return ToYaml(codec, &value_, out, out_len, error);
    }
    DataBindStatus to_csv(DataBind *codec, char **out, size_t *out_len,
                          DataBindError *error) const noexcept {
        return ToCsv(codec, &value_, out, out_len, error);
    }
    DataBindStatus to_xml(DataBind *codec, char **out, size_t *out_len,
                          DataBindError *error) const noexcept {
        return ToXml(codec, &value_, out, out_len, error);
    }

private:
    Record value_{};
};

using RaftWireMessageOwner = Owner<RaftWireMessage_t, RaftWireMessage_init, RaftWireMessage_clear, RaftWireMessage_from_bin,
                          RaftWireMessage_from_json, RaftWireMessage_from_yaml, RaftWireMessage_from_csv,
                          RaftWireMessage_from_xml, RaftWireMessage_to_bin, RaftWireMessage_to_json,
                          RaftWireMessage_to_yaml, RaftWireMessage_to_csv, RaftWireMessage_to_xml>;
using InstallSnapshotChunkOwner = Owner<InstallSnapshotChunk_t, InstallSnapshotChunk_init, InstallSnapshotChunk_clear, InstallSnapshotChunk_from_bin,
                          InstallSnapshotChunk_from_json, InstallSnapshotChunk_from_yaml, InstallSnapshotChunk_from_csv,
                          InstallSnapshotChunk_from_xml, InstallSnapshotChunk_to_bin, InstallSnapshotChunk_to_json,
                          InstallSnapshotChunk_to_yaml, InstallSnapshotChunk_to_csv, InstallSnapshotChunk_to_xml>;
using InstallSnapshotAckOwner = Owner<InstallSnapshotAck_t, InstallSnapshotAck_init, InstallSnapshotAck_clear, InstallSnapshotAck_from_bin,
                          InstallSnapshotAck_from_json, InstallSnapshotAck_from_yaml, InstallSnapshotAck_from_csv,
                          InstallSnapshotAck_from_xml, InstallSnapshotAck_to_bin, InstallSnapshotAck_to_json,
                          InstallSnapshotAck_to_yaml, InstallSnapshotAck_to_csv, InstallSnapshotAck_to_xml>;
} /* namespace TurboRaftWire_typed */
#endif

#undef TBE_GENERATED_MEMCPY
#undef TBE_GENERATED_STRCMP
#undef TBE_GENERATED_STRLEN

#endif /* TurboRaftWire_GENERATED_H */
