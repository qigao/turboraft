#ifndef TurboRaftWireV3_GENERATED_H
#define TurboRaftWireV3_GENERATED_H

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

/* Schema TurboRaftWireV3  */
enum { TurboRaftWireV3_WIRE_BIG_ENDIAN = 0 };
TBE_GENERATED_API DataBindStatus TurboRaftWireV3_codec_create(DataBind **out_codec, DataBindError *error);
TBE_GENERATED_API const char *TurboRaftWireV3_schema_text(void);
TBE_GENERATED_API const tbe_schema_codec_v1_t *TurboRaftWireV3_schema_codec(void);

/* ========================================================================= */
/* Enums                                                                     */
/* ========================================================================= */





/* ========================================================================= */
/* Records                                                                   */
/* ========================================================================= */

/**
 * @brief message RaftWireMessageV3
 * Attributes: 
 */
typedef struct RaftWireMessageV3_s {
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
    uint64_t entry1_index;
    /* size: 8 unsigned */
    uint64_t entry1_term;
    /* size: 8 unsigned */
    uint64_t entry1_command_id;
    /* size: 8 unsigned */
    uint64_t entry2_index;
    /* size: 8 unsigned */
    uint64_t entry2_term;
    /* size: 8 unsigned */
    uint64_t entry2_command_id;
    /* size: 8 unsigned */
    uint64_t entry3_index;
    /* size: 8 unsigned */
    uint64_t entry3_term;
    /* size: 8 unsigned */
    uint64_t entry3_command_id;
    /* size: 8 unsigned */
    uint64_t entry4_index;
    /* size: 8 unsigned */
    uint64_t entry4_term;
    /* size: 8 unsigned */
    uint64_t entry4_command_id;
    /* size: 8 unsigned */
    uint64_t entry5_index;
    /* size: 8 unsigned */
    uint64_t entry5_term;
    /* size: 8 unsigned */
    uint64_t entry5_command_id;
    /* size: 8 unsigned */
    uint64_t entry6_index;
    /* size: 8 unsigned */
    uint64_t entry6_term;
    /* size: 8 unsigned */
    uint64_t entry6_command_id;
    /* size: 8 unsigned */
    uint64_t entry7_index;
    /* size: 8 unsigned */
    uint64_t entry7_term;
    /* size: 8 unsigned */
    uint64_t entry7_command_id;
    /* size: 8 unsigned */
    uint64_t entry8_index;
    /* size: 8 unsigned */
    uint64_t entry8_term;
    /* size: 8 unsigned */
    uint64_t entry8_command_id;
    /* size:   */
    tbe_bytes_t entry1_data;
    /* size:   */
    tbe_bytes_t entry2_data;
    /* size:   */
    tbe_bytes_t entry3_data;
    /* size:   */
    tbe_bytes_t entry4_data;
    /* size:   */
    tbe_bytes_t entry5_data;
    /* size:   */
    tbe_bytes_t entry6_data;
    /* size:   */
    tbe_bytes_t entry7_data;
    /* size:   */
    tbe_bytes_t entry8_data;
} RaftWireMessageV3_t;

/** Owning lifecycle and schema serialization API; serialized buffers use tbe_typed_serialized_free. */
TBE_GENERATED_API void RaftWireMessageV3_init(RaftWireMessageV3_t *object);
TBE_GENERATED_API void RaftWireMessageV3_clear(RaftWireMessageV3_t *object);
TBE_GENERATED_API DataBindStatus RaftWireMessageV3_from_bin(DataBind *codec, RaftWireMessageV3_t *object, const void *data, size_t len, DataBindError *error);
TBE_GENERATED_API DataBindStatus RaftWireMessageV3_from_json(DataBind *codec, RaftWireMessageV3_t *object, const char *data, size_t len, DataBindError *error);
TBE_GENERATED_API DataBindStatus RaftWireMessageV3_from_yaml(DataBind *codec, RaftWireMessageV3_t *object, const char *data, size_t len, DataBindError *error);
TBE_GENERATED_API DataBindStatus RaftWireMessageV3_from_csv(DataBind *codec, RaftWireMessageV3_t *object, const char *data, size_t len, size_t row, DataBindError *error);
TBE_GENERATED_API DataBindStatus RaftWireMessageV3_from_xml(DataBind *codec, RaftWireMessageV3_t *object, const char *data, size_t len, DataBindError *error);
TBE_GENERATED_API DataBindStatus RaftWireMessageV3_to_bin(const RaftWireMessageV3_t *object, uint8_t **out, size_t *out_len, DataBindError *error);
TBE_GENERATED_API DataBindStatus RaftWireMessageV3_to_bin_into(const RaftWireMessageV3_t *object, uint8_t *output, size_t output_capacity, size_t *out_len, DataBindError *error);
TBE_GENERATED_API DataBindStatus RaftWireMessageV3_to_json(DataBind *codec, const RaftWireMessageV3_t *object, char **out, size_t *out_len, DataBindError *error);
TBE_GENERATED_API DataBindStatus RaftWireMessageV3_to_yaml(DataBind *codec, const RaftWireMessageV3_t *object, char **out, size_t *out_len, DataBindError *error);
TBE_GENERATED_API DataBindStatus RaftWireMessageV3_to_csv(DataBind *codec, const RaftWireMessageV3_t *object, char **out, size_t *out_len, DataBindError *error);
TBE_GENERATED_API DataBindStatus RaftWireMessageV3_to_xml(DataBind *codec, const RaftWireMessageV3_t *object, char **out, size_t *out_len, DataBindError *error);

typedef struct RaftWireMessageV3_view_s {
    const uint8_t *data;
    size_t size;
} RaftWireMessageV3_view_t;

typedef struct RaftWireMessageV3_builder_s {
    uint8_t *data;
    size_t size;
} RaftWireMessageV3_builder_t;


enum { RaftWireMessageV3_BLOCK_LENGTH = 288 };

enum { RaftWireMessageV3_message_type_OFFSET = 0 };

enum { RaftWireMessageV3_granted_OFFSET = 1 };

enum { RaftWireMessageV3_reserved_OFFSET = 2 };

enum { RaftWireMessageV3_entry_count_OFFSET = 4 };

enum { RaftWireMessageV3_from_node_OFFSET = 8 };

enum { RaftWireMessageV3_to_node_OFFSET = 16 };

enum { RaftWireMessageV3_term_OFFSET = 24 };

enum { RaftWireMessageV3_campaign_term_OFFSET = 32 };

enum { RaftWireMessageV3_last_log_index_OFFSET = 40 };

enum { RaftWireMessageV3_last_log_term_OFFSET = 48 };

enum { RaftWireMessageV3_leader_commit_OFFSET = 56 };

enum { RaftWireMessageV3_previous_log_index_OFFSET = 64 };

enum { RaftWireMessageV3_previous_log_term_OFFSET = 72 };

enum { RaftWireMessageV3_match_index_OFFSET = 80 };

enum { RaftWireMessageV3_reject_hint_OFFSET = 88 };

enum { RaftWireMessageV3_entry1_index_OFFSET = 96 };

enum { RaftWireMessageV3_entry1_term_OFFSET = 104 };

enum { RaftWireMessageV3_entry1_command_id_OFFSET = 112 };

enum { RaftWireMessageV3_entry2_index_OFFSET = 120 };

enum { RaftWireMessageV3_entry2_term_OFFSET = 128 };

enum { RaftWireMessageV3_entry2_command_id_OFFSET = 136 };

enum { RaftWireMessageV3_entry3_index_OFFSET = 144 };

enum { RaftWireMessageV3_entry3_term_OFFSET = 152 };

enum { RaftWireMessageV3_entry3_command_id_OFFSET = 160 };

enum { RaftWireMessageV3_entry4_index_OFFSET = 168 };

enum { RaftWireMessageV3_entry4_term_OFFSET = 176 };

enum { RaftWireMessageV3_entry4_command_id_OFFSET = 184 };

enum { RaftWireMessageV3_entry5_index_OFFSET = 192 };

enum { RaftWireMessageV3_entry5_term_OFFSET = 200 };

enum { RaftWireMessageV3_entry5_command_id_OFFSET = 208 };

enum { RaftWireMessageV3_entry6_index_OFFSET = 216 };

enum { RaftWireMessageV3_entry6_term_OFFSET = 224 };

enum { RaftWireMessageV3_entry6_command_id_OFFSET = 232 };

enum { RaftWireMessageV3_entry7_index_OFFSET = 240 };

enum { RaftWireMessageV3_entry7_term_OFFSET = 248 };

enum { RaftWireMessageV3_entry7_command_id_OFFSET = 256 };

enum { RaftWireMessageV3_entry8_index_OFFSET = 264 };

enum { RaftWireMessageV3_entry8_term_OFFSET = 272 };

enum { RaftWireMessageV3_entry8_command_id_OFFSET = 280 };


static inline bool RaftWireMessageV3_view_bind(RaftWireMessageV3_view_t *view, const void *data, size_t size) {
    if (!view || !data || size < 288) {
        return false;
    }

    view->data = (const uint8_t *)data;
    view->size = size;
    return true;
}

static inline bool RaftWireMessageV3_builder_bind(RaftWireMessageV3_builder_t *view, void *data, size_t size) {
    if (!view || !data || size < 288) {
        return false;
    }

    view->data = (uint8_t *)data;
    view->size = size;
    return true;
}



static inline bool RaftWireMessageV3_entry1_data_set(RaftWireMessageV3_builder_t *view,
                                               const void *data,
                                               size_t size) {
    size_t payload_offset = RaftWireMessageV3_BLOCK_LENGTH;

    if (!view || !view->data || view->size < payload_offset) {
        return false;
    }

    return tbe_wire_write_var_data(view->data + payload_offset,
                                   view->size - payload_offset,
                                   TurboRaftWireV3_WIRE_BIG_ENDIAN,
                                   data,
                                   size);
}

static inline bool RaftWireMessageV3_entry1_data(const RaftWireMessageV3_view_t *view,
                                           tbe_var_data_t *value) {
    size_t payload_offset = RaftWireMessageV3_BLOCK_LENGTH;

    if (!view || !value || view->size < payload_offset) {
        return false;
    }

    return tbe_wire_read_var_data(view->data + payload_offset,
                                  view->size - payload_offset,
                                  TurboRaftWireV3_WIRE_BIG_ENDIAN,
                                  value);
}


static inline bool RaftWireMessageV3_entry2_data_set(RaftWireMessageV3_builder_t *view,
                                               const void *data,
                                               size_t size) {
    RaftWireMessageV3_view_t read_view;
    tbe_var_data_t previous;
    const uint8_t *payload_data;
    size_t payload_offset;

    if (!view || !view->data) {
        return false;
    }

    read_view.data = view->data;
    read_view.size = view->size;
    if (!RaftWireMessageV3_entry1_data(&read_view, &previous)) {
        return false;
    }

    payload_data = tbe_wire_var_data_end(&previous);
    payload_offset = (size_t)(payload_data - view->data);
    if (payload_offset > view->size) {
        return false;
    }

    return tbe_wire_write_var_data(view->data + payload_offset,
                                   view->size - payload_offset,
                                   TurboRaftWireV3_WIRE_BIG_ENDIAN,
                                   data,
                                   size);
}

static inline bool RaftWireMessageV3_entry2_data(const RaftWireMessageV3_view_t *view,
                                           tbe_var_data_t *value) {
    tbe_var_data_t previous;
    const uint8_t *payload_data;
    size_t payload_offset;

    if (!view || !value) {
        return false;
    }

    if (!RaftWireMessageV3_entry1_data(view, &previous)) {
        return false;
    }

    payload_data = tbe_wire_var_data_end(&previous);
    payload_offset = (size_t)(payload_data - view->data);
    if (payload_offset > view->size) {
        return false;
    }

    return tbe_wire_read_var_data(payload_data,
                                  view->size - payload_offset,
                                  TurboRaftWireV3_WIRE_BIG_ENDIAN,
                                  value);
}


static inline bool RaftWireMessageV3_entry3_data_set(RaftWireMessageV3_builder_t *view,
                                               const void *data,
                                               size_t size) {
    RaftWireMessageV3_view_t read_view;
    tbe_var_data_t previous;
    const uint8_t *payload_data;
    size_t payload_offset;

    if (!view || !view->data) {
        return false;
    }

    read_view.data = view->data;
    read_view.size = view->size;
    if (!RaftWireMessageV3_entry2_data(&read_view, &previous)) {
        return false;
    }

    payload_data = tbe_wire_var_data_end(&previous);
    payload_offset = (size_t)(payload_data - view->data);
    if (payload_offset > view->size) {
        return false;
    }

    return tbe_wire_write_var_data(view->data + payload_offset,
                                   view->size - payload_offset,
                                   TurboRaftWireV3_WIRE_BIG_ENDIAN,
                                   data,
                                   size);
}

static inline bool RaftWireMessageV3_entry3_data(const RaftWireMessageV3_view_t *view,
                                           tbe_var_data_t *value) {
    tbe_var_data_t previous;
    const uint8_t *payload_data;
    size_t payload_offset;

    if (!view || !value) {
        return false;
    }

    if (!RaftWireMessageV3_entry2_data(view, &previous)) {
        return false;
    }

    payload_data = tbe_wire_var_data_end(&previous);
    payload_offset = (size_t)(payload_data - view->data);
    if (payload_offset > view->size) {
        return false;
    }

    return tbe_wire_read_var_data(payload_data,
                                  view->size - payload_offset,
                                  TurboRaftWireV3_WIRE_BIG_ENDIAN,
                                  value);
}


static inline bool RaftWireMessageV3_entry4_data_set(RaftWireMessageV3_builder_t *view,
                                               const void *data,
                                               size_t size) {
    RaftWireMessageV3_view_t read_view;
    tbe_var_data_t previous;
    const uint8_t *payload_data;
    size_t payload_offset;

    if (!view || !view->data) {
        return false;
    }

    read_view.data = view->data;
    read_view.size = view->size;
    if (!RaftWireMessageV3_entry3_data(&read_view, &previous)) {
        return false;
    }

    payload_data = tbe_wire_var_data_end(&previous);
    payload_offset = (size_t)(payload_data - view->data);
    if (payload_offset > view->size) {
        return false;
    }

    return tbe_wire_write_var_data(view->data + payload_offset,
                                   view->size - payload_offset,
                                   TurboRaftWireV3_WIRE_BIG_ENDIAN,
                                   data,
                                   size);
}

static inline bool RaftWireMessageV3_entry4_data(const RaftWireMessageV3_view_t *view,
                                           tbe_var_data_t *value) {
    tbe_var_data_t previous;
    const uint8_t *payload_data;
    size_t payload_offset;

    if (!view || !value) {
        return false;
    }

    if (!RaftWireMessageV3_entry3_data(view, &previous)) {
        return false;
    }

    payload_data = tbe_wire_var_data_end(&previous);
    payload_offset = (size_t)(payload_data - view->data);
    if (payload_offset > view->size) {
        return false;
    }

    return tbe_wire_read_var_data(payload_data,
                                  view->size - payload_offset,
                                  TurboRaftWireV3_WIRE_BIG_ENDIAN,
                                  value);
}


static inline bool RaftWireMessageV3_entry5_data_set(RaftWireMessageV3_builder_t *view,
                                               const void *data,
                                               size_t size) {
    RaftWireMessageV3_view_t read_view;
    tbe_var_data_t previous;
    const uint8_t *payload_data;
    size_t payload_offset;

    if (!view || !view->data) {
        return false;
    }

    read_view.data = view->data;
    read_view.size = view->size;
    if (!RaftWireMessageV3_entry4_data(&read_view, &previous)) {
        return false;
    }

    payload_data = tbe_wire_var_data_end(&previous);
    payload_offset = (size_t)(payload_data - view->data);
    if (payload_offset > view->size) {
        return false;
    }

    return tbe_wire_write_var_data(view->data + payload_offset,
                                   view->size - payload_offset,
                                   TurboRaftWireV3_WIRE_BIG_ENDIAN,
                                   data,
                                   size);
}

static inline bool RaftWireMessageV3_entry5_data(const RaftWireMessageV3_view_t *view,
                                           tbe_var_data_t *value) {
    tbe_var_data_t previous;
    const uint8_t *payload_data;
    size_t payload_offset;

    if (!view || !value) {
        return false;
    }

    if (!RaftWireMessageV3_entry4_data(view, &previous)) {
        return false;
    }

    payload_data = tbe_wire_var_data_end(&previous);
    payload_offset = (size_t)(payload_data - view->data);
    if (payload_offset > view->size) {
        return false;
    }

    return tbe_wire_read_var_data(payload_data,
                                  view->size - payload_offset,
                                  TurboRaftWireV3_WIRE_BIG_ENDIAN,
                                  value);
}


static inline bool RaftWireMessageV3_entry6_data_set(RaftWireMessageV3_builder_t *view,
                                               const void *data,
                                               size_t size) {
    RaftWireMessageV3_view_t read_view;
    tbe_var_data_t previous;
    const uint8_t *payload_data;
    size_t payload_offset;

    if (!view || !view->data) {
        return false;
    }

    read_view.data = view->data;
    read_view.size = view->size;
    if (!RaftWireMessageV3_entry5_data(&read_view, &previous)) {
        return false;
    }

    payload_data = tbe_wire_var_data_end(&previous);
    payload_offset = (size_t)(payload_data - view->data);
    if (payload_offset > view->size) {
        return false;
    }

    return tbe_wire_write_var_data(view->data + payload_offset,
                                   view->size - payload_offset,
                                   TurboRaftWireV3_WIRE_BIG_ENDIAN,
                                   data,
                                   size);
}

static inline bool RaftWireMessageV3_entry6_data(const RaftWireMessageV3_view_t *view,
                                           tbe_var_data_t *value) {
    tbe_var_data_t previous;
    const uint8_t *payload_data;
    size_t payload_offset;

    if (!view || !value) {
        return false;
    }

    if (!RaftWireMessageV3_entry5_data(view, &previous)) {
        return false;
    }

    payload_data = tbe_wire_var_data_end(&previous);
    payload_offset = (size_t)(payload_data - view->data);
    if (payload_offset > view->size) {
        return false;
    }

    return tbe_wire_read_var_data(payload_data,
                                  view->size - payload_offset,
                                  TurboRaftWireV3_WIRE_BIG_ENDIAN,
                                  value);
}


static inline bool RaftWireMessageV3_entry7_data_set(RaftWireMessageV3_builder_t *view,
                                               const void *data,
                                               size_t size) {
    RaftWireMessageV3_view_t read_view;
    tbe_var_data_t previous;
    const uint8_t *payload_data;
    size_t payload_offset;

    if (!view || !view->data) {
        return false;
    }

    read_view.data = view->data;
    read_view.size = view->size;
    if (!RaftWireMessageV3_entry6_data(&read_view, &previous)) {
        return false;
    }

    payload_data = tbe_wire_var_data_end(&previous);
    payload_offset = (size_t)(payload_data - view->data);
    if (payload_offset > view->size) {
        return false;
    }

    return tbe_wire_write_var_data(view->data + payload_offset,
                                   view->size - payload_offset,
                                   TurboRaftWireV3_WIRE_BIG_ENDIAN,
                                   data,
                                   size);
}

static inline bool RaftWireMessageV3_entry7_data(const RaftWireMessageV3_view_t *view,
                                           tbe_var_data_t *value) {
    tbe_var_data_t previous;
    const uint8_t *payload_data;
    size_t payload_offset;

    if (!view || !value) {
        return false;
    }

    if (!RaftWireMessageV3_entry6_data(view, &previous)) {
        return false;
    }

    payload_data = tbe_wire_var_data_end(&previous);
    payload_offset = (size_t)(payload_data - view->data);
    if (payload_offset > view->size) {
        return false;
    }

    return tbe_wire_read_var_data(payload_data,
                                  view->size - payload_offset,
                                  TurboRaftWireV3_WIRE_BIG_ENDIAN,
                                  value);
}


static inline bool RaftWireMessageV3_entry8_data_set(RaftWireMessageV3_builder_t *view,
                                               const void *data,
                                               size_t size) {
    RaftWireMessageV3_view_t read_view;
    tbe_var_data_t previous;
    const uint8_t *payload_data;
    size_t payload_offset;

    if (!view || !view->data) {
        return false;
    }

    read_view.data = view->data;
    read_view.size = view->size;
    if (!RaftWireMessageV3_entry7_data(&read_view, &previous)) {
        return false;
    }

    payload_data = tbe_wire_var_data_end(&previous);
    payload_offset = (size_t)(payload_data - view->data);
    if (payload_offset > view->size) {
        return false;
    }

    return tbe_wire_write_var_data(view->data + payload_offset,
                                   view->size - payload_offset,
                                   TurboRaftWireV3_WIRE_BIG_ENDIAN,
                                   data,
                                   size);
}

static inline bool RaftWireMessageV3_entry8_data(const RaftWireMessageV3_view_t *view,
                                           tbe_var_data_t *value) {
    tbe_var_data_t previous;
    const uint8_t *payload_data;
    size_t payload_offset;

    if (!view || !value) {
        return false;
    }

    if (!RaftWireMessageV3_entry7_data(view, &previous)) {
        return false;
    }

    payload_data = tbe_wire_var_data_end(&previous);
    payload_offset = (size_t)(payload_data - view->data);
    if (payload_offset > view->size) {
        return false;
    }

    return tbe_wire_read_var_data(payload_data,
                                  view->size - payload_offset,
                                  TurboRaftWireV3_WIRE_BIG_ENDIAN,
                                  value);
}



static inline bool RaftWireMessageV3_message_type_set(RaftWireMessageV3_builder_t *view,
                                                uint8_t value) {
    if (!view || !view->data || view->size < 0 + 1) {
        return false;
    }

    tbe_wire_write_u8(view->data + 0, TurboRaftWireV3_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint8_t RaftWireMessageV3_message_type_get(const RaftWireMessageV3_view_t *view) {
    return tbe_wire_read_u8(view->data + 0, TurboRaftWireV3_WIRE_BIG_ENDIAN);
}




static inline bool RaftWireMessageV3_granted_set(RaftWireMessageV3_builder_t *view,
                                                uint8_t value) {
    if (!view || !view->data || view->size < 1 + 1) {
        return false;
    }

    tbe_wire_write_u8(view->data + 1, TurboRaftWireV3_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint8_t RaftWireMessageV3_granted_get(const RaftWireMessageV3_view_t *view) {
    return tbe_wire_read_u8(view->data + 1, TurboRaftWireV3_WIRE_BIG_ENDIAN);
}




static inline bool RaftWireMessageV3_reserved_set(RaftWireMessageV3_builder_t *view,
                                                uint16_t value) {
    if (!view || !view->data || view->size < 2 + 2) {
        return false;
    }

    tbe_wire_write_u16(view->data + 2, TurboRaftWireV3_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint16_t RaftWireMessageV3_reserved_get(const RaftWireMessageV3_view_t *view) {
    return tbe_wire_read_u16(view->data + 2, TurboRaftWireV3_WIRE_BIG_ENDIAN);
}




static inline bool RaftWireMessageV3_entry_count_set(RaftWireMessageV3_builder_t *view,
                                                uint32_t value) {
    if (!view || !view->data || view->size < 4 + 4) {
        return false;
    }

    tbe_wire_write_u32(view->data + 4, TurboRaftWireV3_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint32_t RaftWireMessageV3_entry_count_get(const RaftWireMessageV3_view_t *view) {
    return tbe_wire_read_u32(view->data + 4, TurboRaftWireV3_WIRE_BIG_ENDIAN);
}




static inline bool RaftWireMessageV3_from_node_set(RaftWireMessageV3_builder_t *view,
                                                uint64_t value) {
    if (!view || !view->data || view->size < 8 + 8) {
        return false;
    }

    tbe_wire_write_u64(view->data + 8, TurboRaftWireV3_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint64_t RaftWireMessageV3_from_node_get(const RaftWireMessageV3_view_t *view) {
    return tbe_wire_read_u64(view->data + 8, TurboRaftWireV3_WIRE_BIG_ENDIAN);
}




static inline bool RaftWireMessageV3_to_node_set(RaftWireMessageV3_builder_t *view,
                                                uint64_t value) {
    if (!view || !view->data || view->size < 16 + 8) {
        return false;
    }

    tbe_wire_write_u64(view->data + 16, TurboRaftWireV3_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint64_t RaftWireMessageV3_to_node_get(const RaftWireMessageV3_view_t *view) {
    return tbe_wire_read_u64(view->data + 16, TurboRaftWireV3_WIRE_BIG_ENDIAN);
}




static inline bool RaftWireMessageV3_term_set(RaftWireMessageV3_builder_t *view,
                                                uint64_t value) {
    if (!view || !view->data || view->size < 24 + 8) {
        return false;
    }

    tbe_wire_write_u64(view->data + 24, TurboRaftWireV3_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint64_t RaftWireMessageV3_term_get(const RaftWireMessageV3_view_t *view) {
    return tbe_wire_read_u64(view->data + 24, TurboRaftWireV3_WIRE_BIG_ENDIAN);
}




static inline bool RaftWireMessageV3_campaign_term_set(RaftWireMessageV3_builder_t *view,
                                                uint64_t value) {
    if (!view || !view->data || view->size < 32 + 8) {
        return false;
    }

    tbe_wire_write_u64(view->data + 32, TurboRaftWireV3_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint64_t RaftWireMessageV3_campaign_term_get(const RaftWireMessageV3_view_t *view) {
    return tbe_wire_read_u64(view->data + 32, TurboRaftWireV3_WIRE_BIG_ENDIAN);
}




static inline bool RaftWireMessageV3_last_log_index_set(RaftWireMessageV3_builder_t *view,
                                                uint64_t value) {
    if (!view || !view->data || view->size < 40 + 8) {
        return false;
    }

    tbe_wire_write_u64(view->data + 40, TurboRaftWireV3_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint64_t RaftWireMessageV3_last_log_index_get(const RaftWireMessageV3_view_t *view) {
    return tbe_wire_read_u64(view->data + 40, TurboRaftWireV3_WIRE_BIG_ENDIAN);
}




static inline bool RaftWireMessageV3_last_log_term_set(RaftWireMessageV3_builder_t *view,
                                                uint64_t value) {
    if (!view || !view->data || view->size < 48 + 8) {
        return false;
    }

    tbe_wire_write_u64(view->data + 48, TurboRaftWireV3_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint64_t RaftWireMessageV3_last_log_term_get(const RaftWireMessageV3_view_t *view) {
    return tbe_wire_read_u64(view->data + 48, TurboRaftWireV3_WIRE_BIG_ENDIAN);
}




static inline bool RaftWireMessageV3_leader_commit_set(RaftWireMessageV3_builder_t *view,
                                                uint64_t value) {
    if (!view || !view->data || view->size < 56 + 8) {
        return false;
    }

    tbe_wire_write_u64(view->data + 56, TurboRaftWireV3_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint64_t RaftWireMessageV3_leader_commit_get(const RaftWireMessageV3_view_t *view) {
    return tbe_wire_read_u64(view->data + 56, TurboRaftWireV3_WIRE_BIG_ENDIAN);
}




static inline bool RaftWireMessageV3_previous_log_index_set(RaftWireMessageV3_builder_t *view,
                                                uint64_t value) {
    if (!view || !view->data || view->size < 64 + 8) {
        return false;
    }

    tbe_wire_write_u64(view->data + 64, TurboRaftWireV3_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint64_t RaftWireMessageV3_previous_log_index_get(const RaftWireMessageV3_view_t *view) {
    return tbe_wire_read_u64(view->data + 64, TurboRaftWireV3_WIRE_BIG_ENDIAN);
}




static inline bool RaftWireMessageV3_previous_log_term_set(RaftWireMessageV3_builder_t *view,
                                                uint64_t value) {
    if (!view || !view->data || view->size < 72 + 8) {
        return false;
    }

    tbe_wire_write_u64(view->data + 72, TurboRaftWireV3_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint64_t RaftWireMessageV3_previous_log_term_get(const RaftWireMessageV3_view_t *view) {
    return tbe_wire_read_u64(view->data + 72, TurboRaftWireV3_WIRE_BIG_ENDIAN);
}




static inline bool RaftWireMessageV3_match_index_set(RaftWireMessageV3_builder_t *view,
                                                uint64_t value) {
    if (!view || !view->data || view->size < 80 + 8) {
        return false;
    }

    tbe_wire_write_u64(view->data + 80, TurboRaftWireV3_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint64_t RaftWireMessageV3_match_index_get(const RaftWireMessageV3_view_t *view) {
    return tbe_wire_read_u64(view->data + 80, TurboRaftWireV3_WIRE_BIG_ENDIAN);
}




static inline bool RaftWireMessageV3_reject_hint_set(RaftWireMessageV3_builder_t *view,
                                                uint64_t value) {
    if (!view || !view->data || view->size < 88 + 8) {
        return false;
    }

    tbe_wire_write_u64(view->data + 88, TurboRaftWireV3_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint64_t RaftWireMessageV3_reject_hint_get(const RaftWireMessageV3_view_t *view) {
    return tbe_wire_read_u64(view->data + 88, TurboRaftWireV3_WIRE_BIG_ENDIAN);
}




static inline bool RaftWireMessageV3_entry1_index_set(RaftWireMessageV3_builder_t *view,
                                                uint64_t value) {
    if (!view || !view->data || view->size < 96 + 8) {
        return false;
    }

    tbe_wire_write_u64(view->data + 96, TurboRaftWireV3_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint64_t RaftWireMessageV3_entry1_index_get(const RaftWireMessageV3_view_t *view) {
    return tbe_wire_read_u64(view->data + 96, TurboRaftWireV3_WIRE_BIG_ENDIAN);
}




static inline bool RaftWireMessageV3_entry1_term_set(RaftWireMessageV3_builder_t *view,
                                                uint64_t value) {
    if (!view || !view->data || view->size < 104 + 8) {
        return false;
    }

    tbe_wire_write_u64(view->data + 104, TurboRaftWireV3_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint64_t RaftWireMessageV3_entry1_term_get(const RaftWireMessageV3_view_t *view) {
    return tbe_wire_read_u64(view->data + 104, TurboRaftWireV3_WIRE_BIG_ENDIAN);
}




static inline bool RaftWireMessageV3_entry1_command_id_set(RaftWireMessageV3_builder_t *view,
                                                uint64_t value) {
    if (!view || !view->data || view->size < 112 + 8) {
        return false;
    }

    tbe_wire_write_u64(view->data + 112, TurboRaftWireV3_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint64_t RaftWireMessageV3_entry1_command_id_get(const RaftWireMessageV3_view_t *view) {
    return tbe_wire_read_u64(view->data + 112, TurboRaftWireV3_WIRE_BIG_ENDIAN);
}




static inline bool RaftWireMessageV3_entry2_index_set(RaftWireMessageV3_builder_t *view,
                                                uint64_t value) {
    if (!view || !view->data || view->size < 120 + 8) {
        return false;
    }

    tbe_wire_write_u64(view->data + 120, TurboRaftWireV3_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint64_t RaftWireMessageV3_entry2_index_get(const RaftWireMessageV3_view_t *view) {
    return tbe_wire_read_u64(view->data + 120, TurboRaftWireV3_WIRE_BIG_ENDIAN);
}




static inline bool RaftWireMessageV3_entry2_term_set(RaftWireMessageV3_builder_t *view,
                                                uint64_t value) {
    if (!view || !view->data || view->size < 128 + 8) {
        return false;
    }

    tbe_wire_write_u64(view->data + 128, TurboRaftWireV3_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint64_t RaftWireMessageV3_entry2_term_get(const RaftWireMessageV3_view_t *view) {
    return tbe_wire_read_u64(view->data + 128, TurboRaftWireV3_WIRE_BIG_ENDIAN);
}




static inline bool RaftWireMessageV3_entry2_command_id_set(RaftWireMessageV3_builder_t *view,
                                                uint64_t value) {
    if (!view || !view->data || view->size < 136 + 8) {
        return false;
    }

    tbe_wire_write_u64(view->data + 136, TurboRaftWireV3_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint64_t RaftWireMessageV3_entry2_command_id_get(const RaftWireMessageV3_view_t *view) {
    return tbe_wire_read_u64(view->data + 136, TurboRaftWireV3_WIRE_BIG_ENDIAN);
}




static inline bool RaftWireMessageV3_entry3_index_set(RaftWireMessageV3_builder_t *view,
                                                uint64_t value) {
    if (!view || !view->data || view->size < 144 + 8) {
        return false;
    }

    tbe_wire_write_u64(view->data + 144, TurboRaftWireV3_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint64_t RaftWireMessageV3_entry3_index_get(const RaftWireMessageV3_view_t *view) {
    return tbe_wire_read_u64(view->data + 144, TurboRaftWireV3_WIRE_BIG_ENDIAN);
}




static inline bool RaftWireMessageV3_entry3_term_set(RaftWireMessageV3_builder_t *view,
                                                uint64_t value) {
    if (!view || !view->data || view->size < 152 + 8) {
        return false;
    }

    tbe_wire_write_u64(view->data + 152, TurboRaftWireV3_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint64_t RaftWireMessageV3_entry3_term_get(const RaftWireMessageV3_view_t *view) {
    return tbe_wire_read_u64(view->data + 152, TurboRaftWireV3_WIRE_BIG_ENDIAN);
}




static inline bool RaftWireMessageV3_entry3_command_id_set(RaftWireMessageV3_builder_t *view,
                                                uint64_t value) {
    if (!view || !view->data || view->size < 160 + 8) {
        return false;
    }

    tbe_wire_write_u64(view->data + 160, TurboRaftWireV3_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint64_t RaftWireMessageV3_entry3_command_id_get(const RaftWireMessageV3_view_t *view) {
    return tbe_wire_read_u64(view->data + 160, TurboRaftWireV3_WIRE_BIG_ENDIAN);
}




static inline bool RaftWireMessageV3_entry4_index_set(RaftWireMessageV3_builder_t *view,
                                                uint64_t value) {
    if (!view || !view->data || view->size < 168 + 8) {
        return false;
    }

    tbe_wire_write_u64(view->data + 168, TurboRaftWireV3_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint64_t RaftWireMessageV3_entry4_index_get(const RaftWireMessageV3_view_t *view) {
    return tbe_wire_read_u64(view->data + 168, TurboRaftWireV3_WIRE_BIG_ENDIAN);
}




static inline bool RaftWireMessageV3_entry4_term_set(RaftWireMessageV3_builder_t *view,
                                                uint64_t value) {
    if (!view || !view->data || view->size < 176 + 8) {
        return false;
    }

    tbe_wire_write_u64(view->data + 176, TurboRaftWireV3_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint64_t RaftWireMessageV3_entry4_term_get(const RaftWireMessageV3_view_t *view) {
    return tbe_wire_read_u64(view->data + 176, TurboRaftWireV3_WIRE_BIG_ENDIAN);
}




static inline bool RaftWireMessageV3_entry4_command_id_set(RaftWireMessageV3_builder_t *view,
                                                uint64_t value) {
    if (!view || !view->data || view->size < 184 + 8) {
        return false;
    }

    tbe_wire_write_u64(view->data + 184, TurboRaftWireV3_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint64_t RaftWireMessageV3_entry4_command_id_get(const RaftWireMessageV3_view_t *view) {
    return tbe_wire_read_u64(view->data + 184, TurboRaftWireV3_WIRE_BIG_ENDIAN);
}




static inline bool RaftWireMessageV3_entry5_index_set(RaftWireMessageV3_builder_t *view,
                                                uint64_t value) {
    if (!view || !view->data || view->size < 192 + 8) {
        return false;
    }

    tbe_wire_write_u64(view->data + 192, TurboRaftWireV3_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint64_t RaftWireMessageV3_entry5_index_get(const RaftWireMessageV3_view_t *view) {
    return tbe_wire_read_u64(view->data + 192, TurboRaftWireV3_WIRE_BIG_ENDIAN);
}




static inline bool RaftWireMessageV3_entry5_term_set(RaftWireMessageV3_builder_t *view,
                                                uint64_t value) {
    if (!view || !view->data || view->size < 200 + 8) {
        return false;
    }

    tbe_wire_write_u64(view->data + 200, TurboRaftWireV3_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint64_t RaftWireMessageV3_entry5_term_get(const RaftWireMessageV3_view_t *view) {
    return tbe_wire_read_u64(view->data + 200, TurboRaftWireV3_WIRE_BIG_ENDIAN);
}




static inline bool RaftWireMessageV3_entry5_command_id_set(RaftWireMessageV3_builder_t *view,
                                                uint64_t value) {
    if (!view || !view->data || view->size < 208 + 8) {
        return false;
    }

    tbe_wire_write_u64(view->data + 208, TurboRaftWireV3_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint64_t RaftWireMessageV3_entry5_command_id_get(const RaftWireMessageV3_view_t *view) {
    return tbe_wire_read_u64(view->data + 208, TurboRaftWireV3_WIRE_BIG_ENDIAN);
}




static inline bool RaftWireMessageV3_entry6_index_set(RaftWireMessageV3_builder_t *view,
                                                uint64_t value) {
    if (!view || !view->data || view->size < 216 + 8) {
        return false;
    }

    tbe_wire_write_u64(view->data + 216, TurboRaftWireV3_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint64_t RaftWireMessageV3_entry6_index_get(const RaftWireMessageV3_view_t *view) {
    return tbe_wire_read_u64(view->data + 216, TurboRaftWireV3_WIRE_BIG_ENDIAN);
}




static inline bool RaftWireMessageV3_entry6_term_set(RaftWireMessageV3_builder_t *view,
                                                uint64_t value) {
    if (!view || !view->data || view->size < 224 + 8) {
        return false;
    }

    tbe_wire_write_u64(view->data + 224, TurboRaftWireV3_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint64_t RaftWireMessageV3_entry6_term_get(const RaftWireMessageV3_view_t *view) {
    return tbe_wire_read_u64(view->data + 224, TurboRaftWireV3_WIRE_BIG_ENDIAN);
}




static inline bool RaftWireMessageV3_entry6_command_id_set(RaftWireMessageV3_builder_t *view,
                                                uint64_t value) {
    if (!view || !view->data || view->size < 232 + 8) {
        return false;
    }

    tbe_wire_write_u64(view->data + 232, TurboRaftWireV3_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint64_t RaftWireMessageV3_entry6_command_id_get(const RaftWireMessageV3_view_t *view) {
    return tbe_wire_read_u64(view->data + 232, TurboRaftWireV3_WIRE_BIG_ENDIAN);
}




static inline bool RaftWireMessageV3_entry7_index_set(RaftWireMessageV3_builder_t *view,
                                                uint64_t value) {
    if (!view || !view->data || view->size < 240 + 8) {
        return false;
    }

    tbe_wire_write_u64(view->data + 240, TurboRaftWireV3_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint64_t RaftWireMessageV3_entry7_index_get(const RaftWireMessageV3_view_t *view) {
    return tbe_wire_read_u64(view->data + 240, TurboRaftWireV3_WIRE_BIG_ENDIAN);
}




static inline bool RaftWireMessageV3_entry7_term_set(RaftWireMessageV3_builder_t *view,
                                                uint64_t value) {
    if (!view || !view->data || view->size < 248 + 8) {
        return false;
    }

    tbe_wire_write_u64(view->data + 248, TurboRaftWireV3_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint64_t RaftWireMessageV3_entry7_term_get(const RaftWireMessageV3_view_t *view) {
    return tbe_wire_read_u64(view->data + 248, TurboRaftWireV3_WIRE_BIG_ENDIAN);
}




static inline bool RaftWireMessageV3_entry7_command_id_set(RaftWireMessageV3_builder_t *view,
                                                uint64_t value) {
    if (!view || !view->data || view->size < 256 + 8) {
        return false;
    }

    tbe_wire_write_u64(view->data + 256, TurboRaftWireV3_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint64_t RaftWireMessageV3_entry7_command_id_get(const RaftWireMessageV3_view_t *view) {
    return tbe_wire_read_u64(view->data + 256, TurboRaftWireV3_WIRE_BIG_ENDIAN);
}




static inline bool RaftWireMessageV3_entry8_index_set(RaftWireMessageV3_builder_t *view,
                                                uint64_t value) {
    if (!view || !view->data || view->size < 264 + 8) {
        return false;
    }

    tbe_wire_write_u64(view->data + 264, TurboRaftWireV3_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint64_t RaftWireMessageV3_entry8_index_get(const RaftWireMessageV3_view_t *view) {
    return tbe_wire_read_u64(view->data + 264, TurboRaftWireV3_WIRE_BIG_ENDIAN);
}




static inline bool RaftWireMessageV3_entry8_term_set(RaftWireMessageV3_builder_t *view,
                                                uint64_t value) {
    if (!view || !view->data || view->size < 272 + 8) {
        return false;
    }

    tbe_wire_write_u64(view->data + 272, TurboRaftWireV3_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint64_t RaftWireMessageV3_entry8_term_get(const RaftWireMessageV3_view_t *view) {
    return tbe_wire_read_u64(view->data + 272, TurboRaftWireV3_WIRE_BIG_ENDIAN);
}




static inline bool RaftWireMessageV3_entry8_command_id_set(RaftWireMessageV3_builder_t *view,
                                                uint64_t value) {
    if (!view || !view->data || view->size < 280 + 8) {
        return false;
    }

    tbe_wire_write_u64(view->data + 280, TurboRaftWireV3_WIRE_BIG_ENDIAN, value);
    return true;
}

static inline uint64_t RaftWireMessageV3_entry8_command_id_get(const RaftWireMessageV3_view_t *view) {
    return tbe_wire_read_u64(view->data + 280, TurboRaftWireV3_WIRE_BIG_ENDIAN);
}







/* ========================================================================= */
/* Union Types                                                               */
/* ========================================================================= */


#ifdef __cplusplus
} /* extern "C" */

namespace TurboRaftWireV3_typed {
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

using RaftWireMessageV3Owner = Owner<RaftWireMessageV3_t, RaftWireMessageV3_init, RaftWireMessageV3_clear, RaftWireMessageV3_from_bin,
                          RaftWireMessageV3_from_json, RaftWireMessageV3_from_yaml, RaftWireMessageV3_from_csv,
                          RaftWireMessageV3_from_xml, RaftWireMessageV3_to_bin, RaftWireMessageV3_to_json,
                          RaftWireMessageV3_to_yaml, RaftWireMessageV3_to_csv, RaftWireMessageV3_to_xml>;
} /* namespace TurboRaftWireV3_typed */
#endif

#undef TBE_GENERATED_MEMCPY
#undef TBE_GENERATED_STRCMP
#undef TBE_GENERATED_STRLEN

#endif /* TurboRaftWireV3_GENERATED_H */
