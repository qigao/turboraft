#include "turboraft_wire_v3_tbe.h"

#include <stddef.h>
#include <stdio.h>
#include <cmeta_thread.h>
#include <stdlib.h>
#include <string.h>
#include <cmeta/cleanup.h>
#include <cmeta/fixed_array.h>
#include <cmeta/pp.h>

#include "data_bind_format_provider.h"
#include "data_bind_message_plan.h"
#include "data_bind_native.h"
#include "data_bind_xml_writer.h"
#include "data_bind_projection_plan.h"


static inline const DataBindFormatProvider *RaftWireMessageV3_binary_RaftWireMessageV3_databind_binary_provider(void);


static const char TurboRaftWireV3_SCHEMA_TEXT[] = "schema TurboRaftWireV3 [id(8102), version(3), byte_order(little)];\n\nmessage RaftWireMessageV3 {\n    uint8 message_type;\n    uint8 granted;\n    uint16 reserved;\n    uint32 entry_count;\n    uint64 from_node;\n    uint64 to_node;\n    uint64 term;\n    uint64 campaign_term;\n    uint64 last_log_index;\n    uint64 last_log_term;\n    uint64 leader_commit;\n    uint64 previous_log_index;\n    uint64 previous_log_term;\n    uint64 match_index;\n    uint64 reject_hint;\n    uint64 entry1_index;\n    uint64 entry1_term;\n    uint64 entry1_command_id;\n    uint64 entry2_index;\n    uint64 entry2_term;\n    uint64 entry2_command_id;\n    uint64 entry3_index;\n    uint64 entry3_term;\n    uint64 entry3_command_id;\n    uint64 entry4_index;\n    uint64 entry4_term;\n    uint64 entry4_command_id;\n    uint64 entry5_index;\n    uint64 entry5_term;\n    uint64 entry5_command_id;\n    uint64 entry6_index;\n    uint64 entry6_term;\n    uint64 entry6_command_id;\n    uint64 entry7_index;\n    uint64 entry7_term;\n    uint64 entry7_command_id;\n    uint64 entry8_index;\n    uint64 entry8_term;\n    uint64 entry8_command_id;\n    bytes entry1_data;\n    bytes entry2_data;\n    bytes entry3_data;\n    bytes entry4_data;\n    bytes entry5_data;\n    bytes entry6_data;\n    bytes entry7_data;\n    bytes entry8_data;\n}\n";
static void TurboRaftWireV3_CMETA_INIT_ALL(void);

const char *TurboRaftWireV3_schema_text(void) {
    return TurboRaftWireV3_SCHEMA_TEXT;
}

DataBindStatus TurboRaftWireV3_codec_create(DataBind **out_codec, DataBindError *error) {
    return data_bind_create_from_text(TurboRaftWireV3_SCHEMA_TEXT,
                                      sizeof(TurboRaftWireV3_SCHEMA_TEXT) - 1,
                                      out_codec, error);
}


static DataBindStatus RaftWireMessageV3__databind_message_native_binding(
    DataBindNativeTypeBinding *out, DataBindError *error) {
    const cmeta_data_desc *data = NULL;
    DataBindStatus status;
    DataBindNativeTypeBinding value;
    if (out == NULL) return DATA_BIND_ERR_INVALID_ARG;
    status = RaftWireMessageV3_cmeta_data(&data, error);
    if (status != DATA_BIND_OK) return status;
    value = (DataBindNativeTypeBinding)
        DATA_BIND_NATIVE_TYPE_BINDING_INIT("RaftWireMessageV3", data);
    *out = value;
    return DATA_BIND_OK;
}

static const DataBindMessageNativeArtifact RaftWireMessageV3_NATIVE_ARTIFACT = {
    sizeof(DataBindMessageNativeArtifact),
    DATA_BIND_MESSAGE_NATIVE_ARTIFACT_ABI_VERSION,
    "RaftWireMessageV3",
    RaftWireMessageV3__databind_message_native_binding
};

const DataBindMessageNativeArtifact *RaftWireMessageV3_native_artifact(void) {
    return &RaftWireMessageV3_NATIVE_ARTIFACT;
}

/* Native graph metadata is immutable and borrowed. Schema semantics and
 * wire layout remain separate from native structure and ownership. */
#include <cmeta_cmeta_fixed_width.h>
#include <cmeta_cmeta_data.h>
#define TBE_GENERATED_CMETA_GRAPH 1

static DataBindStatus TurboRaftWireV3_schema_codec_error(
    DataBindError *error, DataBindStatus status, const char *path,
    const char *message);



extern const cmeta_type_desc RaftWireMessageV3_CMETA_TYPE;
extern const cmeta_data_desc RaftWireMessageV3_CMETA_DATA;





/* Inline storage keeps the existing C array ABI; canonical CMeta owns every
 * live slot and all copy, move, collect and release operations. */
#define DATABIND_DEFINE_FIXED_ARRAY(Name, Storage, Element, Count, Data, Id, Display) \
    typedef Element Storage[Count]; \
    CMETA_DEFINE_FIXED_ARRAY(Name, Storage, Element, Count, Data, Id, Display)



#undef DATABIND_DEFINE_FIXED_ARRAY

/* Generated container semantics are provided directly by canonical Salts
 * typed CSTL descriptors emitted in the public generated header. */


/* One field schema drives layout, semantic metadata and exact field count.
 * Imported provider addresses are bound once at runtime on every platform. */
#define DATABIND_RECORD_COUNT(...) + 1u
#define DATABIND_RECORD_LAYOUT(index_, owner_, member_, semantic_, native_, align_, type_, data_, declared_) \
    owner_##_CMETA_LAYOUT_FIELDS[index_] = (cmeta_field_desc){ \
        semantic_, native_, offsetof(owner_##_t, member_), \
        sizeof(((owner_##_t *)0)->member_), align_, type_, declared_ };
#define DATABIND_RECORD_DATA(index_, owner_, member_, semantic_, native_, align_, type_, data_, declared_) \
    owner_##_CMETA_FIELDS[index_] = (cmeta_data_field_desc){ \
        "tbe.native.TurboRaftWireV3." #owner_ "_t." semantic_, \
        semantic_, offsetof(owner_##_t, member_), data_ };

CMETA_DEFINE_DATA_TRAITS(RaftWireMessageV3, &RaftWireMessageV3_CMETA_DATA);
static const cmeta_type_identity RaftWireMessageV3_CMETA_ID =
    CMETA_TYPE_ID_ATOM_INIT("tbe.native.TurboRaftWireV3.RaftWireMessageV3_t");
TBE_GENERATED_API const cmeta_type_desc RaftWireMessageV3_CMETA_TYPE = {
    "RaftWireMessageV3_t", sizeof(RaftWireMessageV3_t), _Alignof(RaftWireMessageV3_t),
    CMETA_T_OBJECT, NULL,
    &cmeta_traits_RaftWireMessageV3,
    &RaftWireMessageV3_CMETA_ID
};
#define RaftWireMessageV3_CMETA_FIELD_SCHEMA(M) \
    Schema(M, \
        (0, RaftWireMessageV3, message_type, "message_type", "uint8_t", _Alignof(uint8_t), &cmeta_type_uint8, &cmeta_data_uint8, NULL), \
        (1, RaftWireMessageV3, granted, "granted", "uint8_t", _Alignof(uint8_t), &cmeta_type_uint8, &cmeta_data_uint8, NULL), \
        (2, RaftWireMessageV3, reserved, "reserved", "uint16_t", _Alignof(uint16_t), &cmeta_type_uint16, &cmeta_data_uint16, NULL), \
        (3, RaftWireMessageV3, entry_count, "entry_count", "uint32_t", _Alignof(uint32_t), &cmeta_type_uint32, &cmeta_data_uint32, NULL), \
        (4, RaftWireMessageV3, from_node, "from_node", "uint64_t", _Alignof(uint64_t), &cmeta_type_uint64, &cmeta_data_uint64, NULL), \
        (5, RaftWireMessageV3, to_node, "to_node", "uint64_t", _Alignof(uint64_t), &cmeta_type_uint64, &cmeta_data_uint64, NULL), \
        (6, RaftWireMessageV3, term, "term", "uint64_t", _Alignof(uint64_t), &cmeta_type_uint64, &cmeta_data_uint64, NULL), \
        (7, RaftWireMessageV3, campaign_term, "campaign_term", "uint64_t", _Alignof(uint64_t), &cmeta_type_uint64, &cmeta_data_uint64, NULL), \
        (8, RaftWireMessageV3, last_log_index, "last_log_index", "uint64_t", _Alignof(uint64_t), &cmeta_type_uint64, &cmeta_data_uint64, NULL), \
        (9, RaftWireMessageV3, last_log_term, "last_log_term", "uint64_t", _Alignof(uint64_t), &cmeta_type_uint64, &cmeta_data_uint64, NULL), \
        (10, RaftWireMessageV3, leader_commit, "leader_commit", "uint64_t", _Alignof(uint64_t), &cmeta_type_uint64, &cmeta_data_uint64, NULL), \
        (11, RaftWireMessageV3, previous_log_index, "previous_log_index", "uint64_t", _Alignof(uint64_t), &cmeta_type_uint64, &cmeta_data_uint64, NULL), \
        (12, RaftWireMessageV3, previous_log_term, "previous_log_term", "uint64_t", _Alignof(uint64_t), &cmeta_type_uint64, &cmeta_data_uint64, NULL), \
        (13, RaftWireMessageV3, match_index, "match_index", "uint64_t", _Alignof(uint64_t), &cmeta_type_uint64, &cmeta_data_uint64, NULL), \
        (14, RaftWireMessageV3, reject_hint, "reject_hint", "uint64_t", _Alignof(uint64_t), &cmeta_type_uint64, &cmeta_data_uint64, NULL), \
        (15, RaftWireMessageV3, entry1_index, "entry1_index", "uint64_t", _Alignof(uint64_t), &cmeta_type_uint64, &cmeta_data_uint64, NULL)) \
    Schema(M, \
        (16, RaftWireMessageV3, entry1_term, "entry1_term", "uint64_t", _Alignof(uint64_t), &cmeta_type_uint64, &cmeta_data_uint64, NULL), \
        (17, RaftWireMessageV3, entry1_command_id, "entry1_command_id", "uint64_t", _Alignof(uint64_t), &cmeta_type_uint64, &cmeta_data_uint64, NULL), \
        (18, RaftWireMessageV3, entry2_index, "entry2_index", "uint64_t", _Alignof(uint64_t), &cmeta_type_uint64, &cmeta_data_uint64, NULL), \
        (19, RaftWireMessageV3, entry2_term, "entry2_term", "uint64_t", _Alignof(uint64_t), &cmeta_type_uint64, &cmeta_data_uint64, NULL), \
        (20, RaftWireMessageV3, entry2_command_id, "entry2_command_id", "uint64_t", _Alignof(uint64_t), &cmeta_type_uint64, &cmeta_data_uint64, NULL), \
        (21, RaftWireMessageV3, entry3_index, "entry3_index", "uint64_t", _Alignof(uint64_t), &cmeta_type_uint64, &cmeta_data_uint64, NULL), \
        (22, RaftWireMessageV3, entry3_term, "entry3_term", "uint64_t", _Alignof(uint64_t), &cmeta_type_uint64, &cmeta_data_uint64, NULL), \
        (23, RaftWireMessageV3, entry3_command_id, "entry3_command_id", "uint64_t", _Alignof(uint64_t), &cmeta_type_uint64, &cmeta_data_uint64, NULL), \
        (24, RaftWireMessageV3, entry4_index, "entry4_index", "uint64_t", _Alignof(uint64_t), &cmeta_type_uint64, &cmeta_data_uint64, NULL), \
        (25, RaftWireMessageV3, entry4_term, "entry4_term", "uint64_t", _Alignof(uint64_t), &cmeta_type_uint64, &cmeta_data_uint64, NULL), \
        (26, RaftWireMessageV3, entry4_command_id, "entry4_command_id", "uint64_t", _Alignof(uint64_t), &cmeta_type_uint64, &cmeta_data_uint64, NULL), \
        (27, RaftWireMessageV3, entry5_index, "entry5_index", "uint64_t", _Alignof(uint64_t), &cmeta_type_uint64, &cmeta_data_uint64, NULL), \
        (28, RaftWireMessageV3, entry5_term, "entry5_term", "uint64_t", _Alignof(uint64_t), &cmeta_type_uint64, &cmeta_data_uint64, NULL), \
        (29, RaftWireMessageV3, entry5_command_id, "entry5_command_id", "uint64_t", _Alignof(uint64_t), &cmeta_type_uint64, &cmeta_data_uint64, NULL), \
        (30, RaftWireMessageV3, entry6_index, "entry6_index", "uint64_t", _Alignof(uint64_t), &cmeta_type_uint64, &cmeta_data_uint64, NULL), \
        (31, RaftWireMessageV3, entry6_term, "entry6_term", "uint64_t", _Alignof(uint64_t), &cmeta_type_uint64, &cmeta_data_uint64, NULL)) \
    Schema(M, \
        (32, RaftWireMessageV3, entry6_command_id, "entry6_command_id", "uint64_t", _Alignof(uint64_t), &cmeta_type_uint64, &cmeta_data_uint64, NULL), \
        (33, RaftWireMessageV3, entry7_index, "entry7_index", "uint64_t", _Alignof(uint64_t), &cmeta_type_uint64, &cmeta_data_uint64, NULL), \
        (34, RaftWireMessageV3, entry7_term, "entry7_term", "uint64_t", _Alignof(uint64_t), &cmeta_type_uint64, &cmeta_data_uint64, NULL), \
        (35, RaftWireMessageV3, entry7_command_id, "entry7_command_id", "uint64_t", _Alignof(uint64_t), &cmeta_type_uint64, &cmeta_data_uint64, NULL), \
        (36, RaftWireMessageV3, entry8_index, "entry8_index", "uint64_t", _Alignof(uint64_t), &cmeta_type_uint64, &cmeta_data_uint64, NULL), \
        (37, RaftWireMessageV3, entry8_term, "entry8_term", "uint64_t", _Alignof(uint64_t), &cmeta_type_uint64, &cmeta_data_uint64, NULL), \
        (38, RaftWireMessageV3, entry8_command_id, "entry8_command_id", "uint64_t", _Alignof(uint64_t), &cmeta_type_uint64, &cmeta_data_uint64, NULL), \
        (39, RaftWireMessageV3, entry1_data, "entry1_data", "stl_byte_buffer", _Alignof(stl_byte_buffer), &stl_byte_buffer_cmeta_type, &stl_byte_buffer_cmeta_data, NULL), \
        (40, RaftWireMessageV3, entry2_data, "entry2_data", "stl_byte_buffer", _Alignof(stl_byte_buffer), &stl_byte_buffer_cmeta_type, &stl_byte_buffer_cmeta_data, NULL), \
        (41, RaftWireMessageV3, entry3_data, "entry3_data", "stl_byte_buffer", _Alignof(stl_byte_buffer), &stl_byte_buffer_cmeta_type, &stl_byte_buffer_cmeta_data, NULL), \
        (42, RaftWireMessageV3, entry4_data, "entry4_data", "stl_byte_buffer", _Alignof(stl_byte_buffer), &stl_byte_buffer_cmeta_type, &stl_byte_buffer_cmeta_data, NULL), \
        (43, RaftWireMessageV3, entry5_data, "entry5_data", "stl_byte_buffer", _Alignof(stl_byte_buffer), &stl_byte_buffer_cmeta_type, &stl_byte_buffer_cmeta_data, NULL), \
        (44, RaftWireMessageV3, entry6_data, "entry6_data", "stl_byte_buffer", _Alignof(stl_byte_buffer), &stl_byte_buffer_cmeta_type, &stl_byte_buffer_cmeta_data, NULL), \
        (45, RaftWireMessageV3, entry7_data, "entry7_data", "stl_byte_buffer", _Alignof(stl_byte_buffer), &stl_byte_buffer_cmeta_type, &stl_byte_buffer_cmeta_data, NULL), \
        (46, RaftWireMessageV3, entry8_data, "entry8_data", "stl_byte_buffer", _Alignof(stl_byte_buffer), &stl_byte_buffer_cmeta_type, &stl_byte_buffer_cmeta_data, NULL)) \

enum { RaftWireMessageV3_CMETA_FIELD_COUNT = 0 Replay(RaftWireMessageV3_CMETA_FIELD_SCHEMA, DATABIND_RECORD_COUNT) };
static cmeta_field_desc RaftWireMessageV3_CMETA_LAYOUT_FIELDS[RaftWireMessageV3_CMETA_FIELD_COUNT ? RaftWireMessageV3_CMETA_FIELD_COUNT : 1u];
static cmeta_data_field_desc RaftWireMessageV3_CMETA_FIELDS[RaftWireMessageV3_CMETA_FIELD_COUNT ? RaftWireMessageV3_CMETA_FIELD_COUNT : 1u];
static const cmeta_struct_desc RaftWireMessageV3_CMETA_LAYOUT = {
    "RaftWireMessageV3_t", sizeof(RaftWireMessageV3_t), _Alignof(RaftWireMessageV3_t),
    RaftWireMessageV3_CMETA_LAYOUT_FIELDS, RaftWireMessageV3_CMETA_FIELD_COUNT
};
static void RaftWireMessageV3_CMETA_INIT_FIELDS(void) {
    Replay(RaftWireMessageV3_CMETA_FIELD_SCHEMA, DATABIND_RECORD_LAYOUT)
    Replay(RaftWireMessageV3_CMETA_FIELD_SCHEMA, DATABIND_RECORD_DATA)
}
#undef RaftWireMessageV3_CMETA_FIELD_SCHEMA
static const cmeta_data_struct_shape RaftWireMessageV3_CMETA_SHAPE = {
    &RaftWireMessageV3_CMETA_LAYOUT, RaftWireMessageV3_CMETA_FIELDS,
    RaftWireMessageV3_CMETA_FIELD_COUNT
};
TBE_GENERATED_API const cmeta_data_desc RaftWireMessageV3_CMETA_DATA = {
    .struct_size = sizeof(cmeta_data_desc), .abi_version = CMETA_DATA_DESC_ABI_VERSION,
    .stable_id = "tbe.native.TurboRaftWireV3.RaftWireMessageV3_t.data", .display_name = "RaftWireMessageV3_t",
    .kind = CMETA_DATA_STRUCT, .storage_type = &RaftWireMessageV3_CMETA_TYPE,
    .shape = &RaftWireMessageV3_CMETA_SHAPE
};
DataBindStatus RaftWireMessageV3_cmeta_data(const cmeta_data_desc **out, DataBindError *error) {
    TurboRaftWireV3_CMETA_INIT_ALL();
    if (out == NULL)
        return TurboRaftWireV3_schema_codec_error(
            error, DATA_BIND_ERR_INVALID_ARG, "RaftWireMessageV3",
            "Native CMeta output is required");
    *out = &RaftWireMessageV3_CMETA_DATA;
    return TurboRaftWireV3_schema_codec_error(error, DATA_BIND_OK, NULL, NULL);
}
#undef DATABIND_RECORD_COUNT
#undef DATABIND_RECORD_LAYOUT
#undef DATABIND_RECORD_DATA

static void TurboRaftWireV3_CMETA_INIT_FIELDS_ALL(void) {

    RaftWireMessageV3_CMETA_INIT_FIELDS();
}

static void TurboRaftWireV3_CMETA_INIT_ALL(void) {
    static cmeta_once_t initialized = SALTS_ONCE_INIT;
    cmeta_once(&initialized, TurboRaftWireV3_CMETA_INIT_FIELDS_ALL);
}


typedef struct TurboRaftWireV3_message_workspace_s {
    DataBindNativeOptions options;
    void *allocation;
} TurboRaftWireV3_message_workspace_t;

typedef struct TurboRaftWireV3_text_output_s {
    char *data;
    size_t length;
    size_t capacity;
    DataBindStatus status;
} TurboRaftWireV3_text_output_t;

typedef struct TurboRaftWireV3_binary_output_s {
    uint8_t *data;
    uint8_t *fixed;
    size_t capacity;
    size_t length;
    DataBindStatus status;
} TurboRaftWireV3_binary_output_t;

static DataBindStatus TurboRaftWireV3_message_diag_error(
    DataBindError *error, DataBindStatus status, const char *type_name,
    const DataBindMessagePlanDiagnostic *diagnostic) {
    const char *path = type_name;
    const char *message = "Generated MessagePlan conversion failed";
    if (diagnostic != NULL) {
        if (diagnostic->schema_field[0] != '\0')
            path = diagnostic->schema_field;
        if (diagnostic->message[0] != '\0')
            message = diagnostic->message;
    }
    return TurboRaftWireV3_schema_codec_error(
        error, status, path, message);
}

static int TurboRaftWireV3_message_state_bindings_valid(
    const DataBindNativeStateBinding *bindings, size_t count,
    size_t object_size) {
    size_t i;
    if (count != 0u && bindings == NULL) return 0;
    for (i = 0u; i < count; ++i) {
        if (bindings[i].size < sizeof(bindings[i]) ||
            bindings[i].bit >= 8u ||
            bindings[i].byte_offset >= object_size)
            return 0;
    }
    return 1;
}

static int TurboRaftWireV3_message_state_copy(
    const DataBindNativeStateBinding *bindings, size_t count,
    unsigned char *destination, const unsigned char *source,
    size_t object_size) {
    size_t i;
    if (count != 0u && (bindings == NULL || destination == NULL || source == NULL))
        return 0;
    for (i = 0u; i < count; ++i) {
        const DataBindNativeStateBinding *binding = &bindings[i];
        unsigned char mask;
        if (binding->size < sizeof(*binding) || binding->bit >= 8u ||
            binding->byte_offset >= object_size)
            return 0;
        mask = (unsigned char)(1u << binding->bit);
        if ((source[binding->byte_offset] & mask) != 0u)
            destination[binding->byte_offset] |= mask;
    }
    return 1;
}

static int TurboRaftWireV3_message_state_clear(
    const DataBindNativeStateBinding *bindings, size_t count,
    unsigned char *object, size_t object_size) {
    size_t i;
    if (count != 0u && (bindings == NULL || object == NULL))
        return 0;
    for (i = 0u; i < count; ++i) {
        const DataBindNativeStateBinding *binding = &bindings[i];
        unsigned char mask;
        if (binding->size < sizeof(*binding) || binding->bit >= 8u ||
            binding->byte_offset >= object_size)
            return 0;
        mask = (unsigned char)(1u << binding->bit);
        object[binding->byte_offset] &= (unsigned char)~mask;
    }
    return 1;
}

static void TurboRaftWireV3_message_workspace_close(
    TurboRaftWireV3_message_workspace_t *workspace) {
    if (workspace == NULL) return;
    free(workspace->allocation);
    *workspace = (TurboRaftWireV3_message_workspace_t){0};
}

static DataBindStatus TurboRaftWireV3_message_workspace_open(
    const DataBindNativeTypeBinding *binding,
    const DataBindMessagePlan *plan,
    size_t descriptor_depth,
    size_t descriptor_nodes,
    int include_message_bitmap,
    TurboRaftWireV3_message_workspace_t *out,
    DataBindError *error) {
    DataBindNativeOptions options = DATA_BIND_NATIVE_OPTIONS_INIT;
    DataBindNativeRequirements requirements = DATA_BIND_NATIVE_REQUIREMENTS_INIT;
    DataBindNativeDiagnostic diagnostic = DATA_BIND_NATIVE_DIAGNOSTIC_INIT;
    size_t probe_bytes = 0u;
    size_t bitmap_bytes = 0u;
    size_t total_bytes;
    size_t alignment_extra;
    void *probe = NULL;
    void *storage = NULL;
    DataBindStatus status;

    if (out == NULL || binding == NULL || binding->data == NULL ||
        descriptor_depth == 0u || descriptor_nodes == 0u)
        return TurboRaftWireV3_schema_codec_error(
            error, DATA_BIND_ERR_INVALID_ARG,
            binding != NULL ? binding->idl_type_name : NULL,
            "Invalid generated native workspace request");
    *out = (TurboRaftWireV3_message_workspace_t){0};

    options.max_depth = descriptor_depth;
    options.max_items = descriptor_nodes;
    options.max_owned_bytes = SIZE_MAX;

    status = data_bind_native_probe_workspace_size(
        descriptor_depth, &probe_bytes);
    if (status != DATA_BIND_OK)
        return TurboRaftWireV3_schema_codec_error(
            error, status, binding->idl_type_name,
            "Unable to size generated native probe workspace");

    probe = malloc(probe_bytes != 0u ? probe_bytes : 1u);
    if (probe == NULL)
        return TurboRaftWireV3_schema_codec_error(
            error, DATA_BIND_ERR_OOM, binding->idl_type_name,
            "Unable to allocate generated native probe workspace");

    options.workspace = probe;
    options.workspace_bytes = probe_bytes;
    status = data_bind_native_measure(
        &options, binding->data, &requirements, &diagnostic);
    free(probe);
    probe = NULL;
    if (status != DATA_BIND_OK) {
        if (error != NULL) *error = diagnostic.error;
        return status;
    }

    /*
     * descriptor_nodes is a compile-time graph-admission bound, not a runtime
     * payload cardinality limit. Reuse exact measured scratch while preserving
     * the historical generated conversion contract: list/set/map element count
     * is not implicitly capped by the number of descriptor nodes.
     */
    options.max_items = SIZE_MAX;

    if (include_message_bitmap) {
        size_t field_count =
            data_bind_message_plan_field_count(plan);
        if (field_count > SIZE_MAX - 7u)
            return TurboRaftWireV3_schema_codec_error(
                error, DATA_BIND_ERR_LIMIT, binding->idl_type_name,
                "Generated MessagePlan field bitmap size overflow");
        bitmap_bytes = (field_count + 7u) / 8u;
    }

    if (requirements.workspace_alignment == 0u ||
        requirements.decode_bytes > SIZE_MAX - bitmap_bytes)
        return TurboRaftWireV3_schema_codec_error(
            error, DATA_BIND_ERR_LIMIT, binding->idl_type_name,
            "Generated native workspace size overflow");
    total_bytes = bitmap_bytes + requirements.decode_bytes;
    alignment_extra = requirements.workspace_alignment - 1u;
    if (alignment_extra > SIZE_MAX - total_bytes)
        return TurboRaftWireV3_schema_codec_error(
            error, DATA_BIND_ERR_LIMIT, binding->idl_type_name,
            "Generated native workspace alignment overflow");
    total_bytes += alignment_extra;

    storage = malloc(total_bytes != 0u ? total_bytes : 1u);
    if (storage == NULL)
        return TurboRaftWireV3_schema_codec_error(
            error, DATA_BIND_ERR_OOM, binding->idl_type_name,
            "Unable to allocate generated native conversion workspace");

    options.workspace = storage;
    options.workspace_bytes = total_bytes;
    out->options = options;
    out->allocation = storage;
    return DATA_BIND_OK;
}

static void TurboRaftWireV3_message_heap_release(
    void *authority, void *resource) {
    (void)authority;
    free(resource);
}

static void TurboRaftWireV3_message_workspace_release(
    void *authority, void *resource) {
    (void)authority;
    TurboRaftWireV3_message_workspace_close(
        (TurboRaftWireV3_message_workspace_t *)resource);
}

static void TurboRaftWireV3_message_format_plan_release(
    void *authority, void *resource) {
    (void)authority;
    data_bind_format_plan_free((DataBindFormatPlan *)resource);
}

static void TurboRaftWireV3_message_value_release(
    void *authority, void *resource) {
    const DataBindNativeTypeBinding *binding =
        (const DataBindNativeTypeBinding *)authority;
    /* Admitted providers must restore owned values without failure. */
    if (cmeta_data_value_restore_zero(binding->data, resource) != CMETA_OK)
        abort();
}

static DataBindStatus TurboRaftWireV3_message_from_text(
    DataBind *codec,
    const DataBindMessageNativeArtifact *artifact,
    const DataBindFormatProvider *provider,
    size_t descriptor_depth,
    size_t descriptor_nodes,
    size_t csv_row,
    void *destination,
    size_t object_size,
    const char *data,
    size_t len,
    DataBindError *error) {
    const char *type_name = artifact != NULL ? artifact->type_name : NULL;
    DataBindNativeTypeBindingResolverFn resolve =
        artifact != NULL ? artifact->native_binding : NULL;
    DataBindNativeTypeBinding binding =
        DATA_BIND_NATIVE_TYPE_BINDING_INIT(NULL, NULL);
    const DataBindMessagePlan *plan = NULL;
    DataBindMessagePlanDiagnostic diagnostic =
        DATA_BIND_MESSAGE_PLAN_DIAGNOSTIC_INIT;
    DataBindFormatReader reader = DATA_BIND_FORMAT_READER_INIT;
    DataBindFormatPlan *format_plan = NULL;
    DataBindFormatCanonicalReader canonical =
        DATA_BIND_FORMAT_CANONICAL_READER_INIT;
    cserde_reader *decode_reader = NULL;
    TurboRaftWireV3_message_workspace_t workspace = {0};
    DataBindStatus status;
    DataBindStatus close_status;
    cmeta_status cmeta_result;
    void *temporary_allocation = NULL;
    void *temporary = NULL;
    size_t temporary_bytes;
    size_t temporary_align;
    uintptr_t temporary_base;
    size_t temporary_padding;
    enum {
        CLEANUP_HEAP,
        CLEANUP_WORKSPACE,
        CLEANUP_FORMAT_PLAN,
        CLEANUP_VALUE,
        CLEANUP_COUNT
    };
    cmeta_cleanup cleanups[CLEANUP_COUNT] = {CMETA_CLEANUP_INIT};

    if (codec == NULL || type_name == NULL || resolve == NULL ||
        provider == NULL || destination == NULL ||
        data == NULL || object_size == 0u)
        return TurboRaftWireV3_schema_codec_error(
            error, DATA_BIND_ERR_INVALID_ARG, type_name,
            "Invalid generated text-to-native arguments");

    status = data_bind_message_plan_acquire_generated(codec, artifact, &plan, error);
    if (status != DATA_BIND_OK) return status;
    binding = *data_bind_message_plan_native_binding(plan);
    if (binding.data == NULL || binding.data->storage_type == NULL ||
        binding.data->storage_type->size != object_size ||
        binding.data->storage_type->align == 0u ||
        !cmeta_data_value_move_supported(binding.data) ||
        !TurboRaftWireV3_message_state_bindings_valid(
            binding.presence, binding.presence_count, object_size) ||
        !TurboRaftWireV3_message_state_bindings_valid(
            binding.nulls, binding.null_count, object_size))
        return TurboRaftWireV3_schema_codec_error(
            error, DATA_BIND_ERR_SCHEMA, type_name,
            "Generated Message native graph is not publishable");

    temporary_align = binding.data->storage_type->align;
    if (temporary_align - 1u > SIZE_MAX - object_size)
        return TurboRaftWireV3_schema_codec_error(
            error, DATA_BIND_ERR_LIMIT, type_name,
            "Generated native staging size overflow");
    temporary_bytes = object_size + temporary_align - 1u;
    temporary_allocation = malloc(
        temporary_bytes != 0u ? temporary_bytes : 1u);
    if (temporary_allocation == NULL)
        return TurboRaftWireV3_schema_codec_error(
            error, DATA_BIND_ERR_OOM, type_name,
            "Unable to allocate generated native staging");
    if (cmeta_cleanup_arm(&cleanups[CLEANUP_HEAP],
            TurboRaftWireV3_message_heap_release, NULL,
            temporary_allocation) != CMETA_OK)
        abort();
    temporary_base = (uintptr_t)temporary_allocation;
    temporary_padding = (size_t)(temporary_base % temporary_align);
    if (temporary_padding != 0u)
        temporary_padding = temporary_align - temporary_padding;
    temporary = (unsigned char *)temporary_allocation + temporary_padding;
    memset(temporary, 0, object_size);

    status = TurboRaftWireV3_message_workspace_open(
        &binding, plan, descriptor_depth, descriptor_nodes, 1,
        &workspace, error);
    if (status != DATA_BIND_OK) goto cleanup;

    if (cmeta_cleanup_arm(&cleanups[CLEANUP_WORKSPACE],
            TurboRaftWireV3_message_workspace_release, NULL,
            &workspace) != CMETA_OK)
        abort();
    if (provider->format == DATA_BIND_FORMAT_XML) {
        status = data_bind_format_plan_compile_reader(
            codec, type_name, DATA_BIND_FORMAT_XML, &format_plan, error);
        if (status != DATA_BIND_OK) goto cleanup;
    } else if (provider->format == DATA_BIND_FORMAT_CSV ||
               provider->format == DATA_BIND_FORMAT_JSON ||
               provider->format == DATA_BIND_FORMAT_YAML) {
        status = data_bind_format_plan_compile(
            codec, type_name, provider->format, &format_plan, error);
        if (status != DATA_BIND_OK) goto cleanup;
    }
    if (format_plan != NULL &&
        cmeta_cleanup_arm(&cleanups[CLEANUP_FORMAT_PLAN],
            TurboRaftWireV3_message_format_plan_release, NULL,
            format_plan) != CMETA_OK)
        abort();

    if (provider->format == DATA_BIND_FORMAT_CSV) {
        status = data_bind_builtin_format_reader_open_csv_row(
            data, len, csv_row, descriptor_depth, &reader, error);
        if (status != DATA_BIND_OK) goto cleanup;
    } else {
        status = data_bind_format_reader_open(
            provider, data, len, descriptor_depth, &reader, error);
        if (status != DATA_BIND_OK) goto cleanup;
    }

    if (format_plan != NULL) {
        status = data_bind_format_canonical_reader_init(
            format_plan, reader.reader, &canonical, error);
        if (status != DATA_BIND_OK) goto cleanup;
        decode_reader = data_bind_format_canonical_reader_reader(&canonical);
        if (decode_reader == NULL) {
            status = TurboRaftWireV3_schema_codec_error(
                error, DATA_BIND_ERR_RUNTIME, type_name,
                "Generated canonical format reader is unavailable");
            goto cleanup;
        }
    } else {
        decode_reader = reader.reader;
    }

    status = data_bind_message_plan_decode_native_format(
        plan, &workspace.options, provider->format, decode_reader,
        temporary, object_size, &diagnostic);
    /* MessagePlan owns rollback until a successful decode transfers the value. */
    if (status == DATA_BIND_OK &&
        cmeta_cleanup_arm(&cleanups[CLEANUP_VALUE],
            TurboRaftWireV3_message_value_release, &binding,
            temporary) != CMETA_OK)
        abort();
    /*
     * Historical generated CSV treats an empty required cell as a row/type
     * mismatch after omitting that cell from the logical object. MessagePlan's
     * generic missing-required status is TYPE_NOT_FOUND, so preserve the CSV
     * public contract only at this generated format boundary.
     */
    if (provider->format == DATA_BIND_FORMAT_CSV &&
        status == DATA_BIND_ERR_TYPE_NOT_FOUND)
        status = DATA_BIND_ERR_TYPE_MISMATCH;
    close_status = data_bind_format_reader_close(&reader);
    if (status == DATA_BIND_OK && close_status != DATA_BIND_OK)
        status = TurboRaftWireV3_schema_codec_error(
            error, close_status, type_name,
            "Generated format reader close failed");
    else if (status != DATA_BIND_OK)
        (void)TurboRaftWireV3_message_diag_error(
            error, status, type_name, &diagnostic);
    if (status != DATA_BIND_OK) goto cleanup;

    cmeta_result = cmeta_data_value_restore_zero(binding.data, destination);
    if (cmeta_result != CMETA_OK) {
        status = TurboRaftWireV3_schema_codec_error(
            error, DATA_BIND_ERR_RUNTIME, type_name,
            "Generated destination could not restore semantic zero");
        goto cleanup;
    }
    if (!TurboRaftWireV3_message_state_clear(
            binding.presence, binding.presence_count,
            (unsigned char *)destination, object_size) ||
        !TurboRaftWireV3_message_state_clear(
            binding.nulls, binding.null_count,
            (unsigned char *)destination, object_size)) {
        status = TurboRaftWireV3_schema_codec_error(
            error, DATA_BIND_ERR_SCHEMA, type_name,
            "Generated destination state overlay is invalid");
        goto cleanup;
    }

    cmeta_result = cmeta_data_value_move(
        binding.data, destination, temporary);
    if (cmeta_result != CMETA_OK) {
        (void)cmeta_data_value_restore_zero(binding.data, destination);
        (void)TurboRaftWireV3_message_state_clear(
            binding.presence, binding.presence_count,
            (unsigned char *)destination, object_size);
        (void)TurboRaftWireV3_message_state_clear(
            binding.nulls, binding.null_count,
            (unsigned char *)destination, object_size);
        status = TurboRaftWireV3_schema_codec_error(
            error, DATA_BIND_ERR_RUNTIME, type_name,
            "Generated native value publication failed");
        goto cleanup;
    }
    cmeta_cleanup_disarm(&cleanups[CLEANUP_VALUE]);

    if (!TurboRaftWireV3_message_state_copy(
            binding.presence, binding.presence_count,
            (unsigned char *)destination,
            (const unsigned char *)temporary, object_size) ||
        !TurboRaftWireV3_message_state_copy(
            binding.nulls, binding.null_count,
            (unsigned char *)destination,
            (const unsigned char *)temporary, object_size)) {
        (void)cmeta_data_value_restore_zero(binding.data, destination);
        (void)TurboRaftWireV3_message_state_clear(
            binding.presence, binding.presence_count,
            (unsigned char *)destination, object_size);
        (void)TurboRaftWireV3_message_state_clear(
            binding.nulls, binding.null_count,
            (unsigned char *)destination, object_size);
        status = TurboRaftWireV3_schema_codec_error(
            error, DATA_BIND_ERR_SCHEMA, type_name,
            "Generated native state overlay is invalid");
        goto cleanup;
    }

    memset(temporary, 0, object_size);
    status = TurboRaftWireV3_schema_codec_error(
        error, DATA_BIND_OK, NULL, NULL);

cleanup:
    /* Close consumes the reader even on failure; preserve the first error. */
    if (reader.provider != NULL) {
        close_status = data_bind_format_reader_close(&reader);
        if (status == DATA_BIND_OK && close_status != DATA_BIND_OK)
            status = TurboRaftWireV3_schema_codec_error(
                error, close_status, type_name,
                "Generated format reader close failed");
    }
    cmeta_cleanup_reverse(cleanups, CLEANUP_COUNT);
    return status;
}

static int TurboRaftWireV3_text_output_write(
    const void *data, size_t len, void *user) {
    TurboRaftWireV3_text_output_t *output =
        (TurboRaftWireV3_text_output_t *)user;
    size_t required;
    size_t capacity;
    char *resized;

    if (output == NULL || (data == NULL && len != 0u)) return -1;
    if (output->status != DATA_BIND_OK) return -1;
    if (len > SIZE_MAX - output->length - 1u) {
        output->status = DATA_BIND_ERR_LIMIT;
        return -1;
    }
    required = output->length + len + 1u;
    if (required > output->capacity) {
        capacity = output->capacity != 0u ? output->capacity : 128u;
        while (capacity < required) {
            if (capacity > SIZE_MAX / 2u) {
                capacity = required;
                break;
            }
            capacity *= 2u;
        }
        resized = (char *)realloc(output->data, capacity);
        if (resized == NULL) {
            output->status = DATA_BIND_ERR_OOM;
            return -1;
        }
        output->data = resized;
        output->capacity = capacity;
    }
    if (len != 0u)
        memcpy(output->data + output->length, data, len);
    output->length += len;
    output->data[output->length] = '\0';
    return 0;
}

static int TurboRaftWireV3_binary_output_write(
    const void *data, size_t len, void *user) {
    TurboRaftWireV3_binary_output_t *output =
        (TurboRaftWireV3_binary_output_t *)user;
    uint8_t *allocated;

    if (output == NULL || (data == NULL && len != 0u)) return -1;
    if (output->status != DATA_BIND_OK || output->length != 0u ||
        output->data != NULL)
        return -1;

    if (output->fixed != NULL) {
        output->length = len;
        if (len > output->capacity) {
            output->status = DATA_BIND_ERR_LIMIT;
            return -1;
        }
        if (len != 0u) memcpy(output->fixed, data, len);
        return 0;
    }

    allocated = (uint8_t *)malloc(len != 0u ? len : 1u);
    if (allocated == NULL) {
        output->status = DATA_BIND_ERR_OOM;
        return -1;
    }
    if (len != 0u) memcpy(allocated, data, len);
    output->data = allocated;
    output->length = len;
    output->capacity = len;
    return 0;
}

static DataBindStatus TurboRaftWireV3_message_to_binary(
    DataBind *codec,
    const DataBindMessageNativeArtifact *artifact,
    const DataBindFormatProvider *provider,
    size_t descriptor_depth,
    size_t descriptor_nodes,
    const void *object,
    size_t object_size,
    uint8_t **out,
    uint8_t *fixed_output,
    size_t fixed_capacity,
    size_t *out_len,
    DataBindError *error) {
    const char *type_name = artifact != NULL ? artifact->type_name : NULL;
    DataBindNativeTypeBindingResolverFn resolve =
        artifact != NULL ? artifact->native_binding : NULL;
    DataBindNativeTypeBinding binding =
        DATA_BIND_NATIVE_TYPE_BINDING_INIT(NULL, NULL);
    const DataBindMessagePlan *plan = NULL;
    DataBindMessagePlanDiagnostic diagnostic =
        DATA_BIND_MESSAGE_PLAN_DIAGNOSTIC_INIT;
    DataBindFormatWriter writer = DATA_BIND_FORMAT_WRITER_INIT;
    TurboRaftWireV3_message_workspace_t workspace = {0};
    TurboRaftWireV3_binary_output_t output = {0};
    DataBindStatus status;
    DataBindStatus close_status;
    cserde_status finish_status = CSERDE_OK;

    if (out != NULL) *out = NULL;
    if (out_len != NULL) *out_len = 0u;
    if (codec == NULL || type_name == NULL || resolve == NULL ||
        provider == NULL || provider->format != DATA_BIND_FORMAT_BINARY ||
        object == NULL || object_size == 0u || out_len == NULL ||
        ((out == NULL) == (fixed_output == NULL)))
        return TurboRaftWireV3_schema_codec_error(
            error, DATA_BIND_ERR_INVALID_ARG, type_name,
            "Invalid generated native-to-Binary arguments");

    output.fixed = fixed_output;
    output.capacity = fixed_capacity;

    status = data_bind_message_plan_acquire_generated(codec, artifact, &plan, error);
    if (status != DATA_BIND_OK) return status;
    binding = *data_bind_message_plan_native_binding(plan);
    if (binding.data == NULL || binding.data->storage_type == NULL ||
        binding.data->storage_type->size != object_size)
        return TurboRaftWireV3_schema_codec_error(
            error, DATA_BIND_ERR_SCHEMA, type_name,
            "Generated Message native graph is invalid");

    status = TurboRaftWireV3_message_workspace_open(
        &binding, plan, descriptor_depth, descriptor_nodes, 0,
        &workspace, error);
    if (status != DATA_BIND_OK) goto cleanup;

    status = data_bind_format_writer_open(
        provider, TurboRaftWireV3_binary_output_write, &output,
        descriptor_depth, &writer, error);
    if (status != DATA_BIND_OK) goto cleanup;

    status = data_bind_message_plan_encode_native(
        plan, &workspace.options, object, object_size,
        writer.writer, &diagnostic);
    if (status == DATA_BIND_OK) {
        finish_status = cserde_writer_finish(writer.writer);
        if (finish_status != CSERDE_OK)
            status = DATA_BIND_ERR_RUNTIME;
    } else {
        (void)TurboRaftWireV3_message_diag_error(
            error, status, type_name, &diagnostic);
    }

    close_status = data_bind_format_writer_close(
        &writer,
        (status == DATA_BIND_OK || finish_status != CSERDE_OK)
            ? error
            : NULL);
    if (status == DATA_BIND_OK && close_status != DATA_BIND_OK)
        status = close_status;
    else if (status == DATA_BIND_ERR_RUNTIME &&
             finish_status != CSERDE_OK &&
             close_status != DATA_BIND_OK)
        status = close_status;

    if (output.status != DATA_BIND_OK)
        status = TurboRaftWireV3_schema_codec_error(
            error, output.status, type_name,
            "Generated Binary output sink rejected publication");
    if (output.status == DATA_BIND_ERR_LIMIT) *out_len = output.length;
    if (status != DATA_BIND_OK) goto cleanup;

    if (out != NULL) {
        *out = output.data;
        output.data = NULL;
    }
    *out_len = output.length;
    status = TurboRaftWireV3_schema_codec_error(
        error, DATA_BIND_OK, NULL, NULL);

cleanup:
    if (writer.provider != NULL)
        (void)data_bind_format_writer_close(&writer, error);
    free(output.data);
    TurboRaftWireV3_message_workspace_close(&workspace);
    return status;
}

static DataBindStatus TurboRaftWireV3_message_to_text(
    DataBind *codec,
    const DataBindMessageNativeArtifact *artifact,
    DataBindFormat format,
    size_t descriptor_depth,
    size_t descriptor_nodes,
    const void *object,
    size_t object_size,
    char **out,
    size_t *out_len,
    DataBindError *error) {
    const char *type_name = artifact != NULL ? artifact->type_name : NULL;
    DataBindNativeTypeBindingResolverFn resolve =
        artifact != NULL ? artifact->native_binding : NULL;
    DataBindNativeTypeBinding binding =
        DATA_BIND_NATIVE_TYPE_BINDING_INIT(NULL, NULL);
    const DataBindMessagePlan *plan = NULL;
    DataBindMessagePlanDiagnostic diagnostic =
        DATA_BIND_MESSAGE_PLAN_DIAGNOSTIC_INIT;
    DataBindFormatPlan *format_plan = NULL;
    DataBindFormatWriter writer = DATA_BIND_FORMAT_WRITER_INIT;
    DataBindFormatCanonicalWriter canonical_writer =
        DATA_BIND_FORMAT_CANONICAL_WRITER_INIT;
    TurboRaftWireV3_message_workspace_t workspace = {0};
    TurboRaftWireV3_text_output_t output = {0};
    const DataBindFormatProvider *provider = NULL;
    DataBindStatus status;
    DataBindStatus close_status;

    if (out != NULL) *out = NULL;
    if (out_len != NULL) *out_len = 0u;
    if (codec == NULL || type_name == NULL || resolve == NULL ||
        object == NULL || object_size == 0u || out == NULL || out_len == NULL ||
        (format != DATA_BIND_FORMAT_JSON &&
         format != DATA_BIND_FORMAT_YAML))
        return TurboRaftWireV3_schema_codec_error(
            error, DATA_BIND_ERR_INVALID_ARG, type_name,
            "Invalid generated native-to-text arguments");

    provider = data_bind_builtin_format_provider(format);
    if (provider == NULL || provider->format != format)
        return TurboRaftWireV3_schema_codec_error(
            error, DATA_BIND_ERR_SCHEMA, type_name,
            "Generated text output provider is unavailable");

    status = data_bind_message_plan_acquire_generated(codec, artifact, &plan, error);
    if (status != DATA_BIND_OK) return status;
    binding = *data_bind_message_plan_native_binding(plan);
    if (binding.data == NULL || binding.data->storage_type == NULL ||
        binding.data->storage_type->size != object_size)
        return TurboRaftWireV3_schema_codec_error(
            error, DATA_BIND_ERR_SCHEMA, type_name,
            "Generated Message native graph is invalid");

    status = data_bind_format_plan_compile(
        codec, type_name, format, &format_plan, error);
    if (status != DATA_BIND_OK) goto cleanup;

    status = TurboRaftWireV3_message_workspace_open(
        &binding, plan, descriptor_depth, descriptor_nodes, 0,
        &workspace, error);
    if (status != DATA_BIND_OK) goto cleanup;

    status = data_bind_format_writer_open(
        provider, TurboRaftWireV3_text_output_write, &output,
        descriptor_depth, &writer, error);
    if (status != DATA_BIND_OK) goto cleanup;

    status = data_bind_format_canonical_writer_init(
        format_plan, writer.writer, &canonical_writer, error);
    if (status != DATA_BIND_OK) goto cleanup;

    status = data_bind_message_plan_encode_native(
        plan, &workspace.options, object, object_size,
        data_bind_format_canonical_writer_writer(&canonical_writer),
        &diagnostic);
    if (status == DATA_BIND_OK) {
        if (cserde_writer_finish(
                data_bind_format_canonical_writer_writer(
                    &canonical_writer)) != CSERDE_OK)
            status = TurboRaftWireV3_schema_codec_error(
                error, DATA_BIND_ERR_RUNTIME, type_name,
                "Generated canonical text writer did not finish one record");
    } else {
        (void)TurboRaftWireV3_message_diag_error(
            error, status, type_name, &diagnostic);
    }

    close_status = data_bind_format_writer_close(&writer, error);
    if (status == DATA_BIND_OK && close_status != DATA_BIND_OK)
        status = close_status;

    if (output.status != DATA_BIND_OK)
        status = TurboRaftWireV3_schema_codec_error(
            error, output.status, type_name,
            "Generated text output buffer failed");
    if (status != DATA_BIND_OK) goto cleanup;

    if (output.data == NULL) {
        output.data = (char *)malloc(1u);
        if (output.data == NULL) {
            status = TurboRaftWireV3_schema_codec_error(
                error, DATA_BIND_ERR_OOM, type_name,
                "Unable to allocate generated text output");
            goto cleanup;
        }
        output.data[0] = '\0';
    }
    *out = output.data;
    *out_len = output.length;
    output.data = NULL;
    status = TurboRaftWireV3_schema_codec_error(
        error, DATA_BIND_OK, NULL, NULL);

cleanup:
    if (writer.provider != NULL)
        (void)data_bind_format_writer_close(&writer, error);
    free(output.data);
    data_bind_format_plan_free(format_plan);
    TurboRaftWireV3_message_workspace_close(&workspace);
    return status;
}

static DataBindStatus TurboRaftWireV3_message_to_xml(
    DataBind *codec,
    const DataBindMessageNativeArtifact *artifact,
    size_t descriptor_depth,
    size_t descriptor_nodes,
    const void *object,
    size_t object_size,
    char **out,
    size_t *out_len,
    DataBindError *error) {
    const char *type_name = artifact != NULL ? artifact->type_name : NULL;
    DataBindNativeTypeBindingResolverFn resolve =
        artifact != NULL ? artifact->native_binding : NULL;
    DataBindNativeTypeBinding binding =
        DATA_BIND_NATIVE_TYPE_BINDING_INIT(NULL, NULL);
    const DataBindMessagePlan *plan = NULL;
    DataBindMessagePlanDiagnostic diagnostic =
        DATA_BIND_MESSAGE_PLAN_DIAGNOSTIC_INIT;
    DataBindFormatPlan *format_plan = NULL;
    DataBindXmlWriter writer = DATA_BIND_XML_WRITER_INIT;
    DataBindFormatCanonicalWriter canonical_writer =
        DATA_BIND_FORMAT_CANONICAL_WRITER_INIT;
    TurboRaftWireV3_message_workspace_t workspace = {0};
    TurboRaftWireV3_text_output_t output = {0};
    DataBindStatus status;
    DataBindStatus close_status;

    if (out != NULL) *out = NULL;
    if (out_len != NULL) *out_len = 0u;
    if (codec == NULL || type_name == NULL || resolve == NULL ||
        object == NULL || object_size == 0u || out == NULL || out_len == NULL)
        return TurboRaftWireV3_schema_codec_error(
            error, DATA_BIND_ERR_INVALID_ARG, type_name,
            "Invalid generated native-to-XML arguments");

    status = data_bind_message_plan_acquire_generated(codec, artifact, &plan, error);
    if (status != DATA_BIND_OK) return status;
    binding = *data_bind_message_plan_native_binding(plan);
    if (binding.data == NULL || binding.data->storage_type == NULL ||
        binding.data->storage_type->size != object_size)
        return TurboRaftWireV3_schema_codec_error(
            error, DATA_BIND_ERR_SCHEMA, type_name,
            "Generated Message native graph is invalid");

    status = data_bind_format_plan_compile(
        codec, type_name, DATA_BIND_FORMAT_XML, &format_plan, error);
    if (status != DATA_BIND_OK) goto cleanup;

    status = TurboRaftWireV3_message_workspace_open(
        &binding, plan, descriptor_depth, descriptor_nodes, 0,
        &workspace, error);
    if (status != DATA_BIND_OK) goto cleanup;

    status = data_bind_xml_writer_open_root(
        type_name, TurboRaftWireV3_text_output_write, &output,
        descriptor_depth, &writer, error);
    if (status != DATA_BIND_OK) goto cleanup;

    status = data_bind_format_canonical_writer_init(
        format_plan, data_bind_xml_writer_writer(&writer),
        &canonical_writer, error);
    if (status != DATA_BIND_OK) goto cleanup;

    status = data_bind_message_plan_encode_native(
        plan, &workspace.options, object, object_size,
        data_bind_format_canonical_writer_writer(&canonical_writer),
        &diagnostic);
    if (status == DATA_BIND_OK) {
        if (cserde_writer_finish(
                data_bind_format_canonical_writer_writer(
                    &canonical_writer)) != CSERDE_OK)
            status = TurboRaftWireV3_schema_codec_error(
                error, DATA_BIND_ERR_RUNTIME, type_name,
                "Generated canonical XML writer did not finish one record");
    } else {
        (void)TurboRaftWireV3_message_diag_error(
            error, status, type_name, &diagnostic);
    }

    close_status = data_bind_xml_writer_close(&writer, error);
    if (status == DATA_BIND_OK && close_status != DATA_BIND_OK)
        status = close_status;

    if (output.status != DATA_BIND_OK)
        status = TurboRaftWireV3_schema_codec_error(
            error, output.status, type_name,
            "Generated XML output buffer failed");
    if (status != DATA_BIND_OK) goto cleanup;

    if (output.data == NULL) {
        output.data = (char *)malloc(1u);
        if (output.data == NULL) {
            status = TurboRaftWireV3_schema_codec_error(
                error, DATA_BIND_ERR_OOM, type_name,
                "Unable to allocate generated XML output");
            goto cleanup;
        }
        output.data[0] = '\0';
    }
    *out = output.data;
    *out_len = output.length;
    output.data = NULL;
    status = TurboRaftWireV3_schema_codec_error(
        error, DATA_BIND_OK, NULL, NULL);

cleanup:
    if (writer.owner != NULL)
        (void)data_bind_xml_writer_close(&writer, error);
    free(output.data);
    data_bind_format_plan_free(format_plan);
    TurboRaftWireV3_message_workspace_close(&workspace);
    return status;
}

static DataBindStatus TurboRaftWireV3_native_conversion_unavailable(
    DataBindError *error, const char *type_name) {
    return TurboRaftWireV3_schema_codec_error(
        error, DATA_BIND_ERR_SCHEMA, type_name,
        "Canonical native conversion is unavailable for this shape");
}

#define DATABIND_DEFINE_UNAVAILABLE_BINARY(name) \
    DataBindStatus name##_from_bin(DataBind *codec, name##_t *object, const void *data, size_t len, DataBindError *error) { \
        (void)codec; (void)object; (void)data; (void)len; \
        return TurboRaftWireV3_schema_codec_error(error, DATA_BIND_ERR_SCHEMA, #name, \
            "Canonical Binary native binding is unavailable for this shape"); \
    } \
    DataBindStatus name##_to_bin(DataBind *codec, const name##_t *object, uint8_t **out, size_t *out_len, DataBindError *error) { \
        (void)codec; (void)object; if (out != NULL) *out = NULL; if (out_len != NULL) *out_len = 0u; \
        return TurboRaftWireV3_schema_codec_error(error, DATA_BIND_ERR_SCHEMA, #name, \
            "Canonical Binary native output is unavailable for this shape"); \
    } \
    DataBindStatus name##_to_bin_into(DataBind *codec, const name##_t *object, uint8_t *output, size_t output_capacity, size_t *out_len, DataBindError *error) { \
        (void)codec; (void)object; (void)output; (void)output_capacity; if (out_len != NULL) *out_len = 0u; \
        return TurboRaftWireV3_schema_codec_error(error, DATA_BIND_ERR_SCHEMA, #name, \
            "Canonical Binary native output is unavailable for this shape"); \
    }

#define DATABIND_DEFINE_CANONICAL_CSTL_RECORD(name) \
    DATABIND_DEFINE_CMETA_MESSAGE_LIFECYCLE(name) \
    DATABIND_DEFINE_UNAVAILABLE_CSTL_CONVERSIONS(name)

#define DATABIND_DEFINE_UNAVAILABLE_CSTL_CONVERSIONS(name) \
    DataBindStatus name##_from_bin(DataBind *codec, name##_t *object, const void *data, size_t len, DataBindError *error) { \
        (void)codec; (void)object; (void)data; (void)len; \
        return TurboRaftWireV3_native_conversion_unavailable(error, #name); \
    } \
    DataBindStatus name##_from_json(DataBind *codec, name##_t *object, const char *data, size_t len, DataBindError *error) { \
        (void)codec; (void)object; (void)data; (void)len; \
        return TurboRaftWireV3_native_conversion_unavailable(error, #name); \
    } \
    DataBindStatus name##_from_yaml(DataBind *codec, name##_t *object, const char *data, size_t len, DataBindError *error) { \
        (void)codec; (void)object; (void)data; (void)len; \
        return TurboRaftWireV3_native_conversion_unavailable(error, #name); \
    } \
    DataBindStatus name##_from_csv(DataBind *codec, name##_t *object, const char *data, size_t len, size_t row, DataBindError *error) { \
        (void)codec; (void)object; (void)data; (void)len; (void)row; \
        return TurboRaftWireV3_native_conversion_unavailable(error, #name); \
    } \
    DataBindStatus name##_from_xml(DataBind *codec, name##_t *object, const char *data, size_t len, DataBindError *error) { \
        (void)codec; (void)object; (void)data; (void)len; \
        return TurboRaftWireV3_native_conversion_unavailable(error, #name); \
    } \
    DataBindStatus name##_to_bin(DataBind *codec, const name##_t *object, uint8_t **out, size_t *out_len, DataBindError *error) { \
        (void)codec; (void)object; if (out != NULL) *out = NULL; if (out_len != NULL) *out_len = 0u; \
        return TurboRaftWireV3_native_conversion_unavailable(error, #name); \
    } \
    DataBindStatus name##_to_bin_into(DataBind *codec, const name##_t *object, uint8_t *output, size_t output_capacity, size_t *out_len, DataBindError *error) { \
        (void)codec; (void)object; (void)output; (void)output_capacity; if (out_len != NULL) *out_len = 0u; \
        return TurboRaftWireV3_native_conversion_unavailable(error, #name); \
    } \
    DataBindStatus name##_to_json(DataBind *codec, const name##_t *object, char **out, size_t *out_len, DataBindError *error) { \
        (void)codec; (void)object; if (out != NULL) *out = NULL; if (out_len != NULL) *out_len = 0u; \
        return TurboRaftWireV3_native_conversion_unavailable(error, #name); \
    } \
    DataBindStatus name##_to_yaml(DataBind *codec, const name##_t *object, char **out, size_t *out_len, DataBindError *error) { \
        (void)codec; (void)object; if (out != NULL) *out = NULL; if (out_len != NULL) *out_len = 0u; \
        return TurboRaftWireV3_native_conversion_unavailable(error, #name); \
    } \
    DataBindStatus name##_to_csv(DataBind *codec, const name##_t *object, char **out, size_t *out_len, DataBindError *error) { \
        (void)codec; (void)object; if (out != NULL) *out = NULL; if (out_len != NULL) *out_len = 0u; \
        return TurboRaftWireV3_native_conversion_unavailable(error, #name); \
    } \
    DataBindStatus name##_to_xml(DataBind *codec, const name##_t *object, char **out, size_t *out_len, DataBindError *error) { \
        (void)codec; (void)object; if (out != NULL) *out = NULL; if (out_len != NULL) *out_len = 0u; \
        return TurboRaftWireV3_native_conversion_unavailable(error, #name); \
    }

#define DATABIND_DEFINE_UNAVAILABLE_RAW_CONVERSIONS(name) \
    DATABIND_DEFINE_UNAVAILABLE_BINARY(name) \
    DataBindStatus name##_from_json(DataBind *codec, name##_t *object, const char *data, size_t len, DataBindError *error) { \
        (void)codec; (void)object; (void)data; (void)len; \
        return TurboRaftWireV3_schema_codec_error(error, DATA_BIND_ERR_SCHEMA, #name, \
            "Canonical JSON native binding is unavailable for this record shape"); \
    } \
    DataBindStatus name##_from_yaml(DataBind *codec, name##_t *object, const char *data, size_t len, DataBindError *error) { \
        (void)codec; (void)object; (void)data; (void)len; \
        return TurboRaftWireV3_schema_codec_error(error, DATA_BIND_ERR_SCHEMA, #name, \
            "Canonical YAML native binding is unavailable for this record shape"); \
    } \
    DataBindStatus name##_from_csv(DataBind *codec, name##_t *object, const char *data, size_t len, size_t row, DataBindError *error) { \
        (void)codec; (void)object; (void)data; (void)len; (void)row; \
        return TurboRaftWireV3_schema_codec_error(error, DATA_BIND_ERR_SCHEMA, #name, \
            "Canonical CSV native binding is unavailable for this record shape"); \
    } \
    DataBindStatus name##_from_xml(DataBind *codec, name##_t *object, const char *data, size_t len, DataBindError *error) { \
        (void)codec; (void)object; (void)data; (void)len; \
        return TurboRaftWireV3_schema_codec_error(error, DATA_BIND_ERR_SCHEMA, #name, \
            "Canonical XML native binding is unavailable for this record shape"); \
    } \
    DataBindStatus name##_to_json(DataBind *codec, const name##_t *object, char **out, size_t *out_len, DataBindError *error) { \
        (void)codec; (void)object; if (out != NULL) *out = NULL; if (out_len != NULL) *out_len = 0u; \
        return TurboRaftWireV3_schema_codec_error(error, DATA_BIND_ERR_SCHEMA, #name, \
            "Canonical JSON native output is unavailable for this record shape"); \
    } \
    DataBindStatus name##_to_yaml(DataBind *codec, const name##_t *object, char **out, size_t *out_len, DataBindError *error) { \
        (void)codec; (void)object; if (out != NULL) *out = NULL; if (out_len != NULL) *out_len = 0u; \
        return TurboRaftWireV3_schema_codec_error(error, DATA_BIND_ERR_SCHEMA, #name, \
            "Canonical YAML native output is unavailable for this record shape"); \
    } \
    DataBindStatus name##_to_csv(DataBind *codec, const name##_t *object, char **out, size_t *out_len, DataBindError *error) { \
        (void)codec; (void)object; if (out != NULL) *out = NULL; if (out_len != NULL) *out_len = 0u; \
        return TurboRaftWireV3_schema_codec_error(error, DATA_BIND_ERR_SCHEMA, #name, \
            "Canonical CSV native output is unavailable for this record shape"); \
    } \
    DataBindStatus name##_to_xml(DataBind *codec, const name##_t *object, char **out, size_t *out_len, DataBindError *error) { \
        (void)codec; (void)object; if (out != NULL) *out = NULL; if (out_len != NULL) *out_len = 0u; \
        return TurboRaftWireV3_schema_codec_error(error, DATA_BIND_ERR_SCHEMA, #name, \
            "Canonical XML native output is unavailable for this record shape"); \
    }

#define DATABIND_DEFINE_CMETA_RAW_RECORD(name) \
    DATABIND_DEFINE_CMETA_MESSAGE_LIFECYCLE(name) \
    DATABIND_DEFINE_UNAVAILABLE_RAW_CONVERSIONS(name)



#define DATABIND_DEFINE_LOCAL_OVERLAY_RAW_RECORD(name) \
    DATABIND_DEFINE_CMETA_MESSAGE_LIFECYCLE(name) \
    DATABIND_DEFINE_UNAVAILABLE_RAW_CONVERSIONS(name)

/* Void lifecycle APIs admit zero construction/release without recoverable
 * failures. A broken provider is an invariant violation, never a reason to
 * replace an unfinished owner with raw zero bytes. */
#define DATABIND_DEFINE_CMETA_MESSAGE_LIFECYCLE(name) \
    void name##_init(name##_t *object) { \
        TurboRaftWireV3_CMETA_INIT_ALL(); \
        if (object != NULL) { \
            memset(object, 0, sizeof(*object)); \
            if (cmeta_data_value_init_zero(&name##_CMETA_DATA, object) != CMETA_OK) \
                abort(); \
        } \
    } \
    void name##_clear(name##_t *object) { \
        TurboRaftWireV3_CMETA_INIT_ALL(); \
        if (object != NULL) { \
            if (cmeta_data_value_restore_zero(&name##_CMETA_DATA, object) != CMETA_OK) \
                abort(); \
            memset(object, 0, sizeof(*object)); \
        } \
    }

#define DATABIND_DEFINE_CANONICAL_MESSAGE_TEXT(name, depth_, nodes_) \
    DataBindStatus name##_from_json(DataBind *codec, name##_t *object, const char *data, size_t len, DataBindError *error) { \
        TurboRaftWireV3_CMETA_INIT_ALL(); \
        return TurboRaftWireV3_message_from_text( \
            codec, name##_native_artifact(), \
            data_bind_builtin_format_provider(DATA_BIND_FORMAT_JSON), (size_t)(depth_), (size_t)(nodes_), 0u, \
            object, sizeof(*object), data, len, error); \
    } \
    DataBindStatus name##_from_yaml(DataBind *codec, name##_t *object, const char *data, size_t len, DataBindError *error) { \
        TurboRaftWireV3_CMETA_INIT_ALL(); \
        return TurboRaftWireV3_message_from_text( \
            codec, name##_native_artifact(), \
            data_bind_builtin_format_provider(DATA_BIND_FORMAT_YAML), (size_t)(depth_), (size_t)(nodes_), 0u, \
            object, sizeof(*object), data, len, error); \
    } \
    DataBindStatus name##_to_json(DataBind *codec, const name##_t *object, char **out, size_t *out_len, DataBindError *error) { \
        TurboRaftWireV3_CMETA_INIT_ALL(); \
        return TurboRaftWireV3_message_to_text( \
            codec, name##_native_artifact(), \
            DATA_BIND_FORMAT_JSON, (size_t)(depth_), (size_t)(nodes_), \
            object, sizeof(*object), out, out_len, error); \
    } \
    DataBindStatus name##_to_yaml(DataBind *codec, const name##_t *object, char **out, size_t *out_len, DataBindError *error) { \
        TurboRaftWireV3_CMETA_INIT_ALL(); \
        return TurboRaftWireV3_message_to_text( \
            codec, name##_native_artifact(), \
            DATA_BIND_FORMAT_YAML, (size_t)(depth_), (size_t)(nodes_), \
            object, sizeof(*object), out, out_len, error); \
    }

#define DATABIND_DEFINE_CANONICAL_MESSAGE_XML(name, depth_, nodes_) \
    DataBindStatus name##_from_xml(DataBind *codec, name##_t *object, const char *data, size_t len, DataBindError *error) { \
        TurboRaftWireV3_CMETA_INIT_ALL(); \
        return TurboRaftWireV3_message_from_text( \
            codec, name##_native_artifact(), \
            data_bind_builtin_format_provider(DATA_BIND_FORMAT_XML), (size_t)(depth_), (size_t)(nodes_), 0u, \
            object, sizeof(*object), data, len, error); \
    }

#define DATABIND_DEFINE_CANONICAL_MESSAGE_XML_OUTPUT(name, depth_, nodes_) \
    DataBindStatus name##_to_xml(DataBind *codec, const name##_t *object, char **out, size_t *out_len, DataBindError *error) { \
        TurboRaftWireV3_CMETA_INIT_ALL(); \
        return TurboRaftWireV3_message_to_xml( \
            codec, name##_native_artifact(), \
            (size_t)(depth_), (size_t)(nodes_), object, sizeof(*object), \
            out, out_len, error); \
    }

#define DATABIND_DEFINE_RAW_MESSAGE_XML_OUTPUT(name) \
    DataBindStatus name##_to_xml(DataBind *codec, const name##_t *object, char **out, size_t *out_len, DataBindError *error) { \
        (void)codec; (void)object; if (out != NULL) *out = NULL; if (out_len != NULL) *out_len = 0u; \
        return TurboRaftWireV3_schema_codec_error(error, DATA_BIND_ERR_SCHEMA, #name, \
            "Canonical XML native output is unavailable for this shape"); \
    }

#define DATABIND_DEFINE_CSTL_MESSAGE_XML_OUTPUT(name) \
    DataBindStatus name##_to_xml(DataBind *codec, const name##_t *object, char **out, size_t *out_len, DataBindError *error) { \
        (void)codec; (void)object; if (out != NULL) *out = NULL; if (out_len != NULL) *out_len = 0u; \
        return TurboRaftWireV3_native_conversion_unavailable(error, #name); \
    }

#define DATABIND_DEFINE_CANONICAL_MESSAGE_CSV(name, depth_, nodes_) \
    DataBindStatus name##_from_csv(DataBind *codec, name##_t *object, const char *data, size_t len, size_t row, DataBindError *error) { \
        TurboRaftWireV3_CMETA_INIT_ALL(); \
        return TurboRaftWireV3_message_from_text( \
            codec, name##_native_artifact(), \
            data_bind_builtin_format_provider(DATA_BIND_FORMAT_CSV), (size_t)(depth_), (size_t)(nodes_), row, \
            object, sizeof(*object), data, len, error); \
    }

#define DATABIND_DEFINE_RAW_MESSAGE_CSV(name) \
    DataBindStatus name##_from_csv(DataBind *codec, name##_t *object, const char *data, size_t len, size_t row, DataBindError *error) { \
        (void)codec; (void)object; (void)data; (void)len; (void)row; \
        return TurboRaftWireV3_schema_codec_error(error, DATA_BIND_ERR_SCHEMA, #name, \
            "Canonical CSV native input is unavailable for this shape"); \
    }

#define DATABIND_DEFINE_CSTL_MESSAGE_CSV(name) \
    DataBindStatus name##_from_csv(DataBind *codec, name##_t *object, const char *data, size_t len, size_t row, DataBindError *error) { \
        (void)codec; (void)object; (void)data; (void)len; (void)row; \
        return TurboRaftWireV3_native_conversion_unavailable(error, #name); \
    }

#define DATABIND_DEFINE_RAW_MESSAGE_XML(name) \
    DataBindStatus name##_from_xml(DataBind *codec, name##_t *object, const char *data, size_t len, DataBindError *error) { \
        (void)codec; (void)object; (void)data; (void)len; \
        return TurboRaftWireV3_schema_codec_error(error, DATA_BIND_ERR_SCHEMA, #name, \
            "Canonical XML native input is unavailable for this shape"); \
    }

#define DATABIND_DEFINE_RAW_MESSAGE_TEXT(name) \
    DataBindStatus name##_from_json(DataBind *codec, name##_t *object, const char *data, size_t len, DataBindError *error) { \
        (void)codec; (void)object; (void)data; (void)len; \
        return TurboRaftWireV3_schema_codec_error(error, DATA_BIND_ERR_SCHEMA, #name, \
            "Canonical JSON native binding is unavailable for this shape"); \
    } \
    DataBindStatus name##_from_yaml(DataBind *codec, name##_t *object, const char *data, size_t len, DataBindError *error) { \
        (void)codec; (void)object; (void)data; (void)len; \
        return TurboRaftWireV3_schema_codec_error(error, DATA_BIND_ERR_SCHEMA, #name, \
            "Canonical YAML native binding is unavailable for this shape"); \
    } \
    DataBindStatus name##_to_json(DataBind *codec, const name##_t *object, char **out, size_t *out_len, DataBindError *error) { \
        (void)codec; (void)object; if (out != NULL) *out = NULL; if (out_len != NULL) *out_len = 0u; \
        return TurboRaftWireV3_schema_codec_error(error, DATA_BIND_ERR_SCHEMA, #name, \
            "Canonical JSON native output is unavailable for this shape"); \
    }

#define DATABIND_DEFINE_CSTL_MESSAGE_XML(name) \
    DataBindStatus name##_from_xml(DataBind *codec, name##_t *object, const char *data, size_t len, DataBindError *error) { \
        (void)codec; (void)object; (void)data; (void)len; \
        return TurboRaftWireV3_native_conversion_unavailable(error, #name); \
    }

#define DATABIND_DEFINE_CANONICAL_MESSAGE_BINARY(name, depth_, nodes_) \
    DataBindStatus name##_from_bin(DataBind *codec, name##_t *object, const void *data, size_t len, DataBindError *error) { \
        TurboRaftWireV3_CMETA_INIT_ALL(); \
        return TurboRaftWireV3_message_from_text( \
            codec, name##_native_artifact(), \
            name##_binary_##name##_databind_binary_provider(), (size_t)(depth_), (size_t)(nodes_), 0u, \
            object, sizeof(*object), (const char *)data, len, error); \
    } \
    DataBindStatus name##_to_bin(DataBind *codec, const name##_t *object, uint8_t **out, size_t *out_len, DataBindError *error) { \
        TurboRaftWireV3_CMETA_INIT_ALL(); \
        return TurboRaftWireV3_message_to_binary( \
            codec, name##_native_artifact(), \
            name##_binary_##name##_databind_binary_provider(), (size_t)(depth_), (size_t)(nodes_), \
            object, sizeof(*object), out, NULL, 0u, out_len, error); \
    } \
    DataBindStatus name##_to_bin_into(DataBind *codec, const name##_t *object, uint8_t *output, size_t output_capacity, size_t *out_len, DataBindError *error) { \
        TurboRaftWireV3_CMETA_INIT_ALL(); \
        return TurboRaftWireV3_message_to_binary( \
            codec, name##_native_artifact(), \
            name##_binary_##name##_databind_binary_provider(), (size_t)(depth_), (size_t)(nodes_), \
            object, sizeof(*object), NULL, output, output_capacity, out_len, error); \
    }

#define DATABIND_DEFINE_RAW_MESSAGE_YAML(name) \
    DataBindStatus name##_to_yaml(DataBind *codec, const name##_t *object, char **out, size_t *out_len, DataBindError *error) { \
        (void)codec; (void)object; if (out != NULL) *out = NULL; if (out_len != NULL) *out_len = 0u; \
        return TurboRaftWireV3_schema_codec_error(error, DATA_BIND_ERR_SCHEMA, #name, \
            "Canonical YAML native output is unavailable for this shape"); \
    }

#define DATABIND_DEFINE_RAW_MESSAGE_REMAINDER(name) \
    DATABIND_DEFINE_UNAVAILABLE_BINARY(name) \
    DataBindStatus name##_to_csv(DataBind *codec, const name##_t *object, char **out, size_t *out_len, DataBindError *error) { \
        (void)codec; (void)object; if (out != NULL) *out = NULL; if (out_len != NULL) *out_len = 0u; \
        return TurboRaftWireV3_schema_codec_error(error, DATA_BIND_ERR_SCHEMA, #name, \
            "Canonical CSV native output is unavailable for this shape"); \
    }

#define DATABIND_DEFINE_UNAVAILABLE_MESSAGE_CSV_OUTPUT(name) \
    DataBindStatus name##_to_csv(DataBind *codec, const name##_t *object, char **out, size_t *out_len, DataBindError *error) { \
        (void)codec; (void)object; if (out != NULL) *out = NULL; if (out_len != NULL) *out_len = 0u; \
        return TurboRaftWireV3_schema_codec_error(error, DATA_BIND_ERR_SCHEMA, #name, \
            "Canonical CSV native output is unavailable for this shape"); \
    }

#define DATABIND_DEFINE_CSTL_MESSAGE_REMAINDER(name) \
    DataBindStatus name##_from_bin(DataBind *codec, name##_t *object, const void *data, size_t len, DataBindError *error) { \
        (void)codec; (void)object; (void)data; (void)len; \
        return TurboRaftWireV3_native_conversion_unavailable(error, #name); \
    } \
    DataBindStatus name##_to_bin(DataBind *codec, const name##_t *object, uint8_t **out, size_t *out_len, DataBindError *error) { \
        (void)codec; (void)object; if (out != NULL) *out = NULL; if (out_len != NULL) *out_len = 0u; \
        return TurboRaftWireV3_native_conversion_unavailable(error, #name); \
    } \
    DataBindStatus name##_to_bin_into(DataBind *codec, const name##_t *object, uint8_t *output, size_t output_capacity, size_t *out_len, DataBindError *error) { \
        (void)codec; (void)object; (void)output; (void)output_capacity; if (out_len != NULL) *out_len = 0u; \
        return TurboRaftWireV3_native_conversion_unavailable(error, #name); \
    } \
    DataBindStatus name##_to_csv(DataBind *codec, const name##_t *object, char **out, size_t *out_len, DataBindError *error) { \
        (void)codec; (void)object; if (out != NULL) *out = NULL; if (out_len != NULL) *out_len = 0u; \
        return TurboRaftWireV3_native_conversion_unavailable(error, #name); \
    }

DATABIND_DEFINE_CMETA_MESSAGE_LIFECYCLE(RaftWireMessageV3)
DATABIND_DEFINE_CANONICAL_MESSAGE_TEXT(RaftWireMessageV3, 2, 48)
DATABIND_DEFINE_CANONICAL_MESSAGE_XML(RaftWireMessageV3, 2, 48)

DATABIND_DEFINE_CANONICAL_MESSAGE_CSV(RaftWireMessageV3, 2, 48)
DATABIND_DEFINE_UNAVAILABLE_MESSAGE_CSV_OUTPUT(RaftWireMessageV3)
DATABIND_DEFINE_CANONICAL_MESSAGE_BINARY(RaftWireMessageV3, 2, 48)

DATABIND_DEFINE_RAW_MESSAGE_XML_OUTPUT(RaftWireMessageV3)


static DataBindStatus TurboRaftWireV3_schema_codec_error(DataBindError *error,
                                                                DataBindStatus status,
                                                                const char *path,
                                                                const char *message) {
    if (error != NULL && error->size >= sizeof(error->size)) {
        if (error->size >= offsetof(DataBindError, code) + sizeof(error->code))
            error->code = status;
        if (error->size >= offsetof(DataBindError, line) + sizeof(error->line))
            error->line = -1;
        if (error->size >= offsetof(DataBindError, column) + sizeof(error->column))
            error->column = -1;
        if (error->size >= offsetof(DataBindError, path) + sizeof(error->path))
            snprintf(error->path, sizeof(error->path), "%s", path != NULL ? path : "");
        if (error->size >= offsetof(DataBindError, message) + sizeof(error->message))
            snprintf(error->message, sizeof(error->message), "%s",
                     message != NULL ? message : "");
    }
    return status;
}

static DataBindStatus TurboRaftWireV3_text_to_binary_into(
    DataBind *codec, const char *type_name, uint32_t format, const void *input,
    size_t input_len, size_t csv_row, uint8_t *output, size_t output_capacity,
    size_t *out_len, DataBindError *error) {
    if (out_len != NULL) *out_len = 0;
    if (codec == NULL || type_name == NULL || input == NULL || output == NULL || out_len == NULL)
        return TurboRaftWireV3_schema_codec_error(
            error, DATA_BIND_ERR_INVALID_ARG, type_name,
            "Invalid schema codec text-to-binary arguments");

#define TBE_SCHEMA_TEXT_TO_BINARY_CASE(name) \
    if (strcmp(type_name, #name) == 0) { \
        name##_t object; \
        DataBindStatus status; \
        name##_init(&object); \
        switch (format) { \
        case TBE_SCHEMA_FORMAT_JSON: \
            status = name##_from_json(codec, &object, (const char *)input, input_len, error); \
            break; \
        case TBE_SCHEMA_FORMAT_YAML: \
            status = name##_from_yaml(codec, &object, (const char *)input, input_len, error); \
            break; \
        case TBE_SCHEMA_FORMAT_CSV: \
            status = name##_from_csv(codec, &object, (const char *)input, input_len, csv_row, error); \
            break; \
        case TBE_SCHEMA_FORMAT_XML: \
            status = name##_from_xml(codec, &object, (const char *)input, input_len, error); \
            break; \
        default: \
            status = TurboRaftWireV3_schema_codec_error( \
                error, DATA_BIND_ERR_INVALID_ARG, type_name, "Unknown schema text format"); \
            break; \
        } \
        if (status == DATA_BIND_OK) status = name##_to_bin_into(codec, &object, output, output_capacity, out_len, error); \
        name##_clear(&object); \
        return status; \
    }

    TBE_SCHEMA_TEXT_TO_BINARY_CASE(RaftWireMessageV3)
#undef TBE_SCHEMA_TEXT_TO_BINARY_CASE

    return TurboRaftWireV3_schema_codec_error(
        error, DATA_BIND_ERR_TYPE_NOT_FOUND, type_name, "Schema codec type not found");
}

static DataBindStatus TurboRaftWireV3_binary_to_text(
    DataBind *codec, const char *type_name, uint32_t format, const void *input,
    size_t input_len, char **out, size_t *out_len, DataBindError *error) {
    if (out != NULL) *out = NULL;
    if (out_len != NULL) *out_len = 0;
    if (codec == NULL || type_name == NULL || input == NULL || input_len == 0 ||
        out == NULL || out_len == NULL)
        return TurboRaftWireV3_schema_codec_error(
            error, DATA_BIND_ERR_INVALID_ARG, type_name,
            "Invalid schema codec binary-to-text arguments");

#define TBE_SCHEMA_BINARY_TO_TEXT_CASE(name) \
    if (strcmp(type_name, #name) == 0) { \
        name##_t object; \
        DataBindStatus status; \
        name##_init(&object); \
        status = name##_from_bin(codec, &object, input, input_len, error); \
        if (status == DATA_BIND_OK) { \
            switch (format) { \
            case TBE_SCHEMA_FORMAT_JSON: \
                status = name##_to_json(codec, &object, out, out_len, error); \
                break; \
            case TBE_SCHEMA_FORMAT_YAML: \
                status = name##_to_yaml(codec, &object, out, out_len, error); \
                break; \
            case TBE_SCHEMA_FORMAT_CSV: \
                status = name##_to_csv(codec, &object, out, out_len, error); \
                break; \
            case TBE_SCHEMA_FORMAT_XML: \
                status = name##_to_xml(codec, &object, out, out_len, error); \
                break; \
            default: \
                status = TurboRaftWireV3_schema_codec_error( \
                    error, DATA_BIND_ERR_INVALID_ARG, type_name, "Unknown schema text format"); \
                break; \
            } \
        } \
        name##_clear(&object); \
        return status; \
    }

    TBE_SCHEMA_BINARY_TO_TEXT_CASE(RaftWireMessageV3)
#undef TBE_SCHEMA_BINARY_TO_TEXT_CASE

    return TurboRaftWireV3_schema_codec_error(
        error, DATA_BIND_ERR_TYPE_NOT_FOUND, type_name, "Schema codec type not found");
}

static void TurboRaftWireV3_schema_codec_free_output(void *output) {
    free(output);
}

static const tbe_schema_codec_v1_t TurboRaftWireV3_SCHEMA_CODEC = {
    .struct_size = sizeof(tbe_schema_codec_v1_t),
    .abi_version = TBE_SCHEMA_CODEC_ABI_VERSION,
    .schema_id = "TurboRaftWireV3",
    .create = TurboRaftWireV3_codec_create,
    .text_to_binary_into = TurboRaftWireV3_text_to_binary_into,
    .binary_to_text = TurboRaftWireV3_binary_to_text,
    .free_output = TurboRaftWireV3_schema_codec_free_output
};

const tbe_schema_codec_v1_t *TurboRaftWireV3_schema_codec(void) {
    return &TurboRaftWireV3_SCHEMA_CODEC;
}

#ifndef DATABIND_GENERATED_RaftWireMessageV3_binary_RaftWireMessageV3_READER_INCLUDED
#define DATABIND_GENERATED_RaftWireMessageV3_binary_RaftWireMessageV3_READER_INCLUDED

#include <data_bind_binary_reader.h>
#include <data_bind_binary_writer.h>
#include <data_bind_format_provider.h>

static const DataBindBinaryFieldPlan RaftWireMessageV3_binary_RaftWireMessageV3_fields[] = {
  {sizeof(DataBindBinaryFieldPlan), "message_type", CSERDE_UINT, 8u, 0u, 1u, 0u, 0u, 0u, DATA_BIND_BINARY_REP_FIXED, 0u},
  {sizeof(DataBindBinaryFieldPlan), "granted", CSERDE_UINT, 8u, 1u, 1u, 0u, 0u, 0u, DATA_BIND_BINARY_REP_FIXED, 0u},
  {sizeof(DataBindBinaryFieldPlan), "reserved", CSERDE_UINT, 16u, 2u, 2u, 0u, 0u, 0u, DATA_BIND_BINARY_REP_FIXED, 0u},
  {sizeof(DataBindBinaryFieldPlan), "entry_count", CSERDE_UINT, 32u, 4u, 4u, 0u, 0u, 0u, DATA_BIND_BINARY_REP_FIXED, 0u},
  {sizeof(DataBindBinaryFieldPlan), "from_node", CSERDE_UINT, 64u, 8u, 8u, 0u, 0u, 0u, DATA_BIND_BINARY_REP_FIXED, 0u},
  {sizeof(DataBindBinaryFieldPlan), "to_node", CSERDE_UINT, 64u, 16u, 8u, 0u, 0u, 0u, DATA_BIND_BINARY_REP_FIXED, 0u},
  {sizeof(DataBindBinaryFieldPlan), "term", CSERDE_UINT, 64u, 24u, 8u, 0u, 0u, 0u, DATA_BIND_BINARY_REP_FIXED, 0u},
  {sizeof(DataBindBinaryFieldPlan), "campaign_term", CSERDE_UINT, 64u, 32u, 8u, 0u, 0u, 0u, DATA_BIND_BINARY_REP_FIXED, 0u},
  {sizeof(DataBindBinaryFieldPlan), "last_log_index", CSERDE_UINT, 64u, 40u, 8u, 0u, 0u, 0u, DATA_BIND_BINARY_REP_FIXED, 0u},
  {sizeof(DataBindBinaryFieldPlan), "last_log_term", CSERDE_UINT, 64u, 48u, 8u, 0u, 0u, 0u, DATA_BIND_BINARY_REP_FIXED, 0u},
  {sizeof(DataBindBinaryFieldPlan), "leader_commit", CSERDE_UINT, 64u, 56u, 8u, 0u, 0u, 0u, DATA_BIND_BINARY_REP_FIXED, 0u},
  {sizeof(DataBindBinaryFieldPlan), "previous_log_index", CSERDE_UINT, 64u, 64u, 8u, 0u, 0u, 0u, DATA_BIND_BINARY_REP_FIXED, 0u},
  {sizeof(DataBindBinaryFieldPlan), "previous_log_term", CSERDE_UINT, 64u, 72u, 8u, 0u, 0u, 0u, DATA_BIND_BINARY_REP_FIXED, 0u},
  {sizeof(DataBindBinaryFieldPlan), "match_index", CSERDE_UINT, 64u, 80u, 8u, 0u, 0u, 0u, DATA_BIND_BINARY_REP_FIXED, 0u},
  {sizeof(DataBindBinaryFieldPlan), "reject_hint", CSERDE_UINT, 64u, 88u, 8u, 0u, 0u, 0u, DATA_BIND_BINARY_REP_FIXED, 0u},
  {sizeof(DataBindBinaryFieldPlan), "entry1_index", CSERDE_UINT, 64u, 96u, 8u, 0u, 0u, 0u, DATA_BIND_BINARY_REP_FIXED, 0u},
  {sizeof(DataBindBinaryFieldPlan), "entry1_term", CSERDE_UINT, 64u, 104u, 8u, 0u, 0u, 0u, DATA_BIND_BINARY_REP_FIXED, 0u},
  {sizeof(DataBindBinaryFieldPlan), "entry1_command_id", CSERDE_UINT, 64u, 112u, 8u, 0u, 0u, 0u, DATA_BIND_BINARY_REP_FIXED, 0u},
  {sizeof(DataBindBinaryFieldPlan), "entry2_index", CSERDE_UINT, 64u, 120u, 8u, 0u, 0u, 0u, DATA_BIND_BINARY_REP_FIXED, 0u},
  {sizeof(DataBindBinaryFieldPlan), "entry2_term", CSERDE_UINT, 64u, 128u, 8u, 0u, 0u, 0u, DATA_BIND_BINARY_REP_FIXED, 0u},
  {sizeof(DataBindBinaryFieldPlan), "entry2_command_id", CSERDE_UINT, 64u, 136u, 8u, 0u, 0u, 0u, DATA_BIND_BINARY_REP_FIXED, 0u},
  {sizeof(DataBindBinaryFieldPlan), "entry3_index", CSERDE_UINT, 64u, 144u, 8u, 0u, 0u, 0u, DATA_BIND_BINARY_REP_FIXED, 0u},
  {sizeof(DataBindBinaryFieldPlan), "entry3_term", CSERDE_UINT, 64u, 152u, 8u, 0u, 0u, 0u, DATA_BIND_BINARY_REP_FIXED, 0u},
  {sizeof(DataBindBinaryFieldPlan), "entry3_command_id", CSERDE_UINT, 64u, 160u, 8u, 0u, 0u, 0u, DATA_BIND_BINARY_REP_FIXED, 0u},
  {sizeof(DataBindBinaryFieldPlan), "entry4_index", CSERDE_UINT, 64u, 168u, 8u, 0u, 0u, 0u, DATA_BIND_BINARY_REP_FIXED, 0u},
  {sizeof(DataBindBinaryFieldPlan), "entry4_term", CSERDE_UINT, 64u, 176u, 8u, 0u, 0u, 0u, DATA_BIND_BINARY_REP_FIXED, 0u},
  {sizeof(DataBindBinaryFieldPlan), "entry4_command_id", CSERDE_UINT, 64u, 184u, 8u, 0u, 0u, 0u, DATA_BIND_BINARY_REP_FIXED, 0u},
  {sizeof(DataBindBinaryFieldPlan), "entry5_index", CSERDE_UINT, 64u, 192u, 8u, 0u, 0u, 0u, DATA_BIND_BINARY_REP_FIXED, 0u},
  {sizeof(DataBindBinaryFieldPlan), "entry5_term", CSERDE_UINT, 64u, 200u, 8u, 0u, 0u, 0u, DATA_BIND_BINARY_REP_FIXED, 0u},
  {sizeof(DataBindBinaryFieldPlan), "entry5_command_id", CSERDE_UINT, 64u, 208u, 8u, 0u, 0u, 0u, DATA_BIND_BINARY_REP_FIXED, 0u},
  {sizeof(DataBindBinaryFieldPlan), "entry6_index", CSERDE_UINT, 64u, 216u, 8u, 0u, 0u, 0u, DATA_BIND_BINARY_REP_FIXED, 0u},
  {sizeof(DataBindBinaryFieldPlan), "entry6_term", CSERDE_UINT, 64u, 224u, 8u, 0u, 0u, 0u, DATA_BIND_BINARY_REP_FIXED, 0u},
  {sizeof(DataBindBinaryFieldPlan), "entry6_command_id", CSERDE_UINT, 64u, 232u, 8u, 0u, 0u, 0u, DATA_BIND_BINARY_REP_FIXED, 0u},
  {sizeof(DataBindBinaryFieldPlan), "entry7_index", CSERDE_UINT, 64u, 240u, 8u, 0u, 0u, 0u, DATA_BIND_BINARY_REP_FIXED, 0u},
  {sizeof(DataBindBinaryFieldPlan), "entry7_term", CSERDE_UINT, 64u, 248u, 8u, 0u, 0u, 0u, DATA_BIND_BINARY_REP_FIXED, 0u},
  {sizeof(DataBindBinaryFieldPlan), "entry7_command_id", CSERDE_UINT, 64u, 256u, 8u, 0u, 0u, 0u, DATA_BIND_BINARY_REP_FIXED, 0u},
  {sizeof(DataBindBinaryFieldPlan), "entry8_index", CSERDE_UINT, 64u, 264u, 8u, 0u, 0u, 0u, DATA_BIND_BINARY_REP_FIXED, 0u},
  {sizeof(DataBindBinaryFieldPlan), "entry8_term", CSERDE_UINT, 64u, 272u, 8u, 0u, 0u, 0u, DATA_BIND_BINARY_REP_FIXED, 0u},
  {sizeof(DataBindBinaryFieldPlan), "entry8_command_id", CSERDE_UINT, 64u, 280u, 8u, 0u, 0u, 0u, DATA_BIND_BINARY_REP_FIXED, 0u},
  {sizeof(DataBindBinaryFieldPlan), "entry1_data", CSERDE_BYTES, 0u, 0u, 0u, 0u, 0u, 0u, DATA_BIND_BINARY_REP_VAR_DATA, 4u},
  {sizeof(DataBindBinaryFieldPlan), "entry2_data", CSERDE_BYTES, 0u, 0u, 0u, 0u, 0u, 0u, DATA_BIND_BINARY_REP_VAR_DATA, 4u},
  {sizeof(DataBindBinaryFieldPlan), "entry3_data", CSERDE_BYTES, 0u, 0u, 0u, 0u, 0u, 0u, DATA_BIND_BINARY_REP_VAR_DATA, 4u},
  {sizeof(DataBindBinaryFieldPlan), "entry4_data", CSERDE_BYTES, 0u, 0u, 0u, 0u, 0u, 0u, DATA_BIND_BINARY_REP_VAR_DATA, 4u},
  {sizeof(DataBindBinaryFieldPlan), "entry5_data", CSERDE_BYTES, 0u, 0u, 0u, 0u, 0u, 0u, DATA_BIND_BINARY_REP_VAR_DATA, 4u},
  {sizeof(DataBindBinaryFieldPlan), "entry6_data", CSERDE_BYTES, 0u, 0u, 0u, 0u, 0u, 0u, DATA_BIND_BINARY_REP_VAR_DATA, 4u},
  {sizeof(DataBindBinaryFieldPlan), "entry7_data", CSERDE_BYTES, 0u, 0u, 0u, 0u, 0u, 0u, DATA_BIND_BINARY_REP_VAR_DATA, 4u},
  {sizeof(DataBindBinaryFieldPlan), "entry8_data", CSERDE_BYTES, 0u, 0u, 0u, 0u, 0u, 0u, DATA_BIND_BINARY_REP_VAR_DATA, 4u},
};

static const DataBindBinaryLayoutPlan RaftWireMessageV3_binary_RaftWireMessageV3_plan = {
  sizeof(DataBindBinaryLayoutPlan), DATA_BIND_BINARY_LAYOUT_PLAN_ABI_VERSION,
  "RaftWireMessageV3",
  0, 288u, 0u, 0u, 0u, 0u,
  RaftWireMessageV3_binary_RaftWireMessageV3_fields, 47u, NULL, NULL
};

static DataBindStatus RaftWireMessageV3_binary_RaftWireMessageV3_open(
    const char *data, size_t len, size_t max_depth,
    cserde_reader **out_reader, void **out_owner,
    DataBindError *error) {
  return data_bind_binary_reader_open(
      &RaftWireMessageV3_binary_RaftWireMessageV3_plan, data, len, max_depth,
      out_reader, out_owner, error);
}
static void RaftWireMessageV3_binary_RaftWireMessageV3_close(cserde_reader *reader, void *owner) {
  data_bind_binary_reader_close(reader, owner);
}
static DataBindStatus RaftWireMessageV3_binary_RaftWireMessageV3_writer_open(
    DataBindWriteFn write, void *write_user, size_t max_depth,
    cserde_writer **out_writer, void **out_owner,
    DataBindError *error) {
  return data_bind_binary_writer_open(
      &RaftWireMessageV3_binary_RaftWireMessageV3_plan, write, write_user, max_depth,
      out_writer, out_owner, error);
}
static DataBindStatus RaftWireMessageV3_binary_RaftWireMessageV3_writer_close(
    cserde_writer *writer, void *owner, DataBindError *error) {
  return data_bind_binary_writer_close(writer, owner, error);
}
static const DataBindFormatProvider RaftWireMessageV3_binary_RaftWireMessageV3_provider =
    DATA_BIND_FORMAT_PROVIDER_WITH_SELECTION_AND_WRITER_INIT(
        DATA_BIND_FORMAT_BINARY, RaftWireMessageV3_binary_RaftWireMessageV3_open, RaftWireMessageV3_binary_RaftWireMessageV3_close, NULL,
        RaftWireMessageV3_binary_RaftWireMessageV3_writer_open, RaftWireMessageV3_binary_RaftWireMessageV3_writer_close);

static inline const DataBindBinaryLayoutPlan *
RaftWireMessageV3_binary_RaftWireMessageV3_databind_binary_layout_plan(void) {
  return &RaftWireMessageV3_binary_RaftWireMessageV3_plan;
}
static inline const DataBindFormatProvider *
RaftWireMessageV3_binary_RaftWireMessageV3_databind_binary_provider(void) {
  return &RaftWireMessageV3_binary_RaftWireMessageV3_provider;
}

#endif /* DATABIND_GENERATED_RaftWireMessageV3_binary_RaftWireMessageV3_READER_INCLUDED */
