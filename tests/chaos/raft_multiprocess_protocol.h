#ifndef TURBORAFT_RAFT_MULTIPROCESS_PROTOCOL_H
#define TURBORAFT_RAFT_MULTIPROCESS_PROTOCOL_H

#include <stddef.h>
#include <stdint.h>

#define TR_CHAOS_PROTOCOL_VERSION 1U
#define TR_CHAOS_COMMAND_HEADER_SIZE 16U
#define TR_CHAOS_RESPONSE_HEADER_SIZE 96U
#define TR_CHAOS_MAX_FRAME_BYTES (64U * 1024U)
#define TR_CHAOS_MAX_RESPONSE_BYTES (512U * 1024U)
#define TR_CHAOS_MAX_QUEUED_FRAMES 512U

typedef enum tr_chaos_command_kind {
    TR_CHAOS_COMMAND_TICK = 1,
    TR_CHAOS_COMMAND_STEP = 2,
    TR_CHAOS_COMMAND_PROPOSE = 3,
    TR_CHAOS_COMMAND_STATUS = 4,
    TR_CHAOS_COMMAND_STOP = 5,
    TR_CHAOS_COMMAND_BACKUP_HANDOFF = 6
} tr_chaos_command_kind_t;

typedef enum tr_chaos_backup_handoff_mode {
    TR_CHAOS_BACKUP_HANDOFF_NORMAL = 0,
    TR_CHAOS_BACKUP_HANDOFF_FAIL_REOPEN = 1
} tr_chaos_backup_handoff_mode_t;

static const uint8_t tr_chaos_command_magic[4] = {'T', 'R', 'C', 'Q'};
static const uint8_t tr_chaos_response_magic[4] = {'T', 'R', 'C', 'R'};

static void tr_chaos_put_u16(uint8_t *output, uint16_t value)
{
    output[0] = (uint8_t) (value >> 8U);
    output[1] = (uint8_t) value;
}

static uint16_t tr_chaos_get_u16(const uint8_t *input)
{
    return (uint16_t) (((uint16_t) input[0] << 8U) |
                       (uint16_t) input[1]);
}

static void tr_chaos_put_u32(uint8_t *output, uint32_t value)
{
    output[0] = (uint8_t) (value >> 24U);
    output[1] = (uint8_t) (value >> 16U);
    output[2] = (uint8_t) (value >> 8U);
    output[3] = (uint8_t) value;
}

static uint32_t tr_chaos_get_u32(const uint8_t *input)
{
    return ((uint32_t) input[0] << 24U) |
           ((uint32_t) input[1] << 16U) |
           ((uint32_t) input[2] << 8U) |
           (uint32_t) input[3];
}

static void tr_chaos_put_u64(uint8_t *output, uint64_t value)
{
    size_t index;

    for (index = 0U; index < 8U; ++index) {
        output[index] = (uint8_t) (value >> (56U - index * 8U));
    }
}

static uint64_t tr_chaos_get_u64(const uint8_t *input)
{
    uint64_t value = 0U;
    size_t index;

    for (index = 0U; index < 8U; ++index) {
        value = (value << 8U) | input[index];
    }
    return value;
}

#endif
