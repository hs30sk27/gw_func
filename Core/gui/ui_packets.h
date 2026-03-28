/*
 * ui_packets.h
 *
 * LoRa 패킷 포맷(비콘/노드데이터) 정의 + encode/decode
 *
 * 기존 고정 길이 포맷은 그대로 수신(parse) 호환을 유지하고,
 * 송신(build)은 가능한 경우 가변 길이 compact 포맷으로 줄입니다.
 *
 * [BEACON legacy fixed]
 *  - NETID UI_NET_ID_LEN bytes (UTF-8 raw bytes)
 *  - TIME  6 bytes (YY,MM,DD,hh,mm,ss)
 *  - TEST  3 bytes ("01M" 같은 SETTING ASCII)
 *  - CRC16 2 bytes (CCITT)
 *
 * [BEACON compact]
 *  - MAGIC  1 byte  (0xF8)
 *  - NETLEN 1 byte  (0..UI_NET_ID_LEN)
 *  - NETID  NETLEN bytes (trailing zero 제거)
 *  - TIME   6 bytes (YY,MM,DD,hh,mm,ss)
 *  - SET    1 byte  (bit7: unit(H=1,M=0), bit6..0: value 0..99)
 *  - CRC16  2 bytes
 *
 * [NODE DATA legacy fixed]
 *  - NODE_NUM 1 byte
 *  - NETID    UI_NET_ID_LEN bytes (UTF-8 raw bytes)
 *  - BATT     uint8  (1=normal, 0=low)
 *  - TEMP     int8   ('C) range -50..100
 *  - BCN_CNT  uint16
 *  - X,Y,Z    uint16 each (0=unused/test fail, valid=1..50000)
 *  - ADC      uint16 (0..59999)
 *  - PULSE    uint32
 *  - SENSOR_EN 1 byte (bit0=ICM20948, bit1=ADC, bit2=PULSE)
 *  - CRC16    2 bytes
 *
 * [NODE DATA compact]
 *  - PREFIX   1 byte  (0xC0 | NODE_NUM[5:0], NODE_NUM 0..49)
 *  - HDR      1 byte  ([7:3]=NETLEN 0..24, [2:0]=SENSOR_EN mask)
 *  - NETID    NETLEN bytes (trailing zero 제거)
 *  - BATT     1 byte
 *  - TEMP     1 byte
 *  - BCN_CNT  2 bytes
 *  - XYZ      6 bytes (mask bit0 set일 때만)
 *  - ADC      2 bytes (mask bit1 set일 때만)
 *  - PULSE    4 bytes (mask bit2 set일 때만)
 *  - CRC16    2 bytes
 *
 * 참고:
 *  - UI_BEACON_PAYLOAD_LEN / UI_NODE_PAYLOAD_LEN 은 "최대 버퍼 크기"입니다.
 *  - 실제 build 결과 길이는 이보다 짧을 수 있습니다.
 */

#ifndef UI_PACKETS_H
#define UI_PACKETS_H

#include <stdint.h>
#include <stdbool.h>
#include "ui_types.h"
#include "ui_time.h"

#ifdef __cplusplus
extern "C" {
#endif

#define UI_BEACON_PAYLOAD_LEN   (UI_NET_ID_LEN + 11u) /* legacy/max */
#define UI_NODE_PAYLOAD_LEN     (UI_NET_ID_LEN + 20u) /* legacy/max */

#define UI_BEACON_COMPACT_MAGIC       (0xF8u)
#define UI_NODE_COMPACT_PREFIX_BASE   (0xC0u)
#define UI_NODE_COMPACT_PREFIX_MASK   (0xC0u)
#define UI_NODE_COMPACT_NODE_MASK     (0x3Fu)

#define UI_NODE_MEAS_UNUSED_U16      (0u)
#define UI_NODE_AXIS_VALID_MIN_U16   (1u)
#define UI_NODE_AXIS_VALID_MAX_U16   (50000u)
#define UI_NODE_AXIS_VALID_SPAN_U16  ((UI_NODE_AXIS_VALID_MAX_U16) - (UI_NODE_AXIS_VALID_MIN_U16))
#define UI_NODE_ADC_SCALED_MAX_U16   (59999u)
#define UI_NODE_AXIS_RAW_HALF_RANGE  (16384)
#define UI_NODE_ADC_RAW_MAX          (65535u)

/* 비콘 파싱 결과 */
typedef struct
{
    uint8_t net_id[UI_NET_ID_LEN];
    UI_DateTime_t dt;        /* centi는 사용하지 않음(전송에 포함 X) */
    uint8_t setting_ascii[3];
} UI_Beacon_t;

/* 노드 데이터 파싱/빌드용 */
typedef struct
{
    uint8_t  node_num;       /* 0..49, sync request legacy 특수값 0xAA 허용 */
    uint8_t  net_id[UI_NET_ID_LEN];

    uint8_t  batt_lvl;       /* 1=normal, 0=low, 0xFF=internal invalid */
    int8_t   temp_c;         /* -50..100'C, UI_NODE_TEMP_INVALID_C=internal invalid */

    uint16_t beacon_cnt;

    uint16_t x;
    uint16_t y;
    uint16_t z;

    uint16_t adc;

    uint32_t pulse_cnt;
    uint8_t  sensor_en_mask;

} UI_NodeData_t;

/* 빌드/파싱 */
uint8_t UI_Pkt_BuildBeacon(uint8_t out[UI_BEACON_PAYLOAD_LEN],
                           const uint8_t net_id[UI_NET_ID_LEN],
                           const UI_DateTime_t* dt_no_centi,
                           const uint8_t setting_ascii[3]);

bool UI_Pkt_ParseBeacon(const uint8_t* buf, uint16_t len, UI_Beacon_t* out);

uint8_t UI_Pkt_BuildNodeData(uint8_t out[UI_NODE_PAYLOAD_LEN],
                             const UI_NodeData_t* in);

bool UI_Pkt_ParseNodeData(const uint8_t* buf, uint16_t len, UI_NodeData_t* out);

#ifdef __cplusplus
}
#endif

#endif /* UI_PACKETS_H */
