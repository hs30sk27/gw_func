#include "ui_packets.h"
#include "ui_crc16.h"
#include <string.h>

static void prv_put_u16_le(uint8_t* p, uint16_t v)
{
    p[0] = (uint8_t)(v & 0xFFu);
    p[1] = (uint8_t)((v >> 8) & 0xFFu);
}

static uint16_t prv_get_u16_le(const uint8_t* p)
{
    return (uint16_t)((uint16_t)p[0] | ((uint16_t)p[1] << 8));
}

static void prv_put_u32_le(uint8_t* p, uint32_t v)
{
    p[0] = (uint8_t)(v & 0xFFu);
    p[1] = (uint8_t)((v >> 8) & 0xFFu);
    p[2] = (uint8_t)((v >> 16) & 0xFFu);
    p[3] = (uint8_t)((v >> 24) & 0xFFu);
}

static uint32_t prv_get_u32_le(const uint8_t* p)
{
    return (uint32_t)p[0]
         | ((uint32_t)p[1] << 8)
         | ((uint32_t)p[2] << 16)
         | ((uint32_t)p[3] << 24);
}

static uint8_t prv_trim_net_id_len(const uint8_t net_id[UI_NET_ID_LEN])
{
    uint8_t len = UI_NET_ID_LEN;

    while ((len > 0u) && (net_id[len - 1u] == 0u)) {
        len--;
    }
    return len;
}

static bool prv_is_ascii_digit(uint8_t ch)
{
    return ((ch >= (uint8_t)'0') && (ch <= (uint8_t)'9'));
}

static bool prv_pack_setting_ascii(const uint8_t setting_ascii[3], uint8_t* out_packed)
{
    uint8_t value;
    uint8_t unit;

    if ((setting_ascii == NULL) || (out_packed == NULL)) {
        return false;
    }
    if (!prv_is_ascii_digit(setting_ascii[0]) || !prv_is_ascii_digit(setting_ascii[1])) {
        return false;
    }

    value = (uint8_t)(((setting_ascii[0] - (uint8_t)'0') * 10u)
                    + (setting_ascii[1] - (uint8_t)'0'));
    unit = setting_ascii[2];

    if ((unit != (uint8_t)'M') && (unit != (uint8_t)'H')) {
        return false;
    }

    *out_packed = (uint8_t)(value | ((unit == (uint8_t)'H') ? 0x80u : 0u));
    return true;
}

static bool prv_unpack_setting_ascii(uint8_t packed, uint8_t setting_ascii[3])
{
    uint8_t value = (uint8_t)(packed & 0x7Fu);

    if (setting_ascii == NULL) {
        return false;
    }
    if (value > 99u) {
        return false;
    }

    setting_ascii[0] = (uint8_t)('0' + (value / 10u));
    setting_ascii[1] = (uint8_t)('0' + (value % 10u));
    setting_ascii[2] = ((packed & 0x80u) != 0u) ? (uint8_t)'H' : (uint8_t)'M';
    return true;
}

static bool prv_is_compact_node_prefix(uint8_t prefix)
{
    if ((prefix & UI_NODE_COMPACT_PREFIX_MASK) != UI_NODE_COMPACT_PREFIX_BASE) {
        return false;
    }
    return ((prefix & UI_NODE_COMPACT_NODE_MASK) < UI_MAX_NODES);
}

static uint8_t prv_build_legacy_beacon(uint8_t out[UI_BEACON_PAYLOAD_LEN],
                                       const uint8_t net_id[UI_NET_ID_LEN],
                                       const UI_DateTime_t* dt,
                                       const uint8_t setting_ascii[3])
{
    uint8_t* p = out;
    uint16_t crc;

    memcpy(p, net_id, UI_NET_ID_LEN); p += UI_NET_ID_LEN;

    p[0] = (uint8_t)(dt->year % 100u);
    p[1] = dt->month;
    p[2] = dt->day;
    p[3] = dt->hour;
    p[4] = dt->min;
    p[5] = dt->sec;
    p += 6;

    memcpy(p, setting_ascii, 3u); p += 3u;

    crc = UI_CRC16_CCITT(out, (size_t)(UI_NET_ID_LEN + 6u + 3u), UI_CRC16_INIT);
    prv_put_u16_le(p, crc);
    p += 2u;

    return (uint8_t)(p - out);
}

static bool prv_parse_legacy_beacon(const uint8_t* buf, uint16_t len, UI_Beacon_t* out)
{
    const uint8_t* p;
    uint16_t crc_rx;
    uint16_t crc;

    if ((buf == NULL) || (out == NULL)) {
        return false;
    }
    if (len < UI_BEACON_PAYLOAD_LEN) {
        return false;
    }

    crc_rx = prv_get_u16_le(&buf[UI_NET_ID_LEN + 6u + 3u]);
    crc = UI_CRC16_CCITT(buf, (size_t)(UI_NET_ID_LEN + 6u + 3u), UI_CRC16_INIT);
    if (crc != crc_rx) {
        return false;
    }

    memcpy(out->net_id, buf, UI_NET_ID_LEN);

    p = &buf[UI_NET_ID_LEN];
    out->dt.year  = (uint16_t)(2000u + (uint16_t)p[0]);
    out->dt.month = p[1];
    out->dt.day   = p[2];
    out->dt.hour  = p[3];
    out->dt.min   = p[4];
    out->dt.sec   = p[5];
    out->dt.centi = 0u;

    memcpy(out->setting_ascii, &buf[UI_NET_ID_LEN + 6u], 3u);
    return true;
}

static uint8_t prv_build_compact_beacon(uint8_t out[UI_BEACON_PAYLOAD_LEN],
                                        const uint8_t net_id[UI_NET_ID_LEN],
                                        const UI_DateTime_t* dt,
                                        const uint8_t setting_ascii[3])
{
    uint8_t* p = out;
    uint8_t net_len;
    uint8_t setting_packed;
    uint16_t crc;

    if (!prv_pack_setting_ascii(setting_ascii, &setting_packed)) {
        return 0u;
    }

    net_len = prv_trim_net_id_len(net_id);

    p[0] = UI_BEACON_COMPACT_MAGIC;
    p[1] = net_len;
    p += 2u;

    if (net_len > 0u) {
        memcpy(p, net_id, net_len);
        p += net_len;
    }

    p[0] = (uint8_t)(dt->year % 100u);
    p[1] = dt->month;
    p[2] = dt->day;
    p[3] = dt->hour;
    p[4] = dt->min;
    p[5] = dt->sec;
    p += 6u;

    p[0] = setting_packed;
    p += 1u;

    crc = UI_CRC16_CCITT(out, (size_t)(p - out), UI_CRC16_INIT);
    prv_put_u16_le(p, crc);
    p += 2u;

    return (uint8_t)(p - out);
}

static bool prv_parse_compact_beacon(const uint8_t* buf, uint16_t len, UI_Beacon_t* out)
{
    const uint8_t* p;
    uint8_t net_len;
    uint8_t setting_packed;
    uint16_t expected_len;
    uint16_t crc_rx;
    uint16_t crc;

    if ((buf == NULL) || (out == NULL)) {
        return false;
    }
    if (len < 11u) {
        return false;
    }
    if (buf[0] != UI_BEACON_COMPACT_MAGIC) {
        return false;
    }

    net_len = buf[1];
    if (net_len > UI_NET_ID_LEN) {
        return false;
    }

    expected_len = (uint16_t)(2u + (uint16_t)net_len + 6u + 1u + 2u);
    if (len < expected_len) {
        return false;
    }

    /* 일부 radio callback은 수신 size를 실제 payload 길이 대신 설정된 max length로
     * 보고할 수 있다. compact payload는 선두 expected_len 바이트만 검증하고
     * 뒤 trailing byte는 무시한다. */
    crc_rx = prv_get_u16_le(&buf[expected_len - 2u]);
    crc = UI_CRC16_CCITT(buf, (size_t)(expected_len - 2u), UI_CRC16_INIT);
    if (crc != crc_rx) {
        return false;
    }

    memset(out->net_id, 0, UI_NET_ID_LEN);
    p = &buf[2];
    if (net_len > 0u) {
        memcpy(out->net_id, p, net_len);
        p += net_len;
    }

    out->dt.year  = (uint16_t)(2000u + (uint16_t)p[0]);
    out->dt.month = p[1];
    out->dt.day   = p[2];
    out->dt.hour  = p[3];
    out->dt.min   = p[4];
    out->dt.sec   = p[5];
    out->dt.centi = 0u;
    p += 6u;

    setting_packed = p[0];
    return prv_unpack_setting_ascii(setting_packed, out->setting_ascii);
}

static uint8_t prv_build_legacy_node_data(uint8_t out[UI_NODE_PAYLOAD_LEN],
                                          const UI_NodeData_t* in)
{
    uint8_t* p = out;
    uint16_t crc;

    p[0] = in->node_num; p += 1u;
    memcpy(p, in->net_id, UI_NET_ID_LEN); p += UI_NET_ID_LEN;

    p[0] = in->batt_lvl; p += 1u;
    p[0] = (uint8_t)in->temp_c; p += 1u;
    prv_put_u16_le(p, in->beacon_cnt); p += 2u;

    prv_put_u16_le(p, in->x); p += 2u;
    prv_put_u16_le(p, in->y); p += 2u;
    prv_put_u16_le(p, in->z); p += 2u;

    prv_put_u16_le(p, in->adc); p += 2u;
    prv_put_u32_le(p, in->pulse_cnt); p += 4u;
    p[0] = in->sensor_en_mask; p += 1u;

    crc = UI_CRC16_CCITT(out, (size_t)(UI_NODE_PAYLOAD_LEN - 2u), UI_CRC16_INIT);
    prv_put_u16_le(p, crc);
    p += 2u;

    return (uint8_t)(p - out);
}

static bool prv_parse_legacy_node_data(const uint8_t* buf, uint16_t len, UI_NodeData_t* out)
{
    const uint8_t* p;
    uint16_t crc_rx;
    uint16_t crc;

    if ((buf == NULL) || (out == NULL)) {
        return false;
    }
    if (len < UI_NODE_PAYLOAD_LEN) {
        return false;
    }

    crc_rx = prv_get_u16_le(&buf[UI_NODE_PAYLOAD_LEN - 2u]);
    crc = UI_CRC16_CCITT(buf, (size_t)(UI_NODE_PAYLOAD_LEN - 2u), UI_CRC16_INIT);
    if (crc != crc_rx) {
        return false;
    }

    p = buf;
    out->node_num = p[0]; p += 1u;
    memcpy(out->net_id, p, UI_NET_ID_LEN); p += UI_NET_ID_LEN;

    out->batt_lvl = p[0]; p += 1u;
    out->temp_c = (int8_t)p[0]; p += 1u;
    out->beacon_cnt = prv_get_u16_le(p); p += 2u;

    out->x = prv_get_u16_le(p); p += 2u;
    out->y = prv_get_u16_le(p); p += 2u;
    out->z = prv_get_u16_le(p); p += 2u;

    out->adc = prv_get_u16_le(p); p += 2u;
    out->pulse_cnt = prv_get_u32_le(p); p += 4u;
    out->sensor_en_mask = p[0];
    return true;
}

static uint8_t prv_build_compact_node_data(uint8_t out[UI_NODE_PAYLOAD_LEN],
                                           const UI_NodeData_t* in)
{
    uint8_t* p = out;
    uint8_t net_len;
    uint8_t sensor_mask;
    uint16_t crc;

    if (in->node_num >= UI_MAX_NODES) {
        return 0u;
    }

    net_len = prv_trim_net_id_len(in->net_id);
    sensor_mask = (uint8_t)(in->sensor_en_mask & UI_SENSOR_EN_ALL);

    p[0] = (uint8_t)(UI_NODE_COMPACT_PREFIX_BASE | (in->node_num & UI_NODE_COMPACT_NODE_MASK));
    p[1] = (uint8_t)((uint8_t)(net_len << 3) | sensor_mask);
    p += 2u;

    if (net_len > 0u) {
        memcpy(p, in->net_id, net_len);
        p += net_len;
    }

    p[0] = in->batt_lvl; p += 1u;
    p[0] = (uint8_t)in->temp_c; p += 1u;
    prv_put_u16_le(p, in->beacon_cnt); p += 2u;

    if ((sensor_mask & UI_SENSOR_EN_ICM20948) != 0u) {
        prv_put_u16_le(p, in->x); p += 2u;
        prv_put_u16_le(p, in->y); p += 2u;
        prv_put_u16_le(p, in->z); p += 2u;
    }
    if ((sensor_mask & UI_SENSOR_EN_ADC) != 0u) {
        prv_put_u16_le(p, in->adc); p += 2u;
    }
    if ((sensor_mask & UI_SENSOR_EN_PULSE) != 0u) {
        prv_put_u32_le(p, in->pulse_cnt); p += 4u;
    }

    crc = UI_CRC16_CCITT(out, (size_t)(p - out), UI_CRC16_INIT);
    prv_put_u16_le(p, crc);
    p += 2u;

    return (uint8_t)(p - out);
}

static bool prv_parse_compact_node_data(const uint8_t* buf, uint16_t len, UI_NodeData_t* out)
{
    const uint8_t* p;
    uint8_t node_num;
    uint8_t hdr;
    uint8_t net_len;
    uint8_t sensor_mask;
    uint16_t expected_len;
    uint16_t crc_rx;
    uint16_t crc;

    if ((buf == NULL) || (out == NULL)) {
        return false;
    }
    if (len < 8u) {
        return false;
    }
    if (!prv_is_compact_node_prefix(buf[0])) {
        return false;
    }

    node_num = (uint8_t)(buf[0] & UI_NODE_COMPACT_NODE_MASK);
    hdr = buf[1];
    net_len = (uint8_t)(hdr >> 3);
    sensor_mask = (uint8_t)(hdr & UI_SENSOR_EN_ALL);

    if (net_len > UI_NET_ID_LEN) {
        return false;
    }

    expected_len = (uint16_t)(2u + (uint16_t)net_len + 1u + 1u + 2u + 2u);
    if ((sensor_mask & UI_SENSOR_EN_ICM20948) != 0u) {
        expected_len = (uint16_t)(expected_len + 6u);
    }
    if ((sensor_mask & UI_SENSOR_EN_ADC) != 0u) {
        expected_len = (uint16_t)(expected_len + 2u);
    }
    if ((sensor_mask & UI_SENSOR_EN_PULSE) != 0u) {
        expected_len = (uint16_t)(expected_len + 4u);
    }
    if (len < expected_len) {
        return false;
    }

    /* trailing padding/garbage size 보고는 무시하고 실제 compact payload 범위만 확인 */
    crc_rx = prv_get_u16_le(&buf[expected_len - 2u]);
    crc = UI_CRC16_CCITT(buf, (size_t)(expected_len - 2u), UI_CRC16_INIT);
    if (crc != crc_rx) {
        return false;
    }

    memset(out, 0, sizeof(*out));
    out->node_num = node_num;
    out->sensor_en_mask = sensor_mask;
    out->pulse_cnt = 0xFFFFFFFFu;

    p = &buf[2];
    if (net_len > 0u) {
        memcpy(out->net_id, p, net_len);
        p += net_len;
    }

    out->batt_lvl = p[0]; p += 1u;
    out->temp_c = (int8_t)p[0]; p += 1u;
    out->beacon_cnt = prv_get_u16_le(p); p += 2u;

    if ((sensor_mask & UI_SENSOR_EN_ICM20948) != 0u) {
        out->x = prv_get_u16_le(p); p += 2u;
        out->y = prv_get_u16_le(p); p += 2u;
        out->z = prv_get_u16_le(p); p += 2u;
    } else {
        out->x = UI_NODE_MEAS_UNUSED_U16;
        out->y = UI_NODE_MEAS_UNUSED_U16;
        out->z = UI_NODE_MEAS_UNUSED_U16;
    }

    if ((sensor_mask & UI_SENSOR_EN_ADC) != 0u) {
        out->adc = prv_get_u16_le(p); p += 2u;
    } else {
        out->adc = UI_NODE_MEAS_UNUSED_U16;
    }

    if ((sensor_mask & UI_SENSOR_EN_PULSE) != 0u) {
        out->pulse_cnt = prv_get_u32_le(p); p += 4u;
    }

    return true;
}

uint8_t UI_Pkt_BuildBeacon(uint8_t out[UI_BEACON_PAYLOAD_LEN],
                           const uint8_t net_id[UI_NET_ID_LEN],
                           const UI_DateTime_t* dt,
                           const uint8_t setting_ascii[3])
{
    uint8_t len;

    if ((out == NULL) || (net_id == NULL) || (dt == NULL) || (setting_ascii == NULL)) {
        return 0u;
    }

    /* 비콘은 전 노드 동기 호환성을 최우선으로 두고 legacy fixed 길이로 송신한다.
     * compact beacon은 수신(parse) 호환만 유지한다. */
    len = prv_build_legacy_beacon(out, net_id, dt, setting_ascii);
    if (len != 0u) {
        return len;
    }

    /* fallback: 이 경로는 정상적으로는 타지 않지만 compact builder 참조는 유지한다. */
    return prv_build_compact_beacon(out, net_id, dt, setting_ascii);
}

bool UI_Pkt_ParseBeacon(const uint8_t* buf, uint16_t len, UI_Beacon_t* out)
{
    if (prv_parse_compact_beacon(buf, len, out)) {
        return true;
    }
    return prv_parse_legacy_beacon(buf, len, out);
}

uint8_t UI_Pkt_BuildNodeData(uint8_t out[UI_NODE_PAYLOAD_LEN],
                             const UI_NodeData_t* in)
{
    uint8_t len;

    if ((out == NULL) || (in == NULL)) {
        return 0u;
    }

    /* sync request(0xAA) 등 legacy 특수 패킷은 기존 포맷 유지 */
    len = prv_build_compact_node_data(out, in);
    if (len != 0u) {
        return len;
    }

    return prv_build_legacy_node_data(out, in);
}

bool UI_Pkt_ParseNodeData(const uint8_t* buf, uint16_t len, UI_NodeData_t* out)
{
    if (prv_parse_compact_node_data(buf, len, out)) {
        return true;
    }
    return prv_parse_legacy_node_data(buf, len, out);
}
