/*
 * ui_crc16.h
 *
 * CRC16 (CCITT-FALSE) 유틸
 * - poly: 0x1021
 * - init: 0xFFFF
 * - xorout: 0x0000
 */

#ifndef UI_CRC16_H
#define UI_CRC16_H

#include <stdint.h>
#include <stddef.h>
#include "ui_conf.h"

#ifdef __cplusplus
extern "C" {
#endif

uint16_t UI_CRC16_CCITT(const uint8_t* data, size_t len, uint16_t init);

#ifdef __cplusplus
}
#endif

#endif /* UI_CRC16_H */
