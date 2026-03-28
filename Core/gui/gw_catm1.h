/*
 * gw_catm1.h
 *
 * SIM7080(CATM1) minimal helper
 *
 * - 1NCE APN(iot.1nce.net) 기반 PDP active
 * - 세션마다 SIM7080 network time(CCLK)로 GW 시간을 보정
 * - time sync delta buffer에 보정 차이(초)를 저장
 * - TCP open -> CASEND -> CAACK 확인 -> close
 */

#ifndef GW_CATM1_H
#define GW_CATM1_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>
#include "stm32wlxx_hal.h"
#include "gw_storage.h"

#define GW_CATM1_TIME_SYNC_DELTA_BUF_LEN  (8u)

#ifdef __cplusplus
extern "C" {
#endif

void GW_Catm1_Init(void);
void GW_Catm1_PowerOn(void);
void GW_Catm1_PowerOff(void);

/* LPUART1 RX interrupt callback routing (project uart_if.c / weak callback override용) */
void GW_Catm1_UartRxCpltCallback(UART_HandleTypeDef *huart);
void GW_Catm1_UartErrorCallback(UART_HandleTypeDef *huart);

/* power-fault(SMS Ready loop) stop request latch */
bool GW_Catm1_ConsumePowerFaultStopRequest(void);

/* 서버 전송 등 고전류 동작 여부(비콘/BLE 충돌 회피에 사용) */
bool GW_Catm1_IsBusy(void);
void GW_Catm1_SetBusy(bool busy);

typedef enum
{
    GW_CATM1_JOB_NONE = 0,
    GW_CATM1_JOB_TIME_SYNC,
    GW_CATM1_JOB_QUERY_LOC,
    GW_CATM1_JOB_SNAPSHOT,
    GW_CATM1_JOB_STORED_RANGE,
} GW_Catm1JobType_t;

typedef enum
{
    GW_CATM1_JOB_STATE_IDLE = 0,
    GW_CATM1_JOB_STATE_BUSY,
    GW_CATM1_JOB_STATE_DONE_OK,
    GW_CATM1_JOB_STATE_DONE_FAIL,
} GW_Catm1JobState_t;

/* cooperative function-pointer runner */
void GW_Catm1_Process(void);
GW_Catm1JobState_t GW_Catm1_GetJobState(void);
GW_Catm1JobType_t GW_Catm1_GetJobType(void);
bool GW_Catm1_TakeJobResult(GW_Catm1JobType_t* out_type,
                            bool* out_ok,
                            uint32_t* out_sent_count);

/* non-blocking request API */
bool GW_Catm1_RequestTimeSync(void);
bool GW_Catm1_RequestSnapshot(const GW_HourRec_t* rec);
bool GW_Catm1_RequestStoredRange(uint32_t first_rec_index,
                                 uint32_t max_count);

/* one-shot session: power on -> network time sync -> power off */
bool GW_Catm1_SyncTimeOnce(void);

/* one-shot session: power on -> GNSS read/save -> power off */
bool GW_Catm1_QueryAndStoreLoc(char* out_line, size_t out_sz);

/* one-shot session: power on -> TCP send -> power off */
bool GW_Catm1_SendSnapshot(const GW_HourRec_t* rec);

/* send oldest-first records already saved in flash (global record index based) */
bool GW_Catm1_SendStoredRange(uint32_t first_rec_index,
                              uint32_t max_count,
                              uint32_t* out_sent_count);

/* time sync delta buffer helpers */
void GW_Catm1_ClearTimeSyncDeltaBuf(void);
uint8_t GW_Catm1_GetTimeSyncDeltaCount(void);
uint8_t GW_Catm1_CopyTimeSyncDeltaBuf(int64_t* out_buf, uint8_t max_items);

#ifdef __cplusplus
}
#endif

#endif /* GW_CATM1_H */
