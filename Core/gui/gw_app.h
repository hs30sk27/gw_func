/*
 * gw_app.h
 *
 * Gateway 동작(비콘 송신 + 노드 데이터 수신 스케줄러)
 *
 * 요구사항 핵심:
 *  - Gateway 3개(gw0/1/2): 비콘 5분마다, 00/02/04초
 *  - 비콘 payload: NETID(10) + TIME(6) + TEST(3) + CRC16
 *  - 데이터 수신: 정상모드 1시간마다 01:00부터 노드별 2초 슬롯
 *  - 테스트모드(SETTING:xxM): 1분 주기, 30초부터 수신, 노드 10개 제한
 *  - LoRa 동작 종료 시 Radio.Sleep()
 *  - Stop 모드 진입 중 동작 방지(UI_LPM_LockStop 사용)
 */

#ifndef GW_APP_H
#define GW_APP_H

#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

void GW_App_Init(void);

/* 단일 task 모드에서 UI_MAIN에서 호출하여 이벤트를 처리 */
void GW_App_Process(void);

/* BLE END/KEY로 강제 stop 진입할 때 GW 주기 wake source를 정리 */
void GW_App_PrepareForDormantStop(void);

/* BLE가 막 꺼진 직후에도 즉시 TCP uplink를 시작해야 하면 true */
bool GW_App_ShouldStayAwakeAfterBleOff(void);

/* GW TCP ON/OFF runtime gate */
bool GW_App_IsTcpEnabled(void);

/* Radio event에서 호출 (subghz_phy_app.c USER CODE에 삽입) */
void GW_Radio_OnTxDone(void);
void GW_Radio_OnTxTimeout(void);
void GW_Radio_OnRxDone(uint8_t *payload, uint16_t size, int16_t rssi, int8_t snr);
void GW_Radio_OnRxTimeout(void);
void GW_Radio_OnRxError(void);

#ifdef __cplusplus
}
#endif

#endif /* GW_APP_H */
