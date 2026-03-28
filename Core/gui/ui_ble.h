/*
 * ui_ble.h
 *
 * TEST_KEY로 BLE 전원(BT_EN) ON + 3분 타이머 + LED0 블링크
 *
 * 요구사항:
 *  - TEST_KEY 누르면 BT_EN ON 후 3분 동작, stop mode 진입 방지
 *  - 명령 수신 시 3분 연장
 *  - BLE END 명령 시 BLE OFF 후 stop mode 진입
 *  - 동작 중 LED0: 10ms ON / 490ms OFF
 */

#ifndef UI_BLE_H
#define UI_BLE_H

#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

void UI_BLE_Init(void);

/* 단일 task 모드에서 UI_MAIN에서 호출하여 BLE 이벤트를 처리 */
void UI_BLE_Process(void);

void UI_BLE_EnableForMs(uint32_t duration_ms);
void UI_BLE_ExtendMs(uint32_t duration_ms);
void UI_BLE_Disable(void);

bool UI_BLE_IsActive(void);

/* DX-BT05 이름 변경: 예) "GW NUM 1" */
bool UI_BLE_ApplyDeviceName(const char* name_ascii);

/* TEST START 세션 등에서 BLE 자동 종료를 막는 유지 모드 */
void UI_BLE_SetPersistent(bool enable);
bool UI_BLE_IsPersistent(void);

/*
 * BLE 전원 ON 직후 UART init 지연을 강제한다.
 * - 메인 컨텍스트에서만 호출
 * - 지연이 남아 있으면 남은 시간만큼 대기 후 UART 준비를 완료
 */
void UI_BLE_EnsureSerialReady(void);
bool UI_BLE_IsSerialReady(void);

/* BLE END 등에서 호출: BLE 끄고 Stop 진입 시도 */
void UI_BLE_RequestStopNow(void);

/* Stop 진입 직전: BLE 관련 플래그/타이머/LED 상태를 정리 */
void UI_BLE_ClearFlagsBeforeStop(void);

#ifdef __cplusplus
}
#endif

#endif /* UI_BLE_H */
