/*
 * ui_lpm.h
 *
 * Low Power(Stop) 제어 + Stop 진입 전 주변장치 DeInit
 *
 * 요구사항(업데이트 반영)
 *  - 동작 중(LoRa, BLE, 센서 측정 등)에는 Stop 진입 방지(LOCK)
 *  - Stop 들어가기 전에 SPI/UART1/(GW는 LPUART1)/ADC DeInit
 *  - DeInit 후 해당 peripheral RCC clock도 함께 Disable
 *  - UART1 TX DMA 미사용 -> 별도 DeInit 함수 제공(메인에서 1회 호출)
 *
 * 저전력 정책(중요)
 *  - Wake-up 직후에 "무조건" 주변장치를 ReInit 하지 않습니다.
 *    (배터리 구동에서 불필요한 주변장치 Init은 전류 증가)
 *  - 각 모듈이 필요할 때만 해당 주변장치를 Init(Ensure) 하도록 구성하세요.
 *
 * NOTE
 *  - UI_LPM_AfterStop_ReInitPeripherals()는 호환을 위해 남겨두었고,
 *    기본 구현은 "아무 것도 하지 않음" 입니다.
 */

#ifndef UI_LPM_H
#define UI_LPM_H

#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

void UI_LPM_Init(void);

/* Stop mode lock/unlock (ref-count) */
void UI_LPM_LockStop(void);
void UI_LPM_UnlockStop(void);
bool UI_LPM_IsStopLocked(void);

/* Stop 진입 전/후 주변장치 정리 */
void UI_LPM_BeforeStop_DeInitPeripherals(void);
void UI_LPM_AfterStop_ReInitPeripherals(void);

/* 즉시 Stop 진입 요청(메인 컨텍스트에서 호출 권장) */
void UI_LPM_EnterStopNow(void);

/* UART1 TX DMA 비사용 정책: DMA DeInit (메인에서 1회 호출 권장) */
void UI_UART1_TxDma_DeInit(void);

#ifdef __cplusplus
}
#endif

#endif /* UI_LPM_H */
