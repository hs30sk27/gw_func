/*
 * ui_uart.h
 *
 * UART1(내부 명령) - 1바이트 RX 인터럽트 + 링버퍼
 *
 * 요구사항(요약)
 *  - 데이터 구조: "<내부명령>CRLF"
 *  - TX는 DMA 사용하지 않음(Blocking)
 *  - 프로젝트의 uart_if.c에서 HAL UART callback이 UI_*를 호출
 *
 * 저전력 포인트
 *  - 기본값(UI_UART_BOOT_START=0): 부팅 시 UART RX를 시작하지 않습니다.
 *    (BT 전원 OFF 상태에서 UART가 불필요한 경우 전류 최소)
 *  - BLE가 켜질 때(UI_BLE_EnableForMs) UI_UART_EnsureStarted()로 RX 시작.
 */

#ifndef UI_UART_H
#define UI_UART_H

#include <stdint.h>
#include <stdbool.h>
#include "stm32wlxx_hal.h"

#ifdef __cplusplus
extern "C" {
#endif

/* 링버퍼 초기화(부팅 1회 호출) */
void UI_UART_Init(void);

/* 필요할 때만 UART1을 Init + RX 재개(저전력 lazy init) */
void UI_UART_EnsureStarted(void);

/*
 * UART를 "확실히" 다시 초기화
 *  - TEST_KEY로 BLE 세션 시작 시, 이전 상태(에러/중단)를 깨끗이 정리하기 위한 용도.
 *  - DeInit -> MX_USART1_UART_Init -> Receive_IT 재시작
 */
void UI_UART_ReInit(void);

/* 저전력 목적: UART1 DeInit(Stop 진입 전/ BLE OFF 등) */
void UI_UART_DeInitLowPower(void);

/*
 * Stop 진입 전 등: RX 링버퍼/타이머/상태를 정리
 * - 미완성/쓰레기 데이터를 버려 다음 세션에서 동작이 꼬이지 않게 함
 */
void UI_UART_ResetRxBuffer(void);

/* HAL callback에서 호출 */
void UI_UART_RxCpltCallback(UART_HandleTypeDef *huart);
void UI_UART_ErrorCallback(UART_HandleTypeDef *huart);

/* 링버퍼 read */
bool UI_UART_ReadByte(uint8_t* out);

/* 송신 */
void UI_UART_SendString(const char* s);
void UI_UART_SendBytes(const uint8_t* data, uint16_t len);

/*
 * 마지막 RX 바이트가 들어온 시간(ms)
 * - BLE 패킷 분할/지연에 대응하여 "100ms 무응답" 기준으로
 *   미완성 프레임을 폐기(리셋)할 때 사용.
 */
uint32_t UI_UART_GetLastRxMs(void);

#ifdef __cplusplus
}
#endif

#endif /* UI_UART_H */
