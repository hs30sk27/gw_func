/*
 * ui_gpio.h
 *
 * GPIO EXTI callback 처리
 *
 * 요구사항:
 *  - __weak HAL_GPIO_EXTI_Callback(uint16_t GPIO_Pin) 존재
 *  - 함수 추가/링크: EXTI 콜백에서 호출되는 함수를 제공
 *
 * 주의:
 *  - ISR에서 delay 금지. 여기서는 이벤트 플래그만 설정하고,
 *    실제 동작(BLE on, 센서측정 등)은 메인(Task)에서 처리.
 */

#ifndef UI_GPIO_H
#define UI_GPIO_H

#include <stdint.h>
#include "ui_types.h"

#ifdef __cplusplus
extern "C" {
#endif

void UI_GPIO_Init(void);

/* HAL callback에서 호출 */
void UI_GPIO_ExtiCallback(uint16_t GPIO_Pin);

/* 메인에서 이벤트 확인 */
uint32_t UI_GPIO_FetchEvents(void);

/* Stop 진입 전 등: 이벤트 플래그 강제 클리어 */
void UI_GPIO_ClearEvents(void);

/* PULSE 카운터 */
uint32_t UI_GPIO_GetPulseCount(void);
void     UI_GPIO_ResetPulseCount(void);

#ifdef __cplusplus
}
#endif

#endif /* UI_GPIO_H */
