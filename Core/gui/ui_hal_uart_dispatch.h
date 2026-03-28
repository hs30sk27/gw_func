/*
 * ui_hal_uart_dispatch.h
 *
 * 프로젝트의 uart_if.c / stm32wlxx_it.c에서 호출할 수 있는 UART IRQ dispatch helper
 */
#ifndef UI_HAL_UART_DISPATCH_H
#define UI_HAL_UART_DISPATCH_H

#include "stm32wlxx_hal.h"

#ifdef __cplusplus
extern "C" {
#endif

void UI_HAL_UART_RxCpltDispatch(UART_HandleTypeDef *huart);
void UI_HAL_UART_ErrorDispatch(UART_HandleTypeDef *huart);
void UI_HAL_LPUART1_IrqDispatch(void);

#ifdef __cplusplus
}
#endif

#endif /* UI_HAL_UART_DISPATCH_H */
