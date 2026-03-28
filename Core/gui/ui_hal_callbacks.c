/*
 * ui_hal_callbacks.c
 *
 * GUI 전용 HAL/UART dispatch helper.
 *
 * - 기본은 weak callback override를 제공하여, 프로젝트에 별도 uart_if.c가 없으면 바로 동작합니다.
 * - 프로젝트가 uart_if.c / stm32wlxx_it.c를 이미 소유하고 있으면,
 *   거기서 UI_HAL_UART_RxCpltDispatch(), UI_HAL_UART_ErrorDispatch(),
 *   UI_HAL_LPUART1_IrqDispatch()를 호출하면 됩니다.
 */

#include "ui_conf.h"
#include "ui_gpio.h"
#include "ui_uart.h"
#include "ui_hal_uart_dispatch.h"
#include "gw_catm1.h"
#include "stm32wlxx_hal.h"

extern UART_HandleTypeDef hlpuart1;

void UI_HAL_UART_RxCpltDispatch(UART_HandleTypeDef *huart)
{
    /* CAT-M1 session 동안에는 LPUART1 RX를 gw_catm1으로 직접 라우팅한다.
     * 이렇게 해야 UI_UART 쪽 dispatch 구현과 무관하게 SIM7080 초기화 URC/SMS Ready/AT 응답을
     * 안정적으로 CAT-M1 ring buffer가 받는다. */
    if ((huart == &hlpuart1) && GW_Catm1_IsBusy())
    {
        GW_Catm1_UartRxCpltCallback(huart);
        return;
    }

    UI_UART_RxCpltCallback(huart);
}

void UI_HAL_UART_ErrorDispatch(UART_HandleTypeDef *huart)
{
    if ((huart == &hlpuart1) && GW_Catm1_IsBusy())
    {
        GW_Catm1_UartErrorCallback(huart);
        return;
    }

    UI_UART_ErrorCallback(huart);
}

void UI_HAL_LPUART1_IrqDispatch(void)
{
    if (hlpuart1.Instance != NULL)
    {
        HAL_UART_IRQHandler(&hlpuart1);
    }
}

#if (UI_OVERRIDE_HAL_UART_WEAK_DISPATCH == 1u)
__attribute__((weak)) void HAL_UART_RxCpltCallback(UART_HandleTypeDef *huart)
{
    UI_HAL_UART_RxCpltDispatch(huart);
}

__attribute__((weak)) void HAL_UART_ErrorCallback(UART_HandleTypeDef *huart)
{
    UI_HAL_UART_ErrorDispatch(huart);
}

__attribute__((weak)) void LPUART1_IRQHandler(void)
{
    UI_HAL_LPUART1_IrqDispatch();
}
#endif

#if (UI_OVERRIDE_HAL_GPIO_EXTI_CALLBACK == 1u)
void HAL_GPIO_EXTI_Callback(uint16_t GPIO_Pin)
{
    UI_GPIO_ExtiCallback(GPIO_Pin);
}
#endif /* UI_OVERRIDE_HAL_GPIO_EXTI_CALLBACK */
