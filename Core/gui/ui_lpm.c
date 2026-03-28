#include "ui_lpm.h"
#include "ui_conf.h"

/* Stop 진입 전 런타임 플래그 정리용 */
#include "ui_core.h"
#include "ui_gpio.h"
#include "ui_uart.h"
#include "ui_ble.h"
#include "ui_time.h"
#include "ui_radio.h"
#include "gw_storage.h"
#include "timer_if.h"

#include "stm32_lpm.h"
#include "utilities_def.h" /* CFG_LPM_APPLI_Id */
#include "main.h"

#include "stm32wlxx_hal.h"
#include <stddef.h>

#if defined(HAL_SPI_MODULE_ENABLED)
#include "stm32wlxx_hal_spi.h"
#endif

#if defined(HAL_UART_MODULE_ENABLED)
#include "stm32wlxx_hal_uart.h"
#endif

#if defined(HAL_ADC_MODULE_ENABLED)
#include "stm32wlxx_hal_adc.h"
#endif
#include "ui_radio.h"

/* 주변장치 핸들(프로젝트 main.c에 존재) */
extern SPI_HandleTypeDef  hspi1;
extern UART_HandleTypeDef huart1;
extern UART_HandleTypeDef hlpuart1;
extern ADC_HandleTypeDef  hadc;

#if defined(DMA1_Channel2)
extern DMA_HandleTypeDef  hdma_usart1_tx;
#endif
extern void UI_Radio_EnterSleep(void);

static void prv_abort_peripheral_activity_before_deinit(void)
{
#if defined(HAL_UART_MODULE_ENABLED)
    (void)HAL_UART_Abort(&hlpuart1);
    (void)HAL_UART_Abort(&huart1);
#endif

#if defined(HAL_SPI_MODULE_ENABLED)
    (void)HAL_SPI_Abort(&hspi1);
#endif
}

static void prv_force_stop_pin_levels(void)
{
//#if defined(W25Q128_CS_GPIO_Port) && defined(W25Q128_CS_Pin)
//    HAL_GPIO_WritePin(W25Q128_CS_GPIO_Port, W25Q128_CS_Pin, GPIO_PIN_SET);
//#endif
#if defined(CATM1_PWR_GPIO_Port) && defined(CATM1_PWR_Pin)
    HAL_GPIO_WritePin(CATM1_PWR_GPIO_Port, CATM1_PWR_Pin, GPIO_PIN_RESET);
#endif
#if defined(PWR_KEY_GPIO_Port) && defined(PWR_KEY_Pin)
    HAL_GPIO_WritePin(PWR_KEY_GPIO_Port, PWR_KEY_Pin, UI_CATM1_PWRKEY_INACTIVE_STATE);
#endif
#if defined(BT_EN_GPIO_Port) && defined(BT_EN_Pin)
    HAL_GPIO_WritePin(BT_EN_GPIO_Port, BT_EN_Pin, GPIO_PIN_RESET);
#endif
#if defined(LED0_GPIO_Port) && defined(LED0_Pin)
    HAL_GPIO_WritePin(LED0_GPIO_Port, LED0_Pin, GPIO_PIN_RESET);
#endif
#if defined(LED1_GPIO_Port) && defined(LED1_Pin)
    HAL_GPIO_WritePin(LED1_GPIO_Port, LED1_Pin, GPIO_PIN_RESET);
#endif
#if defined(RF_TXEN_GPIO_Port) && defined(RF_TXEN_Pin)
    HAL_GPIO_WritePin(RF_TXEN_GPIO_Port, RF_TXEN_Pin, GPIO_PIN_RESET);
#endif
#if defined(RF_RXEN_GPIO_Port) && defined(RF_RXEN_Pin)
    HAL_GPIO_WritePin(RF_RXEN_GPIO_Port, RF_RXEN_Pin, GPIO_PIN_RESET);
#endif
}

static void prv_set_gpio_analog(GPIO_TypeDef *port, uint32_t pins)
{
    GPIO_InitTypeDef GPIO_InitStruct = {0};

    if ((port == NULL) || (pins == 0u))
    {
        return;
    }

    GPIO_InitStruct.Pin = pins;
    GPIO_InitStruct.Mode = GPIO_MODE_ANALOG;
    GPIO_InitStruct.Pull = GPIO_NOPULL;
    HAL_GPIO_Init(port, &GPIO_InitStruct);
}

static void prv_configure_deinited_pins_for_stop(void)
{
    /*
     * Stop 직전 deinit된 UART/SPI 핀과 사용하지 않는 입력 핀을
     * analog/no-pull로 내려 누설 전류를 차단한다.
     *
     * [주의] 핀별 처리 근거:
     *  - UART(BLE/CATM1), SPI SCK/MOSI/MISO : AF → analog
     *  - BATT_LVL (PA9) : INPUT + NOPULL → analog
     *    → 플로팅 디지털 입력은 Schmitt trigger 내부에서
     *      부정(不定) 전위로 수 μA 이상 소모. 반드시 analog로 내려야 한다.
     *  - W25Q128_CS (PB8) : OUTPUT_PP + PULLUP → analog
     *    → GW_Storage_W25Q_PowerDown() 이후 flash는 deep power-down 상태이므로
     *      CS 레벨 무관. 내부 pull-up 전류를 없애기 위해 analog로 전환한다.
     *      SPI가 재필요할 때 prv_spi_ensure_ready()에서 핀이 다시 설정된다.
     *  - BT_EN (PC13) : OUTPUT_PP LOW → OUTPUT 유지
     *    → BLE 모듈 EN 핀을 직접 구동하므로 analog(플로팅)로 전환하면
     *      모듈이 의도치 않게 켜질 수 있다. 구동 레벨(LOW)을 유지한다.
     */

    /* ---- GPIOA ---- */
#if defined(__HAL_RCC_GPIOA_CLK_ENABLE)
    __HAL_RCC_GPIOA_CLK_ENABLE();
#endif

    /* CATM1 UART TX/RX (AF → analog) */
#if defined(CATM1_RX_GPIO_Port) && defined(CATM1_RX_Pin)
    prv_set_gpio_analog(CATM1_RX_GPIO_Port, CATM1_RX_Pin);
#endif
#if defined(CATM1_TX_GPIO_Port) && defined(CATM1_TX_Pin)
    prv_set_gpio_analog(CATM1_TX_GPIO_Port, CATM1_TX_Pin);
#endif

    /* BATT_LVL (PA9): INPUT NOPULL → analog.
     * 플로팅 디지털 입력은 Stop 모드에서 누설 전류의 주요 원인이다.
     * 복귀 후 HAL_GPIO_ReadPin()이 필요하면 UI_LPM_AfterStop_ReInitPeripherals()
     * 에서 INPUT으로 복구한다. */
//#if defined(BATT_LVL_GPIO_Port) && defined(BATT_LVL_Pin)
//    prv_set_gpio_analog(BATT_LVL_GPIO_Port, BATT_LVL_Pin);
//#endif
#if defined(TH_GPIO_Port) && defined(TH_Pin)
    prv_set_gpio_analog(TH_GPIO_Port, TH_Pin);
#endif

    /* ---- GPIOB ---- */
#if defined(__HAL_RCC_GPIOB_CLK_ENABLE)
    __HAL_RCC_GPIOB_CLK_ENABLE();
#endif

    /* BLE UART TX/RX (AF → analog) */
#if defined(BLE_TX_GPIO_Port) && defined(BLE_TX_Pin)
    prv_set_gpio_analog(BLE_TX_GPIO_Port, BLE_TX_Pin);
#endif
#if defined(BLE_RX_GPIO_Port) && defined(BLE_RX_Pin)
    prv_set_gpio_analog(BLE_RX_GPIO_Port, BLE_RX_Pin);
#endif

    /* SPI1 SCK/MISO/MOSI (PB3/PB4/PB5, AF → analog) */
#if defined(GPIOB)
    prv_set_gpio_analog(GPIOB, GPIO_PIN_3 | GPIO_PIN_4 | GPIO_PIN_5);
#endif

    /* W25Q128 CS (PB8): OUTPUT_PP+PULLUP → analog.
     * flash는 이미 deep power-down 상태이므로 CS 레벨 불필요.
     * 내부 pull-up(~40kΩ)을 제거해 잔류 전류를 차단한다. */
#if defined(W25Q128_CS_GPIO_Port) && defined(W25Q128_CS_Pin)
    prv_set_gpio_analog(W25Q128_CS_GPIO_Port, W25Q128_CS_Pin);
#endif

    /* ---- GPIOC ---- */
    /* BT_EN (PC13): OUTPUT_PP LOW 유지 → BLE 모듈 구동 안전. analog 전환 생략. */
    /* GPIOC 클록은 GPIO output retention 이 동작하도록 유지한다. */
}

static void prv_disable_spi_clock(const SPI_HandleTypeDef *hspi)
{
#if defined(SPI1) && defined(__HAL_RCC_SPI1_CLK_DISABLE)
    if ((hspi != NULL) && (hspi->Instance == SPI1))
    {
        __HAL_RCC_SPI1_CLK_DISABLE();
        return;
    }
#endif
#if defined(SPI2) && defined(__HAL_RCC_SPI2_CLK_DISABLE)
    if ((hspi != NULL) && (hspi->Instance == SPI2))
    {
        __HAL_RCC_SPI2_CLK_DISABLE();
        return;
    }
#endif
#if defined(SPI3) && defined(__HAL_RCC_SPI3_CLK_DISABLE)
    if ((hspi != NULL) && (hspi->Instance == SPI3))
    {
        __HAL_RCC_SPI3_CLK_DISABLE();
        return;
    }
#endif
}

static void prv_disable_uart_clock(const UART_HandleTypeDef *huart)
{
#if defined(USART1) && defined(__HAL_RCC_USART1_CLK_DISABLE)
    if ((huart != NULL) && (huart->Instance == USART1))
    {
        __HAL_RCC_USART1_CLK_DISABLE();
        return;
    }
#endif
#if defined(USART2) && defined(__HAL_RCC_USART2_CLK_DISABLE)
    if ((huart != NULL) && (huart->Instance == USART2))
    {
        __HAL_RCC_USART2_CLK_DISABLE();
        return;
    }
#endif
#if defined(LPUART1) && defined(__HAL_RCC_LPUART1_CLK_DISABLE)
    if ((huart != NULL) && (huart->Instance == LPUART1))
    {
        __HAL_RCC_LPUART1_CLK_DISABLE();
        return;
    }
#endif
}

static void prv_disable_adc_clock(const ADC_HandleTypeDef *hadc_ptr)
{
#if defined(HAL_ADC_MODULE_ENABLED)
# if defined(ADC) && defined(__HAL_RCC_ADC_CLK_DISABLE)
    if ((hadc_ptr != NULL) && (hadc_ptr->Instance == ADC))
    {
        __HAL_RCC_ADC_CLK_DISABLE();
        return;
    }
# endif
# if defined(ADC1) && defined(__HAL_RCC_ADC_CLK_DISABLE)
    if ((hadc_ptr != NULL) && (hadc_ptr->Instance == ADC1))
    {
        __HAL_RCC_ADC_CLK_DISABLE();
        return;
    }
# endif
#endif
}

static void prv_clear_pending_irq_sources_before_stop(void)
{
    /* RTC Alarm A backend(TIMER_IF) uses EXTI line 17.
     * GPIO EXTI만 지우면 RTC alarm 경로의 stale pending이 남을 수 있으므로
     * STOP 직전 함께 정리한다. */
    TIMER_IF_ClearAlarmWakeupFlags();

#if defined(__HAL_GPIO_EXTI_CLEAR_IT)
# if defined(PULSE_IN_Pin)
    __HAL_GPIO_EXTI_CLEAR_IT(PULSE_IN_Pin);
# endif
# if defined(OP_KEY_Pin)
    __HAL_GPIO_EXTI_CLEAR_IT(OP_KEY_Pin);
# endif
# if defined(TEST_KEY_Pin)
    __HAL_GPIO_EXTI_CLEAR_IT(TEST_KEY_Pin);
# endif
#endif

#if defined(DMA1_Channel2_IRQn)
    HAL_NVIC_ClearPendingIRQ(DMA1_Channel2_IRQn);
#endif
#if defined(EXTI9_5_IRQn)
    HAL_NVIC_ClearPendingIRQ(EXTI9_5_IRQn);
#endif
#if defined(USART1_IRQn)
    HAL_NVIC_ClearPendingIRQ(USART1_IRQn);
#endif
#if defined(LPUART1_IRQn)
    HAL_NVIC_ClearPendingIRQ(LPUART1_IRQn);
#endif
#if defined(EXTI15_10_IRQn)
    HAL_NVIC_ClearPendingIRQ(EXTI15_10_IRQn);
#endif
#if defined(SUBGHZ_Radio_IRQn)
    HAL_NVIC_ClearPendingIRQ(SUBGHZ_Radio_IRQn);
#endif
}

static volatile uint32_t s_stop_lock = 0;

void UI_LPM_Init(void)
{
    /* 요구사항: off-mode(standby) 비활성화 */
    UTIL_LPM_SetOffMode((1U << CFG_LPM_APPLI_Id), UTIL_LPM_DISABLE);
}

void UI_LPM_LockStop(void)
{
    s_stop_lock++;
    UTIL_LPM_SetStopMode((1U << CFG_LPM_APPLI_Id), UTIL_LPM_DISABLE);
}

void UI_LPM_UnlockStop(void)
{
    if (s_stop_lock > 0u)
    {
        s_stop_lock--;
    }

    if (s_stop_lock == 0u)
    {
        UTIL_LPM_SetStopMode((1U << CFG_LPM_APPLI_Id), UTIL_LPM_ENABLE);
    }
}

bool UI_LPM_IsStopLocked(void)
{
    return (s_stop_lock != 0u);
}

void UI_UART1_TxDma_DeInit(void)
{
#if defined(DMA1_Channel2)
    /*
     * 요구사항:
     *  - UART1 TX DMA는 사용하지 않음.
     *  - 불필요한 DMA 동작/전류 증가를 줄이기 위해 DeInit.
     */
    (void)HAL_DMA_DeInit(&hdma_usart1_tx);
#endif
}

void UI_LPM_BeforeStop_DeInitPeripherals(void)
{
    /*
     * 요구사항: stop 들어가기 전에 spi, uart1, lpuart1, adc deinit
     *
     * NOTE
     *  - RF 작업 종료 직후에는 GW 이벤트 경로에서 Radio.Sleep()으로 내리고,
     *    stop 직전에도 한 번 더 내려서 SubGHz가 깨어 남지 않도록 한다.
     */
    /* Reset 대비: Stop 진입 직전에 현재 시간을 Backup Register에 저장 */
    UI_Time_SaveToBackupNow();

    /* RF가 마지막 상태에 남아 있지 않도록 stop 직전 강제 sleep */
    UI_Radio_EnterSleep();



    /*
     * W25Q128은 평상시 LittleFS unmount에서 deep power-down으로 내리지만,
     * stop 직전에도 한 번 더 내려서 SPI DeInit 이후 flash가 standby 전류로
     * 남아 있지 않도록 한다.
     */


    GW_Storage_W25Q_PowerDown();

    /* 외부 부하가 남지 않도록 제어 핀을 저전력 상태로 고정 */
    prv_force_stop_pin_levels();

    /* SW 상태 정리: 다음 wake-up 이후 재진입 시 꼬임 방지 */
    UI_Core_ClearFlagsBeforeStop();
    UI_GPIO_ClearEvents();
    UI_UART_ResetRxBuffer();
    UI_BLE_ClearFlagsBeforeStop();
//    prv_abort_peripheral_activity_before_deinit();
#if defined(HAL_ADC_MODULE_ENABLED)
    (void)HAL_ADC_Stop(&hadc);
    (void)HAL_ADC_DeInit(&hadc);
    prv_disable_adc_clock(&hadc);
#endif

#if defined(HAL_UART_MODULE_ENABLED)
    (void)HAL_UART_DeInit(&hlpuart1);
    prv_disable_uart_clock(&hlpuart1);

    (void)HAL_UART_DeInit(&huart1);
    prv_disable_uart_clock(&huart1);
#endif

#if defined(HAL_SPI_MODULE_ENABLED)
    (void)HAL_SPI_DeInit(&hspi1);
    prv_disable_spi_clock(&hspi1);
#endif

//    prv_configure_deinited_pins_for_stop();
    /* stop 직전 남아 있는 EXTI/UART/radio NVIC pending을 비워 즉시 재기상 방지 */
    prv_clear_pending_irq_sources_before_stop();
}

void UI_LPM_AfterStop_ReInitPeripherals(void)
{
    /*
     * 저전력(배터리) 정책:
     *  - Wake-up 직후 주변장치를 "무조건" ReInit 하지 않습니다.
     *  - 각 모듈이 필요할 때만 Init(Ensure) 하세요.
     *
     * 예)
     *  - BLE ON 시점: UI_UART_EnsureStarted() 호출
     *  - 센서 측정 시점: MX_ADC_Init(), MX_SPI1_Init() 등 필요 부분만 호출
     *
     * [핀 복구]
     * BeforeStop에서 analog로 내린 핀 중, 디지털 구동이 필요한 핀만 복구한다.
     * UART/SPI 핀은 각 모듈의 Ensure 경로에서 MspInit이 재설정하므로 여기서 건드리지 않는다.
     */

    /* W25Q128 CS (PB8): analog → OUTPUT_PP HIGH 복구
     * SPI 재초기화 전까지 CS HIGH를 보장해 flash 신호선 혼입을 막는다.
     * pull-up은 OUTPUT HIGH 구동 상태에서 불필요하므로 NOPULL 유지. */
#if defined(W25Q128_CS_GPIO_Port) && defined(W25Q128_CS_Pin)
    {
        GPIO_InitTypeDef g = {0};
        __HAL_RCC_GPIOB_CLK_ENABLE();
        g.Pin   = W25Q128_CS_Pin;
        g.Mode  = GPIO_MODE_OUTPUT_PP;
        g.Pull  = GPIO_NOPULL;
        g.Speed = GPIO_SPEED_FREQ_LOW;
        HAL_GPIO_Init(W25Q128_CS_GPIO_Port, &g);
        HAL_GPIO_WritePin(W25Q128_CS_GPIO_Port, W25Q128_CS_Pin, GPIO_PIN_SET);
    }
#endif

    /* BATT_LVL (PA9): analog → INPUT NOPULL 복구
     * 다음 사이클에서 HAL_GPIO_ReadPin()이 정상 동작하도록 원래 설정으로 되돌린다. */
#if defined(BATT_LVL_GPIO_Port) && defined(BATT_LVL_Pin)
    {
        GPIO_InitTypeDef g = {0};
        __HAL_RCC_GPIOA_CLK_ENABLE();
        g.Pin  = BATT_LVL_Pin;
        g.Mode = GPIO_MODE_INPUT;
        g.Pull = GPIO_NOPULL;
        HAL_GPIO_Init(BATT_LVL_GPIO_Port, &g);
    }
#endif
}

void UI_LPM_EnterStopNow(void)
{
    /* 동작 중이면 stop 진입 금지 */
    if (UI_LPM_IsStopLocked())
    {
        return;
    }

    /* 실제 STOP 진입/복귀 정리는 stm32_lpm_if.c의 PowerDriver 콜백에서 수행한다. */
    HAL_SuspendTick();
    UTIL_LPM_EnterLowPower();
    HAL_ResumeTick();
}
