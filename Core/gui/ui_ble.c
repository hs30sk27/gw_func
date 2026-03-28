#include "ui_ble.h"
#include "ui_conf.h"
#include "ui_lpm.h"
#include "ui_uart.h"
#include "gw_app.h"
#include "gw_catm1.h"
#include "stm32_timer.h"
#include "stm32_seq.h"
#include "main.h"

#include <string.h>
#include <stdio.h>

extern UART_HandleTypeDef hlpuart1;

/* -------------------------------------------------------------------------- */
/* 내부 상태                                                                  */
/* -------------------------------------------------------------------------- */
static volatile uint32_t s_evt_flags = 0;
#define BLE_EVT_TIMEOUT    (1u << 0)
#define BLE_EVT_LED_STEP   (1u << 1)
#define BLE_EVT_STOP_REQ   (1u << 2)
#define BLE_EVT_UART_INIT  (1u << 3)

static bool s_ble_active = false;
static bool s_ble_persistent = false;
static bool s_led_on = false;
static bool s_uart_init_pending = false;
static uint32_t s_uart_ready_deadline_ms = 0;
static uint32_t s_bt_on_tick_ms = 0;

static UTIL_TIMER_Object_t s_tmr_timeout;
static UTIL_TIMER_Object_t s_tmr_led;
static UTIL_TIMER_Object_t s_tmr_uart_init;

static char s_ble_name_cmd_buf[48];
static char s_ble_name_cmd_alt_buf[48];

#define UI_BLE_NAMECFG_PWR_OFF_MS      300u
#define UI_BLE_NAMECFG_BOOT_SETTLE_MS 1200u
#define UI_BLE_NAMECFG_CMD_GAP_MS      120u
#define UI_BLE_NAMECFG_RESTART_OFF_MS  300u

/* timer callback은 ISR 컨텍스트로 동작할 수 있으므로 task로 defer */
static void prv_tmr_timeout_cb(void *context)
{
    (void)context;
    s_evt_flags |= BLE_EVT_TIMEOUT;
    UTIL_SEQ_SetTask(UI_TASK_BIT_BLE, 0);
}

static void prv_tmr_led_cb(void *context)
{
    (void)context;
    s_evt_flags |= BLE_EVT_LED_STEP;
    UTIL_SEQ_SetTask(UI_TASK_BIT_BLE, 0);
}

static void prv_tmr_uart_init_cb(void *context)
{
    (void)context;
    s_evt_flags |= BLE_EVT_UART_INIT;
    UTIL_SEQ_SetTask(UI_TASK_BIT_BLE, 0);
}

static void prv_hw_set_bt(bool on)
{
#if UI_HAVE_BT_EN
    HAL_GPIO_WritePin(BT_EN_GPIO_Port, BT_EN_Pin, on ? GPIO_PIN_SET : GPIO_PIN_RESET);
#else
    (void)on;
#endif
}

static void prv_hw_set_led0(bool on)
{
#if UI_HAVE_LED0
    HAL_GPIO_WritePin(LED0_GPIO_Port, LED0_Pin, on ? GPIO_PIN_SET : GPIO_PIN_RESET);
#else
    (void)on;
#endif
}

static bool prv_lpuart1_is_inited(void)
{
    return ((hlpuart1.gState != HAL_UART_STATE_RESET) || (hlpuart1.RxState != HAL_UART_STATE_RESET));
}

static void prv_lpuart1_prepare_for_ble_on(void)
{
    if (GW_Catm1_IsBusy())
    {
        return;
    }

    if (prv_lpuart1_is_inited())
    {
        (void)HAL_UART_DeInit(&hlpuart1);
    }
}

static void prv_lpuart1_init_after_ble_delay(void)
{
    if (GW_Catm1_IsBusy())
    {
        return;
    }

    if (hlpuart1.Instance != NULL)
    {
        (void)HAL_UART_Init(&hlpuart1);
    }
}

static void prv_lpuart1_release_after_ble_off(void)
{
    if (GW_Catm1_IsBusy())
    {
        return;
    }

    if (prv_lpuart1_is_inited())
    {
        (void)HAL_UART_DeInit(&hlpuart1);
    }
}

static void prv_serial_prepare_for_ble_on(void)
{
    /* BT_EN 상승 직전 UART pin 구동을 해제해서 리셋 상승 구간 간섭을 줄인다. */
    UI_UART_DeInitLowPower();
    prv_lpuart1_prepare_for_ble_on();
}

static void prv_wait_uart_guard_after_bt_on(void)
{
    if (s_bt_on_tick_ms == 0u)
    {
        return;
    }

    uint32_t now = HAL_GetTick();
    uint32_t elapsed = now - s_bt_on_tick_ms;
    if (elapsed < UI_BLE_UART_INIT_DELAY_MS)
    {
        HAL_Delay(UI_BLE_UART_INIT_DELAY_MS - elapsed);
    }
}

static void prv_serial_init_after_ble_delay(void)
{
    /* timer가 일찍 깨우거나, 다른 경로가 먼저 들어와도 BT_EN ON 후 10ms는 반드시 보장한다. */
    prv_wait_uart_guard_after_bt_on();
    UI_UART_ReInit();
    prv_lpuart1_init_after_ble_delay();
    s_uart_init_pending = false;
    s_uart_ready_deadline_ms = 0;
#if (UI_BLE_AT_CMD_AFTER_UART_INIT == 1u)
    HAL_Delay(UI_BLE_AT_CMD_DELAY_MS);
    if (s_ble_active)
    {
        UI_UART_SendString(UI_BLE_AT_CMD);
    }
#endif
}

static void prv_led_schedule_next(void)
{
    uint32_t next_ms = s_led_on ? UI_LED0_ON_MS : UI_LED0_OFF_MS;

    /* one-shot */
    (void)UTIL_TIMER_Stop(&s_tmr_led);
    (void)UTIL_TIMER_SetPeriod(&s_tmr_led, next_ms);
    (void)UTIL_TIMER_Start(&s_tmr_led);
}

void UI_BLE_Process(void)
{
    /*
     * 단일 task 모드(UI_USE_SEQ_MULTI_TASKS=0)에서는 BLE 전용 task가 없으므로
     * UI_MAIN에서 이 함수를 주기적으로 호출하여 이벤트를 처리합니다.
     * 멀티 task 모드에서도 호출해도 무방합니다(이벤트가 없으면 즉시 return).
     */
    uint32_t ev = s_evt_flags;
    if (ev == 0u)
    {
        return;
    }
    s_evt_flags = 0;

    /* (1) BLE END 등: 즉시 stop 요청이 최우선 */
    if ((ev & BLE_EVT_STOP_REQ) != 0u)
    {
        UI_BLE_Disable();
        GW_App_PrepareForDormantStop();
        if (!GW_App_ShouldStayAwakeAfterBleOff())
        {
            UI_LPM_EnterStopNow();
        }
        return;
    }

    /* (2) 3분 타임아웃: BLE OFF 후 stop mode 진입 */
    if ((ev & BLE_EVT_TIMEOUT) != 0u)
    {
        if (s_ble_active && s_ble_persistent)
        {
            UI_BLE_EnableForMs(UI_BLE_ACTIVE_MS);
        }
        else
        {
            UI_BLE_Disable();
            GW_App_PrepareForDormantStop();
            if (!GW_App_ShouldStayAwakeAfterBleOff())
            {
                UI_LPM_EnterStopNow();
            }
            return;
        }
    }

    /* (3) BT_EN ON 후 UART1/LPUART1 init 지연 처리 */
    if ((ev & BLE_EVT_UART_INIT) != 0u)
    {
        if (s_ble_active && s_uart_init_pending)
        {
            prv_serial_init_after_ble_delay();
        }
    }

    /* (4) LED0 blink step */
    if ((ev & BLE_EVT_LED_STEP) != 0u)
    {
        if (s_ble_active)
        {
            /* toggle */
            s_led_on = !s_led_on;
            prv_hw_set_led0(s_led_on);

            /* 다음 타이밍 예약 */
            prv_led_schedule_next();
        }
        else
        {
            /* 비활성이면 LED 정리 */
            s_led_on = false;
            prv_hw_set_led0(false);
            (void)UTIL_TIMER_Stop(&s_tmr_led);
        }
    }
}

static void UI_BLE_Task(void)
{
    /* 멀티 task 모드에서만 등록/호출됨. 단일 모드에서도 안전하게 남겨둠 */
    UI_BLE_Process();
}

void UI_BLE_Init(void)
{
    /* Task 등록 (task bit이 충분한 경우에만 분리) */
#if (UI_USE_SEQ_MULTI_TASKS == 1u)
    UTIL_SEQ_RegTask(UI_TASK_BIT_BLE, 0, UI_BLE_Task);
#endif

    /* 타이머 생성 */
    (void)UTIL_TIMER_Create(&s_tmr_timeout, UI_BLE_ACTIVE_MS, UTIL_TIMER_ONESHOT, prv_tmr_timeout_cb, NULL);
    (void)UTIL_TIMER_Create(&s_tmr_led, UI_LED0_OFF_MS, UTIL_TIMER_ONESHOT, prv_tmr_led_cb, NULL);
    (void)UTIL_TIMER_Create(&s_tmr_uart_init, UI_BLE_UART_INIT_DELAY_MS, UTIL_TIMER_ONESHOT, prv_tmr_uart_init_cb, NULL);

    /* 최소 전류: BLE OFF 시 UART1 정리(필요 시 다시 Ensure로 켜면 됨) */
#if (UI_UART_DEINIT_WHEN_BLE_OFF == 1u)
    UI_UART_DeInitLowPower();
#endif

    s_ble_active = false;
    s_ble_persistent = false;
    s_led_on = false;
    s_uart_init_pending = false;
    s_uart_ready_deadline_ms = 0;
    s_bt_on_tick_ms = 0;
    prv_hw_set_bt(false);
    prv_hw_set_led0(false);
}

void UI_BLE_EnableForMs(uint32_t duration_ms)
{
#if UI_HAVE_BT_EN
    if (duration_ms == 0u) { duration_ms = UI_BLE_ACTIVE_MS; }

    if (!s_ble_active)
    {
        s_ble_active = true;

        /* stop mode 진입 방지 */
        UI_LPM_LockStop();

        /*
         * BT_EN ON 전에 UART1/LPUART1를 먼저 내려서 TX/RX 핀 High 구동을 해제한다.
         * 이후 BT_EN 상승 뒤 10ms 기다렸다가 다시 init 한다.
         */
        prv_serial_prepare_for_ble_on();

        /* HW ON */
        prv_hw_set_bt(true);
        s_bt_on_tick_ms = HAL_GetTick();

        s_uart_init_pending = true;
        s_uart_ready_deadline_ms = UTIL_TIMER_GetCurrentTime() + UI_BLE_UART_INIT_DELAY_MS;
        (void)UTIL_TIMER_Stop(&s_tmr_uart_init);
        (void)UTIL_TIMER_SetPeriod(&s_tmr_uart_init, UI_BLE_UART_INIT_DELAY_MS);
        (void)UTIL_TIMER_Start(&s_tmr_uart_init);

        /* LED blink 시작: ON(10ms) -> OFF(490ms) ... */
        s_led_on = true;
        prv_hw_set_led0(true);
        prv_led_schedule_next();
    }

    /* timeout 재설정(현재 시점부터 duration_ms) */
    (void)UTIL_TIMER_Stop(&s_tmr_timeout);
    (void)UTIL_TIMER_SetPeriod(&s_tmr_timeout, duration_ms);
    (void)UTIL_TIMER_Start(&s_tmr_timeout);
#else
    (void)duration_ms;
#endif
}

bool UI_BLE_ApplyDeviceName(const char* name_ascii)
{
#if UI_HAVE_BT_EN
    bool persistent = s_ble_persistent;

    if ((name_ascii == NULL) || (*name_ascii == '\0'))
    {
        return false;
    }

    /*
     * 요구 순서:
     *   BLE OFF -> delay -> BLE ON -> delay -> 이름 변경 -> delay -> BLE OFF -> delay -> BLE ON
     */
    UI_BLE_Disable();
    HAL_Delay(UI_BLE_NAMECFG_PWR_OFF_MS);

    UI_BLE_EnableForMs(UI_BLE_ACTIVE_MS);
    UI_BLE_EnsureSerialReady();
    HAL_Delay(UI_BLE_NAMECFG_BOOT_SETTLE_MS);

    UI_UART_SendString("AT\r\n");
    HAL_Delay(UI_BLE_NAMECFG_CMD_GAP_MS);
    UI_UART_SendString("AT\r\n");
    HAL_Delay(UI_BLE_NAMECFG_CMD_GAP_MS);

    (void)snprintf(s_ble_name_cmd_buf, sizeof(s_ble_name_cmd_buf), "AT+NAME%s\r\n", name_ascii);
    UI_UART_SendString(s_ble_name_cmd_buf);
    HAL_Delay(UI_BLE_NAMECFG_CMD_GAP_MS);

    (void)snprintf(s_ble_name_cmd_alt_buf, sizeof(s_ble_name_cmd_alt_buf), "AT+NAME=%s\r\n", name_ascii);
    UI_UART_SendString(s_ble_name_cmd_alt_buf);
    HAL_Delay(UI_BLE_NAMECFG_CMD_GAP_MS);

    UI_BLE_Disable();
    HAL_Delay(UI_BLE_NAMECFG_RESTART_OFF_MS);

    UI_BLE_EnableForMs(UI_BLE_ACTIVE_MS);
    if (persistent)
    {
        UI_BLE_SetPersistent(true);
    }
    return true;
#else
    (void)name_ascii;
    return false;
#endif
}


void UI_BLE_ExtendMs(uint32_t duration_ms)
{
    if (!s_ble_active)
    {
        /* BLE OFF 상태에서는 연장 의미 없음 */
        return;
    }
    UI_BLE_EnableForMs(duration_ms);
}

void UI_BLE_Disable(void)
{
    if (!s_ble_active) { return; }

    (void)UTIL_TIMER_Stop(&s_tmr_timeout);
    (void)UTIL_TIMER_Stop(&s_tmr_led);
    (void)UTIL_TIMER_Stop(&s_tmr_uart_init);

    prv_hw_set_led0(false);
    prv_hw_set_bt(false);

    /* 최소 전류: BLE OFF 시 UART1/LPUART1 정리 */
#if (UI_UART_DEINIT_WHEN_BLE_OFF == 1u)
    UI_UART_DeInitLowPower();
    prv_lpuart1_release_after_ble_off();
#endif

    s_ble_active = false;
    s_ble_persistent = false;
    s_uart_init_pending = false;
    s_uart_ready_deadline_ms = 0;
    s_bt_on_tick_ms = 0;

    /* stop mode 허용 */
    UI_LPM_UnlockStop();
}

bool UI_BLE_IsActive(void)
{
    return s_ble_active;
}

void UI_BLE_SetPersistent(bool enable)
{
    s_ble_persistent = enable;
    if (enable && s_ble_active)
    {
        UI_BLE_EnableForMs(UI_BLE_ACTIVE_MS);
    }
}

bool UI_BLE_IsPersistent(void)
{
    return s_ble_persistent;
}

void UI_BLE_RequestStopNow(void)
{
    /* task에서 처리하도록 defer */
    s_evt_flags |= BLE_EVT_STOP_REQ;
    UTIL_SEQ_SetTask(UI_TASK_BIT_BLE, 0);
}

void UI_BLE_ClearFlagsBeforeStop(void)
{
    (void)UTIL_TIMER_Stop(&s_tmr_timeout);
    (void)UTIL_TIMER_Stop(&s_tmr_led);
    (void)UTIL_TIMER_Stop(&s_tmr_uart_init);

    s_evt_flags  = 0;
    s_ble_active = false;
    s_ble_persistent = false;
    s_led_on     = false;
    s_uart_init_pending = false;
    s_uart_ready_deadline_ms = 0;
    s_bt_on_tick_ms = 0;

    prv_hw_set_led0(false);
    prv_hw_set_bt(false);

#if (UI_UART_DEINIT_WHEN_BLE_OFF == 1u)
    UI_UART_DeInitLowPower();
    prv_lpuart1_release_after_ble_off();
#endif

    /* Stop lock은 UI_LPM_LockStop/UnlockStop 흐름으로 관리(여기서 강제 변경하지 않음) */
}

bool UI_BLE_IsSerialReady(void)
{
    return (!s_uart_init_pending);
}

void UI_BLE_EnsureSerialReady(void)
{
    if (!s_ble_active || !s_uart_init_pending)
    {
        return;
    }

    /* 송신/수신 경로가 먼저 들어와도 BT_EN ON 후 10ms를 강제 보장하고 UART를 올린다. */
    (void)UTIL_TIMER_Stop(&s_tmr_uart_init);
    if (s_ble_active && s_uart_init_pending)
    {
        prv_serial_init_after_ble_delay();
    }
}

/* -------------------------------------------------------------------------- */
/* UI_CMD hook override: BLE END 명령 처리                                     */
/* -------------------------------------------------------------------------- */
void UI_Hook_OnBleEndRequested(void)
{
    UI_BLE_RequestStopNow();
}
