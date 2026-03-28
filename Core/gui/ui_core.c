#include "ui_core.h"
#include "ui_conf.h"
#include "ui_types.h"
#include "ui_uart.h"
#include "ui_gpio.h"
#include "ui_cmd.h"
#include "ui_time.h"
#include "ui_ble.h"
#include "ui_lpm.h"
#include "gw_app.h"
#include "stm32_seq.h"
#include "stm32_timer.h"
#include "stm32wlxx_hal.h" /* __weak */

#include <string.h>

/* -------------------------------------------------------------------------- */
/* Hook functions (ND/GW에서 override 가능)                                     */
/* -------------------------------------------------------------------------- */
__weak void UI_Hook_OnOpKeyPressed(void) {}
__weak bool UI_Hook_OnBleStartRequested(void) { return false; }

/* -------------------------------------------------------------------------- */
/* UART 명령 수신 파서 ("<CMD>CRLF" only)                                     */
/* -------------------------------------------------------------------------- */
typedef enum
{
    CMD_RX_WAIT_START = 0,
    CMD_RX_IN_FRAME,
    CMD_RX_WAIT_LF,
} CmdRxState_t;

static CmdRxState_t s_cmdrx_state = CMD_RX_WAIT_START;
static char         s_cmdrx_buf[UI_UART_LINE_MAX];
static uint16_t     s_cmdrx_len = 0;

static void prv_cmdrx_reset(void)
{
    s_cmdrx_state = CMD_RX_WAIT_START;
    s_cmdrx_len   = 0;
}

static void prv_cmdrx_finalize(void)
{
    if (s_cmdrx_len >= (UI_UART_LINE_MAX - 1u))
    {
        prv_cmdrx_reset();
        return;
    }

    s_cmdrx_buf[s_cmdrx_len] = '\0';
    if (s_cmdrx_len > 0u)
    {
        UI_Cmd_ProcessLine(s_cmdrx_buf);
    }
    prv_cmdrx_reset();
}

static void prv_cmdrx_feed(uint8_t b)
{
    switch (s_cmdrx_state)
    {
        case CMD_RX_WAIT_START:
        {
            if (b == '<')
            {
                s_cmdrx_len = 0;
                s_cmdrx_buf[s_cmdrx_len++] = '<';
                s_cmdrx_state = CMD_RX_IN_FRAME;
            }
            break;
        }

        case CMD_RX_IN_FRAME:
        {
            if (b == '<')
            {
                /* resync */
                s_cmdrx_len = 0;
                s_cmdrx_buf[s_cmdrx_len++] = '<';
                s_cmdrx_state = CMD_RX_IN_FRAME;
                break;
            }
            if (b == '\r' || b == '\n')
            {
                /* garbage */
                prv_cmdrx_reset();
                break;
            }

            if (s_cmdrx_len < (UI_UART_LINE_MAX - 1u))
            {
                s_cmdrx_buf[s_cmdrx_len++] = (char)b;
            }
            else
            {
                prv_cmdrx_reset();
                break;
            }

            if (b == '>')
            {
                s_cmdrx_state = CMD_RX_WAIT_LF;
            }
            break;
        }

        case CMD_RX_WAIT_LF:
        {
            if (b == '\r') { break; }
            if (b == '\n')
            {
                prv_cmdrx_finalize();
                break;
            }
            if (b == '<')
            {
                /* drop prev and start new */
                s_cmdrx_len = 0;
                s_cmdrx_buf[s_cmdrx_len++] = '<';
                s_cmdrx_state = CMD_RX_IN_FRAME;
                break;
            }
            prv_cmdrx_reset();
            break;
        }

        default:
            prv_cmdrx_reset();
            break;
    }
}

typedef void (*UI_CoreFunc_t)(void);

static void prv_ui_idle(void)
{
}

static void prv_ui_uart_func(void)
{
    uint8_t b = 0u;

    while (UI_UART_ReadByte(&b))
    {
        prv_cmdrx_feed(b);
    }

    if (s_cmdrx_state != CMD_RX_WAIT_START)
    {
        uint32_t now  = UTIL_TIMER_GetCurrentTime();
        uint32_t last = UI_UART_GetLastRxMs();
        if ((uint32_t)(now - last) >= UI_UART_COALESCE_MS)
        {
            prv_cmdrx_reset();
        }
    }
}

static void prv_ui_key_func(void)
{
    uint32_t ev = UI_GPIO_FetchEvents();

    if ((ev & UI_GPIO_EVT_TEST_KEY) != 0u)
    {
        if (!UI_Hook_OnBleStartRequested())
        {
            UI_BLE_EnableForMs(UI_BLE_ACTIVE_MS);
        }
    }

    if ((ev & UI_GPIO_EVT_OP_KEY) != 0u)
    {
        if (UI_BLE_IsActive())
        {
            UI_BLE_RequestStopNow();
        }
    }
}

static void prv_ui_ble_func(void)
{
    UI_BLE_Process();
}

static void prv_ui_role_func(void)
{
    GW_App_Process();
}

static UI_CoreFunc_t s_uart_func = prv_ui_uart_func;
static UI_CoreFunc_t s_key_func  = prv_ui_key_func;
static UI_CoreFunc_t s_ble_func  = prv_ui_ble_func;
static UI_CoreFunc_t s_role_func = prv_ui_role_func;

/* -------------------------------------------------------------------------- */
static void UI_TaskMain(void)
{
    if (s_uart_func == NULL) { s_uart_func = prv_ui_idle; }
    if (s_key_func  == NULL) { s_key_func  = prv_ui_idle; }
    if (s_ble_func  == NULL) { s_ble_func  = prv_ui_idle; }
    if (s_role_func == NULL) { s_role_func = prv_ui_idle; }

    s_uart_func();
    s_key_func();
    s_ble_func();
    s_role_func();
}

void UI_Core_ClearFlagsBeforeStop(void)
{
    prv_cmdrx_reset();
}

void UI_Init(void)
{

    /* Task 등록 */
    UTIL_SEQ_RegTask(UI_TASK_BIT_UI_MAIN, 0, UI_TaskMain);

    /* 모듈 init */
    (void)UI_GetConfig(); /* config default init */
    UI_Time_Init();
    UI_LPM_Init();
    UI_GPIO_Init();
    UI_UART_Init();
    UI_BLE_Init();

    /* 역할별 앱은 UI에서 직접 초기화한다.
     * - main.c에서 별도 GW_App_Init(); 호출이 빠져도 동작해야 함
     * - 이전 버전에서는 이 초기화가 빠지면 BEACON ON/SETTING 후 타이머 객체가
     *   미생성 상태로 접근되어 비콘 미동작 또는 HardFault 원인이 될 수 있었다.
     */
    GW_App_Init();

    /* 초기 한 번 task 실행 유도(상태 정리) */
    UTIL_SEQ_SetTask(UI_TASK_BIT_UI_MAIN, 0);
}
