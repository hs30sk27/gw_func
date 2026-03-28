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

/* -------------------------------------------------------------------------- */
static void UI_TaskMain(void)
{
    /* 1) UART 명령 파싱(프레임: <CMD>CRLF only) */
    uint8_t b = 0;
    while (UI_UART_ReadByte(&b))
    {
        prv_cmdrx_feed(b);
    }

    /* 100ms 이상 끊기면 미완성 프레임은 폐기(응답 없음) */
    if (s_cmdrx_state != CMD_RX_WAIT_START)
    {
        uint32_t now  = UTIL_TIMER_GetCurrentTime();
        uint32_t last = UI_UART_GetLastRxMs();
        if ((uint32_t)(now - last) >= UI_UART_COALESCE_MS)
        {
            prv_cmdrx_reset();
        }
    }

    /* 2) GPIO 이벤트 처리 (ISR에서 delay 금지이므로 여기서 처리) */
    uint32_t ev = UI_GPIO_FetchEvents();

    if ((ev & UI_GPIO_EVT_TEST_KEY) != 0u)
    {
        /* 공통: TEST_KEY = BLE ON / timeout 연장
         * sync wait 중이면 UI_Hook_OnBleStartRequested()가 BLE resume을 처리한다.
         */
        if (!UI_Hook_OnBleStartRequested())
        {
            UI_BLE_EnableForMs(UI_BLE_ACTIVE_MS);
        }
    }

    if ((ev & UI_GPIO_EVT_OP_KEY) != 0u)
    {
        /* 공통: OP_KEY = BLE OFF */
        if (UI_BLE_IsActive())
        {
            UI_BLE_RequestStopNow();
        }
    }

    /* 3) BLE 이벤트 처리 (task bit이 부족한 경우 UI_MAIN에서 처리됨) */
    UI_BLE_Process();

    /* 4) Role(GW/ND) 이벤트 처리 (task bit이 부족한 경우 UI_MAIN에서 처리됨) */
    /* 4) GW 이벤트 처리 */
    GW_App_Process();
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
