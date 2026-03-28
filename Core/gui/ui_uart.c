#include "ui_uart.h"
#include "ui_conf.h"
#include "ui_ble.h"
#include "ui_ringbuf.h"

#include "main.h"
#include "stm32_seq.h"
#include "stm32_timer.h"

#include <string.h>

/* 프로젝트에서 생성된 UART 핸들 */
extern UART_HandleTypeDef huart1;
extern void GW_Catm1_UartRxCpltCallback(UART_HandleTypeDef *huart);

/* main.c에서 생성된 Init 함수 */
extern void MX_USART1_UART_Init(void);

static uint8_t s_rx_byte = 0;
static uint8_t s_rb_mem[UI_UART_RX_RING_SIZE];
static UI_RingBuf_t s_rb;

/* BLE 패킷 분할/지연 대응: "100ms 무응답"이면 라인을 확정할 수 있도록 task를 깨움 */
static UTIL_TIMER_Object_t s_tmr_rx_idle;
static volatile uint32_t s_last_rx_ms = 0;

static void prv_rx_idle_cb(void *context)
{
    (void)context;
    /* UI_CORE에서 idle 기준으로 line을 확정(process)하도록 UI_MAIN을 한번 더 실행 */
    UTIL_SEQ_SetTask(UI_TASK_BIT_UI_MAIN, 0);
}

static bool prv_uart1_is_inited(void)
{
    /*
     * HAL_UART_DeInit() 이후 gState/RxState가 RESET으로 떨어지는 것을 이용.
     * (HAL 내부 멤버이지만, 저전력에서 "init 여부" 판단이 필요하여 사용)
     */
    return ((huart1.gState != HAL_UART_STATE_RESET) || (huart1.RxState != HAL_UART_STATE_RESET));
}

void UI_UART_Init(void)
{
    /* 링버퍼만 준비. RX 시작 여부는 정책(UI_UART_BOOT_START)에 따름 */
    UI_RingBuf_Init(&s_rb, s_rb_mem, UI_UART_RX_RING_SIZE);

    /* RX idle coalescing timer (one-shot) */
    (void)UTIL_TIMER_Create(&s_tmr_rx_idle, UI_UART_COALESCE_MS, UTIL_TIMER_ONESHOT, prv_rx_idle_cb, NULL);

#if (UI_UART_BOOT_START == 1u)
    UI_UART_EnsureStarted();
#endif
}

void UI_UART_EnsureStarted(void)
{
    /* BLE ON 직후 10ms 동안은 UART init을 미뤄서 BT 모듈 reset 상승 구간 간섭을 피한다. */
    UI_BLE_EnsureSerialReady();

    /* 필요할 때만 UART를 깨우고 RX를 시작(전류 최소) */
    if (!prv_uart1_is_inited())
    {
        MX_USART1_UART_Init();
    }

    /* 1바이트 RX 시작(이미 RX 중이면 HAL_BUSY가 나올 수 있으나 무시) */
    (void)HAL_UART_Receive_IT(&huart1, &s_rx_byte, 1);
}

void UI_UART_ReInit(void)
{
    /*
     * TEST_KEY로 BLE 세션을 시작할 때 "확실한" 초기화가 필요할 수 있습니다.
     * (이전 RX 에러/중단 상태, 라인 노이즈 등)
     */
    if (prv_uart1_is_inited())
    {
        (void)HAL_UART_DeInit(&huart1);
    }

    MX_USART1_UART_Init();

    /* 링버퍼는 남아 있는 쓰레기 데이터를 제거하기 위해 리셋 */
    UI_RingBuf_Init(&s_rb, s_rb_mem, UI_UART_RX_RING_SIZE);

    (void)HAL_UART_Receive_IT(&huart1, &s_rx_byte, 1);
}

void UI_UART_DeInitLowPower(void)
{
    /* BLE OFF 또는 Stop 진입 전 호출용 */
    (void)UTIL_TIMER_Stop(&s_tmr_rx_idle);
    if (prv_uart1_is_inited())
    {
        /* RX가 걸려 있어도 Stop 전에는 정리하는 편이 안전 */
        (void)HAL_UART_DeInit(&huart1);
    }
}

void UI_UART_ResetRxBuffer(void)
{
    /* Stop 진입 전/후: 남아있는 쓰레기 데이터로 인해 다음 세션이 꼬이는 것 방지 */
    (void)UTIL_TIMER_Stop(&s_tmr_rx_idle);
    UI_RingBuf_Init(&s_rb, s_rb_mem, UI_UART_RX_RING_SIZE);
    s_last_rx_ms = 0;
}

void UI_UART_RxCpltCallback(UART_HandleTypeDef *huart)
{
    if (huart == &huart1)
    {
        (void)UI_RingBuf_Push(&s_rb, s_rx_byte);

        /* 다음 바이트 수신 재시작 */
        (void)HAL_UART_Receive_IT(&huart1, &s_rx_byte, 1);

        /* 마지막 RX 시각 갱신 + idle timer 재시작 */
        s_last_rx_ms = UTIL_TIMER_GetCurrentTime();
        (void)UTIL_TIMER_Stop(&s_tmr_rx_idle);
        (void)UTIL_TIMER_SetPeriod(&s_tmr_rx_idle, UI_UART_COALESCE_MS);
        (void)UTIL_TIMER_Start(&s_tmr_rx_idle);

        /* 메인(UI) 처리 task 깨우기 */
        UTIL_SEQ_SetTask(UI_TASK_BIT_UI_MAIN, 0);
    }
    if (huart == &hlpuart1)
    {
        /* CATM1/SIM7080 전용 RX 경로 */
        GW_Catm1_UartRxCpltCallback(huart);
        return;
    }

}

void UI_UART_ErrorCallback(UART_HandleTypeDef *huart)
{
    if (huart == &huart1)
    {
        /* 에러가 나도 RX 재시작 */
        (void)HAL_UART_Receive_IT(&huart1, &s_rx_byte, 1);
    }
}

bool UI_UART_ReadByte(uint8_t* out)
{
    return UI_RingBuf_Pop(&s_rb, out);
}

void UI_UART_SendBytes(const uint8_t* data, uint16_t len)
{
    if (data == NULL || len == 0u) { return; }

    /* 송신 전에 UART가 꺼져 있을 수 있으므로 Ensure */
    UI_UART_EnsureStarted();

    (void)HAL_UART_Transmit(&huart1, (uint8_t*)data, len, UI_UART_TX_TIMEOUT_MS);
}

void UI_UART_SendString(const char* s)
{
    if (s == NULL) { return; }
    UI_UART_SendBytes((const uint8_t*)s, (uint16_t)strlen(s));
}

uint32_t UI_UART_GetLastRxMs(void)
{
    return (uint32_t)s_last_rx_ms;
}
