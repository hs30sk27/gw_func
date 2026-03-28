#include "ui_gpio.h"
#include "ui_conf.h"
#include "stm32_seq.h"
#include "stm32_timer.h"
#include "main.h"

static volatile uint32_t s_events = 0;
static volatile uint32_t s_pulse_count = 0;

/*
 * TEST_KEY / OP_KEY per-key debounce state
 *
 * rev34:
 * - 200ms 이내 채터링은 계속 무시
 * - main.c 에서 EXTI 가 GPIO_MODE_IT_FALLING 으로만 들어오므로,
 *   callback 시점의 현재 GPIO level 재검사는 하지 않음
 * - 첫 눌림이 wake-up 에만 소모되고 이벤트가 버려지는 현상 방지
 */
static volatile uint32_t s_test_key_last_ms = 0;
static volatile uint32_t s_op_key_last_ms   = 0;

static bool prv_accept_key_event(uint16_t GPIO_Pin)
{
    uint32_t now = UTIL_TIMER_GetCurrentTime();

    if (GPIO_Pin == TEST_KEY_Pin)
    {
        if ((uint32_t)(now - s_test_key_last_ms) < UI_KEY_DEBOUNCE_MS)
        {
            return false;
        }

        s_test_key_last_ms = now;
        return true;
    }

    if (GPIO_Pin == OP_KEY_Pin)
    {
        if ((uint32_t)(now - s_op_key_last_ms) < UI_KEY_DEBOUNCE_MS)
        {
            return false;
        }

        s_op_key_last_ms = now;
        return true;
    }

    return true;
}

void UI_GPIO_Init(void)
{
    s_events = 0;
    s_pulse_count = 0;
    s_test_key_last_ms = 0u;
    s_op_key_last_ms = 0u;
}

void UI_GPIO_ExtiCallback(uint16_t GPIO_Pin)
{
#if UI_HAVE_PULSE_IN
    if (GPIO_Pin == PULSE_IN_Pin)
    {
        s_pulse_count++;
        s_events |= UI_GPIO_EVT_PULSE_IN;
        UTIL_SEQ_SetTask(UI_TASK_BIT_UI_MAIN, 0);
        return;
    }
#endif

    /* TEST_KEY / OP_KEY 는 GW/ND 공통 키로 "항상" 동작해야 함.
     *
     * 과거 구현에서는 UI_HAVE_TEST_KEY / UI_HAVE_OP_KEY 조건을 사용했는데,
     * ui_conf.h가 main.h 보다 먼저 include 되는 경우 핀 매크로가 아직 정의되지 않아
     * 자동 감지가 0으로 떨어져 키 동작이 죽는 문제가 발생할 수 있습니다.
     *
     * 요구사항:
     * - #if UI_HAVE_TEST_KEY / #if UI_HAVE_OP_KEY 를 제거하여 무조건 동작.
     *
     * 추가 요구사항:
     * - TEST_KEY / OP_KEY 채터링으로 200ms 이내에 반복 들어온 EXTI는 무시.
     * - 첫 입력은 즉시 처리하고, 같은 키의 bounce만 차단.
     */
    if (GPIO_Pin == TEST_KEY_Pin)
    {
        if (!prv_accept_key_event(GPIO_Pin))
        {
            return;
        }

        s_events |= UI_GPIO_EVT_TEST_KEY;
        UTIL_SEQ_SetTask(UI_TASK_BIT_UI_MAIN, 0);
        return;
    }

    if (GPIO_Pin == OP_KEY_Pin)
    {
        if (!prv_accept_key_event(GPIO_Pin))
        {
            return;
        }

        s_events |= UI_GPIO_EVT_OP_KEY;
        UTIL_SEQ_SetTask(UI_TASK_BIT_UI_MAIN, 0);
        return;
    }
}

uint32_t UI_GPIO_FetchEvents(void)
{
    uint32_t e = s_events;
    s_events = 0;
    return e;
}

void UI_GPIO_ClearEvents(void)
{
    s_events = 0;
}

uint32_t UI_GPIO_GetPulseCount(void)
{
    return s_pulse_count;
}

void UI_GPIO_ResetPulseCount(void)
{
    s_pulse_count = 0;
}
