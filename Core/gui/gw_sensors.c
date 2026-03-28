#include "gw_sensors.h"
#include "ui_conf.h"

#include "main.h"
#include "stm32wlxx_hal.h"

/* 프로젝트 핸들 */
extern ADC_HandleTypeDef hadc;

/* main.c에 생성된 Init 함수(Stop wake 후 필요할 때만 호출) */
extern void MX_ADC_Init(void);

/* -------------------------------------------------------------------------- */
/* 유틸: 정렬 + 트림 평균 (샘플 수가 작으므로 단순 구현)                      */
/* -------------------------------------------------------------------------- */
static void prv_sort_u16(uint16_t* a, uint16_t n)
{
    for (uint16_t i = 1; i < n; i++)
    {
        uint16_t key = a[i];
        int j = (int)i - 1;
        while (j >= 0 && a[j] > key)
        {
            a[j+1] = a[j];
            j--;
        }
        a[j+1] = key;
    }
}

static uint16_t prv_trimmed_mean_u16(uint16_t* a, uint16_t n, uint16_t trim_each_side)
{
    if (n == 0u) return 0xFFFFu;
    prv_sort_u16(a, n);

    uint16_t start = trim_each_side;
    uint16_t end   = (uint16_t)(n - trim_each_side);
    if (end <= start) return a[n/2u];

    uint32_t sum = 0;
    uint32_t cnt = 0;
    for (uint16_t i = start; i < end; i++)
    {
        sum += a[i];
        cnt++;
    }
    return (uint16_t)(sum / cnt);
}

/* -------------------------------------------------------------------------- */
/* ADC_EN 전원 스위치 (보드에 존재할 때만 사용)                                */
/* -------------------------------------------------------------------------- */
static void prv_set_adc_en(bool on)
{
#if defined(ADC_EN_Pin)
    HAL_GPIO_WritePin(ADC_EN_GPIO_Port, ADC_EN_Pin, on ? GPIO_PIN_SET : GPIO_PIN_RESET);
#else
    (void)on;
#endif
}

static void prv_ensure_adc_init(void)
{
#if defined(HAL_ADC_MODULE_ENABLED)
    if (hadc.State == HAL_ADC_STATE_RESET)
    {
        MX_ADC_Init();
    }
#endif
}

static bool prv_adc_read(uint32_t channel, uint16_t* out_raw)
{
#if defined(HAL_ADC_MODULE_ENABLED)
    ADC_ChannelConfTypeDef sConfig = {0};

    sConfig.Channel      = channel;
    sConfig.Rank         = ADC_REGULAR_RANK_1;
    sConfig.SamplingTime = UI_ADC_SAMPLINGTIME;
    /* HAL 버전/시리즈 호환성을 위해 SingleDiff/Offset 필드는 건드리지 않음 */

    if (HAL_ADC_ConfigChannel(&hadc, &sConfig) != HAL_OK) return false;
    if (HAL_ADC_Start(&hadc) != HAL_OK) return false;
    if (HAL_ADC_PollForConversion(&hadc, 50) != HAL_OK)
    {
        (void)HAL_ADC_Stop(&hadc);
        return false;
    }

    uint32_t v = HAL_ADC_GetValue(&hadc);
    (void)HAL_ADC_Stop(&hadc);

    *out_raw = (uint16_t)v;
    return true;
#else
    (void)channel;
    (void)out_raw;
    return false;
#endif
}

static bool prv_read_vdd_mv(uint16_t* out_vdd_mv)
{
#if defined(ADC_CHANNEL_VREFINT) && defined(__HAL_ADC_CALC_VREFANALOG_VOLTAGE)
    uint16_t raw = 0;
    if (!prv_adc_read(ADC_CHANNEL_VREFINT, &raw)) return false;

    uint32_t vdd_mv = __HAL_ADC_CALC_VREFANALOG_VOLTAGE(raw, ADC_RESOLUTION_12B);
    *out_vdd_mv = (uint16_t)vdd_mv;
    return true;
#else
    (void)out_vdd_mv;
    return false;
#endif
}

static bool prv_read_temp_x10(uint16_t vdd_mv, int16_t* out_temp_x10)
{
#if defined(ADC_CHANNEL_TEMPSENSOR) && defined(__HAL_ADC_CALC_TEMPERATURE)
    uint16_t samples[10];
    for (uint32_t i = 0; i < 10u; i++)
    {
        uint16_t raw = 0;
        if (!prv_adc_read(ADC_CHANNEL_TEMPSENSOR, &raw)) return false;
        samples[i] = raw;
        /* 내부 채널 안정화: 필요 시 짧게만 */
        HAL_Delay(2);
    }

    /* 중간 6개 평균 */
    uint16_t raw_mid = prv_trimmed_mean_u16(samples, 10u, 2u);

    int32_t temp_c = __HAL_ADC_CALC_TEMPERATURE(vdd_mv, raw_mid, ADC_RESOLUTION_12B);
    *out_temp_x10 = (int16_t)(temp_c * 10);
    return true;
#else
    (void)vdd_mv;
    (void)out_temp_x10;
    return false;
#endif
}

static bool prv_read_batt_lvl_gpio(uint8_t* out_batt_lvl)
{
#if defined(BATT_LVL_Pin) && defined(BATT_LVL_GPIO_Port)
    if (out_batt_lvl == NULL)
    {
        return false;
    }

    *out_batt_lvl = (HAL_GPIO_ReadPin(BATT_LVL_GPIO_Port, BATT_LVL_Pin) == UI_BATT_LVL_NORMAL_GPIO_STATE)
                  ? UI_NODE_BATT_LVL_NORMAL
                  : UI_NODE_BATT_LVL_LOW;
    return true;
#else
    (void)out_batt_lvl;
    return false;
#endif
}

static uint16_t prv_batt_lvl_to_compat_gw_volt_x10(uint8_t batt_lvl)
{
    if (batt_lvl == UI_NODE_BATT_LVL_NORMAL)
    {
        return UI_GW_BATT_GPIO_NORMAL_REPR_X10;
    }
    if (batt_lvl == UI_NODE_BATT_LVL_LOW)
    {
        return UI_GW_BATT_GPIO_LOW_REPR_X10;
    }
    return 0xFFFFu;
}

bool GW_Sensors_MeasureGw(uint16_t* volt_x10, int16_t* temp_x10)
{
    bool have_any = false;
    uint8_t batt_lvl = UI_NODE_BATT_LVL_INVALID;

    if ((volt_x10 == NULL) || (temp_x10 == NULL))
    {
        return false;
    }

    *volt_x10 = 0xFFFFu;
    *temp_x10 = (int16_t)0xFFFF;

    /* 배터리 레벨은 ADC가 아니라 BATT_LVL GPIO 입력을 그대로 사용한다. */
    if (prv_read_batt_lvl_gpio(&batt_lvl))
    {
        *volt_x10 = prv_batt_lvl_to_compat_gw_volt_x10(batt_lvl);
        have_any = true;
    }

#if defined(HAL_ADC_MODULE_ENABLED)
    /* 온도만 내부 ADC 사용 */
    prv_set_adc_en(true);
    prv_ensure_adc_init();

    uint16_t vdd_mv = 0u;
    if (prv_read_vdd_mv(&vdd_mv) && (vdd_mv != 0u))
    {
        int16_t t_x10 = (int16_t)0xFFFF;
        if (prv_read_temp_x10(vdd_mv, &t_x10))
        {
            *temp_x10 = t_x10;
            have_any = true;
        }
    }

    /* 전류 최소: 측정 끝나면 ADC 끄기 */
    (void)HAL_ADC_DeInit(&hadc);
    prv_set_adc_en(false);
#endif

    return have_any;
}
