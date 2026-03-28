#include "ui_rf_plan_kr920.h"

/*
 * KR920(한국)에서 일반적으로 사용되는 대역 중,
 * 출력 제한/간섭 가능성이 상대적으로 적은 상위 채널(>=922MHz)만 사용.
 *
 * 필요 시 아래 테이블을 그대로 수정하면 됩니다.
 */
static const uint32_t s_data_freq_hz[] =
{
    922100000UL,
    922300000UL,
    922500000UL,
    /* 922700000Hz (중간 채널) 은 비콘용으로 고정 사용 */
    922900000UL,
    923100000UL,
    923300000UL,
};

uint32_t UI_RF_GetBeaconFreqHz(void)
{
    return 922700000UL; /* 테이블 중간 고정 */
}

uint8_t UI_RF_GetDataChannelCount(void)
{
    return (uint8_t)(sizeof(s_data_freq_hz) / sizeof(s_data_freq_hz[0]));
}

uint32_t UI_RF_GetDataFreqHz(uint32_t epoch_sec, uint32_t hop_period_sec, uint8_t seed)
{
    if (hop_period_sec == 0u)
    {
        hop_period_sec = 3600u;
    }

    uint32_t period_index = epoch_sec / hop_period_sec;
    uint8_t  ch_cnt = UI_RF_GetDataChannelCount();

    /* 간단한 호핑: (period_index + seed) % ch_cnt */
    uint8_t idx = (uint8_t)((period_index + (uint32_t)seed) % (uint32_t)ch_cnt);
    return s_data_freq_hz[idx];
}
