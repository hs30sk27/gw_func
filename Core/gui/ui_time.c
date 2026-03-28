#include "ui_time.h"
#include "ui_conf.h"
#include "stm32_timer.h"  /* UTIL_TIMER_GetCurrentTime() */
#include <string.h>
#include <stdio.h>



/* -------------------------------------------------------------------------- */
/* RTC Backup Register 기반 시간 유지 (Reset에도 시간 유지)                    */
/* -------------------------------------------------------------------------- */
/*
 * 증상:
 *  - MCU Reset(리셋) 시 RAM 변수(s_base_epoch_centi)가 초기화되어 시간이 0으로 돌아감.
 *
 * 해결:
 *  - RTC Backup Register에 epoch2016(sec) + centi를 저장해 두고
 *    UI_Time_Init()에서 자동 복원.
 *
 * 저장 주기:
 *  - TIME 설정 시 즉시 저장
 *  - Stop 진입 직전(UI_LPM_BeforeStop_DeInitPeripherals) 저장
 *  - 동작 중에는 60초마다 자동 저장(호출 빈도가 있어야 함)
 */

#if defined(HAL_RTC_MODULE_ENABLED)
#include "stm32wlxx_hal_rtc_ex.h"
extern RTC_HandleTypeDef hrtc;

#define UI_TIME_BKP_MAGIC       (0x5549544Du) /* 'UITM' */
#define UI_TIME_BKP_DR_MAGIC    (RTC_BKP_DR4)
#define UI_TIME_BKP_DR_EPOCH    (RTC_BKP_DR5)
#define UI_TIME_BKP_DR_CENTI    (RTC_BKP_DR6)
#define UI_TIME_BKP_DR_EPOCH_INV (RTC_BKP_DR7)

#define UI_TIME_BKP_AUTOSAVE_PERIOD_S (60u)

static uint32_t s_last_bkp_save_sec = 0;

static bool prv_bkp_read(uint32_t* out_sec, uint8_t* out_centi)
{
    uint32_t magic = HAL_RTCEx_BKUPRead(&hrtc, UI_TIME_BKP_DR_MAGIC);
    if (magic != UI_TIME_BKP_MAGIC)
    {
        return false;
    }

    uint32_t sec = HAL_RTCEx_BKUPRead(&hrtc, UI_TIME_BKP_DR_EPOCH);
    uint32_t inv = HAL_RTCEx_BKUPRead(&hrtc, UI_TIME_BKP_DR_EPOCH_INV);
    if (inv != (~sec))
    {
        return false;
    }

    uint32_t c = HAL_RTCEx_BKUPRead(&hrtc, UI_TIME_BKP_DR_CENTI);
    uint8_t centi = (uint8_t)(c & 0xFFu);
    if (centi > 99u)
    {
        return false;
    }

    *out_sec   = sec;
    *out_centi = centi;
    return true;
}

static void prv_bkp_write(uint32_t sec, uint8_t centi)
{
    /* Backup domain write enable */
    HAL_PWR_EnableBkUpAccess();

    HAL_RTCEx_BKUPWrite(&hrtc, UI_TIME_BKP_DR_MAGIC, UI_TIME_BKP_MAGIC);
    HAL_RTCEx_BKUPWrite(&hrtc, UI_TIME_BKP_DR_EPOCH, sec);
    HAL_RTCEx_BKUPWrite(&hrtc, UI_TIME_BKP_DR_CENTI, (uint32_t)centi);
    HAL_RTCEx_BKUPWrite(&hrtc, UI_TIME_BKP_DR_EPOCH_INV, (uint32_t)(~sec));
}
#endif /* HAL_RTC_MODULE_ENABLED */

/* -------------------------------------------------------------------------- */
/* 내부 상태                                                                  */
/* -------------------------------------------------------------------------- */
static uint64_t s_base_epoch_centi = 0; /* epoch2016 in centi */
static uint32_t s_base_ms          = 0; /* UTIL_TIMER ms at base */
static bool     s_time_valid       = false;

static uint32_t prv_get_ms(void)
{
    /* UTIL_TIMER는 RTC 기반으로 동작 (stop 모드에서도 유지) */
    return UTIL_TIMER_GetCurrentTime();
}

/* -------------------------------------------------------------------------- */
/* 달력 변환 (단순/명료 버전)                                                 */
/* -------------------------------------------------------------------------- */
static bool prv_is_leap(uint16_t y)
{
    /* Gregorian */
    return ((y % 4u) == 0u && ((y % 100u) != 0u || (y % 400u) == 0u));
}

static uint8_t prv_days_in_month(uint16_t y, uint8_t m)
{
    static const uint8_t dim[12] =
    {
        31,28,31,30,31,30,31,31,30,31,30,31
    };
    if (m == 2u)
    {
        return (uint8_t)(dim[1] + (prv_is_leap(y) ? 1u : 0u));
    }
    if (m >= 1u && m <= 12u)
    {
        return dim[m-1u];
    }
    return 30u;
}

uint32_t UI_Time_Epoch2016_FromCalendar(const UI_DateTime_t* dt)
{
    /* 2016-01-01 00:00:00 기준 */
    uint32_t days = 0;

    uint16_t y = dt->year;
    if (y < UI_EPOCH_BASE_YEAR) { y = UI_EPOCH_BASE_YEAR; }

    for (uint16_t yy = UI_EPOCH_BASE_YEAR; yy < y; yy++)
    {
        days += prv_is_leap(yy) ? 366u : 365u;
    }

    for (uint8_t mm = 1u; mm < dt->month; mm++)
    {
        days += prv_days_in_month(y, mm);
    }

    if (dt->day >= 1u)
    {
        days += (uint32_t)(dt->day - 1u);
    }

    uint32_t sec = days * 86400u;
    sec += (uint32_t)dt->hour * 3600u;
    sec += (uint32_t)dt->min  * 60u;
    sec += (uint32_t)dt->sec;
    return sec;
}

void UI_Time_Epoch2016_ToCalendar(uint32_t epoch_sec, UI_DateTime_t* out_dt)
{
    uint32_t days = epoch_sec / 86400u;
    uint32_t rem  = epoch_sec % 86400u;

    out_dt->hour = (uint8_t)(rem / 3600u);
    rem %= 3600u;
    out_dt->min  = (uint8_t)(rem / 60u);
    out_dt->sec  = (uint8_t)(rem % 60u);

    uint16_t y = UI_EPOCH_BASE_YEAR;
    while (1)
    {
        uint32_t ydays = prv_is_leap(y) ? 366u : 365u;
        if (days < ydays) { break; }
        days -= ydays;
        y++;
    }
    out_dt->year = y;

    uint8_t m = 1u;
    while (1)
    {
        uint32_t mdays = (uint32_t)prv_days_in_month(y, m);
        if (days < mdays) { break; }
        days -= mdays;
        m++;
        if (m > 12u) { m = 12u; break; }
    }
    out_dt->month = m;
    out_dt->day   = (uint8_t)(days + 1u);
}

/* -------------------------------------------------------------------------- */
/* Public API                                                                 */
/* -------------------------------------------------------------------------- */
void UI_Time_Init(void)
{
    s_base_ms          = prv_get_ms();
    s_base_epoch_centi = 0;
    s_time_valid       = false;
#if defined(HAL_RTC_MODULE_ENABLED)
    s_last_bkp_save_sec = 0;

    /* Reset 후에도 시간을 유지: Backup Register에 저장된 값이 있으면 복원 */
    uint32_t sec = 0;
    uint8_t  cs  = 0;
    if (prv_bkp_read(&sec, &cs))
    {
        s_base_ms          = prv_get_ms();
        s_base_epoch_centi = (uint64_t)sec * 100u + (uint64_t)cs;
        s_time_valid       = true;
        s_last_bkp_save_sec = sec;
    }
#endif
}

static uint64_t prv_now_centi_raw(void)
{
    uint32_t now_ms = prv_get_ms();
    uint32_t delta_ms = now_ms - s_base_ms; /* uint32 wrap-safe */
    uint64_t delta_centi = (uint64_t)(delta_ms / 10u);
    return s_base_epoch_centi + delta_centi;
}

uint64_t UI_Time_NowCenti2016(void)
{
    uint64_t now_centi = prv_now_centi_raw();

#if defined(HAL_RTC_MODULE_ENABLED)
    /* 60초마다 자동 저장 (Reset 시 최근 시간을 복원하기 위함) */
    if (s_time_valid)
    {
        uint32_t now_sec = (uint32_t)(now_centi / 100u);
        uint32_t delta   = (uint32_t)(now_sec - s_last_bkp_save_sec);
        if (delta >= UI_TIME_BKP_AUTOSAVE_PERIOD_S)
        {
            prv_bkp_write(now_sec, (uint8_t)(now_centi % 100u));
            s_last_bkp_save_sec = now_sec;
        }
    }
#endif

    return now_centi;
}

uint32_t UI_Time_NowSec2016(void)
{
    return (uint32_t)(UI_Time_NowCenti2016() / 100u);
}

uint8_t UI_Time_NowCentiPart(void)
{
    return (uint8_t)(UI_Time_NowCenti2016() % 100u);
}

bool UI_Time_IsValid(void)
{
    return s_time_valid;
}

void UI_Time_SetEpochCenti2016(uint64_t epoch_centi)
{
    s_base_ms          = prv_get_ms();
    s_base_epoch_centi = epoch_centi;
    s_time_valid       = true;
#if defined(HAL_RTC_MODULE_ENABLED)
    {
        uint32_t sec = (uint32_t)(epoch_centi / 100u);
        uint8_t  cs  = (uint8_t)(epoch_centi % 100u);
        prv_bkp_write(sec, cs);
        s_last_bkp_save_sec = sec;
    }
#endif
}

/* "YY-MM-DD hh:mm:ss.mm" 파싱 */
bool UI_Time_SetFromString(const char* time_str)
{
    /*
     * 수신 포맷(호환):
     *  - TIME:YY-MM-DD hh:mm:ss.mm
     *  - TIME:YY-MM-DD hh:mm:ss        (mm 생략 허용)
     *  - TIME:YYYY-MM-DD hh:mm:ss.mm   (4자리 연도 허용)
     *  - 앞/뒤 공백 허용(상위에서 rstrip 처리)
     */
    UI_DateTime_t dt = {0};
    int year=0, mon=0, dd=0, hh=0, mm=0, ss=0, cs=0;

    /* 허용: 앞에 "TIME:"가 붙어있을 수도 있음 */
    const char* p = time_str;
    if (strncmp(p, "TIME:", 5) == 0) { p += 5; }

    /* TIME: 뒤에 공백이 들어오는 케이스 대응 */
    while (*p == ' ' || *p == '\t') { p++; }

    /*
     * sscanf는 표준 C지만, 임베디드에서 사용 가능하도록 최소 사용.
     *  - centi(".mm")는 선택.
     */
    int n = sscanf(p, "%4d-%2d-%2d %2d:%2d:%2d.%2d", &year, &mon, &dd, &hh, &mm, &ss, &cs);
    if (n < 6)
    {
        /* centi 생략 케이스 재시도 */
        cs = 0;
        n = sscanf(p, "%4d-%2d-%2d %2d:%2d:%2d", &year, &mon, &dd, &hh, &mm, &ss);
        if (n != 6)
        {
            return false;
        }
    }
    if (n == 6) { cs = 0; }

    if (year < 0) return false;
    if (mon < 1 || mon > 12) return false;
    if (dd < 1) return false;
    if (hh < 0 || hh > 23) return false;
    if (mm < 0 || mm > 59) return false;
    if (ss < 0 || ss > 59) return false;
    if (cs < 0 || cs > 99) return false;

    /* 2자리 연도는 2000+YY로 해석 */
    if (year <= 99) { year = 2000 + year; }

    /* day 범위(월/윤년 반영) */
    {
        uint8_t dim = prv_days_in_month((uint16_t)year, (uint8_t)mon);
        if ((uint32_t)dd > (uint32_t)dim) return false;
    }

    dt.year  = (uint16_t)year;
    dt.month = (uint8_t)mon;
    dt.day   = (uint8_t)dd;
    dt.hour  = (uint8_t)hh;
    dt.min   = (uint8_t)mm;
    dt.sec   = (uint8_t)ss;
    dt.centi = (uint8_t)cs;

    uint32_t epoch_sec = UI_Time_Epoch2016_FromCalendar(&dt);
    uint64_t epoch_centi = (uint64_t)epoch_sec * 100u + (uint64_t)dt.centi;

    UI_Time_SetEpochCenti2016(epoch_centi);
    return true;
}


void UI_Time_SaveToBackupNow(void)
{
#if defined(HAL_RTC_MODULE_ENABLED)
    if (!s_time_valid)
    {
        return;
    }
    uint64_t now_centi = prv_now_centi_raw();
    uint32_t sec = (uint32_t)(now_centi / 100u);
    uint8_t  cs  = (uint8_t)(now_centi % 100u);
    prv_bkp_write(sec, cs);
    s_last_bkp_save_sec = sec;
#else
    (void)0;
#endif
}

void UI_Time_FormatNow(char* out, size_t out_size)
{
    UI_DateTime_t dt = {0};

    uint64_t now_centi = UI_Time_NowCenti2016();
    uint32_t now_sec   = (uint32_t)(now_centi / 100u);
    uint8_t  centi     = (uint8_t)(now_centi % 100u);

    UI_Time_Epoch2016_ToCalendar(now_sec, &dt);
    dt.centi = centi;

    /* TIME:YY-MM-DD hh:mm:ss.mm */
    uint16_t yy = (uint16_t)(dt.year % 100u);
    (void)snprintf(out, out_size,
                   "TIME:%02u-%02u-%02u %02u:%02u:%02u.%02u",
                   (unsigned)yy,
                   (unsigned)dt.month,
                   (unsigned)dt.day,
                   (unsigned)dt.hour,
                   (unsigned)dt.min,
                   (unsigned)dt.sec,
                   (unsigned)dt.centi);
}
