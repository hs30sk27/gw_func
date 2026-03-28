/*
 * ui_time.h
 *
 * Epoch(2016-01-01) 기반 시간 관리
 *
 * 요구사항 요약:
 *  - RTC가 EPOCH 기반이며, 기준점을 2016년으로 시작
 *  - TIME:YY-MM-DD hh:mm:ss.mm 명령으로 시간 설정
 *  - TIME CHECK 명령으로 현재 시간 리턴
 *  - 1/100초(centisecond) 단위 사용 (beacon 전송 시 0일 때)
 *
 * 구현 방식(간단화):
 *  - HW RTC Calendar를 직접 건드리지 않고,
 *    UTIL_TIMER_GetCurrentTime() (RTC 기반 ms 타임서버)로 시간을 유지.
 *  - TIME/BEACON 수신 시 epoch2016 기준 시간을 "기준점(base)"으로 설정.
 *
 * 장점:
 *  - Binary RTC / Calendar RTC 어떤 설정이든 영향 최소.
 *  - stop 모드에서도 RTC 기반 타이머가 유지되므로 시간 유지 가능.
 */

#ifndef UI_TIME_H
#define UI_TIME_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct
{
    uint16_t year; /* 4자리(예: 2026) */
    uint8_t  month;
    uint8_t  day;
    uint8_t  hour;
    uint8_t  min;
    uint8_t  sec;
    uint8_t  centi; /* 0..99 */
} UI_DateTime_t;

void     UI_Time_Init(void);

/* 현재 시간 (epoch2016, 1/100초) */
uint64_t UI_Time_NowCenti2016(void);
uint32_t UI_Time_NowSec2016(void);
uint8_t  UI_Time_NowCentiPart(void);

/* 시간 유효 여부(비콘/명령 등으로 세팅됐는지) */
bool     UI_Time_IsValid(void);

/* epoch2016 centi를 기준점으로 설정 */
void     UI_Time_SetEpochCenti2016(uint64_t epoch_centi);

/* TIME 명령 문자열 파싱/설정 (TIME:YY-MM-DD hh:mm:ss.mm) */
bool     UI_Time_SetFromString(const char* time_str);

/* 현재 시간을 "TIME:YY-MM-DD hh:mm:ss.mm" 형태로 포맷 */
void     UI_Time_FormatNow(char* out, size_t out_size);

/*
 * Reset(리셋) 시에도 시간을 유지하기 위해 RTC Backup Register에 현재 시간을 저장.
 *
 * - UI_Time_Init()에서 Backup Register에 저장된 시간이 있으면 자동 복원합니다.
 * - Stop 진입 직전/중요 구간에서 명시적으로 저장하고 싶을 때 호출하세요.
 */
void     UI_Time_SaveToBackupNow(void);

/* epoch2016(sec) <-> 달력 변환 */
uint32_t UI_Time_Epoch2016_FromCalendar(const UI_DateTime_t* dt);
void     UI_Time_Epoch2016_ToCalendar(uint32_t epoch_sec, UI_DateTime_t* out_dt);

#ifdef __cplusplus
}
#endif

#endif /* UI_TIME_H */
