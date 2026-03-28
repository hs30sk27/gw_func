/*
 * ui_rf_plan_kr920.h
 *
 * KR(920MHz) 주파수 테이블 + 비콘/데이터 주파수 선택
 *
 * 요구사항 요약:
 *  - KR 주파수 사용
 *  - 혼선 채널은 사용하지 않음 -> 테이블에서 제외
 *  - 비콘 주파수: 테이블 중간 주파수 고정
 *  - 데이터 주파수: 시간(주기)마다 호핑
 *
 * NOTE:
 *  - 실제 현장 간섭 상황에 따라 테이블을 조정하세요.
 *  - 본 기본 테이블은 922.1~923.3MHz 200kHz step 7채널(상위 대역)만 사용합니다.
 */

#ifndef UI_RF_PLAN_KR920_H
#define UI_RF_PLAN_KR920_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* 비콘 주파수(Hz) */
uint32_t UI_RF_GetBeaconFreqHz(void);

/* 데이터 채널 개수 */
uint8_t  UI_RF_GetDataChannelCount(void);

/*
 * 데이터 주파수 선택
 * - epoch_sec: epoch2016 seconds
 * - hop_period_sec: 3600(정상) 또는 60(테스트)
 * - seed: GW 번호/Node 번호 등을 넣어서 약간의 분산을 줄 수 있음 (0..255)
 */
uint32_t UI_RF_GetDataFreqHz(uint32_t epoch_sec, uint32_t hop_period_sec, uint8_t seed);

#ifdef __cplusplus
}
#endif

#endif /* UI_RF_PLAN_KR920_H */
