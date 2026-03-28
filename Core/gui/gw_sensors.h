/*
 * gw_sensors.h
 *
 * Gateway 내부 센서(내부 온도 + batt level) 측정 모듈
 *
 * 요구사항:
 *  - GW도 내부 온도 체크 필요(누락 보완)
 *  - batt level은 ADC가 아니라 BATT_LVL GPIO 입력으로 판정
 *  - 기존 저장 구조(gw_volt_x10)와의 호환을 위해 batt level을 대표 x10 값으로 매핑
 *  - 온도만 내부 ADC(hadc) 사용
 */

#ifndef GW_SENSORS_H
#define GW_SENSORS_H

#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * GW 내부 값 측정
 *  - volt_x10:
 *      BATT_LVL GPIO를 legacy gw_volt_x10 저장 포맷과 호환되도록
 *      대표값(x10)으로 반환
 *      예) normal=UI_GW_BATT_GPIO_NORMAL_REPR_X10, low=UI_GW_BATT_GPIO_LOW_REPR_X10
 *  - temp_x10: 0.1'C 단위 (예: 253 => 25.3'C)
 *
 * 성공 시 true, 실패 시 false.
 * (배터리와 온도 중 하나라도 유효하게 읽으면 true)
 */
bool GW_Sensors_MeasureGw(uint16_t* volt_x10, int16_t* temp_x10);

#ifdef __cplusplus
}
#endif

#endif /* GW_SENSORS_H */
