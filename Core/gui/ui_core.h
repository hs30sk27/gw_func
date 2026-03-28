/*
 * ui_core.h
 *
 * Core/ui 모듈의 엔트리 포인트
 * - UI_Init() 한 번 호출하면 UART/KEY/BLE/시간/저전력 유틸이 동작합니다.
 */

#ifndef UI_CORE_H
#define UI_CORE_H

#ifdef __cplusplus
extern "C" {
#endif

void UI_Init(void);

/*
 * Stop mode 진입 직전에 호출(선택)
 *  - UART 명령 파서/임시 버퍼 등 "런타임 플래그"를 정리하여
 *    다음 wake-up 이후 동작이 꼬이지 않도록 합니다.
 */
void UI_Core_ClearFlagsBeforeStop(void);

#ifdef __cplusplus
}
#endif

#endif /* UI_CORE_H */
