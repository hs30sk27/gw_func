/*
 * ui_cmd.h
 *
 * UART 내부명령 파서
 * - 입력: "<명령>CRLF"
 * - 출력: OK/ERROR + 필요한 경우 응답 문자열
 */

#ifndef UI_CMD_H
#define UI_CMD_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

void UI_Cmd_ProcessLine(const char* line);
void UI_Cmd_ProcessLineSilent(const char* line);

#ifdef __cplusplus
}
#endif

#endif /* UI_CMD_H */
