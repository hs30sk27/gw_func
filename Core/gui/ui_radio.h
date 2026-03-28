#ifndef UI_RADIO_H
#define UI_RADIO_H

#include <stdint.h>
#include <stdbool.h>

#include "subghz_phy_app.h"

#ifdef __cplusplus
extern "C" {
#endif

bool UI_Radio_PrepareTx(uint8_t payload_len);
bool UI_Radio_PrepareRx(uint8_t payload_len);
void UI_Radio_MarkRecoverNeeded(void);
void UI_Radio_ClearRecoverNeeded(void);
void UI_Radio_EnterSleep(void);

#ifdef __cplusplus
}
#endif

#endif /* UI_RADIO_H */
