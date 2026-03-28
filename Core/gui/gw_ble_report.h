#ifndef GW_BLE_REPORT_H
#define GW_BLE_REPORT_H

#include <stdbool.h>
#include "gw_storage.h"

#ifdef __cplusplus
extern "C" {
#endif

bool GW_BleReport_SendMinuteTestRecord(const GW_HourRec_t* rec);

#ifdef __cplusplus
}
#endif

#endif /* GW_BLE_REPORT_H */
