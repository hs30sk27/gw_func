#ifndef UI_FAULT_H
#define UI_FAULT_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef enum
{
    UI_CP_NONE             = 0x0000u,
    UI_CP_BOOT_RECOVERED   = 0x0101u,
    UI_CP_BOOT_INIT_BEGIN  = 0x0102u,
    UI_CP_BOOT_INIT_DONE   = 0x0103u,
    UI_CP_STOP_PRE         = 0x0110u,
    UI_CP_STOP_SAVE        = 0x0111u,
    UI_CP_BLE_ENABLE       = 0x0201u,
    UI_CP_BLE_UART_INIT    = 0x0202u,
    UI_CP_BLE_AT_RESET     = 0x0203u,
    UI_CP_BLE_DISABLE      = 0x0204u,
    UI_CP_BLE_TIMEOUT      = 0x0205u,
    UI_CP_UART_ERROR       = 0x0210u,
    UI_CP_GW_INIT          = 0x0301u,
    UI_CP_GW_KEEPALIVE     = 0x0302u,
    UI_CP_GW_WAKE_CB       = 0x0303u,
    UI_CP_GW_BEACON_SEND   = 0x0304u,
    UI_CP_GW_RX_ARM        = 0x0305u,
    UI_CP_GW_RX_DONE       = 0x0306u,
    UI_CP_HARDFAULT        = 0x0F01u,
} UI_FaultCheckpointId_t;

typedef struct
{
    uint32_t reserved;
} UI_FaultLog_t;

extern volatile UI_FaultLog_t g_ui_fault_log;

void UI_Fault_Init(void);
void UI_Fault_ClearRuntime(void);
void UI_Fault_Mark(const char* name, uint32_t line, uint32_t a0, uint32_t a1);
void UI_Fault_MarkCmd(const char* cmd);
void UI_Fault_Checkpoint(uint32_t checkpoint_id, const char* name, uint32_t line, uint32_t a0, uint32_t a1);
void UI_Fault_CaptureResetFlags(void);
void UI_Fault_CaptureHardFault(void);
void UI_Fault_CaptureHardFaultStack(uint32_t* sp, uint32_t exc_return);
void UI_Fault_HardFaultEntry(void);

void UI_Fault_Bp_ResetRecovered(void);
void UI_Fault_Bp_StopEnter(void);
void UI_Fault_Bp_BleBtOn(void);
void UI_Fault_Bp_BleUartInit(void);
void UI_Fault_Bp_BleAtReset(void);
void UI_Fault_Bp_UartError(void);
void UI_Fault_Bp_GwKeepalive(void);
void UI_Fault_Bp_GwWakeCb(void);
void UI_Fault_Bp_GwBeaconSend(void);
void UI_Fault_Bp_GwRxArm(void);
void UI_Fault_Bp_GwRxDone(void);
void UI_Fault_Bp_HardFault(void);

#define UI_FAULT_MARK(name, a0, a1)       ((void)0)
#define UI_FAULT_CP(id, name, a0, a1)     ((void)0)

#ifdef __cplusplus
}
#endif

#endif /* UI_FAULT_H */
