#include "ui_fault.h"

volatile UI_FaultLog_t g_ui_fault_log = { 0u };

void UI_Fault_Init(void) {}
void UI_Fault_ClearRuntime(void) {}
void UI_Fault_Mark(const char* name, uint32_t line, uint32_t a0, uint32_t a1)
{
    (void)name;
    (void)line;
    (void)a0;
    (void)a1;
}
void UI_Fault_MarkCmd(const char* cmd) { (void)cmd; }
void UI_Fault_Checkpoint(uint32_t checkpoint_id, const char* name, uint32_t line, uint32_t a0, uint32_t a1)
{
    (void)checkpoint_id;
    (void)name;
    (void)line;
    (void)a0;
    (void)a1;
}
void UI_Fault_CaptureResetFlags(void) {}
void UI_Fault_CaptureHardFault(void) {}
void UI_Fault_CaptureHardFaultStack(uint32_t* sp, uint32_t exc_return)
{
    (void)sp;
    (void)exc_return;
}
void UI_Fault_HardFaultEntry(void) { while (1) {} }
void UI_Fault_Bp_ResetRecovered(void) {}
void UI_Fault_Bp_StopEnter(void) {}
void UI_Fault_Bp_BleBtOn(void) {}
void UI_Fault_Bp_BleUartInit(void) {}
void UI_Fault_Bp_BleAtReset(void) {}
void UI_Fault_Bp_UartError(void) {}
void UI_Fault_Bp_GwKeepalive(void) {}
void UI_Fault_Bp_GwWakeCb(void) {}
void UI_Fault_Bp_GwBeaconSend(void) {}
void UI_Fault_Bp_GwRxArm(void) {}
void UI_Fault_Bp_GwRxDone(void) {}
void UI_Fault_Bp_HardFault(void) {}
