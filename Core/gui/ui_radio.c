#include "ui_radio.h"

#include <stddef.h>

#include "radio.h"
#include "subghz_phy_app.h"
#include "main.h"

#ifndef UI_RF_LORA_BW_ENUM
#define UI_RF_LORA_BW_ENUM            (0u)
#endif
#ifndef UI_RF_LORA_SF
#define UI_RF_LORA_SF                 (11u)
#endif
#ifndef UI_RF_LORA_CR
#define UI_RF_LORA_CR                 (1u)
#endif
#ifndef UI_RF_LORA_PREAMBLE_LEN
#define UI_RF_LORA_PREAMBLE_LEN       (8u)
#endif
#ifndef UI_RF_LORA_SYMBOL_TIMEOUT
#define UI_RF_LORA_SYMBOL_TIMEOUT     (0u)
#endif
#ifndef UI_RF_LORA_FIX_LEN
#define UI_RF_LORA_FIX_LEN            (false)
#endif
#ifndef UI_RF_LORA_CRC_ON
#define UI_RF_LORA_CRC_ON             (true)
#endif
#ifndef UI_RF_LORA_FREQ_HOP_ON
#define UI_RF_LORA_FREQ_HOP_ON        (false)
#endif
#ifndef UI_RF_LORA_HOP_PERIOD
#define UI_RF_LORA_HOP_PERIOD         (0u)
#endif
#ifndef UI_RF_LORA_IQ_INVERTED
#define UI_RF_LORA_IQ_INVERTED        (false)
#endif
#ifndef UI_RF_LORA_RX_CONTINUOUS
#define UI_RF_LORA_RX_CONTINUOUS      (false)
#endif
#ifndef UI_RF_TX_OUTPUT_POWER_DBM
#define UI_RF_TX_OUTPUT_POWER_DBM     (22)
#endif
#ifndef UI_RF_TX_TIMEOUT_MS
#define UI_RF_TX_TIMEOUT_MS           (4000u)
#endif

static bool s_recover_needed = true;

static bool prv_prepare_common(uint8_t payload_len)
{
    if ((Radio.SetChannel == NULL) || (Radio.Sleep == NULL))
    {
        return false;
    }

    if (s_recover_needed)
    {
        SubghzApp_ReInitRadio();
        s_recover_needed = false;
    }

    if (Radio.SetModem != NULL)
    {
        Radio.SetModem(MODEM_LORA);
    }
    if (Radio.SetPublicNetwork != NULL)
    {
        Radio.SetPublicNetwork(false);
    }
    if (Radio.SetMaxPayloadLength != NULL)
    {
        Radio.SetMaxPayloadLength(MODEM_LORA, payload_len);
    }
    return true;
}

bool UI_Radio_PrepareTx(uint8_t payload_len)
{
    if ((Radio.Send == NULL) || (Radio.SetTxConfig == NULL))
    {
        return false;
    }
    if (!prv_prepare_common(payload_len))
    {
        return false;
    }
    Radio.SetTxConfig(MODEM_LORA,
                      UI_RF_TX_OUTPUT_POWER_DBM,
                      0u,
                      UI_RF_LORA_BW_ENUM,
                      UI_RF_LORA_SF,
                      UI_RF_LORA_CR,
                      UI_RF_LORA_PREAMBLE_LEN,
                      UI_RF_LORA_FIX_LEN,
                      UI_RF_LORA_CRC_ON,
                      UI_RF_LORA_FREQ_HOP_ON,
                      UI_RF_LORA_HOP_PERIOD,
                      UI_RF_LORA_IQ_INVERTED,
                      UI_RF_TX_TIMEOUT_MS);
    return true;
}

bool UI_Radio_PrepareRx(uint8_t payload_len)
{
    if ((Radio.Rx == NULL) || (Radio.SetRxConfig == NULL))
    {
        return false;
    }
    if (!prv_prepare_common(payload_len))
    {
        return false;
    }

    Radio.SetRxConfig(MODEM_LORA,
                      UI_RF_LORA_BW_ENUM,
                      UI_RF_LORA_SF,
                      UI_RF_LORA_CR,
                      0u,
                      UI_RF_LORA_PREAMBLE_LEN,
                      UI_RF_LORA_SYMBOL_TIMEOUT,
                      UI_RF_LORA_FIX_LEN,
                      payload_len,
                      UI_RF_LORA_CRC_ON,
                      UI_RF_LORA_FREQ_HOP_ON,
                      UI_RF_LORA_HOP_PERIOD,
                      UI_RF_LORA_IQ_INVERTED,
                      UI_RF_LORA_RX_CONTINUOUS);
    return true;
}

void UI_Radio_MarkRecoverNeeded(void)
{
    s_recover_needed = true;
}

void UI_Radio_ClearRecoverNeeded(void)
{
    s_recover_needed = false;
}

void UI_Radio_EnterSleep(void)
{
/*    if (Radio.Sleep != NULL) */{
        Radio.Sleep();
    }
}
