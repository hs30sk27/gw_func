#include "ui_crc16.h"

uint16_t UI_CRC16_CCITT(const uint8_t* data, size_t len, uint16_t init)
{
    uint16_t crc = init;

    for (size_t i = 0; i < len; i++)
    {
        crc ^= (uint16_t)((uint16_t)data[i] << 8);

        for (uint8_t b = 0; b < 8u; b++)
        {
            if ((crc & 0x8000u) != 0u)
            {
                crc = (uint16_t)((crc << 1) ^ UI_CRC16_POLY);
            }
            else
            {
                crc = (uint16_t)(crc << 1);
            }
        }
    }
    return crc;
}
