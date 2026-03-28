#include "gw_ble_report.h"

#include "ui_types.h"
#include "ui_time.h"
#include "ui_uart.h"

#include <stdio.h>
#include <string.h>

static bool prv_node_valid(const GW_NodeRec_t* r)
{
    if (r == NULL)
    {
        return false;
    }

    if (r->batt_lvl != UI_NODE_BATT_LVL_INVALID) { return true; }
    if (r->temp_c != UI_NODE_TEMP_INVALID_C) { return true; }
    if (r->x != 0xFFFFu) { return true; }
    if (r->y != 0xFFFFu) { return true; }
    if (r->z != 0xFFFFu) { return true; }
    if (r->adc != 0xFFFFu) { return true; }
    if (r->pulse_cnt != 0xFFFFFFFFu) { return true; }
    return false;
}

static uint8_t prv_valid_node_count(const GW_HourRec_t* rec)
{
    uint8_t cnt = 0u;
    uint32_t i;

    for (i = 0u; i < UI_MAX_NODES; i++)
    {
        if (prv_node_valid(&rec->nodes[i]))
        {
            cnt++;
        }
    }
    return cnt;
}

static void prv_format_x10(char* out, size_t out_sz, int32_t v, bool invalid)
{
    if ((out == NULL) || (out_sz == 0u))
    {
        return;
    }

    if (invalid)
    {
        (void)snprintf(out, out_sz, "NA");
        return;
    }

    if (v < 0)
    {
        int32_t a = -v;
        (void)snprintf(out, out_sz, "-%ld.%01ld", (long)(a / 10), (long)(a % 10));
    }
    else
    {
        (void)snprintf(out, out_sz, "%ld.%01ld", (long)(v / 10), (long)(v % 10));
    }
}

static void prv_format_batt(char* out, size_t out_sz, uint8_t batt_lvl)
{
    if ((out == NULL) || (out_sz == 0u))
    {
        return;
    }

    if (batt_lvl == UI_NODE_BATT_LVL_INVALID)
    {
        (void)snprintf(out, out_sz, "NA");
        return;
    }

    (void)snprintf(out, out_sz, "%s",
                   (batt_lvl == UI_NODE_BATT_LVL_NORMAL) ? "Y" : "N");
}

static void prv_format_gw_batt_like_tcp(char* out, size_t out_sz, uint8_t gw_volt_x10)
{
    if ((out == NULL) || (out_sz == 0u))
    {
        return;
    }

    if (gw_volt_x10 == 0xFFu)
    {
        (void)snprintf(out, out_sz, "NA");
        return;
    }

    (void)snprintf(out, out_sz, "%s",
                   (gw_volt_x10 >= UI_NODE_BATT_LOW_THRESHOLD_X10) ? "3.5" : "LOW");
}

static void prv_format_temp_c(char* out, size_t out_sz, int8_t temp_c)
{
    if ((out == NULL) || (out_sz == 0u))
    {
        return;
    }

    if (temp_c == UI_NODE_TEMP_INVALID_C)
    {
        (void)snprintf(out, out_sz, "NA");
        return;
    }

    (void)snprintf(out, out_sz, "%d", (int)temp_c);
}

bool GW_BleReport_SendMinuteTestRecord(const GW_HourRec_t* rec)
{
    const UI_Config_t* cfg;
    UI_DateTime_t dt;
    char line[192];
    char vbuf[24];
    char tbuf[24];
    char bbuf[16];
    char ntbuf[16];
    uint8_t valid_cnt;
    uint32_t i;

    if (rec == NULL)
    {
        return false;
    }

    cfg = UI_GetConfig();
    UI_Time_Epoch2016_ToCalendar(rec->epoch_sec, &dt);
    valid_cnt = prv_valid_node_count(rec);

    (void)snprintf(line, sizeof(line),
                   "TEST50,GW:%u,NETID:%.*s,T:%04u-%02u-%02u %02u:%02u:%02u,NODES:%u\r\n",
                   (unsigned)cfg->gw_num,
                   (int)UI_NET_ID_LEN,
                   (const char*)cfg->net_id,
                   (unsigned)dt.year,
                   (unsigned)dt.month,
                   (unsigned)dt.day,
                   (unsigned)dt.hour,
                   (unsigned)dt.min,
                   (unsigned)dt.sec,
                   (unsigned)valid_cnt);
    UI_UART_SendString(line);

    prv_format_gw_batt_like_tcp(vbuf, sizeof(vbuf), rec->gw_volt_x10);
    prv_format_temp_c(tbuf, sizeof(tbuf), rec->gw_temp_c);

    (void)snprintf(line, sizeof(line),
                   "GW,V:%s,T:%s\r\n",
                   vbuf,
                   tbuf);
    UI_UART_SendString(line);

    for (i = 0u; i < UI_MAX_NODES; i++)
    {
        const GW_NodeRec_t* r = &rec->nodes[i];
        if (!prv_node_valid(r))
        {
            continue;
        }

        size_t len;
        bool icm_valid;
        prv_format_batt(bbuf, sizeof(bbuf), r->batt_lvl);
        prv_format_temp_c(ntbuf, sizeof(ntbuf), r->temp_c);

        len = (size_t)snprintf(line, sizeof(line),
                               "ND:%02lu,B:%s,T:%s",
                               (unsigned long)i,
                               bbuf,
                               ntbuf);

        icm_valid = ((r->x != 0xFFFFu) ||
                     (r->y != 0xFFFFu) ||
                     (r->z != 0xFFFFu));
        if ((len < sizeof(line)) && icm_valid)
        {
            len += (size_t)snprintf(line + len, sizeof(line) - len,
                                    ",X:%u,Y:%u,Z:%u",
                                    (unsigned)r->x,
                                    (unsigned)r->y,
                                    (unsigned)r->z);
        }
        if ((len < sizeof(line)) && (r->adc != 0xFFFFu))
        {
            len += (size_t)snprintf(line + len, sizeof(line) - len,
                                    ",ADC:%u",
                                    (unsigned)r->adc);
        }
        if ((len < sizeof(line)) && (r->pulse_cnt != 0xFFFFFFFFu))
        {
            len += (size_t)snprintf(line + len, sizeof(line) - len,
                                    ",PULSE:%lu",
                                    (unsigned long)r->pulse_cnt);
        }
        if (len < sizeof(line))
        {
            (void)snprintf(line + len, sizeof(line) - len, "\r\n");
        }
        else
        {
            line[sizeof(line) - 3u] = '\r';
            line[sizeof(line) - 2u] = '\n';
            line[sizeof(line) - 1u] = '\0';
        }
        UI_UART_SendString(line);
    }

    UI_UART_SendString("TEST50,END\r\n");
    return true;
}
