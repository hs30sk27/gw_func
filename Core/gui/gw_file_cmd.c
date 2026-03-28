#include "gw_file_cmd.h"
#include "gw_storage.h"
#include "ui_uart.h"
#include "ui_time.h"

#include <stdio.h>
#include <string.h>
#include <ctype.h>

#ifndef GW_FILE_CMD_MAX_FILES
#define GW_FILE_CMD_MAX_FILES  (96u)
#endif

typedef struct
{
    uint32_t file_index_0based;
    uint32_t rec_count;
    char line[192];
} GW_FileReadCtx_t;

static void prv_send(const char* s)
{
    if (s != NULL)
    {
        UI_UART_SendString(s);
    }
}

static void prv_dt(uint32_t epoch_sec, char* out, size_t out_sz)
{
    UI_DateTime_t dt;
    UI_Time_Epoch2016_ToCalendar(epoch_sec, &dt);
    (void)snprintf(out, out_sz,
                   "%04u-%02u-%02u %02u:%02u:%02u",
                   (unsigned)dt.year,
                   (unsigned)dt.month,
                   (unsigned)dt.day,
                   (unsigned)dt.hour,
                   (unsigned)dt.min,
                   (unsigned)dt.sec);
}

static void prv_fmt_x10(char* out, size_t out_sz, int32_t v, bool invalid)
{
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

static void prv_fmt_batt(char* out, size_t out_sz, uint8_t batt_lvl)
{
    if (batt_lvl == UI_NODE_BATT_LVL_INVALID)
    {
        (void)snprintf(out, out_sz, "NA");
        return;
    }
    (void)snprintf(out, out_sz, "%s",
                   (batt_lvl == UI_NODE_BATT_LVL_NORMAL) ? "Y" : "N");
}

static void prv_fmt_temp_c(char* out, size_t out_sz, int8_t temp_c)
{
    if (temp_c == UI_NODE_TEMP_INVALID_C)
    {
        (void)snprintf(out, out_sz, "NA");
        return;
    }
    (void)snprintf(out, out_sz, "%d", (int)temp_c);
}

static const char* prv_trim_arg(const char* arg, char* out, size_t out_sz)
{
    const char* s = arg;
    size_t n;
    if ((out == NULL) || (out_sz == 0u))
    {
        return NULL;
    }
    if (s == NULL)
    {
        out[0] = '\0';
        return out;
    }
    while (*s && isspace((unsigned char)*s)) { s++; }
    if (*s == '(') { s++; }
    while (*s && isspace((unsigned char)*s)) { s++; }
    (void)snprintf(out, out_sz, "%s", s);
    n = strlen(out);
    while (n > 0u)
    {
        char c = out[n - 1u];
        if ((c == ')') || (c == '\r') || (c == '\n') || isspace((unsigned char)c))
        {
            out[n - 1u] = '\0';
            n--;
        }
        else
        {
            break;
        }
    }
    return out;
}

static bool prv_rec_cb(const GW_StorageFileInfo_t* info,
                       const GW_FileRec_t* rec,
                       uint32_t rec_index,
                       void* user)
{
    GW_FileReadCtx_t* ctx = (GW_FileReadCtx_t*)user;
    char tbuf[32];
    char vbuf[24];
    char t2buf[24];
    char bbuf[16];
    char ntbuf[16];
    uint32_t i;

    if ((info == NULL) || (rec == NULL) || (ctx == NULL))
    {
        return false;
    }

    if (rec_index == 0u)
    {
        (void)snprintf(ctx->line, sizeof(ctx->line),
                       "FILE,INDEX:%lu,NAME:%s,SIZE:%lu,RECS:%u\r\n",
                       (unsigned long)((info->list_index != 0u) ? ((uint32_t)info->list_index - 1u) : ctx->file_index_0based),
                       info->name,
                       (unsigned long)info->size,
                       (unsigned)info->rec_count);
        prv_send(ctx->line);
    }

    prv_dt(rec->epoch_sec, tbuf, sizeof(tbuf));
    prv_fmt_x10(vbuf, sizeof(vbuf), (int32_t)rec->rec.gw_volt_x10, rec->rec.gw_volt_x10 == 0xFFu);
    prv_fmt_temp_c(t2buf, sizeof(t2buf), rec->rec.gw_temp_c);

    (void)snprintf(ctx->line, sizeof(ctx->line),
                   "REC:%lu,T:%s,GW:%u,NETID:%.*s,GWV:%s,GWT:%s\r\n",
                   (unsigned long)(rec_index + 1u),
                   tbuf,
                   (unsigned)rec->gw_num,
                   (int)UI_NET_ID_LEN,
                   (const char*)rec->net_id,
                   vbuf,
                   t2buf);
    prv_send(ctx->line);

    for (i = 0u; i < UI_MAX_NODES; i++)
    {
        const GW_NodeRec_t* nr = &rec->rec.nodes[i];
        bool valid = false;
        if (nr->batt_lvl != UI_NODE_BATT_LVL_INVALID) valid = true;
        if (nr->temp_c != UI_NODE_TEMP_INVALID_C) valid = true;
        if (nr->x != 0xFFFFu) valid = true;
        if (nr->y != 0xFFFFu) valid = true;
        if (nr->z != 0xFFFFu) valid = true;
        if (nr->adc != 0xFFFFu) valid = true;
        if (nr->pulse_cnt != 0xFFFFFFFFu) valid = true;
        if (!valid)
        {
            continue;
        }

        prv_fmt_batt(bbuf, sizeof(bbuf), nr->batt_lvl);
        prv_fmt_temp_c(ntbuf, sizeof(ntbuf), nr->temp_c);
        (void)snprintf(ctx->line, sizeof(ctx->line),
                       "ND:%02lu,B:%s,T:%s,X:%u,Y:%u,Z:%u,ADC:%u,PULSE:%lu\r\n",
                       (unsigned long)i,
                       bbuf,
                       ntbuf,
                       (unsigned)nr->x,
                       (unsigned)nr->y,
                       (unsigned)nr->z,
                       (unsigned)nr->adc,
                       (unsigned long)nr->pulse_cnt);
        prv_send(ctx->line);
    }

    ctx->rec_count++;
    return true;
}

bool GW_FileCmd_List(void)
{
    GW_StorageFileInfo_t items[GW_FILE_CMD_MAX_FILES];
    uint16_t cnt = GW_Storage_ListFiles(items, GW_FILE_CMD_MAX_FILES);
    uint16_t i;
    char line[192];
    char t0[32];
    char t1[32];

    (void)snprintf(line, sizeof(line), "FILE LIST,COUNT:%u\r\n", (unsigned)cnt);
    prv_send(line);

    for (i = 0u; i < cnt; i++)
    {
        prv_dt(items[i].first_epoch_sec, t0, sizeof(t0));
        prv_dt(items[i].last_epoch_sec, t1, sizeof(t1));
        (void)snprintf(line, sizeof(line),
                       "%u,NAME:%s,SIZE:%lu,RECS:%u,FROM:%s,TO:%s\r\n",
                       (unsigned)i,
                       items[i].name,
                       (unsigned long)items[i].size,
                       (unsigned)items[i].rec_count,
                       t0,
                       t1);
        prv_send(line);
    }
    return true;
}

bool GW_FileCmd_ReadArg(const char* arg)
{
    char tmp[32];
    const char* a = prv_trim_arg(arg, tmp, sizeof(tmp));

    if ((a == NULL) || (*a == '\0'))
    {
        return false;
    }

    if ((strcmp(a, "ALL") == 0) || (strcmp(a, "all") == 0))
    {
        GW_FileReadCtx_t ctx;
        memset(&ctx, 0, sizeof(ctx));
        if (!GW_Storage_ReadAllFiles(prv_rec_cb, &ctx))
        {
            return false;
        }
        prv_send("FILE READ,END\r\n");
        return true;
    }
    else
    {
        unsigned idx = 0u;
        GW_FileReadCtx_t ctx;
        if (sscanf(a, "%u", &idx) != 1)
        {
            return false;
        }
        memset(&ctx, 0, sizeof(ctx));
        if (idx >= 0xFFFFu)
        {
            return false;
        }

        ctx.file_index_0based = idx;
        if (!GW_Storage_ReadFileByIndex((uint16_t)(idx + 1u), prv_rec_cb, &ctx))
        {
            return false;
        }
        prv_send("FILE READ,END\r\n");
        return true;
    }
}

bool GW_FileCmd_DeleteArg(const char* arg)
{
    char tmp[32];
    const char* a = prv_trim_arg(arg, tmp, sizeof(tmp));
    char line[96];

    if ((a == NULL) || (*a == '\0'))
    {
        return false;
    }

    if ((strcmp(a, "ALL") == 0) || (strcmp(a, "all") == 0))
    {
        if (!GW_Storage_DeleteAllFiles())
        {
            return false;
        }
        prv_send("FILE DEL,ALL\r\n");
        return true;
    }
    else
    {
        unsigned idx = 0u;
        if (sscanf(a, "%u", &idx) != 1)
        {
            return false;
        }
        if (idx >= 0xFFFFu)
        {
            return false;
        }

        if (!GW_Storage_DeleteFileByIndex((uint16_t)(idx + 1u)))
        {
            return false;
        }
        (void)snprintf(line, sizeof(line), "FILE DEL,INDEX:%u\r\n", idx);
        prv_send(line);
        return true;
    }
}
