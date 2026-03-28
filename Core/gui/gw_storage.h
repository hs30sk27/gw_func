/*
 * gw_storage.h
 *
 * Gateway data save to W25Q128 + LittleFS
 *
 * - one file per day
 * - keep 60 days
 * - append binary records
 * - in 1-minute test mode, save the current minute record at second 50
 *
 * The code below is self-contained and keeps compile safety:
 * - if littlefs/lfs.h is not available, UI_USE_LITTLEFS becomes 0
 * - if W25Q128 low-level hooks are not implemented, save returns false
 */

#ifndef GW_STORAGE_H
#define GW_STORAGE_H

#include <stdint.h>
#include <stdbool.h>
#include "ui_conf.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct __attribute__((packed))
{
    uint8_t  batt_lvl;   /* 1=normal, 0=low, 0xFF=invalid */
    int8_t   temp_c;     /* -50..100'C, UI_NODE_TEMP_INVALID_C=invalid */
    uint16_t x;
    uint16_t y;
    uint16_t z;
    uint16_t adc;
    uint32_t pulse_cnt;
} GW_NodeRec_t;

typedef struct __attribute__((packed))
{
    uint8_t  gw_volt_x10;  /* 0.1V unit, 0xFF=invalid */
    int8_t   gw_temp_c;    /* 1'C unit, UI_NODE_TEMP_INVALID_C=invalid */
    uint32_t epoch_sec; /* epoch 2016 sec */
    GW_NodeRec_t nodes[UI_MAX_NODES];
} GW_HourRec_t;

typedef struct __attribute__((packed))
{
    uint8_t  net_id[UI_NET_ID_LEN];
    uint8_t  gw_num;
    uint8_t  rec_type;      /* 1 = periodic/test snapshot */
    uint32_t epoch_sec;
    GW_HourRec_t rec;
    uint16_t crc16;
} GW_FileRec_t;

void GW_Storage_Init(void);

#ifndef GW_LOC_LINE_MAX
#define GW_LOC_LINE_MAX (160u)
#endif

typedef struct __attribute__((packed))
{
    uint32_t saved_epoch_sec;
    char     line[GW_LOC_LINE_MAX];
} GW_LocRec_t;

typedef struct
{
    uint16_t list_index;
    char     name[64];
    uint32_t size;
    uint16_t rec_count;
    uint32_t first_epoch_sec;
    uint32_t last_epoch_sec;
} GW_StorageFileInfo_t;

typedef bool (*GW_StorageReadCb_t)(const GW_StorageFileInfo_t* info,
                                   const GW_FileRec_t* rec,
                                   uint32_t rec_index,
                                   void* user);

/* append one record to the current day file */
bool GW_Storage_SaveHourRec(const GW_HourRec_t* rec);

/* keep at most 60 days */
void GW_Storage_PurgeOldFiles(uint32_t now_epoch_sec);

/* file service for BLE commands */
uint16_t GW_Storage_ListFiles(GW_StorageFileInfo_t* out, uint16_t max_items);
bool GW_Storage_ReadAllFiles(GW_StorageReadCb_t cb, void* user);
bool GW_Storage_ReadFileByIndex(uint16_t list_index_1based, GW_StorageReadCb_t cb, void* user);
bool GW_Storage_DeleteAllFiles(void);
bool GW_Storage_DeleteFileByIndex(uint16_t list_index_1based);

/* saved GNSS location line */
bool GW_Storage_SaveLocRec(const GW_LocRec_t* rec);
bool GW_Storage_ReadLocRec(GW_LocRec_t* out_rec);

/* oldest-first global record view across all day files */
uint32_t GW_Storage_GetTotalRecordCount(void);
bool GW_Storage_ReadRecordByGlobalIndex(uint32_t rec_index_0based,
                                        GW_FileRec_t* out_rec,
                                        GW_StorageFileInfo_t* out_info);

/*
 * W25Q128 low-level hooks
 *
 * REV22 includes a real GW driver in `gw_w25q128.c`.
 * The declarations remain here because LittleFS block-device callbacks use them.
 * If a project needs a different flash driver, it can still override these symbols.
 */
bool GW_Storage_W25Q_PowerOn(void);
void GW_Storage_W25Q_PowerDown(void);
int  GW_Storage_W25Q_Read(uint32_t addr, void* buf, uint32_t size);
int  GW_Storage_W25Q_Prog(uint32_t addr, const void* buf, uint32_t size);
int  GW_Storage_W25Q_Erase4K(uint32_t addr);
int  GW_Storage_W25Q_Sync(void);

#ifdef __cplusplus
}
#endif

#endif /* GW_STORAGE_H */
