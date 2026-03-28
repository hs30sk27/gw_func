#include "gw_storage.h"

#include "ui_crc16.h"
#include "ui_time.h"
#include "ui_types.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#if !defined(UI_USE_LITTLEFS)
# if defined(__has_include)
#  if __has_include("lfs.h")
#   define UI_USE_LITTLEFS (1)
#  else
#   define UI_USE_LITTLEFS (0)
#  endif
# else
#  define UI_USE_LITTLEFS (0)
# endif
#endif

#if (UI_USE_LITTLEFS == 1)
#include "lfs.h"
#endif

#ifndef UI_W25Q128_READ_SIZE
#define UI_W25Q128_READ_SIZE        (16u)
#endif
#ifndef UI_W25Q128_PROG_SIZE
#define UI_W25Q128_PROG_SIZE        (256u)
#endif
#ifndef UI_W25Q128_BLOCK_SIZE
#define UI_W25Q128_BLOCK_SIZE       (4096u)
#endif
#ifndef UI_W25Q128_BLOCK_COUNT
#define UI_W25Q128_BLOCK_COUNT      (4096u)
#endif
#ifndef UI_W25Q128_CACHE_SIZE
#define UI_W25Q128_CACHE_SIZE       (256u)
#endif
#ifndef UI_W25Q128_LOOKAHEAD_SIZE
#define UI_W25Q128_LOOKAHEAD_SIZE   (32u)
#endif
#ifndef UI_W25Q128_BLOCK_CYCLES
#define UI_W25Q128_BLOCK_CYCLES     (500u)
#endif
#ifndef GW_STORAGE_MAX_FILES
#define GW_STORAGE_MAX_FILES        (96u)
#endif

#ifndef GW_STORAGE_LAYOUT_TAG
#define GW_STORAGE_LAYOUT_TAG       "_r2"
#endif

#if (UI_USE_LITTLEFS == 1)
static lfs_t s_lfs;
static uint8_t s_lfs_read_buf[UI_W25Q128_CACHE_SIZE];
static uint8_t s_lfs_prog_buf[UI_W25Q128_CACHE_SIZE];
static uint8_t s_lfs_lookahead_buf[UI_W25Q128_LOOKAHEAD_SIZE];

static int prv_bd_read(const struct lfs_config *c,
                       lfs_block_t block,
                       lfs_off_t off,
                       void *buffer,
                       lfs_size_t size)
{
    uint32_t addr = ((uint32_t)block * (uint32_t)c->block_size) + (uint32_t)off;
    return GW_Storage_W25Q_Read(addr, buffer, (uint32_t)size);
}

static int prv_bd_prog(const struct lfs_config *c,
                       lfs_block_t block,
                       lfs_off_t off,
                       const void *buffer,
                       lfs_size_t size)
{
    uint32_t addr = ((uint32_t)block * (uint32_t)c->block_size) + (uint32_t)off;
    return GW_Storage_W25Q_Prog(addr, buffer, (uint32_t)size);
}

static int prv_bd_erase(const struct lfs_config *c, lfs_block_t block)
{
    uint32_t addr = (uint32_t)block * (uint32_t)c->block_size;
    (void)c;
    return GW_Storage_W25Q_Erase4K(addr);
}

static int prv_bd_sync(const struct lfs_config *c)
{
    (void)c;
    return GW_Storage_W25Q_Sync();
}

static const struct lfs_config s_lfs_cfg = {
    .read = prv_bd_read,
    .prog = prv_bd_prog,
    .erase = prv_bd_erase,
    .sync = prv_bd_sync,
    .read_size = UI_W25Q128_READ_SIZE,
    .prog_size = UI_W25Q128_PROG_SIZE,
    .block_size = UI_W25Q128_BLOCK_SIZE,
    .block_count = UI_W25Q128_BLOCK_COUNT,
    .block_cycles = UI_W25Q128_BLOCK_CYCLES,
    .cache_size = UI_W25Q128_CACHE_SIZE,
    .lookahead_size = UI_W25Q128_LOOKAHEAD_SIZE,
    .read_buffer = s_lfs_read_buf,
    .prog_buffer = s_lfs_prog_buf,
    .lookahead_buffer = s_lfs_lookahead_buf,
    .name_max = 64,
    .file_max = 0,
    .attr_max = 0,
};
#endif

__attribute__((weak)) bool GW_Storage_W25Q_PowerOn(void)
{
    return false;
}

__attribute__((weak)) void GW_Storage_W25Q_PowerDown(void)
{
}

__attribute__((weak)) int GW_Storage_W25Q_Read(uint32_t addr, void* buf, uint32_t size)
{
    (void)addr;
    (void)buf;
    (void)size;
    return -1;
}

__attribute__((weak)) int GW_Storage_W25Q_Prog(uint32_t addr, const void* buf, uint32_t size)
{
    (void)addr;
    (void)buf;
    (void)size;
    return -1;
}

__attribute__((weak)) int GW_Storage_W25Q_Erase4K(uint32_t addr)
{
    (void)addr;
    return -1;
}

__attribute__((weak)) int GW_Storage_W25Q_Sync(void)
{
    return 0;
}

static void prv_make_day_path(uint32_t epoch_sec, char* out, size_t out_sz)
{
    UI_DateTime_t dt;
    UI_Time_Epoch2016_ToCalendar(epoch_sec, &dt);
    (void)snprintf(out,
                   out_sz,
                   "/%04u%02u%02u%s.gwd",
                   (unsigned)dt.year,
                   (unsigned)dt.month,
                   (unsigned)dt.day,
                   GW_STORAGE_LAYOUT_TAG);
}

static int32_t prv_day_index_from_name(const char* name)
{
    UI_DateTime_t dt;
    int y, m, d;

    if ((name == NULL) || (strlen(name) < 8u))
    {
        return -1;
    }

    if (sscanf(name, "%4d%2d%2d", &y, &m, &d) != 3)
    {
        return -1;
    }

    dt.year = (uint16_t)y;
    dt.month = (uint8_t)m;
    dt.day = (uint8_t)d;
    dt.hour = 0u;
    dt.min = 0u;
    dt.sec = 0u;
    dt.centi = 0u;
    return (int32_t)(UI_Time_Epoch2016_FromCalendar(&dt) / 86400u);
}

static int32_t prv_day_index_from_epoch(uint32_t epoch_sec)
{
    return (int32_t)(epoch_sec / 86400u);
}

#if (UI_USE_LITTLEFS == 1)
static bool prv_mount_fs(bool allow_format)
{
    int rc;

    if (!GW_Storage_W25Q_PowerOn())
    {
        return false;
    }

    rc = lfs_mount(&s_lfs, &s_lfs_cfg);
    if ((rc != 0) && allow_format)
    {
        rc = lfs_format(&s_lfs, &s_lfs_cfg);
        if (rc == 0)
        {
            rc = lfs_mount(&s_lfs, &s_lfs_cfg);
        }
    }
    if (rc != 0)
    {
        GW_Storage_W25Q_PowerDown();
        return false;
    }
    return true;
}

static void prv_unmount_fs(void)
{
    (void)lfs_unmount(&s_lfs);
    GW_Storage_W25Q_PowerDown();
}

static bool prv_is_gwd_name(const char* name)
{
    size_t n;
    if (name == NULL)
    {
        return false;
    }
    n = strlen(name);
    if (n < 5u)
    {
        return false;
    }
    return (strcmp(&name[n - 4u], ".gwd") == 0);
}

static int prv_cmp_file_desc(const void* a, const void* b)
{
    const GW_StorageFileInfo_t* fa = (const GW_StorageFileInfo_t*)a;
    const GW_StorageFileInfo_t* fb = (const GW_StorageFileInfo_t*)b;
    return strcmp(fb->name, fa->name); /* newest date file first */
}

static void prv_fill_file_bounds(GW_StorageFileInfo_t* info)
{
    char path[80];
    lfs_file_t file;
    GW_FileRec_t rec;

    if ((info == NULL) || (info->size < sizeof(GW_FileRec_t)) || (info->rec_count == 0u))
    {
        return;
    }

    (void)snprintf(path, sizeof(path), "/%s", info->name);
    if (lfs_file_open(&s_lfs, &file, path, LFS_O_RDONLY) != 0)
    {
        return;
    }

    if (lfs_file_read(&s_lfs, &file, &rec, sizeof(rec)) == (lfs_ssize_t)sizeof(rec))
    {
        info->first_epoch_sec = rec.epoch_sec;
    }
    if (info->size >= sizeof(rec))
    {
        (void)lfs_file_seek(&s_lfs, &file, (lfs_soff_t)(info->size - sizeof(rec)), LFS_SEEK_SET);
        if (lfs_file_read(&s_lfs, &file, &rec, sizeof(rec)) == (lfs_ssize_t)sizeof(rec))
        {
            info->last_epoch_sec = rec.epoch_sec;
        }
    }

    (void)lfs_file_close(&s_lfs, &file);
}

static uint16_t prv_collect_files(GW_StorageFileInfo_t* out, uint16_t max_items)
{
    lfs_dir_t dir;
    struct lfs_info info;
    uint16_t cnt = 0u;

    if (lfs_dir_open(&s_lfs, &dir, "/") != 0)
    {
        return 0u;
    }

    while (lfs_dir_read(&s_lfs, &dir, &info) > 0)
    {
        if ((info.type != LFS_TYPE_REG) || !prv_is_gwd_name(info.name))
        {
            continue;
        }
        if ((out != NULL) && (cnt < max_items))
        {
            memset(&out[cnt], 0, sizeof(out[cnt]));
            (void)snprintf(out[cnt].name, sizeof(out[cnt].name), "%s", info.name);
            out[cnt].size = info.size;
            if ((info.size < sizeof(GW_FileRec_t)) || ((info.size % sizeof(GW_FileRec_t)) != 0u))
            {
                out[cnt].rec_count = 0u;
            }
            else
            {
                out[cnt].rec_count = (uint16_t)(info.size / sizeof(GW_FileRec_t));
            }
            prv_fill_file_bounds(&out[cnt]);
        }
        cnt++;
    }

    (void)lfs_dir_close(&s_lfs, &dir);

    if ((out != NULL))
    {
        uint16_t n = (cnt > max_items) ? max_items : cnt;
        if (n > 1u)
        {
            qsort(out, n, sizeof(out[0]), prv_cmp_file_desc);
        }
        for (uint16_t i = 0u; i < n; i++)
        {
            out[i].list_index = (uint16_t)(i + 1u);
        }
    }

    return cnt;
}

static bool prv_get_file_by_index(uint16_t idx1, GW_StorageFileInfo_t* out)
{
    GW_StorageFileInfo_t items[GW_STORAGE_MAX_FILES];
    uint16_t cnt = prv_collect_files(items, GW_STORAGE_MAX_FILES);

    if ((idx1 == 0u) || (idx1 > cnt) || (idx1 > GW_STORAGE_MAX_FILES))
    {
        return false;
    }
    if (out != NULL)
    {
        *out = items[idx1 - 1u];
    }
    return true;
}

static bool prv_read_record_from_file(const GW_StorageFileInfo_t* info,
                                      uint32_t rec_index,
                                      GW_FileRec_t* out_rec)
{
    char path[80];
    lfs_file_t file;
    lfs_soff_t off;

    if ((info == NULL) || (out_rec == NULL) || (rec_index >= info->rec_count))
    {
        return false;
    }

    (void)snprintf(path, sizeof(path), "/%s", info->name);
    if (lfs_file_open(&s_lfs, &file, path, LFS_O_RDONLY) != 0)
    {
        return false;
    }

    off = (lfs_soff_t)((lfs_soff_t)rec_index * (lfs_soff_t)sizeof(GW_FileRec_t));
    if (lfs_file_seek(&s_lfs, &file, off, LFS_SEEK_SET) < 0)
    {
        (void)lfs_file_close(&s_lfs, &file);
        return false;
    }

    if (lfs_file_read(&s_lfs, &file, out_rec, sizeof(*out_rec)) != (lfs_ssize_t)sizeof(*out_rec))
    {
        (void)lfs_file_close(&s_lfs, &file);
        return false;
    }

    (void)lfs_file_close(&s_lfs, &file);

    {
        uint16_t crc = UI_CRC16_CCITT((const uint8_t*)out_rec,
                                      sizeof(*out_rec) - sizeof(out_rec->crc16),
                                      UI_CRC16_INIT);
        if (crc != out_rec->crc16)
        {
            return false;
        }
    }

    return true;
}

static bool prv_stream_file(const GW_StorageFileInfo_t* info, uint32_t display_index, GW_StorageReadCb_t cb, void* user)
{
    char path[80];
    lfs_file_t file;
    uint32_t rec_idx = 0u;
    GW_FileRec_t rec;

    if ((info == NULL) || (cb == NULL))
    {
        return false;
    }

    (void)display_index;
    (void)snprintf(path, sizeof(path), "/%s", info->name);
    if (lfs_file_open(&s_lfs, &file, path, LFS_O_RDONLY) != 0)
    {
        return false;
    }

    while (lfs_file_read(&s_lfs, &file, &rec, sizeof(rec)) == (lfs_ssize_t)sizeof(rec))
    {
        uint16_t crc = UI_CRC16_CCITT((const uint8_t*)&rec,
                                      sizeof(rec) - sizeof(rec.crc16),
                                      UI_CRC16_INIT);
        if (crc != rec.crc16)
        {
            continue;
        }
        if (!cb(info, &rec, rec_idx, user))
        {
            (void)lfs_file_close(&s_lfs, &file);
            return false;
        }
        rec_idx++;
    }

    (void)lfs_file_close(&s_lfs, &file);
    return true;
}
#endif

void GW_Storage_Init(void)
{
#if (UI_USE_LITTLEFS == 1)
    if (!prv_mount_fs(true))
    {
        return;
    }
    prv_unmount_fs();
#endif
}

bool GW_Storage_SaveHourRec(const GW_HourRec_t* rec)
{
#if (UI_USE_LITTLEFS == 1)
    const UI_Config_t* cfg;
    GW_FileRec_t file_rec;
    lfs_file_t file;
    char path[32];
    int rc = -1;

    if (rec == NULL)
    {
        return false;
    }
    if (!prv_mount_fs(true))
    {
        return false;
    }

    cfg = UI_GetConfig();
    memset(&file_rec, 0xFF, sizeof(file_rec));
    memcpy(file_rec.net_id, cfg->net_id, UI_NET_ID_LEN);
    file_rec.gw_num = cfg->gw_num;
    file_rec.rec_type = 1u;
    file_rec.epoch_sec = rec->epoch_sec;
    memcpy(&file_rec.rec, rec, sizeof(*rec));
    file_rec.crc16 = UI_CRC16_CCITT((const uint8_t*)&file_rec,
                                    sizeof(file_rec) - sizeof(file_rec.crc16),
                                    UI_CRC16_INIT);

    prv_make_day_path(rec->epoch_sec, path, sizeof(path));

    rc = lfs_file_open(&s_lfs, &file, path, LFS_O_WRONLY | LFS_O_CREAT | LFS_O_APPEND);
    if (rc == 0)
    {
        lfs_ssize_t wr = lfs_file_write(&s_lfs, &file, &file_rec, sizeof(file_rec));
        rc = (wr == (lfs_ssize_t)sizeof(file_rec)) ? 0 : -1;
        (void)lfs_file_sync(&s_lfs, &file);
        (void)lfs_file_close(&s_lfs, &file);
    }

    prv_unmount_fs();
    return (rc == 0);
#else
    (void)rec;
    return false;
#endif
}

void GW_Storage_PurgeOldFiles(uint32_t now_epoch_sec)
{
#if (UI_USE_LITTLEFS == 1)
    lfs_dir_t dir;
    struct lfs_info info;
    int32_t now_day_idx;

    if (!prv_mount_fs(false))
    {
        return;
    }

    now_day_idx = prv_day_index_from_epoch(now_epoch_sec);
    if (lfs_dir_open(&s_lfs, &dir, "/") == 0)
    {
        while (lfs_dir_read(&s_lfs, &dir, &info) > 0)
        {
            int32_t day_idx;
            if (info.type != LFS_TYPE_REG)
            {
                continue;
            }
            day_idx = prv_day_index_from_name(info.name);
            if (day_idx < 0)
            {
                continue;
            }
            if ((now_day_idx - day_idx) > 60)
            {
                char path[80];
                (void)snprintf(path, sizeof(path), "/%s", info.name);
                (void)lfs_remove(&s_lfs, path);
            }
        }
        (void)lfs_dir_close(&s_lfs, &dir);
    }

    prv_unmount_fs();
#else
    (void)now_epoch_sec;
#endif
}

uint16_t GW_Storage_ListFiles(GW_StorageFileInfo_t* out, uint16_t max_items)
{
#if (UI_USE_LITTLEFS == 1)
    uint16_t cnt;
    if (!prv_mount_fs(false))
    {
        return 0u;
    }
    cnt = prv_collect_files(out, max_items);
    prv_unmount_fs();
    return (cnt > max_items) ? max_items : cnt;
#else
    (void)out;
    (void)max_items;
    return 0u;
#endif
}



bool GW_Storage_SaveLocRec(const GW_LocRec_t* rec)
{
#if (UI_USE_LITTLEFS == 1)
    lfs_file_t file;
    int rc = -1;

    if (rec == NULL)
    {
        return false;
    }
    if (!prv_mount_fs(true))
    {
        return false;
    }

    rc = lfs_file_open(&s_lfs, &file, "/loc.dat", LFS_O_WRONLY | LFS_O_CREAT | LFS_O_TRUNC);
    if (rc == 0)
    {
        lfs_ssize_t wr = lfs_file_write(&s_lfs, &file, rec, sizeof(*rec));
        rc = (wr == (lfs_ssize_t)sizeof(*rec)) ? 0 : -1;
        (void)lfs_file_sync(&s_lfs, &file);
        (void)lfs_file_close(&s_lfs, &file);
    }

    prv_unmount_fs();
    return (rc == 0);
#else
    (void)rec;
    return false;
#endif
}

bool GW_Storage_ReadLocRec(GW_LocRec_t* out_rec)
{
#if (UI_USE_LITTLEFS == 1)
    lfs_file_t file;
    bool ok = false;

    if (out_rec == NULL)
    {
        return false;
    }
    memset(out_rec, 0, sizeof(*out_rec));

    if (!prv_mount_fs(false))
    {
        return false;
    }

    if (lfs_file_open(&s_lfs, &file, "/loc.dat", LFS_O_RDONLY) == 0)
    {
        lfs_ssize_t rd = lfs_file_read(&s_lfs, &file, out_rec, sizeof(*out_rec));
        ok = (rd == (lfs_ssize_t)sizeof(*out_rec));
        (void)lfs_file_close(&s_lfs, &file);
    }

    prv_unmount_fs();
    return ok;
#else
    (void)out_rec;
    return false;
#endif
}

uint32_t GW_Storage_GetTotalRecordCount(void)
{
#if (UI_USE_LITTLEFS == 1)
    GW_StorageFileInfo_t items[GW_STORAGE_MAX_FILES];
    uint16_t cnt;
    uint32_t total = 0u;
    uint16_t i;

    if (!prv_mount_fs(false))
    {
        return 0u;
    }

    cnt = prv_collect_files(items, GW_STORAGE_MAX_FILES);
    for (i = 0u; (i < cnt) && (i < GW_STORAGE_MAX_FILES); i++)
    {
        total += items[i].rec_count;
    }

    prv_unmount_fs();
    return total;
#else
    return 0u;
#endif
}

bool GW_Storage_ReadRecordByGlobalIndex(uint32_t rec_index_0based,
                                        GW_FileRec_t* out_rec,
                                        GW_StorageFileInfo_t* out_info)
{
#if (UI_USE_LITTLEFS == 1)
    GW_StorageFileInfo_t items[GW_STORAGE_MAX_FILES];
    uint16_t cnt;
    int32_t i;
    uint32_t remain = rec_index_0based;

    if (out_rec == NULL)
    {
        return false;
    }

    if (!prv_mount_fs(false))
    {
        return false;
    }

    cnt = prv_collect_files(items, GW_STORAGE_MAX_FILES);
    for (i = (int32_t)cnt - 1; i >= 0; i--)
    {
        uint32_t file_rec_count = items[i].rec_count;
        if (remain < file_rec_count)
        {
            bool ok = prv_read_record_from_file(&items[i], remain, out_rec);
            if (ok && (out_info != NULL))
            {
                *out_info = items[i];
            }
            prv_unmount_fs();
            return ok;
        }
        remain -= file_rec_count;
    }

    prv_unmount_fs();
    return false;
#else
    (void)rec_index_0based;
    (void)out_rec;
    (void)out_info;
    return false;
#endif
}

bool GW_Storage_ReadAllFiles(GW_StorageReadCb_t cb, void* user)
{
#if (UI_USE_LITTLEFS == 1)
    GW_StorageFileInfo_t items[GW_STORAGE_MAX_FILES];
    uint16_t cnt;
    uint16_t i;

    if (cb == NULL)
    {
        return false;
    }
    if (!prv_mount_fs(false))
    {
        return false;
    }
    cnt = prv_collect_files(items, GW_STORAGE_MAX_FILES);
    for (i = 0u; i < cnt && i < GW_STORAGE_MAX_FILES; i++)
    {
        if (!prv_stream_file(&items[i], (uint32_t)(i + 1u), cb, user))
        {
            prv_unmount_fs();
            return false;
        }
    }
    prv_unmount_fs();
    return true;
#else
    (void)cb;
    (void)user;
    return false;
#endif
}

bool GW_Storage_ReadFileByIndex(uint16_t list_index_1based, GW_StorageReadCb_t cb, void* user)
{
#if (UI_USE_LITTLEFS == 1)
    GW_StorageFileInfo_t item;
    bool ok;

    if ((cb == NULL) || !prv_mount_fs(false))
    {
        return false;
    }
    ok = prv_get_file_by_index(list_index_1based, &item) && prv_stream_file(&item, list_index_1based, cb, user);
    prv_unmount_fs();
    return ok;
#else
    (void)list_index_1based;
    (void)cb;
    (void)user;
    return false;
#endif
}

bool GW_Storage_DeleteAllFiles(void)
{
#if (UI_USE_LITTLEFS == 1)
    GW_StorageFileInfo_t items[GW_STORAGE_MAX_FILES];
    uint16_t cnt;
    uint16_t i;

    if (!prv_mount_fs(false))
    {
        return false;
    }
    cnt = prv_collect_files(items, GW_STORAGE_MAX_FILES);
    for (i = 0u; i < cnt && i < GW_STORAGE_MAX_FILES; i++)
    {
        char path[80];
        (void)snprintf(path, sizeof(path), "/%s", items[i].name);
        (void)lfs_remove(&s_lfs, path);
    }
    prv_unmount_fs();
    return true;
#else
    return false;
#endif
}

bool GW_Storage_DeleteFileByIndex(uint16_t list_index_1based)
{
#if (UI_USE_LITTLEFS == 1)
    GW_StorageFileInfo_t item;
    char path[80];
    bool ok = false;

    if (!prv_mount_fs(false))
    {
        return false;
    }
    if (prv_get_file_by_index(list_index_1based, &item))
    {
        (void)snprintf(path, sizeof(path), "/%s", item.name);
        ok = (lfs_remove(&s_lfs, path) == 0);
    }
    prv_unmount_fs();
    return ok;
#else
    (void)list_index_1based;
    return false;
#endif
}
