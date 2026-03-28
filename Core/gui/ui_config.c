/*
 * ui_config.c
 *
 * 설정값을 내부 Flash 마지막 페이지에 저장/복원한다.
 * - power on 시 저장본이 유효하면 복원
 * - 저장본이 없거나 CRC가 틀리면 default 사용
 * - 명령으로 값이 바뀌면 상위(UI_CMD)에서 UI_Config_Save() 호출
 */

#include "ui_types.h"
#include "ui_crc16.h"
#include "stm32wlxx_hal.h"
#include <string.h>
#include <stdbool.h>
#include <stdio.h>

#define UI_CFG_FLASH_MAGIC              (0x55494346u) /* 'UICF' */
#define UI_CFG_FLASH_VERSION            (0x0002u)

#ifndef UI_CFG_FLASH_FALLBACK_SIZE_BYTES
#define UI_CFG_FLASH_FALLBACK_SIZE_BYTES (256u * 1024u)
#endif

#ifndef UI_CFG_FLASH_PAGE_SIZE_BYTES
#if defined(FLASH_PAGE_SIZE)
#define UI_CFG_FLASH_PAGE_SIZE_BYTES    (FLASH_PAGE_SIZE)
#else
#define UI_CFG_FLASH_PAGE_SIZE_BYTES    (2048u)
#endif
#endif

typedef struct
{
    uint32_t    magic;
    uint16_t    version;
    uint16_t    cfg_len;
    UI_Config_t cfg;
    uint16_t    crc16;
    uint16_t    reserved;
} UI_ConfigFlashRec_t;

typedef union
{
    UI_ConfigFlashRec_t rec;
    uint8_t  bytes[((sizeof(UI_ConfigFlashRec_t) + 7u) / 8u) * 8u];
    uint64_t qwords[((sizeof(UI_ConfigFlashRec_t) + 7u) / 8u)];
} UI_ConfigFlashImage_t;

static UI_Config_t s_cfg;
static bool s_inited = false;

static void prv_set_setting_ascii(uint8_t value, char unit)
{
    s_cfg.setting_ascii[0] = (uint8_t)('0' + ((value / 10u) % 10u));
    s_cfg.setting_ascii[1] = (uint8_t)('0' + (value % 10u));
    s_cfg.setting_ascii[2] = (uint8_t)unit;
}

static void prv_init_defaults(void)
{
    memset(&s_cfg, 0, sizeof(s_cfg));

    memset(s_cfg.net_id, 0, sizeof(s_cfg.net_id));
    memcpy(s_cfg.net_id, "ABCD123456", sizeof("ABCD123456") - 1u);

    s_cfg.gw_num    = 0u;
    s_cfg.max_nodes = UI_MAX_NODES;
    s_cfg.node_num  = 0u;

    s_cfg.setting_value = 1u;
    s_cfg.setting_unit  = 'H';
    prv_set_setting_ascii(s_cfg.setting_value, s_cfg.setting_unit);

    s_cfg.tcpip_ip[0] = UI_TCPIP_DEFAULT_IP0;
    s_cfg.tcpip_ip[1] = UI_TCPIP_DEFAULT_IP1;
    s_cfg.tcpip_ip[2] = UI_TCPIP_DEFAULT_IP2;
    s_cfg.tcpip_ip[3] = UI_TCPIP_DEFAULT_IP3;
    s_cfg.tcpip_port  = UI_TCPIP_DEFAULT_PORT;

    s_cfg.loc_ascii[0] = '\0';
}

static uint32_t prv_flash_total_bytes(void)
{
#if defined(FLASHSIZE_BASE)
    uint32_t kb = (uint32_t)(*((const uint16_t*)FLASHSIZE_BASE));
    if ((kb == 0u) || (kb == 0xFFFFu))
    {
        return UI_CFG_FLASH_FALLBACK_SIZE_BYTES;
    }
    return kb * 1024u;
#else
    return UI_CFG_FLASH_FALLBACK_SIZE_BYTES;
#endif
}

static uint32_t prv_flash_cfg_addr(void)
{
    uint32_t total_bytes = prv_flash_total_bytes();
    return FLASH_BASE + total_bytes - UI_CFG_FLASH_PAGE_SIZE_BYTES;
}

static uint32_t prv_flash_page_from_addr(uint32_t addr)
{
    return (addr - FLASH_BASE) / UI_CFG_FLASH_PAGE_SIZE_BYTES;
}

static uint32_t prv_flash_bank_from_addr(uint32_t addr)
{
#if defined(FLASH_BANK_SIZE) && defined(FLASH_BANK_2)
    if (addr >= (FLASH_BASE + FLASH_BANK_SIZE))
    {
        return FLASH_BANK_2;
    }
#endif
#if defined(FLASH_BANK_1)
    (void)addr;
    return FLASH_BANK_1;
#else
    (void)addr;
    return 0u;
#endif
}

static void prv_sanitize_cfg(void)
{
    if (s_cfg.gw_num > 2u)
    {
        s_cfg.gw_num = 2u;
    }

    if (s_cfg.max_nodes < 1u)
    {
        s_cfg.max_nodes = 1u;
    }
    if (s_cfg.max_nodes > UI_MAX_NODES)
    {
        s_cfg.max_nodes = UI_MAX_NODES;
    }

    if (s_cfg.node_num >= UI_MAX_NODES)
    {
        s_cfg.node_num = (UI_MAX_NODES - 1u);
    }

    if (s_cfg.setting_value > 99u)
    {
        s_cfg.setting_value = 99u;
    }
    /* legacy 00H / invalid unit는 새 기본값 01H로 정규화 */
    if ((s_cfg.setting_value == 0u) || ((s_cfg.setting_unit != 'M') && (s_cfg.setting_unit != 'H')))
    {
        s_cfg.setting_unit = 'H';
        s_cfg.setting_value = 1u;
    }

    if (s_cfg.tcpip_port < UI_TCPIP_MIN_PORT)
    {
        s_cfg.tcpip_port = UI_TCPIP_DEFAULT_PORT;
    }

    s_cfg.loc_ascii[UI_LOC_ASCII_MAX - 1u] = '\0';
    prv_set_setting_ascii(s_cfg.setting_value, s_cfg.setting_unit);
}

static uint16_t prv_cfg_crc16(const UI_Config_t* cfg)
{
    return UI_CRC16_CCITT((const uint8_t*)cfg, sizeof(*cfg), UI_CRC16_INIT);
}

static bool prv_flash_rec_is_valid(const UI_ConfigFlashRec_t* rec)
{
    uint16_t crc;

    if (rec == NULL)
    {
        return false;
    }

    if (rec->magic != UI_CFG_FLASH_MAGIC)
    {
        return false;
    }
    if (rec->version != UI_CFG_FLASH_VERSION)
    {
        return false;
    }
    if (rec->cfg_len != (uint16_t)sizeof(UI_Config_t))
    {
        return false;
    }

    crc = prv_cfg_crc16(&rec->cfg);
    if (crc != rec->crc16)
    {
        return false;
    }

    return true;
}

static bool prv_load_from_flash(void)
{
    const UI_ConfigFlashRec_t* rec = (const UI_ConfigFlashRec_t*)prv_flash_cfg_addr();

    if (!prv_flash_rec_is_valid(rec))
    {
        return false;
    }

    memcpy(&s_cfg, &rec->cfg, sizeof(s_cfg));
    prv_sanitize_cfg();
    return true;
}

const UI_Config_t* UI_GetConfig(void)
{
    if (!s_inited)
    {
        prv_init_defaults();
        (void)prv_load_from_flash();
        s_inited = true;
    }
    return &s_cfg;
}

bool UI_Config_Save(void)
{
    UI_ConfigFlashImage_t img;
    uint32_t addr;
    uint32_t page_error = 0u;
    HAL_StatusTypeDef st;
    FLASH_EraseInitTypeDef erase = {0};
    uint32_t i;

    (void)UI_GetConfig();
    prv_sanitize_cfg();

    memset(&img, 0xFF, sizeof(img));
    img.rec.magic   = UI_CFG_FLASH_MAGIC;
    img.rec.version = UI_CFG_FLASH_VERSION;
    img.rec.cfg_len = (uint16_t)sizeof(UI_Config_t);
    memcpy(&img.rec.cfg, &s_cfg, sizeof(s_cfg));
    img.rec.crc16   = prv_cfg_crc16(&img.rec.cfg);
    img.rec.reserved = 0xFFFFu;

    addr = prv_flash_cfg_addr();

    if (HAL_FLASH_Unlock() != HAL_OK)
    {
        return false;
    }

    erase.TypeErase = FLASH_TYPEERASE_PAGES;
#if defined(FLASH_BANK_1)
    erase.Banks = prv_flash_bank_from_addr(addr);
#endif
    erase.Page = prv_flash_page_from_addr(addr);
    erase.NbPages = 1u;

    st = HAL_FLASHEx_Erase(&erase, &page_error);
    if (st != HAL_OK)
    {
        (void)HAL_FLASH_Lock();
        return false;
    }

    for (i = 0u; i < (uint32_t)(sizeof(img.qwords) / sizeof(img.qwords[0])); i++)
    {
        st = HAL_FLASH_Program(FLASH_TYPEPROGRAM_DOUBLEWORD,
                               addr + (i * 8u),
                               img.qwords[i]);
        if (st != HAL_OK)
        {
            (void)HAL_FLASH_Lock();
            return false;
        }
    }

    (void)HAL_FLASH_Lock();
    return prv_load_from_flash();
}

void UI_SetNetId(const uint8_t net_id_10[UI_NET_ID_LEN])
{
    memset(s_cfg.net_id, 0, sizeof(s_cfg.net_id));
    memcpy(s_cfg.net_id, net_id_10, UI_NET_ID_LEN);
}

void UI_SetGwNum(uint8_t gw_num)
{
    if (gw_num > 2u) { gw_num = 2u; }
    s_cfg.gw_num = gw_num;
}

void UI_SetMaxNodes(uint8_t max_nodes)
{
    if (max_nodes < 1u) { max_nodes = 1u; }
    if (max_nodes > UI_MAX_NODES) { max_nodes = UI_MAX_NODES; }
    s_cfg.max_nodes = max_nodes;
}

void UI_SetNodeNum(uint8_t node_num)
{
    if (node_num >= UI_MAX_NODES) { node_num = (UI_MAX_NODES - 1u); }
    s_cfg.node_num = node_num;
}

void UI_SetSetting(uint8_t value, char unit)
{
    if (value > 99u) { value = 99u; }

    if ((value == 0u) || ((unit != 'M') && (unit != 'H')))
    {
        unit = 'H';
        value = 1u;
    }

    s_cfg.setting_value = value;
    s_cfg.setting_unit  = unit;
    prv_set_setting_ascii(value, unit);
}

void UI_SetTcpIp(const uint8_t ip[4], uint16_t port)
{
    memcpy(s_cfg.tcpip_ip, ip, 4u);
    s_cfg.tcpip_port = port;
}

void UI_SetLocAscii(const char* loc_ascii)
{
    if (loc_ascii == NULL)
    {
        s_cfg.loc_ascii[0] = '\0';
        return;
    }

    (void)snprintf(s_cfg.loc_ascii, sizeof(s_cfg.loc_ascii), "%s", loc_ascii);
    s_cfg.loc_ascii[UI_LOC_ASCII_MAX - 1u] = '\0';
}

const char* UI_GetLocAscii(void)
{
    return s_cfg.loc_ascii;
}
