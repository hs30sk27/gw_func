#include "gw_w25q128.h"
#include "gw_storage.h"
#include "main.h"
#include "stm32wlxx_hal.h"

#include <string.h>

extern SPI_HandleTypeDef hspi1;

#ifndef GW_W25_SPI_TIMEOUT_MS
#define GW_W25_SPI_TIMEOUT_MS      (100u)
#endif
#ifndef GW_W25_PAGE_SIZE
#define GW_W25_PAGE_SIZE           (256u)
#endif
#ifndef GW_W25_SECTOR_SIZE
#define GW_W25_SECTOR_SIZE         (4096u)
#endif
#ifndef GW_W25_WAIT_PP_MS
#define GW_W25_WAIT_PP_MS          (50u)
#endif
#ifndef GW_W25_WAIT_SE_MS
#define GW_W25_WAIT_SE_MS          (3000u)
#endif
#ifndef GW_W25_WAIT_RESUME_MS
#define GW_W25_WAIT_RESUME_MS      (2u)
#endif
#ifndef GW_W25_WAIT_PD_READY_MS
#define GW_W25_WAIT_PD_READY_MS    (GW_W25_WAIT_SE_MS)
#endif

#define GW_W25_CMD_WREN            (0x06u)
#define GW_W25_CMD_RDSR1           (0x05u)
#define GW_W25_CMD_READ            (0x03u)
#define GW_W25_CMD_PP              (0x02u)
#define GW_W25_CMD_SE4K            (0x20u)
#define GW_W25_CMD_JEDEC_ID        (0x9Fu)
#define GW_W25_CMD_POWER_DOWN      (0xB9u)
#define GW_W25_CMD_RELEASE_PD      (0xABu)

#if defined(UI_W25_CS_GPIO_Port) && defined(UI_W25_CS_Pin)
#define GW_W25_HAVE_CS             (1u)
#define GW_W25_CS_GPIO_Port        UI_W25_CS_GPIO_Port
#define GW_W25_CS_Pin              UI_W25_CS_Pin
#elif defined(W25_CS_GPIO_Port) && defined(W25_CS_Pin)
#define GW_W25_HAVE_CS             (1u)
#define GW_W25_CS_GPIO_Port        W25_CS_GPIO_Port
#define GW_W25_CS_Pin              W25_CS_Pin
#elif defined(W25Q128_CS_GPIO_Port) && defined(W25Q128_CS_Pin)
#define GW_W25_HAVE_CS             (1u)
#define GW_W25_CS_GPIO_Port        W25Q128_CS_GPIO_Port
#define GW_W25_CS_Pin              W25Q128_CS_Pin
#elif defined(FLASH_CS_GPIO_Port) && defined(FLASH_CS_Pin)
#define GW_W25_HAVE_CS             (1u)
#define GW_W25_CS_GPIO_Port        FLASH_CS_GPIO_Port
#define GW_W25_CS_Pin              FLASH_CS_Pin
#elif defined(MEM_CS_GPIO_Port) && defined(MEM_CS_Pin)
#define GW_W25_HAVE_CS             (1u)
#define GW_W25_CS_GPIO_Port        MEM_CS_GPIO_Port
#define GW_W25_CS_Pin              MEM_CS_Pin
#else
#define GW_W25_HAVE_CS             (0u)
#endif

static bool s_inited = false;



static void prv_cs_high(void)
{
#if (GW_W25_HAVE_CS == 1u)
    HAL_GPIO_WritePin(GW_W25_CS_GPIO_Port, GW_W25_CS_Pin, GPIO_PIN_SET);
#endif
}

static void prv_cs_low(void)
{
#if (GW_W25_HAVE_CS == 1u)
    HAL_GPIO_WritePin(GW_W25_CS_GPIO_Port, GW_W25_CS_Pin, GPIO_PIN_RESET);
#endif
}

static bool prv_spi_ensure_ready(void)
{
#if (GW_W25_HAVE_CS == 1u)
    __HAL_RCC_SPI1_CLK_ENABLE();
    if ((hspi1.Instance != SPI1) || (hspi1.State == HAL_SPI_STATE_RESET))
    {
        MX_SPI1_Init();
    }
    prv_cs_high();
    return true;
#else
    return false;
#endif
}

static bool prv_tx(const uint8_t* tx, uint16_t size)
{
    return (HAL_SPI_Transmit(&hspi1, (uint8_t*)tx, size, GW_W25_SPI_TIMEOUT_MS) == HAL_OK);
}

static bool prv_rx(uint8_t* rx, uint16_t size)
{
    return (HAL_SPI_Receive(&hspi1, rx, size, GW_W25_SPI_TIMEOUT_MS) == HAL_OK);
}

static bool prv_cmd_only(uint8_t cmd)
{
    prv_cs_low();
    bool ok = prv_tx(&cmd, 1u);
    prv_cs_high();
    return ok;
}

static bool prv_read_sr1(uint8_t* sr1)
{
    uint8_t cmd = GW_W25_CMD_RDSR1;
    if (sr1 == NULL)
    {
        return false;
    }
    prv_cs_low();
    if (!prv_tx(&cmd, 1u))
    {
        prv_cs_high();
        return false;
    }
    if (!prv_rx(sr1, 1u))
    {
        prv_cs_high();
        return false;
    }
    prv_cs_high();
    return true;
}

static bool prv_wait_ready(uint32_t timeout_ms)
{
    uint32_t start = HAL_GetTick();
    uint8_t sr1 = 0x01u;

    do
    {
        if (!prv_read_sr1(&sr1))
        {
            return false;
        }
        if ((sr1 & 0x01u) == 0u)
        {
            return true;
        }
    }
    while ((HAL_GetTick() - start) < timeout_ms);

    return false;
}

static bool prv_wren(void)
{
    return prv_cmd_only(GW_W25_CMD_WREN);
}

static bool prv_release_power_down(void)
{
    uint8_t cmd[4] = { GW_W25_CMD_RELEASE_PD, 0u, 0u, 0u };
    prv_cs_low();
    bool ok = prv_tx(cmd, 4u);
    prv_cs_high();
    HAL_Delay(GW_W25_WAIT_RESUME_MS);
    return ok;
}

static bool prv_read_jedec_id(uint8_t jedec[3])
{
    uint8_t cmd = GW_W25_CMD_JEDEC_ID;
    if (jedec == NULL)
    {
        return false;
    }
    prv_cs_low();
    if (!prv_tx(&cmd, 1u))
    {
        prv_cs_high();
        return false;
    }
    if (!prv_rx(jedec, 3u))
    {
        prv_cs_high();
        return false;
    }
    prv_cs_high();
    return true;
}

bool GW_W25Q128_Init(void)
{
    uint8_t jedec[3] = {0};

    if (!prv_spi_ensure_ready())
    {
        return false;
    }
    if (!prv_release_power_down())
    {
        return false;
    }
    if (!prv_read_jedec_id(jedec))
    {
        return false;
    }
    /* Winbond/JV 계열 외에도 read 성공이면 사용 허용 */
    s_inited = true;
    return true;
}

bool GW_W25Q128_IsReady(void)
{
    return s_inited;
}

bool GW_Storage_W25Q_PowerOn(void)
{
    if (!prv_spi_ensure_ready())
    {
        return false;
    }
    if (!s_inited)
    {
        return GW_W25Q128_Init();
    }
    return prv_release_power_down();
}

void GW_Storage_W25Q_PowerDown(void)
{
    if (!s_inited)
    {
        return;
    }
    if (!prv_spi_ensure_ready())
    {
        return;
    }
    /*
     * Stop 진입 직전에도 이 함수를 재사용하므로, page-program보다 긴 sector erase까지
     * 포함해 ready를 기다린 뒤 deep power-down으로 내린다.
     */
    (void)prv_wait_ready(GW_W25_WAIT_PD_READY_MS);
    (void)prv_cmd_only(GW_W25_CMD_POWER_DOWN);
    prv_cs_high();
}

int GW_Storage_W25Q_Read(uint32_t addr, void* buf, uint32_t size)
{
    uint8_t cmd[4];
    uint8_t* p = (uint8_t*)buf;

    if ((buf == NULL) || (size == 0u))
    {
        return 0;
    }
    if (!prv_spi_ensure_ready())
    {
        return -1;
    }

    cmd[0] = GW_W25_CMD_READ;
    cmd[1] = (uint8_t)(addr >> 16);
    cmd[2] = (uint8_t)(addr >> 8);
    cmd[3] = (uint8_t)(addr >> 0);

    prv_cs_low();
    if (!prv_tx(cmd, 4u))
    {
        prv_cs_high();
        return -1;
    }
    if (!prv_rx(p, (uint16_t)size))
    {
        prv_cs_high();
        return -1;
    }
    prv_cs_high();
    return 0;
}

int GW_Storage_W25Q_Prog(uint32_t addr, const void* buf, uint32_t size)
{
    const uint8_t* p = (const uint8_t*)buf;

    if ((buf == NULL) || (size == 0u))
    {
        return 0;
    }
    if (!prv_spi_ensure_ready())
    {
        return -1;
    }

    while (size > 0u)
    {
        uint32_t page_off = addr % GW_W25_PAGE_SIZE;
        uint32_t chunk = GW_W25_PAGE_SIZE - page_off;
        uint8_t cmd[4];

        if (chunk > size)
        {
            chunk = size;
        }

        if (!prv_wren())
        {
            return -1;
        }

        cmd[0] = GW_W25_CMD_PP;
        cmd[1] = (uint8_t)(addr >> 16);
        cmd[2] = (uint8_t)(addr >> 8);
        cmd[3] = (uint8_t)(addr >> 0);

        prv_cs_low();
        if (!prv_tx(cmd, 4u) || !prv_tx(p, (uint16_t)chunk))
        {
            prv_cs_high();
            return -1;
        }
        prv_cs_high();

        if (!prv_wait_ready(GW_W25_WAIT_PP_MS))
        {
            return -1;
        }

        addr += chunk;
        p += chunk;
        size -= chunk;
    }

    return 0;
}

int GW_Storage_W25Q_Erase4K(uint32_t addr)
{
    uint8_t cmd[4];

    if (!prv_spi_ensure_ready())
    {
        return -1;
    }
    if (!prv_wren())
    {
        return -1;
    }

    cmd[0] = GW_W25_CMD_SE4K;
    cmd[1] = (uint8_t)(addr >> 16);
    cmd[2] = (uint8_t)(addr >> 8);
    cmd[3] = (uint8_t)(addr >> 0);

    prv_cs_low();
    if (!prv_tx(cmd, 4u))
    {
        prv_cs_high();
        return -1;
    }
    prv_cs_high();

    return prv_wait_ready(GW_W25_WAIT_SE_MS) ? 0 : -1;
}

int GW_Storage_W25Q_Sync(void)
{
    return prv_wait_ready(GW_W25_WAIT_SE_MS) ? 0 : -1;
}

