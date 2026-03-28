#ifndef GW_W25Q128_H
#define GW_W25Q128_H

#include <stdbool.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

bool GW_W25Q128_Init(void);
bool GW_W25Q128_IsReady(void);

#ifdef __cplusplus
}
#endif

#endif /* GW_W25Q128_H */
