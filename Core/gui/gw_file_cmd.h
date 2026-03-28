#ifndef GW_FILE_CMD_H
#define GW_FILE_CMD_H

#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

bool GW_FileCmd_List(void);
bool GW_FileCmd_ReadArg(const char* arg);
bool GW_FileCmd_DeleteArg(const char* arg);

#ifdef __cplusplus
}
#endif

#endif /* GW_FILE_CMD_H */
