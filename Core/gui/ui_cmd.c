#include "ui_cmd.h"
#include "ui_types.h"
#include "ui_uart.h"
#include "ui_time.h"
#include "ui_ble.h"
#include "gw_file_cmd.h"
#include "stm32wlxx_hal.h" /* __weak */

#include <string.h>
#include <stdio.h>
#include <ctype.h>

/* -------------------------------------------------------------------------- */
/* Hook functions (GW/ND에서 필요 시 override)                                */
/* -------------------------------------------------------------------------- */
__weak void UI_Hook_OnConfigChanged(void) {}
__weak void UI_Hook_OnSettingChanged(uint8_t value, char unit)
{
    (void)value;
    (void)unit;
}
__weak void UI_Hook_OnTimeChanged(void) {}
__weak void UI_Hook_OnBeaconOnceRequested(void) {}
__weak void UI_Hook_OnBleEndRequested(void) {}
__weak void UI_Hook_OnTestStartRequested(void) {}
__weak bool UI_Hook_OnSyncRequested(uint16_t duration_hours)
{
    (void)duration_hours;
    return false;
}

__weak bool UI_Hook_IsTcpEnabled(void)
{
    return true;
}

__weak void UI_Hook_OnTcpModeChanged(bool enabled)
{
    (void)enabled;
}

/* -------------------------------------------------------------------------- */
static bool s_cmd_silent_reply = false;
static bool prv_cmd_equals_relaxed(const char* s, const char* base);

static size_t prv_netid_copy_padded(uint8_t out[UI_NET_ID_LEN], const char* id)
{
    size_t len = (id != NULL) ? strlen(id) : 0u;
    if ((len == 0u) || (len > UI_NET_ID_LEN)) {
        return 0u;
    }
    memset(out, 0, UI_NET_ID_LEN);
    memcpy(out, id, len);
    return len;
}

static void prv_send_ok(void)
{
    if (!s_cmd_silent_reply) {
        UI_UART_SendString("OK\r\n");
    }
}

static void prv_send_error(void)
{
    if (!s_cmd_silent_reply) {
        UI_UART_SendString("ERROR\r\n");
    }
}

static const char* prv_skip_spaces(const char* s)
{
    while (s && *s && isspace((unsigned char)*s)) {
        s++;
    }
    return s;
}

/* CR/LF 제거용 (line은 이미 null-terminated 가정) */
static void prv_rstrip(char* s)
{
    size_t n = strlen(s);
    while (n > 0u) {
        char c = s[n - 1u];
        if (c == '\r' || c == '\n' || c == ' ' || c == '\t') {
            s[n - 1u] = '\0';
            n--;
        } else {
            break;
        }
    }
}

static int prv_parse_u8_dec(const char* s, uint8_t* out)
{
    unsigned v = 0u;
    int cnt = 0;

    while (*s && isdigit((unsigned char)*s) && cnt < 3) {
        v = v * 10u + (unsigned)(*s - '0');
        s++;
        cnt++;
    }
    if (cnt == 0) {
        return 0;
    }
    if (v > 255u) {
        return 0;
    }
    *out = (uint8_t)v;
    return cnt;
}

static int prv_parse_u16_dec(const char* s, uint16_t* out)
{
    unsigned long v = 0u;
    int cnt = 0;

    while (*s && isdigit((unsigned char)*s) && cnt < 5) {
        v = (v * 10u) + (unsigned long)(*s - '0');
        s++;
        cnt++;
    }
    if (cnt == 0) {
        return 0;
    }
    if (v > 65535u) {
        return 0;
    }
    *out = (uint16_t)v;
    return cnt;
}

static bool prv_match_cmd_head(const char* s, const char* head, const char** out_tail)
{
    const char* p = prv_skip_spaces(s);
    const char* h = head;

    if ((p == NULL) || (h == NULL)) {
        return false;
    }

    while (*h != '\0') {
        if (toupper((unsigned char)*p) != toupper((unsigned char)*h)) {
            return false;
        }
        p++;
        h++;
    }
    while (isspace((unsigned char)*p)) {
        p++;
    }
    if (*p != ':') {
        return false;
    }
    p++;
    while (isspace((unsigned char)*p)) {
        p++;
    }
    if (out_tail != NULL) {
        *out_tail = p;
    }
    return true;
}

static bool prv_is_plain_safe_command(const char* s)
{
    const char* tail = NULL;

    if (prv_cmd_equals_relaxed(s, "SETTING READ")) {
        return true;
    }
    if (prv_cmd_equals_relaxed(s, "BEACON ON")) {
        return true;
    }
    if (prv_cmd_equals_relaxed(s, "GW BEACON ON")) {
        return true;
    }
    if (prv_match_cmd_head(s, "TCPIP", &tail)) {
        (void)tail;
        return true;
    }
    if (prv_match_cmd_head(s, "GW TCPIP", &tail)) {
        (void)tail;
        return true;
    }
    if (prv_cmd_equals_relaxed(s, "TCP ON")) {
        return true;
    }
    if (prv_cmd_equals_relaxed(s, "GW TCP ON")) {
        return true;
    }
    if (prv_cmd_equals_relaxed(s, "TCP OFF")) {
        return true;
    }
    if (prv_cmd_equals_relaxed(s, "GW TCP OFF")) {
        return true;
    }
    if (prv_cmd_equals_relaxed(s, "FILE LIST")) {
        return true;
    }
    if (prv_cmd_equals_relaxed(s, "LOC")) {
        return true;
    }
    if (prv_match_cmd_head(s, "GW SYNC", &tail)) {
        (void)tail;
        return true;
    }
    if (prv_match_cmd_head(s, "SYNC", &tail)) {
        (void)tail;
        return true;
    }
    if (prv_match_cmd_head(s, "LOC", &tail)) {
        (void)tail;
        return true;
    }
    if (prv_match_cmd_head(s, "FILE READ", &tail)) {
        (void)tail;
        return true;
    }
    if (prv_match_cmd_head(s, "FILE DEL", &tail)) {
        (void)tail;
        return true;
    }
    return false;
}

static bool prv_parse_tcpip_endpoint(const char* s, uint8_t ip[4], uint16_t* port)
{
    const char* p = prv_skip_spaces(s);
    int n = 0;
    uint16_t parsed_port = 0u;

    if ((p == NULL) || (ip == NULL) || (port == NULL)) {
        return false;
    }

    for (uint32_t i = 0u; i < 4u; i++) {
        n = prv_parse_u8_dec(p, &ip[i]);
        if (n <= 0) {
            return false;
        }
        p += n;
        while (isspace((unsigned char)*p)) {
            p++;
        }
        if (i < 3u) {
            if (*p != '.') {
                return false;
            }
            p++;
            while (isspace((unsigned char)*p)) {
                p++;
            }
        }
    }

    while (isspace((unsigned char)*p)) {
        p++;
    }
    if (*p != ':') {
        return false;
    }
    p++;
    while (isspace((unsigned char)*p)) {
        p++;
    }

    n = prv_parse_u16_dec(p, &parsed_port);
    if (n <= 0) {
        return false;
    }
    p += n;
    while (isspace((unsigned char)*p)) {
        p++;
    }
    if (*p != '\0') {
        return false;
    }
    if (parsed_port < UI_TCPIP_MIN_PORT) {
        return false;
    }

    *port = parsed_port;
    return true;
}

static bool prv_cmd_equals_relaxed(const char* s, const char* base)
{
    if ((s == NULL) || (base == NULL)) {
        return false;
    }

    while (*base != '\0') {
        if (toupper((unsigned char)*s) != toupper((unsigned char)*base)) {
            return false;
        }
        s++;
        base++;
    }
    while ((*s == ' ') || (*s == '\t')) {
        s++;
    }
    if (*s == ':') {
        s++;
        while (isspace((unsigned char)*s)) {
            s++;
        }
    }
    return (*s == '\0');
}

static bool prv_commit_config_changed(void)
{
    if (!UI_Config_Save()) {
        prv_send_error();
        return false;
    }
    prv_send_ok();
    UI_Hook_OnConfigChanged();
    return true;
}

static bool prv_commit_setting_changed(uint8_t value, char unit)
{
    if (!UI_Config_Save()) {
        prv_send_error();
        return false;
    }
    prv_send_ok();
    UI_Hook_OnSettingChanged(value, unit);
    return true;
}

static void prv_send_setting_read(void)
{
    const UI_Config_t* cfg = UI_GetConfig();
    char netid[UI_NET_ID_LEN + 1u];
    char line[192];

    if (s_cmd_silent_reply) {
        return;
    }

    memcpy(netid, cfg->net_id, UI_NET_ID_LEN);
    netid[UI_NET_ID_LEN] = '\0';

    (void)snprintf(line, sizeof(line), "NETID:%s\r\n", netid);
    UI_UART_SendString(line);

    (void)snprintf(line, sizeof(line), "ND CNT:%u\r\n", cfg->max_nodes);
    UI_UART_SendString(line);

    (void)snprintf(line, sizeof(line), "GW NUM:%u\r\n", cfg->gw_num);
    UI_UART_SendString(line);

    (void)snprintf(line, sizeof(line), "SETTING:%c%c%c\r\n",
                   (char)cfg->setting_ascii[0],
                   (char)cfg->setting_ascii[1],
                   (char)cfg->setting_ascii[2]);
    UI_UART_SendString(line);

    (void)snprintf(line, sizeof(line), "TCPIP:%u.%u.%u.%u:%u\r\n",
                   (unsigned)cfg->tcpip_ip[0], (unsigned)cfg->tcpip_ip[1],
                   (unsigned)cfg->tcpip_ip[2], (unsigned)cfg->tcpip_ip[3],
                   (unsigned)cfg->tcpip_port);
    UI_UART_SendString(line);

    (void)snprintf(line, sizeof(line), "TCP:%s\r\n",
                   UI_Hook_IsTcpEnabled() ? "ON" : "OFF");
    UI_UART_SendString(line);

    (void)snprintf(line, sizeof(line), "LOC:%s\r\n", UI_GetLocAscii());
    UI_UART_SendString(line);
}

static void prv_apply_gw_ble_name(uint8_t gw_num)
{
    char ble_name[16];
    (void)snprintf(ble_name, sizeof(ble_name), "BT GW %u", (unsigned)gw_num);
    (void)UI_BLE_ApplyDeviceName(ble_name);
}

static void prv_process_line_impl(const char* line_in, bool silent)
{
    s_cmd_silent_reply = silent;
    if (line_in == NULL) {
        return;
    }

    char line[UI_UART_LINE_MAX];
    (void)snprintf(line, sizeof(line), "%s", line_in);
    prv_rstrip(line);

    const char* s0 = prv_skip_spaces(line);
    bool valid_plain = false;
    char* s = NULL;

    if ((s0 != NULL) && (*s0 == '<')) {
        size_t n0 = strlen(s0);
        if ((n0 < 3u) || (s0[n0 - 1u] != '>')) {
            return;
        }
        s = (char*)prv_skip_spaces(line);
        s++;
        s = (char*)prv_skip_spaces(s);
        prv_rstrip(s);
        size_t n = strlen(s);
        if ((n > 0u) && (s[n - 1u] == '>')) {
            s[n - 1u] = '\0';
            prv_rstrip(s);
        }
    } else {
        if (!prv_is_plain_safe_command(s0)) {
            return;
        }
        valid_plain = true;
        s = (char*)s0;
    }

    const char* p = prv_skip_spaces(s);
    if ((p == NULL) || (*p == '\0')) {
        return;
    }

    if (((s0 != NULL) && (*s0 == '<')) || valid_plain) {
        UI_BLE_ExtendMs(UI_BLE_ACTIVE_MS);
    }

    /* -------------------- GW SYNC:X / SYNC:X ------------------ */
    {
        const char* q = NULL;
        uint16_t duration_hours = 0u;
        int n2 = 0;

        if (prv_match_cmd_head(p, "GW SYNC", &q) || prv_match_cmd_head(p, "SYNC", &q)) {
            n2 = prv_parse_u16_dec(q, &duration_hours);
            if (n2 <= 0) {
                prv_send_error();
                return;
            }
            q += n2;
            while (isspace((unsigned char)*q)) {
                q++;
            }
            if ((*q != '\0') || (duration_hours == 0u)) {
                prv_send_error();
                return;
            }
            if (UI_Hook_OnSyncRequested(duration_hours)) {
                prv_send_ok();
            } else {
                prv_send_error();
            }
            return;
        }
    }

    /* -------------------- TEST START -------------------- */
    if (prv_cmd_equals_relaxed(p, "TEST START")) {
        UI_Hook_OnTestStartRequested();
        prv_send_ok();
        return;
    }

    /* -------------------- BEACON ON --------------------- */
    if (prv_cmd_equals_relaxed(p, "BEACON ON") || prv_cmd_equals_relaxed(p, "GW BEACON ON")) {
        UI_Hook_OnBeaconOnceRequested();
        prv_send_ok();
        return;
    }

    /* -------------------- SETTING READ ------------------ */
    if (prv_cmd_equals_relaxed(p, "SETTING READ")) {
        prv_send_setting_read();
        prv_send_ok();
        return;
    }

    /* -------------------- TIME CHECK -------------------- */
    if ((strcmp(p, "TIME CHECK") == 0) || (strcmp(p, "TIME CHECK:") == 0)) {
        char ts[48];
        UI_Time_FormatNow(ts, sizeof(ts));
        UI_UART_SendString(ts);
        UI_UART_SendString("\r\n");
        prv_send_ok();
        return;
    }

    /* -------------------- TIME:... ---------------------- */
    if (strncmp(p, "TIME:", 5) == 0) {
        if (UI_Time_SetFromString(p)) {
            prv_send_ok();
            UI_Hook_OnTimeChanged();
        } else {
            prv_send_error();
        }
        return;
    }

    /* -------------------- LOC / LOC:... ----------------- */
    if (prv_cmd_equals_relaxed(p, "LOC")) {
        char line_loc[UI_LOC_ASCII_MAX + 8u];
        (void)snprintf(line_loc, sizeof(line_loc), "LOC:%s\r\n", UI_GetLocAscii());
        UI_UART_SendString(line_loc);
        prv_send_ok();
        return;
    }
    {
        const char* q = NULL;
        if (prv_match_cmd_head(p, "LOC", &q)) {
            if ((q == NULL) || (*q == '\0')) {
                char line_loc[UI_LOC_ASCII_MAX + 8u];
                (void)snprintf(line_loc, sizeof(line_loc), "LOC:%s\r\n", UI_GetLocAscii());
                UI_UART_SendString(line_loc);
                prv_send_ok();
                return;
            }
            if (strlen(q) >= UI_LOC_ASCII_MAX) {
                prv_send_error();
                return;
            }
            UI_SetLocAscii(q);
            (void)prv_commit_config_changed();
            return;
        }
    }

    /* -------------------- NETID:UTF8/ASCII -------------- */
    if (strncmp(p, "NETID:", 6) == 0) {
        const char* id = p + 6;
        uint8_t net_id[UI_NET_ID_LEN];
        if (prv_netid_copy_padded(net_id, id) == 0u) {
            prv_send_error();
            return;
        }
        UI_SetNetId(net_id);
        (void)prv_commit_config_changed();
        return;
    }

    /* -------------------- GW NUM:xx --------------------- */
    if (strncmp(p, "GW NUM:", 7) == 0) {
        uint8_t v = 0u;
        if (prv_parse_u8_dec(p + 7, &v) <= 0) {
            prv_send_error();
            return;
        }
        if (v <= 2u) {
            UI_SetGwNum(v);
            if (prv_commit_config_changed()) {
                prv_apply_gw_ble_name(v);
            }
        } else {
            prv_send_error();
        }
        return;
    }

    /* -------------------- ND CNT:xx / GW ND CNT:xx ------ */
    if ((strncmp(p, "ND CNT:", 7) == 0) || (strncmp(p, "GW ND CNT:", 10) == 0)) {
        const char* q = (strncmp(p, "GW ND CNT:", 10) == 0) ? (p + 10) : (p + 7);
        uint8_t v = 0u;
        if (prv_parse_u8_dec(q, &v) <= 0) {
            prv_send_error();
            return;
        }
        if ((v >= 1u) && (v <= UI_MAX_NODES)) {
            UI_SetMaxNodes(v);
            (void)prv_commit_config_changed();
        } else {
            prv_send_error();
        }
        return;
    }

    /* -------------------- SETTING:xxM/H ------------------ */
    if (strncmp(p, "SETTING:", 8) == 0) {
        const char* q = p + 8;
        uint8_t v = 0u;
        int n2 = prv_parse_u8_dec(q, &v);

        if (n2 <= 0) {
            prv_send_error();
            return;
        }
        q += n2;

        char unit = *q;
        if ((unit != 'M') && (unit != 'H')) {
            prv_send_error();
            return;
        }
        if (v == 0u) {
            prv_send_error();
            return;
        }

        UI_SetSetting(v, unit);
        (void)prv_commit_setting_changed(v, unit);
        return;
    }

    /* -------------------- TCP ON/OFF -------------------- */
    if (prv_cmd_equals_relaxed(p, "TCP ON") || prv_cmd_equals_relaxed(p, "GW TCP ON")) {
        UI_Hook_OnTcpModeChanged(true);
        prv_send_ok();
        return;
    }
    if (prv_cmd_equals_relaxed(p, "TCP OFF") || prv_cmd_equals_relaxed(p, "GW TCP OFF")) {
        UI_Hook_OnTcpModeChanged(false);
        prv_send_ok();
        return;
    }

    /* -------------------- TCPIP:ip:port ------------------ */
    {
        const char* q = NULL;
        uint8_t ip[4] = {0u, 0u, 0u, 0u};
        uint16_t port = 0u;

        if (prv_match_cmd_head(p, "TCPIP", &q) || prv_match_cmd_head(p, "GW TCPIP", &q)) {
            if (!prv_parse_tcpip_endpoint(q, ip, &port)) {
                prv_send_error();
                return;
            }
            UI_SetTcpIp(ip, port);
            (void)prv_commit_config_changed();
            return;
        }
    }

    /* -------------------- FILE LIST ---------------------- */
    if (prv_cmd_equals_relaxed(p, "FILE LIST")) {
        if (GW_FileCmd_List()) {
            prv_send_ok();
        } else {
            prv_send_error();
        }
        return;
    }

    /* -------------------- FILE READ:... ------------------ */
    {
        const char* q = NULL;
        if (prv_match_cmd_head(p, "FILE READ", &q)) {
            if (GW_FileCmd_ReadArg(q)) {
                prv_send_ok();
            } else {
                prv_send_error();
            }
            return;
        }
    }

    /* -------------------- FILE DEL:... ------------------- */
    {
        const char* q = NULL;
        if (prv_match_cmd_head(p, "FILE DEL", &q)) {
            if (GW_FileCmd_DeleteArg(q)) {
                prv_send_ok();
            } else {
                prv_send_error();
            }
            return;
        }
    }

    /* -------------------- BLE END ------------------------ */
    if ((strncmp(p, "BLE END", 7) == 0) || (strncmp(p, "BLE END:", 8) == 0)) {
        prv_send_ok();
        UI_Hook_OnBleEndRequested();
        return;
    }

    /* Unknown */
    prv_send_error();
}

void UI_Cmd_ProcessLine(const char* line)
{
    prv_process_line_impl(line, false);
}

void UI_Cmd_ProcessLineSilent(const char* line)
{
    prv_process_line_impl(line, true);
}
