#include "gw_catm1.h"
#include "gw_app.h"
#include "ui_conf.h"
#include "ui_types.h"
#include "ui_time.h"
#include "ui_lpm.h"
#include "ui_ringbuf.h"
#include "ui_ble.h"
#include "ui_cmd.h"
#include "main.h"
#include "stm32wlxx_hal.h"
#include <string.h>
#include <stdio.h>
#include <stdarg.h>

extern UART_HandleTypeDef hlpuart1;
extern bool GW_Storage_SaveHourRec(const GW_HourRec_t* rec);
extern uint32_t GW_Storage_GetTotalRecordCount(void);
extern void UI_Hook_OnTimeChanged(void);
extern void UI_Hook_OnBootTimeSyncBeaconRequested(void);
extern void UI_Hook_OnCatm1PowerFaultStopRequested(void);
__attribute__((weak)) bool GW_App_CopyTcpSnapshotRecord(const GW_HourRec_t* src, GW_HourRec_t* dst)
{
    if ((src == NULL) || (dst == NULL)) {
        return false;
    }

    *dst = *src;
    return true;
}
static bool prv_parse_cereg_stat(const char* rsp, uint8_t* stat);

static volatile bool s_catm1_busy = false;
static volatile bool s_catm1_session_at_ok = false;
static uint8_t s_catm1_rx_byte = 0u;
static uint8_t s_catm1_rb_mem[UI_CATM1_RX_RING_SIZE];
static UI_RingBuf_t s_catm1_rb;
static bool s_catm1_rb_ready = false;
static volatile uint32_t s_catm1_last_rx_ms = 0u;
static volatile uint32_t s_catm1_last_poweroff_ms = 0u;
static volatile uint32_t s_catm1_last_caopen_ms = 0u;
static volatile bool s_catm1_waiting_boot_sms_ready = false;
static volatile bool s_catm1_boot_sms_ready_seen = false;
static volatile bool s_catm1_tcp_open_fail_powerdown_pending = false;
static volatile bool s_catm1_expect_sms_ready_token = false;
static bool s_catm1_session_sms_ready_loop_seen = false;
static uint8_t s_catm1_sms_ready_loop_attempt_count = 0u;
static int64_t s_time_sync_delta_sec_buf[GW_CATM1_TIME_SYNC_DELTA_BUF_LEN];
static uint8_t s_time_sync_delta_wr = 0u;
static uint8_t s_time_sync_delta_count = 0u;
static bool s_catm1_time_auto_update_attempted_this_power = false;
static bool s_catm1_startup_apn_configured_this_power = false;
static bool s_catm1_bandcfg_applied_this_power = false;
/* Power-on 직후 *PSUTTZ/+PSUTTZ URC로 시간을 이미 받았는지 추적한다.
 * boot-time strict-order 구간에서는 URC로 받은 epoch를 보관해 두었다가
 * 사용자 요구 순서를 끝낸 뒤 즉시 반영할 수 있게 한다. */
static bool s_catm1_time_synced_from_urc_this_power = false;
static bool s_catm1_pending_psuttz_valid = false;
static uint64_t s_catm1_pending_psuttz_epoch_centi = 0u;
static bool s_catm1_tcp_send_session_active = false;
static bool s_catm1_tcp_time_sync_pending = false;
static bool s_catm1_boot_time_sync_strict_order_active = false;
static volatile bool s_catm1_server_cmd_ind_seen = false;
static volatile bool s_catm1_power_fault_stop_request = false;

static bool s_failed_snapshot_queued_valid = false;
static uint32_t s_failed_snapshot_queued_epoch_sec = 0u;
static uint8_t s_failed_snapshot_queued_gw_num = 0u;

#ifndef GW_CATM1_NTP_HOST
#define GW_CATM1_NTP_HOST "pool.ntp.org"
#endif
#ifndef GW_CATM1_NTP_TZ_QH
#define GW_CATM1_NTP_TZ_QH (36)
#endif
#ifndef GW_CATM1_STARTUP_CCLK_RETRY
#define GW_CATM1_STARTUP_CCLK_RETRY (20u)
#endif
#ifndef GW_CATM1_STARTUP_CCLK_GAP_MS
#define GW_CATM1_STARTUP_CCLK_GAP_MS (1500u)
#endif
#ifndef GW_CATM1_NTP_TIMEOUT_MS
#define GW_CATM1_NTP_TIMEOUT_MS (65000u)
#endif
#ifndef GW_CATM1_BOOT_URC_WAIT_MS
#define GW_CATM1_BOOT_URC_WAIT_MS (5000u)
#endif
#ifndef GW_CATM1_BOOT_QUIET_MS
#define GW_CATM1_BOOT_QUIET_MS (400u)
#endif
#ifndef GW_CATM1_SIM_READY_TIMEOUT_MS
#define GW_CATM1_SIM_READY_TIMEOUT_MS (20000u)
#endif
#ifndef GW_CATM1_START_SESSION_TIMEOUT_MS
#define GW_CATM1_START_SESSION_TIMEOUT_MS (45000u)
#endif
#ifndef GW_CATM1_STARTUP_SYNC_ATTEMPTS
#define GW_CATM1_STARTUP_SYNC_ATTEMPTS (3u)
#endif
#ifndef GW_CATM1_STARTUP_SYNC_RETRY_GAP_MS
#define GW_CATM1_STARTUP_SYNC_RETRY_GAP_MS (1500u)
#endif
#ifndef GW_CATM1_STARTUP_REG_WAIT_MS
#define GW_CATM1_STARTUP_REG_WAIT_MS (15000u)
#endif
#ifndef GW_CATM1_STARTUP_PS_WAIT_MS
#define GW_CATM1_STARTUP_PS_WAIT_MS (10000u)
#endif
#ifndef GW_CATM1_STARTUP_CCLK_FIRST_TRY
#define GW_CATM1_STARTUP_CCLK_FIRST_TRY (3u)
#endif
#ifndef GW_CATM1_STARTUP_CCLK_POST_REG_TRY
#define GW_CATM1_STARTUP_CCLK_POST_REG_TRY (3u)
#endif
#ifndef GW_CATM1_STARTUP_CCLK_POST_ATTACH_TRY
#define GW_CATM1_STARTUP_CCLK_POST_ATTACH_TRY (2u)
#endif
#ifndef GW_CATM1_CCLK_NO_RSP_DIRECT_RETRY_MAX
/* Power-on 직후에는 +CCLK? 응답이 잠깐 비는 경우가 있어,
 * 바로 AT resync로 들어가기 전에 짧게 연속 재질의한다. */
#define GW_CATM1_CCLK_NO_RSP_DIRECT_RETRY_MAX (3u)
#endif
#ifndef GW_CATM1_CCLK_NO_RSP_RETRY_GAP_MS
#define GW_CATM1_CCLK_NO_RSP_RETRY_GAP_MS (150u)
#endif
#ifndef GW_CATM1_AT_SYNC_MAX_TRY
#define GW_CATM1_AT_SYNC_MAX_TRY (4u)
#endif
#ifndef GW_CATM1_TCP_OPEN_HARD_TIMEOUT_MS
#define GW_CATM1_TCP_OPEN_HARD_TIMEOUT_MS (12000u)
#endif
#ifndef GW_CATM1_TCP_OPEN_SUCCESS_URC_TIMEOUT_MS
#define GW_CATM1_TCP_OPEN_SUCCESS_URC_TIMEOUT_MS (2000u)
#endif
#ifndef GW_CATM1_TCP_OPEN_FAIL_CPOWD_DELAY_MS
#define GW_CATM1_TCP_OPEN_FAIL_CPOWD_DELAY_MS (2000u)
#endif
#ifndef GW_CATM1_TCP_OPEN_FAIL_PWRDOWN_TIMEOUT_MS
#define GW_CATM1_TCP_OPEN_FAIL_PWRDOWN_TIMEOUT_MS (2000u)
#endif
#ifndef GW_CATM1_TCP_OPEN_FAIL_PWRDOWN_WAIT_MS
#define GW_CATM1_TCP_OPEN_FAIL_PWRDOWN_WAIT_MS (1000u)
#endif
#ifndef GW_CATM1_TCP_OPEN_FAIL_CPOWD_POST_TX_HOLD_MS
#define GW_CATM1_TCP_OPEN_FAIL_CPOWD_POST_TX_HOLD_MS (150u)
#endif
#ifndef GW_CATM1_TCP_POST_CLOSE_POWER_CUT_GUARD_MS
#define GW_CATM1_TCP_POST_CLOSE_POWER_CUT_GUARD_MS (300u)
#endif
#ifndef GW_CATM1_POST_TIME_SYNC_POWER_CUT_GUARD_MS
#define GW_CATM1_POST_TIME_SYNC_POWER_CUT_GUARD_MS (100u)
#endif
#ifndef GW_CATM1_CCLK_QUERY_COMP_MAX_CENTI
#define GW_CATM1_CCLK_QUERY_COMP_MAX_CENTI (250u)
#endif
#ifndef GW_CATM1_POST_SMS_READY_SETTLE_MS
#define GW_CATM1_POST_SMS_READY_SETTLE_MS (2000u)
#endif
#ifndef GW_CATM1_POST_SMS_READY_AT_SYNC_WINDOW_MS
#define GW_CATM1_POST_SMS_READY_AT_SYNC_WINDOW_MS (12000u)
#endif
#ifndef GW_CATM1_SERVER_CMD_WAIT_MS
#define GW_CATM1_SERVER_CMD_WAIT_MS (1000u)
#endif
#ifndef GW_CATM1_SERVER_CMD_READLEN
#define GW_CATM1_SERVER_CMD_READLEN (160u)
#endif
#ifndef GW_CATM1_SERVER_CMD_MAX_READ_PASSES
#define GW_CATM1_SERVER_CMD_MAX_READ_PASSES (3u)
#endif
#ifndef GW_CATM1_POST_SMS_READY_RETRY_GAP_MS
#define GW_CATM1_POST_SMS_READY_RETRY_GAP_MS (300u)
#endif
#ifndef GW_CATM1_POWER_CYCLE_GUARD_MS
#define GW_CATM1_POWER_CYCLE_GUARD_MS (3500u)
#endif
#ifndef GW_CATM1_RECENT_POWEROFF_SKIP_MS
#define GW_CATM1_RECENT_POWEROFF_SKIP_MS (1000u)
#endif
#ifndef GW_CATM1_SESSION_RESYNC_QUIET_MS
#define GW_CATM1_SESSION_RESYNC_QUIET_MS (200u)
#endif
#ifndef GW_CATM1_SESSION_RESYNC_MAX_WAIT_MS
#define GW_CATM1_SESSION_RESYNC_MAX_WAIT_MS (1200u)
#endif
#ifndef GW_CATM1_NET_REG_MIN_TIMEOUT_MS
#define GW_CATM1_NET_REG_MIN_TIMEOUT_MS (60000u)
#endif
#ifndef GW_CATM1_PS_ATTACH_MIN_TIMEOUT_MS
#define GW_CATM1_PS_ATTACH_MIN_TIMEOUT_MS (30000u)
#endif
#ifndef GW_CATM1_QUERY_FAIL_RESYNC_STREAK
#define GW_CATM1_QUERY_FAIL_RESYNC_STREAK (3u)
#endif
#ifndef GW_CATM1_CEREG_MAX_POLLS
#define GW_CATM1_CEREG_MAX_POLLS (10u)
#endif
#ifndef GW_CATM1_POST_CPIN_NETWORK_SETTLE_MS
#define GW_CATM1_POST_CPIN_NETWORK_SETTLE_MS (1500u)
#endif
#ifndef GW_CATM1_APN_BOOTSTRAP_CFUN_OFF_SETTLE_MS
#define GW_CATM1_APN_BOOTSTRAP_CFUN_OFF_SETTLE_MS (600u)
#endif
#ifndef GW_CATM1_APN_BOOTSTRAP_CFUN_ON_SETTLE_MS
#define GW_CATM1_APN_BOOTSTRAP_CFUN_ON_SETTLE_MS (1200u)
#endif
#ifndef GW_CATM1_APN_BOOTSTRAP_CFUN_TIMEOUT_MS
#define GW_CATM1_APN_BOOTSTRAP_CFUN_TIMEOUT_MS (5000u)
#endif
#ifndef GW_CATM1_APN_VERIFY_RETRY_MAX
#define GW_CATM1_APN_VERIFY_RETRY_MAX (3u)
#endif
#ifndef GW_CATM1_APN_VERIFY_GAP_MS
#define GW_CATM1_APN_VERIFY_GAP_MS (300u)
#endif
#ifndef GW_CATM1_APN_POST_CFUN1_SMS_READY_TIMEOUT_MS
#define GW_CATM1_APN_POST_CFUN1_SMS_READY_TIMEOUT_MS (5000u)
#endif
#ifndef GW_CATM1_BANDCFG_TIMEOUT_MS
#define GW_CATM1_BANDCFG_TIMEOUT_MS (10000u)
#endif
#ifndef GW_CATM1_CATM_BAND_CFG_CMD
#define GW_CATM1_CATM_BAND_CFG_CMD "AT+CBANDCFG=\"CAT-M\",1,3,5,8\r\n"
#endif
#ifndef GW_CATM1_COPS_AUTO_TIMEOUT_MS
#define GW_CATM1_COPS_AUTO_TIMEOUT_MS (30000u)
#endif
#ifndef GW_CATM1_COPS_AUTO_SETTLE_MS
#define GW_CATM1_COPS_AUTO_SETTLE_MS (1500u)
#endif
#ifndef GW_CATM1_CEREG_RESELECT_POLL
#define GW_CATM1_CEREG_RESELECT_POLL (4u)
#endif
#ifndef GW_CATM1_CEREG_POLL_GAP_MS
#define GW_CATM1_CEREG_POLL_GAP_MS (5000u)
#endif
#ifndef GW_CATM1_BOOT_CEREG_TIMEOUT_MS
#define GW_CATM1_BOOT_CEREG_TIMEOUT_MS (20000u)
#endif
#ifndef GW_CATM1_BOOT_CEREG_POLL_GAP_MS
#define GW_CATM1_BOOT_CEREG_POLL_GAP_MS (2500u)
#endif
#ifndef GW_CATM1_BOOT_CEREG_DIAG_STRIDE
#define GW_CATM1_BOOT_CEREG_DIAG_STRIDE (3u)
#endif
#ifndef GW_CATM1_BOOT_CSQ_TIMEOUT_MS
#define GW_CATM1_BOOT_CSQ_TIMEOUT_MS (1500u)
#endif
#ifndef GW_CATM1_BOOT_CPSI_TIMEOUT_MS
#define GW_CATM1_BOOT_CPSI_TIMEOUT_MS (2000u)
#endif
#ifndef GW_CATM1_BOOT_CEREG_RECOVERY_MAX
#define GW_CATM1_BOOT_CEREG_RECOVERY_MAX (1u)
#endif
#ifndef GW_CATM1_BOOT_CEREG_RECOVERY_SETTLE_MS
#define GW_CATM1_BOOT_CEREG_RECOVERY_SETTLE_MS (1500u)
#endif
#ifndef GW_CATM1_CGATT_ATTACH_TIMEOUT_MS
#define GW_CATM1_CGATT_ATTACH_TIMEOUT_MS (75000u)
#endif
#ifndef GW_CATM1_CGATT_ATTACH_SETTLE_MS
#define GW_CATM1_CGATT_ATTACH_SETTLE_MS (1200u)
#endif
#ifndef GW_CATM1_SERVER_CCLK_TIMEOUT_MS
#define GW_CATM1_SERVER_CCLK_TIMEOUT_MS (1500u)
#endif
#ifndef GW_CATM1_SERVER_CCLK_SYNC_RETRY
#define GW_CATM1_SERVER_CCLK_SYNC_RETRY (2u)
#endif
#ifndef GW_CATM1_SERVER_CCLK_SYNC_GAP_MS
#define GW_CATM1_SERVER_CCLK_SYNC_GAP_MS (150u)
#endif
#ifndef GW_CATM1_BOOT_FORCE_CCLK_TRY
#define GW_CATM1_BOOT_FORCE_CCLK_TRY (3u)
#endif
#ifndef GW_CATM1_BOOT_FORCE_CCLK_GAP_MS
#define GW_CATM1_BOOT_FORCE_CCLK_GAP_MS (1000u)
#endif
#ifndef GW_CATM1_BOOT_FORCE_CCLK_SETTLE_MS
#define GW_CATM1_BOOT_FORCE_CCLK_SETTLE_MS (300u)
#endif
#ifndef GW_CATM1_PROFILE_SAVE_SETTLE_MS
#define GW_CATM1_PROFILE_SAVE_SETTLE_MS (300u)
#endif
#ifndef GW_CATM1_POWEROFF_CFUN_TIMEOUT_MS
#define GW_CATM1_POWEROFF_CFUN_TIMEOUT_MS (2000u)
#endif
#ifndef GW_CATM1_POWEROFF_CFUN_SETTLE_MS
#define GW_CATM1_POWEROFF_CFUN_SETTLE_MS (300u)
#endif

#ifndef GW_CATM1_USER_BOOT_CEREG_MAX_POLLS
#define GW_CATM1_USER_BOOT_CEREG_MAX_POLLS (5u)
#endif
#ifndef GW_CATM1_USER_BOOT_CEREG_POLL_GAP_MS
#define GW_CATM1_USER_BOOT_CEREG_POLL_GAP_MS (3000u)
#endif
#ifndef GW_CATM1_USER_TCP_CEREG_MAX_POLLS
#define GW_CATM1_USER_TCP_CEREG_MAX_POLLS (5u)
#endif
#ifndef GW_CATM1_USER_TCP_CEREG_POLL_GAP_MS
#define GW_CATM1_USER_TCP_CEREG_POLL_GAP_MS (3000u)
#endif
#ifndef GW_CATM1_USER_BOOT_CCLK_MAX_TRY
#define GW_CATM1_USER_BOOT_CCLK_MAX_TRY (3u)
#endif
#ifndef GW_CATM1_USER_BOOT_CCLK_GAP_MS
#define GW_CATM1_USER_BOOT_CCLK_GAP_MS (1000u)
#endif

#ifndef GW_TCP_INTERNAL_TEMP_COMP_C
#define GW_TCP_INTERNAL_TEMP_COMP_C ((int8_t)-4)
#endif
#ifndef GW_CATM1_TIME_RESYNC_BEACON_DELTA_CENTI
#define GW_CATM1_TIME_RESYNC_BEACON_DELTA_CENTI (50u)
#endif

#ifndef GW_CATM1_SMS_READY_LOOP_STOP_TRY
#define GW_CATM1_SMS_READY_LOOP_STOP_TRY (3u)
#endif

static bool prv_is_trim_char(char ch);

static void prv_begin_sms_ready_loop_attempt(void)
{
    s_catm1_session_sms_ready_loop_seen = false;
}

static void prv_mark_sms_ready_loop_seen(void)
{
    if (s_catm1_busy) {
        s_catm1_session_sms_ready_loop_seen = true;
    }
}

static void prv_finish_sms_ready_loop_attempt(bool success)
{
    if (success) {
        s_catm1_session_sms_ready_loop_seen = false;
        s_catm1_sms_ready_loop_attempt_count = 0u;
        return;
    }

    if (!s_catm1_session_sms_ready_loop_seen) {
        return;
    }

    s_catm1_session_sms_ready_loop_seen = false;
    if (s_catm1_sms_ready_loop_attempt_count < 0xFFu) {
        s_catm1_sms_ready_loop_attempt_count++;
    }

    if (s_catm1_sms_ready_loop_attempt_count == GW_CATM1_SMS_READY_LOOP_STOP_TRY) {
        s_catm1_power_fault_stop_request = true;
        UI_Hook_OnCatm1PowerFaultStopRequested();
    }
}

static bool prv_is_trimmed_line_equal(const char* begin, const char* end, const char* token)
{
    size_t tok_len;

    if ((begin == NULL) || (end == NULL) || (token == NULL) || (end < begin)) {
        return false;
    }

    while ((begin < end) && prv_is_trim_char(*begin)) {
        begin++;
    }
    while ((end > begin) && prv_is_trim_char(end[-1])) {
        end--;
    }

    tok_len = strlen(token);
    if ((size_t)(end - begin) != tok_len) {
        return false;
    }

    return (strncmp(begin, token, tok_len) == 0);
}

static bool prv_tcp_blocked_by_ble(void)
{
#if UI_HAVE_BT_EN
    if (!UI_BLE_IsActive()) {
        return false;
    }
    /* TEST START 등으로 BLE를 persistent하게 유지한 상태에서는
     * 현장 확인용 BLE와 TCP uplink를 동시에 허용한다.
     * 일반 짧은 BLE 세션만 CAT-M1 TCP와 상호배제를 유지한다. */
    return !UI_BLE_IsPersistent();
#else
    return false;
#endif
}

static int8_t prv_apply_tcp_temp_comp_c(int8_t temp_c)
{
    int16_t v;

    if (temp_c == UI_NODE_TEMP_INVALID_C) {
        return UI_NODE_TEMP_INVALID_C;
    }

    v = (int16_t)temp_c + (int16_t)GW_TCP_INTERNAL_TEMP_COMP_C;
    if (v < (int16_t)UI_NODE_TEMP_MIN_C) {
        v = (int16_t)UI_NODE_TEMP_MIN_C;
    }
    if (v > (int16_t)UI_NODE_TEMP_MAX_C) {
        v = (int16_t)UI_NODE_TEMP_MAX_C;
    }
    return (int8_t)v;
}

static const char* prv_gw_voltage_text(uint8_t gw_volt_x10)
{
    if (gw_volt_x10 == 0xFFu) {
        return "-";
    }
    return (gw_volt_x10 >= UI_NODE_BATT_LOW_THRESHOLD_X10) ? "3.5" : "LOW";
}

static const char* prv_node_voltage_text(uint8_t batt_lvl)
{
    if (batt_lvl == UI_NODE_BATT_LVL_INVALID) {
        return "-";
    }
    return (batt_lvl == UI_NODE_BATT_LVL_NORMAL) ? "3.5" : "LOW";
}

static bool prv_is_leap_year(uint16_t year)
{
    return ((year % 4u) == 0u) && ((((year % 100u) != 0u) || ((year % 400u) == 0u)));
}

static uint8_t prv_days_in_month(uint16_t year, uint8_t month)
{
    static const uint8_t s_days[12] = { 31u, 28u, 31u, 30u, 31u, 30u, 31u, 31u, 30u, 31u, 30u, 31u };

    if ((month == 0u) || (month > 12u)) {
        return 30u;
    }
    if ((month == 2u) && prv_is_leap_year(year)) {
        return 29u;
    }
    return s_days[month - 1u];
}

static void prv_format_epoch2016(uint32_t epoch_sec, char* out, size_t out_sz)
{
    uint32_t days;
    uint32_t rem;
    uint16_t year = 2016u;
    uint8_t month = 1u;
    uint8_t day;
    uint8_t hour;
    uint8_t min;
    uint8_t sec;

    if ((out == NULL) || (out_sz == 0u)) {
        return;
    }

    days = epoch_sec / 86400u;
    rem = epoch_sec % 86400u;

    while (1) {
        uint32_t diy = prv_is_leap_year(year) ? 366u : 365u;
        if (days < diy) {
            break;
        }
        days -= diy;
        year++;
    }

    while (1) {
        uint8_t dim = prv_days_in_month(year, month);
        if (days < dim) {
            break;
        }
        days -= dim;
        month++;
    }

    day = (uint8_t)(days + 1u);
    hour = (uint8_t)(rem / 3600u);
    rem %= 3600u;
    min = (uint8_t)(rem / 60u);
    sec = (uint8_t)(rem % 60u);

    (void)snprintf(out, out_sz, "%04u-%02u-%02u %02u:%02u:%02u",
                   (unsigned)year, (unsigned)month, (unsigned)day,
                   (unsigned)hour, (unsigned)min, (unsigned)sec);
}

static bool prv_activate_pdp(void);
static bool prv_activate_boot_time_pdp(void);
static bool prv_wait_boot_time_sync_registered(uint32_t timeout_ms);
static bool prv_prepare_apn_before_time_sync(void);
static void prv_try_apply_korea_catm_bands(void);
static void prv_enable_network_time_auto_update(void);
static bool prv_sync_time_from_modem_startup_try(bool run_time_auto_update_setup,
                                                  bool abort_on_initial_invalid,
                                                  bool* out_initial_invalid_cclk);
static bool prv_sync_time_from_modem_quick(bool notify_hook);
static void prv_force_power_cut(void);
static bool prv_was_powered_off_recently(uint32_t window_ms);
static void prv_abort_tcp_open_and_power_off(uint32_t caopen_ms);
static void prv_shutdown_modem_prefer_poweroff(void);
static void prv_shutdown_modem_after_tcp_close(void);
static bool prv_try_normal_power_down(bool reduce_current_first);
static void prv_close_tcp_and_force_power_cut(bool opened, char* rsp, size_t rsp_sz);
static void prv_note_failed_snapshot_sent(void);
static bool prv_wait_eps_registered(void);
static bool prv_wait_eps_registered_until(uint32_t timeout_ms);
static bool prv_wait_ps_attached(void);
static bool prv_wait_ps_attached_until(uint32_t timeout_ms);
static bool prv_try_session_resync(void);
static void prv_try_restore_legacy_attach_profile(void);
static void prv_delay_ms(uint32_t ms);
static void prv_wait_rx_quiet(uint32_t quiet_ms, uint32_t max_wait_ms);
static bool prv_query_network_time_epoch_retry(uint32_t max_try, uint32_t gap_ms,
                                               bool break_on_invalid, bool* out_invalid_seen,
                                               uint64_t* out_epoch_centi);
static bool prv_apply_time_auto_update_cfg(void);
static bool prv_request_auto_operator_select(bool force_send);
static bool prv_is_eps_registered_stat(uint8_t stat);
static bool prv_query_signal_quality(uint8_t* out_rssi, uint8_t* out_ber);
static void prv_capture_attach_debug_snapshot(bool include_cpsi);
static bool prv_recover_boot_time_sync_registration(void);
static void prv_prepare_low_current_before_poweroff(void);
static bool prv_have_time_from_boot_urc_this_power(void);
static bool prv_apply_pending_psuttz_time(bool notify_hook);
static void prv_consume_deferred_tcp_session_time_sync(void);
static bool prv_force_cclk_time_sync(uint32_t max_try, uint32_t gap_ms, bool notify_hook);
static bool prv_force_cclk_time_sync_after_ctzu(bool notify_hook);
static bool prv_finish_time_sync_from_boot_urc(bool notify_hook);
static bool prv_wait_cereg_registered_fixed(uint32_t max_polls, uint32_t gap_ms);
static bool prv_startup_time_sync_user_sequence(void);
static bool prv_prepare_tcp_send_user_sequence(void);
static bool prv_rsp_contains_ok_line(const char* rsp);
static bool prv_query_tcp_connected_state(uint8_t cid);

static void prv_catm1_rb_reset(void)
{
    UI_RingBuf_Init(&s_catm1_rb, s_catm1_rb_mem, UI_CATM1_RX_RING_SIZE);
    s_catm1_rb_ready = true;
    s_catm1_last_rx_ms = HAL_GetTick();
}

static void prv_time_sync_delta_reset(void)
{
    uint32_t i;
    for (i = 0u; i < GW_CATM1_TIME_SYNC_DELTA_BUF_LEN; i++) {
        s_time_sync_delta_sec_buf[i] = 0;
    }
    s_time_sync_delta_wr = 0u;
    s_time_sync_delta_count = 0u;
}

static void prv_time_sync_delta_push(int64_t delta_sec)
{
    s_time_sync_delta_sec_buf[s_time_sync_delta_wr] = delta_sec;
    s_time_sync_delta_wr = (uint8_t)((s_time_sync_delta_wr + 1u) % GW_CATM1_TIME_SYNC_DELTA_BUF_LEN);
    if (s_time_sync_delta_count < GW_CATM1_TIME_SYNC_DELTA_BUF_LEN) {
        s_time_sync_delta_count++;
    }
}

static bool prv_is_trim_char(char ch)
{
    return ((ch == ' ') || (ch == '\t') || (ch == '\r') || (ch == '\n'));
}

static bool prv_parse_psuttz_epoch(const char* line, uint64_t* out_epoch_centi)
{
    const char* p;
    int year = 0;
    int mon = 0;
    int day = 0;
    int hh = 0;
    int mm = 0;
    int ss = 0;
    int dst = 0;
    int tz_qh = 0;
    int n;
    int64_t epoch_centi;
    char tz_str[8] = {0};
    UI_DateTime_t dt = {0};

    if ((line == NULL) || (out_epoch_centi == NULL)) {
        return false;
    }

    p = strstr(line, "*PSUTTZ:");
    if (p == NULL) {
        p = strstr(line, "+PSUTTZ:");
    }
    if (p == NULL) {
        return false;
    }
    p += 8;
    while ((*p == ' ') || (*p == '	')) {
        p++;
    }

    /* SIM7080 commonly reports *PSUTTZ as yy/MM/dd,hh:mm:ss,"+zz",dst,
     * while older docs also show comma-only fields. Accept both forms. */
    n = sscanf(p, "%d/%d/%d,%d:%d:%d,\"%7[^\"]\",%d",
               &year, &mon, &day, &hh, &mm, &ss, tz_str, &dst);
    if (n < 7) {
        n = sscanf(p, "%d,%d,%d,%d,%d,%d,\"%7[^\"]\",%d",
                   &year, &mon, &day, &hh, &mm, &ss, tz_str, &dst);
    }
    if (n < 7) {
        n = sscanf(p, "%d/%d/%d,%d,%d,%d,\"%7[^\"]\",%d",
                   &year, &mon, &day, &hh, &mm, &ss, tz_str, &dst);
    }
    if (n < 7) {
        return false;
    }
    (void)dst;

    if ((year >= 0) && (year < 100)) {
        year += 2000;
    }

    if ((year < 2016) || (year > 2099) ||
        (mon < 1) || (mon > 12) ||
        (day < 1) || (day > 31) ||
        (hh < 0) || (hh > 23) ||
        (mm < 0) || (mm > 59) ||
        (ss < 0) || (ss > 59)) {
        return false;
    }

    dt.year = (uint16_t)year;
    dt.month = (uint8_t)mon;
    dt.day = (uint8_t)day;
    dt.hour = (uint8_t)hh;
    dt.min = (uint8_t)mm;
    dt.sec = (uint8_t)ss;
    dt.centi = 0u;

    epoch_centi = (int64_t)((uint64_t)UI_Time_Epoch2016_FromCalendar(&dt) * 100u);

    if ((tz_str[0] == '+') || (tz_str[0] == '-')) {
        if (sscanf(&tz_str[1], "%d", &tz_qh) != 1) {
            return false;
        }
        if (tz_str[0] == '-') {
            tz_qh = -tz_qh;
        }
    } else if (sscanf(tz_str, "%d", &tz_qh) != 1) {
        return false;
    }

    /* *PSUTTZ is UTC time; convert to the same local-time epoch basis used by +CCLK. */
    epoch_centi += ((int64_t)tz_qh * 15LL * 60LL * 100LL);
    if (epoch_centi < 0) {
        return false;
    }

    *out_epoch_centi = (uint64_t)epoch_centi;
    return true;
}

static uint64_t prv_uabs_i64(int64_t v)
{
    return (v < 0) ? (uint64_t)(-v) : (uint64_t)v;
}

static void prv_apply_time_epoch(uint64_t epoch_centi, bool notify_hook)
{
    uint64_t now_centi = UI_Time_NowCenti2016();
    int64_t delta_centi = (int64_t)epoch_centi - (int64_t)now_centi;
    int64_t delta_sec = (int64_t)(epoch_centi / 100u) - (int64_t)(now_centi / 100u);

    prv_time_sync_delta_push(delta_sec);
    UI_Time_SetEpochCenti2016(epoch_centi);
    if (notify_hook) {
        UI_Hook_OnTimeChanged();
        if (!s_catm1_boot_time_sync_strict_order_active &&
            (prv_uabs_i64(delta_centi) >= GW_CATM1_TIME_RESYNC_BEACON_DELTA_CENTI)) {
            UI_Hook_OnBootTimeSyncBeaconRequested();
        }
    }
}

static bool prv_have_time_from_boot_urc_this_power(void)
{
    return (s_catm1_time_synced_from_urc_this_power && UI_Time_IsValid());
}

static bool prv_apply_pending_psuttz_time(bool notify_hook)
{
    if (!s_catm1_pending_psuttz_valid) {
        return false;
    }

    prv_apply_time_epoch(s_catm1_pending_psuttz_epoch_centi, notify_hook);
    s_catm1_pending_psuttz_valid = false;
    return true;
}

static bool prv_apply_pending_psuttz_time_with_cclk(bool notify_hook)
{
    if (!s_catm1_pending_psuttz_valid) {
        return false;
    }

    /* URC로 시간을 먼저 받았더라도 stop으로 내려가기 전 한 번은 AT+CCLK?로 보정한다.
     * +CCLK?가 실패하면 보관해 둔 PSUTTZ epoch를 그대로 적용한다. */
    if (prv_sync_time_from_modem_quick(notify_hook)) {
        s_catm1_pending_psuttz_valid = false;
        return true;
    }

    return prv_apply_pending_psuttz_time(notify_hook);
}

static void prv_consume_deferred_tcp_session_time_sync(void)
{
    if (!s_catm1_pending_psuttz_valid) {
        return;
    }

    /* TCP 전송 중 비동기 +PSUTTZ/*PSUTTZ가 들어오면 즉시 UI_Hook_OnTimeChanged()를
     * 올리지 말고, 현재 uplink가 끝난 뒤 조용히 시간을 반영한다.
     * 가능하면 AT+CCLK?로 한 번 더 보정하고, 실패하면 보관해 둔 URC epoch를 쓴다. */
    (void)prv_apply_pending_psuttz_time_with_cclk(false);
}

static bool prv_force_cclk_time_sync(uint32_t max_try, uint32_t gap_ms, bool notify_hook)
{
    uint64_t epoch_centi = 0u;

    if (max_try == 0u) {
        return false;
    }

    if (!prv_query_network_time_epoch_retry(max_try, gap_ms, false, NULL, &epoch_centi)) {
        return false;
    }

    prv_apply_time_epoch(epoch_centi, notify_hook);
    return true;
}

static bool prv_force_cclk_time_sync_after_ctzu(bool notify_hook)
{
    if (!s_catm1_time_auto_update_attempted_this_power) {
        return false;
    }

    prv_wait_rx_quiet(100u, 400u);
    prv_delay_ms(GW_CATM1_BOOT_FORCE_CCLK_SETTLE_MS);
    return prv_force_cclk_time_sync(GW_CATM1_BOOT_FORCE_CCLK_TRY,
                                    GW_CATM1_BOOT_FORCE_CCLK_GAP_MS,
                                    notify_hook);
}

static bool prv_finish_time_sync_from_boot_urc(bool notify_hook)
{
    if (!prv_have_time_from_boot_urc_this_power()) {
        return false;
    }

    /* *PSUTTZ로 시간을 먼저 받더라도 boot-time sync 성공으로 끝내기 전에는
     * 실제 +CCLK? 응답을 한 번 이상 받아 같은 세션에서 최종 시간을 적용한다. */
    return prv_force_cclk_time_sync_after_ctzu(notify_hook);
}

static bool prv_try_consume_async_urc_line(const char* line, size_t line_len)
{
    const char* begin = line;
    const char* end = line + line_len;
    uint64_t epoch_centi = 0u;
    size_t copy_len;
    char tmp[96];

    while ((begin < end) && prv_is_trim_char(*begin)) {
        begin++;
    }
    while ((end > begin) && prv_is_trim_char(end[-1])) {
        end--;
    }
    if (end <= begin) {
        return false;
    }

    if (prv_is_trimmed_line_equal(begin, end, "SMS Ready")) {
        if (s_catm1_expect_sms_ready_token) {
            return false;
        }

        prv_mark_sms_ready_loop_seen();
        return true;
    }

    if ((strncmp(begin, "*PSUTTZ:", 8) != 0) &&
        (strncmp(begin, "+PSUTTZ:", 8) != 0)) {
        return false;
    }

    copy_len = (size_t)(end - begin);
    if (copy_len >= sizeof(tmp)) {
        copy_len = sizeof(tmp) - 1u;
    }
    memcpy(tmp, begin, copy_len);
    tmp[copy_len] = '\0';

    if (prv_parse_psuttz_epoch(tmp, &epoch_centi)) {
        if (s_catm1_boot_time_sync_strict_order_active || s_catm1_tcp_send_session_active) {
            s_catm1_pending_psuttz_epoch_centi = epoch_centi;
            s_catm1_pending_psuttz_valid = true;
        } else {
            prv_apply_time_epoch(epoch_centi, true);
        }
        s_catm1_time_synced_from_urc_this_power = true;
    }
    return true;
}

static void prv_filter_async_urc_buffer(char* buf)
{
    size_t len;
    size_t rd = 0u;
    size_t wr = 0u;

    if (buf == NULL) {
        return;
    }

    len = strlen(buf);
    while (rd < len) {
        size_t line_start = rd;
        size_t line_end = rd;
        bool have_term = false;

        while (line_end < len) {
            if (buf[line_end] == '\n') {
                line_end++;
                have_term = true;
                break;
            }
            line_end++;
        }

        if (!have_term) {
            if (wr != line_start) {
                memmove(&buf[wr], &buf[line_start], len - line_start);
            }
            wr += (len - line_start);
            break;
        }

        if (!prv_try_consume_async_urc_line(&buf[line_start], line_end - line_start)) {
            if (wr != line_start) {
                memmove(&buf[wr], &buf[line_start], line_end - line_start);
            }
            wr += (line_end - line_start);
        }
        rd = line_end;
    }

    buf[wr] = '\0';
}

static void prv_catm1_rx_start_it(void)
{
    HAL_StatusTypeDef st;
    if (!s_catm1_rb_ready) {
        prv_catm1_rb_reset();
    }
    st = HAL_UART_Receive_IT(&hlpuart1, &s_catm1_rx_byte, 1u);
    if ((st != HAL_OK) && (st != HAL_BUSY)) {
    }
}

static bool prv_catm1_rb_pop_wait(uint8_t* out, uint32_t timeout_ms)
{
    uint32_t start = HAL_GetTick();
    if (out == NULL) {
        return false;
    }
    if (!s_catm1_rb_ready) {
        prv_catm1_rb_reset();
    }
    while ((uint32_t)(HAL_GetTick() - start) < timeout_ms) {
        if (UI_RingBuf_Pop(&s_catm1_rb, out)) {
            return true;
        }
        HAL_Delay(1u);
    }
    return UI_RingBuf_Pop(&s_catm1_rb, out);
}

static void prv_delay_ms(uint32_t ms)
{
    HAL_Delay(ms);
}

static void prv_wait_rx_quiet(uint32_t quiet_ms, uint32_t max_wait_ms)
{
    uint32_t start = HAL_GetTick();
    uint32_t last_rx = start;
    uint8_t ch = 0u;

    while ((uint32_t)(HAL_GetTick() - start) < max_wait_ms) {
        if (prv_catm1_rb_pop_wait(&ch, 20u)) {
            if (ch != 0u) {
                last_rx = HAL_GetTick();
            }
        }
        if ((uint32_t)(HAL_GetTick() - last_rx) >= quiet_ms) {
            break;
        }
    }
}

static void prv_power_leds_write(GPIO_PinState state)
{
#if defined(LED0_GPIO_Port) && defined(LED0_Pin)
    HAL_GPIO_WritePin(LED0_GPIO_Port, LED0_Pin, state);
#endif
#if defined(LED1_GPIO_Port) && defined(LED1_Pin)
    HAL_GPIO_WritePin(LED1_GPIO_Port, LED1_Pin, state);
#endif
}

static void prv_power_leds_blink_twice(void)
{
    uint32_t i;
    for (i = 0u; i < 2u; i++) {
        prv_power_leds_write(GPIO_PIN_SET);
        HAL_Delay(100u);
        prv_power_leds_write(GPIO_PIN_RESET);
        HAL_Delay(100u);
    }
}

static void prv_catm1_uart_pins_to_analog(void)
{
    GPIO_InitTypeDef GPIO_InitStruct = {0};

#if defined(CATM1_RX_GPIO_Port) && defined(CATM1_RX_Pin)
    GPIO_InitStruct.Pin = CATM1_RX_Pin;
    GPIO_InitStruct.Mode = GPIO_MODE_ANALOG;
    GPIO_InitStruct.Pull = GPIO_NOPULL;
    HAL_GPIO_Init(CATM1_RX_GPIO_Port, &GPIO_InitStruct);
#endif
#if defined(CATM1_TX_GPIO_Port) && defined(CATM1_TX_Pin)
    GPIO_InitStruct.Pin = CATM1_TX_Pin;
    GPIO_InitStruct.Mode = GPIO_MODE_ANALOG;
    GPIO_InitStruct.Pull = GPIO_NOPULL;
    HAL_GPIO_Init(CATM1_TX_GPIO_Port, &GPIO_InitStruct);
#endif
}

static void prv_lpuart_force_idle_low_power(void)
{
#if defined(PWR_KEY_Pin)
    HAL_GPIO_WritePin(PWR_KEY_GPIO_Port, PWR_KEY_Pin, UI_CATM1_PWRKEY_INACTIVE_STATE);
#endif

    if ((hlpuart1.gState != HAL_UART_STATE_RESET) || (hlpuart1.RxState != HAL_UART_STATE_RESET)) {
#if defined(HAL_UART_MODULE_ENABLED)
        (void)HAL_UART_Abort(&hlpuart1);
#endif
        (void)HAL_UART_DeInit(&hlpuart1);
    }

    prv_catm1_uart_pins_to_analog();

#if defined(LPUART1_IRQn)
    HAL_NVIC_ClearPendingIRQ(LPUART1_IRQn);
#endif
}

static bool prv_lpuart_is_inited(void)
{
    return ((hlpuart1.gState != HAL_UART_STATE_RESET) || (hlpuart1.RxState != HAL_UART_STATE_RESET));
}

static bool prv_lpuart_ensure(void)
{
    if (hlpuart1.Instance == NULL) {
        return false;
    }
    if (!prv_lpuart_is_inited()) {
        if (HAL_UART_Init(&hlpuart1) != HAL_OK) {
            return false;
        }
    }
    if (!s_catm1_rb_ready) {
        prv_catm1_rb_reset();
    }
    prv_catm1_rx_start_it();
    return true;
}

static void prv_lpuart_release(void)
{
    prv_lpuart_force_idle_low_power();
    if (s_catm1_rb_ready) {
        prv_catm1_rb_reset();
    }
}

static void prv_get_server(uint8_t ip[4], uint16_t* port)
{
    const UI_Config_t* cfg = UI_GetConfig();
    bool ip_zero;

    ip[0] = cfg->tcpip_ip[0];
    ip[1] = cfg->tcpip_ip[1];
    ip[2] = cfg->tcpip_ip[2];
    ip[3] = cfg->tcpip_ip[3];
    *port = cfg->tcpip_port;

    ip_zero = ((ip[0] == 0u) && (ip[1] == 0u) && (ip[2] == 0u) && (ip[3] == 0u));
    if (ip_zero || (*port < UI_TCPIP_MIN_PORT)) {
        ip[0] = UI_TCPIP_DEFAULT_IP0;
        ip[1] = UI_TCPIP_DEFAULT_IP1;
        ip[2] = UI_TCPIP_DEFAULT_IP2;
        ip[3] = UI_TCPIP_DEFAULT_IP3;
        *port = UI_TCPIP_DEFAULT_PORT;
    }
}

static bool prv_uart_send_bytes(const void* data, uint16_t len, uint32_t timeout_ms)
{
    if (s_catm1_waiting_boot_sms_ready && !s_catm1_boot_sms_ready_seen) {
        return false;
    }
    if (!prv_lpuart_ensure()) {
        return false;
    }
    return (HAL_UART_Transmit(&hlpuart1, (uint8_t*)(uintptr_t)data, len, timeout_ms) == HAL_OK);
}

static bool prv_uart_send_text(const char* s, uint32_t timeout_ms)
{
    return prv_uart_send_bytes(s, (uint16_t)strlen(s), timeout_ms);
}

static void prv_uart_flush_rx(void)
{
    if (!prv_lpuart_ensure()) {
        return;
    }
    prv_catm1_rb_reset();
    prv_catm1_rx_start_it();
}

static bool prv_uart_wait_for(char* out, size_t out_sz, uint32_t timeout_ms,
                              const char* tok1, const char* tok2, const char* tok3)
{
    uint8_t ch;
    uint32_t start = HAL_GetTick();
    size_t n = 0u;

    if ((out == NULL) || (out_sz == 0u)) {
        return false;
    }

    out[0] = '\0';
    while ((uint32_t)(HAL_GetTick() - start) < timeout_ms) {
        if (!prv_catm1_rb_pop_wait(&ch, 20u)) {
            continue;
        }
        if (ch == 0u) {
            continue;
        }
        if ((n + 1u) < out_sz) {
            out[n++] = (char)ch;
            out[n] = '\0';
        } else if (out_sz > 16u) {
            size_t keep = (out_sz / 2u);
            memmove(out, &out[out_sz - keep - 1u], keep);
            n = keep;
            out[n++] = (char)ch;
            out[n] = '\0';
        }
        prv_filter_async_urc_buffer(out);
        n = strlen(out);
        if ((strstr(out, "ERROR") != NULL) || (strstr(out, "+CME ERROR") != NULL)) {
            return false;
        }
        if ((tok1 != NULL) && (strstr(out, tok1) != NULL)) {
            return true;
        }
        if ((tok2 != NULL) && (strstr(out, tok2) != NULL)) {
            return true;
        }
        if ((tok3 != NULL) && (strstr(out, tok3) != NULL)) {
            return true;
        }
    }
    return false;
}

static bool prv_send_cmd_wait(const char* cmd, const char* tok1, const char* tok2, const char* tok3,
                              uint32_t timeout_ms, char* out, size_t out_sz)
{
    prv_uart_flush_rx();
    if (!prv_uart_send_text(cmd, UI_CATM1_AT_TIMEOUT_MS)) {
        return false;
    }
    return prv_uart_wait_for(out, out_sz, timeout_ms, tok1, tok2, tok3);
}

static bool prv_send_query_wait_ok(const char* cmd, uint32_t timeout_ms, char* out, size_t out_sz)
{
    return prv_send_cmd_wait(cmd, "OK", NULL, NULL, timeout_ms, out, out_sz);
}

static bool prv_send_query_wait_prefix_ok(const char* cmd, const char* prefix,
                                          uint32_t timeout_ms, char* out, size_t out_sz)
{
    uint8_t ch;
    uint32_t start = HAL_GetTick();
    uint32_t last_rx_tick = start;
    bool saw_ok_after = false;
    size_t n = 0u;

    if ((cmd == NULL) || (out == NULL) || (out_sz == 0u)) {
        return false;
    }
    if (prefix == NULL) {
        return prv_send_query_wait_ok(cmd, timeout_ms, out, out_sz);
    }

    prv_uart_flush_rx();
    if (!prv_uart_send_text(cmd, UI_CATM1_AT_TIMEOUT_MS)) {
        return false;
    }

    out[0] = '\0';
    while ((uint32_t)(HAL_GetTick() - start) < timeout_ms) {
        const char* pfx;
        const char* line_end;
        const char* ok_after;

        if (!prv_catm1_rb_pop_wait(&ch, UI_CATM1_QUERY_RX_POLL_MS)) {
            if (saw_ok_after && ((uint32_t)(HAL_GetTick() - last_rx_tick) >= UI_CATM1_QUERY_OK_IDLE_MS)) {
                return true;
            }
            continue;
        }
        if (ch == 0u) {
            continue;
        }
        last_rx_tick = HAL_GetTick();
        if ((n + 1u) < out_sz) {
            out[n++] = (char)ch;
            out[n] = '\0';
        } else if (out_sz > 16u) {
            size_t keep = (out_sz / 2u);
            memmove(out, &out[out_sz - keep - 1u], keep);
            n = keep;
            out[n++] = (char)ch;
            out[n] = '\0';
        }
        prv_filter_async_urc_buffer(out);
        n = strlen(out);
        if ((strstr(out, "ERROR") != NULL) || (strstr(out, "+CME ERROR") != NULL)) {
            return false;
        }
        pfx = strstr(out, prefix);
        if (pfx == NULL) {
            continue;
        }
        line_end = strstr(pfx, "\r\n");
        if (line_end == NULL) {
            line_end = strchr(pfx, '\n');
        }
        if (line_end == NULL) {
            continue;
        }
        ok_after = strstr(line_end, "\r\nOK\r\n");
        if (ok_after == NULL) {
            ok_after = strstr(line_end, "\nOK\r\n");
        }
        if (ok_after == NULL) {
            ok_after = strstr(line_end, "\nOK\n");
        }
        if (ok_after != NULL) {
            saw_ok_after = true;
        }
    }
    return saw_ok_after;
}

static bool prv_send_query_wait_cnact_ok(char* out, size_t out_sz)
{
    uint8_t ch;
    uint32_t start = HAL_GetTick();
    bool saw_cnact = false;
    size_t n = 0u;

    if ((out == NULL) || (out_sz == 0u)) {
        return false;
    }

    prv_uart_flush_rx();
    if (!prv_uart_send_text("AT+CNACT?\r\n", UI_CATM1_AT_TIMEOUT_MS)) {
        return false;
    }

    out[0] = '\0';
    while ((uint32_t)(HAL_GetTick() - start) < UI_CATM1_CNACT_QUERY_TIMEOUT_MS) {
        if (!prv_catm1_rb_pop_wait(&ch, UI_CATM1_QUERY_RX_POLL_MS)) {
            continue;
        }
        if (ch == 0u) {
            continue;
        }
        if ((n + 1u) < out_sz) {
            out[n++] = (char)ch;
            out[n] = '\0';
        } else if (out_sz > 16u) {
            size_t keep = (out_sz / 2u);
            memmove(out, &out[out_sz - keep - 1u], keep);
            n = keep;
            out[n++] = (char)ch;
            out[n] = '\0';
        }
        prv_filter_async_urc_buffer(out);
        n = strlen(out);
        if ((strstr(out, "ERROR") != NULL) || (strstr(out, "+CME ERROR") != NULL)) {
            return false;
        }
        if (strstr(out, "+CNACT:") != NULL) {
            saw_cnact = true;
        }
        if (saw_cnact) {
            if ((strstr(out, "\r\nOK\r\n") != NULL) || (strstr(out, "\nOK\r\n") != NULL) || (strstr(out, "\nOK\n") != NULL)) {
                return true;
            }
        }
    }
    return false;
}

static bool prv_send_at_sync(void)
{
    char rsp[UI_CATM1_RX_BUF_SZ];
    uint32_t i;
    uint32_t max_try = GW_CATM1_AT_SYNC_MAX_TRY;

    if ((UI_CATM1_AT_SYNC_RETRY != 0u) && (UI_CATM1_AT_SYNC_RETRY < max_try)) {
        max_try = UI_CATM1_AT_SYNC_RETRY;
    }
    if (max_try == 0u) {
        max_try = GW_CATM1_AT_SYNC_MAX_TRY;
    }

    for (i = 0u; i < max_try; i++) {
        rsp[0] = '\0';
        prv_uart_flush_rx();
        if (!prv_uart_send_text("AT\r\n", UI_CATM1_AT_TIMEOUT_MS)) {
            prv_delay_ms(UI_CATM1_AT_SYNC_GAP_MS);
            continue;
        }
        if (prv_uart_wait_for(rsp, sizeof(rsp), UI_CATM1_AT_TIMEOUT_MS, "OK", NULL, NULL)) {
            s_catm1_session_at_ok = true;
            (void)prv_send_cmd_wait("ATE0\r\n", "OK", NULL, NULL, UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp));
            (void)prv_send_cmd_wait("AT+CMEE=2\r\n", "OK", NULL, NULL, UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp));
            return true;
        }
        prv_delay_ms(UI_CATM1_AT_SYNC_GAP_MS);
    }
    return false;
}

static bool prv_wait_sim_ready(void)
{
    char rsp[UI_CATM1_RX_BUF_SZ];
    uint32_t start = HAL_GetTick();

    while ((uint32_t)(HAL_GetTick() - start) < GW_CATM1_SIM_READY_TIMEOUT_MS) {
        if (prv_send_query_wait_prefix_ok("AT+CPIN?\r\n", "+CPIN:", UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp)) &&
            (strstr(rsp, "+CPIN: READY") != NULL)) {
            return true;
        }
        prv_wait_rx_quiet(200u, 800u);
    }
    return false;
}

static bool prv_rsp_has_boot_ready_fallback(const char* rsp)
{
    if (rsp == NULL) {
        return false;
    }

    if ((strstr(rsp, "\r\nRDY\r\n") != NULL) ||
        (strstr(rsp, "\nRDY\r\n") != NULL) ||
        (strstr(rsp, "\nRDY\n") != NULL) ||
        (strstr(rsp, "\r\n+CFUN: 1\r\n") != NULL) ||
        (strstr(rsp, "\n+CFUN: 1\r\n") != NULL) ||
        (strstr(rsp, "\n+CFUN: 1\n") != NULL) ||
        (strstr(rsp, "\r\n+CFUN:1\r\n") != NULL) ||
        (strstr(rsp, "\n+CFUN:1\r\n") != NULL) ||
        (strstr(rsp, "\n+CFUN:1\n") != NULL) ||
        (strstr(rsp, "+CPIN: READY") != NULL)) {
        return true;
    }

    return false;
}

static bool prv_wait_boot_sms_ready(void)
{
    char rsp[UI_CATM1_RX_BUF_SZ];
    uint8_t ch = 0u;
    uint32_t start = HAL_GetTick();
    size_t n = 0u;

    rsp[0] = '\0';
    s_catm1_expect_sms_ready_token = true;
    while ((uint32_t)(HAL_GetTick() - start) < GW_CATM1_BOOT_URC_WAIT_MS) {
        if (!prv_catm1_rb_pop_wait(&ch, 20u)) {
            continue;
        }
        if (ch == 0u) {
            continue;
        }

        if ((n + 1u) < sizeof(rsp)) {
            rsp[n++] = (char)ch;
            rsp[n] = '\0';
        } else if (sizeof(rsp) > 16u) {
            size_t keep = (sizeof(rsp) / 2u);
            memmove(rsp, &rsp[sizeof(rsp) - keep - 1u], keep);
            n = keep;
            rsp[n++] = (char)ch;
            rsp[n] = '\0';
        }

        prv_filter_async_urc_buffer(rsp);
        n = strlen(rsp);

        if (strstr(rsp, "SMS Ready") != NULL) {
            s_catm1_expect_sms_ready_token = false;
            s_catm1_boot_sms_ready_seen = true;
            s_catm1_waiting_boot_sms_ready = false;
            return true;
        }

        if (prv_rsp_has_boot_ready_fallback(rsp)) {
            s_catm1_expect_sms_ready_token = false;
            s_catm1_waiting_boot_sms_ready = false;
            return true;
        }
    }

    s_catm1_expect_sms_ready_token = false;

    /* 일부 부팅에서는 SMS Ready 없이 RDY/+CFUN:1 이후 바로 AT를 받을 수 있다.
     * 여기서 hard fail/power-off 하지 말고, 다음 AT sync 단계로 넘겨 실제 응답을 확인한다. */
    s_catm1_waiting_boot_sms_ready = false;
    return true;
}

static bool prv_try_session_resync(void)
{
    prv_wait_rx_quiet(GW_CATM1_SESSION_RESYNC_QUIET_MS,
                      GW_CATM1_SESSION_RESYNC_MAX_WAIT_MS);
    return prv_send_at_sync();
}

static bool prv_wait_at_sync_after_sms_ready(void)
{
    uint32_t start = HAL_GetTick();
    uint32_t elapsed = 0u;

    prv_wait_rx_quiet(GW_CATM1_BOOT_QUIET_MS, GW_CATM1_BOOT_QUIET_MS + 400u);

    elapsed = (uint32_t)(HAL_GetTick() - start);
    if (elapsed < GW_CATM1_POST_SMS_READY_SETTLE_MS) {
        prv_delay_ms(GW_CATM1_POST_SMS_READY_SETTLE_MS - elapsed);
    }

    while ((uint32_t)(HAL_GetTick() - start) < GW_CATM1_POST_SMS_READY_AT_SYNC_WINDOW_MS) {
        prv_uart_flush_rx();
        if (prv_send_at_sync()) {
            return true;
        }

        prv_wait_rx_quiet(200u, 1000u);
        elapsed = (uint32_t)(HAL_GetTick() - start);
        if (elapsed >= GW_CATM1_POST_SMS_READY_AT_SYNC_WINDOW_MS) {
            break;
        }

        {
            uint32_t gap_ms = GW_CATM1_POST_SMS_READY_RETRY_GAP_MS;
            uint32_t remain_ms = GW_CATM1_POST_SMS_READY_AT_SYNC_WINDOW_MS - elapsed;
            if (gap_ms > remain_ms) {
                gap_ms = remain_ms;
            }
            if (gap_ms != 0u) {
                prv_delay_ms(gap_ms);
            }
        }
    }

    return false;
}

static bool prv_start_session(bool enable_time_auto_update)
{
    char rsp[UI_CATM1_RX_BUF_SZ];
    bool cpin_ready = false;

    (void)enable_time_auto_update;

    /* 세션 시작은 power-on -> boot URC 관찰 -> AT/ATE0 -> CPIN READY까지만 맡는다.
     * 이후 time-sync/TCP용 상세 순서는 각 user-sequence 함수에서 고정한다. */
    prv_uart_flush_rx();
    s_catm1_session_at_ok = false;
    s_catm1_boot_sms_ready_seen = false;
    s_catm1_waiting_boot_sms_ready = true;

    GW_Catm1_PowerOn();

    if (!prv_wait_boot_sms_ready()) {
        return false;
    }
    if (!prv_wait_at_sync_after_sms_ready()) {
        prv_mark_sms_ready_loop_seen();
        return false;
    }

    rsp[0] = '\0';
    if (prv_send_query_wait_prefix_ok("AT+CPIN?\r\n", "+CPIN:", UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp)) &&
        (strstr(rsp, "+CPIN: READY") != NULL)) {
        cpin_ready = true;
    }

    if (!cpin_ready) {
        cpin_ready = prv_wait_sim_ready();
    }
    if (!cpin_ready) {
        return false;
    }

    prv_wait_rx_quiet(100u, 400u);
    return true;
}

static void prv_try_apply_korea_catm_bands(void)
{
    char rsp[UI_CATM1_RX_BUF_SZ];

    if (s_catm1_bandcfg_applied_this_power) {
        return;
    }

    rsp[0] = '\0';
    if (prv_send_cmd_wait(GW_CATM1_CATM_BAND_CFG_CMD, "OK", NULL, NULL,
                          GW_CATM1_BANDCFG_TIMEOUT_MS, rsp, sizeof(rsp))) {
        s_catm1_bandcfg_applied_this_power = true;
        prv_wait_rx_quiet(100u, 300u);
    }
}

static bool prv_wait_sms_ready_after_cfun1(uint32_t timeout_ms)
{
    char rsp[UI_CATM1_RX_BUF_SZ];
    bool ready;

    if (timeout_ms == 0u) {
        return false;
    }

    rsp[0] = '\0';
    s_catm1_expect_sms_ready_token = true;
    ready = prv_uart_wait_for(rsp, sizeof(rsp), timeout_ms, "SMS Ready", NULL, NULL);
    s_catm1_expect_sms_ready_token = false;
    return ready;
}

static bool prv_apply_bootstrap_apn_profile(bool verify_after_set)
{
    char rsp[UI_CATM1_RX_BUF_SZ];
    char cmd[96];

    (void)snprintf(cmd, sizeof(cmd), "AT+CGDCONT=1,\"IP\",\"%s\"\r\n", UI_CATM1_1NCE_APN);
    rsp[0] = '\0';
    if (!prv_send_cmd_wait(cmd, "OK", NULL, NULL, UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp))) {
        return false;
    }

    if (!verify_after_set) {
        return true;
    }

    prv_wait_rx_quiet(100u, 400u);
    rsp[0] = '\0';
    if (!prv_send_query_wait_prefix_ok("AT+CGDCONT?\r\n", "+CGDCONT:",
                                       UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp))) {
        return false;
    }

    return (strstr(rsp, UI_CATM1_1NCE_APN) != NULL);
}



static bool prv_apply_app_network_profile(bool verify_after_set)
{
    char rsp[UI_CATM1_RX_BUF_SZ];
    char cmd[112];

    /* CNACT(APP PDP) 경로는 CGDCONT만으로는 불안정할 수 있어
     * 동일 APN을 CNCFG에도 명시한다. */
    (void)snprintf(cmd, sizeof(cmd), "AT+CNCFG=0,1,\"%s\",\"\",\"\",1\r\n", UI_CATM1_1NCE_APN);
    rsp[0] = '\0';
    if (!prv_send_cmd_wait(cmd, "OK", NULL, NULL, UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp))) {
        return false;
    }

    if (!verify_after_set) {
        return true;
    }

    prv_wait_rx_quiet(100u, 300u);
    rsp[0] = '\0';
    if (!prv_send_query_wait_prefix_ok("AT+CNCFG?\r\n", "+CNCFG:",
                                       UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp))) {
        return false;
    }

    return (((strstr(rsp, "+CNCFG: 0,1,") != NULL) ||
             (strstr(rsp, "+CNCFG:0,1,") != NULL)) &&
            (strstr(rsp, UI_CATM1_1NCE_APN) != NULL));
}


static bool prv_query_signal_quality(uint8_t* out_rssi, uint8_t* out_ber)
{
    char rsp[UI_CATM1_RX_BUF_SZ];
    const char* p;
    unsigned rssi = 99u;
    unsigned ber = 99u;

    rsp[0] = '\0';
    if (!prv_send_query_wait_prefix_ok("AT+CSQ\r\n", "+CSQ:",
                                       GW_CATM1_BOOT_CSQ_TIMEOUT_MS, rsp, sizeof(rsp))) {
        return false;
    }

    p = strstr(rsp, "+CSQ:");
    if (p == NULL) {
        return false;
    }
    while (strstr(p + 1, "+CSQ:") != NULL) {
        p = strstr(p + 1, "+CSQ:");
    }

    if ((sscanf(p, "+CSQ: %u,%u", &rssi, &ber) != 2) &&
        (sscanf(p, "+CSQ:%u,%u", &rssi, &ber) != 2)) {
        return false;
    }

    if (out_rssi != NULL) {
        *out_rssi = (uint8_t)rssi;
    }
    if (out_ber != NULL) {
        *out_ber = (uint8_t)ber;
    }
    return true;
}

static void prv_capture_attach_debug_snapshot(bool include_cpsi)
{
    char rsp[UI_CATM1_RX_BUF_SZ];
    uint8_t rssi = 99u;
    uint8_t ber = 99u;
    bool csq_ok;

    csq_ok = prv_query_signal_quality(&rssi, &ber);

    if (include_cpsi || (csq_ok && (rssi == 99u) && (ber == 99u))) {
        rsp[0] = '\0';
        (void)prv_send_query_wait_prefix_ok("AT+CPSI?\r\n", "+CPSI:",
                                            GW_CATM1_BOOT_CPSI_TIMEOUT_MS, rsp, sizeof(rsp));
    }
}

static bool prv_recover_boot_time_sync_registration(void)
{
    char rsp[UI_CATM1_RX_BUF_SZ];

    prv_wait_rx_quiet(100u, 400u);

    rsp[0] = '\0';
    if (!prv_send_cmd_wait("AT+CFUN=0\r\n", "OK", NULL, NULL,
                           GW_CATM1_APN_BOOTSTRAP_CFUN_TIMEOUT_MS, rsp, sizeof(rsp))) {
        return false;
    }
    prv_wait_rx_quiet(100u, GW_CATM1_APN_BOOTSTRAP_CFUN_OFF_SETTLE_MS);
    prv_delay_ms(GW_CATM1_BOOT_CEREG_RECOVERY_SETTLE_MS);

    rsp[0] = '\0';
    if (!prv_send_cmd_wait("AT+CFUN=1\r\n", "OK", NULL, NULL,
                           GW_CATM1_APN_BOOTSTRAP_CFUN_TIMEOUT_MS, rsp, sizeof(rsp))) {
        return false;
    }
    if (!prv_wait_sms_ready_after_cfun1(GW_CATM1_APN_POST_CFUN1_SMS_READY_TIMEOUT_MS)) {
        return false;
    }
    s_catm1_boot_sms_ready_seen = true;
    s_catm1_waiting_boot_sms_ready = false;
    if (!prv_wait_at_sync_after_sms_ready()) {
        prv_mark_sms_ready_loop_seen();
        return false;
    }
    if (!prv_wait_sim_ready()) {
        return false;
    }

    s_catm1_startup_apn_configured_this_power = false;
    prv_wait_rx_quiet(100u, GW_CATM1_APN_BOOTSTRAP_CFUN_ON_SETTLE_MS);
    return prv_prepare_apn_before_time_sync();
}

static bool prv_prepare_apn_before_time_sync(void)
{
    char rsp[UI_CATM1_RX_BUF_SZ];
    bool apn_verified = false;
    uint32_t apn_try = 0u;

    if (s_catm1_startup_apn_configured_this_power) {
        return true;
    }

    /* Boot-time one-shot sync는 사용자가 준 bring-up 순서에 맞춰
     * SMS Ready/CPIN READY 이후 아래 순서를 고정한다.
     * 1) CFUN=1
     * 2) CGDCONT(APN)
     * 3) CNMP=38
     * 4) CMNB=1
     * 5) COPS=0
     * 6) CSQ 확인
     * 7) CEREG? 등록 대기 */
    prv_wait_rx_quiet(100u, 500u);

    rsp[0] = '\0';
    if (!prv_send_cmd_wait("AT+CFUN=1\r\n", "OK", NULL, NULL,
                           GW_CATM1_APN_BOOTSTRAP_CFUN_TIMEOUT_MS, rsp, sizeof(rsp))) {
        return false;
    }
    prv_wait_rx_quiet(100u, GW_CATM1_APN_BOOTSTRAP_CFUN_ON_SETTLE_MS);

    for (apn_try = 0u; apn_try < GW_CATM1_APN_VERIFY_RETRY_MAX; apn_try++) {
        apn_verified = prv_apply_bootstrap_apn_profile(true);
        if (apn_verified) {
            break;
        }
        prv_wait_rx_quiet(100u, 400u);
        if ((apn_try + 1u) < GW_CATM1_APN_VERIFY_RETRY_MAX) {
            prv_delay_ms(GW_CATM1_APN_VERIFY_GAP_MS);
        }
    }
    if (!apn_verified) {
        return false;
    }

    prv_wait_rx_quiet(100u, 300u);
    rsp[0] = '\0';
    if (!prv_send_cmd_wait("AT+CNMP=38\r\n", "OK", NULL, NULL,
                           UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp))) {
        return false;
    }

    prv_wait_rx_quiet(100u, 300u);
    rsp[0] = '\0';
    if (!prv_send_cmd_wait("AT+CMNB=1\r\n", "OK", NULL, NULL,
                           UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp))) {
        return false;
    }

    prv_wait_rx_quiet(100u, 300u);
    rsp[0] = '\0';
    if (!prv_send_cmd_wait("AT+COPS=0\r\n", "OK", NULL, NULL,
                           GW_CATM1_COPS_AUTO_TIMEOUT_MS, rsp, sizeof(rsp))) {
        return false;
    }
    prv_wait_rx_quiet(100u, GW_CATM1_COPS_AUTO_SETTLE_MS);
    prv_capture_attach_debug_snapshot(false);
    prv_wait_rx_quiet(100u, 300u);

    s_catm1_startup_apn_configured_this_power = true;
    return true;
}


static void prv_try_restore_legacy_attach_profile(void)
{
    char rsp[UI_CATM1_RX_BUF_SZ];

    /* 국내 주요 Cat-M 밴드(1/3/5/8)를 먼저 제한해 불필요한 band scan을 줄인다. */
    prv_try_apply_korea_catm_bands();

    rsp[0] = '\0';
    /* 이전 정상 로그에 있던 CMNB=1은 best-effort로 복구한다. */
    (void)prv_send_cmd_wait("AT+CMNB=1\r\n", "OK", NULL, NULL,
                            UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp));
    prv_wait_rx_quiet(100u, 300u);

    /* 자동 사업자 선택은 빠져 있으면 attach가 지연될 수 있다.
     * 다만 이미 등록된 상태에서는 재선택으로 흔들리지 않게 CEREG 상태를 보고 필요할 때만 보낸다. */
    (void)prv_request_auto_operator_select(false);
}

static bool prv_apply_time_auto_update_cfg(void)
{
    char rsp[UI_CATM1_RX_BUF_SZ];

    rsp[0] = '\0';
    if (!prv_send_cmd_wait("AT+CTZU=1\r\n", "OK", NULL, NULL, UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp))) {
        return false;
    }
    prv_wait_rx_quiet(100u, 300u);
    return true;
}

static void prv_enable_network_time_auto_update(void)
{
    if (s_catm1_time_auto_update_attempted_this_power) {
        return;
    }

    /* Apply once immediately after boot. */
    if (!prv_apply_time_auto_update_cfg()) {
        return;
    }

    s_catm1_time_auto_update_attempted_this_power = true;
    prv_wait_rx_quiet(100u, 400u);
}

static bool prv_is_invalid_cclk_rsp(const char* rsp)
{
    const char* p;
    int yy = 0;

    if (rsp == NULL) {
        return false;
    }
    p = strstr(rsp, "+CCLK:");
    if (p == NULL) {
        return false;
    }
    while (strstr(p + 1, "+CCLK:") != NULL) {
        p = strstr(p + 1, "+CCLK:");
    }
    p = strchr(p, '"');
    if (p == NULL) {
        return false;
    }
    if (sscanf(p + 1, "%2d/", &yy) != 1) {
        return false;
    }
    return ((yy < 0) || (yy >= 80));
}

static bool prv_parse_cclk_epoch(const char* rsp, uint64_t* out_epoch_centi)
{
    const char* p;
    int yy = 0;
    int mon = 0;
    int day = 0;
    int hh = 0;
    int mm = 0;
    int ss = 0;
    int tz_q15 = 0;
    char sign = '+';
    int n;
    UI_DateTime_t dt = {0};

    if ((rsp == NULL) || (out_epoch_centi == NULL)) {
        return false;
    }
    p = strstr(rsp, "+CCLK:");
    if (p == NULL) {
        return false;
    }
    while (strstr(p + 1, "+CCLK:") != NULL) {
        p = strstr(p + 1, "+CCLK:");
    }
    p = strchr(p, '"');
    if (p == NULL) {
        return false;
    }

    n = sscanf(p + 1, "%2d/%2d/%2d,%2d:%2d:%2d%c%2d", &yy, &mon, &day, &hh, &mm, &ss, &sign, &tz_q15);
    if (n < 6) {
        return false;
    }
    if ((yy < 0) || (yy >= 80)) {
        return false;
    }
    if ((mon < 1) || (mon > 12) || (day < 1) || (day > 31) ||
        (hh < 0) || (hh > 23) || (mm < 0) || (mm > 59) || (ss < 0) || (ss > 59)) {
        return false;
    }

    (void)sign;
    (void)tz_q15;
    dt.year = (uint16_t)(2000 + yy);
    dt.month = (uint8_t)mon;
    dt.day = (uint8_t)day;
    dt.hour = (uint8_t)hh;
    dt.min = (uint8_t)mm;
    dt.sec = (uint8_t)ss;
    dt.centi = 0u;
    *out_epoch_centi = (uint64_t)UI_Time_Epoch2016_FromCalendar(&dt) * 100u;
    return true;
}

static uint64_t prv_compensate_cclk_epoch(uint64_t epoch_centi, uint32_t elapsed_ms)
{
    uint32_t comp_centi = (elapsed_ms + 9u) / 10u;
    if (comp_centi > GW_CATM1_CCLK_QUERY_COMP_MAX_CENTI) {
        comp_centi = GW_CATM1_CCLK_QUERY_COMP_MAX_CENTI;
    }
    return epoch_centi + (uint64_t)comp_centi;
}

static bool prv_query_network_time_epoch_once(uint32_t timeout_ms, uint64_t* out_epoch_centi)
{
    char rsp[UI_CATM1_RX_BUF_SZ];
    uint32_t query_start_ms;

    if (out_epoch_centi == NULL) {
        return false;
    }

    query_start_ms = HAL_GetTick();
    if (!prv_send_query_wait_prefix_ok("AT+CCLK?\r\n", "+CCLK:", timeout_ms, rsp, sizeof(rsp))) {
        return false;
    }
    if (!prv_parse_cclk_epoch(rsp, out_epoch_centi)) {
        return false;
    }

    *out_epoch_centi = prv_compensate_cclk_epoch(*out_epoch_centi,
                                                 (uint32_t)(HAL_GetTick() - query_start_ms));
    return true;
}

static bool prv_query_network_time_epoch_retry(uint32_t max_try, uint32_t gap_ms,
                                               bool break_on_invalid, bool* out_invalid_seen,
                                               uint64_t* out_epoch_centi)
{
    char rsp[UI_CATM1_RX_BUF_SZ];
    uint32_t i;
    uint8_t no_rsp_retry_streak = 0u;

    if ((out_epoch_centi == NULL) || (max_try == 0u)) {
        return false;
    }

    for (i = 0u; i < max_try; i++) {
        uint32_t query_start_ms = HAL_GetTick();

        if (!prv_send_query_wait_prefix_ok("AT+CCLK?\r\n", "+CCLK:", UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp))) {
            uint32_t retry_gap_ms = gap_ms;

            /* Power-on trace에서는 +CCLK? 무응답 뒤에 긴 AT resync 구간이 생겼다.
             * 무응답 몇 번은 곧바로 +CCLK?만 다시 보내고, 그 뒤에만 세션 resync를 건다. */
            no_rsp_retry_streak++;
            if ((retry_gap_ms == 0u) || (retry_gap_ms > GW_CATM1_CCLK_NO_RSP_RETRY_GAP_MS)) {
                retry_gap_ms = GW_CATM1_CCLK_NO_RSP_RETRY_GAP_MS;
            }

            if (no_rsp_retry_streak >= GW_CATM1_CCLK_NO_RSP_DIRECT_RETRY_MAX) {
                (void)prv_try_session_resync();
                no_rsp_retry_streak = 0u;
            }

            if ((i + 1u) < max_try) {
                prv_delay_ms(retry_gap_ms);
            }
            continue;
        }

        no_rsp_retry_streak = 0u;
        if (prv_parse_cclk_epoch(rsp, out_epoch_centi)) {
            *out_epoch_centi = prv_compensate_cclk_epoch(*out_epoch_centi,
                                                         (uint32_t)(HAL_GetTick() - query_start_ms));
            return true;
        }
        if (prv_is_invalid_cclk_rsp(rsp)) {
            if (out_invalid_seen != NULL) {
                *out_invalid_seen = true;
            }
            if (break_on_invalid) {
                break;
            }
        }
        if ((i + 1u) < max_try) {
            prv_delay_ms(gap_ms);
        }
    }
    return false;
}

static bool prv_query_network_time_epoch(uint64_t* out_epoch_centi)
{
    uint32_t max_try = UI_CATM1_TIME_SYNC_RETRY;
    if (max_try > 2u) {
        max_try = 2u;
    }
    return prv_query_network_time_epoch_retry(max_try, UI_CATM1_TIME_SYNC_GAP_MS, true, NULL, out_epoch_centi);
}

static bool prv_parse_cntp_result(const char* rsp, uint8_t* out_code)
{
    const char* p;
    unsigned code = 0u;

    if ((rsp == NULL) || (out_code == NULL)) {
        return false;
    }

    p = rsp;
    while ((p = strstr(p, "+CNTP:")) != NULL) {
        if ((sscanf(p, "+CNTP: %u", &code) == 1) ||
            (sscanf(p, "+CNTP:%u", &code) == 1)) {
            *out_code = (uint8_t)code;
            return true;
        }
        p += 6;
    }

    return false;
}

static bool prv_ntp_sync_time(uint64_t* out_epoch_centi)
{
    char rsp[UI_CATM1_RX_BUF_SZ];
    char cmd[96];
    uint8_t cntp_code = 0u;

    if (out_epoch_centi == NULL) {
        return false;
    }

    rsp[0] = '\0';
    (void)prv_send_cmd_wait("AT+CNTPCID=0\r\n", "OK", NULL, NULL, UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp));

    /* SIM7080 CNTP mode 0 updates local time, and the manual recommends
     * querying AT+CCLK after a successful synchronization. */
    (void)snprintf(cmd, sizeof(cmd), "AT+CNTP=\"%s\",%d,0,0\r\n", GW_CATM1_NTP_HOST, GW_CATM1_NTP_TZ_QH);
    rsp[0] = '\0';
    if (!prv_send_cmd_wait(cmd, "OK", NULL, NULL, UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp))) {
        return false;
    }

    rsp[0] = '\0';
    if (!prv_send_cmd_wait("AT+CNTP\r\n", "+CNTP:", NULL, NULL, GW_CATM1_NTP_TIMEOUT_MS, rsp, sizeof(rsp))) {
        return false;
    }
    if (!prv_parse_cntp_result(rsp, &cntp_code) || (cntp_code != 1u)) {
        return false;
    }

    prv_delay_ms(1000u);
    return prv_query_network_time_epoch_retry(6u, 1000u, false, NULL, out_epoch_centi);
}

static bool prv_startup_time_sync_user_sequence(void)
{
    uint64_t epoch_centi = 0u;
    char rsp[UI_CATM1_RX_BUF_SZ];
    uint32_t poll_idx;
    uint8_t stat = 0u;
    bool registered = false;

    s_catm1_pending_psuttz_valid = false;

    /* 부팅 직후 시간 확보는 사용자 지정 순서를 그대로 따른다.
     * 1) AT / ATE0      (prv_start_session(false)에서 처리)
     * 2) AT+CTZU=1
     * 3) AT+CGDCONT=1,"IP","APN"
     * 4) AT+CFUN=1
     * 5) AT+CEREG?      (3초 간격 5회, 0,1 또는 0,5 대기)
     * 6) AT+CNACT=0,1
     * 7) *PSUTTZ/+PSUTTZ가 오면 즉시 시간 정리 후 종료
     * 8) 없으면 AT+CCLK?로 최종 확인 */
    if (!prv_apply_time_auto_update_cfg()) {
        return false;
    }
    s_catm1_time_auto_update_attempted_this_power = true;
    if (prv_apply_pending_psuttz_time_with_cclk(true)) {
        return true;
    }

    prv_wait_rx_quiet(100u, 300u);
    if (!prv_apply_bootstrap_apn_profile(false)) {
        if (prv_apply_pending_psuttz_time_with_cclk(true)) {
            return true;
        }
        return false;
    }
    if (prv_apply_pending_psuttz_time_with_cclk(true)) {
        return true;
    }

    prv_wait_rx_quiet(100u, 300u);
    rsp[0] = '\0';
    if (!prv_send_cmd_wait("AT+CFUN=1\r\n", "OK", NULL, NULL,
                           GW_CATM1_APN_BOOTSTRAP_CFUN_TIMEOUT_MS, rsp, sizeof(rsp))) {
        if (prv_apply_pending_psuttz_time_with_cclk(true)) {
            return true;
        }
        return false;
    }
    prv_wait_rx_quiet(100u, GW_CATM1_APN_BOOTSTRAP_CFUN_ON_SETTLE_MS);
    if (prv_apply_pending_psuttz_time_with_cclk(true)) {
        return true;
    }

    for (poll_idx = 0u; poll_idx < GW_CATM1_USER_BOOT_CEREG_MAX_POLLS; poll_idx++) {
        rsp[0] = '\0';
        if (prv_send_query_wait_prefix_ok("AT+CEREG?\r\n", "+CEREG:",
                                          UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp)) &&
            prv_parse_cereg_stat(rsp, &stat) &&
            ((stat == 1u) || (stat == 5u))) {
            registered = true;
            break;
        }

        if (prv_apply_pending_psuttz_time_with_cclk(true)) {
            return true;
        }

        if ((poll_idx + 1u) < GW_CATM1_USER_BOOT_CEREG_MAX_POLLS) {
            prv_wait_rx_quiet(100u, 300u);
            prv_delay_ms(GW_CATM1_USER_BOOT_CEREG_POLL_GAP_MS);
        }
    }

    if (!registered) {
        if (prv_apply_pending_psuttz_time_with_cclk(true)) {
            return true;
        }
        return false;
    }

    if (!prv_activate_boot_time_pdp()) {
        if (prv_apply_pending_psuttz_time_with_cclk(true)) {
            return true;
        }
        return false;
    }

    if (prv_apply_pending_psuttz_time_with_cclk(true)) {
        return true;
    }

    if (!prv_query_network_time_epoch_retry(GW_CATM1_USER_BOOT_CCLK_MAX_TRY,
                                            GW_CATM1_USER_BOOT_CCLK_GAP_MS,
                                            false,
                                            NULL,
                                            &epoch_centi)) {
        if (prv_apply_pending_psuttz_time_with_cclk(true)) {
            return true;
        }
        return false;
    }

    prv_apply_time_epoch(epoch_centi, true);
    return true;
}

static bool prv_wait_cereg_registered_fixed(uint32_t max_polls, uint32_t gap_ms)
{
    char rsp[UI_CATM1_RX_BUF_SZ];
    uint8_t stat = 0u;
    uint32_t poll_idx;

    if (max_polls == 0u) {
        return false;
    }

    for (poll_idx = 0u; poll_idx < max_polls; poll_idx++) {
        rsp[0] = '\0';
        if (prv_send_query_wait_prefix_ok("AT+CEREG?\r\n", "+CEREG:",
                                          UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp)) &&
            prv_parse_cereg_stat(rsp, &stat) &&
            ((stat == 1u) || (stat == 5u))) {
            return true;
        }

        if ((poll_idx + 1u) < max_polls) {
            prv_wait_rx_quiet(100u, 300u);
            prv_delay_ms(gap_ms);
        }
    }

    return false;
}

static bool prv_prepare_tcp_send_user_sequence(void)
{
    prv_wait_rx_quiet(100u, 300u);
    if (!prv_apply_bootstrap_apn_profile(false)) {
        return false;
    }

    if (!prv_wait_cereg_registered_fixed(GW_CATM1_USER_TCP_CEREG_MAX_POLLS,
                                         GW_CATM1_USER_TCP_CEREG_POLL_GAP_MS)) {
        return false;
    }

    if (!prv_wait_ps_attached()) {
        return false;
    }

    if (!prv_activate_pdp()) {
        return false;
    }

    return true;
}


static bool prv_sync_time_from_modem_startup_try(bool run_time_auto_update_setup,
                                                  bool abort_on_initial_invalid,
                                                  bool* out_initial_invalid_cclk)
{
    uint64_t epoch_centi = 0u;
    bool cpin_ready = false;
    bool reg_ok = false;
    bool ps_ok = false;
    bool initial_invalid_cclk = false;
    char rsp[UI_CATM1_RX_BUF_SZ];

    if (prv_finish_time_sync_from_boot_urc(true)) {
        return true;
    }

    /* CTZU 적용 이후에는 PSUTTZ 여부와 관계없이 실제 +CCLK?를 먼저 한 번 이상 질의한다.
     * 이렇게 해 두면 이후 CPIN/CEREG 재확인에서 조기 실패해도 CCLK 읽기 자체는 건너뛰지 않는다. */
    if (prv_force_cclk_time_sync_after_ctzu(true)) {
        return true;
    }
    rsp[0] = '\0';
    if (prv_send_query_wait_prefix_ok("AT+CPIN?\r\n", "+CPIN:", UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp)) &&
        (strstr(rsp, "+CPIN: READY") != NULL)) {
        cpin_ready = true;
    }
    if (out_initial_invalid_cclk != NULL) {
        *out_initial_invalid_cclk = false;
    }
    if (!cpin_ready) {
        return false;
    }

    if (run_time_auto_update_setup) {
        prv_enable_network_time_auto_update();
        if (prv_finish_time_sync_from_boot_urc(true)) {
            return true;
        }
    }

    (void)prv_send_query_wait_prefix_ok("AT+CEREG?\r\n", "+CEREG:", UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp));
    (void)prv_send_query_wait_prefix_ok("AT+CGATT?\r\n", "+CGATT:", UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp));

    if (prv_query_network_time_epoch_retry(GW_CATM1_STARTUP_CCLK_FIRST_TRY,
                                           GW_CATM1_STARTUP_CCLK_GAP_MS,
                                           true,
                                           &initial_invalid_cclk,
                                           &epoch_centi)) {
        goto apply_time;
    }
    if (out_initial_invalid_cclk != NULL) {
        *out_initial_invalid_cclk = initial_invalid_cclk;
    }
    if (prv_finish_time_sync_from_boot_urc(true)) {
        return true;
    }
    if (abort_on_initial_invalid && initial_invalid_cclk) {
        return false;
    }

    reg_ok = prv_wait_eps_registered_until(GW_CATM1_STARTUP_REG_WAIT_MS);
    if (prv_finish_time_sync_from_boot_urc(true)) {
        return true;
    }
    if (reg_ok && prv_query_network_time_epoch_retry(GW_CATM1_STARTUP_CCLK_POST_REG_TRY,
                                                     1000u,
                                                     false,
                                                     NULL,
                                                     &epoch_centi)) {
        goto apply_time;
    }
    if (prv_finish_time_sync_from_boot_urc(true)) {
        return true;
    }

    if (reg_ok) {
        ps_ok = prv_wait_ps_attached_until(GW_CATM1_STARTUP_PS_WAIT_MS);
        if (prv_finish_time_sync_from_boot_urc(true)) {
            return true;
        }
        if (ps_ok && prv_query_network_time_epoch_retry(GW_CATM1_STARTUP_CCLK_POST_ATTACH_TRY,
                                                        1000u,
                                                        false,
                                                        NULL,
                                                        &epoch_centi)) {
            goto apply_time;
        }
        if (prv_finish_time_sync_from_boot_urc(true)) {
            return true;
        }
        if (ps_ok && prv_activate_pdp() && prv_ntp_sync_time(&epoch_centi)) {
            goto apply_time;
        }
    }

    return prv_finish_time_sync_from_boot_urc(true);

apply_time:
    prv_apply_time_epoch(epoch_centi, true);
    return true;
}

static bool prv_sync_time_from_modem_startup(void)
{
    return prv_sync_time_from_modem_startup_try(true, false, NULL);
}

static bool prv_sync_time_from_modem(void)
{
    uint64_t epoch_centi = 0u;

    if (!prv_query_network_time_epoch(&epoch_centi)) {
        return false;
    }
    prv_apply_time_epoch(epoch_centi, false);
    return true;
}

static bool prv_sync_time_from_modem_quick(bool notify_hook)
{
    uint64_t epoch_centi = 0u;

    if (!prv_query_network_time_epoch_once(GW_CATM1_SERVER_CCLK_TIMEOUT_MS, &epoch_centi)) {
        return false;
    }
    prv_apply_time_epoch(epoch_centi, notify_hook);
    return true;
}

static bool prv_query_gnss_loc_line(char* out, size_t out_sz)
{
    char rsp[UI_CATM1_RX_BUF_SZ];
    char* p;
    char* e;

    if ((out == NULL) || (out_sz == 0u)) {
        return false;
    }

    out[0] = '\0';
    if (!prv_send_cmd_wait("AT+CGNSPWR=1\r\n", "OK", NULL, NULL, UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp))) {
        return false;
    }
    prv_delay_ms(UI_CATM1_GNSS_WAIT_MS);
    if (!prv_send_cmd_wait("AT+CGNSINF\r\n", "+CGNSINF:", NULL, NULL, UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp))) {
        (void)prv_send_cmd_wait("AT+CGNSPWR=0\r\n", "OK", NULL, NULL, UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp));
        return false;
    }

    p = strstr(rsp, "+CGNSINF:");
    if (p != NULL) {
        while (strstr(p + 1, "+CGNSINF:") != NULL) {
            p = strstr(p + 1, "+CGNSINF:");
        }
        e = strpbrk(p, "\r\n");
        if (e != NULL) {
            *e = '\0';
        }
        (void)snprintf(out, out_sz, "LOC:%s", p);
    }

    (void)prv_send_cmd_wait("AT+CGNSPWR=0\r\n", "OK", NULL, NULL, UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp));
    return (out[0] != '\0');
}

static bool prv_parse_cereg_stat(const char* rsp, uint8_t* stat)
{
    const char* p;
    bool found = false;
    uint8_t last_stat = 0u;

    if ((rsp == NULL) || (stat == NULL)) {
        return false;
    }

    p = rsp;
    while ((p = strstr(p, "+CEREG:")) != NULL) {
        const char* q = p + 7u;
        unsigned a = 0u;
        unsigned b = 0u;

        while ((*q == ' ') || (*q == '\t')) {
            q++;
        }

        /* Read response: +CEREG: <n>,<stat>[,...]
         * URC (n=1/2):   +CEREG: <stat>[,...]
         * Keep the last parsable status so mixed query/URC buffers still work. */
        if (sscanf(q, "%u,%u", &a, &b) == 2) {
            last_stat = (uint8_t)b;
            found = true;
        } else if (sscanf(q, "%u", &a) == 1) {
            last_stat = (uint8_t)a;
            found = true;
        }

        p = q;
    }

    if (!found) {
        return false;
    }

    *stat = last_stat;
    return true;
}

static bool prv_is_eps_registered_stat(uint8_t stat)
{
    return ((stat == 1u) || (stat == 5u) || (stat == 9u) || (stat == 10u));
}

static bool prv_request_auto_operator_select(bool force_send)
{
    char rsp[UI_CATM1_RX_BUF_SZ];
    uint8_t stat = 0u;

    if (!force_send) {
        rsp[0] = '\0';
        if (prv_send_query_wait_prefix_ok("AT+CEREG?\r\n", "+CEREG:",
                                          UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp)) &&
            prv_parse_cereg_stat(rsp, &stat) &&
            prv_is_eps_registered_stat(stat)) {
            return true;
        }
    }

    prv_wait_rx_quiet(100u, 400u);
    rsp[0] = '\0';
    if (!prv_send_cmd_wait("AT+COPS=0\r\n", "OK", NULL, NULL,
                           GW_CATM1_COPS_AUTO_TIMEOUT_MS, rsp, sizeof(rsp))) {
        /* COPS 실패 직후에는 모뎀이 +CME ERROR / +CPIN / SMS Ready URC를
         * 뒤이어 흘릴 수 있다. 여기서 바로 다음 AT를 보내면 외부 캡처에서는
         * 실패 메시지와 다음 명령이 문자 단위로 섞여 보이므로 quiet 구간을 확보한다. */
        prv_wait_rx_quiet(200u, GW_CATM1_COPS_AUTO_SETTLE_MS + 500u);
        return false;
    }

    prv_wait_rx_quiet(100u, GW_CATM1_COPS_AUTO_SETTLE_MS);
    prv_capture_attach_debug_snapshot(false);
    return true;
}

static bool prv_wait_eps_registered(void)
{
    return prv_wait_eps_registered_until(UI_CATM1_NET_REG_TIMEOUT_MS);
}

static bool prv_is_boot_time_sync_cereg_ready_stat(uint8_t stat)
{
    return ((stat == 1u) || (stat == 5u));
}

static bool prv_wait_boot_time_sync_registered(uint32_t timeout_ms)
{
    char rsp[UI_CATM1_RX_BUF_SZ];
    uint8_t stat = 0u;
    uint32_t start = HAL_GetTick();
    uint32_t effective_timeout_ms = timeout_ms;
    uint32_t poll_count = 0u;
    uint32_t recovery_count = 0u;

    if ((effective_timeout_ms == 0u) || (effective_timeout_ms < GW_CATM1_BOOT_CEREG_TIMEOUT_MS)) {
        effective_timeout_ms = GW_CATM1_BOOT_CEREG_TIMEOUT_MS;
    }

    while (1) {
        if ((uint32_t)(HAL_GetTick() - start) >= effective_timeout_ms) {
            if (recovery_count < GW_CATM1_BOOT_CEREG_RECOVERY_MAX) {
                recovery_count++;
                if (!prv_recover_boot_time_sync_registration()) {
                    return false;
                }
                start = HAL_GetTick();
                poll_count = 0u;
                continue;
            }
            break;
        }

        poll_count++;
        rsp[0] = '\0';
        if (prv_send_query_wait_prefix_ok("AT+CEREG?\r\n", "+CEREG:",
                                          UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp)) &&
            prv_parse_cereg_stat(rsp, &stat)) {
            if (prv_is_boot_time_sync_cereg_ready_stat(stat)) {
                return true;
            }
            if ((stat == 2u) &&
                ((poll_count % GW_CATM1_BOOT_CEREG_DIAG_STRIDE) == 0u)) {
                prv_capture_attach_debug_snapshot(true);
            }
        } else if ((poll_count % GW_CATM1_BOOT_CEREG_DIAG_STRIDE) == 0u) {
            prv_capture_attach_debug_snapshot(true);
        }

        prv_wait_rx_quiet(100u, 400u);
        if ((uint32_t)(HAL_GetTick() - start) < effective_timeout_ms) {
            prv_delay_ms(GW_CATM1_BOOT_CEREG_POLL_GAP_MS);
        }
    }

    return false;
}

static bool prv_wait_eps_registered_until(uint32_t timeout_ms)
{
    char rsp[UI_CATM1_RX_BUF_SZ];
    uint8_t stat = 0u;
    uint32_t start = HAL_GetTick();
    uint32_t effective_timeout_ms = timeout_ms;
    uint32_t poll_count = 0u;
    uint8_t query_fail_streak = 0u;
    bool reselect_sent = false;

    if (effective_timeout_ms < GW_CATM1_NET_REG_MIN_TIMEOUT_MS) {
        effective_timeout_ms = GW_CATM1_NET_REG_MIN_TIMEOUT_MS;
    }

    while (((uint32_t)(HAL_GetTick() - start) < effective_timeout_ms) &&
           (poll_count < GW_CATM1_CEREG_MAX_POLLS)) {
        poll_count++;

        if (prv_send_query_wait_prefix_ok("AT+CEREG?\r\n", "+CEREG:", UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp)) &&
            prv_parse_cereg_stat(rsp, &stat)) {
            query_fail_streak = 0u;
            if (prv_is_eps_registered_stat(stat)) {
                return true;
            }

            if ((!reselect_sent) && (poll_count >= GW_CATM1_CEREG_RESELECT_POLL)) {
                (void)prv_request_auto_operator_select(true);
                reselect_sent = true;
            }
        } else {
            query_fail_streak++;
            if (query_fail_streak >= GW_CATM1_QUERY_FAIL_RESYNC_STREAK) {
                (void)prv_try_session_resync();
                query_fail_streak = 0u;
            } else {
                prv_wait_rx_quiet(150u, 700u);
            }
        }

        if (poll_count < GW_CATM1_CEREG_MAX_POLLS) {
            prv_delay_ms(GW_CATM1_CEREG_POLL_GAP_MS);
        }
    }
    return false;
}

static bool prv_parse_cgatt_state(const char* rsp, uint8_t* state)
{
    const char* p;
    unsigned v = 0u;

    if ((rsp == NULL) || (state == NULL)) {
        return false;
    }
    p = strstr(rsp, "+CGATT:");
    if (p == NULL) {
        return false;
    }
    while (strstr(p + 1, "+CGATT:") != NULL) {
        p = strstr(p + 1, "+CGATT:");
    }
    if ((sscanf(p, "+CGATT: %u", &v) != 1) && (sscanf(p, "+CGATT:%u", &v) != 1)) {
        return false;
    }
    *state = (uint8_t)v;
    return true;
}

static bool prv_wait_ps_attached(void)
{
    return prv_wait_ps_attached_until(UI_CATM1_PS_ATTACH_TIMEOUT_MS);
}

static bool prv_wait_ps_attached_until(uint32_t timeout_ms)
{
    char rsp[UI_CATM1_RX_BUF_SZ];
    uint8_t state = 0u;
    uint32_t start = HAL_GetTick();
    uint32_t effective_timeout_ms = timeout_ms;
    uint8_t query_fail_streak = 0u;

    if (effective_timeout_ms < GW_CATM1_PS_ATTACH_MIN_TIMEOUT_MS) {
        effective_timeout_ms = GW_CATM1_PS_ATTACH_MIN_TIMEOUT_MS;
    }

    while ((uint32_t)(HAL_GetTick() - start) < effective_timeout_ms) {
        if (prv_send_query_wait_prefix_ok("AT+CGATT?\r\n", "+CGATT:", UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp)) &&
            prv_parse_cgatt_state(rsp, &state)) {
            query_fail_streak = 0u;
            if (state == 1u) {
                return true;
            }
        } else {
            query_fail_streak++;
            if (query_fail_streak >= GW_CATM1_QUERY_FAIL_RESYNC_STREAK) {
                (void)prv_try_session_resync();
                query_fail_streak = 0u;
            } else {
                prv_wait_rx_quiet(150u, 700u);
            }
        }
        prv_delay_ms(UI_CATM1_PS_ATTACH_POLL_MS);
    }
    return false;
}

static bool prv_parse_cnact_ctx0(const char* rsp, uint8_t* state, uint8_t ip[4])
{
    const char* p;
    unsigned cid = 0u;
    unsigned vstate = 0u;
    unsigned a = 0u, b = 0u, c = 0u, d = 0u;

    if ((rsp == NULL) || (state == NULL) || (ip == NULL)) {
        return false;
    }
    p = rsp;
    while ((p = strstr(p, "+CNACT:")) != NULL) {
        if ((((sscanf(p, "+CNACT: %u,%u,\"%u.%u.%u.%u\"", &cid, &vstate, &a, &b, &c, &d) == 6) ||
              (sscanf(p, "+CNACT:%u,%u,\"%u.%u.%u.%u\"", &cid, &vstate, &a, &b, &c, &d) == 6))) &&
            (cid == 0u)) {
            *state = (uint8_t)vstate;
            ip[0] = (uint8_t)a;
            ip[1] = (uint8_t)b;
            ip[2] = (uint8_t)c;
            ip[3] = (uint8_t)d;
            return true;
        }
        p += 7;
    }
    return false;
}

static bool prv_activate_boot_time_pdp(void)
{
    char rsp[UI_CATM1_RX_BUF_SZ];
    uint8_t state = 0u;
    uint8_t ip[4] = {0u, 0u, 0u, 0u};
    uint32_t start = HAL_GetTick();

    /* 사용자 요구 순서의 실동작 보강:
     * CGDCONT 이후 CNACT(APP PDP)를 쓸 때는 CNCFG 쪽 APN도 맞춰 둬야
     * 실제 IP 할당과 TCP send가 안정적이다. */
    if (!prv_apply_app_network_profile(false)) {
        return false;
    }

    /* Boot-time sync에서는 사용자가 준 순서대로 CNACT=0,1을 직접 보낸 뒤,
     * 실제 context 0이 active/IP 할당 상태인지 query로 확인한다. */
    rsp[0] = '\0';
    (void)prv_send_cmd_wait("AT+CNACT=0,1\r\n", "OK", "+APP PDP: 0,ACTIVE", NULL,
                            UI_CATM1_NET_ACT_TIMEOUT_MS, rsp, sizeof(rsp));

    prv_delay_ms(UI_CATM1_QUERY_OK_IDLE_MS);
    while ((uint32_t)(HAL_GetTick() - start) < UI_CATM1_NET_ACT_TIMEOUT_MS) {
        rsp[0] = '\0';
        if (prv_send_query_wait_cnact_ok(rsp, sizeof(rsp)) &&
            prv_parse_cnact_ctx0(rsp, &state, ip) &&
            (state == 1u) && ((ip[0] | ip[1] | ip[2] | ip[3]) != 0u)) {
            return true;
        }
        prv_delay_ms(300u);
    }

    return false;
}

static bool prv_activate_pdp(void)
{
    char rsp[UI_CATM1_RX_BUF_SZ];
    char cmd[96];
    uint8_t state = 0u;
    uint8_t ip[4] = {0u, 0u, 0u, 0u};
    uint32_t start;

    (void)snprintf(cmd, sizeof(cmd), "AT+CNCFG=0,1,\"%s\",\"\",\"\",1\r\n", UI_CATM1_1NCE_APN);
    if (!prv_send_cmd_wait(cmd, "OK", NULL, NULL, UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp))) {
        return false;
    }

    if (prv_send_query_wait_cnact_ok(rsp, sizeof(rsp)) &&
        prv_parse_cnact_ctx0(rsp, &state, ip) &&
        (state == 1u) && ((ip[0] | ip[1] | ip[2] | ip[3]) != 0u)) {
        return true;
    }

    if (!prv_send_cmd_wait("AT+CNACT=0,1\r\n", "OK", "+APP PDP: 0,ACTIVE", NULL, UI_CATM1_NET_ACT_TIMEOUT_MS, rsp, sizeof(rsp))) {
        return false;
    }

    prv_delay_ms(UI_CATM1_QUERY_OK_IDLE_MS);
    if (prv_send_query_wait_cnact_ok(rsp, sizeof(rsp)) &&
        prv_parse_cnact_ctx0(rsp, &state, ip) &&
        (state == 1u) && ((ip[0] | ip[1] | ip[2] | ip[3]) != 0u)) {
        return true;
    }

    start = HAL_GetTick();
    while ((uint32_t)(HAL_GetTick() - start) < UI_CATM1_NET_ACT_TIMEOUT_MS) {
        prv_delay_ms(300u);
        if (prv_send_query_wait_cnact_ok(rsp, sizeof(rsp)) &&
            prv_parse_cnact_ctx0(rsp, &state, ip) &&
            (state == 1u) && ((ip[0] | ip[1] | ip[2] | ip[3]) != 0u)) {
            return true;
        }
    }
    return false;
}

static bool prv_parse_caopen_result(const char* rsp, uint8_t* out_cid, uint8_t* out_result)
{
    const char* p;
    unsigned cid = 0u;
    unsigned result = 0u;

    if (rsp == NULL) {
        return false;
    }

    p = rsp;
    while ((p = strstr(p, "+CAOPEN:")) != NULL) {
        if (((sscanf(p, "+CAOPEN: %u,%u", &cid, &result) == 2) ||
             (sscanf(p, "+CAOPEN:%u,%u", &cid, &result) == 2))) {
            if (out_cid != NULL) {
                *out_cid = (uint8_t)cid;
            }
            if (out_result != NULL) {
                *out_result = (uint8_t)result;
            }
            return true;
        }
        p += 8; /* strlen("+CAOPEN:") */
    }

    return false;
}

static bool prv_rsp_contains_ok_line(const char* rsp)
{
    const char* line;
    const char* end;

    if (rsp == NULL) {
        return false;
    }

    line = rsp;
    while (*line != '\0') {
        end = strchr(line, '\n');
        if (end == NULL) {
            end = line + strlen(line);
        } else {
            end++;
        }

        if (prv_is_trimmed_line_equal(line, end, "OK")) {
            return true;
        }

        if (*end == '\0') {
            break;
        }
        line = end;
    }

    return false;
}

static bool prv_query_tcp_connected_state(uint8_t cid)
{
    char rsp[UI_CATM1_RX_BUF_SZ];
    const char* p;
    unsigned q_cid = 0u;
    unsigned state = 0u;

    rsp[0] = '\0';
    if (!prv_send_query_wait_prefix_ok("AT+CASTATE?\r\n", "+CASTATE:",
                                       UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp))) {
        return false;
    }

    p = rsp;
    while ((p = strstr(p, "+CASTATE:")) != NULL) {
        if (((sscanf(p, "+CASTATE: %u,%u", &q_cid, &state) == 2) ||
             (sscanf(p, "+CASTATE:%u,%u", &q_cid, &state) == 2))) {
            if (((uint8_t)q_cid == cid) && (state == 1u)) {
                return true;
            }
        }
        p += 9; /* strlen("+CASTATE:") */
    }

    return false;
}

static void prv_mark_tcp_open_failure(uint32_t caopen_ms)
{
    s_catm1_last_caopen_ms = caopen_ms;
    s_catm1_tcp_open_fail_powerdown_pending = true;
}

static void prv_finish_power_off_state(void)
{
    s_catm1_session_at_ok = false;
    s_catm1_boot_sms_ready_seen = false;
    s_catm1_waiting_boot_sms_ready = false;
    s_catm1_tcp_open_fail_powerdown_pending = false;
    s_catm1_expect_sms_ready_token = false;
    s_catm1_session_sms_ready_loop_seen = false;
    s_catm1_last_caopen_ms = 0u;
    s_catm1_time_auto_update_attempted_this_power = false;
    s_catm1_time_synced_from_urc_this_power = false;
    s_catm1_pending_psuttz_valid = false;
    s_catm1_pending_psuttz_epoch_centi = 0u;
    s_catm1_tcp_send_session_active = false;
    s_catm1_tcp_time_sync_pending = false;
    s_catm1_server_cmd_ind_seen = false;
    s_catm1_startup_apn_configured_this_power = false;
    s_catm1_bandcfg_applied_this_power = false;
    prv_lpuart_release();
    s_catm1_last_poweroff_ms = HAL_GetTick();
}

static bool prv_was_powered_off_recently(uint32_t window_ms)
{
    if ((window_ms == 0u) || (s_catm1_last_poweroff_ms == 0u)) {
        return false;
    }

    return ((uint32_t)(HAL_GetTick() - s_catm1_last_poweroff_ms) < window_ms);
}

static void prv_abort_tcp_open_and_power_off(uint32_t caopen_ms)
{
    prv_mark_tcp_open_failure(caopen_ms);

    /* +CAOPEN: 0,23 등 TCP open 실패는 cleanup까지 기다리지 말고
     * 여기서 즉시 power off 경로로 내려서 모뎀이 계속 켜져 있지 않게 한다. */
    prv_shutdown_modem_prefer_poweroff();
}

static void prv_force_power_cut(void)
{
#if defined(PWR_KEY_Pin)
    HAL_GPIO_WritePin(PWR_KEY_GPIO_Port, PWR_KEY_Pin, UI_CATM1_PWRKEY_INACTIVE_STATE);
#endif
#if defined(CATM1_PWR_Pin)
    HAL_GPIO_WritePin(CATM1_PWR_GPIO_Port, CATM1_PWR_Pin, GPIO_PIN_RESET);
#elif defined(PWR_KEY_Pin)
    HAL_GPIO_WritePin(PWR_KEY_GPIO_Port, PWR_KEY_Pin, UI_CATM1_PWRKEY_ACTIVE_STATE);
    prv_delay_ms(UI_CATM1_PWRKEY_OFF_PULSE_MS);
    HAL_GPIO_WritePin(PWR_KEY_GPIO_Port, PWR_KEY_Pin, UI_CATM1_PWRKEY_INACTIVE_STATE);
#endif
    prv_finish_power_off_state();
}

static void prv_shutdown_modem_prefer_poweroff(void)
{
    if (prv_lpuart_is_inited() && s_catm1_session_at_ok) {
        /* Normal session end should use the low-current shutdown path.
         * GW_Catm1_PowerOff() also handles the fast CAOPEN-failure case internally. */
        GW_Catm1_PowerOff();
        return;
    }

    prv_force_power_cut();
}

static void prv_shutdown_modem_after_tcp_close(void)
{
    bool normal_pd = false;

    if (prv_lpuart_is_inited() && s_catm1_session_at_ok) {
        /* User-requested TCP teardown path:
         * CACLOSE -> OK -> CPOWD=1 without the extra CFUN=0 wait path. */
        normal_pd = prv_try_normal_power_down(false);
    }

#if defined(PWR_KEY_Pin)
    if (!normal_pd) {
        HAL_GPIO_WritePin(PWR_KEY_GPIO_Port, PWR_KEY_Pin, UI_CATM1_PWRKEY_ACTIVE_STATE);
        prv_delay_ms(UI_CATM1_PWRKEY_OFF_PULSE_MS);
        HAL_GPIO_WritePin(PWR_KEY_GPIO_Port, PWR_KEY_Pin, UI_CATM1_PWRKEY_INACTIVE_STATE);
        prv_delay_ms(UI_CATM1_PWRDOWN_WAIT_MS);
    }
#endif
#if defined(CATM1_PWR_Pin)
    HAL_GPIO_WritePin(CATM1_PWR_GPIO_Port, CATM1_PWR_Pin, GPIO_PIN_RESET);
#endif
    prv_finish_power_off_state();
}

static bool prv_close_tcp_socket_gracefully(char* rsp, size_t rsp_sz)
{
    if ((rsp == NULL) || (rsp_sz == 0u)) {
        return false;
    }

    /* SIM7080 CACLOSE write command completes on OK.
     * Do not wait for +CACLOSE/+CASTATE URC here; the next command can
     * follow immediately after OK. */
    rsp[0] = '\0';
    return prv_send_cmd_wait("AT+CACLOSE=0\r\n", "OK", NULL, NULL,
                             UI_CATM1_AT_TIMEOUT_MS, rsp, rsp_sz);
}

static void prv_close_tcp_and_force_power_cut(bool opened, char* rsp, size_t rsp_sz)
{
    if ((!opened) && prv_was_powered_off_recently(GW_CATM1_TCP_OPEN_FAIL_CPOWD_DELAY_MS +
                                                 GW_CATM1_TCP_OPEN_FAIL_PWRDOWN_WAIT_MS + 500u)) {
        return;
    }

    if (opened) {
        (void)prv_close_tcp_socket_gracefully(rsp, rsp_sz);
        prv_shutdown_modem_after_tcp_close();
        return;
    }
    prv_shutdown_modem_prefer_poweroff();
}

static bool prv_open_tcp(const uint8_t ip[4], uint16_t port)
{
    char cmd[96];
    char rsp[UI_CATM1_RX_BUF_SZ];
    uint8_t ch = 0u;
    uint8_t cid = 0xFFu;
    uint8_t result = 0xFFu;
    uint32_t start;
    uint32_t timeout_ms = GW_CATM1_TCP_OPEN_HARD_TIMEOUT_MS;
    uint32_t ok_seen_tick = 0u;
    uint32_t next_state_query_tick = 0u;
    size_t n = 0u;
    bool saw_ok = false;
    bool saw_caopen = false;

    s_catm1_tcp_open_fail_powerdown_pending = false;
    s_catm1_last_caopen_ms = 0u;

    if ((timeout_ms == 0u) || (timeout_ms > GW_CATM1_TCP_OPEN_HARD_TIMEOUT_MS)) {
        timeout_ms = GW_CATM1_TCP_OPEN_HARD_TIMEOUT_MS;
    }

    if (!prv_send_cmd_wait("AT+CACID=0\r\n", "OK", NULL, NULL, UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp))) {
        return false;
    }

    /* 이전 세션에 남은 소켓이 있으면 새 open이 오래 붙잡힐 수 있어 먼저 정리한다. */
    (void)prv_send_cmd_wait("AT+CACLOSE=0\r\n", "OK", NULL, NULL, UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp));

    (void)snprintf(cmd, sizeof(cmd), "AT+CAOPEN=0,0,\"TCP\",\"%u.%u.%u.%u\",%u\r\n",
                   (unsigned)ip[0], (unsigned)ip[1], (unsigned)ip[2], (unsigned)ip[3], (unsigned)port);

    prv_uart_flush_rx();
    if (!prv_uart_send_text(cmd, UI_CATM1_AT_TIMEOUT_MS)) {
        return false;
    }

    rsp[0] = '\0';
    start = HAL_GetTick();
    s_catm1_last_caopen_ms = start;
    while ((uint32_t)(HAL_GetTick() - start) < timeout_ms) {
        uint32_t now = HAL_GetTick();

        if (!prv_catm1_rb_pop_wait(&ch, 20u)) {
            if (saw_ok && !saw_caopen &&
                ((uint32_t)(now - ok_seen_tick) >= 200u) &&
                ((next_state_query_tick == 0u) || ((uint32_t)(now - next_state_query_tick) >= 400u))) {
                next_state_query_tick = now;
                if (prv_query_tcp_connected_state(0u)) {
                    s_catm1_last_caopen_ms = 0u;
                    s_catm1_tcp_open_fail_powerdown_pending = false;
                    s_catm1_tcp_time_sync_pending = true;
                    prv_delay_ms(UI_CATM1_QUERY_OK_IDLE_MS);
                    return true;
                }
            }
            continue;
        }
        if (ch == 0u) {
            continue;
        }

        if ((n + 1u) < sizeof(rsp)) {
            rsp[n++] = (char)ch;
            rsp[n] = '\0';
        } else if (sizeof(rsp) > 16u) {
            size_t keep = (sizeof(rsp) / 2u);
            memmove(rsp, &rsp[sizeof(rsp) - keep - 1u], keep);
            n = keep;
            rsp[n++] = (char)ch;
            rsp[n] = '\0';
        }

        if ((strstr(rsp, "ERROR") != NULL) || (strstr(rsp, "+CME ERROR") != NULL)) {
            /* CAOPEN 실패는 여기서 즉시 power off 처리한다. */
            prv_abort_tcp_open_and_power_off(start);
            return false;
        }

        if (!saw_ok && prv_rsp_contains_ok_line(rsp)) {
            saw_ok = true;
            ok_seen_tick = HAL_GetTick();
        }

        if (!saw_caopen && prv_parse_caopen_result(rsp, &cid, &result) && (cid == 0u)) {
            saw_caopen = true;
            if (result != 0u) {
                /* +CAOPEN: 0,23 등 비정상 결과는 여기서 즉시 power off 처리한다. */
                prv_abort_tcp_open_and_power_off(start);
                return false;
            }
        }

        /* SIM7080 문서상 CAOPEN write command는 최종 OK까지 받아야 다음 AT를 보낼 수 있고,
         * asyncOpen_enable 설정에 따라 응답 순서가
         *   1) +CAOPEN -> OK
         *   2) OK -> +CAOPEN
         * 둘 다 가능하다. 둘 중 하나만 본 상태에서는 다음 CASEND로 넘어가지 않는다. */
        if (saw_caopen && saw_ok) {
            s_catm1_last_caopen_ms = 0u;
            s_catm1_tcp_open_fail_powerdown_pending = false;
            s_catm1_tcp_time_sync_pending = true;
            prv_delay_ms(UI_CATM1_QUERY_OK_IDLE_MS);
            return true;
        }
    }

    /* +CAOPEN URC를 못 봤더라도 final OK 이후 실제 socket state가 connected면
     * 곧바로 CASEND로 진행할 수 있게 마지막으로 한 번 더 확인한다. */
    if (saw_ok && prv_query_tcp_connected_state(0u)) {
        s_catm1_last_caopen_ms = 0u;
        s_catm1_tcp_open_fail_powerdown_pending = false;
        s_catm1_tcp_time_sync_pending = true;
        prv_delay_ms(UI_CATM1_QUERY_OK_IDLE_MS);
        return true;
    }

    /* +CAOPEN result URC는 망 상태에 따라 수 초 늦게 올 수 있으므로
     * hard timeout까지 기다린 뒤에만 실패 처리한다. */
    prv_abort_tcp_open_and_power_off(start);
    return false;
}

static void prv_wait_post_send_settle(void)
{
    /* Do not transmit AT+CAACK queries here.
     * The query path flushes RX and can discard immediate server indications or
     * command/time payloads that arrive right after CASEND. Preserve RX for
     * prv_receive_server_cmd_after_first_payload(). */
    prv_delay_ms(30u);
}

static bool prv_wait_server_cmd_ind(uint32_t timeout_ms)
{
    char rsp[UI_CATM1_RX_BUF_SZ];
    uint8_t ch = 0u;
    uint32_t start = HAL_GetTick();
    size_t n = 0u;

    rsp[0] = '\0';
    while ((uint32_t)(HAL_GetTick() - start) < timeout_ms) {
        if (!prv_catm1_rb_pop_wait(&ch, 20u)) {
            continue;
        }
        if (ch == 0u) {
            continue;
        }

        if ((n + 1u) < sizeof(rsp)) {
            rsp[n++] = (char)ch;
            rsp[n] = '\0';
        } else if (sizeof(rsp) > 16u) {
            size_t keep = (sizeof(rsp) / 2u);
            memmove(rsp, &rsp[sizeof(rsp) - keep - 1u], keep);
            n = keep;
            rsp[n++] = (char)ch;
            rsp[n] = '\0';
        }

        prv_filter_async_urc_buffer(rsp);
        n = strlen(rsp);

        if ((strstr(rsp, "+CADATAIND: 0") != NULL) ||
            (strstr(rsp, "+CAURC: \"recv\",0,") != NULL)) {
            return true;
        }
        if ((strstr(rsp, "+CASTATE: 0,0") != NULL) ||
            (strstr(rsp, "+CACLOSE: 0") != NULL) ||
            (strstr(rsp, "ERROR") != NULL) ||
            (strstr(rsp, "+CME ERROR") != NULL)) {
            return false;
        }
    }
    return false;
}

static bool prv_parse_dec_field(const char* begin, const char* end, uint32_t* out)
{
    uint32_t v = 0u;
    bool saw_digit = false;

    if ((begin == NULL) || (end == NULL) || (out == NULL) || (begin >= end)) {
        return false;
    }

    while ((begin < end) && ((*begin == ' ') || (*begin == '\t'))) {
        begin++;
    }
    while ((end > begin) && ((end[-1] == ' ') || (end[-1] == '\t'))) {
        end--;
    }
    while (begin < end) {
        char ch = *begin++;

        if ((ch < '0') || (ch > '9')) {
            return false;
        }
        saw_digit = true;
        v = (v * 10u) + (uint32_t)(ch - '0');
    }
    if (!saw_digit) {
        return false;
    }
    *out = v;
    return true;
}

static bool prv_extract_carecv_payload(const char* rsp, char* out, size_t out_sz, size_t* out_len)
{
    const char* p;
    const char* c1;
    const char* c2;
    const char* c3;
    const char* data;
    uint32_t recv_len = 0u;

    if ((rsp == NULL) || (out == NULL) || (out_sz == 0u)) {
        return false;
    }
    if (out_len != NULL) {
        *out_len = 0u;
    }

    p = strstr(rsp, "+CARECV:");
    if (p == NULL) {
        return false;
    }
    p += 8; /* strlen("+CARECV:") */
    while ((*p == ' ') || (*p == '\t')) {
        p++;
    }

    c1 = strchr(p, ',');
    if (c1 == NULL) {
        return false;
    }

    if (memchr(p, '.', (size_t)(c1 - p)) != NULL) {
        c2 = strchr(c1 + 1, ',');
        if (c2 == NULL) {
            return false;
        }
        c3 = strchr(c2 + 1, ',');
        if (c3 == NULL) {
            return false;
        }
        if (!prv_parse_dec_field(c2 + 1, c3, &recv_len)) {
            return false;
        }
        data = c3 + 1;
    } else {
        if (!prv_parse_dec_field(p, c1, &recv_len)) {
            return false;
        }
        data = c1 + 1;
    }

    if (recv_len == 0u) {
        out[0] = '\0';
        return true;
    }
    if (strlen(data) < (size_t)recv_len) {
        return false;
    }

    if ((size_t)recv_len >= out_sz) {
        recv_len = (uint32_t)(out_sz - 1u);
    }
    memcpy(out, data, (size_t)recv_len);
    out[recv_len] = '\0';
    if (out_len != NULL) {
        *out_len = (size_t)recv_len;
    }
    return true;
}

static bool prv_read_server_cmd_data(char* out, size_t out_sz, size_t* out_len)
{
    char cmd[48];
    char rsp[UI_CATM1_RX_BUF_SZ];

    if ((out == NULL) || (out_sz == 0u)) {
        return false;
    }
    if (out_len != NULL) {
        *out_len = 0u;
    }

    (void)snprintf(cmd, sizeof(cmd), "AT+CARECV=0,%u\r\n", (unsigned)GW_CATM1_SERVER_CMD_READLEN);
    if (!prv_send_query_wait_prefix_ok(cmd, "+CARECV:", UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp))) {
        return false;
    }
    return prv_extract_carecv_payload(rsp, out, out_sz, out_len);
}

static bool prv_is_server_plain_time_cmd(const char* s)
{
    if (s == NULL) {
        return false;
    }

    while (prv_is_trim_char(*s)) {
        s++;
    }
    return (strncmp(s, "TIME:", 5) == 0);
}

static void prv_dispatch_server_plain_time_line(const char* line, size_t line_len)
{
    const char* begin = line;
    const char* end = line + line_len;
    char framed[UI_UART_LINE_MAX];
    size_t cmd_len;

    while ((begin < end) && prv_is_trim_char(*begin)) {
        begin++;
    }
    while ((end > begin) && prv_is_trim_char(end[-1])) {
        end--;
    }
    if (end <= begin) {
        return;
    }
    if (*begin == '<') {
        return;
    }
    if (!prv_is_server_plain_time_cmd(begin)) {
        return;
    }

    cmd_len = (size_t)(end - begin);
    if ((cmd_len + 3u) > sizeof(framed)) {
        return;
    }

    framed[0] = '<';
    memcpy(&framed[1], begin, cmd_len);
    framed[1u + cmd_len] = '>';
    framed[1u + cmd_len + 1u] = '\0';
    UI_Cmd_ProcessLineSilent(framed);
}

static void prv_dispatch_server_cmd_frames(const char* data, size_t data_len)
{
    size_t i = 0u;
    char frame[UI_UART_LINE_MAX];

    if ((data == NULL) || (data_len == 0u)) {
        return;
    }

    while (i < data_len) {
        size_t start;
        size_t end;
        size_t frame_len;

        while ((i < data_len) && (data[i] != '<')) {
            i++;
        }
        if (i >= data_len) {
            break;
        }

        start = i;
        end = start;
        while ((end < data_len) && (data[end] != '>')) {
            end++;
        }
        if ((end >= data_len) || (data[end] != '>')) {
            break;
        }

        frame_len = (end - start) + 1u;
        if (frame_len < sizeof(frame)) {
            memcpy(frame, &data[start], frame_len);
            frame[frame_len] = '\0';
            UI_Cmd_ProcessLineSilent(frame);
        }
        i = end + 1u;
    }

    i = 0u;
    while (i < data_len) {
        size_t start;
        size_t end;

        while ((i < data_len) && ((data[i] == '\r') || (data[i] == '\n'))) {
            i++;
        }
        if (i >= data_len) {
            break;
        }

        start = i;
        end = start;
        while ((end < data_len) && (data[end] != '\r') && (data[end] != '\n')) {
            end++;
        }
        prv_dispatch_server_plain_time_line(&data[start], end - start);
        i = end;
    }
}

static void prv_receive_server_cmd_after_first_payload(void)
{
    char data[GW_CATM1_SERVER_CMD_READLEN + 1u];
    size_t data_len = 0u;
    uint32_t pass;
    bool saw_ind;

    saw_ind = s_catm1_server_cmd_ind_seen;
    s_catm1_server_cmd_ind_seen = false;
    if (!saw_ind) {
        saw_ind = prv_wait_server_cmd_ind(GW_CATM1_SERVER_CMD_WAIT_MS);
    }
    for (pass = 0u; pass < GW_CATM1_SERVER_CMD_MAX_READ_PASSES; pass++) {
        if (!prv_read_server_cmd_data(data, sizeof(data), &data_len)) {
            if (!saw_ind || (pass > 0u)) {
                break;
            }
            continue;
        }
        if (data_len == 0u) {
            break;
        }
        prv_dispatch_server_cmd_frames(data, data_len);
        if (data_len < GW_CATM1_SERVER_CMD_READLEN) {
            break;
        }
        prv_delay_ms(20u);
    }
}

static bool prv_send_tcp_payload(const char* payload)
{
    char cmd[48];
    char rsp[UI_CATM1_RX_BUF_SZ];
    size_t len;

    if (payload == NULL) {
        return false;
    }
    if (prv_tcp_blocked_by_ble()) {
        return false;
    }
    len = strlen(payload);
    if ((len == 0u) || (len > 1460u)) {
        return false;
    }

    /* TCP 전송 경로는 APN -> CEREG -> CNACT -> CAOPEN 이후 곧바로 payload를 보낸다. */
    (void)snprintf(cmd, sizeof(cmd), "AT+CASEND=0,%u,%u\r\n", (unsigned)len, (unsigned)UI_CATM1_SEND_INPUT_TIMEOUT_MS);
    if (!prv_send_cmd_wait(cmd, ">", NULL, NULL, UI_CATM1_AT_TIMEOUT_MS, rsp, sizeof(rsp))) {
        return false;
    }
    if (!prv_uart_send_bytes(payload, (uint16_t)len, UI_CATM1_SEND_INPUT_TIMEOUT_MS + 2000u)) {
        return false;
    }
    if (!prv_uart_wait_for(rsp, sizeof(rsp), UI_CATM1_SEND_INPUT_TIMEOUT_MS + 3000u, "OK", NULL, NULL)) {
        return false;
    }
    if ((strstr(rsp, "+CADATAIND: 0") != NULL) ||
        (strstr(rsp, "+CAURC: \"recv\",0,") != NULL)) {
        s_catm1_server_cmd_ind_seen = true;
    }

    prv_wait_post_send_settle();
    return true;
}

static bool prv_node_valid(const GW_NodeRec_t* r)
{
    if (r == NULL) {
        return false;
    }
    if (r->batt_lvl != UI_NODE_BATT_LVL_INVALID) {
        return true;
    }
    if (r->temp_c != UI_NODE_TEMP_INVALID_C) {
        return true;
    }
    if (r->x != 0xFFFFu) {
        return true;
    }
    if (r->y != 0xFFFFu) {
        return true;
    }
    if (r->z != 0xFFFFu) {
        return true;
    }
    if (r->adc != 0xFFFFu) {
        return true;
    }
    if (r->pulse_cnt != 0xFFFFFFFFu) {
        return true;
    }
    return false;
}

static uint32_t prv_get_snapshot_node_limit(void)
{
    /* TCP payload는 설정값(max_nodes)보다 실제 record 내용을 우선한다.
     * 최신 설정/명령 반영이 어긋나도 이미 받은 ND 데이터가 서버 payload에서
     * 사라지지 않도록 record 전체 범위를 스캔한다. */
    return UI_MAX_NODES;
}

static bool prv_is_ascii_space(char ch)
{
    return (ch == ' ') || (ch == '\t') || (ch == '\r') || (ch == '\n');
}

static void prv_trim_ascii_inplace(char* s)
{
    size_t start = 0u;
    size_t len;

    if (s == NULL) {
        return;
    }

    while (prv_is_ascii_space(s[start])) {
        start++;
    }

    if (start > 0u) {
        memmove(s, &s[start], strlen(&s[start]) + 1u);
    }

    len = strlen(s);
    while ((len > 0u) && prv_is_ascii_space(s[len - 1u])) {
        s[len - 1u] = '\0';
        len--;
    }
}

static void prv_get_loc_ascii_safe(char* out, size_t out_sz)
{
    const char* src;
    char tmp[192];
    size_t si = 0u;
    size_t di = 0u;
    char* sep = NULL;

    if ((out == NULL) || (out_sz == 0u)) {
        return;
    }

    src = UI_GetLocAscii();
    if ((src == NULL) || (src[0] == '\0')) {
        (void)snprintf(out, out_sz, "-");
        return;
    }

    while ((src[si] != '\0') && ((di + 1u) < sizeof(tmp))) {
        char ch = src[si++];

        if ((ch == '\r') || (ch == '\n')) {
            ch = ' ';
        }
        tmp[di++] = ch;
    }
    tmp[di] = '\0';
    prv_trim_ascii_inplace(tmp);

    if (tmp[0] == '\0') {
        (void)snprintf(out, out_sz, "-");
        return;
    }

    if ((tmp[0] == '(') && (tmp[strlen(tmp) - 1u] == ')')) {
        (void)snprintf(out, out_sz, "%s", tmp);
        return;
    }

    sep = strchr(tmp, ';');
    if (sep == NULL) {
        sep = strchr(tmp, ',');
    }

    if ((sep != NULL) &&
        (strchr(sep + 1, ';') == NULL) &&
        (strchr(sep + 1, ',') == NULL)) {
        char left[96];
        char right[96];
        size_t left_len = (size_t)(sep - tmp);

        memset(left, 0, sizeof(left));
        memset(right, 0, sizeof(right));
        if (left_len >= sizeof(left)) {
            left_len = sizeof(left) - 1u;
        }
        memcpy(left, tmp, left_len);
        left[left_len] = '\0';
        (void)snprintf(right, sizeof(right), "%s", sep + 1);
        prv_trim_ascii_inplace(left);
        prv_trim_ascii_inplace(right);

        if ((left[0] != '\0') && (right[0] != '\0')) {
            (void)snprintf(out, out_sz, "(%s,%s)", left, right);
            return;
        }
    }

    di = 0u;
    for (si = 0u; (tmp[si] != '\0') && ((di + 1u) < out_sz); si++) {
        char ch = tmp[si];

        if (ch == ',') {
            ch = ';';
        }
        out[di++] = ch;
    }
    out[di] = '\0';

    if (out[0] == '\0') {
        (void)snprintf(out, out_sz, "-");
    }
}

static void prv_append_fmt(char* out, size_t out_sz, size_t* io_len, const char* fmt, ...)
{
    va_list ap;
    int n;

    if ((out == NULL) || (io_len == NULL) || (*io_len >= out_sz)) {
        return;
    }

    va_start(ap, fmt);
    n = vsnprintf(&out[*io_len], out_sz - *io_len, fmt, ap);
    va_end(ap);

    if (n <= 0) {
        return;
    }
    if ((size_t)n >= (out_sz - *io_len)) {
        *io_len = out_sz - 1u;
        out[*io_len] = '\0';
        return;
    }
    *io_len += (size_t)n;
}

static size_t prv_build_snapshot_payload(const GW_HourRec_t* rec, char* out, size_t out_sz)
{
    const UI_Config_t* cfg = UI_GetConfig();
    size_t len = 0u;
    uint32_t i;
    uint32_t node_limit;
    bool truncated = false;
    char set0, set1, set2;
    char loc_buf[192];
    char ts_buf[32];
    const char* gw_volt_text;
    int gw_temp_c;

    if ((rec == NULL) || (out == NULL) || (out_sz < 64u) || (cfg == NULL)) {
        return 0u;
    }

    out[0] = '\0';
    node_limit = prv_get_snapshot_node_limit();
    prv_get_loc_ascii_safe(loc_buf, sizeof(loc_buf));
    set0 = (char)cfg->setting_ascii[0];
    set1 = (char)cfg->setting_ascii[1];
    set2 = (char)cfg->setting_ascii[2];
    prv_format_epoch2016(rec->epoch_sec, ts_buf, sizeof(ts_buf));
    gw_volt_text = prv_gw_voltage_text(rec->gw_volt_x10);
    gw_temp_c = (int)prv_apply_tcp_temp_comp_c(rec->gw_temp_c);

    prv_append_fmt(out, out_sz, &len,
                   "T:%s,NID:%.*s,GW:%u,L:%s,S:%c%c%c,GV:%s,GT:%d",
                   ts_buf,
                   (int)UI_NET_ID_LEN,
                   (const char*)cfg->net_id,
                   (unsigned)cfg->gw_num,
                   loc_buf,
                   set0, set1, set2,
                   gw_volt_text,
                   gw_temp_c);

    for (i = 0u; i < node_limit; i++) {
        const GW_NodeRec_t* r = &rec->nodes[i];
        const char* node_volt_text;
        int node_temp_c;
        bool icm_valid;

        if (!prv_node_valid(r)) {
            continue;
        }
        if ((out_sz - len) < 128u) {
            truncated = true;
            break;
        }

        node_volt_text = prv_node_voltage_text(r->batt_lvl);
        node_temp_c = (int)r->temp_c;
        icm_valid = ((r->x != 0xFFFFu) ||
                     (r->y != 0xFFFFu) ||
                     (r->z != 0xFFFFu));

        prv_append_fmt(out, out_sz, &len,
                       ",ND:%02lu,V:%s,T:%d",
                       (unsigned long)i,
                       node_volt_text,
                       node_temp_c);

        if (icm_valid) {
            prv_append_fmt(out, out_sz, &len,
                           ",X:%u,Y:%u,Z:%u",
                           (unsigned)r->x,
                           (unsigned)r->y,
                           (unsigned)r->z);
        }
        if (r->adc != 0xFFFFu) {
            prv_append_fmt(out, out_sz, &len,
                           ",A:%u",
                           (unsigned)r->adc);
        }
        if (r->pulse_cnt != 0xFFFFFFFFu) {
            prv_append_fmt(out, out_sz, &len,
                           ",P:%lu",
                           (unsigned long)r->pulse_cnt);
        }
    }
    if (truncated) {
        prv_append_fmt(out, out_sz, &len, ",TRUNC:1");
    }
    prv_append_fmt(out, out_sz, &len, "\r\n");
    return len;
}

void GW_Catm1_UartRxCpltCallback(UART_HandleTypeDef *huart)
{
    if (huart != &hlpuart1) {
        return;
    }
    if (!s_catm1_rb_ready) {
        prv_catm1_rb_reset();
    }
    if (s_catm1_rx_byte != 0u) {
        (void)UI_RingBuf_Push(&s_catm1_rb, s_catm1_rx_byte);
        s_catm1_last_rx_ms = HAL_GetTick();
    }
    prv_catm1_rx_start_it();
}

void GW_Catm1_UartErrorCallback(UART_HandleTypeDef *huart)
{
    if (huart != &hlpuart1) {
        return;
    }
    prv_catm1_rx_start_it();
}

void GW_Catm1_Init(void)
{
    prv_catm1_rb_reset();
    s_catm1_power_fault_stop_request = false;
    s_catm1_session_at_ok = false;
    s_catm1_boot_sms_ready_seen = false;
    s_catm1_waiting_boot_sms_ready = false;
    s_catm1_tcp_open_fail_powerdown_pending = false;
    s_catm1_expect_sms_ready_token = false;
    s_catm1_session_sms_ready_loop_seen = false;
    s_catm1_sms_ready_loop_attempt_count = 0u;
    s_catm1_last_caopen_ms = 0u;
    s_catm1_time_auto_update_attempted_this_power = false;
    s_catm1_time_synced_from_urc_this_power = false;
    s_catm1_tcp_send_session_active = false;
    s_catm1_tcp_time_sync_pending = false;
    s_catm1_server_cmd_ind_seen = false;
    s_catm1_startup_apn_configured_this_power = false;
    s_catm1_bandcfg_applied_this_power = false;
#if defined(PWR_KEY_Pin)
    HAL_GPIO_WritePin(PWR_KEY_GPIO_Port, PWR_KEY_Pin, UI_CATM1_PWRKEY_INACTIVE_STATE);
#endif
#if defined(CATM1_PWR_Pin)
    HAL_GPIO_WritePin(CATM1_PWR_GPIO_Port, CATM1_PWR_Pin, GPIO_PIN_RESET);
#endif
    /* 부팅 직후 기본 idle 전류를 줄이기 위해 LPUART도 즉시 내려 둔다. */
    prv_lpuart_release();
    s_catm1_last_poweroff_ms = HAL_GetTick();
    prv_power_leds_blink_twice();
}

void GW_Catm1_PowerOn(void)
{
    uint32_t now = HAL_GetTick();
    s_catm1_session_at_ok = false;
    s_catm1_boot_sms_ready_seen = false;
    s_catm1_waiting_boot_sms_ready = true;
    s_catm1_tcp_open_fail_powerdown_pending = false;
    s_catm1_expect_sms_ready_token = false;
    s_catm1_session_sms_ready_loop_seen = false;
    s_catm1_last_caopen_ms = 0u;
    s_catm1_time_auto_update_attempted_this_power = false;
    s_catm1_time_synced_from_urc_this_power = false;
    s_catm1_tcp_send_session_active = false;
    s_catm1_tcp_time_sync_pending = false;
    s_catm1_server_cmd_ind_seen = false;
    s_catm1_startup_apn_configured_this_power = false;
    s_catm1_bandcfg_applied_this_power = false;

    if (s_catm1_last_poweroff_ms != 0u) {
        uint32_t elapsed = (uint32_t)(now - s_catm1_last_poweroff_ms);
        if (elapsed < GW_CATM1_POWER_CYCLE_GUARD_MS) {
            prv_delay_ms(GW_CATM1_POWER_CYCLE_GUARD_MS - elapsed);
        }
    }
#if defined(PWR_KEY_Pin)
    HAL_GPIO_WritePin(PWR_KEY_GPIO_Port, PWR_KEY_Pin, UI_CATM1_PWRKEY_INACTIVE_STATE);
#endif
#if defined(CATM1_PWR_Pin)
    HAL_GPIO_WritePin(CATM1_PWR_GPIO_Port, CATM1_PWR_Pin, GPIO_PIN_SET);
#endif
#if defined(PWR_KEY_Pin)
    prv_delay_ms(UI_CATM1_PWRKEY_GUARD_MS);
    HAL_GPIO_WritePin(PWR_KEY_GPIO_Port, PWR_KEY_Pin, UI_CATM1_PWRKEY_ACTIVE_STATE);
    prv_delay_ms(UI_CATM1_PWRKEY_ON_PULSE_MS);
    HAL_GPIO_WritePin(PWR_KEY_GPIO_Port, PWR_KEY_Pin, UI_CATM1_PWRKEY_INACTIVE_STATE);
#endif
}

static void prv_prepare_low_current_before_poweroff(void)
{
    char rsp[UI_CATM1_RX_BUF_SZ];

    if (!prv_lpuart_is_inited() || !s_catm1_session_at_ok) {
        return;
    }

    prv_wait_rx_quiet(50u, 200u);
    rsp[0] = '\0';
    if (prv_send_cmd_wait("AT+CFUN=0\r\n", "OK", NULL, NULL,
                          GW_CATM1_POWEROFF_CFUN_TIMEOUT_MS, rsp, sizeof(rsp))) {
        prv_wait_rx_quiet(50u, GW_CATM1_POWEROFF_CFUN_SETTLE_MS);
    }
}

static bool prv_try_normal_power_down(bool reduce_current_first)
{
    char rsp[UI_CATM1_RX_BUF_SZ];

#if defined(PWR_KEY_Pin)
    HAL_GPIO_WritePin(PWR_KEY_GPIO_Port, PWR_KEY_Pin, UI_CATM1_PWRKEY_INACTIVE_STATE);
#endif
    if (!prv_lpuart_is_inited() || !s_catm1_session_at_ok) {
        return false;
    }
    if (reduce_current_first) {
        prv_prepare_low_current_before_poweroff();
    }
    if (!prv_send_cmd_wait("AT+CPOWD=1\r\n", "NORMAL POWER DOWN", "OK", NULL,
                           UI_CATM1_PWRDOWN_TIMEOUT_MS, rsp, sizeof(rsp))) {
        return false;
    }

    prv_delay_ms(UI_CATM1_PWRDOWN_WAIT_MS);
    return true;
}

void GW_Catm1_PowerOff(void)
{
    bool normal_pd = false;
    bool fast_pd = s_catm1_tcp_open_fail_powerdown_pending;
    uint32_t pd_wait_ms = UI_CATM1_PWRDOWN_WAIT_MS;

    if (fast_pd) {
        uint32_t elapsed = 0u;

        if (s_catm1_last_caopen_ms != 0u) {
            elapsed = (uint32_t)(HAL_GetTick() - s_catm1_last_caopen_ms);
        }
        if (elapsed < GW_CATM1_TCP_OPEN_FAIL_CPOWD_DELAY_MS) {
            prv_delay_ms(GW_CATM1_TCP_OPEN_FAIL_CPOWD_DELAY_MS - elapsed);
        }

#if defined(PWR_KEY_Pin)
        HAL_GPIO_WritePin(PWR_KEY_GPIO_Port, PWR_KEY_Pin, UI_CATM1_PWRKEY_INACTIVE_STATE);
#endif
        if (prv_lpuart_is_inited()) {
            prv_uart_flush_rx();
            (void)prv_uart_send_text("AT+CPOWD=1\r\n", UI_CATM1_AT_TIMEOUT_MS);
            prv_delay_ms(GW_CATM1_TCP_OPEN_FAIL_CPOWD_POST_TX_HOLD_MS);
        }
#if defined(CATM1_PWR_Pin)
        HAL_GPIO_WritePin(CATM1_PWR_GPIO_Port, CATM1_PWR_Pin, GPIO_PIN_RESET);
#elif defined(PWR_KEY_Pin)
        HAL_GPIO_WritePin(PWR_KEY_GPIO_Port, PWR_KEY_Pin, UI_CATM1_PWRKEY_ACTIVE_STATE);
        prv_delay_ms(UI_CATM1_PWRKEY_OFF_PULSE_MS);
        HAL_GPIO_WritePin(PWR_KEY_GPIO_Port, PWR_KEY_Pin, UI_CATM1_PWRKEY_INACTIVE_STATE);
#endif
        prv_finish_power_off_state();
        return;
    }

    /* Default session shutdown path keeps the low-current CFUN=0 step first. */
    normal_pd = prv_try_normal_power_down(true);
#if defined(PWR_KEY_Pin)
    if (!normal_pd) {
        HAL_GPIO_WritePin(PWR_KEY_GPIO_Port, PWR_KEY_Pin, UI_CATM1_PWRKEY_ACTIVE_STATE);
        prv_delay_ms(UI_CATM1_PWRKEY_OFF_PULSE_MS);
        HAL_GPIO_WritePin(PWR_KEY_GPIO_Port, PWR_KEY_Pin, UI_CATM1_PWRKEY_INACTIVE_STATE);
        prv_delay_ms(pd_wait_ms);
    }
#endif
#if defined(CATM1_PWR_Pin)
    HAL_GPIO_WritePin(CATM1_PWR_GPIO_Port, CATM1_PWR_Pin, GPIO_PIN_RESET);
#endif
    prv_finish_power_off_state();
}

bool GW_Catm1_IsBusy(void)
{
    return s_catm1_busy;
}

void GW_Catm1_SetBusy(bool busy)
{
    s_catm1_busy = busy;
}

bool GW_Catm1_ConsumePowerFaultStopRequest(void)
{
    bool pending = s_catm1_power_fault_stop_request;

    s_catm1_power_fault_stop_request = false;
    return pending;
}

void GW_Catm1_ClearTimeSyncDeltaBuf(void)
{
    prv_time_sync_delta_reset();
}

uint8_t GW_Catm1_GetTimeSyncDeltaCount(void)
{
    return s_time_sync_delta_count;
}

uint8_t GW_Catm1_CopyTimeSyncDeltaBuf(int64_t* out_buf, uint8_t max_items)
{
    uint8_t stored;
    uint8_t cnt;
    uint8_t i;
    uint8_t oldest;
    uint8_t start;
    uint8_t skip;

    if ((out_buf == NULL) || (max_items == 0u)) {
        return 0u;
    }

    stored = s_time_sync_delta_count;
    cnt = stored;
    if (cnt > max_items) {
        cnt = max_items;
    }
    oldest = (stored < GW_CATM1_TIME_SYNC_DELTA_BUF_LEN) ? 0u : s_time_sync_delta_wr;
    skip = (uint8_t)(stored - cnt);
    start = (uint8_t)((oldest + skip) % GW_CATM1_TIME_SYNC_DELTA_BUF_LEN);
    for (i = 0u; i < cnt; i++) {
        out_buf[i] = s_time_sync_delta_sec_buf[(uint8_t)((start + i) % GW_CATM1_TIME_SYNC_DELTA_BUF_LEN)];
    }
    return cnt;
}

bool GW_Catm1_SyncTimeOnce(void)
{
    bool success = false;
    uint32_t attempt = 0u;

    if (GW_Catm1_IsBusy()) {
        return false;
    }

    s_catm1_boot_time_sync_strict_order_active = true;
    /* MCU가 다시 부팅된 경우라도 boot one-shot sync에서는
     * 첫 세팅을 무조건 APN bootstrap으로 다시 보장한다. */
    s_catm1_startup_apn_configured_this_power = false;
    s_catm1_pending_psuttz_valid = false;
    s_catm1_pending_psuttz_epoch_centi = 0u;

    UI_LPM_LockStop();
    GW_Catm1_SetBusy(true);
    if (!prv_lpuart_ensure()) {
        goto cleanup;
    }

    for (attempt = 0u; attempt < GW_CATM1_STARTUP_SYNC_ATTEMPTS; attempt++) {
        prv_begin_sms_ready_loop_attempt();

        if ((attempt > 0u) && !prv_was_powered_off_recently(GW_CATM1_RECENT_POWEROFF_SKIP_MS)) {
            prv_force_power_cut();
        }

        if (!prv_start_session(false)) {
            prv_finish_sms_ready_loop_attempt(false);
            if (s_catm1_sms_ready_loop_attempt_count >= GW_CATM1_SMS_READY_LOOP_STOP_TRY) {
                break;
            }
            continue;
        }

        success = prv_startup_time_sync_user_sequence();
        prv_finish_sms_ready_loop_attempt(success);
        if (success || (s_catm1_sms_ready_loop_attempt_count >= GW_CATM1_SMS_READY_LOOP_STOP_TRY)) {
            break;
        }
    }

cleanup:
    prv_delay_ms(GW_CATM1_POST_TIME_SYNC_POWER_CUT_GUARD_MS);
    if (!prv_was_powered_off_recently(GW_CATM1_RECENT_POWEROFF_SKIP_MS)) {
        prv_shutdown_modem_prefer_poweroff();
    }
    prv_lpuart_release();
    GW_Catm1_SetBusy(false);
    UI_LPM_UnlockStop();
    s_catm1_boot_time_sync_strict_order_active = false;

    return success;
}

bool GW_Catm1_QueryAndStoreLoc(char* out_line, size_t out_sz)
{
    char loc_line[GW_LOC_LINE_MAX];
    GW_LocRec_t loc_rec;
    bool success = false;

    if (GW_Catm1_IsBusy()) {
        return false;
    }
    if ((out_line != NULL) && (out_sz > 0u)) {
        out_line[0] = '\0';
    }

    UI_LPM_LockStop();
    GW_Catm1_SetBusy(true);
    if (!prv_lpuart_ensure()) {
        goto cleanup;
    }
    prv_begin_sms_ready_loop_attempt();
    if (!prv_start_session(true)) {
        prv_finish_sms_ready_loop_attempt(false);
        goto cleanup;
    }
    if (prv_wait_eps_registered() && prv_wait_ps_attached()) {
        (void)prv_sync_time_from_modem_startup_try(true, false, NULL);
    }
    if (!prv_query_gnss_loc_line(loc_line, sizeof(loc_line))) {
        goto cleanup;
    }

    memset(&loc_rec, 0, sizeof(loc_rec));
    loc_rec.saved_epoch_sec = UI_Time_NowSec2016();
    (void)snprintf(loc_rec.line, sizeof(loc_rec.line), "%s", loc_line);
    if (!GW_Storage_SaveLocRec(&loc_rec)) {
        goto cleanup;
    }
    if ((out_line != NULL) && (out_sz > 0u)) {
        (void)snprintf(out_line, out_sz, "%s", loc_rec.line);
    }
    success = true;

cleanup:
    prv_finish_sms_ready_loop_attempt(success);
    if (!prv_was_powered_off_recently(GW_CATM1_RECENT_POWEROFF_SKIP_MS)) {
        GW_Catm1_PowerOff();
    }
    prv_lpuart_release();
    GW_Catm1_SetBusy(false);
    UI_LPM_UnlockStop();
    return success;
}

static void prv_note_failed_snapshot_sent(void)
{
    s_failed_snapshot_queued_valid = false;
    s_failed_snapshot_queued_epoch_sec = 0u;
    s_failed_snapshot_queued_gw_num = 0u;
}

static bool prv_flash_tail_matches_hour_rec(const GW_HourRec_t* rec)
{
    GW_FileRec_t last_rec;
    uint32_t tail;
    const UI_Config_t* cfg = UI_GetConfig();
    uint8_t cur_gw_num = (cfg != NULL) ? cfg->gw_num : 0u;

    if (rec == NULL) {
        return false;
    }

    tail = GW_Storage_GetTotalRecordCount();
    if (tail == 0u) {
        return false;
    }
    if (!GW_Storage_ReadRecordByGlobalIndex(tail - 1u, &last_rec, NULL)) {
        return false;
    }

    return ((last_rec.gw_num == cur_gw_num) &&
            (last_rec.rec.epoch_sec == rec->epoch_sec));
}

static bool prv_store_failed_snapshot_to_flash(const GW_HourRec_t* rec)
{
    uint32_t before_cnt;
    uint32_t after_cnt;
    uint32_t try_idx;
    const UI_Config_t* cfg;
    uint8_t cur_gw_num;

    if (rec == NULL) {
        return false;
    }

    cfg = UI_GetConfig();
    cur_gw_num = (cfg != NULL) ? cfg->gw_num : 0u;

    if (s_failed_snapshot_queued_valid &&
        (s_failed_snapshot_queued_epoch_sec == rec->epoch_sec) &&
        (s_failed_snapshot_queued_gw_num == cur_gw_num)) {
        return true;
    }

    if (prv_flash_tail_matches_hour_rec(rec)) {
        s_failed_snapshot_queued_valid = true;
        s_failed_snapshot_queued_epoch_sec = rec->epoch_sec;
        s_failed_snapshot_queued_gw_num = cur_gw_num;
        return true;
    }

    before_cnt = GW_Storage_GetTotalRecordCount();
    for (try_idx = 0u; try_idx < 2u; try_idx++) {
        if (!GW_Storage_SaveHourRec(rec)) {
            continue;
        }
        after_cnt = GW_Storage_GetTotalRecordCount();
        if (after_cnt > before_cnt) {
            s_failed_snapshot_queued_valid = true;
            s_failed_snapshot_queued_epoch_sec = rec->epoch_sec;
            s_failed_snapshot_queued_gw_num = cur_gw_num;
            return true;
        }
    }

    return false;
}

bool GW_Catm1_SendSnapshot(const GW_HourRec_t* rec)
{
    uint8_t ip[4];
    uint16_t port;
    char payload[UI_CATM1_SERVER_PAYLOAD_MAX + 1u];
    char rsp[UI_CATM1_RX_BUF_SZ];
    bool success = false;
    bool opened = false;
    bool pdp_active = false;
    bool should_store_on_fail = false;
    size_t len;
    GW_HourRec_t live_rec;

    if ((!GW_App_IsTcpEnabled()) || (rec == NULL)) {
        return false;
    }

    if (!GW_App_CopyTcpSnapshotRecord(rec, &live_rec)) {
        return false;
    }
    should_store_on_fail = true;

    if ((!GW_App_IsTcpEnabled()) || prv_tcp_blocked_by_ble()) {
        if (GW_App_IsTcpEnabled()) {
            (void)prv_store_failed_snapshot_to_flash(&live_rec);
        }
        return false;
    }

    len = prv_build_snapshot_payload(&live_rec, payload, sizeof(payload));
    if (len == 0u) {
        return false;
    }

    if ((!GW_App_IsTcpEnabled()) || prv_tcp_blocked_by_ble()) {
        if (GW_App_IsTcpEnabled()) {
            (void)prv_store_failed_snapshot_to_flash(&live_rec);
        }
        return false;
    }

    prv_get_server(ip, &port);
    UI_LPM_LockStop();
    GW_Catm1_SetBusy(true);
    if (!prv_lpuart_ensure()) {
        goto cleanup;
    }
    prv_begin_sms_ready_loop_attempt();
    if (!prv_start_session(true)) {
        prv_finish_sms_ready_loop_attempt(false);
        goto cleanup;
    }
    s_catm1_tcp_send_session_active = true;
    if ((!GW_App_IsTcpEnabled()) || !prv_prepare_tcp_send_user_sequence()) {
        goto cleanup;
    }
    pdp_active = true;
    if ((!GW_App_IsTcpEnabled()) || !prv_open_tcp(ip, port)) {
        goto cleanup;
    }
    opened = true;
    if ((!GW_App_IsTcpEnabled()) || !prv_send_tcp_payload(payload)) {
        goto cleanup;
    }
    prv_receive_server_cmd_after_first_payload();
    success = true;
    prv_note_failed_snapshot_sent();

cleanup:
    prv_consume_deferred_tcp_session_time_sync();
    s_catm1_tcp_send_session_active = false;
    prv_finish_sms_ready_loop_attempt(success);
    prv_close_tcp_and_force_power_cut(opened, rsp, sizeof(rsp));
    (void)pdp_active;
    prv_lpuart_release();
    GW_Catm1_SetBusy(false);
    UI_LPM_UnlockStop();
    if ((!success) && should_store_on_fail && GW_App_IsTcpEnabled()) {
        (void)prv_store_failed_snapshot_to_flash(&live_rec);
    }
    return success;
}

bool GW_Catm1_SendStoredRange(uint32_t first_rec_index, uint32_t max_count, uint32_t* out_sent_count)
{
    uint8_t ip[4];
    uint16_t port;
    char payload[UI_CATM1_SERVER_PAYLOAD_MAX + 1u];
    char rsp[UI_CATM1_RX_BUF_SZ];
    GW_FileRec_t file_rec;
    bool success = false;
    bool pdp_active = false;
    bool opened = false;
    uint32_t i;

    if (out_sent_count != NULL) {
        *out_sent_count = 0u;
    }
    if ((!GW_App_IsTcpEnabled()) || (max_count == 0u) || (out_sent_count == NULL)) {
        return false;
    }
    if (!GW_Storage_ReadRecordByGlobalIndex(first_rec_index, &file_rec, NULL)) {
        return false;
    }
    if (prv_tcp_blocked_by_ble()) {
        return false;
    }

    prv_get_server(ip, &port);
    UI_LPM_LockStop();
    GW_Catm1_SetBusy(true);
    if (!prv_lpuart_ensure()) {
        goto cleanup;
    }
    prv_begin_sms_ready_loop_attempt();
    if (!prv_start_session(true)) {
        prv_finish_sms_ready_loop_attempt(false);
        goto cleanup;
    }
    s_catm1_tcp_send_session_active = true;
    if ((!GW_App_IsTcpEnabled()) || !prv_prepare_tcp_send_user_sequence()) {
        goto cleanup;
    }
    pdp_active = true;
    if ((!GW_App_IsTcpEnabled()) || !prv_open_tcp(ip, port)) {
        goto cleanup;
    }
    opened = true;

    for (i = 0u; i < max_count; i++) {
        size_t len;
        GW_HourRec_t tx_rec;

        if ((!GW_App_IsTcpEnabled()) || prv_tcp_blocked_by_ble()) {
            break;
        }
        if (!GW_Storage_ReadRecordByGlobalIndex(first_rec_index + i, &file_rec, NULL)) {
            break;
        }
        if (!GW_App_CopyTcpSnapshotRecord(&file_rec.rec, &tx_rec)) {
            break;
        }
        len = prv_build_snapshot_payload(&tx_rec, payload, sizeof(payload));
        if (len == 0u) {
            break;
        }
        if ((!GW_App_IsTcpEnabled()) || !prv_send_tcp_payload(payload)) {
            break;
        }
        if ((*out_sent_count) == 0u) {
            prv_receive_server_cmd_after_first_payload();
        }
        (*out_sent_count)++;
        prv_note_failed_snapshot_sent();
    }
    success = ((*out_sent_count) > 0u);

cleanup:
    prv_consume_deferred_tcp_session_time_sync();
    s_catm1_tcp_send_session_active = false;
    prv_finish_sms_ready_loop_attempt(success);
    prv_close_tcp_and_force_power_cut(opened, rsp, sizeof(rsp));
    (void)pdp_active;
    prv_lpuart_release();
    GW_Catm1_SetBusy(false);
    UI_LPM_UnlockStop();
    return success;
}
