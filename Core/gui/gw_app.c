#include "gw_app.h"
#include "ui_conf.h"
#include "ui_types.h"
#include "ui_time.h"
#include "ui_packets.h"
#include "ui_rf_plan_kr920.h"
#include "ui_lpm.h"
#include "ui_uart.h"
#include "ui_radio.h"
#include "ui_ble.h"
#include "gw_storage.h"
#include "gw_catm1.h"
#include "gw_sensors.h"
#include "gw_ble_report.h"
#include "stm32_timer.h"
#include "stm32_seq.h"
#include "radio.h"
#include "main.h"
#include <string.h>

/* -------------------------------------------------------------------------- */
/* Gateway 상태 */
/* -------------------------------------------------------------------------- */
typedef enum {
    GW_STATE_IDLE = 0,
    GW_STATE_BEACON_TX,
    GW_STATE_RX_SLOTS,
    GW_STATE_SYNC_WAIT_RX,
    GW_STATE_SYNC_WAIT_TX,
} GW_State_t;

typedef enum {
    GW_BEACON_TX_KIND_NONE = 0,
    GW_BEACON_TX_KIND_MANUAL,
    GW_BEACON_TX_KIND_SCHEDULED,
    GW_BEACON_TX_KIND_REMINDER,
} GW_BeaconTxKind_t;

static GW_State_t s_state = GW_STATE_IDLE;
static bool s_inited = false;
static bool s_tcp_enabled = true;
static UTIL_TIMER_Object_t s_tmr_wakeup;
static UTIL_TIMER_Object_t s_tmr_led1_pulse;
static UTIL_TIMER_Object_t s_tmr_ble_test_expire;
static volatile uint32_t s_evt_flags = 0;

/* gw_catm1.c가 아직 교체되지 않은 빌드에서도 링크가 깨지지 않도록 fallback 제공.
 * 최신 gw_catm1.c가 함께 빌드되면 strong definition이 우선된다. */
__attribute__((weak)) bool GW_Catm1_ConsumePowerFaultStopRequest(void)
{
    return false;
}

#define GW_EVT_WAKEUP           (1u << 0)
#define GW_EVT_BEACON_ONESHOT   (1u << 1)
#define GW_EVT_RADIO_TX_DONE    (1u << 2)
#define GW_EVT_RADIO_TX_TIMEOUT (1u << 3)
#define GW_EVT_RADIO_RX_DONE    (1u << 4)
#define GW_EVT_RADIO_RX_TIMEOUT (1u << 5)
#define GW_EVT_RADIO_RX_ERROR   (1u << 6)
#define GW_EVT_BLE_TEST_EXPIRE  (1u << 7)


static uint32_t prv_evt_lock(void)
{
    uint32_t primask = __get_PRIMASK();
    __disable_irq();
    return primask;
}

static void prv_evt_unlock(uint32_t primask)
{
    __set_PRIMASK(primask);
}

static void prv_evt_set(uint32_t flags)
{
    uint32_t primask;

    if (flags == 0u) {
        return;
    }

    primask = prv_evt_lock();
    s_evt_flags |= flags;
    prv_evt_unlock(primask);
}

static void prv_evt_clear(uint32_t flags)
{
    uint32_t primask;

    if (flags == 0u) {
        return;
    }

    primask = prv_evt_lock();
    s_evt_flags &= ~flags;
    prv_evt_unlock(primask);
}

static void prv_evt_clear_all(void)
{
    uint32_t primask = prv_evt_lock();
    s_evt_flags = 0u;
    prv_evt_unlock(primask);
}

static uint32_t prv_evt_fetch_and_clear_all(void)
{
    uint32_t primask;
    uint32_t flags;

    primask = prv_evt_lock();
    flags = s_evt_flags;
    s_evt_flags = 0u;
    prv_evt_unlock(primask);

    return flags;
}

#define GW_FLASH_TX_BACKLOG_MAX (24u * 5u)
#define GW_BLE_TEST_SESSION_MS  (60u * 60u * 1000u)
#define GW_RX_WINDOW_GUARD_MS   (2000u)
#define GW_RX_PRESTART_MS       (1000u)
#define GW_TX_EVT_SAFETY_WAKE_MS (80u)
#define GW_RX_EVT_SAFETY_SLACK_MS (20u)

static bool s_test_mode = false;
static bool s_ble_test_session_active = false;
static uint16_t s_beacon_counter = 0;
static uint8_t s_slot_cnt = 0;
static uint8_t s_rx_expected_nodes = 0u;
static uint32_t s_rx_window_deadline_ms = 0u;
static uint32_t s_rx_cycle_start_ms = 0u;
static uint64_t s_rx_nodes_seen_mask = 0u;
static uint64_t s_last_cycle_nodes_seen_mask = 0u;
static bool s_last_cycle_nodes_seen_mask_valid = false;
static uint32_t s_data_freq_hz = 0;
static bool s_rx_cycle_minute_test = false;
static uint32_t s_rx_cycle_stamp_sec = 0u;
static uint32_t s_catm1_retry_not_before_ms = 0u;
static bool s_beacon_oneshot_pending = false;
static GW_HourRec_t s_hour_rec;
static GW_HourRec_t s_last_cycle_rec;
static bool s_last_cycle_valid = false;
static uint32_t s_last_cycle_minute_id = 0u;
static uint32_t s_last_save_minute_id = 0xFFFFFFFFu;
static bool s_catm1_uplink_pending = false;
static bool s_catm1_immediate_try_pending = false;
static uint32_t s_last_catm1_slot_id = 0xFFFFFFFFu;
static uint32_t s_last_minute_test_uplink_minute_id = 0xFFFFFFFFu;
static uint32_t s_last_live_uplink_epoch_sec = 0xFFFFFFFFu;
static uint32_t s_last_2m_prep_slot_id = 0xFFFFFFFFu;
static uint32_t s_flash_tx_boot_tail_index = 0u;
static uint32_t s_flash_tx_next_send_index = 0u;
static bool s_boot_time_sync_pending = false;
static bool s_beacon_recovery_mode = false;
static uint8_t s_beacon_burst_remaining = 0u;
static uint32_t s_beacon_burst_anchor_sec = 0u;
static uint32_t s_beacon_burst_gap_ms = 0u;
static GW_BeaconTxKind_t s_beacon_tx_kind = GW_BEACON_TX_KIND_NONE;
static uint32_t s_active_periodic_beacon_slot_id = 0xFFFFFFFFu;
static uint32_t s_last_periodic_beacon_slot_id = 0xFFFFFFFFu;
static uint32_t s_sync_wait_deadline_ms = 0u;
static bool s_led1_sync_blink_active = false;
static bool s_led1_sync_blink_on = false;
static bool s_dormant_stop_mode = false;
static bool s_boot_time_sync_beacon_pending = false;

typedef enum {
    GW_CATM1_TX_CTX_NONE = 0,
    GW_CATM1_TX_CTX_LIVE,
    GW_CATM1_TX_CTX_BACKLOG,
} GW_Catm1TxCtxKind_t;

typedef struct {
    bool active;
    GW_Catm1TxCtxKind_t kind;
    bool rec_valid;
    GW_HourRec_t rec;
    uint32_t first_index;
    uint32_t batch_count;
} GW_Catm1TxCtx_t;

static GW_Catm1TxCtx_t s_catm1_tx_ctx = {0};

#define GW_BEACON_BURST_COUNT_NORMAL   (3u)
#define GW_BEACON_BURST_COUNT_RECOVERY (5u)
#define GW_BEACON_BURST_GAP_MS         (250u)
#define GW_BEACON_REMINDER_BURST_COUNT (2u)
#define GW_BEACON_REMINDER_GAP_MS      (200u)
#define GW_SYNC_WAIT_RX_CHUNK_MS       (5000u)
#define GW_SYNC_REQUEST_NODE_NUM       (0xAAu)
#define GW_SYNC_WAIT_MAX_HOURS         (8u)
#define GW_SYNC_LED1_ON_MS             (200u)
#define GW_SYNC_LED1_OFF_MS            (500u)
#define GW_RADIO_LED_PULSE_MS          (50u)

#ifndef GW_CATM1_RETRY_DELAY_MS
#define GW_CATM1_RETRY_DELAY_MS        (120000u)
#endif

#ifndef GW_CATM1_PENDING_POLL_MS
#define GW_CATM1_PENDING_POLL_MS       (1000u)
#endif
#ifndef GW_BOOT_TIME_SYNC_RETRY_DELAY_MS
#define GW_BOOT_TIME_SYNC_RETRY_DELAY_MS (5000u)
#endif

static uint8_t s_beacon_tx_payload[UI_BEACON_PAYLOAD_LEN];
static uint8_t s_beacon_tx_payload_len = 0u;
static uint8_t s_rx_shadow[UI_NODE_PAYLOAD_LEN];
static uint16_t s_rx_shadow_size = 0u;
static int16_t s_rx_shadow_rssi = 0;
static int8_t s_rx_shadow_snr = 0;

static void prv_schedule_wakeup(void);
static void prv_schedule_after_ms(uint32_t delay_ms);
static void prv_arm_busy_state_safety_wakeup(uint32_t delay_ms);
static void prv_requeue_events(uint32_t ev_mask);
static bool prv_arm_rx_slot(void);
static bool prv_rearm_current_rx_slot(void);
static void prv_rx_next_slot(void);
static bool prv_start_beacon_tx(uint32_t now_sec);
static uint8_t prv_get_beacon_burst_count(void);
static void prv_prepare_beacon_burst(uint32_t anchor_sec,
                                    uint8_t total_count,
                                    uint32_t gap_ms,
                                    GW_BeaconTxKind_t kind,
                                    uint32_t periodic_slot_id);
static uint32_t prv_get_beacon_offset_sec(void);
static uint32_t prv_get_beacon_interval_sec(void);
static void prv_schedule_next_second_tick(uint64_t now_centi);
static void prv_reset_rx_cycle_state(void);
static void prv_abort_active_radio_session(void);
static uint32_t prv_get_sync_wait_remaining_ms(void);
static bool prv_arm_sync_wait_rx(void);
static bool prv_start_sync_response_beacon_tx(void);
static bool prv_is_sync_request_payload(const uint8_t *payload, uint16_t size);
static void prv_continue_sync_wait_or_stop(void);
static void prv_finish_sync_wait_and_stop(void);
static void prv_cleanup_sync_wait_context(void);
static void prv_cancel_sync_wait_and_resume_ble(void);
static bool prv_start_sync_wait_mode(uint16_t duration_hours);
static bool prv_is_two_minute_mode_active(void);
static void prv_close_rx_cycle_and_commit(void);
static bool prv_start_pending_beacon_burst(void);
static void prv_cancel_pending_beacon_burst(void);
static uint32_t prv_beacon_slot_id_from_epoch_sec(uint32_t epoch_sec, uint32_t period_sec, uint32_t offset_sec);
static bool prv_get_reminder_offset_sec(uint32_t* out_offset_sec);
static void prv_send_ble_test_report_if_active(const GW_HourRec_t* rec);
static void prv_start_ble_test_session(void);
static void prv_stop_ble_test_session(bool disable_ble_now);
static void prv_get_effective_setting_ascii(uint8_t out_setting_ascii[3]);
static bool prv_should_try_catm1_uplink_now(uint32_t now_sec);
static bool prv_flash_head_matches_last_live_uplink(void);
static void prv_flash_tx_skip_last_live_uplink_head(void);
static bool prv_live_uplink_already_sent(const GW_HourRec_t* rec);
static void prv_note_live_uplink_sent(const GW_HourRec_t* rec);
static bool prv_backlog_batch_included_live_record(uint32_t first_index, uint32_t sent_count, const GW_HourRec_t* rec);
static uint32_t prv_flash_tx_pending_count(void);
static void prv_flash_tx_reset_to_tail(void);
static void prv_flash_tx_note_sent(uint32_t sent_count);
static void prv_exit_dormant_stop_mode(void);
static void prv_enter_dormant_stop_mode(void);
static void prv_enter_scheduled_stop_once(void);
static void prv_led1_sync_blink_stop(void);
static void prv_led1_blocking_pulse_ms(uint32_t pulse_ms);
static void prv_request_schedule_recheck_now(void);
static bool prv_handle_boot_time_sync_beacon(void);
static void prv_request_catm1_uplink(void);
static void prv_request_catm1_uplink_immediate(void);
static void prv_request_ble_stop_for_immediate_tcp_uplink(void);
static bool prv_have_immediate_tcp_uplink_request(void);
static bool prv_tcp_uplink_blocked_by_ble(void);
static bool prv_have_any_tcp_uplink_candidate(void);
static void prv_request_tcp_uplink_after_enable(bool immediate);
static bool prv_restore_runtime_after_sync_wait_end(void);
static void prv_clear_catm1_tx_ctx(void);
static void prv_handle_catm1_job_result(void);
bool GW_App_CopyTcpSnapshotRecord(const GW_HourRec_t* src, GW_HourRec_t* dst);
static void prv_mark_periodic_beacon_slot_consumed_if_due(uint32_t epoch_sec);

static void prv_mark_periodic_beacon_slot_consumed_if_due(uint32_t epoch_sec)
{
    uint32_t beacon_interval = prv_get_beacon_interval_sec();
    uint32_t beacon_off = prv_get_beacon_offset_sec();

    if ((beacon_interval == 0u) || ((epoch_sec % beacon_interval) != beacon_off)) {
        return;
    }

    s_last_periodic_beacon_slot_id = prv_beacon_slot_id_from_epoch_sec(epoch_sec, beacon_interval, beacon_off);
}

static bool prv_handle_boot_time_sync_beacon(void)
{
    uint64_t now_centi;
    uint32_t now_sec;

    if (!s_boot_time_sync_beacon_pending) {
        return false;
    }
    if ((s_state != GW_STATE_IDLE) ||
        (GW_Catm1_GetJobState() != GW_CATM1_JOB_STATE_IDLE) ||
        GW_Catm1_IsBusy()) {
        return false;
    }

    now_centi = UI_Time_NowCenti2016();
    if ((now_centi % 100u) != 0u) {
        prv_schedule_next_second_tick(now_centi);
        return true;
    }

    now_sec = (uint32_t)(now_centi / 100u);
    prv_prepare_beacon_burst(now_sec,
                             prv_get_beacon_burst_count(),
                             GW_BEACON_BURST_GAP_MS,
                             GW_BEACON_TX_KIND_MANUAL,
                             0xFFFFFFFFu);
    if (prv_start_pending_beacon_burst()) {
        s_boot_time_sync_beacon_pending = false;
        prv_mark_periodic_beacon_slot_consumed_if_due(now_sec);
        return true;
    }

    prv_schedule_next_second_tick(UI_Time_NowCenti2016());
    return true;
}

static void prv_cancel_pending_beacon_burst(void)
{
    s_beacon_burst_remaining = 0u;
    s_beacon_burst_anchor_sec = 0u;
    s_beacon_burst_gap_ms = 0u;
    s_beacon_oneshot_pending = false;
    s_beacon_tx_kind = GW_BEACON_TX_KIND_NONE;
    s_active_periodic_beacon_slot_id = 0xFFFFFFFFu;
}

static void prv_exit_dormant_stop_mode(void)
{
    if (!s_dormant_stop_mode) {
        return;
    }

    s_dormant_stop_mode = false;
    prv_evt_clear_all();
}

static void prv_enter_dormant_stop_mode(void)
{
    if (!s_inited) {
        return;
    }

    prv_cancel_pending_beacon_burst();
    (void)UTIL_TIMER_Stop(&s_tmr_wakeup);
    (void)UTIL_TIMER_Stop(&s_tmr_led1_pulse);
    (void)UTIL_TIMER_Stop(&s_tmr_ble_test_expire);
    prv_led1_sync_blink_stop();

    s_ble_test_session_active = false;
    s_beacon_oneshot_pending = false;
    s_boot_time_sync_pending = false;
    s_boot_time_sync_beacon_pending = false;
    s_catm1_uplink_pending = false;
    s_catm1_immediate_try_pending = false;
    s_catm1_retry_not_before_ms = 0u;
    s_last_catm1_slot_id = 0xFFFFFFFFu;
    s_last_minute_test_uplink_minute_id = 0xFFFFFFFFu;
    s_last_live_uplink_epoch_sec = 0xFFFFFFFFu;
    s_last_2m_prep_slot_id = 0xFFFFFFFFu;
    s_last_periodic_beacon_slot_id = 0xFFFFFFFFu;
    s_active_periodic_beacon_slot_id = 0xFFFFFFFFu;
    s_sync_wait_deadline_ms = 0u;
    s_dormant_stop_mode = true;

    prv_evt_clear_all();
    UTIL_SEQ_SetTask(UI_TASK_BIT_GW_MAIN, 0);
}

static void prv_enter_scheduled_stop_once(void)
{
    if (!s_inited) {
        return;
    }

    /* boot-time CAT-M1 시간 확보가 끝난 뒤에는 dormant로 scheduler를 지우지 않고,
     * 다음 RTC/UTIL wake 시 정시 beacon/RX/TCP가 계속 동작하도록 현재 스케줄만 유지한 채
     * 한 번만 STOP에 들어간다. */
    s_dormant_stop_mode = false;
    prv_evt_clear_all();
    prv_schedule_wakeup();
    UI_LPM_EnterStopNow();
}

void GW_App_PrepareForDormantStop(void)
{
    if (!s_inited) {
        return;
    }

    /* BLE OFF/timeout와 RADIO_TX_DONE/RX_DONE가 겹칠 때 이 함수가
     * gw 쪽 pending event / wake timer / radio state를 지워 버리면,
     * IRQ는 왔어도 main process가 그 결과를 처리하기 전에 정보가 사라질 수 있다.
     *
     * 여기서는 BLE test session 상태만 정리하고 GW scheduler/radio state는 유지한다.
     * - TX/RX/sync 진행 중이면 UI_LPM lock 때문에 즉시 STOP 진입이 막힌다.
     * - 진행 중이 아니면 UI_BLE_Process()가 이 상태를 보고 same-loop에서
     *   GW_App_Process()를 바로 다시 태워 CAT-M1을 시작할 수 있다.
     */
    s_ble_test_session_active = false;
    s_dormant_stop_mode = false;

    if (prv_have_immediate_tcp_uplink_request()) {
        /* BLE OFF 직후에는 same-loop 재평가를 먼저 시도하고,
         * 그 직후 STOP으로 내려가더라도 놓치지 않게 1ms wake를 backup으로 둔다. */
        prv_request_schedule_recheck_now();
        prv_schedule_after_ms(1u);
    }
}

bool GW_App_ShouldStayAwakeAfterBleOff(void)
{
    if (!s_inited) {
        return false;
    }

    return prv_have_immediate_tcp_uplink_request();
}

static void prv_tmr_ble_test_expire_cb(void *context)
{
    (void)context;
    prv_evt_set(GW_EVT_BLE_TEST_EXPIRE);
    UTIL_SEQ_SetTask(UI_TASK_BIT_GW_MAIN, 0);
}

static void prv_invalidate_node_rec(GW_NodeRec_t* r)
{
    if (r == NULL) {
        return;
    }

    r->batt_lvl = UI_NODE_BATT_LVL_INVALID;
    r->temp_c = UI_NODE_TEMP_INVALID_C;
    r->x = 0xFFFFu;
    r->y = 0xFFFFu;
    r->z = 0xFFFFu;
    r->adc = 0xFFFFu;
    r->pulse_cnt = 0xFFFFFFFFu;
}

static void prv_sanitize_unreceived_nodes(GW_HourRec_t* rec, uint64_t rx_seen_mask)
{
    uint32_t i;

    if (rec == NULL) {
        return;
    }

    for (i = 0u; i < UI_MAX_NODES; i++) {
        if ((i >= 64u) || ((rx_seen_mask & (1ULL << i)) == 0u)) {
            prv_invalidate_node_rec(&rec->nodes[i]);
        }
    }
}

bool GW_App_CopyTcpSnapshotRecord(const GW_HourRec_t* src, GW_HourRec_t* dst)
{
    if ((!s_tcp_enabled) || (src == NULL) || (dst == NULL)) {
        return false;
    }

    *dst = *src;

    if (s_last_cycle_valid && s_last_cycle_nodes_seen_mask_valid &&
        (src->epoch_sec == s_last_cycle_rec.epoch_sec)) {
        prv_sanitize_unreceived_nodes(dst, s_last_cycle_nodes_seen_mask);
        return true;
    }

    if (src->epoch_sec == s_hour_rec.epoch_sec) {
        prv_sanitize_unreceived_nodes(dst, s_rx_nodes_seen_mask);
    }

    return true;
}


static bool prv_tcp_uplink_blocked_by_ble(void)
{
#if UI_HAVE_BT_EN
    if (!UI_BLE_IsActive()) {
        return false;
    }

    /* 일반 짧은 BLE 세션 동안에는 CAT-M1 TCP를 바로 올리지 않는다.
     * 대신 immediate pending 상태를 유지해서 BLE가 꺼지면 곧바로 재시도한다. */
    return !UI_BLE_IsPersistent();
#else
    return false;
#endif
}

static bool prv_have_any_tcp_uplink_candidate(void)
{
    if (prv_flash_tx_pending_count() > 0u) {
        return true;
    }
    if (s_last_cycle_valid) {
        return true;
    }
    return (s_hour_rec.epoch_sec != 0u);
}

static void prv_request_tcp_uplink_after_enable(bool immediate)
{
    if (!s_tcp_enabled) {
        return;
    }
    if (!prv_have_any_tcp_uplink_candidate()) {
        return;
    }

    if (immediate) {
        prv_request_catm1_uplink_immediate();
    } else {
        prv_request_catm1_uplink();
    }
}

static bool prv_have_immediate_tcp_uplink_request(void)
{
    if (!s_tcp_enabled) {
        return false;
    }
    if (!s_catm1_uplink_pending || !s_catm1_immediate_try_pending) {
        return false;
    }
    if (s_state != GW_STATE_IDLE) {
        return false;
    }
    if (s_sync_wait_deadline_ms != 0u) {
        return false;
    }
    if (s_dormant_stop_mode) {
        return false;
    }
    return true;
}

static void prv_request_ble_stop_for_immediate_tcp_uplink(void)
{
#if UI_HAVE_BT_EN
    if (!prv_have_immediate_tcp_uplink_request()) {
        return;
    }
    if (!UI_BLE_IsActive()) {
        return;
    }
    if (UI_BLE_IsPersistent()) {
        return;
    }

    /* 일반 BLE 명령 세션은 CAT-M1 TCP와 상호배제이므로,
     * TCP ON을 받은 경우에는 BLE를 먼저 정리해서 CAT-M1 시작을 막지 않게 한다.
     * RequestStopNow는 defer 처리라서 현재 명령 응답(OK)을 보낸 뒤에 종료된다. */
    UI_BLE_RequestStopNow();
#endif
}

static uint8_t prv_count_valid_nodes_in_rec(const GW_HourRec_t* rec)
{
    uint8_t count = 0u;
    if (rec == NULL) {
        return 0u;
    }
    for (uint32_t i = 0u; i < UI_MAX_NODES; i++) {
        const GW_NodeRec_t* n = &rec->nodes[i];
        if ((n->batt_lvl != UI_NODE_BATT_LVL_INVALID) ||
            (n->temp_c != UI_NODE_TEMP_INVALID_C) ||
            (n->x != 0xFFFFu) ||
            (n->y != 0xFFFFu) ||
            (n->z != 0xFFFFu) ||
            (n->adc != 0xFFFFu) ||
            (n->pulse_cnt != 0xFFFFFFFFu)) {
            count++;
        }
    }
    return count;
}

static void prv_update_beacon_recovery_mode_from_rec(const GW_HourRec_t* rec)
{
    s_beacon_recovery_mode = (prv_count_valid_nodes_in_rec(rec) == 0u);
}

static uint8_t prv_get_beacon_burst_count(void)
{
    return 1u;
}

static void prv_prepare_beacon_burst(uint32_t anchor_sec,
                                    uint8_t total_count,
                                    uint32_t gap_ms,
                                    GW_BeaconTxKind_t kind,
                                    uint32_t periodic_slot_id)
{
    if (total_count == 0u) {
        total_count = 1u;
    }
    if (gap_ms == 0u) {
        gap_ms = GW_BEACON_BURST_GAP_MS;
    }
    s_beacon_burst_anchor_sec = anchor_sec;
    s_beacon_burst_remaining = total_count;
    s_beacon_burst_gap_ms = gap_ms;
    s_beacon_tx_kind = kind;
    s_active_periodic_beacon_slot_id = (kind == GW_BEACON_TX_KIND_SCHEDULED) ? periodic_slot_id : 0xFFFFFFFFu;
    s_beacon_oneshot_pending = true;
}

static void prv_led1(bool on)
{
#if UI_HAVE_LED1
    HAL_GPIO_WritePin(LED1_GPIO_Port, LED1_Pin, on ? GPIO_PIN_SET : GPIO_PIN_RESET);
#else
    (void)on;
#endif
}

static void prv_led1_sync_blink_stop(void)
{
    s_led1_sync_blink_active = false;
    s_led1_sync_blink_on = false;
    (void)UTIL_TIMER_Stop(&s_tmr_led1_pulse);
    prv_led1(false);
}

static void prv_led1_sync_blink_start(void)
{
    s_led1_sync_blink_active = true;
    s_led1_sync_blink_on = true;
    prv_led1(true);
    (void)UTIL_TIMER_Stop(&s_tmr_led1_pulse);
    (void)UTIL_TIMER_SetPeriod(&s_tmr_led1_pulse, GW_SYNC_LED1_ON_MS);
    (void)UTIL_TIMER_Start(&s_tmr_led1_pulse);
}

static void prv_led1_pulse_off_cb(void *context)
{
    (void)context;

    if (s_led1_sync_blink_active) {
        uint32_t next_ms;

        s_led1_sync_blink_on = !s_led1_sync_blink_on;
        prv_led1(s_led1_sync_blink_on);
        next_ms = s_led1_sync_blink_on ? GW_SYNC_LED1_ON_MS : GW_SYNC_LED1_OFF_MS;
        (void)UTIL_TIMER_Stop(&s_tmr_led1_pulse);
        (void)UTIL_TIMER_SetPeriod(&s_tmr_led1_pulse, next_ms);
        (void)UTIL_TIMER_Start(&s_tmr_led1_pulse);
        return;
    }

    prv_led1(false);
}

static void prv_led1_blocking_pulse_ms(uint32_t pulse_ms)
{
#if UI_HAVE_LED1
    bool restore_sync = s_led1_sync_blink_active;
    bool restore_sync_on = s_led1_sync_blink_on;
    uint32_t restore_ms = restore_sync
                        ? (restore_sync_on ? GW_SYNC_LED1_ON_MS : GW_SYNC_LED1_OFF_MS)
                        : 0u;

    if (pulse_ms == 0u) {
        pulse_ms = 1u;
    }

    if (restore_sync) {
        (void)UTIL_TIMER_Stop(&s_tmr_led1_pulse);
    }

    prv_led1(true);
    HAL_Delay(pulse_ms);

    if (restore_sync) {
        prv_led1(restore_sync_on);
        (void)UTIL_TIMER_Stop(&s_tmr_led1_pulse);
        (void)UTIL_TIMER_SetPeriod(&s_tmr_led1_pulse, restore_ms);
        (void)UTIL_TIMER_Start(&s_tmr_led1_pulse);
    } else {
        prv_led1(false);
    }
#else
    (void)pulse_ms;
#endif
}

static bool prv_radio_ready_for_tx(void)
{
    if ((Radio.SetChannel == NULL) || (Radio.Send == NULL) ||
        (Radio.Sleep == NULL) || (Radio.SetTxConfig == NULL)) {
        return false;
    }
    return true;
}

static bool prv_radio_ready_for_rx(void)
{
    if ((Radio.SetChannel == NULL) || (Radio.Rx == NULL) ||
        (Radio.Sleep == NULL) || (Radio.SetRxConfig == NULL)) {
        return false;
    }
    return true;
}

static uint8_t prv_pack_gw_volt_x10(uint16_t raw_x10)
{
    if (raw_x10 == 0xFFFFu) {
        return 0xFFu;
    }
    if (raw_x10 > 254u) {
        return 254u;
    }
    return (uint8_t)raw_x10;
}

static int8_t prv_pack_gw_temp_c(int16_t raw_x10)
{
    int16_t c;
    if ((uint16_t)raw_x10 == 0xFFFFu) {
        return UI_NODE_TEMP_INVALID_C;
    }
    if (raw_x10 >= 0) {
        c = (int16_t)((raw_x10 + 5) / 10);
    } else {
        c = (int16_t)((raw_x10 - 5) / 10);
    }
    if (c < -50) {
        c = -50;
    }
    if (c > 100) {
        c = 100;
    }
    return (int8_t)c;
}

static void prv_hour_rec_init(uint32_t epoch_sec)
{
    uint16_t gw_volt_x10_raw = 0xFFFFu;
    int16_t gw_temp_x10_raw = (int16_t)0xFFFFu;

    s_hour_rec.gw_volt_x10 = 0xFFu;
    s_hour_rec.gw_temp_c = UI_NODE_TEMP_INVALID_C;
    s_hour_rec.epoch_sec = epoch_sec;

    (void)GW_Sensors_MeasureGw(&gw_volt_x10_raw, &gw_temp_x10_raw);
    s_hour_rec.gw_volt_x10 = prv_pack_gw_volt_x10(gw_volt_x10_raw);
    s_hour_rec.gw_temp_c = prv_pack_gw_temp_c(gw_temp_x10_raw);
    for (uint32_t i = 0; i < UI_MAX_NODES; i++) {
        prv_invalidate_node_rec(&s_hour_rec.nodes[i]);
    }
}

static uint32_t prv_gw_offset_sec(void)
{
    const UI_Config_t* cfg = UI_GetConfig();
    uint8_t gw = cfg->gw_num;
    if (gw > 2u) gw = 2u;
    return (uint32_t)gw * 2u;
}

static uint32_t prv_get_beacon_offset_sec(void)
{
    /* beacon은 1분/2분/5분+ 모두 GW 번호 phase(0/2/4초)를 유지한다. */
    return prv_gw_offset_sec();
}

static uint64_t prv_next_event_centi(uint64_t now_centi, uint32_t interval_sec, uint32_t offset_sec)
{
    uint32_t now_sec = (uint32_t)(now_centi / 100u);
    uint32_t centi = (uint32_t)(now_centi % 100u);
    uint32_t cand_sec = now_sec + ((centi == 0u) ? 0u : 1u);
    uint32_t rem = (interval_sec == 0u) ? 0u : (cand_sec % interval_sec);
    uint32_t next_sec;
    if (interval_sec == 0u) {
        next_sec = cand_sec + offset_sec;
    } else if (rem <= offset_sec) {
        next_sec = cand_sec - rem + offset_sec;
    } else {
        next_sec = cand_sec - rem + interval_sec + offset_sec;
    }
    return (uint64_t)next_sec * 100u;
}

static bool prv_is_event_due_now(uint64_t now_centi,
                                 uint32_t interval_sec,
                                 uint32_t offset_sec,
                                 uint32_t late_grace_centi,
                                 uint64_t* due_event_centi)
{
    uint64_t next_evt;
    uint64_t step;
    uint64_t prev_evt;

    if ((interval_sec == 0u) || (due_event_centi == NULL)) {
        return false;
    }
    next_evt = prv_next_event_centi(now_centi, interval_sec, offset_sec);
    step = (uint64_t)interval_sec * 100u;
    if (next_evt == now_centi) {
        *due_event_centi = next_evt;
        return true;
    }
    if (next_evt < step) {
        return false;
    }
    prev_evt = next_evt - step;
    if ((now_centi >= prev_evt) &&
        (now_centi < (prev_evt + (uint64_t)late_grace_centi))) {
        *due_event_centi = prev_evt;
        return true;
    }
    return false;
}

static void prv_get_effective_setting_ascii(uint8_t out_setting_ascii[3])
{
    const UI_Config_t* cfg = UI_GetConfig();

    if (out_setting_ascii == NULL) {
        return;
    }

    if (s_ble_test_session_active) {
        out_setting_ascii[0] = (uint8_t)'0';
        out_setting_ascii[1] = (uint8_t)'1';
        out_setting_ascii[2] = (uint8_t)'M';
        return;
    }

    if (cfg == NULL) {
        out_setting_ascii[0] = (uint8_t)'0';
        out_setting_ascii[1] = (uint8_t)'0';
        out_setting_ascii[2] = (uint8_t)'H';
        return;
    }

    out_setting_ascii[0] = cfg->setting_ascii[0];
    out_setting_ascii[1] = cfg->setting_ascii[1];
    out_setting_ascii[2] = cfg->setting_ascii[2];
}

static bool prv_get_setting_value_unit_ascii_first(uint8_t* out_value, char* out_unit)
{
    const UI_Config_t* cfg = UI_GetConfig();
    uint8_t value;
    char unit;

    if ((out_value == NULL) || (out_unit == NULL)) {
        return false;
    }
    if (s_ble_test_session_active) {
        *out_value = 1u;
        *out_unit = 'M';
        return true;
    }
    if (cfg == NULL) {
        return false;
    }
    if ((cfg->setting_ascii[0] >= (uint8_t)'0') &&
        (cfg->setting_ascii[0] <= (uint8_t)'9') &&
        (cfg->setting_ascii[1] >= (uint8_t)'0') &&
        (cfg->setting_ascii[1] <= (uint8_t)'9')) {
        unit = (char)cfg->setting_ascii[2];
        if ((unit == 'M') || (unit == 'H')) {
            value = (uint8_t)(((cfg->setting_ascii[0] - (uint8_t)'0') * 10u) +
                              (cfg->setting_ascii[1] - (uint8_t)'0'));
            if (value > 0u) {
                *out_value = value;
                *out_unit = unit;
                return true;
            }
        }
    }
    value = cfg->setting_value;
    unit = cfg->setting_unit;
    if ((value > 0u) && ((unit == 'M') || (unit == 'H'))) {
        *out_value = value;
        *out_unit = unit;
        return true;
    }
    return false;
}

static void prv_update_test_mode(void)
{
    uint8_t value;
    char unit;
    s_test_mode = (prv_get_setting_value_unit_ascii_first(&value, &unit) &&
                   (value == 1u) && (unit == 'M'));
}

static bool prv_is_two_minute_mode_active(void)
{
    uint8_t value;
    char unit;
    return (prv_get_setting_value_unit_ascii_first(&value, &unit) &&
            (value == 2u) && (unit == 'M'));
}

static uint32_t prv_get_setting_cycle_sec(void)
{
    uint8_t value;
    char unit;
    if (!prv_get_setting_value_unit_ascii_first(&value, &unit)) {
        return 0u;
    }
    if (unit == 'M') {
        return (uint32_t)value * 60u;
    }
    return (uint32_t)value * 3600u;
}

static uint32_t prv_get_normal_cycle_sec(void)
{
    uint32_t cycle_sec = prv_get_setting_cycle_sec();
    if (cycle_sec == 0u) {
        return UI_GW_RX_PERIOD_S_NORMAL;
    }
    if (prv_is_two_minute_mode_active()) {
        return 120u;
    }
    if (cycle_sec < UI_BEACON_PERIOD_S) {
        return UI_BEACON_PERIOD_S;
    }
    return cycle_sec;
}

static uint32_t prv_get_hop_period_sec(void)
{
    return s_test_mode ? 60u : prv_get_normal_cycle_sec();
}

static uint32_t prv_get_beacon_interval_sec(void)
{
    if (s_test_mode) {
        return 60u;
    }
    if (prv_is_two_minute_mode_active()) {
        return 120u;
    }
    return UI_BEACON_PERIOD_S;
}

static uint32_t prv_get_rx_interval_sec(void)
{
    return s_test_mode ? 60u : prv_get_normal_cycle_sec();
}

static uint32_t prv_get_rx_start_offset_sec(void)
{
    if (s_test_mode) {
        return 20u;
    }
    if (prv_is_two_minute_mode_active()) {
        return 20u;
    }
    /* 5분 이상 주기에서는 각 cycle의 +01분00초에 ND RX를 시작한다.
     * RX arm은 nominal slot보다 1000ms 먼저 올리고,
     * 최대 수신 대기 종료는 50노드 기준 cycle +02분42초를 넘지 않게 맞춘다.
     * 1분/2분 모드는 각각 +20초에 맞춘다. */
    return UI_GW_RX_START_OFFSET_S;
}

static bool prv_get_reminder_offset_sec(uint32_t* out_offset_sec)
{
    (void)out_offset_sec;
    return false;
}

static bool prv_get_two_minute_prep_offset_sec(uint32_t* out_offset_sec)
{
    (void)out_offset_sec;
    return false;
}

static uint32_t prv_get_current_cycle_timestamp_sec(void)
{
    uint32_t now_sec = UI_Time_NowSec2016();
    uint32_t cycle_sec = prv_get_setting_cycle_sec();
    if (cycle_sec != 0u) {
        return ((now_sec / cycle_sec) * cycle_sec);
    }
    return now_sec;
}

static bool prv_is_minute_test_active(void)
{
    uint8_t value;
    char unit;
    return (s_test_mode &&
            prv_get_setting_value_unit_ascii_first(&value, &unit) &&
            (value == 1u) && (unit == 'M'));
}

static uint64_t prv_next_test50_centi(uint64_t now_centi)
{
    return prv_next_event_centi(now_centi, 60u, 40u);
}

static uint64_t prv_get_rx_prestart_centi(void)
{
    return ((uint64_t)GW_RX_PRESTART_MS + 9u) / 10u;
}

static void prv_mark_cycle_complete(const GW_HourRec_t* rec)
{
    if (rec == NULL) {
        return;
    }
    s_last_cycle_rec = *rec;
    s_last_cycle_valid = true;
    s_last_cycle_minute_id = rec->epoch_sec / 60u;
}

static bool prv_should_try_catm1_uplink_now(uint32_t now_sec)
{
    if (!s_tcp_enabled) {
        return false;
    }
    if (!s_catm1_uplink_pending) {
        return false;
    }
    if (!s_catm1_immediate_try_pending) {
        return false;
    }
    if (s_state != GW_STATE_IDLE) {
        return false;
    }
    if (prv_is_minute_test_active() && ((now_sec % 60u) < 40u)) {
        return false;
    }
    return true;
}

static bool prv_flash_head_matches_last_live_uplink(void)
{
    GW_FileRec_t first_rec;
    const UI_Config_t* cfg = UI_GetConfig();
    uint8_t cur_gw_num = (cfg != NULL) ? cfg->gw_num : 0u;

    if (s_last_live_uplink_epoch_sec == 0xFFFFFFFFu) {
        return false;
    }
    if (prv_flash_tx_pending_count() == 0u) {
        return false;
    }
    if (!GW_Storage_ReadRecordByGlobalIndex(s_flash_tx_next_send_index, &first_rec, NULL)) {
        return false;
    }

    return ((first_rec.gw_num == cur_gw_num) &&
            (first_rec.rec.epoch_sec == s_last_live_uplink_epoch_sec));
}

static void prv_flash_tx_skip_last_live_uplink_head(void)
{
    while (prv_flash_head_matches_last_live_uplink()) {
        prv_flash_tx_note_sent(1u);
    }
}

static bool prv_live_uplink_already_sent(const GW_HourRec_t* rec)
{
    if ((rec == NULL) || (s_last_live_uplink_epoch_sec == 0xFFFFFFFFu)) {
        return false;
    }
    return (rec->epoch_sec == s_last_live_uplink_epoch_sec);
}

static void prv_note_live_uplink_sent(const GW_HourRec_t* rec)
{
    if (rec == NULL) {
        return;
    }
    s_last_live_uplink_epoch_sec = rec->epoch_sec;
}

static bool prv_backlog_batch_included_live_record(uint32_t first_index, uint32_t sent_count, const GW_HourRec_t* rec)
{
    GW_FileRec_t last_sent_rec;
    const UI_Config_t* cfg = UI_GetConfig();
    uint8_t cur_gw_num = (cfg != NULL) ? cfg->gw_num : 0u;

    if ((rec == NULL) || (sent_count == 0u)) {
        return false;
    }
    if (!GW_Storage_ReadRecordByGlobalIndex(first_index + sent_count - 1u, &last_sent_rec, NULL)) {
        return false;
    }

    return ((last_sent_rec.gw_num == cur_gw_num) &&
            (last_sent_rec.rec.epoch_sec == rec->epoch_sec));
}

static bool prv_should_prioritize_live_snapshot_over_backlog(const GW_HourRec_t* rec, uint32_t pending)
{
    (void)rec;
    (void)pending;
    /* backlog가 하나라도 있으면 oldest..current 범위를 한 번에 보내야 한다.
     * 따라서 live snapshot 단독 우선 전송은 사용하지 않는다. */
    return false;
}

static uint32_t prv_flash_tx_clamp_floor_to_boot_tail(uint32_t floor_index)
{
    if (floor_index < s_flash_tx_boot_tail_index) {
        floor_index = s_flash_tx_boot_tail_index;
    }
    return floor_index;
}

static void prv_flash_tx_reset_to_tail(void)
{
    uint32_t tail = GW_Storage_GetTotalRecordCount();
    s_flash_tx_boot_tail_index = tail;
    s_flash_tx_next_send_index = tail;
}

static uint32_t prv_flash_tx_pending_count(void)
{
    uint32_t tail = GW_Storage_GetTotalRecordCount();

    if (!s_tcp_enabled) {
        return 0u;
    }
    if (s_flash_tx_boot_tail_index > tail) {
        s_flash_tx_boot_tail_index = tail;
    }
    if (s_flash_tx_next_send_index < s_flash_tx_boot_tail_index) {
        s_flash_tx_next_send_index = s_flash_tx_boot_tail_index;
    }
    if (s_flash_tx_next_send_index > tail) {
        s_flash_tx_next_send_index = tail;
        return 0u;
    }
    return (tail - s_flash_tx_next_send_index);
}

static void prv_flash_tx_note_saved(bool saved_ok)
{
    uint32_t tail;
    uint32_t pending;
    uint32_t min_keep_index;
    if (!saved_ok) {
        return;
    }
    tail = GW_Storage_GetTotalRecordCount();
    if (s_flash_tx_boot_tail_index > tail) {
        s_flash_tx_boot_tail_index = tail;
    }
    if (s_flash_tx_next_send_index < s_flash_tx_boot_tail_index) {
        s_flash_tx_next_send_index = s_flash_tx_boot_tail_index;
    }
    if (s_flash_tx_next_send_index > tail) {
        s_flash_tx_next_send_index = tail;
    }
    pending = tail - s_flash_tx_next_send_index;
    if (pending > GW_FLASH_TX_BACKLOG_MAX) {
        min_keep_index = tail - GW_FLASH_TX_BACKLOG_MAX;
        min_keep_index = prv_flash_tx_clamp_floor_to_boot_tail(min_keep_index);
        if (s_flash_tx_next_send_index < min_keep_index) {
            s_flash_tx_next_send_index = min_keep_index;
        }
    }
}

static void prv_flash_tx_note_sent(uint32_t sent_count)
{
    uint32_t pending = prv_flash_tx_pending_count();
    uint32_t tail = GW_Storage_GetTotalRecordCount();
    if (sent_count > pending) {
        sent_count = pending;
    }
    s_flash_tx_next_send_index += sent_count;
    if (s_flash_tx_next_send_index < s_flash_tx_boot_tail_index) {
        s_flash_tx_next_send_index = s_flash_tx_boot_tail_index;
    }
    if (s_flash_tx_next_send_index > tail) {
        s_flash_tx_next_send_index = tail;
    }
}

static void prv_flash_tx_resync_after_storage_change(void)
{
    uint32_t tail = GW_Storage_GetTotalRecordCount();
    uint32_t min_keep_index;
    if (s_flash_tx_boot_tail_index > tail) {
        s_flash_tx_boot_tail_index = tail;
    }
    if (s_flash_tx_next_send_index < s_flash_tx_boot_tail_index) {
        s_flash_tx_next_send_index = s_flash_tx_boot_tail_index;
    }
    if (s_flash_tx_next_send_index > tail) {
        s_flash_tx_next_send_index = tail;
    }
    if ((tail - s_flash_tx_next_send_index) > GW_FLASH_TX_BACKLOG_MAX) {
        min_keep_index = tail - GW_FLASH_TX_BACKLOG_MAX;
        min_keep_index = prv_flash_tx_clamp_floor_to_boot_tail(min_keep_index);
        if (s_flash_tx_next_send_index < min_keep_index) {
            s_flash_tx_next_send_index = min_keep_index;
        }
    }
}

bool GW_App_IsTcpEnabled(void)
{
    return s_tcp_enabled;
}

static void prv_abort_pending_tcp_uplink_state(void)
{
    s_catm1_uplink_pending = false;
    s_catm1_immediate_try_pending = false;
    s_catm1_retry_not_before_ms = 0u;
    s_last_catm1_slot_id = 0xFFFFFFFFu;
    s_last_minute_test_uplink_minute_id = 0xFFFFFFFFu;
    /* 마지막 성공 uplink stamp는 유지해서 OFF->ON 전환 뒤에도
     * flash head 중복 record를 건너뛸 수 있게 둔다. */
    s_last_2m_prep_slot_id = 0xFFFFFFFFu;
    prv_clear_catm1_tx_ctx();
}

static void prv_clear_catm1_tx_ctx(void)
{
    memset(&s_catm1_tx_ctx, 0, sizeof(s_catm1_tx_ctx));
    s_catm1_tx_ctx.kind = GW_CATM1_TX_CTX_NONE;
}

static void prv_handle_catm1_job_result(void)
{
    GW_Catm1JobType_t job_type = GW_CATM1_JOB_NONE;
    bool ok = false;
    uint32_t sent_count = 0u;
    uint32_t pending;

    if (!GW_Catm1_TakeJobResult(&job_type, &ok, &sent_count)) {
        return;
    }

    if (job_type == GW_CATM1_JOB_TIME_SYNC) {
        s_boot_time_sync_pending = false;
        if (!ok) {
            prv_enter_dormant_stop_mode();
            return;
        }

        prv_hour_rec_init(prv_get_current_cycle_timestamp_sec());
        prv_enter_scheduled_stop_once();
        return;
    }

    if (job_type == GW_CATM1_JOB_SNAPSHOT) {
        if (ok && s_catm1_tx_ctx.rec_valid) {
            prv_note_live_uplink_sent(&s_catm1_tx_ctx.rec);
            prv_flash_tx_skip_last_live_uplink_head();
        }

        prv_flash_tx_resync_after_storage_change();
        pending = prv_flash_tx_pending_count();
        s_catm1_uplink_pending = (pending > 0u);
        if (!s_catm1_uplink_pending) {
            s_catm1_immediate_try_pending = false;
        }
        s_catm1_retry_not_before_ms = 0u;
        prv_clear_catm1_tx_ctx();
        prv_schedule_wakeup();
        return;
    }

    if (job_type == GW_CATM1_JOB_STORED_RANGE) {
        if (sent_count > 0u) {
            bool included_live = false;

            if (s_catm1_tx_ctx.rec_valid) {
                included_live = prv_backlog_batch_included_live_record(s_catm1_tx_ctx.first_index,
                                                                       sent_count,
                                                                       &s_catm1_tx_ctx.rec);
            }
            prv_flash_tx_note_sent(sent_count);
            if (included_live) {
                prv_note_live_uplink_sent(&s_catm1_tx_ctx.rec);
            }
        }

        prv_flash_tx_resync_after_storage_change();
        pending = prv_flash_tx_pending_count();
        s_catm1_uplink_pending = (pending > 0u);
        if (!s_catm1_uplink_pending) {
            s_catm1_immediate_try_pending = false;
        }
        s_catm1_retry_not_before_ms = 0u;
        prv_clear_catm1_tx_ctx();
        prv_schedule_wakeup();
        return;
    }

    prv_clear_catm1_tx_ctx();
}

static void prv_reset_tcp_live_record_state(void)
{
    uint32_t epoch_sec;

    s_last_cycle_valid = false;
    s_last_cycle_nodes_seen_mask_valid = false;
    s_rx_nodes_seen_mask = 0u;
    epoch_sec = UI_Time_IsValid() ? UI_Time_NowSec2016()
                                  : prv_get_current_cycle_timestamp_sec();
    s_rx_cycle_stamp_sec = epoch_sec;
    prv_hour_rec_init(epoch_sec);
}

static void prv_apply_tcp_mode_change(bool enabled)
{
    bool state_changed = (s_tcp_enabled != enabled);

    if (!state_changed) {
        if (enabled) {
            prv_request_tcp_uplink_after_enable(true);
            prv_request_ble_stop_for_immediate_tcp_uplink();
        }
        if (s_inited) {
            prv_exit_dormant_stop_mode();
            if (enabled) {
                prv_request_schedule_recheck_now();
            } else {
                prv_schedule_wakeup();
            }
        }
        return;
    }

    s_tcp_enabled = enabled;
    prv_abort_pending_tcp_uplink_state();

    if (enabled) {
        if (!prv_have_any_tcp_uplink_candidate()) {
            prv_reset_tcp_live_record_state();
        }
        prv_request_tcp_uplink_after_enable(true);
        prv_request_ble_stop_for_immediate_tcp_uplink();
    }

    if (!s_inited) {
        return;
    }

    prv_exit_dormant_stop_mode();
    if (enabled) {
        prv_request_schedule_recheck_now();
    } else {
        prv_schedule_wakeup();
    }
}

static bool prv_save_hour_rec_verified(const GW_HourRec_t* rec)
{
    uint32_t before_cnt;

    if (!s_tcp_enabled) {
        return false;
    }
    uint32_t after_cnt;
    uint32_t try_idx;

    if (rec == NULL) {
        return false;
    }
    before_cnt = GW_Storage_GetTotalRecordCount();
    for (try_idx = 0u; try_idx < 2u; try_idx++) {
        if (!GW_Storage_SaveHourRec(rec)) {
            continue;
        }
        after_cnt = GW_Storage_GetTotalRecordCount();
        if (after_cnt > before_cnt) {
            return true;
        }
    }
    return false;
}

static bool prv_flash_tail_matches_rec(const GW_HourRec_t* rec)
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

static void prv_handle_test50_actions_core(uint32_t now_sec, bool treat_as_minute_test)
{
    uint32_t now_minute_id = now_sec / 60u;

    if (!s_tcp_enabled) {
        return;
    }
    if (!treat_as_minute_test) {
        return;
    }
    if ((!s_last_cycle_valid) || (s_last_cycle_minute_id != now_minute_id)) {
        return;
    }
    if (s_last_save_minute_id != now_minute_id) {
        bool saved_ok = prv_save_hour_rec_verified(&s_last_cycle_rec);
        prv_flash_tx_note_saved(saved_ok);
        s_last_save_minute_id = now_minute_id;
        GW_Storage_PurgeOldFiles(s_last_cycle_rec.epoch_sec);
        prv_flash_tx_resync_after_storage_change();
    }
}

static void prv_handle_test50_actions(uint32_t now_sec)
{
    prv_handle_test50_actions_core(now_sec, prv_is_minute_test_active());
}

static void prv_send_ble_test_report_if_active(const GW_HourRec_t* rec)
{
    if (rec == NULL) {
        return;
    }
    if (!UI_BLE_IsActive() || !UI_BLE_IsPersistent()) {
        return;
    }

    UI_BLE_EnsureSerialReady();
    (void)GW_BleReport_SendMinuteTestRecord(rec);
}

static void prv_start_ble_test_session(void)
{
    s_ble_test_session_active = true;
    UI_BLE_SetPersistent(true);
    UI_BLE_EnableForMs(UI_BLE_ACTIVE_MS);

    (void)UTIL_TIMER_Stop(&s_tmr_ble_test_expire);
    (void)UTIL_TIMER_SetPeriod(&s_tmr_ble_test_expire, GW_BLE_TEST_SESSION_MS);
    (void)UTIL_TIMER_Start(&s_tmr_ble_test_expire);

    prv_update_test_mode();
    if (s_state == GW_STATE_IDLE) {
        prv_schedule_wakeup();
    }
}

static void prv_stop_ble_test_session(bool disable_ble_now)
{
    s_ble_test_session_active = false;
    (void)UTIL_TIMER_Stop(&s_tmr_ble_test_expire);
    UI_BLE_SetPersistent(false);

    if (disable_ble_now && UI_BLE_IsActive()) {
        UI_BLE_Disable();
    }

    prv_update_test_mode();
    if (s_state == GW_STATE_IDLE) {
        prv_schedule_wakeup();
    }
}

static bool prv_is_catm1_periodic_active(void)
{
    uint32_t cycle_sec;
    if (!s_tcp_enabled) {
        return false;
    }
    if (!UI_Time_IsValid()) {
        return false;
    }
    if (s_test_mode) {
        return false;
    }
    if (prv_is_two_minute_mode_active()) {
        return true;
    }
    cycle_sec = prv_get_setting_cycle_sec();
    return (cycle_sec >= UI_BEACON_PERIOD_S);
}

static uint32_t prv_get_catm1_period_sec(void)
{
    if (!prv_is_catm1_periodic_active()) {
        return 0u;
    }
    return prv_get_setting_cycle_sec();
}

static uint32_t prv_get_catm1_offset_sec(void)
{
    /* 2분 모드는 +01분20초, 5분 이상 주기는 항상 cycle anchor +03분00초에 TCP uplink를 시도한다. */
    return prv_is_two_minute_mode_active() ? 80u : UI_CATM1_PERIODIC_OFFSET_S;
}

static uint32_t prv_catm1_slot_id_from_epoch_sec(uint32_t epoch_sec, uint32_t period_sec)
{
    if (period_sec == 0u) {
        return 0xFFFFFFFFu;
    }
    return (epoch_sec / period_sec);
}

static uint32_t prv_beacon_slot_id_from_epoch_sec(uint32_t epoch_sec, uint32_t period_sec, uint32_t offset_sec)
{
    if (period_sec == 0u) {
        return 0xFFFFFFFFu;
    }
    if (epoch_sec < offset_sec) {
        return 0u;
    }
    return ((epoch_sec - offset_sec) / period_sec);
}

static bool prv_last_cycle_matches_slot(uint32_t period_sec, uint32_t slot_id)
{
    if ((!s_last_cycle_valid) || (period_sec == 0u)) {
        return false;
    }
    return (prv_catm1_slot_id_from_epoch_sec(s_last_cycle_rec.epoch_sec, period_sec) == slot_id);
}

static void prv_request_catm1_uplink(void)
{
    if (!s_tcp_enabled) {
        return;
    }
    s_catm1_uplink_pending = true;
}

static void prv_request_catm1_uplink_immediate(void)
{
    if (!s_tcp_enabled) {
        return;
    }
    s_catm1_uplink_pending = true;
    s_catm1_immediate_try_pending = true;
}

static void prv_request_minute_test_uplink_for_minute(uint32_t minute_id)
{
    if (s_last_minute_test_uplink_minute_id == minute_id) {
        return;
    }

    /* 1분 모드에서는 backlog가 남아 있어도 현재 minute record가 실제로 닫힌 뒤에만
     * uplink를 건다. 이렇게 해야 실패 시점 record부터 현재 record까지를 같이 보낸다. */
    if ((s_last_cycle_valid) && (s_last_cycle_minute_id == minute_id)) {
        s_last_minute_test_uplink_minute_id = minute_id;
        prv_request_catm1_uplink();
    }
}

static const GW_HourRec_t* prv_get_catm1_uplink_record(void)
{
    if (!s_tcp_enabled) {
        return NULL;
    }
    if (s_last_cycle_valid) {
        return &s_last_cycle_rec;
    }
    return &s_hour_rec;
}

static bool prv_run_catm1_uplink_now(void)
{
    const GW_HourRec_t* rec;
    uint32_t pending;

    if (!s_tcp_enabled) {
        prv_abort_pending_tcp_uplink_state();
        return false;
    }
    if (!s_catm1_uplink_pending) {
        return false;
    }
    if (s_state != GW_STATE_IDLE) {
        return false;
    }
    if (GW_Catm1_GetJobState() != GW_CATM1_JOB_STATE_IDLE) {
        return false;
    }
    if (GW_Catm1_IsBusy()) {
        return false;
    }
    if (prv_tcp_uplink_blocked_by_ble()) {
        prv_schedule_wakeup();
        return false;
    }

    s_catm1_retry_not_before_ms = 0u;
    s_catm1_immediate_try_pending = false;
    rec = prv_get_catm1_uplink_record();
    prv_flash_tx_skip_last_live_uplink_head();
    pending = prv_flash_tx_pending_count();

    if (prv_should_prioritize_live_snapshot_over_backlog(rec, pending)) {
        if (!GW_Catm1_RequestSnapshot(rec)) {
            return false;
        }

        prv_clear_catm1_tx_ctx();
        s_catm1_tx_ctx.active = true;
        s_catm1_tx_ctx.kind = GW_CATM1_TX_CTX_LIVE;
        s_catm1_tx_ctx.rec_valid = (rec != NULL);
        if (rec != NULL) {
            s_catm1_tx_ctx.rec = *rec;
        }
        prv_arm_busy_state_safety_wakeup(20u);
        return true;
    }

    if (pending > 0u) {
        GW_FileRec_t first_rec;
        uint32_t batch_count;

        if ((rec != NULL) && !prv_flash_tail_matches_rec(rec)) {
            bool saved_ok = prv_save_hour_rec_verified(rec);
            prv_flash_tx_note_saved(saved_ok);
        }

        pending = prv_flash_tx_pending_count();
        batch_count = pending;
        if (batch_count > GW_FLASH_TX_BACKLOG_MAX) {
            batch_count = GW_FLASH_TX_BACKLOG_MAX;
        }
        if ((batch_count == 0u) ||
            !GW_Storage_ReadRecordByGlobalIndex(s_flash_tx_next_send_index, &first_rec, NULL)) {
            prv_flash_tx_reset_to_tail();
            s_catm1_uplink_pending = false;
            s_catm1_immediate_try_pending = false;
            s_catm1_retry_not_before_ms = 0u;
            prv_schedule_wakeup();
            return true;
        }

        if (!GW_Catm1_RequestStoredRange(s_flash_tx_next_send_index, batch_count)) {
            return false;
        }

        prv_clear_catm1_tx_ctx();
        s_catm1_tx_ctx.active = true;
        s_catm1_tx_ctx.kind = GW_CATM1_TX_CTX_BACKLOG;
        s_catm1_tx_ctx.first_index = s_flash_tx_next_send_index;
        s_catm1_tx_ctx.batch_count = batch_count;
        s_catm1_tx_ctx.rec_valid = (rec != NULL);
        if (rec != NULL) {
            s_catm1_tx_ctx.rec = *rec;
        }
        prv_arm_busy_state_safety_wakeup(20u);
        return true;
    }

    if (rec != NULL) {
        if (prv_live_uplink_already_sent(rec)) {
            s_catm1_uplink_pending = false;
            s_catm1_immediate_try_pending = false;
            s_catm1_retry_not_before_ms = 0u;
            prv_schedule_wakeup();
            return true;
        }

        if (!GW_Catm1_RequestSnapshot(rec)) {
            return false;
        }

        prv_clear_catm1_tx_ctx();
        s_catm1_tx_ctx.active = true;
        s_catm1_tx_ctx.kind = GW_CATM1_TX_CTX_LIVE;
        s_catm1_tx_ctx.rec_valid = true;
        s_catm1_tx_ctx.rec = *rec;
        prv_arm_busy_state_safety_wakeup(20u);
        return true;
    }

    return false;
}

static void prv_arm_busy_state_safety_wakeup(uint32_t delay_ms)
{
    if (!s_inited) {
        return;
    }
    if (s_dormant_stop_mode) {
        return;
    }
    if (delay_ms == 0u) {
        delay_ms = 1u;
    }

    (void)UTIL_TIMER_Stop(&s_tmr_wakeup);
    (void)UTIL_TIMER_SetPeriod(&s_tmr_wakeup, delay_ms);
    (void)UTIL_TIMER_Start(&s_tmr_wakeup);
}

static void prv_requeue_events(uint32_t ev_mask)
{
    if (ev_mask != 0u) {
        prv_evt_set(ev_mask);
        UTIL_SEQ_SetTask(UI_TASK_BIT_GW_MAIN, 0);
    }
}

static uint32_t prv_get_rx_slot_timeout_ms(void)
{
    uint32_t now_ms;
    uint32_t deadline_ms;

    if ((s_rx_cycle_start_ms == 0u) || (s_rx_window_deadline_ms == 0u)) {
        return UI_SLOT_DURATION_MS;
    }

    now_ms = HAL_GetTick();
    deadline_ms = s_rx_window_deadline_ms;
    if ((int32_t)(deadline_ms - now_ms) <= 0) {
        return 1u;
    }
    return (deadline_ms - now_ms);
}

static bool prv_arm_rx_slot(void)
{
    uint32_t timeout_ms;

    if (!prv_radio_ready_for_rx()) {
        return false;
    }
    if (!UI_Radio_PrepareRx(UI_NODE_PAYLOAD_LEN)) {
        return false;
    }
    timeout_ms = prv_get_rx_slot_timeout_ms();
    Radio.SetChannel(s_data_freq_hz);
    Radio.Rx(timeout_ms);
    prv_arm_busy_state_safety_wakeup(timeout_ms + GW_RX_EVT_SAFETY_SLACK_MS);
    return true;
}

static bool prv_rearm_current_rx_slot(void)
{
    UI_Radio_EnterSleep();

    if (!prv_radio_ready_for_rx()) {
        s_rx_cycle_minute_test = false;
        s_rx_cycle_stamp_sec = 0u;
        s_rx_window_deadline_ms = 0u;
        s_rx_cycle_start_ms = 0u;
        s_rx_expected_nodes = 0u;
        s_rx_nodes_seen_mask = 0u;
        s_state = GW_STATE_IDLE;
        UI_LPM_UnlockStop();
        prv_schedule_wakeup();
        return false;
    }
    if (!prv_arm_rx_slot()) {
        s_rx_cycle_minute_test = false;
        s_rx_cycle_stamp_sec = 0u;
        s_rx_window_deadline_ms = 0u;
        s_rx_cycle_start_ms = 0u;
        s_rx_expected_nodes = 0u;
        s_rx_nodes_seen_mask = 0u;
        s_state = GW_STATE_IDLE;
        UI_LPM_UnlockStop();
        prv_schedule_wakeup();
        return false;
    }
    return true;
}

static void prv_schedule_after_ms(uint32_t delay_ms)
{
    if (s_dormant_stop_mode) {
        return;
    }
    if (delay_ms == 0u) {
        delay_ms = 1u;
    }
    (void)UTIL_TIMER_Stop(&s_tmr_wakeup);
    (void)UTIL_TIMER_SetPeriod(&s_tmr_wakeup, delay_ms);
    (void)UTIL_TIMER_Start(&s_tmr_wakeup);
}

static bool prv_rx_all_expected_nodes_seen(void)
{
    uint64_t full_mask;

    if ((s_rx_expected_nodes == 0u) || (s_rx_expected_nodes >= 64u)) {
        return false;
    }

    full_mask = (1ULL << s_rx_expected_nodes) - 1ULL;
    return ((s_rx_nodes_seen_mask & full_mask) == full_mask);
}

static void prv_rx_note_node_received(uint8_t node_num)
{
    if (node_num < 64u) {
        s_rx_nodes_seen_mask |= (1ULL << node_num);
    }
}

static bool prv_rx_window_expired(void)
{
    if (s_rx_window_deadline_ms == 0u) {
        return true;
    }
    return ((int32_t)(HAL_GetTick() - s_rx_window_deadline_ms) >= 0);
}

static void prv_schedule_next_second_tick(uint64_t now_centi)
{
    uint32_t centi = (uint32_t)(now_centi % 100u);
    uint32_t wait_centi = (centi == 0u) ? 1u : (100u - centi);
    prv_schedule_after_ms(wait_centi * 10u);
}

static void prv_reset_rx_cycle_state(void)
{
    s_data_freq_hz = 0u;
    s_rx_cycle_minute_test = false;
    s_rx_cycle_stamp_sec = 0u;
    s_rx_window_deadline_ms = 0u;
    s_rx_cycle_start_ms = 0u;
    s_rx_expected_nodes = 0u;
    s_rx_nodes_seen_mask = 0u;
}

static void prv_abort_active_radio_session(void)
{
    if (s_state == GW_STATE_IDLE) {
        return;
    }

    UI_Radio_EnterSleep();
    if ((s_state == GW_STATE_RX_SLOTS) ||
        (s_state == GW_STATE_SYNC_WAIT_RX) ||
        (s_state == GW_STATE_SYNC_WAIT_TX)) {
        prv_reset_rx_cycle_state();
    }
    s_state = GW_STATE_IDLE;
    UI_LPM_UnlockStop();
}

static uint32_t prv_get_sync_wait_remaining_ms(void)
{
    uint32_t now_ms;

    if (s_sync_wait_deadline_ms == 0u) {
        return 0u;
    }

    now_ms = HAL_GetTick();
    if ((int32_t)(s_sync_wait_deadline_ms - now_ms) <= 0) {
        return 0u;
    }

    return (s_sync_wait_deadline_ms - now_ms);
}

static bool prv_arm_sync_wait_rx(void)
{
    uint32_t timeout_ms = prv_get_sync_wait_remaining_ms();

    if (timeout_ms == 0u) {
        return false;
    }
    if (timeout_ms > GW_SYNC_WAIT_RX_CHUNK_MS) {
        timeout_ms = GW_SYNC_WAIT_RX_CHUNK_MS;
    }
    if (!prv_radio_ready_for_rx()) {
        return false;
    }
    if (!UI_Radio_PrepareRx(UI_NODE_PAYLOAD_LEN)) {
        return false;
    }

    prv_led1_sync_blink_start();
    UI_LPM_LockStop();
    s_state = GW_STATE_SYNC_WAIT_RX;
    Radio.SetChannel(UI_RF_GetBeaconFreqHz());
    Radio.Rx(timeout_ms);
    prv_arm_busy_state_safety_wakeup(timeout_ms + GW_RX_EVT_SAFETY_SLACK_MS);
    return true;
}

static bool prv_is_sync_request_payload(const uint8_t *payload, uint16_t size)
{
    const UI_Config_t* cfg = UI_GetConfig();
    UI_NodeData_t nd;

    if ((payload == NULL) || (cfg == NULL)) {
        return false;
    }

    if (UI_Pkt_ParseNodeData(payload, size, &nd)) {
        return ((nd.node_num == GW_SYNC_REQUEST_NODE_NUM) &&
                (memcmp(nd.net_id, cfg->net_id, UI_NET_ID_LEN) == 0));
    }

    if (size < (uint16_t)(1u + UI_NET_ID_LEN)) {
        return false;
    }

    return ((payload[0] == GW_SYNC_REQUEST_NODE_NUM) &&
            (memcmp(&payload[1], cfg->net_id, UI_NET_ID_LEN) == 0));
}

static bool prv_start_sync_response_beacon_tx(void)
{
    prv_led1_sync_blink_stop();
    UI_Radio_MarkRecoverNeeded();
    if (!prv_start_beacon_tx(UI_Time_NowSec2016())) {
        return false;
    }

    s_state = GW_STATE_SYNC_WAIT_TX;
    return true;
}

static void prv_cleanup_sync_wait_context(void)
{
    s_sync_wait_deadline_ms = 0u;
    (void)UTIL_TIMER_Stop(&s_tmr_wakeup);
    prv_cancel_pending_beacon_burst();
    prv_evt_clear(GW_EVT_WAKEUP | GW_EVT_BEACON_ONESHOT |
                  GW_EVT_RADIO_TX_DONE | GW_EVT_RADIO_TX_TIMEOUT |
                  GW_EVT_RADIO_RX_DONE | GW_EVT_RADIO_RX_TIMEOUT |
                  GW_EVT_RADIO_RX_ERROR | GW_EVT_BLE_TEST_EXPIRE);
    prv_led1_sync_blink_stop();

    if ((s_state == GW_STATE_SYNC_WAIT_RX) || (s_state == GW_STATE_SYNC_WAIT_TX)) {
        UI_Radio_EnterSleep();
        s_state = GW_STATE_IDLE;
        UI_LPM_UnlockStop();
    }

    prv_reset_rx_cycle_state();
}

static bool prv_restore_runtime_after_sync_wait_end(void)
{
    if (!s_inited) {
        return false;
    }

    /* sync wait 종료 뒤에는 TCP가 OFF 상태였더라도 자동으로 ON 복구하고,
     * 현재/직전 snapshot 또는 flash backlog가 있으면 곧바로 uplink를 재개한다. */
    prv_apply_tcp_mode_change(true);

    if (!(s_tcp_enabled && s_catm1_uplink_pending && s_catm1_immediate_try_pending)) {
        return false;
    }

    if (prv_tcp_uplink_blocked_by_ble()) {
        prv_schedule_after_ms(GW_CATM1_PENDING_POLL_MS);
        return false;
    }

    prv_request_schedule_recheck_now();
    return true;
}

static void prv_finish_sync_wait_and_stop(void)
{
    bool catm1_restart_now;

    prv_cleanup_sync_wait_context();
    UI_BLE_SetPersistent(false);
    if (UI_BLE_IsActive()) {
        UI_BLE_Disable();
    }
    catm1_restart_now = prv_restore_runtime_after_sync_wait_end();
    if (catm1_restart_now) {
        return;
    }
    UI_LPM_EnterStopNow();
}

static void prv_cancel_sync_wait_and_resume_ble(void)
{
    prv_cleanup_sync_wait_context();
    UI_BLE_SetPersistent(false);
    UI_BLE_EnableForMs(UI_BLE_ACTIVE_MS);
    UI_BLE_EnsureSerialReady();
    prv_request_schedule_recheck_now();
}

static void prv_continue_sync_wait_or_stop(void)
{
    if (prv_get_sync_wait_remaining_ms() == 0u) {
        prv_finish_sync_wait_and_stop();
        return;
    }

    if (!prv_arm_sync_wait_rx()) {
        prv_finish_sync_wait_and_stop();
    }
}

static bool prv_start_sync_wait_mode(uint16_t duration_hours)
{
    uint64_t duration_ms64;
    uint32_t duration_ms;

    if ((!s_inited) || (duration_hours == 0u) ||
        (duration_hours > GW_SYNC_WAIT_MAX_HOURS)) {
        return false;
    }

    duration_ms64 = (uint64_t)duration_hours * 60ull * 60ull * 1000ull;
    if ((duration_ms64 == 0ull) || (duration_ms64 > 0x7FFFFFFFull)) {
        return false;
    }

    duration_ms = (uint32_t)duration_ms64;

    prv_stop_ble_test_session(false);
    UI_BLE_SetPersistent(false);
    if (UI_BLE_IsActive()) {
        UI_BLE_Disable();
    }
    (void)UTIL_TIMER_Stop(&s_tmr_wakeup);
    prv_cancel_pending_beacon_burst();
    prv_evt_clear(GW_EVT_WAKEUP | GW_EVT_BEACON_ONESHOT |
                  GW_EVT_RADIO_TX_DONE | GW_EVT_RADIO_TX_TIMEOUT |
                  GW_EVT_RADIO_RX_DONE | GW_EVT_RADIO_RX_TIMEOUT |
                  GW_EVT_RADIO_RX_ERROR | GW_EVT_BLE_TEST_EXPIRE);
    prv_led1_sync_blink_stop();

    prv_abort_active_radio_session();
    s_sync_wait_deadline_ms = HAL_GetTick() + duration_ms;

    if (!prv_arm_sync_wait_rx()) {
        prv_cleanup_sync_wait_context();
        prv_request_schedule_recheck_now();
        return false;
    }

    return true;
}

static bool prv_start_beacon_tx(uint32_t now_sec)
{
    UI_DateTime_t dt;
    const UI_Config_t* cfg = UI_GetConfig();
    uint8_t setting_ascii[3];

    UI_Time_Epoch2016_ToCalendar(now_sec, &dt);
    prv_get_effective_setting_ascii(setting_ascii);
    s_beacon_tx_payload_len = UI_Pkt_BuildBeacon(s_beacon_tx_payload, cfg->net_id, &dt, setting_ascii);
    if ((s_beacon_tx_payload_len == 0u) || (s_beacon_tx_payload_len > UI_BEACON_PAYLOAD_LEN)) {
        s_state = GW_STATE_IDLE;
        UI_LPM_UnlockStop();
        return false;
    }
    if (!prv_radio_ready_for_tx()) {
        s_state = GW_STATE_IDLE;
        UI_LPM_UnlockStop();
        return false;
    }
    if (!UI_Radio_PrepareTx(s_beacon_tx_payload_len)) {
        s_state = GW_STATE_IDLE;
        UI_LPM_UnlockStop();
        return false;
    }
    UI_LPM_LockStop();
    s_state = GW_STATE_BEACON_TX;
    Radio.SetChannel(UI_RF_GetBeaconFreqHz());
    Radio.Send(s_beacon_tx_payload, s_beacon_tx_payload_len);
    prv_arm_busy_state_safety_wakeup(GW_TX_EVT_SAFETY_WAKE_MS);
    return true;
}

static bool prv_start_pending_beacon_burst(void)
{
    if ((!s_beacon_oneshot_pending) || (s_beacon_burst_remaining == 0u)) {
        return false;
    }
    if (!prv_start_beacon_tx(s_beacon_burst_anchor_sec)) {
        prv_cancel_pending_beacon_burst();
        return false;
    }
    s_beacon_burst_remaining--;
    return true;
}

static void prv_tmr_wakeup_cb(void *context)
{
    (void)context;
    prv_evt_set(GW_EVT_WAKEUP);
    UTIL_SEQ_SetTask(UI_TASK_BIT_GW_MAIN, 0);
}

static void prv_request_schedule_recheck_now(void)
{
    if (!s_inited) {
        return;
    }
    if (s_dormant_stop_mode) {
        return;
    }
    (void)UTIL_TIMER_Stop(&s_tmr_wakeup);
    prv_evt_set(GW_EVT_WAKEUP);
    UTIL_SEQ_SetTask(UI_TASK_BIT_GW_MAIN, 0);
}

void GW_App_Process(void)
{
    if (!s_inited) {
        return;
    }

    GW_Catm1_Process();
    prv_handle_catm1_job_result();
    if (GW_Catm1_GetJobState() == GW_CATM1_JOB_STATE_BUSY) {
        prv_arm_busy_state_safety_wakeup(20u);
    }

    if (GW_Catm1_ConsumePowerFaultStopRequest()) {
        prv_enter_dormant_stop_mode();
    }

    if (s_dormant_stop_mode) {
        prv_evt_clear_all();
        UI_LPM_EnterStopNow();
        return;
    }

    uint32_t ev = prv_evt_fetch_and_clear_all();

    if (s_sync_wait_deadline_ms != 0u) {
        if (ev == 0u) {
            return;
        }


        if ((ev & GW_EVT_BLE_TEST_EXPIRE) != 0u) {
            prv_stop_ble_test_session(false);
            ev &= ~GW_EVT_BLE_TEST_EXPIRE;
        }

        if ((ev & GW_EVT_RADIO_TX_DONE) != 0u) {
            if (s_state == GW_STATE_SYNC_WAIT_TX) {
                UI_Radio_EnterSleep();
                s_state = GW_STATE_IDLE;
                UI_LPM_UnlockStop();
                s_beacon_counter++;
                prv_led1_blocking_pulse_ms(GW_RADIO_LED_PULSE_MS);
                prv_continue_sync_wait_or_stop();
            }
            return;
        }

        if ((ev & GW_EVT_RADIO_TX_TIMEOUT) != 0u) {
            UI_Radio_MarkRecoverNeeded();
            UI_Radio_EnterSleep();
            if (s_state == GW_STATE_SYNC_WAIT_TX) {
                s_state = GW_STATE_IDLE;
                UI_LPM_UnlockStop();
            }
            prv_continue_sync_wait_or_stop();
            return;
        }

        if ((ev & GW_EVT_RADIO_RX_DONE) != 0u) {
            if (s_state == GW_STATE_SYNC_WAIT_RX) {
                bool is_sync_req = false;

                UI_Radio_EnterSleep();
                s_state = GW_STATE_IDLE;
                UI_LPM_UnlockStop();

                is_sync_req = prv_is_sync_request_payload(s_rx_shadow, s_rx_shadow_size);
                prv_led1_blocking_pulse_ms(GW_RADIO_LED_PULSE_MS);

                if (is_sync_req && prv_start_sync_response_beacon_tx()) {
                    return;
                }

                prv_continue_sync_wait_or_stop();
            }
            return;
        }

        if ((ev & GW_EVT_RADIO_RX_TIMEOUT) != 0u) {
            if (s_state == GW_STATE_SYNC_WAIT_RX) {
                UI_Radio_EnterSleep();
                s_state = GW_STATE_IDLE;
                UI_LPM_UnlockStop();
            }
            prv_continue_sync_wait_or_stop();
            return;
        }

        if ((ev & GW_EVT_RADIO_RX_ERROR) != 0u) {
            UI_Radio_MarkRecoverNeeded();
            if (s_state == GW_STATE_SYNC_WAIT_RX) {
                UI_Radio_EnterSleep();
                s_state = GW_STATE_IDLE;
                UI_LPM_UnlockStop();
            }
            prv_continue_sync_wait_or_stop();
            return;
        }

        return;
    }

    if (s_boot_time_sync_pending &&
        (s_state == GW_STATE_IDLE) &&
        (GW_Catm1_GetJobState() == GW_CATM1_JOB_STATE_IDLE) &&
        !GW_Catm1_IsBusy()) {
        if (!GW_Catm1_RequestTimeSync()) {
            prv_enter_dormant_stop_mode();
            return;
        }

        prv_arm_busy_state_safety_wakeup(20u);
        return;
    }

    if (s_boot_time_sync_pending &&
        (GW_Catm1_GetJobState() == GW_CATM1_JOB_STATE_BUSY)) {
        return;
    }

    if (prv_handle_boot_time_sync_beacon()) {
        return;
    }

    if (ev == 0u) {
        return;
    }
    prv_update_test_mode();

    if ((ev & GW_EVT_BLE_TEST_EXPIRE) != 0u) {
        prv_stop_ble_test_session(true);
        ev &= ~GW_EVT_BLE_TEST_EXPIRE;
        if (ev == 0u) {
            return;
        }
    }

    if ((ev & GW_EVT_RADIO_TX_DONE) != 0u) {
        if (s_state == GW_STATE_BEACON_TX) {
            GW_BeaconTxKind_t finished_kind = s_beacon_tx_kind;

            UI_Radio_EnterSleep();
            s_state = GW_STATE_IDLE;
            UI_LPM_UnlockStop();
            s_beacon_counter++;
            prv_led1_blocking_pulse_ms(GW_RADIO_LED_PULSE_MS);

            if ((finished_kind == GW_BEACON_TX_KIND_SCHEDULED) &&
                (s_active_periodic_beacon_slot_id != 0xFFFFFFFFu) &&
                (s_beacon_burst_remaining <= 1u)) {
                s_last_periodic_beacon_slot_id = s_active_periodic_beacon_slot_id;
            }

            if (s_beacon_burst_remaining > 0u) {
                s_beacon_oneshot_pending = true;
                prv_schedule_after_ms(s_beacon_burst_gap_ms);
            } else {
                prv_cancel_pending_beacon_burst();
                /* manual one-shot은 즉시 스케줄을 다시 평가해 정시 beacon/RX/TCP를 놓치지 않게 하고,
                 * 정시 beacon 자체는 여기서 즉시 재평가하지 않아 같은 slot에서 두 번 나가지 않게 한다. */
                if (finished_kind == GW_BEACON_TX_KIND_MANUAL) {
                    prv_request_schedule_recheck_now();
                } else {
                    prv_schedule_wakeup();
                }
            }
        }
        prv_requeue_events(ev & ~(GW_EVT_RADIO_TX_DONE));
        return;
    }

    if ((ev & GW_EVT_RADIO_TX_TIMEOUT) != 0u) {
        UI_Radio_MarkRecoverNeeded();
        UI_Radio_EnterSleep();
        if (s_state != GW_STATE_IDLE) {
            s_state = GW_STATE_IDLE;
            UI_LPM_UnlockStop();
        }
        prv_cancel_pending_beacon_burst();
        prv_request_schedule_recheck_now();
        prv_requeue_events(ev & ~(GW_EVT_RADIO_TX_TIMEOUT));
        return;
    }

    if ((ev & GW_EVT_RADIO_RX_DONE) != 0u) {
        if (s_state == GW_STATE_RX_SLOTS) {
            bool accepted_node = false;
            uint64_t seen_bit = 0u;
            UI_NodeData_t nd;

            if (UI_Pkt_ParseNodeData(s_rx_shadow, s_rx_shadow_size, &nd)) {
                if (nd.node_num < UI_MAX_NODES) {
                    const UI_Config_t* cfg = UI_GetConfig();
                    if ((cfg != NULL) && (memcmp(nd.net_id, cfg->net_id, UI_NET_ID_LEN) == 0)) {
                        GW_NodeRec_t* r = &s_hour_rec.nodes[nd.node_num];
                        uint8_t sensor_en_mask = (uint8_t)(nd.sensor_en_mask & UI_SENSOR_EN_ALL);

                        r->batt_lvl = nd.batt_lvl;
                        r->temp_c = nd.temp_c;
                        r->x = ((sensor_en_mask & UI_SENSOR_EN_ICM20948) != 0u) ? nd.x : 0xFFFFu;
                        r->y = ((sensor_en_mask & UI_SENSOR_EN_ICM20948) != 0u) ? nd.y : 0xFFFFu;
                        r->z = ((sensor_en_mask & UI_SENSOR_EN_ICM20948) != 0u) ? nd.z : 0xFFFFu;
                        r->adc = ((sensor_en_mask & UI_SENSOR_EN_ADC) != 0u) ? nd.adc : 0xFFFFu;
                        r->pulse_cnt = ((sensor_en_mask & UI_SENSOR_EN_PULSE) != 0u) ? nd.pulse_cnt : 0xFFFFFFFFu;
                        if (nd.node_num < 64u) {
                            seen_bit = (1ULL << nd.node_num);
                            if ((s_rx_nodes_seen_mask & seen_bit) == 0u) {
                                prv_rx_note_node_received(nd.node_num);
                            }
                        }
                        accepted_node = true;
                    }
                }
            }
            /* NOTE: 수신 즉시 조기 종료(early-close)를 하지 않는다.
             * seen_mask에는 실제로 받은 모든 node bit를 기록하고, cycle 종료 판정은
             * prv_rx_all_expected_nodes_seen()에서 lower expected range만 검사한다.
             * 따라서 max_nodes보다 큰 node_num을 받아도 조기 종료는 발생하지 않고,
             * 윈도우 내에서 계속 수신하다가 timeout 또는 expected 범위 complete에서
             * 사이클을 닫는다. */
            (void)prv_rearm_current_rx_slot();
            if (accepted_node) {
                prv_led1_blocking_pulse_ms(GW_RADIO_LED_PULSE_MS);
            }
        }
        prv_requeue_events(ev & ~(GW_EVT_RADIO_RX_DONE));
        return;
    }

    if ((ev & GW_EVT_RADIO_RX_TIMEOUT) != 0u) {
        if (s_state == GW_STATE_RX_SLOTS) {
            prv_rx_next_slot();
        } else {
            UI_Radio_MarkRecoverNeeded();
            UI_Radio_EnterSleep();
            if (s_state != GW_STATE_IDLE) {
                s_state = GW_STATE_IDLE;
                UI_LPM_UnlockStop();
                prv_schedule_wakeup();
            }
        }
        prv_requeue_events(ev & ~(GW_EVT_RADIO_RX_TIMEOUT));
        return;
    }

    if ((ev & GW_EVT_RADIO_RX_ERROR) != 0u) {
        if (s_state == GW_STATE_RX_SLOTS) {
            (void)prv_rearm_current_rx_slot();
        } else {
            UI_Radio_MarkRecoverNeeded();
            UI_Radio_EnterSleep();
            if (s_state != GW_STATE_IDLE) {
                s_state = GW_STATE_IDLE;
                UI_LPM_UnlockStop();
                prv_schedule_wakeup();
            }
        }
        prv_requeue_events(ev & ~(GW_EVT_RADIO_RX_ERROR));
        return;
    }

    if ((ev & GW_EVT_BEACON_ONESHOT) != 0u) {
        s_beacon_oneshot_pending = true;
    }
    if (s_beacon_oneshot_pending) {
        if (s_state == GW_STATE_IDLE) {
            if (prv_start_pending_beacon_burst()) {
                return;
            }
            prv_request_schedule_recheck_now();
            return;
        }
        return;
    }

    if ((ev & GW_EVT_WAKEUP) != 0u) {
        if (s_state != GW_STATE_IDLE) {
            prv_schedule_wakeup();
            return;
        }

        uint64_t now_centi = UI_Time_NowCenti2016();
        uint32_t now_sec = (uint32_t)(now_centi / 100u);
        uint32_t beacon_interval = prv_get_beacon_interval_sec();
        uint32_t beacon_off = prv_get_beacon_offset_sec();
        uint64_t next_beacon = prv_next_event_centi(now_centi, beacon_interval, beacon_off);
        uint32_t rx_interval = prv_get_rx_interval_sec();
        uint32_t rx_start = prv_get_rx_start_offset_sec();
        uint64_t next_rx = prv_next_event_centi(now_centi, rx_interval, rx_start);
        uint32_t reminder_off = 0u;
        bool have_reminder = prv_get_reminder_offset_sec(&reminder_off);
        uint32_t two_min_prep_off = 0u;
        bool have_two_min_prep = prv_get_two_minute_prep_offset_sec(&two_min_prep_off);
        uint64_t next_test50 = 0xFFFFFFFFFFFFFFFFull;
        uint64_t next_catm1 = 0xFFFFFFFFFFFFFFFFull;
        uint64_t due_beacon = 0u;
        uint64_t due_rx = 0u;
        uint64_t due_reminder = 0u;
        uint64_t due_prep = 0u;
        uint64_t due_test50 = 0u;
        uint64_t due_catm1 = 0u;
        uint64_t rx_prestart_centi = prv_get_rx_prestart_centi();
        uint32_t beacon_slot_id = 0xFFFFFFFFu;
        bool beacon_due_now = prv_is_event_due_now(now_centi, beacon_interval, beacon_off, 120u, &due_beacon);
        bool rx_due_now = prv_is_event_due_now(now_centi, rx_interval, rx_start, 120u, &due_rx);
        bool reminder_due_now = have_reminder && prv_is_event_due_now(now_centi, rx_interval, reminder_off, 120u, &due_reminder);
        bool prep_due_now = have_two_min_prep && prv_is_event_due_now(now_centi, rx_interval, two_min_prep_off, 120u, &due_prep);
        bool rx_prestart_now = false;
        bool test50_due_now = false;
        bool catm1_due_now = false;

        if (prv_is_minute_test_active()) {
            next_test50 = prv_next_test50_centi(now_centi);
            test50_due_now = prv_is_event_due_now(now_centi, 60u, 40u, 120u, &due_test50);
        }
        if (prv_is_catm1_periodic_active()) {
            uint32_t catm1_period = prv_get_catm1_period_sec();
            next_catm1 = prv_next_event_centi(now_centi, catm1_period, prv_get_catm1_offset_sec());
            catm1_due_now = prv_is_event_due_now(now_centi, catm1_period, prv_get_catm1_offset_sec(), 120u, &due_catm1);
        }

        if (beacon_due_now) {
            beacon_slot_id = prv_beacon_slot_id_from_epoch_sec((uint32_t)(due_beacon / 100u), beacon_interval, beacon_off);
            if (s_last_periodic_beacon_slot_id == beacon_slot_id) {
                beacon_due_now = false;
            }
        }

        if ((!rx_due_now) && (next_rx > now_centi) && ((next_rx - now_centi) <= rx_prestart_centi)) {
            /* 1분/2분 모드에서는 ND가 +20초 슬롯에서 바로 송신한다.
             * GW가 정각에야 깨어나면 STOP 복귀/Radio 준비 지연 때문에 첫 패킷을 놓칠 수 있으므로
             * RX를 약간 일찍 arm 해 두고 nominal slot 경계는 별도로 유지한다. */
            rx_prestart_now = true;
        }

        if (catm1_due_now) {
            uint32_t catm1_period = prv_get_catm1_period_sec();
            uint32_t slot_id = prv_catm1_slot_id_from_epoch_sec((uint32_t)(due_catm1 / 100u), catm1_period);
            if (s_last_catm1_slot_id != slot_id) {
                s_last_catm1_slot_id = slot_id;
                if (prv_is_two_minute_mode_active()) {
                    if (s_last_cycle_valid || (prv_flash_tx_pending_count() > 0u)) {
                        prv_request_catm1_uplink();
                    }
                } else if (prv_last_cycle_matches_slot(catm1_period, slot_id)) {
                    prv_request_catm1_uplink();
                }
            }
        }

        if (reminder_due_now && !beacon_due_now && !rx_due_now) {
            uint32_t reminder_sec = (uint32_t)(due_reminder / 100u);
            prv_prepare_beacon_burst(reminder_sec,
                                     GW_BEACON_REMINDER_BURST_COUNT,
                                     GW_BEACON_REMINDER_GAP_MS,
                                     GW_BEACON_TX_KIND_REMINDER,
                                     0xFFFFFFFFu);
            if (prv_start_pending_beacon_burst()) {
                return;
            }
            prv_schedule_wakeup();
            return;
        }

        if (prep_due_now) {
            uint32_t prep_slot_id = prv_catm1_slot_id_from_epoch_sec((uint32_t)(due_prep / 100u), rx_interval);
            if (s_last_2m_prep_slot_id != prep_slot_id) {
                s_last_2m_prep_slot_id = prep_slot_id;
                prv_hour_rec_init(prv_get_current_cycle_timestamp_sec());
            }
            if (!beacon_due_now && !rx_due_now) {
                prv_schedule_wakeup();
                return;
            }
        }

        if (prv_is_minute_test_active() && (test50_due_now || ((next_test50 < next_beacon) && (next_test50 < next_rx)))) {
            prv_handle_test50_actions(now_sec);
            if (test50_due_now) {
                uint32_t due_minute_id = ((uint32_t)(due_test50 / 100u)) / 60u;
                prv_request_minute_test_uplink_for_minute(due_minute_id);
                if (prv_run_catm1_uplink_now()) {
                    return;
                }
            }
            prv_schedule_wakeup();
            return;
        }

        if ((((catm1_due_now) && s_catm1_uplink_pending) ||
             prv_should_try_catm1_uplink_now(now_sec)) &&
            !beacon_due_now && !rx_due_now && !rx_prestart_now) {
            if (prv_run_catm1_uplink_now()) {
                return;
            }
        }

        if (beacon_due_now) {
            uint32_t beacon_sec = (uint32_t)(due_beacon / 100u);
            prv_prepare_beacon_burst(beacon_sec,
                                     prv_get_beacon_burst_count(),
                                     GW_BEACON_BURST_GAP_MS,
                                     GW_BEACON_TX_KIND_SCHEDULED,
                                     beacon_slot_id);
            if (prv_start_pending_beacon_burst()) {
                return;
            }
            prv_schedule_wakeup();
            return;
        }

        if (rx_due_now || rx_prestart_now) {
            uint32_t hop_period = prv_get_hop_period_sec();
            const UI_Config_t* cfg = UI_GetConfig();
            uint32_t cycle_stamp_sec;
            uint64_t rx_event_centi = rx_due_now ? due_rx : next_rx;
            uint32_t rx_arm_lead_ms = 0u;

            if ((!rx_due_now) && (rx_event_centi > now_centi)) {
                rx_arm_lead_ms = (uint32_t)((rx_event_centi - now_centi) * 10u);
            }

            s_data_freq_hz = UI_RF_GetDataFreqHz((uint32_t)(rx_event_centi / 100u), hop_period, 0u);
            s_slot_cnt = (uint8_t)cfg->max_nodes;
            if (s_slot_cnt > UI_MAX_NODES) {
                s_slot_cnt = UI_MAX_NODES;
            }
            if (s_test_mode && (s_slot_cnt > UI_TESTMODE_MAX_NODES)) {
                s_slot_cnt = UI_TESTMODE_MAX_NODES;
            }
            if (prv_is_two_minute_mode_active() && (s_slot_cnt > UI_TESTMODE_MAX_NODES)) {
                s_slot_cnt = UI_TESTMODE_MAX_NODES;
            }
            s_rx_expected_nodes = s_slot_cnt;
            /* s_rx_cycle_start_ms는 실제 arm 시각이 아니라 nominal slot anchor를 유지한다.
             * 그래서 RX를 미리 arm 해도 slot timeout과 cycle 종료 경계는 원래 20/22/...초 기준으로 계산된다. */
            s_rx_cycle_start_ms = HAL_GetTick() + rx_arm_lead_ms;
            s_rx_nodes_seen_mask = 0u;
            /* 2초 슬롯 * 최대 50노드 + 2초 guard = 총 102초 -> cycle +02:42에서 RX 종료 */
            s_rx_window_deadline_ms = s_rx_cycle_start_ms + ((uint32_t)s_slot_cnt * UI_SLOT_DURATION_MS) + GW_RX_WINDOW_GUARD_MS;
            if (UI_BLE_IsActive()) {
                UI_BLE_Disable();
            }
            if (!prv_radio_ready_for_rx()) {
                prv_schedule_wakeup();
                return;
            }
            cycle_stamp_sec = prv_get_current_cycle_timestamp_sec();
            s_rx_cycle_minute_test = prv_is_minute_test_active();
            s_rx_cycle_stamp_sec = cycle_stamp_sec;
            UI_LPM_LockStop();
            s_state = GW_STATE_RX_SLOTS;
            prv_hour_rec_init(cycle_stamp_sec);
            if (!prv_arm_rx_slot()) {
                s_state = GW_STATE_IDLE;
                UI_LPM_UnlockStop();
                s_rx_cycle_minute_test = false;
                s_rx_cycle_stamp_sec = 0u;
                s_rx_window_deadline_ms = 0u;
                s_rx_cycle_start_ms = 0u;
                s_rx_expected_nodes = 0u;
                s_rx_nodes_seen_mask = 0u;
                prv_schedule_wakeup();
            }
            return;
        }
    }

    prv_schedule_wakeup();
}

static void GW_TaskMain(void)
{
    GW_App_Process();
}

static void prv_schedule_wakeup(void)
{
    uint64_t now_centi;
    uint64_t next;
    uint64_t next_beacon;
    uint64_t next_rx;
    uint32_t beacon_interval;
    uint32_t beacon_off;
    uint32_t rx_interval;
    uint32_t rx_start;
    uint32_t reminder_off = 0u;
    uint32_t two_min_prep_off = 0u;
    uint64_t delta_centi;
    uint32_t delta_ms;
    bool have_reminder;
    bool have_two_min_prep;

    if (s_state != GW_STATE_IDLE) {
        return;
    }
    if (s_sync_wait_deadline_ms != 0u) {
        return;
    }
    if (s_dormant_stop_mode) {
        return;
    }
    if (GW_Catm1_GetJobState() == GW_CATM1_JOB_STATE_BUSY) {
        prv_arm_busy_state_safety_wakeup(20u);
        return;
    }
    if (s_boot_time_sync_pending) {
        return;
    }

    prv_update_test_mode();
    now_centi = UI_Time_NowCenti2016();
    if (s_boot_time_sync_beacon_pending) {
        prv_schedule_next_second_tick(now_centi);
        return;
    }
    if (s_beacon_oneshot_pending) {
        prv_schedule_after_ms(10u);
        return;
    }

    beacon_interval = prv_get_beacon_interval_sec();
    beacon_off = prv_get_beacon_offset_sec();
    rx_interval = prv_get_rx_interval_sec();
    rx_start = prv_get_rx_start_offset_sec();
    have_reminder = prv_get_reminder_offset_sec(&reminder_off);
    have_two_min_prep = prv_get_two_minute_prep_offset_sec(&two_min_prep_off);

    next_beacon = prv_next_event_centi(now_centi, beacon_interval, beacon_off);
    next_rx = prv_next_event_centi(now_centi, rx_interval, rx_start);
    if (next_rx > now_centi) {
        uint64_t rx_prestart_centi = prv_get_rx_prestart_centi();
        if ((next_rx - now_centi) > rx_prestart_centi) {
            next_rx -= rx_prestart_centi;
        } else {
            next_rx = now_centi + 1u;
        }
    }
    next = (next_beacon < next_rx) ? next_beacon : next_rx;

    if (have_reminder) {
        uint64_t next_reminder = prv_next_event_centi(now_centi, rx_interval, reminder_off);
        if (next_reminder < next) {
            next = next_reminder;
        }
    }
    if (have_two_min_prep) {
        uint64_t next_prep = prv_next_event_centi(now_centi, rx_interval, two_min_prep_off);
        if (next_prep < next) {
            next = next_prep;
        }
    }
    if (prv_is_minute_test_active()) {
        uint64_t next_test50 = prv_next_test50_centi(now_centi);
        if (next_test50 < next) {
            next = next_test50;
        }
    }
    if (prv_is_catm1_periodic_active()) {
        uint64_t next_catm1 = prv_next_event_centi(now_centi, prv_get_catm1_period_sec(), prv_get_catm1_offset_sec());
        if (next_catm1 < next) {
            next = next_catm1;
        }
    }
    if (s_catm1_uplink_pending && s_catm1_immediate_try_pending) {
        uint64_t next_uplink;

        if (prv_tcp_uplink_blocked_by_ble()) {
            /* BLE 명령 세션이 살아있는 동안에는 10ms busy polling을 하지 않고
             * 1초 간격으로만 재확인한다. BLE가 꺼지면 즉시 uplink가 이어진다. */
            next_uplink = now_centi + (((uint64_t)GW_CATM1_PENDING_POLL_MS + 9u) / 10u);
        } else if (prv_is_minute_test_active() && (((uint32_t)(now_centi / 100u) % 60u) < 40u)) {
            next_uplink = prv_next_test50_centi(now_centi);
        } else {
            next_uplink = now_centi + 1u;
        }

        if (next_uplink < next) {
            next = next_uplink;
        }
    }

    delta_centi = (next > now_centi) ? (next - now_centi) : 1u;
    delta_ms = (uint32_t)(delta_centi * 10u);
    if (delta_ms == 0u) {
        delta_ms = 1u;
    }
    (void)UTIL_TIMER_Stop(&s_tmr_wakeup);
    (void)UTIL_TIMER_SetPeriod(&s_tmr_wakeup, delta_ms);
    (void)UTIL_TIMER_Start(&s_tmr_wakeup);
}

void GW_App_Init(void)
{
    if (s_inited) {
        return;
    }
#if (UI_USE_SEQ_MULTI_TASKS == 1u)
    UTIL_SEQ_RegTask(UI_TASK_BIT_GW_MAIN, 0, GW_TaskMain);
#endif
    (void)UTIL_TIMER_Create(&s_tmr_wakeup, 100u, UTIL_TIMER_ONESHOT, prv_tmr_wakeup_cb, NULL);
    (void)UTIL_TIMER_Create(&s_tmr_led1_pulse, 10u, UTIL_TIMER_ONESHOT, prv_led1_pulse_off_cb, NULL);
    (void)UTIL_TIMER_Create(&s_tmr_ble_test_expire, GW_BLE_TEST_SESSION_MS, UTIL_TIMER_ONESHOT, prv_tmr_ble_test_expire_cb, NULL);
    GW_Storage_Init();
    prv_flash_tx_reset_to_tail();
    GW_Catm1_Init();
    prv_led1(false);
    s_state = GW_STATE_IDLE;
    s_tcp_enabled = true; /* power-on 기본값은 항상 TCP ON */
    prv_evt_clear_all();
    s_beacon_counter = 0;
    s_test_mode = false;
    s_ble_test_session_active = false;
    s_rx_cycle_minute_test = false;
    s_rx_cycle_stamp_sec = 0u;
    s_rx_window_deadline_ms = 0u;
    s_rx_cycle_start_ms = 0u;
    s_rx_expected_nodes = 0u;
    s_rx_nodes_seen_mask = 0u;
    s_last_cycle_nodes_seen_mask = 0u;
    s_last_cycle_nodes_seen_mask_valid = false;
    s_catm1_retry_not_before_ms = 0u;
    prv_cancel_pending_beacon_burst();
    s_beacon_recovery_mode = false;
    s_last_cycle_valid = false;
    s_last_cycle_nodes_seen_mask = 0u;
    s_last_cycle_nodes_seen_mask_valid = false;
    s_last_cycle_minute_id = 0u;
    s_last_save_minute_id = 0xFFFFFFFFu;
    s_catm1_uplink_pending = false;
    s_catm1_immediate_try_pending = false;
    s_catm1_retry_not_before_ms = 0u;
    s_last_catm1_slot_id = 0xFFFFFFFFu;
    s_last_minute_test_uplink_minute_id = 0xFFFFFFFFu;
    s_last_live_uplink_epoch_sec = 0xFFFFFFFFu;
    s_last_2m_prep_slot_id = 0xFFFFFFFFu;
    s_last_periodic_beacon_slot_id = 0xFFFFFFFFu;
    s_active_periodic_beacon_slot_id = 0xFFFFFFFFu;
    s_beacon_tx_kind = GW_BEACON_TX_KIND_NONE;
    s_sync_wait_deadline_ms = 0u;
    s_led1_sync_blink_active = false;
    s_led1_sync_blink_on = false;
    s_dormant_stop_mode = false;
    s_boot_time_sync_beacon_pending = false;
    prv_clear_catm1_tx_ctx();
    /* MCU 부팅 때마다 CAT-M1 one-shot 시간 확인을 수행한다.
     * 다만 boot URC(*PSUTTZ)로 시간이 이미 들어온 세션에서는 gw_catm1 쪽에서
     * +CCLK?만 짧게 확인하고 무거운 bootstrap/retry는 생략한다. */
    s_boot_time_sync_pending = true;
    prv_hour_rec_init(prv_get_current_cycle_timestamp_sec());
    s_inited = true;
    prv_schedule_wakeup();
    UTIL_SEQ_SetTask(UI_TASK_BIT_GW_MAIN, 0);
}

void UI_Hook_OnConfigChanged(void)
{
    if (!s_inited) {
        return;
    }
    prv_exit_dormant_stop_mode();
    prv_cancel_pending_beacon_burst();
    s_catm1_uplink_pending = false;
    s_catm1_immediate_try_pending = false;
    s_catm1_retry_not_before_ms = 0u;
    s_last_catm1_slot_id = 0xFFFFFFFFu;
    s_last_minute_test_uplink_minute_id = 0xFFFFFFFFu;
    s_last_live_uplink_epoch_sec = 0xFFFFFFFFu;
    s_last_2m_prep_slot_id = 0xFFFFFFFFu;
    s_last_periodic_beacon_slot_id = 0xFFFFFFFFu;
    s_boot_time_sync_beacon_pending = false;
    prv_clear_catm1_tx_ctx();
    prv_update_test_mode();
    prv_schedule_wakeup();
}

bool UI_Hook_IsTcpEnabled(void)
{
    return GW_App_IsTcpEnabled();
}

void UI_Hook_OnTcpModeChanged(bool enabled)
{
    prv_apply_tcp_mode_change(enabled);
}

void UI_Hook_OnSettingChanged(uint8_t value, char unit)
{
    (void)value;
    (void)unit;
    if (!s_inited) {
        return;
    }
    prv_exit_dormant_stop_mode();
    prv_cancel_pending_beacon_burst();
    s_last_save_minute_id = 0xFFFFFFFFu;
    s_catm1_retry_not_before_ms = 0u;
    s_last_catm1_slot_id = 0xFFFFFFFFu;
    s_last_minute_test_uplink_minute_id = 0xFFFFFFFFu;
    s_last_live_uplink_epoch_sec = 0xFFFFFFFFu;
    s_last_2m_prep_slot_id = 0xFFFFFFFFu;
    s_last_periodic_beacon_slot_id = 0xFFFFFFFFu;
    prv_update_test_mode();

    /* 같은 값(예: 5M->5M) 재입력도 재동기 요청으로 취급한다.
     * beacon one-shot을 즉시 다시 보내고, TCP uplink도 한 번 즉시 시도한다.
     * 다만 실패하면 이후 재시도는 120초 고정 지연이 아니라 다음 설정 주기 때 수행한다. */
    prv_prepare_beacon_burst(UI_Time_NowSec2016(),
                             prv_get_beacon_burst_count(),
                             GW_BEACON_BURST_GAP_MS,
                             GW_BEACON_TX_KIND_MANUAL,
                             0xFFFFFFFFu);
    prv_request_catm1_uplink_immediate();
    prv_evt_set(GW_EVT_BEACON_ONESHOT);
    UTIL_SEQ_SetTask(UI_TASK_BIT_GW_MAIN, 0);
}

void UI_Hook_OnTimeChanged(void)
{
    if (!s_inited) {
        return;
    }
    prv_exit_dormant_stop_mode();
    prv_cancel_pending_beacon_burst();
    s_catm1_uplink_pending = false;
    s_catm1_immediate_try_pending = false;
    s_last_catm1_slot_id = 0xFFFFFFFFu;
    s_last_minute_test_uplink_minute_id = 0xFFFFFFFFu;
    s_last_live_uplink_epoch_sec = 0xFFFFFFFFu;
    s_last_2m_prep_slot_id = 0xFFFFFFFFu;
    s_last_periodic_beacon_slot_id = 0xFFFFFFFFu;
    s_boot_time_sync_beacon_pending = false;
    prv_clear_catm1_tx_ctx();
    prv_schedule_wakeup();
}

void UI_Hook_OnBootTimeSyncBeaconRequested(void)
{
    if (!s_inited) {
        return;
    }

    s_boot_time_sync_beacon_pending = true;
    prv_request_schedule_recheck_now();
}

void UI_Hook_OnCatm1PowerFaultStopRequested(void)
{
    if (!s_inited) {
        return;
    }

    prv_enter_dormant_stop_mode();
}

void UI_Hook_OnTestStartRequested(void)
{
    if (!s_inited) {
        return;
    }

    prv_exit_dormant_stop_mode();
    prv_start_ble_test_session();
    if (s_last_cycle_valid) {
        prv_send_ble_test_report_if_active(&s_last_cycle_rec);
    } else {
        prv_send_ble_test_report_if_active(&s_hour_rec);
    }
}

void UI_Hook_OnBeaconOnceRequested(void)
{
    if (!s_inited) {
        return;
    }
    prv_exit_dormant_stop_mode();
    prv_prepare_beacon_burst(UI_Time_NowSec2016(),
                             prv_get_beacon_burst_count(),
                             GW_BEACON_BURST_GAP_MS,
                             GW_BEACON_TX_KIND_MANUAL,
                             0xFFFFFFFFu);
    prv_evt_set(GW_EVT_BEACON_ONESHOT);
    UTIL_SEQ_SetTask(UI_TASK_BIT_GW_MAIN, 0);
}

bool UI_Hook_OnSyncRequested(uint16_t duration_hours)
{
    prv_exit_dormant_stop_mode();
    return prv_start_sync_wait_mode(duration_hours);
}

bool UI_Hook_OnBleStartRequested(void)
{
    if ((!s_inited) || (s_sync_wait_deadline_ms == 0u)) {
        return false;
    }

    prv_cancel_sync_wait_and_resume_ble();
    return true;
}

void GW_Radio_OnTxDone(void)
{
    prv_evt_set(GW_EVT_RADIO_TX_DONE);
    UTIL_SEQ_SetTask(UI_TASK_BIT_GW_MAIN, 0);
}

void GW_Radio_OnTxTimeout(void)
{
    UI_Radio_MarkRecoverNeeded();
    prv_evt_set(GW_EVT_RADIO_TX_TIMEOUT);
    UTIL_SEQ_SetTask(UI_TASK_BIT_GW_MAIN, 0);
}

static void prv_close_rx_cycle_and_commit(void)
{
    uint64_t rx_seen_mask = s_rx_nodes_seen_mask;

    UI_Radio_EnterSleep();
    s_state = GW_STATE_IDLE;
    UI_LPM_UnlockStop();

    if (s_rx_cycle_stamp_sec != 0u) {
        s_hour_rec.epoch_sec = s_rx_cycle_stamp_sec;
    } else {
        s_hour_rec.epoch_sec = prv_get_current_cycle_timestamp_sec();
    }

    /* 현재 RX cycle에서 실제로 받은 노드만 남기고, 미수신 노드 slot은
     * invalid marker로 다시 지운다. 이렇게 해야 이전 cycle 값이나 부분 수신 값이
     * TCP payload/flash backlog로 넘어가지 않는다. */
    prv_sanitize_unreceived_nodes(&s_hour_rec, rx_seen_mask);
    s_last_cycle_nodes_seen_mask = rx_seen_mask;
    s_last_cycle_nodes_seen_mask_valid = true;

    s_rx_window_deadline_ms = 0u;
    s_rx_cycle_start_ms = 0u;
    s_rx_expected_nodes = 0u;
    s_rx_nodes_seen_mask = 0u;

    prv_mark_cycle_complete(&s_hour_rec);
    prv_update_beacon_recovery_mode_from_rec(&s_hour_rec);
    if (s_rx_cycle_minute_test) {
        uint32_t now_sec = UI_Time_NowSec2016();
        prv_handle_test50_actions_core(now_sec, true);
        prv_send_ble_test_report_if_active(&s_hour_rec);
        s_rx_cycle_minute_test = false;
        s_rx_cycle_stamp_sec = 0u;
        if ((now_sec % 60u) >= 40u) {
            prv_request_minute_test_uplink_for_minute(now_sec / 60u);
            if (prv_run_catm1_uplink_now()) {
                return;
            }
        }
    } else {
        if (s_tcp_enabled) {
            bool saved_ok = prv_save_hour_rec_verified(&s_hour_rec);
            prv_flash_tx_note_saved(saved_ok);
            GW_Storage_PurgeOldFiles(s_hour_rec.epoch_sec);
            prv_flash_tx_resync_after_storage_change();
        }
        s_rx_cycle_minute_test = false;
        s_rx_cycle_stamp_sec = 0u;
    }
    prv_schedule_wakeup();
}

static void prv_rx_next_slot(void)
{
    UI_Radio_EnterSleep();

    if (prv_rx_window_expired() || prv_rx_all_expected_nodes_seen()) {
        prv_close_rx_cycle_and_commit();
        return;
    }

    if (!prv_radio_ready_for_rx()) {
        s_rx_cycle_minute_test = false;
        s_rx_cycle_stamp_sec = 0u;
        s_rx_window_deadline_ms = 0u;
        s_rx_cycle_start_ms = 0u;
        s_rx_expected_nodes = 0u;
        s_rx_nodes_seen_mask = 0u;
        s_state = GW_STATE_IDLE;
        UI_LPM_UnlockStop();
        prv_schedule_wakeup();
        return;
    }
    if (!prv_arm_rx_slot()) {
        s_rx_cycle_minute_test = false;
        s_rx_cycle_stamp_sec = 0u;
        s_rx_window_deadline_ms = 0u;
        s_rx_cycle_start_ms = 0u;
        s_rx_expected_nodes = 0u;
        s_rx_nodes_seen_mask = 0u;
        s_state = GW_STATE_IDLE;
        UI_LPM_UnlockStop();
        prv_schedule_wakeup();
        return;
    }
}

void GW_Radio_OnRxDone(uint8_t *payload, uint16_t size, int16_t rssi, int8_t snr)
{
    if (size > sizeof(s_rx_shadow)) {
        size = sizeof(s_rx_shadow);
    }
    if ((payload != NULL) && (size > 0u)) {
        memcpy(s_rx_shadow, payload, size);
    }
    s_rx_shadow_size = size;
    s_rx_shadow_rssi = rssi;
    s_rx_shadow_snr = snr;
    prv_evt_set(GW_EVT_RADIO_RX_DONE);
    UTIL_SEQ_SetTask(UI_TASK_BIT_GW_MAIN, 0);
}

void GW_Radio_OnRxTimeout(void)
{
    prv_evt_set(GW_EVT_RADIO_RX_TIMEOUT);
    UTIL_SEQ_SetTask(UI_TASK_BIT_GW_MAIN, 0);
}

void GW_Radio_OnRxError(void)
{
    UI_Radio_MarkRecoverNeeded();
    prv_evt_set(GW_EVT_RADIO_RX_ERROR);
    UTIL_SEQ_SetTask(UI_TASK_BIT_GW_MAIN, 0);
}
