/*
 * ui_conf.h
 *
 * Core/ui 모듈 공통 설정
 *
 * - 가능한 한 기존 프로젝트 파일을 수정하지 않도록, 여기에서 대부분의 설정을 조정합니다.
 * - 핀 이름은 main.h에 생성된 매크로(OP_KEY_Pin 등)를 그대로 사용합니다.
 *
 * NOTE
 *  - 본 파일은 "간단/명료"를 목표로 하여, 복잡한 옵션을 최소화했습니다.
 *  - 필요 시 아래 매크로 값만 조정하면 됩니다.
 */

#ifndef UI_CONF_H
#define UI_CONF_H

#include <stdint.h>

/*
 * 핀 매크로(TEST_KEY_Pin, BT_EN_Pin, LED0_Pin 등)는 CubeMX가 생성한 main.h에 정의됩니다.
 *
 * Core/ui 내부에서 "핀 존재 여부 자동 감지"(defined(TEST_KEY_Pin) 등)를 사용할 때,
 * ui_conf.h가 main.h 보다 먼저 include 되면 감지가 0으로 떨어져 기능이 비활성화되는
 * 문제가 발생할 수 있습니다.
 *
 * 따라서 ui_conf.h에서 main.h를 포함하여 감지 및 하드웨어 제어가 항상 올바르게
 * 동작하도록 합니다.
 */
#include "main.h"

/*
 * 본 gw/nd 프로젝트는 STM32Cube Utilities 구조를 사용합니다.
 * Sequencer 설정 헤더는 "stm32_seq_conf.h"가 아니라 "utilities_conf.h" 입니다.
 * (Utilities/sequencer/stm32_seq.c 에서 utilities_conf.h 를 include)
 */
#include "utilities_conf.h"

/* -------------------------------------------------------------------------- */
/* UART (내부 명령 인터페이스)                                                */
/* -------------------------------------------------------------------------- */
#define UI_UART_LINE_MAX             (128u)   /* <명령>CRLF 최대 길이 */
#define UI_UART_RX_RING_SIZE         (256u)   /* 1바이트 RX 인터럽트 기반 링버퍼 */

/* 송신은 DMA 사용하지 않고, blocking HAL_UART_Transmit() 사용 (요구사항) */
#define UI_UART_TX_TIMEOUT_MS        (100u)
#define UI_BLE_UART_INIT_DELAY_MS      (10u)    /* BT_EN ON 후 UART1/(GW는 LPUART1) init 지연 */
#define UI_BLE_AT_CMD_AFTER_UART_INIT   (1u)     /* UART1 init 후 BLE 모듈에 AT 전송 */
#define UI_BLE_AT_CMD_DELAY_MS          (20u)    /* UART1 init 완료 후 AT 전송 전 가드 지연 */
#define UI_BLE_AT_CMD                   "AT\r\n"

/* -------------------------------------------------------------------------- */
/* GW CATM1 / SIM7080                                                         */
/* -------------------------------------------------------------------------- */
#define UI_TCPIP_DEFAULT_IP0            (221u)
#define UI_TCPIP_DEFAULT_IP1            (141u)
#define UI_TCPIP_DEFAULT_IP2            (29u)
#define UI_TCPIP_DEFAULT_IP3            (173u)
#define UI_TCPIP_DEFAULT_PORT           (9999u)
#define UI_TCPIP_MIN_PORT               (10u)

#define UI_CATM1_1NCE_APN               "iot.1nce.net"
#define UI_CATM1_BOOT_WAIT_MS           (5000u)
#define UI_CATM1_AT_TIMEOUT_MS          (1500u)
#define UI_CATM1_AT_SYNC_RETRY          (5u)
#define UI_CATM1_AT_SYNC_GAP_MS         (250u)
#define UI_CATM1_NET_ACT_TIMEOUT_MS     (15000u)
#define UI_CATM1_TCP_OPEN_TIMEOUT_MS    (30000u)
#define UI_CATM1_SEND_INPUT_TIMEOUT_MS  (5000u)
#define UI_CATM1_ACK_WAIT_MS            (10000u)
#define UI_CATM1_SERVER_PAYLOAD_MAX     (1200u)
#define UI_CATM1_GNSS_WAIT_MS           (10000u)
#define UI_CATM1_TIME_SYNC_REBOOT_WAIT_MS (7000u)
#define UI_CATM1_TIME_SYNC_RETRY        (5u)
#define UI_CATM1_TIME_SYNC_GAP_MS       (1000u)
#define UI_CATM1_NET_REG_TIMEOUT_MS     (90000u)
#define UI_CATM1_NET_REG_POLL_MS        (2000u)
#define UI_CATM1_PS_ATTACH_TIMEOUT_MS   (60000u)
#define UI_CATM1_PS_ATTACH_POLL_MS      (2000u)
#define UI_CATM1_PERIODIC_OFFSET_S      (180u)   /* +3분 */
#define UI_CATM1_DAILY_OFFSET_S         (0u)
#define UI_CATM1_RX_BUF_SZ              (256u)
#define UI_CATM1_RX_RING_SIZE            (512u)
#define UI_CATM1_QUERY_RX_POLL_MS        (2u)
#define UI_CATM1_QUERY_OK_IDLE_MS        (13u)
#define UI_CATM1_CNACT_QUERY_TIMEOUT_MS   (100u)
#define UI_CATM1_PWRDOWN_TIMEOUT_MS     (10000u)
#define UI_CATM1_PWRDOWN_WAIT_MS        (2500u)

/* SIM7080 PWRKEY 극성/펄스 설정
 * - SIM7080 reference는 PWRKEY active-low를 기준으로 설명한다.
 * - 보드에 inverter/transistor가 있으면 아래 active state만 바꿔 대응한다. */
#define UI_CATM1_PWRKEY_ACTIVE_STATE    (GPIO_PIN_RESET)
#define UI_CATM1_PWRKEY_INACTIVE_STATE  (GPIO_PIN_SET)
#define UI_CATM1_PWRKEY_ON_PULSE_MS     (1100u)
#define UI_CATM1_PWRKEY_OFF_PULSE_MS    (1500u)
#define UI_CATM1_PWRKEY_GUARD_MS        (20u)


/*
 * BLE(UART over BLE) 특성:
 *  - 하나의 명령이 여러 notification/패킷으로 쪼개져 들어오거나,
 *  - CRLF가 생략된 상태로 "잠깐의 무응답"으로 메시지 경계를 구분하는 경우가 있습니다.
 *
 * 요구사항:
 *  - 100ms 이내로 들어오는 데이터는 "같은 메시지"로 간주.
 *
 * 구현:
 *  - 마지막 RX 이후 UI_UART_COALESCE_MS 동안 추가 바이트가 없으면
 *    현재 누적된 버퍼를 1개의 라인으로 확정(process)합니다.
 */
#define UI_UART_COALESCE_MS          (100u)

/* -------------------------------------------------------------------------- */
/* ADC (내부 채널 측정용)                                                    */
/* -------------------------------------------------------------------------- */
/*
 * STM32 시리즈/HAL 버전에 따라 sampling time 매크로 이름이 다를 수 있습니다.
 * (WL/L4 계열: 47CYCLES_5 등이 일반적)
 */
#ifndef UI_ADC_SAMPLINGTIME
#if defined(ADC_SAMPLETIME_47CYCLES_5)
#define UI_ADC_SAMPLINGTIME          (ADC_SAMPLETIME_47CYCLES_5)
#elif defined(ADC_SAMPLETIME_39CYCLES_5)
#define UI_ADC_SAMPLINGTIME          (ADC_SAMPLETIME_39CYCLES_5)
#elif defined(ADC_SAMPLETIME_24CYCLES_5)
#define UI_ADC_SAMPLINGTIME          (ADC_SAMPLETIME_24CYCLES_5)
#else
/* 마지막 fallback: 프로젝트에서 직접 UI_ADC_SAMPLINGTIME을 정의하세요. */
#define UI_ADC_SAMPLINGTIME          (0u)
#endif
#endif

/* -------------------------------------------------------------------------- */
/* UART 저전력 정책                                                           */
/* -------------------------------------------------------------------------- */
/*
 * 배터리 구동(최소 전류) 기준 기본값
 *  - BT 전원(BT_EN)이 꺼져 있을 때는 UART1도 불필요하므로 RX를 시작하지 않음.
 *  - TEST_KEY로 BLE가 켜질 때(UI_BLE_EnableForMs) BT_EN만 먼저 ON.
 *  - UART1(그리고 GW는 LPUART1)은 UI_BLE_UART_INIT_DELAY_MS 이후 init.
 *
 * 만약 "항상 UART1로 명령을 받아야" 한다면 아래를 1로 변경하세요.
 */
#define UI_UART_BOOT_START            (0u)  /* 0: 부팅 시 RX 시작 안함(저전력), 1: 부팅 시 RX 시작 */

/* BLE OFF 시 UART1을 DeInit하여 전류 절감 (BT 모듈이 UART에 매달려 있는 경우 효과 큼) */
#define UI_UART_DEINIT_WHEN_BLE_OFF   (1u)

/* -------------------------------------------------------------------------- */
/* Key 입력 디바운스                                                          */
/* -------------------------------------------------------------------------- */
/*
 * TEST_KEY / OP_KEY는 EXTI 입력으로 들어오므로 채터링이 있으면
 * 짧은 시간 안에 여러 번 callback이 발생할 수 있습니다.
 *
 * 정책:
 *  - 첫 입력은 즉시 반영
 *  - 같은 키에서 UI_KEY_DEBOUNCE_MS 이내에 다시 들어온 EXTI는 무시
 *
 * 효과:
 *  - TEST_KEY: ON 직후 bounce로 즉시 OFF로 다시 토글되는 현상 방지
 *  - OP_KEY  : 한 번 눌렀는데 센서 측정/ASCII 전송이 중복 실행되는 현상 방지
 */
#define UI_KEY_DEBOUNCE_MS          (200u)

/* -------------------------------------------------------------------------- */
/* BLE(블루투스) 동작                                                        */
/* -------------------------------------------------------------------------- */
#define UI_BLE_ACTIVE_MS             (180000u) /* 3분 */
#define UI_LED0_ON_MS                (10u)
#define UI_LED0_OFF_MS               (490u)

/* -------------------------------------------------------------------------- */
/* -------------------------------------------------------------------------- */
/* Node/Gateway 공통 파라미터                                                 */
/* -------------------------------------------------------------------------- */
#define UI_NET_ID_LEN                (24u)    /* UTF-8 한글+숫자 NETID 지원 */
#define UI_MAX_NODES                 (50u)
#define UI_TESTMODE_MAX_NODES        (10u)     /* 테스트 모드에서 노드 수 제한 */
#define UI_LOC_ASCII_MAX             (160u)    /* GW LOC ASCII 저장 최대 길이(널 포함) */

/* Node payload/record compact format */
#define UI_NODE_BATT_LVL_LOW         (0u)
#define UI_NODE_BATT_LVL_NORMAL      (1u)
#define UI_NODE_BATT_LVL_INVALID     (0xFFu)
#define UI_NODE_BATT_LOW_THRESHOLD_X10 (33u)   /* legacy x10 경계 / GPIO batt text 매핑에도 사용 */

/* Battery level GPIO polarity & GW legacy voltage compatibility
 * - 실제 batt 판정은 ADC가 아니라 BATT_LVL 입력 핀을 사용한다.
 * - GW 저장 구조는 gw_volt_x10(0.1V) 필드를 그대로 쓰므로,
 *   GPIO level을 대표값(x10)으로 매핑해 하위 호환을 유지한다.
 * - 하드웨어가 active-low면 UI_BATT_LVL_NORMAL_GPIO_STATE를 GPIO_PIN_RESET으로 바꾸면 된다.
 */
#ifndef UI_BATT_LVL_NORMAL_GPIO_STATE
#define UI_BATT_LVL_NORMAL_GPIO_STATE (GPIO_PIN_SET)
#endif
#ifndef UI_GW_BATT_GPIO_NORMAL_REPR_X10
#define UI_GW_BATT_GPIO_NORMAL_REPR_X10 (UI_NODE_BATT_LOW_THRESHOLD_X10 + 2u)
#endif
#ifndef UI_GW_BATT_GPIO_LOW_REPR_X10
#define UI_GW_BATT_GPIO_LOW_REPR_X10 ((UI_NODE_BATT_LOW_THRESHOLD_X10 > 0u) ? (UI_NODE_BATT_LOW_THRESHOLD_X10 - 1u) : 0u)
#endif
#define UI_NODE_TEMP_MIN_C           ((int8_t)-50)
#define UI_NODE_TEMP_MAX_C           ((int8_t)100)
#define UI_NODE_TEMP_INVALID_C       ((int8_t)-128)

/* -------------------------------------------------------------------------- */
/* 스케줄(LoRa)                                                               */
/* -------------------------------------------------------------------------- */
#define UI_BEACON_PERIOD_S           (300u)    /* 5분 */
#define UI_GW_RX_PERIOD_S_NORMAL     (3600u)   /* 1시간 */
#define UI_GW_RX_START_OFFSET_S      (60u)     /* 01분00초 (= 60초) */
#define UI_GW_TEST_RX_START_S        (30u)     /* 테스트 모드: 30초에 수신 준비 */
#define UI_SLOT_DURATION_MS          (2000u)   /* 노드별 2초 슬롯: cycle +02분42초 종료에 맞춤 */

/* Node 전원 인가 후 Beacon 수신 대기 시간 */
#define UI_ND_BOOT_LISTEN_MS         (360000u) /* 6분 */

/* Node 센서 체크 시작 시각 */
#define UI_ND_SENSOR_START_S_NORMAL  (6u)      /* 00분06초 */
#define UI_ND_SENSOR_START_S_TEST    (6u)      /* 테스트 모드에서도 00초 기준 +6초 */

/* 내부 설정 저장용 Flash fallback 크기(FLASHSIZE_BASE를 못 읽는 경우) */
#define UI_CFG_FLASH_FALLBACK_SIZE_BYTES (256u * 1024u)

/* -------------------------------------------------------------------------- */
/* UTIL_SEQ Task id (bit mask)                                                */
/* -------------------------------------------------------------------------- */
/*
 * 요구사항(현장 이슈 대응):
 *  - 프로젝트마다 UTIL_SEQ_CONF_TASK_NBR(=등록 가능한 task bit 수)가 1/2/8/16 등
 *    매우 작게 설정되어 있을 수 있습니다.
 *  - task bit을 3개(UI_MAIN/BLE/ROLE)로 고정하면, task 등록/실행이 되지 않아
 *    TEST_KEY/BLE/LED 등이 "아예 동작하지 않는" 문제가 발생했습니다.
 *
 * 해결 정책(배터리 전류 최소화 + 호환성 우선):
 *  - 기본은 task bit 1개(UI_MAIN)만 사용합니다.
 *    BLE/ROLE 이벤트도 모두 UI_MAIN에서 처리할 수 있도록 설계되어 있습니다.
 *  - 만약 UTIL_SEQ_CONF_TASK_NBR >= 3 이면, 원하면 BLE/ROLE을 별도 task로
 *    분리하여도 됩니다(성능/가독성). 단, 전류 관점에서는 큰 차이가 없습니다.
 */
/* UI 내부 계산용 task bit 수(최소 1로 클램프) */
#ifndef UI_SEQ_TASK_NBR
#if defined(UTIL_SEQ_CONF_TASK_NBR)
  #if (UTIL_SEQ_CONF_TASK_NBR >= 1)
    #define UI_SEQ_TASK_NBR         (UTIL_SEQ_CONF_TASK_NBR)
  #else
    /* 일부 프로젝트에서 0으로 정의되는 사례(Sequencer 비활성). UI는 1개로 동작시킴 */
    #define UI_SEQ_TASK_NBR         (1U)
    #define UI_SEQ_TASK_NBR_CLAMPED (1U)
  #endif
#else
  /* 정의가 없으면 UI는 단일 task(bit0)만 사용 */
  #define UI_SEQ_TASK_NBR           (1U)
#endif
#endif

/* 멀티 task(3개 분리) 사용 여부 */
/* 필요하면 프로젝트에서 -DUI_USE_SEQ_MULTI_TASKS=0/1 로 강제 가능 */
#ifndef UI_USE_SEQ_MULTI_TASKS
#if (UI_SEQ_TASK_NBR >= 3)
#define UI_USE_SEQ_MULTI_TASKS       (1u)
#else
#define UI_USE_SEQ_MULTI_TASKS       (0u)
#endif
#endif

/* UI_MAIN은 항상 1개 사용 (가능하면 마지막 bit를 사용) */
#ifndef UI_TASK_IDX_UI_MAIN
#define UI_TASK_IDX_UI_MAIN          (UI_SEQ_TASK_NBR - 1U)
#endif

#if (UI_USE_SEQ_MULTI_TASKS == 1u)
/* task index (0..UI_SEQ_TASK_NBR-1) */
#ifndef UI_TASK_IDX_BLE
#define UI_TASK_IDX_BLE              (UI_SEQ_TASK_NBR - 2U)
#endif
#ifndef UI_TASK_IDX_ROLE
#define UI_TASK_IDX_ROLE             (UI_SEQ_TASK_NBR - 3U)
#endif

/* bit mask */
#define UI_TASK_BIT_UI_MAIN          (1UL << (UI_TASK_IDX_UI_MAIN))
#define UI_TASK_BIT_BLE              (1UL << (UI_TASK_IDX_BLE))
#define UI_TASK_BIT_ROLE_MAIN        (1UL << (UI_TASK_IDX_ROLE))
#else
/* 단일 task 모드: BLE/ROLE도 UI_MAIN으로 스케줄 */
#define UI_TASK_BIT_UI_MAIN          (1UL << (UI_TASK_IDX_UI_MAIN))
#define UI_TASK_BIT_BLE              (UI_TASK_BIT_UI_MAIN)
#define UI_TASK_BIT_ROLE_MAIN        (UI_TASK_BIT_UI_MAIN)
#endif

/* 역할별 파일에서 기존 이름을 유지하기 위한 alias */
#define UI_TASK_BIT_ND_MAIN          (UI_TASK_BIT_ROLE_MAIN)
#define UI_TASK_BIT_GW_MAIN          (UI_TASK_BIT_ROLE_MAIN)

/* -------------------------------------------------------------------------- */
/* GUI UART callback ownership                                            */
/* -------------------------------------------------------------------------- */
/*
 * HAL_UART_RxCpltCallback()는 프로젝트의 uart_if.c에서 정의합니다.
 *
 * 따라서 Core/gui 또는 Core/nui는 HAL_UART_RxCpltCallback()를 오버라이드하지 않습니다.
 */
#define UI_OVERRIDE_HAL_UART_CALLBACKS (0u)
#define UI_OVERRIDE_HAL_UART_WEAK_DISPATCH (1u)

/* -------------------------------------------------------------------------- */
/* HAL weak callback override                                                 */
/* -------------------------------------------------------------------------- */
/*
 * - UI_USE_HAL_CALLBACK_OVERRIDE는 기존 통합 호환을 위해 남겨둔 옵션입니다.
 * - HAL_UART_RxCpltCallback()는 uart_if.c에서 관리합니다.
 * - EXTI callback은 UI_OVERRIDE_HAL_GPIO_EXTI_CALLBACK로 별도 제어합니다.
 */
#define UI_USE_HAL_CALLBACK_OVERRIDE (0u)

/*
 * HAL_GPIO_EXTI_Callback 오버라이드(기본 ON)
 *
 * 현장에서 가장 흔한 문제:
 *  - 프로젝트에 HAL_GPIO_EXTI_Callback 구현이 없고(=HAL의 __weak만 존재)
 *    TEST_KEY/OP_KEY 같은 EXTI 기반 입력이 전혀 동작하지 않는 경우가 있습니다.
 *
 * 이 옵션이 1이면 Core/ui가 HAL의 __weak HAL_GPIO_EXTI_Callback을 "자동 오버라이드"하여
 * UI_GPIO_ExtiCallback()으로 연결합니다.
 *
 * 만약 프로젝트의 다른 파일에서 HAL_GPIO_EXTI_Callback을 이미 구현했다면
 * 링크 중복이 발생할 수 있으므로 0으로 바꾸고, 그 구현에서 UI_GPIO_ExtiCallback을 호출하세요.
 */
#define UI_OVERRIDE_HAL_GPIO_EXTI_CALLBACK (1u)

/* -------------------------------------------------------------------------- */
/* CRC16                                                                      */
/* -------------------------------------------------------------------------- */
#define UI_CRC16_INIT                (0xFFFFu)
/* CRC-16/CCITT-FALSE: poly 0x1021, init 0xFFFF, xorout 0x0000 */
#define UI_CRC16_POLY                (0x1021u)

/* -------------------------------------------------------------------------- */
/* Epoch 기준                                                                 */
/* -------------------------------------------------------------------------- */
/*
 * 요구사항: Epoch 기준을 2016-01-01로 잡아서 표현 시간을 늘림.
 * 본 구현은 "epoch2016" = 2016-01-01 00:00:00 기준의 초/1/100초를 사용합니다.
 */
#define UI_EPOCH_BASE_YEAR           (2016u)

/* -------------------------------------------------------------------------- */
/* 핀 존재 여부 자동 감지                                                     */
/* -------------------------------------------------------------------------- */
#if defined(TEST_KEY_Pin)
#define UI_HAVE_TEST_KEY             (1u)
#else
#define UI_HAVE_TEST_KEY             (0u)
#endif

#if defined(OP_KEY_Pin)
#define UI_HAVE_OP_KEY               (1u)
#else
#define UI_HAVE_OP_KEY               (0u)
#endif

#if defined(PULSE_IN_Pin)
#define UI_HAVE_PULSE_IN             (1u)
#else
#define UI_HAVE_PULSE_IN             (0u)
#endif

#if defined(BT_EN_Pin)
#define UI_HAVE_BT_EN                (1u)
#else
#define UI_HAVE_BT_EN                (0u)
#endif

#if defined(LED0_Pin)
#define UI_HAVE_LED0                 (1u)
#else
#define UI_HAVE_LED0                 (0u)
#endif

#if defined(LED1_Pin)
#define UI_HAVE_LED1                 (1u)
#else
#define UI_HAVE_LED1                 (0u)
#endif

#endif /* UI_CONF_H */
