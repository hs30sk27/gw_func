/* USER CODE BEGIN Header */
/**
  ******************************************************************************
  * @file           : main.h
  * @brief          : Header for main.c file.
  *                   This file contains the common defines of the application.
  ******************************************************************************
  * @attention
  *
  * Copyright (c) 2026 STMicroelectronics.
  * All rights reserved.
  *
  * This software is licensed under terms that can be found in the LICENSE file
  * in the root directory of this software component.
  * If no LICENSE file comes with this software, it is provided AS-IS.
  *
  ******************************************************************************
  */
/* USER CODE END Header */

/* Define to prevent recursive inclusion -------------------------------------*/
#ifndef __MAIN_H
#define __MAIN_H

#ifdef __cplusplus
extern "C" {
#endif

/* Includes ------------------------------------------------------------------*/
#include "stm32wlxx_hal.h"

/* Private includes ----------------------------------------------------------*/
/* USER CODE BEGIN Includes */
#include "ui_core.h"
#include "gw_app.h"
#include "ui_lpm.h"
#include <stddef.h>



extern void SystemClock_Config(void);

/* USER CODE END Includes */

/* Exported types ------------------------------------------------------------*/
/* USER CODE BEGIN ET */
extern ADC_HandleTypeDef hadc;

extern UART_HandleTypeDef hlpuart1;
extern UART_HandleTypeDef huart1;
extern DMA_HandleTypeDef hdma_usart1_tx;

extern RTC_HandleTypeDef hrtc;

extern SPI_HandleTypeDef hspi1;

extern SUBGHZ_HandleTypeDef hsubghz;

/* USER CODE END ET */

/* Exported constants --------------------------------------------------------*/
/* USER CODE BEGIN EC */

/* USER CODE END EC */

/* Exported macro ------------------------------------------------------------*/
/* USER CODE BEGIN EM */

/* USER CODE END EM */

/* Exported functions prototypes ---------------------------------------------*/
void Error_Handler(void);
void MX_GPIO_Init(void);
void MX_DMA_Init(void);
void MX_SPI1_Init(void);
void MX_SUBGHZ_Init(void);
void MX_USART1_UART_Init(void);
void MX_LPUART1_UART_Init(void);
void MX_RTC_Init(void);
void MX_ADC_Init(void);

/* USER CODE BEGIN EFP */

/* USER CODE END EFP */

/* Private defines -----------------------------------------------------------*/
#define RTC_PREDIV_A ((1<<(15-RTC_N_PREDIV_S))-1)
#define RTC_N_PREDIV_S 10
#define RTC_PREDIV_S ((1<<RTC_N_PREDIV_S)-1)
#define BLE_TX_Pin GPIO_PIN_6
#define BLE_TX_GPIO_Port GPIOB
#define BLE_RX_Pin GPIO_PIN_7
#define BLE_RX_GPIO_Port GPIOB
#define W25Q128_CS_Pin GPIO_PIN_8
#define W25Q128_CS_GPIO_Port GPIOB
#define PWR_KEY_Pin GPIO_PIN_1
#define PWR_KEY_GPIO_Port GPIOA
#define CATM1_RX_Pin GPIO_PIN_2
#define CATM1_RX_GPIO_Port GPIOA
#define CATM1_TX_Pin GPIO_PIN_3
#define CATM1_TX_GPIO_Port GPIOA
#define LED0_Pin GPIO_PIN_4
#define LED0_GPIO_Port GPIOA
#define LED1_Pin GPIO_PIN_5
#define LED1_GPIO_Port GPIOA
#define RF_TXEN_Pin GPIO_PIN_6
#define RF_TXEN_GPIO_Port GPIOA
#define RF_RXEN_Pin GPIO_PIN_7
#define RF_RXEN_GPIO_Port GPIOA
#define OP_KEY_Pin GPIO_PIN_8
#define OP_KEY_GPIO_Port GPIOA
#define OP_KEY_EXTI_IRQn EXTI9_5_IRQn
#define BATT_LVL_Pin GPIO_PIN_9
#define BATT_LVL_GPIO_Port GPIOA
#define TH_Pin GPIO_PIN_2
#define TH_GPIO_Port GPIOB
#define CATM1_PWR_Pin GPIO_PIN_12
#define CATM1_PWR_GPIO_Port GPIOB
#define BT_EN_Pin GPIO_PIN_13
#define BT_EN_GPIO_Port GPIOC
#define TEST_KEY_Pin GPIO_PIN_15
#define TEST_KEY_GPIO_Port GPIOA
#define TEST_KEY_EXTI_IRQn EXTI15_10_IRQn

/* USER CODE BEGIN Private defines */

/* USER CODE END Private defines */

#ifdef __cplusplus
}
#endif

#endif /* __MAIN_H */
