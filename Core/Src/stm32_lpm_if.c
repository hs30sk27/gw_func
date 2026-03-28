/* USER CODE BEGIN Header */
/**
  ******************************************************************************
  * @file    stm32_lpm_if.c
  * @author  MCD Application Team
  * @brief   Low layer function to enter/exit low power modes (stop, sleep)
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

/* Includes ------------------------------------------------------------------*/
#include "platform.h"
#include "stm32_lpm.h"
#include "stm32_lpm_if.h"
#include "usart_if.h"

/* USER CODE BEGIN Includes */
#include "ui_lpm.h"
/* USER CODE END Includes */

/* External variables ---------------------------------------------------------*/
/* USER CODE BEGIN EV */

/* USER CODE END EV */

/* Private typedef -----------------------------------------------------------*/
/**
  * @brief Power driver callbacks handler
  */
const struct UTIL_LPM_Driver_s UTIL_PowerDriver =
{
  PWR_EnterSleepMode,
  PWR_ExitSleepMode,

  PWR_EnterStopMode,
  PWR_ExitStopMode,

  PWR_EnterOffMode,
  PWR_ExitOffMode,
};

/* USER CODE BEGIN PTD */

/* USER CODE END PTD */

/* Private define ------------------------------------------------------------*/
/* USER CODE BEGIN PD */

/* USER CODE END PD */

/* Private macro -------------------------------------------------------------*/
/* USER CODE BEGIN PM */

/* USER CODE END PM */

/* Private variables ---------------------------------------------------------*/
/* USER CODE BEGIN PV */

/* USER CODE END PV */

/* Private function prototypes -----------------------------------------------*/
/* USER CODE BEGIN PFP */

/* USER CODE END PFP */

/* Exported functions --------------------------------------------------------*/

void PWR_EnterOffMode(void)
{
  /* USER CODE BEGIN EnterOffMode_1 */

  /* USER CODE END EnterOffMode_1 */
}

void PWR_ExitOffMode(void)
{
  /* USER CODE BEGIN ExitOffMode_1 */

  /* USER CODE END ExitOffMode_1 */
}

void PWR_EnterStopMode(void)
{
  /* USER CODE BEGIN EnterStopMode_1 */
  UI_LPM_BeforeStop_DeInitPeripherals();
  __NOP();
  /* USER CODE END EnterStopMode_1 */
  HAL_GPIO_WritePin(GPIOB, CATM1_PWR_Pin, GPIO_PIN_RESET);

  HAL_SuspendTick();
  /*
   * nd는 stop 직전 UI/RTC wake source를 이미 별도로 정리한다.
   * 그 위에 SysTick/PendSV pending 이 남아 있으면 WFI 직후 바로 빠져나와
   * STOP2에 못 머물거나 진입 직후 즉시 재기상하는 현상이 생길 수 있다.
   * gw와 동일하게 stale scheduler/tick pending 을 비운 뒤 STOP2로 진입한다.
   */
#if defined(SCB_ICSR_PENDSTCLR_Msk)
  SCB->ICSR = SCB_ICSR_PENDSTCLR_Msk;
#endif
#if defined(SCB_ICSR_PENDSVCLR_Msk)
  SCB->ICSR = SCB_ICSR_PENDSVCLR_Msk;
#endif
#if defined(PWR_FLAG_WU)
  __HAL_PWR_CLEAR_FLAG(PWR_FLAG_WU);
#endif

  LL_PWR_ClearFlag_C1STOP_C1STB();

  /* USER CODE BEGIN EnterStopMode_2 */
  __DSB();
  __ISB();

  /* USER CODE END EnterStopMode_2 */
  HAL_PWREx_EnterSTOP2Mode(PWR_STOPENTRY_WFI);
  /* USER CODE BEGIN EnterStopMode_3 */

  /* USER CODE END EnterStopMode_3 */
}

void PWR_ExitStopMode(void)
{
  /* USER CODE BEGIN ExitStopMode_1 */
#if 0
  /* USER CODE END ExitStopMode_1 */
  /* Resume sysTick : work around for debugger problem in dual core */
  HAL_ResumeTick();
  /*Not retained periph:
    ADC interface
    DAC interface USARTx, TIMx, i2Cx, SPIx
    SRAM ctrls, DMAx, DMAMux, AES, RNG, HSEM  */

  /* Resume not retained USARTx and DMA */
  vcom_Resume();
  /* USER CODE BEGIN ExitStopMode_2 */
#endif
  __NOP();

  SystemClock_Config();
  HAL_ResumeTick();
  UI_LPM_AfterStop_ReInitPeripherals();

  /* USER CODE END ExitStopMode_2 */
}

void PWR_EnterSleepMode(void)
{
  /* USER CODE BEGIN EnterSleepMode_1 */

  /* USER CODE END EnterSleepMode_1 */
  /* Suspend sysTick */
  HAL_SuspendTick();
  /* USER CODE BEGIN EnterSleepMode_2 */

  /* USER CODE END EnterSleepMode_2 */
  HAL_PWR_EnterSLEEPMode(PWR_MAINREGULATOR_ON, PWR_SLEEPENTRY_WFI);
  /* USER CODE BEGIN EnterSleepMode_3 */

  /* USER CODE END EnterSleepMode_3 */
}

void PWR_ExitSleepMode(void)
{
  /* USER CODE BEGIN ExitSleepMode_1 */

  /* USER CODE END ExitSleepMode_1 */
  /* Resume sysTick */
  HAL_ResumeTick();

  /* USER CODE BEGIN ExitSleepMode_2 */

  /* USER CODE END ExitSleepMode_2 */
}

/* USER CODE BEGIN EF */

/* USER CODE END EF */

/* Private Functions Definition -----------------------------------------------*/
/* USER CODE BEGIN PrFD */

/* USER CODE END PrFD */
