/*
 * ui_ringbuf.h
 *
 * 1바이트 RX 인터럽트에서 안전하게 사용 가능한 간단 링버퍼
 */

#ifndef UI_RINGBUF_H
#define UI_RINGBUF_H

#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct
{
    uint8_t*  buf;
    uint16_t  size;
    volatile uint16_t head; /* write */
    volatile uint16_t tail; /* read  */
} UI_RingBuf_t;

void  UI_RingBuf_Init(UI_RingBuf_t* rb, uint8_t* mem, uint16_t size);
bool  UI_RingBuf_Push(UI_RingBuf_t* rb, uint8_t b);
bool  UI_RingBuf_Pop(UI_RingBuf_t* rb, uint8_t* out);
uint16_t UI_RingBuf_Count(const UI_RingBuf_t* rb);

#ifdef __cplusplus
}
#endif

#endif /* UI_RINGBUF_H */
