#include "ui_ringbuf.h"

void UI_RingBuf_Init(UI_RingBuf_t* rb, uint8_t* mem, uint16_t size)
{
    rb->buf  = mem;
    rb->size = size;
    rb->head = 0;
    rb->tail = 0;
}

static uint16_t prv_next(const UI_RingBuf_t* rb, uint16_t idx)
{
    idx++;
    if (idx >= rb->size) { idx = 0; }
    return idx;
}

bool UI_RingBuf_Push(UI_RingBuf_t* rb, uint8_t b)
{
    uint16_t next = prv_next(rb, rb->head);
    if (next == rb->tail)
    {
        /* overflow: drop */
        return false;
    }
    rb->buf[rb->head] = b;
    rb->head = next;
    return true;
}

bool UI_RingBuf_Pop(UI_RingBuf_t* rb, uint8_t* out)
{
    if (rb->tail == rb->head) { return false; }

    *out = rb->buf[rb->tail];
    rb->tail = prv_next(rb, rb->tail);
    return true;
}

uint16_t UI_RingBuf_Count(const UI_RingBuf_t* rb)
{
    if (rb->head >= rb->tail)
    {
        return (uint16_t)(rb->head - rb->tail);
    }
    return (uint16_t)(rb->size - rb->tail + rb->head);
}
