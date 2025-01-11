#ifndef PRODUCER_CONSUMER_H
#define PRODUCER_CONSUMER_H

#include <stddef.h>
#include <semaphore.h>
#include <pthread.h>
#include "../common/utils.h"

typedef struct {
  void **pcq_buffer;            // The circular buffer
  size_t pcq_capacity;          // Maximum number of elements in the queue

  size_t pcq_head;              // Index of the head (next item to dequeue)
  pthread_mutex_t pcq_head_lock;        // Lock for head manipulation

  size_t pcq_tail;              // Index of the tail (next item to enqueue)
  pthread_mutex_t pcq_tail_lock;        // Lock for tail manipulation

  sem_t pcq_empty_slots;        // Semaphore for available slots
  sem_t pcq_filled_slots;       // Semaphore for available items
} pc_queue_t;

// Creates a producer-consumer queue.
// @param queue The queue to be created.
// @param capacity Capacity of the queue.
// Returns 0 on success, 1 on failure.
int pcq_create(pc_queue_t *queue, size_t capacity);

// Destroys a producer-consumer queue.
// @param queue The queue to be destroyed.
// Returns 0 on success, 1 on failure.
int pcq_destroy(pc_queue_t *queue);

// Enqueues an element into the queue.
// @param queue The queue to enqueue the element.
// @param elem The element to be enqueued.
// Returns 0 on success, 1 on failure.
int pcq_enqueue(pc_queue_t *queue, void *elem);

// Dequeues an element from the queue.
// @param queue The queue to dequeue the element.
// Returns the dequeued element on success, NULL on failure.
void *pcq_dequeue(pc_queue_t *queue);

#endif // PRODUCER_CONSUMER_H
