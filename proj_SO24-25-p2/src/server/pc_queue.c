#include "pc_queue.h"

#include <stdlib.h>
#include <semaphore.h>

#include "../common/utils.h"

int pcq_create(pc_queue_t *queue, size_t capacity) {
  if (queue == NULL || capacity <= 0) {
    return 1;
  }

  mutex_init(&queue->pcq_head_lock);
  queue->pcq_head = 0;

  mutex_init(&queue->pcq_tail_lock);
  queue->pcq_tail = 0;

  sem_init(&queue->pcq_empty_slots, 0, capacity); // Tracks empty slots
  sem_init(&queue->pcq_filled_slots, 0, 0);      // Tracks filled slots

  queue->pcq_capacity = capacity;
  queue->pcq_buffer = (void **)malloc(capacity * sizeof(void *));

  return 0;
}

int pcq_destroy(pc_queue_t *queue) {
  if (queue == NULL || queue->pcq_buffer == NULL) {
    return 1;
  }

  free(queue->pcq_buffer);
  queue->pcq_buffer = NULL;

  mutex_destroy(&queue->pcq_head_lock);
  mutex_destroy(&queue->pcq_tail_lock);

  sem_destroy(&queue->pcq_empty_slots);
  sem_destroy(&queue->pcq_filled_slots);

  return 0;
}

int pcq_enqueue(pc_queue_t *queue, void *elem) {
  if (queue == NULL || queue->pcq_buffer == NULL) {
    return 1;
  }

  // Wait for an empty slot
  sem_wait(&queue->pcq_empty_slots);

  // Add the element to the buffer
  mutex_lock(&queue->pcq_tail_lock);
  queue->pcq_buffer[queue->pcq_tail] = elem;
  queue->pcq_tail = (queue->pcq_tail + 1) % queue->pcq_capacity;
  mutex_unlock(&queue->pcq_tail_lock);

  // Signal that a filled slot is available
  sem_post(&queue->pcq_filled_slots);

  return 0;
}

void *pcq_dequeue(pc_queue_t *queue) {
  if (queue == NULL || queue->pcq_buffer == NULL) {
    return NULL;
  }

  // Wait for a filled slot
  sem_wait(&queue->pcq_filled_slots);

  // Remove the element from the buffer
  mutex_lock(&queue->pcq_head_lock);
  void *elem = queue->pcq_buffer[queue->pcq_head];
  queue->pcq_head = (queue->pcq_head + 1) % queue->pcq_capacity;
  mutex_unlock(&queue->pcq_head_lock);

  // Signal that an empty slot is available
  sem_post(&queue->pcq_empty_slots);

  return elem;
}
