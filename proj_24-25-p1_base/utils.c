#include "utils.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

void mutex_init(pthread_mutex_t *mutex) {
  if (pthread_mutex_init(mutex, NULL) != 0) {
    perror("Failed to Initialize the mutex");
    exit(EXIT_FAILURE);
  }
}

void mutex_destroy(pthread_mutex_t *mutex) {
  if (pthread_mutex_destroy(mutex) != 0) {
    perror("Failed to destroy the mutex");
    exit(EXIT_FAILURE);
  }
}

void mutex_lock(pthread_mutex_t *mutex) {
  if (pthread_mutex_lock(mutex) != 0) {
    perror("Failed to lock the mutex");
    exit(EXIT_FAILURE);
  }
}

void mutex_unlock(pthread_mutex_t *mutex) {
  if (pthread_mutex_unlock(mutex) != 0) {
    perror("Failed to unlock the mutex");
    exit(EXIT_FAILURE);
  }
}

void rwlock_init(pthread_rwlock_t *rwlock) {
  if (pthread_rwlock_init(rwlock, NULL) != 0) {
    perror("Failed to Initialize the rwlock");
    exit(EXIT_FAILURE);
  }
}

void rwlock_destroy(pthread_rwlock_t *rwlock) {
  if (pthread_rwlock_destroy(rwlock) != 0) {
    perror("Failed to destroy the rwlock");
    exit(EXIT_FAILURE);
  }
}

void rwlock_rdlock(pthread_rwlock_t *rwlock) {
  if (pthread_rwlock_rdlock(rwlock) != 0) {
    perror("Failed to lock the rwlock for reading");
    exit(EXIT_FAILURE);
  }
}

void rwlock_wrlock(pthread_rwlock_t *rwlock) {
  if (pthread_rwlock_wrlock(rwlock) != 0) {
    perror("Failed to lock the rwlock for writing");
    exit(EXIT_FAILURE);
  }
}

void rwlock_unlock(pthread_rwlock_t *rwlock) {
  if (pthread_rwlock_unlock(rwlock) != 0) {
    perror("Failed to unlock the rwlock");
    exit(EXIT_FAILURE);
  }
}

// File Management Functions

bool is_job_file(const char *filename) {
    // Check if the file has a .job extension
    const char *ext = strrchr(filename, '.');
    if (!ext || strcmp(ext, ".job") != 0) {
        return false;
    }

    // Check if the file is not .out or .bck
    if (strstr(filename, ".out") != NULL || strstr(filename, ".bck") != NULL) {
        return false;
    }

    return true;
}