#ifndef UTILS_H
#define UTILS_H

#include <pthread.h>
#include <stdbool.h>

/// Initializes the mutex, exiting it in case of failure.
/// @param mutex Mutex to be initialized.
void mutex_init(pthread_mutex_t *mutex);

/// Destroys the mutex, exiting it in case of failure.
/// @param mutex Mutex to be destroyed.
void mutex_destroy(pthread_mutex_t *mutex);

/// Locks the mutex, exiting it in case of failure.
/// @param mutex Mutex to be locked.
void mutex_lock(pthread_mutex_t *mutex);

/// Unlocks the mutex, exiting it in case of failure.
/// @param mutex Mutex to be unlocked.
void mutex_unlock(pthread_mutex_t *mutex);

/// Initializes the read-write lock, exiting it in case of failure.
/// @param rwlock Read-write lock to be initialized.
void rwlock_init(pthread_rwlock_t *rwlock);

/// Destroys the read-write lock, exiting it in case of failure.
/// @param rwlock Read-write lock to be destroyed.
void rwlock_destroy(pthread_rwlock_t *rwlock);

/// Locks the read-write lock for reading, exiting it in case of failure.
/// @param rwlock Read-write lock to be locked for reading.
void rwlock_rdlock(pthread_rwlock_t *rwlock);

/// Locks the read-write lock for writing, exiting it in case of failure.
/// @param rwlock Read-write lock to be locked for writing.
void rwlock_wrlock(pthread_rwlock_t *rwlock);

/// Unlocks the read-write lock, exiting it in case of failure.
/// @param rwlock Read-write lock to be unlocked.
void rwlock_unlock(pthread_rwlock_t *rwlock);

#endif // UTILS_H