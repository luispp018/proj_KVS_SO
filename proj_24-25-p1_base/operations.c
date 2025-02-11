#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <fcntl.h> // NEW
#include <pthread.h> // NEW


#include "kvs.h"
#include "constants.h"
#include "utils.h"

static struct HashTable* kvs_table = NULL;

/// Calculates a timespec from a delay in milliseconds.
/// @param delay_ms Delay in milliseconds.
/// @return Timespec with the given delay.
static struct timespec delay_to_timespec(unsigned int delay_ms) {
  return (struct timespec){delay_ms / 1000, (delay_ms % 1000) * 1000000};
}

int kvs_init() {
  //Verify if the hash table is initialized
  if (kvs_table != NULL) {
    fprintf(stderr, "KVS state has already been initialized\n");
    return 1;
  }

  kvs_table = create_hash_table();
  return kvs_table == NULL;
}

int kvs_terminate() {
  //Verify if the hash table is initialized
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  free_table(kvs_table);

  return 0;
}

int kvs_write(size_t num_pairs, char keys[][MAX_STRING_SIZE], char values[][MAX_STRING_SIZE]) {
  //Verify if the hash table is initialized  
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  for (size_t i = 0; i < num_pairs; i++) {
    int index = hash(keys[i]);
    rwlock_wrlock(&kvs_table->entry_locks[index]); // Lock the entry before writing

    if (write_pair(kvs_table, keys[i], values[i]) != 0) {
      fprintf(stderr, "Failed to write keypair (%s,%s)\n", keys[i], values[i]);
    }

    rwlock_unlock(&kvs_table->entry_locks[index]); // Unlock the entry after writing
  }

  return 0;
}

int kvs_read(size_t num_pairs, char keys[][MAX_STRING_SIZE], int out_fd) {
  //Verify if the hash table is initialized
  if (kvs_table == NULL) {
    const char *error_msg = "KVS state must be initialized\n";
    write(out_fd, error_msg, strlen(error_msg));
    return 1;
  }

  // Sort the keys array to ensure a consistent order
  qsort(keys, num_pairs, MAX_STRING_SIZE, (int (*)(const void *, const void *))strcmp);

  write(out_fd, "[", 1);

  for (size_t i = 0; i < num_pairs; i++) {
    int index = hash(keys[i]);
    rwlock_rdlock(&kvs_table->entry_locks[index]); // Lock the entry before reading

    char *result = read_pair(kvs_table, keys[i]);

    rwlock_unlock(&kvs_table->entry_locks[index]); // Unlock the entry after reading
    
    write(out_fd, "(", 1);
    write(out_fd, keys[i], strlen(keys[i]));
    write(out_fd, ",", 1);
    
    // Write the result or error message
    if (result == NULL) {
      const char *error_str = "KVSERROR";
      write(out_fd, error_str, strlen(error_str));
    } else {
      write(out_fd, result, strlen(result));
    }
    free(result);
    
    // Close parenthesis
    write(out_fd, ")", 1);
  }

  write(out_fd, "]\n", 2);

  return 0;
}


int kvs_delete(size_t num_pairs, char keys[][MAX_STRING_SIZE], int out_fd) {
  //Verify if the hash table is initialized
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  rwlock_wrlock(&kvs_table->rwlock); // Lock the table before deleting

  // Start building the output string
  char buffer[1024] = "[";  // Buffer to store the result
  size_t buffer_len = 1;    // Initialize buffer length to account for '['
  int missing_found = 0;    // Flag to indicate if any KVSMISSING was found

  for (size_t i = 0; i < num_pairs; i++) {
    int index = hash(keys[i]);
    rwlock_wrlock(&kvs_table->entry_locks[index]); // Lock the entry before deleting

    // Attempt to delete the pair
    if (delete_pair(kvs_table, keys[i]) != 0) {
      // Add the error message to the buffer
      int written = snprintf(buffer + buffer_len, sizeof(buffer) - buffer_len,
                             "(%s,KVSMISSING)", keys[i]);
      if (written < 0) {
        fprintf(stderr, "Error formatting string\n");
        rwlock_unlock(&kvs_table->rwlock);
        return 1;
      }
      buffer_len += (size_t)written; // Update buffer length
      missing_found = 1;            // Mark that at least one missing key was found
    }

    rwlock_unlock(&kvs_table->entry_locks[index]); // Unlock the entry after deleting
  }

  // If no missing keys were found, do not write anything to the output file
  if (!missing_found) {
    rwlock_unlock(&kvs_table->rwlock); // Unlock before returning
    return 0;
  }

  // Close the output string with ']'
  if (buffer_len < sizeof(buffer) - 1) {
    buffer[buffer_len++] = ']';
    buffer[buffer_len++] = '\n';
    buffer[buffer_len] = '\0';
  } else {
    fprintf(stderr, "Buffer overflow while finalizing results\n");
    rwlock_unlock(&kvs_table->rwlock);
    return 1;
  }

  // Write the entire output to the file descriptor
  write(out_fd, buffer, buffer_len);
  rwlock_unlock(&kvs_table->rwlock); // Unlock after writing
  return 0;
}



void kvs_show(int out_fd) {
  //Verify if the hash table is initialized
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return;
  }

  rwlock_wrlock(&kvs_table->rwlock); // Write lock before showing the table

  for (int i = 0; i < TABLE_SIZE; i++) {
    KeyNode *keyNode = kvs_table->table[i];
    while (keyNode != NULL) {
      // Print "(key, value)\n" for each key-value pair
      write(out_fd, "(", 1);
      write(out_fd, keyNode->key, strlen(keyNode->key));
      write(out_fd, ", ", 2);
      write(out_fd, keyNode->value, strlen(keyNode->value));
      write(out_fd, ")\n", 2);

      keyNode = keyNode->next; // Move to the next node
    }
  }

  rwlock_unlock(&kvs_table->rwlock); // Unlock after showing the table
}

int kvs_backup(const char *backup_file) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  int backup_fd = open(backup_file, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
  if (backup_fd == -1) {
    fprintf(stderr, "Failed to open backup file\n");
    return 1;
  }

  kvs_show(backup_fd);
  close(backup_fd);
  return 0;
}

void kvs_wait(unsigned int delay_ms) {
  struct timespec delay = delay_to_timespec(delay_ms);
  nanosleep(&delay, NULL);
}