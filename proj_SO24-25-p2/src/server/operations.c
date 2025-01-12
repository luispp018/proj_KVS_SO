#include "operations.h"

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>

#include "constants.h"
#include "io.h"
#include "kvs.h"
#include "../common/constants.h"
#include "../common/io.h"

static struct HashTable *kvs_table = NULL;

//static Subscription subscriptions[MAX_NUMBER_SUB];
static pthread_rwlock_t subscriptions_lock = PTHREAD_RWLOCK_INITIALIZER;

static client_t *clients[MAX_SESSION_COUNT];
static pthread_rwlock_t clients_lock = PTHREAD_RWLOCK_INITIALIZER;



/// Calculates a timespec from a delay in milliseconds.
/// @param delay_ms Delay in milliseconds.
/// @return Timespec with the given delay.
static struct timespec delay_to_timespec(unsigned int delay_ms) {
  return (struct timespec){delay_ms / 1000, (delay_ms % 1000) * 1000000};
}

int kvs_init() {
  if (kvs_table != NULL) {
    fprintf(stderr, "KVS state has already been initialized\n");
    return 1;
  }

  kvs_table = create_hash_table();
  return kvs_table == NULL;
}

int kvs_terminate() {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  free_table(kvs_table);
  kvs_table = NULL;
  return 0;
}

int kvs_write(size_t num_pairs, char keys[][MAX_STRING_SIZE],
              char values[][MAX_STRING_SIZE]) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  pthread_rwlock_wrlock(&kvs_table->tablelock);

  for (size_t i = 0; i < num_pairs; i++) {
    if (write_pair(kvs_table, keys[i], values[i]) != 0) {
      fprintf(stderr, "Failed to write key pair (%s,%s)\n", keys[i], values[i]);
    } else {
      notify_subscribers(keys[i], values[i]);
    }
  }

  pthread_rwlock_unlock(&kvs_table->tablelock);
  return 0;
}

int kvs_read(size_t num_pairs, char keys[][MAX_STRING_SIZE], int fd) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  pthread_rwlock_rdlock(&kvs_table->tablelock);

  write_str(fd, "[");
  for (size_t i = 0; i < num_pairs; i++) {
    char *result = read_pair(kvs_table, keys[i]);
    char aux[MAX_STRING_SIZE];
    if (result == NULL) {
      snprintf(aux, MAX_STRING_SIZE, "(%s,KVSERROR)", keys[i]);
    } else {
      snprintf(aux, MAX_STRING_SIZE, "(%s,%s)", keys[i], result);
    }
    write_str(fd, aux);
    free(result);
  }
  write_str(fd, "]\n");

  pthread_rwlock_unlock(&kvs_table->tablelock);
  return 0;
}

int kvs_delete(size_t num_pairs, char keys[][MAX_STRING_SIZE], int fd) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  pthread_rwlock_wrlock(&kvs_table->tablelock);

  int aux = 0;
  for (size_t i = 0; i < num_pairs; i++) {
    if (delete_pair(kvs_table, keys[i]) != 0) {
      if (!aux) {
        write_str(fd, "[");
        aux = 1;
      }
      char str[MAX_STRING_SIZE];
      snprintf(str, MAX_STRING_SIZE, "(%s,KVSMISSING)", keys[i]);
      write_str(fd, str);
    } else {
      notify_subscribers(keys[i], NULL);
    }
  }
  if (aux) {
    write_str(fd, "]\n");
  }

  pthread_rwlock_unlock(&kvs_table->tablelock);
  return 0;
}

void kvs_show(int fd) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return;
  }

  pthread_rwlock_rdlock(&kvs_table->tablelock);
  char aux[MAX_STRING_SIZE];

  for (int i = 0; i < TABLE_SIZE; i++) {
    KeyNode *keyNode = kvs_table->table[i]; // Get the next list head
    while (keyNode != NULL) {
      snprintf(aux, MAX_STRING_SIZE, "(%s, %s)\n", keyNode->key,
               keyNode->value);
      write_str(fd, aux);
      keyNode = keyNode->next; // Move to the next node of the list
    }
  }

  pthread_rwlock_unlock(&kvs_table->tablelock);
}

int kvs_backup(size_t num_backup, char *job_filename, char *directory) {
  pid_t pid;
  char bck_name[50];
  snprintf(bck_name, sizeof(bck_name), "%s/%s-%ld.bck", directory,
           strtok(job_filename, "."), num_backup);

  pthread_rwlock_rdlock(&kvs_table->tablelock);
  pid = fork();
  pthread_rwlock_unlock(&kvs_table->tablelock);
  if (pid == 0) {
    // functions used here have to be async signal safe, since this
    // fork happens in a multi thread context (see man fork)
    int fd = open(bck_name, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    for (int i = 0; i < TABLE_SIZE; i++) {
      KeyNode *keyNode = kvs_table->table[i]; // Get the next list head
      while (keyNode != NULL) {
        char aux[MAX_STRING_SIZE];
        aux[0] = '(';
        size_t num_bytes_copied = 1; // the "("
        // the - 1 are all to leave space for the '/0'
        num_bytes_copied += strn_memcpy(aux + num_bytes_copied, keyNode->key,
                                        MAX_STRING_SIZE - num_bytes_copied - 1);
        num_bytes_copied += strn_memcpy(aux + num_bytes_copied, ", ",
                                        MAX_STRING_SIZE - num_bytes_copied - 1);
        num_bytes_copied += strn_memcpy(aux + num_bytes_copied, keyNode->value,
                                        MAX_STRING_SIZE - num_bytes_copied - 1);
        num_bytes_copied += strn_memcpy(aux + num_bytes_copied, ")\n",
                                        MAX_STRING_SIZE - num_bytes_copied - 1);
        aux[num_bytes_copied] = '\0';
        write_str(fd, aux);
        keyNode = keyNode->next; // Move to the next node of the list
      }
    }
    exit(1);
  } else if (pid < 0) {
    return -1;
  }
  return 0;
}

void kvs_wait(unsigned int delay_ms) {
  struct timespec delay = delay_to_timespec(delay_ms);
  nanosleep(&delay, NULL);
}


// Initializes the subscription system
void kvs_subscribe_init(char *notif_pipe_name, client_t *client) {
    add_client(client);
    for (int i = 0; i < MAX_NUMBER_SUB; i++) {
        client->subscriptions[i].active = false;
        strcpy(client->subscriptions[i].notif_pipe, notif_pipe_name);
    }
}


int kvs_subscribe(const char* key, client_t *client) {

    pthread_rwlock_wrlock(&subscriptions_lock);

    // Check if the key exists
    char *result = read_pair(kvs_table, key);
    if (result == NULL) {
        pthread_rwlock_unlock(&subscriptions_lock);
        fprintf(stderr, "Key does not exist in the kvs table: %s\n", key);
        return 0; // Key does not exist
    }

    for (int i = 0; i < MAX_NUMBER_SUB; i++) {
        if (!client->subscriptions[i].active) {
            strcpy(client->subscriptions[i].key, key);
            client->subscriptions[i].active = true;
            pthread_rwlock_unlock(&subscriptions_lock);
            return 1;
        }
    }

    pthread_rwlock_unlock(&subscriptions_lock);

    return 0; // No more space for subscriptions

}


int kvs_unsubscribe(const char* key, client_t *client) {

    pthread_rwlock_wrlock(&subscriptions_lock);

    for (int i = 0; i < MAX_NUMBER_SUB; i++) {
        if (client->subscriptions[i].active && strcmp(client->subscriptions[i].key, key) == 0) {
            client->subscriptions[i].active = false;
            pthread_rwlock_unlock(&subscriptions_lock);
            return 0;
        }
    }

    remove_client(client);
    pthread_rwlock_unlock(&subscriptions_lock);

    return 1; // Subscription not found
}

int kvs_unsubscribe_all(client_t *client) {
    pthread_rwlock_wrlock(&subscriptions_lock);

    for (int i = 0; i < MAX_NUMBER_SUB; i++) {
        client->subscriptions[i].active = false;
    }

    remove_client(client);
    pthread_rwlock_unlock(&subscriptions_lock);

    return 0;
}

void notify_subscribers(const char* key, const char* value) {
    pthread_rwlock_rdlock(&clients_lock);
    pthread_rwlock_rdlock(&subscriptions_lock);

    for (int i = 0; i < MAX_SESSION_COUNT; i++) {
      client_t *client = clients[i];
      if (client == NULL) {
        continue;
      }

      for (int j = 0; j < MAX_NUMBER_SUB; j++) {
          if (client->subscriptions[j].active && strcmp(client->subscriptions[j].key, key) == 0) {
              int notif_fd = open(client->subscriptions[j].notif_pipe, O_WRONLY);
              if (notif_fd < 0) {
                  fprintf(stderr, "Failed to open notification pipe: %s\n", client->subscriptions[j].notif_pipe);
                  continue;
              }

              // Message type: (<key>, <value>)
              char message[42];
              if (value) {
                  snprintf(message, 42, "(%s,%s)", key, value);
              } else {
                  snprintf(message, 42, "(%s,DELETED)", key);
              }
              size_t offset = 0;
              size_t message_len = 42 * sizeof(char);
              char n_message[message_len];
              create_message(n_message, &offset, &message, message_len);


              if (write_all(notif_fd, &n_message, message_len) != 1) {
                  fprintf(stderr, "Failed to write to notification pipe: %s\n", client->subscriptions[j].notif_pipe);
                  close(notif_fd);
                  continue;
              }
              printf("Notified subscriber with message: %s\n", message);
              close(notif_fd);
          }
      }
    }
    pthread_rwlock_unlock(&subscriptions_lock);
    pthread_rwlock_unlock(&clients_lock);
}

// Adds a client to the clients array
void add_client(client_t *client) {
  pthread_rwlock_wrlock(&clients_lock);
  for (int i = 0; i < MAX_SESSION_COUNT; i++) {
    if (clients[i] == NULL) {
      clients[i] = client;
      break;
    }
  }
  pthread_rwlock_unlock(&clients_lock);
}

void remove_client(client_t *client) {
  pthread_rwlock_wrlock(&clients_lock);
  for (int i = 0; i < MAX_SESSION_COUNT; i++) {
    if (clients[i] == client) {
      clients[i] = NULL;
      break;
    }
  }
  pthread_rwlock_unlock(&clients_lock);
}