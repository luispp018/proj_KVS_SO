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
    add_client(client); // Add client to the clients array
    for (int i = 0; i < MAX_NUMBER_SUB; i++) {  // Initialize the subscriptions
        client->subscriptions[i].active = false;  // Set all subscriptions to inactive
        strcpy(client->subscriptions[i].notif_pipe, notif_pipe_name); // Set the notification pipe name
    }
}


int kvs_subscribe(const char* key, client_t *client) {

    pthread_rwlock_wrlock(&subscriptions_lock); // Lock the subscriptions

    // Check if the key exists
    char *result = read_pair(kvs_table, key); // Read the key from the kvs table
    if (result == NULL) { // Verify if the key exists
        pthread_rwlock_unlock(&subscriptions_lock); // Unlock the subscriptions
        fprintf(stderr, "Key does not exist in the kvs table: %s\n", key);  
        return 0; // Key does not exist
    }

    for (int i = 0; i < MAX_NUMBER_SUB; i++) {  // Iterate over the subscriptions
        if (!client->subscriptions[i].active) { // Check if the subscription is inactive
            strcpy(client->subscriptions[i].key, key);  // Set the key
            client->subscriptions[i].active = true; // Set the subscription to active
            pthread_rwlock_unlock(&subscriptions_lock); // Unlock the subscriptions
            return 1; // Subscription successful
        } else if (strcmp(client->subscriptions[i].key, key) == 0) {  // Check if the key already exists
            pthread_rwlock_unlock(&subscriptions_lock); // Unlock the subscriptions
            return 0; // Subscription already exists
        }
    }

    pthread_rwlock_unlock(&subscriptions_lock); // Unlock the subscriptions

    return 0; // No more space for subscriptions

}


int kvs_unsubscribe(const char* key, client_t *client) {

    pthread_rwlock_wrlock(&subscriptions_lock); // Lock the subscriptions

    for (int i = 0; i < MAX_NUMBER_SUB; i++) {  // Iterate over the subscriptions
        if (client->subscriptions[i].active && strcmp(client->subscriptions[i].key, key) == 0) {  // Check if the subscription is active and the key matches
            client->subscriptions[i].active = false;  // Set the subscription to inactive
            pthread_rwlock_unlock(&subscriptions_lock); // Unlock the subscriptions
            return 0; // Subscription removed
        }
    }

    remove_client(client);  // Remove the client from the clients array
    pthread_rwlock_unlock(&subscriptions_lock); // Unlock the subscriptions

    return 1; // Subscription not found
}

int kvs_unsubscribe_all(client_t *client) {
    pthread_rwlock_wrlock(&subscriptions_lock); // Lock the subscriptions

    for (int i = 0; i < MAX_NUMBER_SUB; i++) {  // Iterate over the subscriptions
        client->subscriptions[i].active = false;  // Set the subscription to inactive
    }

    remove_client(client);  // Remove the client from the clients array
    pthread_rwlock_unlock(&subscriptions_lock); // Unlock the subscriptions

    return 0; // All subscriptions removed
} 

void notify_subscribers(const char* key, const char* value) {
    pthread_rwlock_rdlock(&clients_lock); // Lock the clients
    pthread_rwlock_rdlock(&subscriptions_lock); // Lock the subscriptions

    for (int i = 0; i < MAX_SESSION_COUNT; i++) { // Iterate over the clients
      client_t *client = clients[i];  // Get the client
      if (client == NULL) { // Check if the client is null
        continue; // Continue to the next client
      }

      for (int j = 0; j < MAX_NUMBER_SUB; j++) {   // Iterate over the subscriptions
          if (client->subscriptions[j].active && strcmp(client->subscriptions[j].key, key) == 0) {  // Check if the subscription is active and the key matches
              int notif_fd = open(client->subscriptions[j].notif_pipe, O_WRONLY); // Open the notification pipe
              if (notif_fd < 0) { // Check if the notification pipe was opened
                  fprintf(stderr, "Failed to open notification pipe: %s\n", client->subscriptions[j].notif_pipe); 
                  continue; // Continue to the next subscription
              }

              // Message type: (<key>, <value>)
              char message[42];
              if (value) {  // Check if the value exists
                  snprintf(message, 42, "(%s,%s)", key, value); // Set the message
              } else {  // Value does not exist
                  snprintf(message, 42, "(%s,DELETED)", key); // Set the message
              }
              size_t offset = 0;  // Set the offset
              size_t message_len = 42 * sizeof(char); // Set the message length
              char n_message[message_len];  // Set the notification message
              create_message(n_message, &offset, &message, message_len);  // Create the message


              if (write_all(notif_fd, &n_message, message_len) != 1) {  // Write the message to the notification pipe
                  fprintf(stderr, "Failed to write to notification pipe: %s\n", client->subscriptions[j].notif_pipe); 
                  close(notif_fd);  // Close the notification pipe
                  continue; // Continue to the next subscription
              }
              printf("Notified subscriber with message: %s\n", message);  
              close(notif_fd);  // Close the notification pipe
          }
      }
    }
    pthread_rwlock_unlock(&subscriptions_lock); // Unlock the subscriptions
    pthread_rwlock_unlock(&clients_lock); // Unlock the clients
}

// Adds a client to the clients array
void add_client(client_t *client) {
  pthread_rwlock_wrlock(&clients_lock); // Lock the clients
  for (int i = 0; i < MAX_SESSION_COUNT; i++) { // Iterate over the clients
    if (clients[i] == NULL) { // Check if the client is null
      clients[i] = client;  // Set the client
      break;  // Break the loop
    }
  }
  pthread_rwlock_unlock(&clients_lock); // Unlock the clients
}

void remove_client(client_t *client) {
  pthread_rwlock_wrlock(&clients_lock); // Lock the clients
  for (int i = 0; i < MAX_SESSION_COUNT; i++) {  // Iterate over the clients
    if (clients[i] == client) { // Check if the client matches
      clients[i] = NULL;  // Set the client to null
      break;  // Break the loop
    } 
  }
  pthread_rwlock_unlock(&clients_lock); // Unlock the clients
}