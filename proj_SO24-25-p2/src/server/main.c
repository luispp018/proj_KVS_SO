#include <unistd.h>
#include <dirent.h>
#include <fcntl.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <stdio.h>
#include <errno.h>

#include "constants.h"
#include "parser.h"
#include "operations.h"
#include "io.h"
#include "pthread.h"

#include "../common/constants.h"
#include "../common/io.h"
#include "../common/protocol.h"

struct SharedData {
  DIR* dir;
  char* dir_name;
  pthread_mutex_t directory_mutex;
};

pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t n_current_backups_lock = PTHREAD_MUTEX_INITIALIZER;

size_t active_backups = 0;     // Number of active backups
size_t max_backups;            // Maximum allowed simultaneous backups
size_t max_threads;            // Maximum allowed simultaneous threads
char* jobs_directory = NULL;

char server_pipename[256] = "/tmp/";
int server_fd;

int filter_job_files(const struct dirent* entry) {
    const char* dot = strrchr(entry->d_name, '.');
    if (dot != NULL && strcmp(dot, ".job") == 0) {
        return 1;  // Keep this file (it has the .job extension)
    }
    return 0;
}

static int entry_files(const char* dir, struct dirent* entry, char* in_path, char* out_path) {
  const char* dot = strrchr(entry->d_name, '.');
  if (dot == NULL || dot == entry->d_name || strlen(dot) != 4 || strcmp(dot, ".job")) {
    return 1;
  }

  if (strlen(entry->d_name) + strlen(dir) + 2 > MAX_JOB_FILE_NAME_SIZE) {
    fprintf(stderr, "%s/%s\n", dir, entry->d_name);
    return 1;
  }

  strcpy(in_path, dir);
  strcat(in_path, "/");
  strcat(in_path, entry->d_name);

  strcpy(out_path, in_path);
  strcpy(strrchr(out_path, '.'), ".out");

  return 0;
}

static int run_job(int in_fd, int out_fd, char* filename) {
  size_t file_backups = 0;
  while (1) {
    char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    unsigned int delay;
    size_t num_pairs;

    switch (get_next(in_fd)) {
      case CMD_WRITE:
        num_pairs = parse_write(in_fd, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
        if (num_pairs == 0) {
          write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (kvs_write(num_pairs, keys, values)) {
          write_str(STDERR_FILENO, "Failed to write pair\n");
        }
        break;

      case CMD_READ:
        num_pairs = parse_read_delete(in_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

        if (num_pairs == 0) {
          write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (kvs_read(num_pairs, keys, out_fd)) {
          write_str(STDERR_FILENO, "Failed to read pair\n");
        }
        break;

      case CMD_DELETE:
        num_pairs = parse_read_delete(in_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

        if (num_pairs == 0) {
          write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (kvs_delete(num_pairs, keys, out_fd)) {
          write_str(STDERR_FILENO, "Failed to delete pair\n");
        }
        break;

      case CMD_SHOW:
        kvs_show(out_fd);
        break;

      case CMD_WAIT:
        if (parse_wait(in_fd, &delay, NULL) == -1) {
          write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (delay > 0) {
          printf("Waiting %d seconds\n", delay / 1000);
          kvs_wait(delay);
        }
        break;

      case CMD_BACKUP:
        pthread_mutex_lock(&n_current_backups_lock);
        if (active_backups >= max_backups) {
          wait(NULL);
        } else {
          active_backups++;
        }
        pthread_mutex_unlock(&n_current_backups_lock);
        int aux = kvs_backup(++file_backups, filename, jobs_directory);

        if (aux < 0) {
            write_str(STDERR_FILENO, "Failed to do backup\n");
        } else if (aux == 1) {
          return 1;
        }
        break;

      case CMD_INVALID:
        write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
        break;

      case CMD_HELP:
        write_str(STDOUT_FILENO,
            "Available commands:\n"
            "  WRITE [(key,value)(key2,value2),...]\n"
            "  READ [key,key2,...]\n"
            "  DELETE [key,key2,...]\n"
            "  SHOW\n"
            "  WAIT <delay_ms>\n"
            "  BACKUP\n" // Not implemented
            "  HELP\n");

        break;

      case CMD_EMPTY:
        break;

      case EOC:
        printf("EOF\n");
        return 0;
    }
  }
}

//frees arguments
static void* get_file(void* arguments) {
  struct SharedData* thread_data = (struct SharedData*) arguments;
  DIR* dir = thread_data->dir;
  char* dir_name = thread_data->dir_name;

  if (pthread_mutex_lock(&thread_data->directory_mutex) != 0) {
    fprintf(stderr, "Thread failed to lock directory_mutex\n");
    return NULL;
  }

  struct dirent* entry;
  char in_path[MAX_JOB_FILE_NAME_SIZE], out_path[MAX_JOB_FILE_NAME_SIZE];
  while ((entry = readdir(dir)) != NULL) {
    if (entry_files(dir_name, entry, in_path, out_path)) {
      continue;
    }

    if (pthread_mutex_unlock(&thread_data->directory_mutex) != 0) {
      fprintf(stderr, "Thread failed to unlock directory_mutex\n");
      return NULL;
    }

    int in_fd = open(in_path, O_RDONLY);
    if (in_fd == -1) {
      write_str(STDERR_FILENO, "Failed to open input file: ");
      write_str(STDERR_FILENO, in_path);
      write_str(STDERR_FILENO, "\n");
      pthread_exit(NULL);
    }

    int out_fd = open(out_path, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if (out_fd == -1) {
      write_str(STDERR_FILENO, "Failed to open output file: ");
      write_str(STDERR_FILENO, out_path);
      write_str(STDERR_FILENO, "\n");
      pthread_exit(NULL);
    }

    int out = run_job(in_fd, out_fd, entry->d_name);

    close(in_fd);
    close(out_fd);

    if (out) {
      if (closedir(dir) == -1) {
        fprintf(stderr, "Failed to close directory\n");
        return 0;
      }

      exit(0);
    }

    if (pthread_mutex_lock(&thread_data->directory_mutex) != 0) {
      fprintf(stderr, "Thread failed to lock directory_mutex\n");
      return NULL;
    }
  }

  if (pthread_mutex_unlock(&thread_data->directory_mutex) != 0) {
    fprintf(stderr, "Thread failed to unlock directory_mutex\n");
    return NULL;
  }

  pthread_exit(NULL);
}


static void dispatch_threads(DIR* dir) {
  pthread_t* threads = malloc(max_threads * sizeof(pthread_t));

  if (threads == NULL) {
    fprintf(stderr, "Failed to allocate memory for threads\n");
    return;
  }

  struct SharedData thread_data = {dir, jobs_directory, PTHREAD_MUTEX_INITIALIZER};


  for (size_t i = 0; i < max_threads; i++) {
    if (pthread_create(&threads[i], NULL, get_file, (void*)&thread_data) != 0) {
      fprintf(stderr, "Failed to create thread %zu\n", i);
      pthread_mutex_destroy(&thread_data.directory_mutex);
      free(threads);
      return;
    }
  }

  // ler do FIFO de registo
  // ler o opcode do pedido
  // fazer switch case para cada opcode


  for (unsigned int i = 0; i < max_threads; i++) {
    if (pthread_join(threads[i], NULL) != 0) {
      fprintf(stderr, "Failed to join thread %u\n", i);
      pthread_mutex_destroy(&thread_data.directory_mutex);
      free(threads);
      return;
    }
  }

  if (pthread_mutex_destroy(&thread_data.directory_mutex) != 0) {
    fprintf(stderr, "Failed to destroy directory_mutex\n");
  }

  free(threads);
}


void send_answer(char *response_pipename, int status, char OP_CODE) {
  int response_fd = open(response_pipename, O_WRONLY);
  if (response_fd < 0) {
    fprintf(stderr, "Failed to open response FIFO: %s\n", strerror(errno));
    return;
  }

  size_t offset = 0;
  size_t message_size = 2;
  char opcode = OP_CODE;
  char r_status = (char)status;
  char response[2];
  create_message(response, &offset, &opcode, sizeof(char));
  create_message(response, &offset, &r_status, sizeof(char));
  if (write_all(response_fd, response, message_size) == -1) {
    fprintf(stderr, "Failed to write to response FIFO: %s\n", strerror(errno));
  }
  close(response_fd);
  
}



void handle_requests(client_t *client) {
  int request_fd = open(client->request_pipename, O_RDONLY);
  if (request_fd < 0) {
    fprintf(stderr, "Failed to open client request pipe: %s\n", client->request_pipename);
    free(client);
    return;
  }

  while (1) {
    char opcode;
    if (read_all(request_fd, &opcode, sizeof(char), NULL) == 0) {
      fprintf(stderr, "Failed to read opcode from request pipe\n");
      close(request_fd);
      free(client);
      return;
    }

    switch (opcode) {
      case OP_CODE_DISCONNECT: {
        kvs_unsubscribe_all();
        send_answer(client->response_pipename, 0, OP_CODE_DISCONNECT);
        close(request_fd);
        free(client);
        printf("Client disconnected.\n");
        return;
      }

      case OP_CODE_SUBSCRIBE: {

        char key[41];
        if (read_all(request_fd, key, 41, NULL) == 0) {
          fprintf(stderr, "Failed to read key from request FIFO\n");
          close(request_fd);
          free(client);
          return;
        }
        if (!client->has_subscribed) {
          kvs_subscribe_init(client->notification_pipename);
          client->has_subscribed = true;
        }
        int sub_result = kvs_subscribe(key);
        send_answer(client->response_pipename, sub_result, OP_CODE_SUBSCRIBE);
        break;
      }

      case OP_CODE_UNSUBSCRIBE: {

        char key[41];
        if (read_all(request_fd, key, 41, NULL) == 0) {
          fprintf(stderr, "Failed to read key from request FIFO\n");
          close(request_fd);
          free(client);
          return;
        }
        int unsub_result = kvs_unsubscribe(key);
        send_answer(client->response_pipename, unsub_result, OP_CODE_UNSUBSCRIBE);
        break;
      }

      default:
        fprintf(stderr, "Unknown opcode: %d\n", opcode);
        break;
    }
  }
}



int new_client_connection() {
  printf("New client connection\n");
  client_t *client = (client_t *)malloc(sizeof(client_t));
  int error_status = 0;

  char client_request_pipename[MAX_PIPE_PATH_LENGTH];
  if(read_all(server_fd, client_request_pipename, MAX_PIPE_PATH_LENGTH * sizeof(char), NULL) == -1){
    fprintf(stderr, "Failed to read from server FIFO\n");
    error_status = 1;
  }

  char client_response_pipename[MAX_PIPE_PATH_LENGTH];
  if(read_all(server_fd, client_response_pipename, MAX_PIPE_PATH_LENGTH * sizeof(char), NULL) == -1){
    fprintf(stderr, "Failed to read from server FIFO\n");
    error_status = 1;
  }

  char client_notification_pipename[MAX_PIPE_PATH_LENGTH];
  if(read_all(server_fd, client_notification_pipename, MAX_PIPE_PATH_LENGTH * sizeof(char), NULL) == -1){
    fprintf(stderr, "Failed to read from server FIFO\n");
    error_status = 1;
  }

  if (error_status) {
    send_answer(client->response_pipename, 1, OP_CODE_CONNECT);
    free(client);
    return 1;

  } else {
    strcpy(client->request_pipename, client_request_pipename);
    strcpy(client->response_pipename, client_response_pipename);
    strcpy(client->notification_pipename, client_notification_pipename);
    client->has_subscribed = false;
    send_answer(client->response_pipename, 0, OP_CODE_CONNECT);
    handle_requests(client);
    return 0;
  }

}

void *server_fifo_handler(){
  printf("Server FIFO handler\n");
  while (1) {

    server_fd = open(server_pipename, O_RDONLY);
    if (server_fd < 0) {
      fprintf(stderr, "Failed to open server FIFO: %s\n", strerror(errno));
      continue;
    }
    char opcode;
    if(read_all(server_fd, &opcode, sizeof(char), NULL) == 0){
      continue; // it could not get an opcode, so it will try again
    }

    if(opcode != OP_CODE_CONNECT){
      printf("Failed to read from server FIFO: Invalid opcode\n");
      continue; // it is not a connect opcode, so it will try again
    }

    if(new_client_connection(NULL) == 1){
      fprintf(stderr, "Failed to create new client connection\n");
      unlink(server_pipename);
    }

    printf("Client is now disconnected.\n");
  }
  close(server_fd);
  return NULL;
}


int main(int argc, char** argv) {
  if (argc < 4) {
    write_str(STDERR_FILENO, "Usage: ");
    write_str(STDERR_FILENO, argv[0]);
    write_str(STDERR_FILENO, " <jobs_dir>");
		write_str(STDERR_FILENO, " <max_threads>");
		write_str(STDERR_FILENO, " <max_backups> \n");
    return 1;
  }

  jobs_directory = argv[1];

  char* endptr;
  max_backups = strtoul(argv[3], &endptr, 10);

  if (*endptr != '\0') {
    fprintf(stderr, "Invalid max_proc value\n");
    return 1;
  }

  max_threads = strtoul(argv[2], &endptr, 10);

  if (*endptr != '\0') {
    fprintf(stderr, "Invalid max_threads value\n");
    return 1;
  }

	if (max_backups <= 0) {
		write_str(STDERR_FILENO, "Invalid number of backups\n");
		return 0;
	}

	if (max_threads <= 0) {
		write_str(STDERR_FILENO, "Invalid number of threads\n");
		return 0;
	}

  if (kvs_init()) {
    write_str(STDERR_FILENO, "Failed to initialize KVS\n");
    return 1;
  }

  DIR* dir = opendir(argv[1]);
  if (dir == NULL) {
    fprintf(stderr, "Failed to open directory: %s\n", argv[1]);
    return 0;
  }


  // SERVER FIFO HANDLING
  
  strncat(server_pipename, argv[4], 256 - strlen(server_pipename) - 1);
  printf("Server pipename: %s\n", server_pipename);
  // Create server FIFO
  if ((unlink(server_pipename) != 0 && errno != ENOENT) || mkfifo(server_pipename, 0640) < 0) {
    fprintf(stderr, "Failed to create server FIFO: %s\n", strerror(errno));
    kvs_terminate();
    return 1;
  }
  fprintf(stdout, "The server has been initialized with pipename: %s\n", server_pipename);

  // The handling of the server FIFO should be done in a separate thread
  pthread_t server_thread;
  if (pthread_create(&server_thread, NULL, server_fifo_handler, NULL) != 0) {
    fprintf(stderr, "Failed to create server thread\n");
    kvs_terminate();
    return 1;
  }

  dispatch_threads(dir);

  if (pthread_join(server_thread, NULL) != 0) {
    fprintf(stderr, "Failed to join server thread\n");
    kvs_terminate();
    return 1;
  }

  if (closedir(dir) == -1) {
    fprintf(stderr, "Failed to close directory\n");
    return 0;
  }

  while (active_backups > 0) {
    wait(NULL);
    active_backups--;
  }

  kvs_terminate();
  close(server_fd);
  unlink(server_pipename);

  return 0;
}
