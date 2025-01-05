#include "api.h"
#include "src/common/constants.h"
#include "src/common/protocol.h"
#include "src/common/io.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>

static char req_pipe[41] = {0};
static char resp_pipe[41] = {0};
static char notification_pipe[41] = {0};
static int req_fd = -1;
static int resp_fd = -1;
static int notif_fd = -1;


int kvs_connect(char const* req_pipe_path, char const* resp_pipe_path, char const* server_pipe_path,
                char const* notif_pipe_path, int* notif_pipe) {
  // create pipes and connect

  printf("int* notif_pipe: %p\n", notif_pipe);

  strncpy(req_pipe, req_pipe_path, MAX_PIPE_PATH_LENGTH);
  strncpy(resp_pipe, resp_pipe_path, MAX_PIPE_PATH_LENGTH);
  strncpy(notification_pipe, notif_pipe_path, MAX_PIPE_PATH_LENGTH);

  // remove existing pipes and create new ones
  if ((unlink(req_pipe) != 0 && errno!= ENOENT) || mkfifo(req_pipe, 0640) < 0) {
    fprintf(stderr, "Error creating request pipe: %s\n", req_pipe);
    return 1;
  }
  if ((unlink(resp_pipe) != 0 && errno!= ENOENT) || mkfifo(resp_pipe, 0640) < 0) {
    fprintf(stderr, "Error creating response pipe: %s\n", resp_pipe);
    return 1;
  }
  if ((unlink(notification_pipe) != 0 && errno!= ENOENT) || mkfifo(notification_pipe, 0640) < 0) {
    fprintf(stderr, "Error creating notification pipe: %s\n", notification_pipe);
    return 1;
  }

  // Open server pipe for writing
  int server_fd = open(server_pipe_path, O_WRONLY);
  printf("Server pipe: %s\n", server_pipe_path);
  if (server_fd < 0) {
    fprintf(stderr, "Error opening server pipe: %s. Error type: %s\n", server_pipe_path, strerror(errno));
    return 1;
  }

  printf("Server pipe opened\n");
  printf("Request pipe: %s\n", req_pipe);
  printf("Response pipe: %s\n", resp_pipe);
  printf("Notification pipe: %s\n", notification_pipe);

  // Send connect message

  printf("Sending connect message to server\n");

  char op_code = OP_CODE_CONNECT;
  size_t offset = 0;
  size_t request_len = sizeof(char) + (MAX_PIPE_PATH_LENGTH * sizeof(char)) * 3;
  char request[request_len];
  memset(request, 0, request_len);

  // Create message: 
  // (char) OP_CODE=1 | 
  // (char[40]) nome do pipe do cliente (para pedidos) | 
  // (char[40]) nome do pipe do cliente (para respostas) | 
  // (char[40]) nome do pipe do cliente (para notificações)
  create_message(request, &offset, &op_code, sizeof(char));
  create_message(request, &offset, &req_pipe, MAX_PIPE_PATH_LENGTH * sizeof(char));
  create_message(request, &offset, &resp_pipe, MAX_PIPE_PATH_LENGTH * sizeof(char));
  create_message(request, &offset, &notification_pipe, MAX_PIPE_PATH_LENGTH * sizeof(char));

  if(write_all(server_fd, &request, request_len) != 1) {
    fprintf(stderr, "Failed to send connect message to server\n");
    close(server_fd);
    return 1;
  }
  close(server_fd);

  // Wait for response
  // Response type: (char) OP_CODE=1 | (char) result
  // We also have to open the response pipe first

  resp_fd = open(resp_pipe, O_RDONLY);
  if (resp_fd < 0) {
    fprintf(stderr, "Error opening response pipe: %s\n", resp_pipe);
    return 1;
  }
  printf("Response pipe opened\n");

  char response[2];
  if (read_all(resp_fd, response, 2, NULL) != 1) {
    fprintf(stderr, "Failed to read response from server\n");
    close(resp_fd);
    return 1;
  }

  if (response[0] != OP_CODE_CONNECT) {
    fprintf(stderr, "Unexpected response from server\n");
    close(resp_fd);
    return 1;
  }
  close(resp_fd);

  printf("Server returned %d for operation: CONNECT\n", response[1]);

  return 0;
}
 
int kvs_disconnect(void) {
  // close pipes and unlink pipe files
  char op_code = OP_CODE_DISCONNECT;
  size_t offset = 0;
  size_t request_len = sizeof(char);
  char request[request_len];
  memset(request, 0, request_len);

  // Create message:
  // (char) OP_CODE=2
  create_message(request, &offset, &op_code, sizeof(char));

  req_fd = open(req_pipe, O_WRONLY);
  if (req_fd < 0) {
    fprintf(stderr, "Error opening request pipe: %s\n", req_pipe);
    return 1;
  }
  if (write_all(req_fd, request, request_len) != 1) {
    fprintf(stderr, "Failed to send disconnect message to server\n");
    close(req_fd);
    return 1;
  }
  close(req_fd);

  // Recieve response
  char response[2];
  resp_fd = open(resp_pipe, O_RDONLY);
  if (resp_fd < 0) {
    fprintf(stderr, "Error opening response pipe: %s\n", resp_pipe);
    return 1;
  }
  if (read_all(resp_fd, response, 2, NULL) != 1) {
    fprintf(stderr, "Failed to read response from server\n");
    close(resp_fd);
    return 1;
  }
  if (response[0] != OP_CODE_DISCONNECT) {
    fprintf(stderr, "Unexpected response from server\n");
    close(resp_fd);
    return 1;
  }
  close(resp_fd);

  // Close and unlink pipes
  close(req_fd);
  close(resp_fd);
  close(notif_fd);
  unlink(req_pipe);
  unlink(resp_pipe);
  unlink(notification_pipe);

  printf("Server returned %d for operation: DISCONNECT\n", response[1]);

  return 0;
}

int kvs_subscribe(const char* key) {
  printf("Subscribing to key: %s\n", key);
  // send subscribe message to request pipe and wait for response in response pipe
  return 0;
}

int kvs_unsubscribe(const char* key) {
  printf("Unsubscribing from key: %s\n", key);
    // send unsubscribe message to request pipe and wait for response in response pipe
  return 0;
}


