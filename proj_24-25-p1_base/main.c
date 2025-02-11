#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <dirent.h> // NEW
#include <string.h> // NEW
#include <fcntl.h> // NEW

#include <limits.h> // NEW
#include <ctype.h> // NEW
#include <libgen.h> // NEW

#include <time.h>  // NEW
#include <sys/wait.h> // NEW

#include <pthread.h> // NEW


#include "constants.h"
#include "parser.h"
#include "operations.h"
#include "utils.h"


int active_backups = 0;
int MAX_BACKUPS = 0;





typedef struct {
    char file_path[MAX_FILE_SIZE];
    int max_backups;
    int backup_count;
} ThreadArgs;


void create_backup_file(const char *job_filename, int backup_counter, char *backup_path) {
    // Make mutable copies of job_filename for dirname() and basename()
    char job_copy[MAX_FILE_SIZE];
    char job_copy2[MAX_FILE_SIZE];

    strncpy(job_copy, job_filename, MAX_FILE_SIZE - 1);
    job_copy[MAX_FILE_SIZE - 1] = '\0'; // Ensure null-termination
    strncpy(job_copy2, job_filename, MAX_FILE_SIZE - 1);
    job_copy2[MAX_FILE_SIZE - 1] = '\0'; // Ensure null-termination

    // Extract directory and base name
    char *dir = dirname(job_copy);    // Modifies job_copy
    char *base = basename(job_copy2); // Modifies job_copy2

    // Remove the ".job" extension from the base name
    char *dot = strrchr(base, '.');
    if (dot && strcmp(dot, ".job") == 0) {
        *dot = '\0';  // Truncate the ".job" part
    }

    // Build the backup_path
    char counter_str[16];
    int len = 0;
    do {
        counter_str[len++] = (char)('0' + (backup_counter % 10));
        backup_counter /= 10;
    } while (backup_counter > 0);
    counter_str[len] = '\0';

    // Reverse counter string for correct order
    for (int i = 0; i < len / 2; i++) {
        char temp = counter_str[i];
        counter_str[i] = counter_str[len - i - 1];
        counter_str[len - i - 1] = temp;
    }

    strcpy(backup_path, dir);
    strcat(backup_path, "/");
    strcat(backup_path, base);
    strcat(backup_path, "-");
    strcat(backup_path, counter_str);
    strcat(backup_path, ".bck");
}




void process_jobs_file(int file_fd, int out_fd, const char *job_filename, int *backup_count) {

  while (1) {

    enum Command nLine = get_next(file_fd);
    char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    unsigned int delay;
    size_t num_pairs;

    switch (nLine) {
      case CMD_WRITE:
        num_pairs = parse_write(file_fd, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
        if (num_pairs == 0) {
          fprintf(stderr, "Invalid WRITE command. See HELP for usage\n");
          continue;
        }

        if (kvs_write(num_pairs, keys, values)) {
          fprintf(stderr, "WRITE: Failed to write pair\n");
        }

        break;

      case CMD_READ:
        num_pairs = parse_read_delete(file_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

        if (num_pairs == 0) {
          fprintf(stderr, "Invalid READ command. See HELP for usage\n");
          continue;
        }
        
        if (kvs_read(num_pairs, keys, out_fd)) {
          fprintf(stderr, "READ: Failed to read pair\n");
        }
        break;

      case CMD_DELETE:
        num_pairs = parse_read_delete(file_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

        if (num_pairs == 0) {
          fprintf(stderr, "Invalid DELETE command. See HELP for usage\n");
          continue;
        }

        if (kvs_delete(num_pairs, keys, out_fd)) {
          fprintf(stderr, "DELETE: Failed to delete pair\n");
        }
        break;

      case CMD_SHOW:
        kvs_show(out_fd);
        break;

      case CMD_WAIT:
        if (parse_wait(file_fd, &delay, NULL) == -1) {
          fprintf(stderr, "Invalid WAIT command. See HELP for usage\n");
          continue;
        }

        if (delay > 0) {
          write(out_fd, "Waiting...\n", strlen("Waiting...\n"));
          kvs_wait(delay);
        }
        break;

      case CMD_BACKUP:
        
        // Wait until there's an available slot for a backup process
        while (active_backups >= MAX_BACKUPS) {
            printf("Waiting for backup to finish...\n");
            int status;
            pid_t finished_pid = wait(&status);
            if (finished_pid > 0) {
                active_backups--;
                printf("Backup process %d finished\n", finished_pid);
            }
        }

        (*backup_count)++;
        char backup_file[MAX_FILE_SIZE];
        create_backup_file(job_filename, *backup_count, backup_file);

        pid_t pid = fork();
        if (pid == 0) { // Child process
            printf("Backup process started for %s\n", backup_file);

            if (kvs_backup(backup_file) != 0) {
                fprintf(stderr, "Backup failed.\n");
            }

            _exit(0); // Properly exit child process
        } else if (pid > 0) { // Parent process
            active_backups++;
        } else { // Fork failed
            fprintf(stderr, "Failed to create backup process.\n");
        }
        break;



      case CMD_INVALID:
        fprintf(stderr, "Invalid command. See HELP for usage\n");
        break;

      case CMD_HELP:
        printf( 
            "Available commands:\n"
            "  WRITE [(key,value)(key2,value2),...]\n"
            "  READ [key,key2,...]\n"
            "  DELETE [key,key2,...]\n"
            "  SHOW\n"
            "  WAIT <delay_ms>\n"
            "  BACKUP\n"
            "  HELP\n"
        );

        break;
        
      case CMD_EMPTY:
        break;

      case EOC:
        return;
    }
  }
}

void *process_file_thread(void *arg) {
    ThreadArgs *args = (ThreadArgs *)arg;
    const char *file_path = args->file_path;
    MAX_BACKUPS = args->max_backups;

    char out_file_name[MAX_OUT_FILE_SIZE];
    char file_base[MAX_FILE_SIZE];

    strncpy(file_base, file_path, sizeof(file_base));
    char *dot = strrchr(file_base, '.');
    if (dot && strcmp(dot, ".job") == 0) {
        *dot = '\0';
    }

    sprintf(out_file_name, "%s.out", file_base);
    int out_fd = open(out_file_name, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
    if (out_fd == -1) {
        fprintf(stderr, "Failed to open output file for %s\n", file_path);
        return NULL;
    }

    int file_fd = open(file_path, O_RDONLY);
    if (file_fd == -1) {
        fprintf(stderr, "Failed to open file %s\n", file_path);
        close(out_fd);
        return NULL;
    }

    process_jobs_file(file_fd, out_fd, file_path, &args->backup_count);

    close(file_fd);
    close(out_fd);
    return NULL;
}



int main(int argc, char *argv[]) {

    if (argc < 4) {
        fprintf(stderr, "Usage: %s <directory_path> <max_backups> <max_threads>\n", argv[0]);
        return 1;
    }

    const char *directory = argv[1];
    int max_backups = atoi(argv[2]);
    int max_threads = atoi(argv[3]);

    if (kvs_init()) {
        fprintf(stderr, "Failed to initialize KVS\n");
        return 1;
    }

    DIR *d;
    struct dirent *dir;
    pthread_t threads[max_threads];
    ThreadArgs thread_args[max_threads];
    int thread_count = 0;

    char job_files[1024][MAX_FILE_SIZE]; // Array to store .job file paths
    int job_count = 0;


    d = opendir(directory);
    if (d) {
        while ((dir = readdir(d)) != NULL) {
            if (is_job_file(dir->d_name)) {
                snprintf(job_files[job_count], MAX_FILE_SIZE, "%s/%s", directory, dir->d_name);
                job_count++;
            }
        }
        closedir(d);
    } else {
        fprintf(stderr, "Failed to open directory %s\n", directory);
        return 1;
    }

    // Process each job file
    for (int i = 0; i < job_count; i++) {
        printf("Scheduling processing for job file: %s\n", job_files[i]);

        ThreadArgs *args = &thread_args[thread_count];
        strncpy(args->file_path, job_files[i], MAX_FILE_SIZE);
        args->max_backups = max_backups;
        args->backup_count = 0;

        if (pthread_create(&threads[thread_count], NULL, process_file_thread, &thread_args[thread_count]) != 0) {
            fprintf(stderr, "Thread creation failed\n");
            continue;
        }
        thread_count++;

        if (thread_count == max_threads) {
            for (int j = 0; j < thread_count; j++) {
                int error_check = pthread_join(threads[j], NULL);
                if (error_check != 0) {
                    fprintf(stderr, "Thread join failed.\n");
                }
            }
            thread_count = 0;
        }
    }

    // Join any remaining threads
    for (int i = 0; i < thread_count; i++) {
        int error_check = pthread_join(threads[i], NULL);
        if (error_check != 0) {
            fprintf(stderr, "Thread join failed.\n");
        }
    }

    if (kvs_terminate()) {
        fprintf(stderr, "Failed to terminate KVS\n");
        return 1;
    }

    return 0;
}