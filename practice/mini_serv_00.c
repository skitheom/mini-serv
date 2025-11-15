
#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

static const char *ERR_ARGC = "Wrong number of arguments\n";
static const char *ERR_FATAL = "Fatal error\n";
static const char *MSG_CLIENT_ARRIVAL = "server: client %d just arrived\n";
static const char *MSG_CLIENT_LEAVE = "server: client %d just left\n";
static const char *MSG_PREFIX = "client %d: ";

static fd_set g_main_fds;
static int g_fdmax;
static int g_listener;
static struct s_client *g_clients;

typedef struct s_client {
  int id;
  char *buf; // 書きたいデータをためる
} t_client;

////////////////////////////////////////////////////////////
// Utility

void err_msg(const char *msg) { write(2, msg, strlen(msg)); }

void fatal(void) {
  err_msg(ERR_FATAL);
  exit(1);
}

void clean_up() {
  for (int fd = 0; fd < FD_SETSIZE; ++fd) {
    if (g_clients[fd].id != -1) {
      close(fd);
    }
    if (g_clients[fd].buf) {
      free(g_clients[fd].buf);
      g_clients[fd].buf = 0;
    }
  }
  free(g_clients);
  close(g_listener);
}

char *xcalloc(size_t count, size_t size) {
  if (count == 0 || size == 0) {
    clean_up();
    fatal();
  }
  char *ptr = calloc(count, size);
  if (!ptr) {
    clean_up();
    fatal();
  }
  return ptr;
}

uint16_t ft_htons(uint16_t x) { return (x >> 8) | (x << 8); }

uint32_t ft_htonl(uint32_t x) {
  return ((x >> 24) & 0x000000FF) | ((x >> 8) & 0x0000FF00) |
         ((x << 8) & 0x00FF0000) | ((x << 24) & 0xFF000000);
}

////////////////////////////////////////////////////////////
// fd_set Management

void monitor(int fd) {
  FD_SET(fd, &g_main_fds);
  g_fdmax = (g_fdmax > fd) ? g_fdmax : fd;
}

void unmonitor(int fd) {
  FD_CLR(fd, &g_main_fds);
  if (fd != g_fdmax) {
    return;
  }
  g_fdmax = -1;
  for (int tmp_fd = fd - 1; tmp_fd >= 0; --tmp_fd) {
    if (FD_ISSET(tmp_fd, &g_main_fds)) {
      g_fdmax = tmp_fd;
      break;
    }
  }
}

////////////////////////////////////////////////////////////
//  Messaging

void broadcast(const char *msg, int client_fd) {
  if (!msg || client_fd < 0 || client_fd >= FD_SETSIZE) {
    return;
  }
  char *new_msg = (char *)xcalloc(1, strlen(msg) + 11);
  int new_len = sprintf(new_msg, msg, g_clients[client_fd].id);

  for (int fd = 0; fd < g_fdmax + 1; ++fd) {
    if (fd == g_listener || fd == client_fd || g_clients[fd].id == -1) {
      continue;
    }
    if (!g_clients[fd].buf) {
      g_clients[fd].buf = (char *)xcalloc(1, new_len + 1);
      strcpy(g_clients[fd].buf, new_msg);
    } else {
      int joined_len = strlen(g_clients[fd].buf) + new_len;
      char *joined_buf = (char *)xcalloc(1, joined_len + 1);
      strcpy(joined_buf, g_clients[fd].buf);
      strcat(joined_buf, new_msg);
      free(g_clients[fd].buf);
      g_clients[fd].buf = joined_buf;
    }
  }
  free(new_msg);
}

void broadcast_client_msg(const char *msg, int client_fd) {
  if (!msg || client_fd < 0 || client_fd >= FD_SETSIZE) {
    return;
  }
  char *new_msg = (char *)xcalloc(1, strlen(MSG_PREFIX) + strlen(msg) + 11);
  sprintf(new_msg, MSG_PREFIX, g_clients[client_fd].id);
  strcat(new_msg, msg);
  int new_len = strlen(new_msg);

  for (int fd = 0; fd < g_fdmax + 1; ++fd) {
    if (fd == g_listener || fd == client_fd || g_clients[fd].id == -1) {
      continue;
    }
    if (!g_clients[fd].buf) {
      g_clients[fd].buf = (char *)xcalloc(1, new_len + 1);
      strcpy(g_clients[fd].buf, new_msg);
    } else {
      int joined_len = strlen(g_clients[fd].buf) + new_len;
      char *joined_buf = (char *)xcalloc(1, joined_len + 1);
      strcpy(joined_buf, g_clients[fd].buf);
      strcat(joined_buf, new_msg);
      free(g_clients[fd].buf);
      g_clients[fd].buf = joined_buf;
    }
  }
  free(new_msg);
}

////////////////////////////////////////////////////////////
//  Client Management

void add_to_clients(int client_fd) {
  static int next_client_id = 0;
  if (client_fd < 0 || client_fd >= FD_SETSIZE) {
    return;
  }
  g_clients[client_fd].id = next_client_id++;
  g_clients[client_fd].buf = 0;
  broadcast(MSG_CLIENT_ARRIVAL, client_fd);
}

void remove_from_client(int client_fd) {
  if (client_fd < 0 || client_fd >= FD_SETSIZE) {
    return;
  }
  broadcast(MSG_CLIENT_LEAVE, client_fd);
  g_clients[client_fd].id = -1;
  if (g_clients[client_fd].buf) {
    free(g_clients[client_fd].buf);
    g_clients[client_fd].buf = 0;
  }
  close(client_fd);
}

void send_to_client(int fd) {
  if (fd < 0 || fd >= FD_SETSIZE || g_clients[fd].id == -1 ||
      !g_clients[fd].buf) {
    return;
  }
  ssize_t buf_len = strlen(g_clients[fd].buf);
  ssize_t bytes_sent = send(fd, g_clients[fd].buf, buf_len, 0);
  if (bytes_sent <= 0) {
    remove_from_client(fd);
    unmonitor(fd);
    return;
  }
  if (bytes_sent >= buf_len) { // or rv == buf_len ?
    free(g_clients[fd].buf);
    g_clients[fd].buf = 0;
    return;
  }

  // partial write
  if (bytes_sent < buf_len) {
    char *buf = (char *)xcalloc(1, buf_len - bytes_sent + 1);
    strcpy(buf, g_clients[fd].buf + bytes_sent);
    free(g_clients[fd].buf);
    g_clients[fd].buf = buf;
  }
}

void read_from_client(int client_fd) {
  char buf[1024] = {0};
  ssize_t bytes_read = recv(client_fd, buf, sizeof(buf), 0);
  if (bytes_read == -1) {
    if (errno == EAGAIN || errno == EINTR) {
      return;
    }
    remove_from_client(client_fd);
    unmonitor(client_fd);
    return;
  }
  if (bytes_read == 0) {
    remove_from_client(client_fd);
    unmonitor(client_fd);
    return;
  }
  broadcast_client_msg(buf, client_fd);
}

////////////////////////////////////////////////////////////
//  Connection

void accept_client() {
  struct sockaddr_storage client_addr;
  socklen_t addrlen = sizeof(client_addr);
  int client_fd = accept(g_listener, (struct sockaddr *)&client_addr, &addrlen);
  if (client_fd < 0) {
    clean_up();
    fatal();
  }
  if (client_fd >= FD_SETSIZE) {
    close(client_fd);
    return;
  }
  add_to_clients(client_fd);
  monitor(client_fd);
}

int launch_listener(uint16_t portnum) {

  struct sockaddr_in servaddr;

  memset(&servaddr, 0, sizeof(servaddr));
  servaddr.sin_family = AF_INET;
  servaddr.sin_port = ft_htons(portnum);
  servaddr.sin_addr.s_addr = ft_htonl(INADDR_LOOPBACK);

  int listener = socket(AF_INET, SOCK_STREAM, 0);
  if (listener < 0) {
    fatal();
  }
  if (bind(listener, (struct sockaddr *)&servaddr, sizeof(servaddr)) < 0) {
    close(listener);
    fatal();
  }
  if (listen(listener, 10) < 0) {
    close(listener);
    fatal();
  }
  return listener;
}

////////////////////////////////////////////////////////////
//  Select Loop

void register_write_ready_clients(fd_set *write_fds) {
  for (int fd = 0; fd <= g_fdmax; ++fd) {
    if (g_clients[fd].id != -1 && g_clients[fd].buf) {
      FD_SET(fd, write_fds);
    }
  }
}

void handle_fd_events(int fd, bool is_readable, bool is_writable) {
  if (!is_readable && !is_writable) {
    return;
  }
  if (fd == g_listener) {
    if (is_readable) {
      accept_client();
    }
  } else {
    if (is_readable) {
      read_from_client(fd);
    }
    if (is_writable) {
      send_to_client(fd);
    }
  }
}

void select_loop() {
  fd_set read_fds, write_fds;

  FD_ZERO(&g_main_fds);
  FD_ZERO(&read_fds);
  FD_ZERO(&write_fds);
  FD_SET(g_listener, &g_main_fds);
  g_fdmax = g_listener;

  while (1) {
    read_fds = g_main_fds;
    FD_ZERO(&write_fds);
    register_write_ready_clients(&write_fds);
    errno = 0;
    if (select(g_fdmax + 1, &read_fds, &write_fds, 0, 0) == -1) {
      if (errno == EINTR) {
        continue;
      }
      clean_up();
      fatal();
    }
    for (int fd = 0; fd <= g_fdmax; ++fd) {
      handle_fd_events(fd, FD_ISSET(fd, &read_fds), FD_ISSET(fd, &write_fds));
    }
  }
}

////////////////////////////////////////////////////////////
//  Entry Point

int main(int argc, const char *argv[]) {
  if (argc != 2) {
    err_msg(ERR_ARGC);
    exit(1);
  }
  g_listener = launch_listener((uint16_t)atoi(argv[1]));

  g_clients = (t_client *)calloc(FD_SETSIZE, sizeof(t_client));
  if (!g_clients) {
    close(g_listener);
    fatal();
  }
  for (int fd = 0; fd < FD_SETSIZE; ++fd) {
    g_clients[fd].id = -1;
  }
  select_loop();
  clean_up();
  return 0;
}
