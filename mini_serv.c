
#include <arpa/inet.h>
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

////////////////////////////////////////////////////////////
// from example main.c

int extract_message(char **buf, char **msg) {
  char *newbuf;
  int i;

  *msg = 0;
  if (*buf == 0)
    return (0);
  i = 0;
  while ((*buf)[i]) {
    if ((*buf)[i] == '\n') {
      newbuf = calloc(1, sizeof(*newbuf) * (strlen(*buf + i + 1) + 1));
      if (newbuf == 0)
        return (-1);
      strcpy(newbuf, *buf + i + 1);
      *msg = *buf;
      (*msg)[i + 1] = 0;
      *buf = newbuf;
      return (1);
    }
    i++;
  }
  return (0);
}

char *str_join(char *buf, char *add) {
  char *newbuf;
  int len;

  if (buf == 0)
    len = 0;
  else
    len = strlen(buf);
  newbuf = malloc(sizeof(*newbuf) * (len + strlen(add) + 1));
  if (newbuf == 0)
    return (0);
  newbuf[0] = 0;
  if (buf != 0)
    strcat(newbuf, buf);
  free(buf);
  strcat(newbuf, add);
  return (newbuf);
}

////////////////////////////////////////////////////////////

static const char *ERR_ARGC = "Wrong number of arguments\n";
static const char *ERR_FATAL = "Fatal error\n";
static const char *MSG_ARRIVAL = "server: client %d just arrived\n";
static const char *MSG_LEAVE = "server: client %d just left\n";
static const char *MSG_FORMAT = "client %d: %s";

typedef struct s_client {
  int id;
  char *inbuf;
  char *outbuf;
} t_client;

t_client g_clients[FD_SETSIZE];
int g_server_fd = -1;
int g_fdmax = -1;
fd_set g_main_fds;

////////////////////////////////////////////////////////////
//  Utils

void err_msg(const char *msg) {
  if (!msg)
    return;
  write(2, msg, strlen(msg));
}

void clean_up_client(int fd) {
  if (g_clients[fd].id != -1) {
    close(fd);
    g_clients[fd].id = -1;
  }
  if (g_clients[fd].inbuf) {
    free(g_clients[fd].inbuf);
    g_clients[fd].inbuf = 0;
  }
  if (g_clients[fd].outbuf) {
    free(g_clients[fd].outbuf);
    g_clients[fd].outbuf = 0;
  }
}

void clean_up() {
  if (g_server_fd != -1) {
    close(g_server_fd);
  }
  for (int fd = 0; fd < FD_SETSIZE; ++fd) {
    clean_up_client(fd);
  }
}

void fatal(void) {
  err_msg(ERR_FATAL);
  clean_up();
  exit(1);
}

char *xcalloc(size_t count, size_t size) {
  if (count == 0 || size == 0) {
    count = 1;
    size = 1;
  }
  char *ptr = calloc(count, size);
  if (!ptr) {
    fatal();
  }
  return ptr;
}

char *xstrdup(const char *src) {
  if (!src)
    return 0;
  char *dst = xcalloc(strlen(src) + 1, sizeof(char));
  strcpy(dst, src);
  return dst;
}

char *append_buf(char *buf, char *add) {
  if (!add || !*add) {
    return buf;
  }
  if (!buf) {
    return xstrdup(add);
  }
  char *joined_buf = str_join(buf, add);
  if (!joined_buf)
    fatal();
  return joined_buf;
}

////////////////////////////////////////////////////////////
//  Messaging

bool is_valid_fd(int fd) { return (fd >= 0 && fd < FD_SETSIZE); }

bool is_valid_clientfd(int fd) {
  return (is_valid_fd(fd) && fd != g_server_fd && g_clients[fd].id != -1);
}

void broadcast_msg(char *msg, int exclude_fd) {
  if (!msg || !*msg) {
    return;
  }
  for (int fd = 0; fd <= g_fdmax; ++fd) {
    if (!is_valid_clientfd(fd) || fd == exclude_fd) {
      continue;
    }
    g_clients[fd].outbuf = append_buf(g_clients[fd].outbuf, msg);
  }
}

void build_client_msg(const char *new_buf, int fd) {
  if (!new_buf || !*new_buf) {
    return;
  }
  g_clients[fd].inbuf = append_buf(g_clients[fd].inbuf, (char *)new_buf);
  while (true) {
    char *extracted = 0;
    int result = extract_message(&g_clients[fd].inbuf, &extracted);
    if (result == -1) {
      fatal();
    }
    if (result == 0) {
      break;
    }
    char *client_msg = xcalloc(1, strlen(MSG_FORMAT) + strlen(extracted) + 13);
    sprintf(client_msg, MSG_FORMAT, g_clients[fd].id, extracted);
    broadcast_msg(client_msg, fd);
    free(extracted);
    free(client_msg);
  }
}

void build_system_msg(const char *msg, int fd) {
  if (!msg || !*msg) {
    return;
  }
  char *system_msg = (char *)xcalloc(1, strlen(msg) + 13);
  sprintf(system_msg, msg, g_clients[fd].id);
  broadcast_msg(system_msg, fd);
  free(system_msg);
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
//  Client Management

void add_client(int fd) {
  if (!is_valid_fd(fd)) {
    return;
  }
  static int next_client_id = 0;
  g_clients[fd].id = next_client_id++;
  g_clients[fd].inbuf = 0;
  g_clients[fd].outbuf = 0;
  build_system_msg(MSG_ARRIVAL, fd);
}

void remove_client(int fd) {
  if (!is_valid_clientfd(fd)) {
    return;
  }
  build_system_msg(MSG_LEAVE, fd);
  clean_up_client(fd);
}

void accept_client() {
  int client_fd = accept(g_server_fd, 0, 0);
  if (client_fd == -1) {
    return;
  }
  if (client_fd >= FD_SETSIZE) {
    close(client_fd);
    return;
  }
  add_client(client_fd);
  monitor(client_fd);
}

void read_from_client(int fd) {
  if (!is_valid_clientfd(fd)) {
    return;
  }
  char buf[4096 + 1];
  ssize_t n = recv(fd, buf, 4096, 0);
  if (n <= 0) {
    remove_client(fd);
    unmonitor(fd);
    return;
  }
  buf[n] = '\0';
  build_client_msg(buf, fd);
}

void send_to_client(int fd) {
  if (!is_valid_clientfd(fd) || !g_clients[fd].outbuf) {
    return;
  }
  ssize_t buf_len = strlen(g_clients[fd].outbuf);
  ssize_t bytes_sent = send(fd, g_clients[fd].outbuf, buf_len, 0);
  if (bytes_sent < 0) {
    remove_client(fd);
    unmonitor(fd);
    return;
  }
  if (bytes_sent == 0) {
    return;
  }

  // if bytes_sent > 0; partial write
  if (bytes_sent < buf_len) {
    char *dst = (char *)xcalloc(1, buf_len - bytes_sent + 1);
    strcpy(dst, g_clients[fd].outbuf + bytes_sent);
    free(g_clients[fd].outbuf);
    g_clients[fd].outbuf = dst;
    return;
  }
  free(g_clients[fd].outbuf);
  g_clients[fd].outbuf = 0;
}

////////////////////////////////////////////////////////////
//  Select Loop

void handle_events(int fd, bool is_readable, bool is_writable) {
  if (!is_readable && !is_writable) {
    return;
  }
  if (fd == g_server_fd) {
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
  // add listening fd
  FD_ZERO(&g_main_fds);
  FD_SET(g_server_fd, &g_main_fds);
  g_fdmax = g_server_fd;

  while (1) {
    fd_set read_fds, write_fds;

    FD_ZERO(&read_fds);
    FD_ZERO(&write_fds);
    read_fds = g_main_fds;
    for (int fd = 0; fd <= g_fdmax; ++fd) {
      if (g_clients[fd].id == -1 || !FD_ISSET(fd, &g_main_fds)) {
        continue;
      }
      if (g_clients[fd].outbuf) {
        FD_SET(fd, &write_fds);
      }
    }

    if (select(g_fdmax + 1, &read_fds, &write_fds, 0, 0) <= 0) {
      continue;
    }
    for (int fd = 0; fd <= g_fdmax; ++fd) {
      handle_events(fd, FD_ISSET(fd, &read_fds), FD_ISSET(fd, &write_fds));
    }
  }
}

////////////////////////////////////////////////////////////
//  Init Server & Clients

static void server_init(uint16_t port) {
  struct sockaddr_in addr;

  bzero(&addr, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = htons(port);

  g_server_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (g_server_fd == -1) {
    fatal();
  }
  if (bind(g_server_fd, (struct sockaddr *)&addr, sizeof(addr)) == -1) {
    fatal();
  }
  if (listen(g_server_fd, 128) == -1) {
    fatal();
  }
}

static void clients_init(void) {
  for (int fd = 0; fd < FD_SETSIZE; ++fd) {
    g_clients[fd].id = -1;
    g_clients[fd].inbuf = NULL;
    g_clients[fd].outbuf = NULL;
  }
}

////////////////////////////////////////////////////////////
//  Entry Point

int main(int argc, const char *argv[]) {
  if (argc != 2) {
    err_msg(ERR_ARGC);
    exit(1);
  }
  server_init((uint16_t)atoi(argv[1]));
  clients_init();
  select_loop();
  clean_up();
  return 0;
}
