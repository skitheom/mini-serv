NAME    := mini_serv
SRC     := mini_serv.c
CC      := cc
CFLAGS  := -Wall -Wextra -Werror

all: $(NAME)

$(NAME): $(SRC)
	$(CC) $(CFLAGS) -o $(NAME) $(SRC)

clean:
	$(RM) $(NAME)

fclean: clean

re: fclean all

run: $(NAME)
	@echo "== Running mini_serv =="
	./$(NAME) 8080

test: $(NAME)
	@echo "== Running mini_serv under leak check =="
	valgrind --leak-check=full --track-fds=yes ./$(NAME) 8080

.PHONY: all clean fclean re run test
