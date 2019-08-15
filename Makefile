CC = g++
CFLAGS = -Wall -Wextra -O2 -g -std=c++17 -pthread

main: utils.o database.o main.cpp
	$(CC) $(CFLAGS) $^ -o $@

database.o: database.cpp
	$(CC) $(CFLAGS) -c $^ -o $@

utils.o: utils.cpp
	$(CC) $(CFLAGS) -c $^ -o $@

test: utils.o database.o test.cpp
	$(CC) $(CFLAGS) $^ -o $@
	./test

clean:
	rm -rf main test *.o
