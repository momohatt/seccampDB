CC = g++
CFLAGS = -Wall -O2 -std=c++17

main: database.o main.cpp
	$(CC) $(CFLAGS) $^ -o $@

database.o: database.cpp
	$(CC) $(CFLAGS) -c $^ -o $@

test: database.o test.cpp
	$(CC) $(CFLAGS) $^ -o $@
	./test 2> /dev/null

clean:
	rm -rf main test *.o
