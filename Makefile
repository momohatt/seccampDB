CC = g++
CFLAGS = -Wall -O2 -std=c++17

main: main.cpp database.o
	$(CC) $(CFLAGS) $^ -o $@

database.o: database.cpp
	$(CC) $(CFLAGS) -c $^ -o $@

clean:
	rm -rf main *.o
