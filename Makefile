CC = g++
CFLAGS = -Wall -O2 -std=c++17

main: main.cpp
	$(CC) $(CFLAGS) $^ -o $@

clean:
	rm -rf main *.o
