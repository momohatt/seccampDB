CC = g++
CFLAGS = -Wall -O2 -std=c++0x

main: main.cpp
	$(CC) $(CFLAGS) $^ -o $@

clean:
	rm -rf main *.o
