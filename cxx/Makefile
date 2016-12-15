
.PHONY: clean
CXXFLAGS = -g -c -Wall
OBJS = network.o solve.o
all: solve
solve: network.o solve.o
	g++ $(OBJS) -o solve -g
network.o: network.h
solve.o: network.h
clean:
	rm -rf $(OBJS)
