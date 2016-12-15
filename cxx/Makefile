
.PHONY: clean run
CXXFLAGS = -g -Wall -O2 -Og
TARGET_DIR = ../target
OBJS = network.o solve.o
solve_BIN = $(TARGET_DIR)/solve
all: $(solve_BIN)
$(solve_BIN): network.o solve.o
	g++ $(OBJS) -o $(solve_BIN) -g
network.o: network.h
solve.o: network.h
clean:
	-rm -rf $(OBJS)
dist-clean: clean
	-rm -rf $(solve_BIN)
run: all
	$(solve_BIN)
