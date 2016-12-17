
.PHONY: clean run
CXXFLAGS = -g -Wall -O2 -Og -std=c++0x
TARGET_DIR = ../target
OBJS = network.o solve.o
solve_BIN = $(TARGET_DIR)/solve
invidxfmt_BIN = $(TARGET_DIR)/invidxfmt
offtable_BIN= $(TARGET_DIR)/offtable
all: $(solve_BIN) $(invidxfmt_BIN) $(offtable_BIN)
$(solve_BIN): network.o solve.o
	g++ $(OBJS) -o $(solve_BIN) -g
$(invidxfmt_BIN): invidxfmt.o
	g++ invidxfmt.o -o $(invidxfmt_BIN) -g
$(offtable_BIN): offtable.o
	g++ offtable.o -o $(offtable_BIN) -g
network.o: network.h
solve.o: network.h
invidxfmt.o:
offtable.o:
clean:
	-rm -rf $(OBJS) invidxfmt.o offtable.o
dist-clean: clean
	-rm -rf $(solve_BIN) $(invidxfmt_BIN) $(offtable_BIN)
run: all
	$(solve_BIN)
gdb: $(solve_BIN)
	gdb $(solve_BIN) -tu
