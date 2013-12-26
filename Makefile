BIN = osvm.so

all: $(BIN)

OBJS = main.o udp.o # server.o context.o

OSV_INCLUDES = -I../osv/include -I../osv/arch/x64 -I../osv
CXXFLAGS = -O2 -fPIC -std=c++11 $(OSV_INCLUDES)
LDFLAGS = -shared

$(BIN): $(OBJS)
	$(CXX) $(LDFLAGS) -o $(BIN) $(OBJS)
