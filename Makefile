all: osvm

OBJS = main.o

CXXFLAGS = -O2 -fPIC -std=c++11
LDFLAGS = -shared

osvm: $(OBJS)
	$(CXX) $(LDFLAGS) -o osvm.so $(OBJS)