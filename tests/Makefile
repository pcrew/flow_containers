
.PHONY: clean cleanall

SOURCES = $(wildcard *.cc)
OBJECTS = $(SOURCES:.cc=.o)
PROGRAM = test

CPPFLAGS += -isystem /usr/include/gtest
LDFLAGS += -lgtest -lgtest_main -lpthread

all: $(PROGRAM)

$(PROGRAM): $(OBJECTS)
	$(CC) $(OBJECTS) -o $@ $(LDFLAGS)

%.o: %.cc
	$(CC) $(CPPFLAGS) -c $< -o $@

clean:
	@rm -rf $(PROGRAM)

cleanall: clean
