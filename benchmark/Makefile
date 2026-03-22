
.PHONY: clean cleanall

SOURCES = $(wildcard *.cc)
OBJECTS = $(SOURCES:.cc=.o)
PROGRAM = test

CPPFLAGS += -isystem /usr/include/gtest 
CPPFLAGS += -g

#CPPFLAGS += -fsanitize=address -fsanitize=undefined -g
#CPPFLAGS += -fno-omit-frame-pointer
#LDFLAGS += -fsanitize=address -fsanitize=undefined

LDFLAGS += -lbenchmark -lpthread -ltbb
LDFLAGS += -labsl_hash -labsl_raw_hash_set -labsl_raw_logging_internal

all: $(PROGRAM)

$(PROGRAM): $(OBJECTS)
	$(CC) $(OBJECTS) -o $@ $(LDFLAGS)

%.o: %.cc
	$(CC) $(CPPFLAGS) -c $< -o $@

clean:
	@rm -rf $(PROGRAM)

cleanall: clean
