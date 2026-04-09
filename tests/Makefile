.PHONY: clean cleanall sudo-test

SOURCE := main.cc
CPPFLAGS += -isystem /usr/include/gtest
LDFLAGS += -lgtest -lgtest_main -lpthread

PROGRAMS := test_v1 test_v2 test_v3

all: $(PROGRAMS)

main_v1.o: $(SOURCE)
	$(CC) $(CPPFLAGS) -DFLOW_CONTAINER=flow_container_v1 -c $< -o $@

main_v2.o: $(SOURCE)
	$(CC) $(CPPFLAGS) -DFLOW_CONTAINER=flow_container_v2 -c $< -o $@

main_v3.o: $(SOURCE)
	$(CC) $(CPPFLAGS) -DFLOW_CONTAINER=flow_container_v3 -c $< -o $@

test_v1: main_v1.o
	$(CC) $< -o $@ $(LDFLAGS)

test_v2: main_v2.o
	$(CC) $< -o $@ $(LDFLAGS)

test_v3: main_v3.o
	$(CC) $< -o $@ $(LDFLAGS)

TEST ?= test_v1
sudo-test: $(PROGRAMS)
	sudo -E ./$(TEST) $(ARGS)

clean:
	rm -f $(PROGRAMS) *.o

cleanall: clean
