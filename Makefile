
SHELL := bash

TOPDIR = $(shell /bin/pwd)
INCDIR = $(TOPDIR)/containers

PKGCONF = pkg-config

CC = $(CROSS_COMPILE)g++
STRIP = $(CROSS_COMPILE) strip
CPPFLAGS = -Wall -std=gnu++20 -I$(INCDIR) $(shell $(PKGCONF) --cflags libdpdk)
CPPFLAGS += -O3
CPPFLAGS += -funroll-loops
CPPFLAGS += "-mavx2"
CPPFLAGS += -march=native
CPPFLAGS += -fopt-info-vec-optimized -c

LDFLAGS = -L/usr/lib64
LDFLAGS += -Wl,-rpath=/usr/lib64
LDFLAGS += -Wl,--no-as-needed
LDFLAGS += /usr/lib64/librte_eal.so.25
LDFLAGS += /usr/lib64/librte_mempool.so.25
LDFLAGS += /usr/lib64/librte_ring.so.25
LDFLAGS += /usr/lib64/librte_mbuf.so.25
LDFLAGS += /usr/lib64/librte_hash.so.25
LDFLAGS += -lpthread -lnuma -ldl -lrt


export CC STRIP CPPFLAGS LDFLAGS TOPDIR INCDIR

all:
	$(MAKE) -C tests
	$(MAKE) -C benchmark

clean:
	@$(MAKE) clean -C tests
	@$(MAKE) clean -C benchmark
cleanall: clean
	@rm -vf `find . -name *.o`
	@rm -vf `find . -name *.sw`
