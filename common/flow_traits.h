#ifndef FLOW_TRAITS_H
#define FLOW_TRAITS_H

#include <iostream>

#include <rte_malloc.h>
#include <rte_mempool.h>

#include "flow_data.h"

struct flow_traits {
    using value_type     = flow_data;
    using timestamp_type = uint64_t;
    static void *allocate() {
        flow_data *obj;
        rte_mempool_get(get_pool(), (void **)&obj);
        return obj;
    }

    static void construct(void *ptr) { ::new (ptr) uint8_t[sizeof(flow_data)]; }
    static void deref(void *ptr) {
        if (ptr) {
            rte_mempool_put(get_pool(), ptr);
        }
    }

    static rte_mempool *get_pool() {
        static rte_mempool *pool = nullptr;
        if (unlikely(!pool)) {
            pool = rte_mempool_create("flow_pool", 1000000, sizeof(flow_data), 0, 0, nullptr, nullptr, nullptr, nullptr,
                                      rte_socket_id(), 0);
            if (!pool) {
                std::cerr << "Failed to create mempool\n" << std::endl;
                exit(1);
            }
        }

        return pool;
    }

    static bool can_allocate() { return true; }
};

struct dpdk_timestamp {
    uint64_t __value;
    uint64_t age_since(uint64_t current) { return current - __value; }
};
#endif
