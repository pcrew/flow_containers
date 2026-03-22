#ifndef FLOW_DATA_H
#define FLOW_DATA_H

#include <rte_prefetch.h>

struct flow_data {
    uint64_t packets{0};
    uint64_t bytes{0};
    uint64_t first_seen{0};
    uint64_t last_seen{0};
    uint32_t tcp_state{0};

    void prefetch() { rte_prefetch0(this); }
};

#endif
