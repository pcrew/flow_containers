#ifndef STATS_H
#define STATS_H

#include <iostream>
#include <cstdint>

struct app_stats {
    uint64_t total_pkts;
    uint64_t new_flows;
    uint64_t existing_flows;
    uint64_t lookup_flows;
    uint64_t lookup_hits;
    uint64_t lookup_misses;
    uint64_t insert_failures;
    uint64_t cycles_spent;

    void print() {
        std::cout << "\tTotal packets: " << total_pkts << std::endl;
        std::cout << "\tNew flows: " << new_flows << std::endl;
        std::cout << "\tExsitnig flows: " << existing_flows << std::endl;
        std::cout << "\tLookup flows: " << lookup_flows << std::endl;
        std::cout << "\tLookup hits: " << lookup_hits << std::endl;
        std::cout << "\tLookup misses: " << lookup_misses << std::endl;
        std::cout << "\tInsert failures: " << insert_failures << std::endl;
        std::cout << "\tCycles spent " << cycles_spent << std::endl;
    }
};

#endif
