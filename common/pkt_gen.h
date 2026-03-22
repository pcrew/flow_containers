#ifndef PKT_GEN_H
#define PKT_GEN_H

#include <vector>

#include "pkt_info.h"

class dummy {};

struct pkt_gen {
private:
    std::vector<flow_key_t> __known_flows;
    rte_mempool            *__pkt_pool;
    uint64_t                __pkt_id;

public:
    pkt_gen(rte_mempool *pool, size_t num_flows)
        : __pkt_pool(pool)
        , __pkt_id(0) {
        for (size_t i = 0; i < num_flows; i++) {
            flow_key_t key;
            key.__src_ip   = rte_rand();
            key.__dst_ip   = rte_rand();
            key.__src_port = rte_rand() & 0xFFFF;
            key.__dst_port = rte_rand() & 0xFFFF;
            key.__proto    = (rte_rand() & 1) ? IPPROTO_TCP : IPPROTO_UDP;
            key.__hash     = key.get_flow_hash();

            __known_flows.push_back(key);
        }
    }

    uint16_t generate_burst(pkt_info *pkts, uint16_t max_pkts, double new_flow_probability = 0.1) {
        uint16_t cnt{0};

        for (uint16_t i = 0; i < max_pkts && cnt < max_pkts; i++) {
            pkt_info &pkt         = pkts[cnt];
            auto      probability = static_cast<double>(::rte_rand()) / UINT64_MAX;
            if (probability < new_flow_probability) {
                pkt.__key.__src_ip   = rte_rand();
                pkt.__key.__dst_ip   = rte_rand();
                pkt.__key.__src_port = rte_rand() & 0xFFFF;
                pkt.__key.__dst_port = rte_rand() & 0xFFFF;
                pkt.__key.__proto    = (rte_rand() & 1) ? IPPROTO_TCP : IPPROTO_UDP;
            } else {
                size_t idx = rte_rand() % __known_flows.size();
                pkt.__key  = __known_flows[idx];
            }

            pkt.__key.__hash = pkt.__key.get_flow_hash();
            pkt.__data       = nullptr;
            pkt.__port_id    = 0;
            pkt.__timestamp  = rte_rdtsc();

            cnt++;
        }

        return cnt;
    }
};

#endif
