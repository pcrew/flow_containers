#ifndef FLOW_KEY_H
#define FLOW_KEY_H

#include <rte_ether.h>
#include <rte_ip.h>
#include <rte_tcp.h>
#include <rte_udp.h>
#include <rte_random.h>
#include <rte_cycles.h>
#include <rte_jhash.h>

struct flow_key_t {
    flow_key_t()
        : __src_ip(0)
        , __dst_ip(0)
        , __src_port(0)
        , __dst_port(0)
        , __proto(0)
        , __pad{0, 0, 0} {}

    flow_key_t(const rte_mbuf *mbuf) {
        auto eth = rte_pktmbuf_mtod(mbuf, struct rte_ether_hdr *);
        auto ip  = reinterpret_cast<rte_ipv4_hdr *>(eth + 1);

        __src_ip = ip->src_addr;
        __dst_ip = ip->dst_addr;
        __proto  = ip->next_proto_id;

        switch (__proto) {
        case IPPROTO_TCP: {
            const auto tcp = reinterpret_cast<const rte_tcp_hdr *>(ip + 1);
            __src_port     = rte_be_to_cpu_16(tcp->src_port);
            __dst_port     = rte_be_to_cpu_16(tcp->dst_port);
            break;
        }
        case IPPROTO_UDP: {
            const auto udp = reinterpret_cast<const rte_udp_hdr *>(ip + 1);
            __src_port     = rte_be_to_cpu_16(udp->src_port);
            __dst_port     = rte_be_to_cpu_16(udp->dst_port);
            break;
        }
        default:
            __src_port = 0xFFFF;
            __dst_port = 0xFFFF;
            break;
        }
        __pad[0] = __pad[1] = __pad[2] = 0;
    }

    bool operator==(const flow_key_t &other) const {
        /* Logical fields occupy 13 bytes; padding (last 3) ignored — same as field-wise == */
        return __builtin_memcmp(static_cast<const void *>(this), static_cast<const void *>(&other), 13) == 0;
    }

    uint32_t get_flow_hash() const {
        return ::rte_jhash_3words(__src_ip ^ (__src_port << 16), __dst_ip ^ (__dst_port << 16), __proto, 0);
    }

    uint32_t __src_ip;
    uint32_t __dst_ip;
    uint16_t __src_port;
    uint16_t __dst_port;
    uint8_t  __proto;
    uint8_t  __pad[3];
};

static_assert(sizeof(flow_key_t) == 16, "flow_key_t must be 16 bytes (5-tuple + padding)");

#endif
