#ifndef PKT_INFO_H
#define PKT_INFO_H

#include "flow_key.h"
#include "flow_data.h"

struct pkt_info {
    flow_key_t __key;
    /// Precomputed `rte_jhash_3words` for __key; used by container lookup (not stored in the key).
    uint32_t __flow_hash;
    flow_data *__data;

    uint16_t __port_id;
    uint64_t __timestamp;

    void get_key(const flow_key_t *&key) const { key = &__key; }
    /// Hash for table indexing / sig; must match `__key.get_flow_hash()` unless filled intentionally.
    uint32_t lookup_hash() const { return __flow_hash; }
    void     set_entry(flow_data *data) { __data = data; }
    void     prefetch() const {
        if (__data) __data->prefetch();
    }
};

#endif
