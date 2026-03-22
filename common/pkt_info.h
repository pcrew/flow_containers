#ifndef PKT_INFO_H
#define PKT_INFO_H

#include "flow_key.h"
#include "flow_data.h"

struct pkt_info {
    flow_key_t __key;
    flow_data *__data;

    uint16_t __port_id;
    uint64_t __timestamp;

    void get_key(const flow_key_t *&key) const { key = &__key; }
    void set_entry(flow_data *data) { __data = data; }
    void prefetch() const {
        if (__data) __data->prefetch();
    }
};

#endif
