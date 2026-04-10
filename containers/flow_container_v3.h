// flow_container_v3.h
#ifndef FLOW_CONTAINER_V3_H
#define FLOW_CONTAINER_V3_H

#include <utility>
#include <cstring>
#include <cstdio>
#include <string>
#include <iostream>

#include <rte_common.h>
#include <rte_memzone.h>

#include "flow_container_insert_result.h"
#include "../common/compiler.h"

template <typename T_key, typename T_traits, size_t PKT_LIMIT>
struct flow_container_v3_impl {
    using key_t       = T_key;
    using data_t      = typename T_traits::value_type;
    using data_ptr_t  = data_t *;
    using timestamp_t = typename T_traits::timestamp_type;

    static_assert(sizeof(key_t) == 16, "flow key should be 16 bytes (5-tuple + padding).");

private:
    static constexpr uint32_t ENTRIES_PER_BUCKET   = 4;
    static constexpr int32_t  INVALID_BUCKET_INDEX = -1;

    struct alignas(RTE_CACHE_LINE_SIZE) slot_t {
        key_t      key;
        data_ptr_t data;
        uint16_t   sig;
        uint8_t    valid;

        data_ptr_t init_data() {
            if (likely(data == nullptr)) {
                data = reinterpret_cast<data_ptr_t>(T_traits::allocate());
                T_traits::construct(data);
            }
            return data;
        }
    };

    struct alignas(RTE_CACHE_LINE_SIZE) bucket_t {
        slot_t      slots[ENTRIES_PER_BUCKET];
        uint32_t    __next_bucket;
        uint32_t    __prev_bucket;
        uint16_t    __count;
        timestamp_t __last_check;
    };

public:
    struct iterator {
        bucket_t *__bucket;
        uint32_t  __pos;

        iterator()
            : __bucket(nullptr)
            , __pos(0) {}
        iterator(bucket_t *b, uint32_t p)
            : __bucket(b)
            , __pos(p) {}

        bool operator==(const iterator &other) const { return __bucket == other.__bucket && __pos == other.__pos; }
        bool operator!=(const iterator &other) const { return !operator==(other); }

        bool         is_valid() const { return __bucket && __bucket->slots[__pos].valid; }
        const key_t *key() const { return is_valid() ? &__bucket->slots[__pos].key : nullptr; }
        data_ptr_t   data() const { return is_valid() ? __bucket->slots[__pos].data : nullptr; }
    };

private:
    bucket_t *__buckets;
    uint32_t *__bkt_stack;
    int32_t  *__iter_next;
    int32_t  *__iter_prev;

    uint32_t __n_buckets;
    uint32_t __n_buckets_ext;
    uint32_t __bkt_stack_pos;
    int32_t  __current_iterator_bkt;
    uint32_t __current_iterator_pos;

    const rte_memzone *__arena;

    // statistics
    uint32_t __used[ENTRIES_PER_BUCKET + 1];
    uint64_t __inserted;
    uint64_t __erased;
    uint64_t __no_space;
    uint64_t __memory_used;

public:
    flow_container_v3_impl()
        : __buckets(nullptr)
        , __bkt_stack(nullptr)
        , __iter_next(nullptr)
        , __iter_prev(nullptr)
        , __n_buckets(0)
        , __n_buckets_ext(0)
        , __bkt_stack_pos(0)
        , __current_iterator_bkt(INVALID_BUCKET_INDEX)
        , __current_iterator_pos(0)
        , __inserted(0)
        , __erased(0)
        , __no_space(0)
        , __memory_used(0) {
        for (uint32_t i = 0; i <= ENTRIES_PER_BUCKET; i++) {
            __used[i] = 0;
        }
    }

    ~flow_container_v3_impl() {
        if (__arena) rte_memzone_free(__arena);
    }

    static uint32_t bucket_cnt(uint32_t n_order, uint32_t n_ext) { return (1 << n_order) + n_ext; }

    static uint64_t total_size(uint32_t n_order, uint32_t n_ext) {
        uint32_t n_buckets = bucket_cnt(n_order, n_ext);
        uint64_t size      = n_buckets * sizeof(bucket_t) + n_ext * sizeof(uint32_t) + n_buckets * sizeof(int32_t) * 2;
        return RTE_ALIGN(size, RTE_CACHE_LINE_SIZE);
    }

    void init(const std::string &arena_name, uint32_t order, uint32_t n_ext, uint32_t n_keys = 0, uint32_t flags = 0) {
        uint32_t n    = 1 << order;
        uint64_t size = total_size(order, n_ext);

        __arena = rte_memzone_lookup(arena_name.c_str());
        if (__arena == nullptr) {
            __arena = rte_memzone_reserve(arena_name.c_str(), size, rte_socket_id(),
                                          RTE_MEMZONE_1GB | RTE_MEMZONE_SIZE_HINT_ONLY | flags);
            if (__arena == nullptr) {
                std::cerr << "Can't initialize memzone for " << arena_name << std::endl;
                exit(1);
            }
        }

        void *f = __arena->addr;

        __buckets   = reinterpret_cast<bucket_t *>(f);
        __iter_next = reinterpret_cast<int32_t *>(__buckets + n + n_ext);
        __iter_prev = __iter_next + (n + n_ext);
        __bkt_stack = reinterpret_cast<uint32_t *>(__iter_prev + (n + n_ext));

        __n_buckets     = n;
        __n_buckets_ext = n_ext;
        __used[0]       = n + n_ext;
        __memory_used   = size;

        for (uint32_t i = 0; i < n + n_ext; i++) {
            __buckets[i].__next_bucket = 0;
            __buckets[i].__prev_bucket = 0;
            __buckets[i].__count       = 0;
            __buckets[i].__last_check  = 0;
            __iter_next[i]             = INVALID_BUCKET_INDEX;
            __iter_prev[i]             = INVALID_BUCKET_INDEX;
            for (uint32_t j = 0; j < ENTRIES_PER_BUCKET; j++) {
                __buckets[i].slots[j].valid = 0;
                __buckets[i].slots[j].sig   = 0;
            }
        }

        for (uint32_t i = 0; i < n_ext; i++) {
            __bkt_stack[i] = __n_buckets + i;
        }
        __bkt_stack_pos = n_ext;
    }

    std::pair<iterator, insert_result> add(const key_t &key, timestamp_t current_time) {
        return add(key, current_time, key.get_flow_hash());
    }

    std::pair<iterator, insert_result> add(const key_t &key, timestamp_t current_time, uint32_t flow_hash) {
        uint16_t  sig        = (flow_hash >> 16) | 1;
        uint32_t  bucket_idx = flow_hash & (__n_buckets - 1);
        bucket_t *bucket     = &__buckets[bucket_idx];

        for (bucket_t *b = bucket; b; b = __bucket_next(b)) {
            for (uint32_t i = 0; i < ENTRIES_PER_BUCKET; i++) {
                if (b->slots[i].valid && b->slots[i].sig == sig && b->slots[i].key == key) {
                    return {iterator(b, i), insert_result::ALREADY_EXISTS};
                }
            }
        }

        bucket_t *prev = nullptr;
        for (bucket_t *b = bucket; b; prev = b, b = __bucket_next(b)) {
            for (uint32_t i = 0; i < ENTRIES_PER_BUCKET; i++) {
                if (!b->slots[i].valid) {
                    b->slots[i].key   = key;
                    b->slots[i].sig   = sig;
                    b->slots[i].valid = 1;
                    b->slots[i].init_data();

                    __used[b->__count]--;
                    b->__count++;
                    __used[b->__count]++;
                    __inserted++;

                    if (b->__count == 1) {
                        uint32_t idx = b - __buckets;
                        __add_to_iterator_list(idx, current_time);
                    }

                    return {iterator(b, i), insert_result::INSERTED};
                }
            }
        }

        if (__bkt_stack_pos > 0) {
            uint32_t  new_idx    = __bkt_stack[--__bkt_stack_pos];
            bucket_t *new_bucket = &__buckets[new_idx];

            if (prev) {
                prev->__next_bucket       = new_idx;
                new_bucket->__prev_bucket = prev - __buckets;
            }

            new_bucket->slots[0].key   = key;
            new_bucket->slots[0].sig   = sig;
            new_bucket->slots[0].valid = 1;
            new_bucket->__count        = 1;
            new_bucket->slots[0].init_data();

            __used[0]--;
            __used[1]++;
            __inserted++;

            __add_to_iterator_list(new_idx, current_time);

            return {iterator(new_bucket, 0), insert_result::INSERTED};
        }

        __no_space++;
        return {iterator(), insert_result::NO_MEMORY};
    }

    bool erase(const key_t &key) {
        uint32_t flow_hash  = key.get_flow_hash();
        uint16_t sig        = (flow_hash >> 16) | 1;
        uint32_t bucket_idx = flow_hash & (__n_buckets - 1);

        for (bucket_t *b = &__buckets[bucket_idx]; b; b = __bucket_next(b)) {
            for (uint32_t i = 0; i < ENTRIES_PER_BUCKET; i++) {
                if (b->slots[i].valid && b->slots[i].sig == sig && b->slots[i].key == key) {
                    __rem(b, i);
                    return true;
                }
            }
        }
        return false;
    }

    void erase(const iterator &it) {
        if (it.is_valid()) {
            __rem(it.__bucket, it.__pos);
        }
    }

    iterator get_next() {
        if (__current_iterator_bkt == INVALID_BUCKET_INDEX) {
            return iterator();
        }

        bucket_t *bucket = &__buckets[__current_iterator_bkt];

        for (; __current_iterator_pos < ENTRIES_PER_BUCKET; __current_iterator_pos++) {
            if (bucket->slots[__current_iterator_pos].valid) {
                return iterator(bucket, __current_iterator_pos++);
            }
        }

        __current_iterator_bkt = __iter_next[__current_iterator_bkt];
        __current_iterator_pos = 0;

        if (__current_iterator_bkt == INVALID_BUCKET_INDEX) {
            return iterator();
        }

        bucket = &__buckets[__current_iterator_bkt];

        for (; __current_iterator_pos < ENTRIES_PER_BUCKET; __current_iterator_pos++) {
            if (bucket->slots[__current_iterator_pos].valid) {
                return iterator(bucket, __current_iterator_pos++);
            }
        }

        return iterator();
    }

    template <typename T_info>
    void lookup(T_info *info, uint64_t pkts_mask, uint64_t *lookup_hit_mask) {
        uint64_t hits = 0;

        while (pkts_mask) {
            uint32_t idx = __builtin_ctzll(pkts_mask);
            pkts_mask &= ~(1ULL << idx);

            uint32_t     hash       = info[idx].lookup_hash();
            const key_t *key;
            info[idx].get_key(key);

            uint32_t sig        = (hash >> 16) | 1;
            uint32_t bucket_idx = hash & (__n_buckets - 1);

            for (bucket_t *bkt = &__buckets[bucket_idx]; bkt; bkt = __bucket_next(bkt)) {
                for (uint32_t i = 0; i < ENTRIES_PER_BUCKET; i++) {
                    if (bkt->slots[i].valid && bkt->slots[i].sig == sig && bkt->slots[i].key == *key) {
                        hits |= (1ULL << idx);
                        info[idx].set_entry(bkt->slots[i].data);
                        goto next;
                    }
                }
            }
        next:;
        }

        *lookup_hit_mask = hits;
    }

    // Statistics
    uint32_t get_elements_cnt() const {
        uint32_t res = 0;
        for (uint32_t i = 1; i <= ENTRIES_PER_BUCKET; i++) {
            res += __used[i] * i;
        }
        return res;
    }

    void print_stat() const {
        printf("Flow container (key_size = %lu):\n"
               "       used memory: %lu bytes\n"
               "     total buckets: %u\n"
               "      free buckets: %u\n"
               " 1-element buckets: %u (%f%%)\n"
               " 2-element buckets: %u (%f%%)\n"
               " 3-element buckets: %u (%f%%)\n"
               " 4-element buckets: %u (%f%%)\n"
               "    total elements: %u\n"
               "          inserted: %lu\n"
               "            erased: %lu\n"
               "          no space: %lu\n",
               sizeof(key_t),                                                              /**/
               get_memory_used(),                                                          /**/
               get_total_buckets_cnt(),                                                    /**/
               __used[0],                                                                  /**/
               __used[1], static_cast<float>(__used[1]) * 100.0 / get_total_buckets_cnt(), /**/
               __used[2], static_cast<float>(__used[2]) * 100.0 / get_total_buckets_cnt(), /**/
               __used[3], static_cast<float>(__used[3]) * 100.0 / get_total_buckets_cnt(), /**/
               __used[4], static_cast<float>(__used[4]) * 100.0 / get_total_buckets_cnt(), /**/
               get_elements_cnt(),                                                         /**/
               __inserted,                                                                 /**/
               __erased,                                                                   /**/
               __no_space /**/);
    }

    uint32_t get_total_buckets_cnt() const { return __n_buckets + __n_buckets_ext; }
    uint32_t get_free_buckets_cnt() const { return __used[0]; }
    uint32_t get_1_buckets_cnt() const { return __used[1]; }
    uint32_t get_2_buckets_cnt() const { return __used[2]; }
    uint32_t get_3_buckets_cnt() const { return __used[3]; }
    uint32_t get_4_buckets_cnt() const { return __used[4]; }
    uint64_t get_memory_used() const { return __memory_used; }
    uint64_t get_insert_cnt() const { return __inserted; }
    uint64_t get_erase_cnt() const { return __erased; }
    uint64_t get_no_space_cnt() const { return __no_space; }

private:
    bucket_t *__bucket_next(bucket_t *b) const { return b->__next_bucket ? &__buckets[b->__next_bucket] : nullptr; }

    void __add_to_iterator_list(uint32_t bucket_idx, timestamp_t ctime) {
        if (__current_iterator_bkt == INVALID_BUCKET_INDEX) {
            __iter_prev[bucket_idx] = bucket_idx;
            __iter_next[bucket_idx] = bucket_idx;
            __current_iterator_bkt  = bucket_idx;
            __current_iterator_pos  = 0;
        } else {
            int32_t head = __current_iterator_bkt;
            int32_t tail = __iter_prev[head];

            __iter_next[tail]       = bucket_idx;
            __iter_prev[bucket_idx] = tail;
            __iter_prev[head]       = bucket_idx;
            __iter_next[bucket_idx] = head;
        }
        __buckets[bucket_idx].__last_check = ctime;
    }

    void __rem(bucket_t *bucket, uint32_t pos) {
        bucket->slots[pos].valid = 0;

        __used[bucket->__count]--;
        bucket->__count--;
        __used[bucket->__count]++;

        __erased++;

        if (bucket->__count == 0) {
            uint32_t bucket_idx = bucket - __buckets;

            if (__current_iterator_bkt == __iter_next[bucket_idx]) {
                __current_iterator_bkt = INVALID_BUCKET_INDEX;
            } else {
                int32_t prev = __iter_prev[bucket_idx];
                int32_t next = __iter_next[bucket_idx];

                __iter_next[prev] = next;
                __iter_prev[next] = prev;

                if (__current_iterator_bkt == static_cast<int32_t>(bucket_idx)) {
                    __current_iterator_bkt = next;
                    __current_iterator_pos = 0;
                }
            }

            if (bucket_idx >= __n_buckets) {
                // расширенный бакет — возвращаем в стек
                bucket_t *prev_bucket      = &__buckets[bucket->__prev_bucket];
                prev_bucket->__next_bucket = bucket->__next_bucket;

                if (bucket->__next_bucket) {
                    bucket_t *next_bucket      = &__buckets[bucket->__next_bucket];
                    next_bucket->__prev_bucket = bucket->__prev_bucket;
                }

                __bkt_stack[__bkt_stack_pos++] = bucket_idx;
                __used[0]++;
            }
        }
    }
};

template <typename T_key, typename T_traits, size_t PKT_LIMIT>
using flow_container_v3 = flow_container_v3_impl<T_key, T_traits, PKT_LIMIT>;

#endif // FLOW_CONTAINER_V3_H
