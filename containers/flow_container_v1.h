#ifndef FLOW_CONTAINER_V1_H
#define FLOW_CONTAINER_V1_H

#include <utility>
#include <memory>
#include <cstring>
#include <cstdio>
#include <string>
#include <iostream>

#include <rte_common.h>
#include <rte_malloc.h>
#include <rte_memzone.h>

#include "flow_container_insert_result.h"
#include "../common/compiler.h"

template <typename T_key, typename T_traits>
struct entries_separate : public T_traits {
    using key_t      = T_key;
    using data_t     = typename T_traits::value_type;
    using data_ptr_t = data_t *;

    key_t      *keys;
    data_ptr_t *data;
    uint32_t    n_keys;

    template <typename... Args>
    entries_separate(Args &&...args)
        : T_traits(std::forward<Args>(args)...)
        , keys(nullptr)
        , data(nullptr) {}

    void init(void *mem, uint32_t n) {
        n_keys = n;
        keys   = reinterpret_cast<key_t *>(mem);
        data   = reinterpret_cast<data_ptr_t *>(keys + n);
    }

    data_ptr_t init_data(const key_t &key, uint32_t idx) {
        if (likely(data[idx] == nullptr)) {
            data[idx] = reinterpret_cast<data_ptr_t>(T_traits::allocate());
            T_traits::construct(data[idx]);
        }
        return data[idx];
    }

    void delete_data(uint32_t idx) { erase_data(data[idx]); }
    void erase_data(data_ptr_t &data) {
        if (likely(data)) {
            T_traits::deref(data);
            data = nullptr;
        }
    }

    data_ptr_t set(const key_t &key, uint32_t idx) {
        keys[idx] = key;
        return init_data(keys[idx], idx);
    }

    const key_t &get_key(uint32_t idx) const { return keys[idx]; }
    data_ptr_t   get_data(uint32_t idx) { return data[idx]; }

    bool is_no_memory() const { return !T_traits::can_allocate(); }

    static constexpr uint32_t item_size() { return sizeof(key_t) + sizeof(data_ptr_t); }
    static uint32_t           memory_size(uint32_t n) { return n * item_size(); }
};

template <typename T_key, typename T_traits, template <typename, typename> typename T_entries, size_t PKT_LIMIT>
struct flow_container_v1_impl {
    using traits_t    = T_traits;
    using key_t       = T_key;
    using data_t      = typename T_traits::value_type;
    using entries_t   = T_entries<T_key, T_traits>;
    using data_ptr_t  = typename entries_t::data_ptr_t;
    using timestamp_t = typename T_traits::timestamp_type;

    static constexpr uint32_t ENTRIES_PER_BUCKET{4};
    static constexpr int32_t  INVALID_BUCKET_INDEX{-1};

private:
    struct alignas(RTE_CACHE_LINE_SIZE) bucket_t {
        uintptr_t   __next_bucket;
        bucket_t   *__prev_bucket;
        uint32_t    __key_pos[ENTRIES_PER_BUCKET];
        uint16_t    __sig[ENTRIES_PER_BUCKET];
        int32_t     __next_iterator;
        int32_t     __prev_iterator;
        uint16_t    __count;
        timestamp_t __last_check;
    };

    static_assert(sizeof(bucket_t) == RTE_CACHE_LINE_SIZE, "Bad alignas..,");

public:
    struct iterator {
        iterator()
            : __bucket(nullptr)
            , __data(nullptr) {}

        iterator(bucket_t *b, uint32_t p, data_ptr_t d, const key_t *k)
            : __bucket(b)
            , __pos(p)
            , __key(k)
            , __data(d) {}

        bool operator==(const iterator &other) const { return __bucket == other.__bucket && __pos == other.__pos; }
        bool operator!=(const iterator &other) const { return !operator==(other); }

        bool         is_valid() const { return __bucket != nullptr; }
        const key_t *key() const { return __key; }
        data_ptr_t   data() const { return __data; }

    private:
        bucket_t    *__bucket;
        uint32_t     __pos;
        const key_t *__key;
        data_ptr_t   __data;
    };

private:
    bucket_t *__buckets;
    uint32_t *__bkt_stack;
    uint32_t *__key_stack;
    entries_t __entries;
    uint32_t  __n_buckets;
    uint32_t  __bkt_stack_pos;
    uint32_t  __key_stack_pos;
    int32_t   __current_iterator_bkt;
    uint32_t  __current_iterator_pos;

    uint32_t __n_buckets_ext;
    uint32_t __n_keys;

    // statistics
    uint32_t __used[ENTRIES_PER_BUCKET + 1];
    uint64_t __inserted;
    uint64_t __erased;
    uint64_t __no_space;
    uint64_t __memory_used;

    const rte_memzone *__arena;

public:
    template <typename... Args>
    flow_container_v1_impl(Args &&...args)
        : __buckets(nullptr)
        , __bkt_stack(nullptr)
        , __key_stack(nullptr)
        , __entries(std::forward<Args>(args)...)
        , __n_buckets(0)
        , __bkt_stack_pos(0)
        , __key_stack_pos(0)
        , __current_iterator_bkt(INVALID_BUCKET_INDEX)
        , __current_iterator_pos(0)
        , __n_buckets_ext(0)
        , __n_keys(0)
        , __inserted(0)
        , __erased(0)
        , __no_space(0)
        , __memory_used(0) {
        for (uint32_t i = 0; i < ENTRIES_PER_BUCKET; i++) {
            __used[i] = 0;
        }
    }

    ~flow_container_v1_impl() { ::rte_memzone_free(__arena); }

    constexpr static uint32_t bucket_size() { return sizeof(bucket_t); }
    constexpr static uint32_t stack_item_size() { return sizeof(uint32_t); }
    constexpr static uint32_t key_size() { return entries_t::item_size(); }

    static uint32_t bucket_cnt(uint32_t n_order, uint32_t n_ext) {
        auto n = 1 << n_order;
        return n + n_ext;
    }

    static uint64_t total_size(uint32_t n_order, uint32_t n_ext, uint32_t n_key) {
        uint64_t t_size = bucket_cnt(n_order, n_ext) * bucket_size() + stack_item_size() * (n_ext + n_key) +
                          entries_t::memory_size(n_key);
        return RTE_ALIGN(t_size, RTE_CACHE_LINE_SIZE);
    }

    const rte_memzone *get_arena() const { return __arena; }

    void init(const std::string &arena_name, uint32_t order, uint32_t n_ext, uint32_t n_key, uint32_t flags) {
        uint32_t n{static_cast<uint32_t>(1) << order};
        uint64_t t_size{total_size(order, n_ext, n_key)};

        __arena = ::rte_memzone_lookup(arena_name.c_str());
        if (__arena == nullptr) {
            __arena = ::rte_memzone_reserve(arena_name.c_str(), t_size, ::rte_socket_id(),
                                            RTE_MEMZONE_1GB | RTE_MEMZONE_SIZE_HINT_ONLY);

            if (__arena == nullptr) {
                std::cout << "Can't initialize memzone" << std::endl;
                exit(0);
            }
        }

        void *f{__arena->addr};

        __buckets = reinterpret_cast<bucket_t *>(f);

        for (uint32_t i = 0; i < n + n_ext; i++) {
            __buckets[i].__next_bucket   = 0;
            __buckets[i].__prev_bucket   = nullptr;
            __buckets[i].__count         = 0;
            __buckets[i].__prev_iterator = INVALID_BUCKET_INDEX;
            __buckets[i].__next_iterator = INVALID_BUCKET_INDEX;

            for (uint32_t j = 0; j < ENTRIES_PER_BUCKET; j++) {
                __buckets[i].__key_pos[j] = 0;
                __buckets[i].__sig[j]     = 0;
            }
        }

        __bkt_stack = reinterpret_cast<uint32_t *>(__buckets + n + n_ext);
        __key_stack = reinterpret_cast<uint32_t *>(__bkt_stack + n_ext);

        __entries.init(__key_stack + n_key, n_key);

        __n_buckets     = n;
        __n_buckets_ext = n_ext;
        __n_keys        = n_key;

        __used[0]     = n + n_ext;
        __memory_used = t_size;

        for (uint32_t i = 0; i < n_ext; i++) {
            __bkt_stack[i] = __n_buckets + i;
        }
        __bkt_stack_pos = n_ext;

        for (uint32_t i = 0; i < n_key; i++) {
            __key_stack[i] = i;
        }
        __key_stack_pos = n_key;
    }

    uint32_t get_key_idx(bucket_t *bkt, uint32_t i) { return bkt->__key_pos[i]; }

    std::pair<iterator, insert_result> add(const key_t &key, timestamp_t current_time) {
        uint32_t  sig{key.__hash};
        uint32_t  bucket_index = sig & (__n_buckets - 1);
        bucket_t *bucket0      = &__buckets[bucket_index];

        if (unlikely(__entries.is_no_memory())) {
            __no_space++;
            return std::make_pair(iterator(), insert_result::NO_MEMORY);
        }

        sig = (sig >> 16) | 1;

        for (auto bucket = bucket0; bucket != nullptr; bucket = __bucket_next(bucket)) {
            for (uint32_t i = 0; i < ENTRIES_PER_BUCKET; i++) {
                uint32_t bkt_key_idx = get_key_idx(bucket, i);
                uint32_t bkt_sig     = bucket->__sig[i];

                if (sig == bkt_sig && key == __entries.get_key(bkt_key_idx)) {
                    auto data = __entries.get_data(bkt_key_idx);
                    if (likely(data)) {
                        return std::make_pair(iterator(bucket, i, data, &key), insert_result::ALREADY_EXISTS);
                    } else {
                        return std::make_pair(iterator(bucket, i, data, &key), insert_result::INSERTED);
                    }
                }
            }
        }

        if (unlikely(__key_stack_pos == 0)) {
            __no_space++;
            return std::make_pair(iterator(), insert_result::NO_MEMORY);
        }

        bucket_t *bucket, *bucket_prev;
        for (bucket_prev = nullptr, bucket = bucket0; bucket != nullptr;
             bucket_prev = bucket, bucket = __bucket_next(bucket)) {
            for (uint32_t i = 0; i < ENTRIES_PER_BUCKET; i++) {
                if (bucket->__sig[i] == 0) {
                    uint32_t bkt_key_idx{__key_stack[--__key_stack_pos]};

                    bucket->__sig[i]     = static_cast<uint16_t>(sig);
                    bucket->__key_pos[i] = bkt_key_idx;

                    __used[bucket->__count]--;

                    if (bucket->__count++ == 0) {
                        bucket_index = bucket - __buckets;
                        if (__current_iterator_bkt == INVALID_BUCKET_INDEX) {
                            bucket->__prev_iterator = bucket_index;
                            bucket->__next_iterator = bucket_index;
                            __current_iterator_bkt  = bucket_index;
                            __current_iterator_pos  = 0;
                        } else {
                            auto iter_bucket = &__buckets[__current_iterator_bkt];
                            auto prev_bucket = &__buckets[iter_bucket->__prev_iterator];

                            prev_bucket->__next_iterator = bucket_index;
                            bucket->__prev_iterator      = iter_bucket->__prev_iterator;

                            iter_bucket->__prev_iterator = bucket_index;
                            bucket->__next_iterator      = __current_iterator_bkt;
                        }

                        bucket->__last_check = current_time;
                    }

                    __used[bucket->__count]++;
                    __inserted++;

                    auto data = __entries.set(key, bkt_key_idx);
                    return std::make_pair(iterator(bucket, i, data, &key), insert_result::INSERTED);
                }
            }
        }

        // bucket full :(
        if (__bkt_stack_pos > 0) {
            bucket_index          = __bkt_stack[--__bkt_stack_pos];
            bucket                = &__buckets[bucket_index];
            bucket->__prev_bucket = bucket_prev;

            __bucket_next_set(bucket_prev, bucket);
            __bucket_next_set_null(bucket);

            uint32_t bkt_key_idx = __key_stack[--__key_stack_pos];

            bucket->__sig[0]     = static_cast<uint16_t>(sig);
            bucket->__key_pos[0] = bkt_key_idx;

            if (__current_iterator_bkt == INVALID_BUCKET_INDEX) {
                bucket->__prev_iterator = bucket_index;
                bucket->__next_iterator = bucket_index;
                __current_iterator_bkt  = bucket_index;
                __current_iterator_pos  = 0;
            } else {
                auto list_bucket = &__buckets[__current_iterator_bkt];
                auto prev_bucket = &__buckets[list_bucket->__prev_iterator];

                prev_bucket->__next_iterator = bucket_index;
                bucket->__prev_iterator      = list_bucket->__prev_iterator;
                list_bucket->__prev_iterator = bucket_index;
                bucket->__next_iterator      = __current_iterator_bkt;
            }

            bucket->__last_check = current_time;
            bucket->__count      = 1;

            __used[0]--;
            __used[1]++;
            __inserted++;

            auto data = __entries.set(key, bkt_key_idx);
            return std::make_pair(iterator(bucket, 0, data, &key), insert_result::INSERTED);
        }

        __no_space++;
        return std::make_pair(iterator(), insert_result::NO_MEMORY);
    }

    bool erase(const key_t &key) {
        uint32_t  bucket_index = key.__hash & (__n_buckets - 1);
        bucket_t *bucket0      = &__buckets[bucket_index];

        uint32_t sig = (key.__hash >> 16) | 1;

        for (auto bucket = bucket0; bucket != nullptr; bucket = __bucket_next(bucket)) {
            for (uint32_t i = 0; i < ENTRIES_PER_BUCKET; i++) {
                uint32_t bkt_key_idx = bucket->__key_pos[i];
                uint32_t bkt_sig     = bucket->__sig[i];
                if (sig == bkt_sig && key == __entries.get_key(bkt_key_idx)) {
                    __rem(bucket, i);
                    return true;
                }
            }
        }
        return false;
    }

    void erase(const iterator &it) { __rem(it.__bucket, it.__pos); }
    void erase_data(data_ptr_t &data) { __entries.erase_data(data); }

    iterator get_next() {
        if (__current_iterator_bkt == INVALID_BUCKET_INDEX) {
            return iterator();
        }

        bucket_t *bucket = &__buckets[__current_iterator_bkt];
        for (; __current_iterator_pos < ENTRIES_PER_BUCKET; __current_iterator_pos++) {
            if (bucket->__sig[__current_iterator_pos] != 0) {
                uint32_t     bkt_key_idx = bucket->__key_pos[__current_iterator_pos];
                const key_t *key         = &__entries.get_key(bkt_key_idx);
                auto         data        = __entries.get_data(bkt_key_idx);

                return iterator(bucket, __current_iterator_pos++, data, key);
            }
        }

        __current_iterator_bkt = bucket->__next_iterator;
        __current_iterator_pos = 0;
        bucket                 = &__buckets[__current_iterator_bkt];

        for (; __current_iterator_pos < ENTRIES_PER_BUCKET; __current_iterator_pos++) {
            if (bucket->__sig[__current_iterator_pos] != 0) {
                uint32_t     bkt_key_idx = bucket->__key_pos[__current_iterator_pos];
                const key_t *key         = &__entries.get_key(bkt_key_idx);
                auto         data        = __entries.get_data(bkt_key_idx);

                return iterator(bucket, __current_iterator_pos++, data, key);
            }
        }

        return iterator();
    }

    iterator get_next(timestamp_t current_time, uint32_t period) {
        if (__current_iterator_bkt == INVALID_BUCKET_INDEX) {
            return iterator();
        }

        bucket_t *bucket = &__buckets[__current_iterator_bkt];

        if (bucket->__last_check.age_since(current_time) < period) {
            return iterator();
        }

        for (; __current_iterator_pos < ENTRIES_PER_BUCKET; __current_iterator_pos++) {
            if (bucket->__sig[__current_iterator_pos] != 0) {
                uint32_t     bkt_key_idx = bucket->__key_pos[__current_iterator_pos];
                const key_t *key         = &__entries.get_key(bkt_key_idx);
                return iterator(bucket, __current_iterator_pos++, __entries.get_data(bkt_key_idx), key);
            }
        }

        bucket->__last_check = current_time;

        __current_iterator_bkt = bucket->__next_iterator;
        __current_iterator_pos = 0;
        bucket                 = &__buckets[__current_iterator_bkt];

        if (bucket->__last_check.age_since(current_time).value() < period) {
            return iterator();
        }

        for (; __current_iterator_pos < ENTRIES_PER_BUCKET; __current_iterator_pos++) {
            if (bucket->__sig[__current_iterator_pos] != 0) {
                uint32_t     bkt_key_idx = bucket->__key_pos[__current_iterator_pos];
                const key_t *key         = &__entries.get_key(bkt_key_idx);
                return iterator(bucket, __current_iterator_pos++, __entries.get_data(bkt_key_idx), key);
            }
        }

        return iterator();
    }

    template <typename T_info>
    void lookup(T_info *info, uint64_t pkts_mask, uint64_t *lookup_hit_mask) {
        // TODO: politics
        uint32_t table_size = get_elements_cnt();

        if (__used[2] == 0 && __used[3] == 0 && __used[4] == 0)
            __lookup_direct_small(info, pkts_mask, lookup_hit_mask);
        else if (table_size < 100000)
            __lookup_direct(info, pkts_mask, lookup_hit_mask);
        else
            __lookup(info, pkts_mask, lookup_hit_mask);
    }

private:
    void __lookup_direct_small(auto *info, uint64_t pkts_mask, uint64_t *lookup_hit_mask) {
        uint64_t hits = 0;

        while (pkts_mask) {
            uint32_t idx = __builtin_ctzll(pkts_mask);
            pkts_mask &= ~(1ULL << idx);

            const key_t *key;
            info[idx].get_key(key);

            uint32_t hash       = key->__hash;
            uint32_t bucket_idx = hash & (__n_buckets - 1);
            uint32_t sig        = (hash >> 16) | 1;

            bucket_t *bkt = &__buckets[bucket_idx];

            if (likely(bkt->__sig[0] == sig && *key == __entries.get_key(bkt->__key_pos[0]))) {
                hits |= (1ULL << idx);
                info[idx].set_entry(__entries.get_data(bkt->__key_pos[0]));
            }
        }

        *lookup_hit_mask = hits;
    }

    void __lookup_direct(auto *info, uint64_t pkts_mask, uint64_t *lookup_hit_mask) {
        uint64_t hits = 0;

        while (pkts_mask) {
            uint32_t idx = __builtin_ctzll(pkts_mask);
            pkts_mask &= ~(1ULL << idx);

            const key_t *key;
            info[idx].get_key(key);

            uint32_t hash       = key->__hash;
            uint32_t bucket_idx = hash & (__n_buckets - 1);
            uint32_t sig        = (hash >> 16) | 1;

            bucket_t *bkt = &__buckets[bucket_idx];

            if (likely(bkt->__sig[0] == sig)) {
                uint32_t key_idx = bkt->__key_pos[0];
                if (*key == __entries.get_key(key_idx)) {
                    hits |= (1ULL << idx);
                    info[idx].set_entry(__entries.get_data(key_idx));
                    goto __next__;
                }
            }

            if (unlikely(bkt->__next_bucket || bkt->__count > 1)) {
                if (bkt->__sig[1] == sig) {
                    uint32_t key_idx = bkt->__key_pos[1];
                    if (*key == __entries.get_key(key_idx)) {
                        hits |= (1ULL << idx);
                        info[idx].set_entry(__entries.get_data(key_idx));
                        goto __next__;
                    }
                }

                if (bkt->__sig[2] == sig) {
                    uint32_t key_idx = bkt->__key_pos[2];
                    if (*key == __entries.get_key(key_idx)) {
                        hits |= (1ULL << idx);
                        info[idx].set_entry(__entries.get_data(key_idx));
                        goto __next__;
                    }
                }

                if (bkt->__sig[3] == sig) {
                    uint32_t key_idx = bkt->__key_pos[3];
                    if (*key == __entries.get_key(key_idx)) {
                        hits |= (1ULL << idx);
                        info[idx].set_entry(__entries.get_data(key_idx));
                        goto __next__;
                    }
                }

                for (bkt = __bucket_next(bkt); bkt; bkt = __bucket_next(bkt)) {
                    for (uint32_t i = 0; i < ENTRIES_PER_BUCKET; i++) {
                        if (bkt->__sig[i] == sig) {
                            uint32_t key_idx = bkt->__key_pos[i];
                            if (*key == __entries.get_key(key_idx)) {
                                hits |= (1ULL << idx);
                                info[idx].set_entry(__entries.get_data(key_idx));
                                goto __next__;
                            }
                        }
                    }
                }
            }
        __next__:;
        }

        *lookup_hit_mask = hits;
    }

    void __lookup_simple_pipelined(auto *info, uint64_t pkts_mask, uint64_t *lookup_hit_mask) {
        uint64_t hits = 0;
        uint32_t indices[PKT_LIMIT];
        uint32_t count = 0;

        uint64_t mask = pkts_mask;
        while (mask) {
            uint32_t idx = __builtin_ctzll(mask);
            mask &= ~(1ULL << idx);
            indices[count++] = idx;

            const key_t *key;
            info[idx].get_key(key);
            rte_prefetch0(key);
        }

        for (uint32_t j = 0; j < count; j++) {
            uint32_t idx = indices[j];

            const key_t *key;
            info[idx].get_key(key);

            uint32_t sig        = (key->__hash >> 16) | 1;
            uint32_t bucket_idx = key->__hash & (__n_buckets - 1);

            for (bucket_t *bkt = &__buckets[bucket_idx]; bkt; bkt = __bucket_next(bkt)) {
                for (uint32_t i = 0; i < ENTRIES_PER_BUCKET; i++) {
                    if (bkt->__sig[i] == sig) {
                        uint32_t key_idx = bkt->__key_pos[i];
                        if (likely(*key == __entries.get_key(key_idx))) {
                            hits |= (1ULL << idx);
                            info[idx].set_entry(__entries.get_data(key_idx));
                            break;
                        }
                    }
                }
            }
        }

        *lookup_hit_mask = hits;
    }

    void __lookup(auto *info, uint64_t pkts_mask, uint64_t *lookup_hit_mask) {
        uint64_t hits = 0;
        uint32_t pkt_indices[PKT_LIMIT];
        uint32_t pkt_count = 0;

        uint64_t mask = pkts_mask;
        while (mask) {
            uint32_t idx = __builtin_ctzll(mask);
            mask &= ~(1ULL << idx);
            pkt_indices[pkt_count++] = idx;

            const key_t *key;
            info[idx].get_key(key);

            rte_prefetch0(key);

            uint32_t  bucket_idx = key->__hash & (__n_buckets - 1);
            bucket_t *bkt        = &__buckets[bucket_idx];
            rte_prefetch0(bkt);

            uint32_t chain_prefetched = 1;
            for (bkt = __bucket_next(bkt); bkt && chain_prefetched < 8; bkt = __bucket_next(bkt)) {
                rte_prefetch0(bkt);
                rte_prefetch0(&__entries.get_key(bkt->__key_pos[0]));
                chain_prefetched++;
            }
        }

        for (uint32_t j = 0; j < pkt_count; j++) {
            uint32_t idx = pkt_indices[j];

            const key_t *key;
            info[idx].get_key(key);

            uint32_t sig        = (key->__hash >> 16) | 1;
            uint32_t bucket_idx = key->__hash & (__n_buckets - 1);

            for (bucket_t *bkt = &__buckets[bucket_idx]; bkt; bkt = __bucket_next(bkt)) {
                rte_prefetch0(bkt);
                if (__bucket_next(bkt)) rte_prefetch0(__bucket_next(bkt));

                if (likely(bkt->__sig[0] == sig)) {
                    uint32_t key_idx = bkt->__key_pos[0];
                    if (likely(*key == __entries.get_key(key_idx))) {
                        hits |= (1ULL << idx);
                        info[idx].set_entry(__entries.get_data(key_idx));
                        goto __next__;
                    }
                }

                if (likely(bkt->__sig[1] == sig)) {
                    uint32_t key_idx = bkt->__key_pos[1];
                    if (likely(*key == __entries.get_key(key_idx))) {
                        hits |= (1ULL << idx);
                        info[idx].set_entry(__entries.get_data(key_idx));
                        goto __next__;
                    }
                }

                if ((bkt->__sig[2] == sig)) {
                    uint32_t key_idx = bkt->__key_pos[2];
                    if ((*key == __entries.get_key(key_idx))) {
                        hits |= (1ULL << idx);
                        info[idx].set_entry(__entries.get_data(key_idx));
                        goto __next__;
                    }
                }

                if (bkt->__sig[3] == sig) {
                    uint32_t key_idx = bkt->__key_pos[3];
                    if (likely(*key == __entries.get_key(key_idx))) {
                        hits |= (1ULL << idx);
                        info[idx].set_entry(__entries.get_data(key_idx));
                        goto __next__;
                    }
                }
            }
        __next__:;
        }

        *lookup_hit_mask = hits;
    }

public:
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
               "    bkt_stack_free: %u (%f%%)\n"
               "    key_stack_free: %u (%f%%)\n"
               "          inserted: %lu\n"
               "            erased: %lu\n"
               "          no space: %lu\n",
               sizeof(key_t),                                                                                  /**/
               get_memory_used(),                                                                              /**/
               get_total_buckets_cnt(),                                                                        /**/
               __used[0],                                                                                      /**/
               __used[1], static_cast<float>(__used[1]) * 100.0 / get_total_buckets_cnt(),                     /**/
               __used[2], static_cast<float>(__used[2]) * 100.0 / get_total_buckets_cnt(),                     /**/
               __used[3], static_cast<float>(__used[3]) * 100.0 / get_total_buckets_cnt(),                     /**/
               __used[4], static_cast<float>(__used[4]) * 100.0 / get_total_buckets_cnt(),                     /**/
               get_elements_cnt(),                                                                             /**/
               get_bucket_stack_size(), static_cast<float>(get_bucket_stack_size()) * 100.0 / __n_buckets_ext, /**/
               get_key_stack_size(), static_cast<float>(get_key_stack_size()) * 100.0 / __n_keys,              /**/
               __inserted,                                                                                     /**/
               __erased,                                                                                       /**/
               __no_space /**/);
    }

private:
    bucket_t       *__bucket_next(bucket_t *b) const { return reinterpret_cast<bucket_t *>(b->__next_bucket & (~1LU)); }
    const bucket_t *__bucket_next(const bucket_t *b) const {
        return reinterpret_cast<bucket_t *>(b->__next_bucket & (~1LU));
    }

    uint64_t __bucket_next_valid(bucket_t *b) const { return static_cast<uint64_t>(b->__next_bucket & 1LU); }
    void     __bucket_next_set(bucket_t *b, bucket_t *next) const { b->__next_bucket = (uintptr_t)(next) | 1LU; }
    void     __bucket_next_set_null(bucket_t *b) const { b->__next_bucket = 0; }
    void     __bucket_next_copy(bucket_t *b1, bucket_t *b2) const { b1->__next_bucket = b2->__next_bucket; }

    void __rem(bucket_t *bucket, uint32_t pos) {
        uint32_t bkt_key_idx{bucket->__key_pos[pos]};

        __entries.delete_data(bkt_key_idx);

        bucket->__sig[pos]             = 0;
        __key_stack[__key_stack_pos++] = bkt_key_idx;

        __used[bucket->__count]--;
        bucket->__count--;
        __used[bucket->__count]++;

        __erased++;

        if (bucket->__count == 0) {
            bucket_t *iter_bucket = &__buckets[__current_iterator_bkt];
            if (__current_iterator_bkt == iter_bucket->__next_iterator) {
                __current_iterator_bkt = INVALID_BUCKET_INDEX;
            } else {
                bucket_t *prev_bucket = &__buckets[bucket->__prev_iterator];
                bucket_t *next_bucket = &__buckets[bucket->__next_iterator];

                prev_bucket->__next_iterator = bucket->__next_iterator;
                next_bucket->__prev_iterator = bucket->__prev_iterator;
                if (iter_bucket == bucket) {
                    __current_iterator_bkt = bucket->__next_iterator;
                    __current_iterator_pos = 0;
                }
            }

            uint32_t bucket_idx = bucket - __buckets;
            if (bucket_idx >= __n_buckets) {
                bucket_t *prev_bucket = bucket->__prev_bucket;
                __bucket_next_copy(prev_bucket, bucket);

                if (__bucket_next_valid(bucket)) {
                    bucket_t *bucket_next      = __bucket_next(bucket);
                    bucket_next->__prev_bucket = prev_bucket;
                }

                // memset(reinterpret_cast<void *>(bucket), 0, sizeof(bucket_t));
                __bkt_stack[__bkt_stack_pos++] = bucket_idx;
            }
        }
    }

public:
    uint32_t get_elements_cnt() const {
        uint32_t res{0};
        for (uint32_t i = 0; i <= ENTRIES_PER_BUCKET; i++) {
            res += __used[i] * i;
        }
        return res;
    }

    uint32_t get_total_buckets_cnt() const { return __n_buckets + __n_buckets_ext; }
    uint32_t get_free_buckers_cnt() const { return __used[0]; }
    uint32_t get_1_buckets_cnt() const { return __used[1]; }
    uint32_t get_2_buckets_cnt() const { return __used[2]; }
    uint32_t get_3_buckets_cnt() const { return __used[3]; }
    uint32_t get_4_buckets_cnt() const { return __used[4]; }
    uint32_t get_bucket_stack_size() const { return __bkt_stack_pos; }
    uint32_t get_key_stack_size() const { return __key_stack_pos; }
    uint64_t get_insert_cnt() const { return __inserted; }
    uint64_t get_erase_cnt() const { return __erased; }
    uint64_t get_no_space_cnt() const { return __no_space; }
    uint64_t get_memory_used() const { return __memory_used; }
};

template <typename T_key, typename T_traits, size_t PKT_LIMIT>
using flow_container_v1 = flow_container_v1_impl<T_key, T_traits, entries_separate, PKT_LIMIT>;
#endif
