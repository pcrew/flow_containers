#include <benchmark/benchmark.h>
#include <unordered_map>

#include <boost/unordered/unordered_map.hpp>

#include <tbb/concurrent_hash_map.h>

#include <absl/hash/hash.h>
#include <absl/container/flat_hash_map.h>
#include <absl/container/node_hash_map.h>

#include <vector>
#include <random>

#include <rte_eal.h>
#include <rte_hash.h>
#include <rte_cycles.h>
#include <rte_random.h>

#include "../containers/flow_container_v1.h"
#include "../containers/flow_container_v2.h"
#include "../containers/flow_container_v3.h"
#include "../common/flow_traits.h"
#include "../common/flow_key.h"

#include "flat_hash_map.hpp"

constexpr uint32_t BATCH_SIZE{32};

struct flow_key_hash {
    std::size_t operator()(const flow_key_t &k) const { return k.get_flow_hash(); }
};

struct pkt_info {
    flow_key_t key;
    uint32_t   hash;
    flow_data *data;

    void     get_key(const flow_key_t *&k) const { k = &key; }
    uint32_t lookup_hash() const { return hash; }
    void     set_entry(flow_data *d) { data = d; }
    void     prefetch() const {
        if (data) rte_prefetch0(data);
    }
};

class ContainerBenchmark : public benchmark::Fixture {
protected:
    void SetUp(const benchmark::State &state) override {
        static bool eal_initialized = false;
        if (!eal_initialized) {
            char *argv[] = {strdup("benchmark"), strdup("--no-pci"), nullptr};
            int   argc   = 2;
            rte_eal_init(argc, argv);
            eal_initialized = true;
        }

        int num_keys = state.range(0);
        __keys.reserve(num_keys);

        std::mt19937                            rng(42);
        std::uniform_int_distribution<uint32_t> ip_dist(0, 0xFFFFFFFF);
        std::uniform_int_distribution<uint16_t> port_dist(0, 0xFFFF);
        std::uniform_int_distribution<int>      proto_dist(0, 1);

        for (int i = 0; i < num_keys; i++) {
            flow_key_t key;
            key.__src_ip   = ip_dist(rng);
            key.__dst_ip   = ip_dist(rng);
            key.__src_port = port_dist(rng);
            key.__dst_port = port_dist(rng);
            key.__proto    = proto_dist(rng) ? IPPROTO_TCP : IPPROTO_UDP;
            __keys.push_back(key);
        }
    }

    void TearDown(const benchmark::State &state) override { __keys.clear(); }

    std::vector<flow_key_t> __keys;
};

BENCHMARK_DEFINE_F(ContainerBenchmark, BM_flow_container_v1)(benchmark::State &state) {
    flow_container_v1<flow_key_t, flow_traits, 64> container;
    container.init("bench", 23, 65536 * 8, 10500000, 0);

    for (uint32_t i = 0; i < __keys.size(); i++) {
        container.add(__keys[i], i);
    }

    // container.print_stat();

    uint64_t           hit_mask;
    constexpr uint64_t PKTS_MASK = BATCH_SIZE == 64 ? ~0LLU : (1LLU << BATCH_SIZE) - 1;
    pkt_info           pkts[BATCH_SIZE];

    for (auto _ : state) {
        for (uint64_t i = 0; i < __keys.size(); i += BATCH_SIZE) {
            for (uint64_t j = 0; j < BATCH_SIZE && i + j < __keys.size(); j++) {
                pkts[j].key  = __keys[i + j];
                pkts[j].hash = __keys[i + j].get_flow_hash();
                pkts[j].data = nullptr;
            }

            container.lookup(pkts, PKTS_MASK, &hit_mask);
#if 0
            if (hit_mask != PKTS_MASK) {
                std::cout << i << std::endl;
                std::cout << std::bitset<32>(hit_mask) << std::endl;
                exit(1);
            }
#endif
            benchmark::DoNotOptimize(hit_mask);
        }
    }

    state.SetItemsProcessed(state.iterations() * __keys.size());
    state.SetLabel("flow_container_v1");
}

BENCHMARK_DEFINE_F(ContainerBenchmark, BM_flow_container_v2)(benchmark::State &state) {
    flow_container_v2<flow_key_t, flow_traits, 64> container;
    container.init("bench", 19, 65536 * 8, 10500000, 0);

    for (uint32_t i = 0; i < __keys.size(); i++) {
        container.add(__keys[i], i);
    }

    // container.print_stat();

    uint64_t           hit_mask;
    constexpr uint64_t PKTS_MASK = BATCH_SIZE == 64 ? ~0LLU : (1LLU << BATCH_SIZE) - 1;
    pkt_info           pkts[BATCH_SIZE];

    for (auto _ : state) {
        for (uint64_t i = 0; i < __keys.size(); i += BATCH_SIZE) {
            for (uint64_t j = 0; j < BATCH_SIZE && i + j < __keys.size(); j++) {
                pkts[j].key  = __keys[i + j];
                pkts[j].hash = __keys[i + j].get_flow_hash();
                pkts[j].data = nullptr;
            }

            container.lookup(pkts, PKTS_MASK, &hit_mask);
#if 0
            if (hit_mask != PKTS_MASK) {
                std::cout << i << std::endl;
                std::cout << std::bitset<32>(hit_mask) << std::endl;
                exit(1);
            }
#endif
            benchmark::DoNotOptimize(hit_mask);
        }
    }

    state.SetItemsProcessed(state.iterations() * __keys.size());
    state.SetLabel("flow_container_v2");
}

BENCHMARK_DEFINE_F(ContainerBenchmark, BM_flow_container_v3)(benchmark::State &state) {
    flow_container_v3<flow_key_t, flow_traits, 64> container;
    container.init("bench", 19, 65536 * 8, 10500000, 0);

    for (uint32_t i = 0; i < __keys.size(); i++) {
        container.add(__keys[i], i);
    }

    // container.print_stat();

    uint64_t           hit_mask;
    constexpr uint64_t PKTS_MASK = BATCH_SIZE == 64 ? ~0LLU : (1LLU << BATCH_SIZE) - 1;
    pkt_info           pkts[BATCH_SIZE];

    for (auto _ : state) {
        for (uint64_t i = 0; i < __keys.size(); i += BATCH_SIZE) {
            for (uint64_t j = 0; j < BATCH_SIZE && i + j < __keys.size(); j++) {
                pkts[j].key  = __keys[i + j];
                pkts[j].hash = __keys[i + j].get_flow_hash();
                pkts[j].data = nullptr;
            }

            container.lookup(pkts, PKTS_MASK, &hit_mask);
#if 0
            if (hit_mask != PKTS_MASK) {
                std::cout << i << std::endl;
                std::cout << std::bitset<32>(hit_mask) << std::endl;
                exit(1);
            }
#endif
            benchmark::DoNotOptimize(hit_mask);
        }
    }

    state.SetItemsProcessed(state.iterations() * __keys.size());
    state.SetLabel("flow_container_v3");
}

BENCHMARK_DEFINE_F(ContainerBenchmark, BM_rte_hash)(benchmark::State &state) {
    rte_hash_parameters params = {.name       = "rte_hash_bench",
                                  .entries    = static_cast<uint32_t>(__keys.size() * 1.5), // немного с запасом
                                  .key_len    = sizeof(flow_key_t),
                                  .socket_id  = rte_socket_id(),
                                  .extra_flag = 0};

    rte_hash *hash = ::rte_hash_create(&params);
    if (!hash) {
        state.SkipWithError("Failed to create rte_hash");
        return;
    }

    for (uint32_t i = 0; i < __keys.size(); i++) {
        int ret = rte_hash_add_key_data(hash, &__keys[i], reinterpret_cast<flow_data *>(flow_traits::allocate()));
        // int ret = rte_hash_add_key_data(hash, &__keys[i], NULL);
        if (ret < 0) {
            state.SkipWithError("Failed to insert into rte_hash");
            rte_hash_free(hash);
            return;
        }
    }

    const void *keys_batch[BATCH_SIZE];
    void       *data_batch[BATCH_SIZE];
    uint64_t    hit_mask;

    for (auto _ : state) {
        for (uint32_t i = 0; i < __keys.size(); i += BATCH_SIZE) {
            uint32_t j = 0;
            for (j = 0; j < BATCH_SIZE && j + i < __keys.size(); j++) {
                keys_batch[j] = &__keys[i + j];
            }

            if (j > 0) {
                auto ret = rte_hash_lookup_bulk_data(hash, keys_batch, j, &hit_mask, data_batch);
                if (hit_mask != ((1LLU << j) - 1)) {
                    exit(1);
                }
                benchmark::DoNotOptimize(ret);
                benchmark::DoNotOptimize(hit_mask);
            }
        }
    }

    state.SetItemsProcessed(state.iterations() * __keys.size());
    state.SetLabel("DPDK rte_hash");

    rte_hash_free(hash);
}

BENCHMARK_DEFINE_F(ContainerBenchmark, BM_stl_unordered_map)(benchmark::State &state) {
    std::unordered_map<flow_key_t, flow_data, flow_key_hash> map;

    for (uint32_t i = 0; i < __keys.size(); i++) {
        map[__keys[i]] = flow_data();
    }

    for (auto _ : state) {
        for (const auto &key : __keys) {
            auto it = map.find(key);
            benchmark::DoNotOptimize(it);
        }
    }

    state.SetItemsProcessed(state.iterations() * __keys.size());
    state.SetLabel("std::unordered_map");
}

BENCHMARK_DEFINE_F(ContainerBenchmark, BM_boost_unordered_map)(benchmark::State &state) {
    boost::unordered_map<flow_key_t, flow_data, flow_key_hash> map;

    for (uint32_t i = 0; i < __keys.size(); i++) {
        map[__keys[i]] = flow_data();
    }

    for (auto _ : state) {
        for (const auto &key : __keys) {
            auto it = map.find(key);
            benchmark::DoNotOptimize(it);
        }
    }

    state.SetItemsProcessed(state.iterations() * __keys.size());
    state.SetLabel("boost::unordered_map");
}

struct flow_key_compare {
    std::size_t hash(const flow_key_t &k) const { return k.get_flow_hash(); }
    bool        equal(const flow_key_t &a, const flow_key_t &b) const { return a == b; }
};

BENCHMARK_DEFINE_F(ContainerBenchmark, BM_tbb_hash_map)(benchmark::State &state) {
    tbb::concurrent_hash_map<flow_key_t, flow_data, flow_key_compare> map;

    for (uint32_t i = 0; i < __keys.size(); i++) {
        map.insert(std::make_pair(__keys[i], flow_data()));
    }

    for (auto _ : state) {
        for (const auto &key : __keys) {
            tbb::concurrent_hash_map<flow_key_t, flow_data, flow_key_compare>::const_accessor it;

            bool found = map.find(it, key);
            benchmark::DoNotOptimize(found);
        }
    }

    state.SetItemsProcessed(state.iterations() * __keys.size());
    state.SetLabel("tbb::concurrent_hash_map");
}

BENCHMARK_DEFINE_F(ContainerBenchmark, BM_abseil_node_hash_map)(benchmark::State &state) {
    absl::node_hash_map<flow_key_t, flow_data, flow_key_hash> map;
    map.reserve(__keys.size());

    for (uint32_t i = 0; i < __keys.size(); i++) {
        map[__keys[i]] = flow_data();
    }

    for (auto _ : state) {
        for (const auto &key : __keys) {
            auto it = map.find(key);
            benchmark::DoNotOptimize(it);
        }
    }

    state.SetItemsProcessed(state.iterations() * __keys.size());
    state.SetLabel("absl::flat_hash_map");
}

BENCHMARK_DEFINE_F(ContainerBenchmark, BM_ska_flat_hash_map)(benchmark::State &state) {
    ska::flat_hash_map<flow_key_t, flow_data, flow_key_hash> map;

    for (uint32_t i = 0; i < __keys.size(); i++) {
        map[__keys[i]] = flow_data();
    }

    for (auto _ : state) {
        for (const auto &key : __keys) {
            auto it = map.find(key);
            benchmark::DoNotOptimize(it);
        }
    }

    state.SetItemsProcessed(state.iterations() * __keys.size());
    state.SetLabel("ska::flat_hash_map");
}

#if 1
BENCHMARK_REGISTER_F(ContainerBenchmark, BM_rte_hash)
    ->RangeMultiplier(10)
    ->Range(1000, 1000000)
    ->Unit(benchmark::kNanosecond)
    ->Complexity();

BENCHMARK_REGISTER_F(ContainerBenchmark, BM_flow_container_v1)
    ->RangeMultiplier(10)
    ->Range(1000, 1000000)
    ->Unit(benchmark::kNanosecond)
    ->Complexity();

BENCHMARK_REGISTER_F(ContainerBenchmark, BM_flow_container_v2)
    ->RangeMultiplier(10)
    ->Range(1000, 1000000)
    ->Unit(benchmark::kNanosecond)
    ->Complexity();

BENCHMARK_REGISTER_F(ContainerBenchmark, BM_flow_container_v3)
    ->RangeMultiplier(10)
    ->Range(1000, 1000000)
    ->Unit(benchmark::kNanosecond)
    ->Complexity();

BENCHMARK_REGISTER_F(ContainerBenchmark, BM_stl_unordered_map)
    ->RangeMultiplier(10)
    ->Range(1000, 1000000)
    ->Unit(benchmark::kNanosecond)
    ->Complexity();

BENCHMARK_REGISTER_F(ContainerBenchmark, BM_boost_unordered_map)
    ->RangeMultiplier(10)
    ->Range(1000, 1000000)
    ->Unit(benchmark::kNanosecond)
    ->Complexity();

BENCHMARK_REGISTER_F(ContainerBenchmark, BM_tbb_hash_map)
    ->RangeMultiplier(10)
    ->Range(1000, 1000000)
    ->Unit(benchmark::kNanosecond)
    ->Complexity();

BENCHMARK_REGISTER_F(ContainerBenchmark, BM_abseil_node_hash_map)
    ->RangeMultiplier(10)
    ->Range(1000, 1000000)
    ->Unit(benchmark::kNanosecond)
    ->Complexity();

BENCHMARK_REGISTER_F(ContainerBenchmark, BM_ska_flat_hash_map)
    ->RangeMultiplier(10)
    ->Range(1000, 1000000)
    ->Unit(benchmark::kNanosecond)
    ->Complexity();
#else
BENCHMARK_REGISTER_F(ContainerBenchmark, BM_ska_flat_hash_map)
    ->RangeMultiplier(10)
    ->Range(1000000, 1000000)
    ->Unit(benchmark::kNanosecond)
    ->Complexity();
#endif

BENCHMARK_MAIN();
