
#include <atomic>
#include <algorithm>
#include <gtest/gtest.h>

#include <rte_eal.h>
#include <rte_mempool.h>

#include "../containers/flow_container_v1.h"
#include "../containers/flow_container_v2.h"
#include "../containers/flow_container_v3.h"
#include "../common/flow_traits.h"
#include "../common/flow_key.h"
#include "../common/compiler.h"

static std::atomic<bool> eal_initialized{false};

namespace std {
template <>
struct less<flow_key_t> {
    bool operator()(const flow_key_t &lhs, const flow_key_t &rhs) const {
        if (lhs.__src_ip != rhs.__src_ip) return lhs.__src_ip < rhs.__src_ip;
        if (lhs.__dst_ip != rhs.__dst_ip) return lhs.__dst_ip < rhs.__dst_ip;
        if (lhs.__src_port != rhs.__src_port) return lhs.__src_port < rhs.__src_port;
        if (lhs.__dst_port != rhs.__dst_port) return lhs.__dst_port < rhs.__dst_port;
        return lhs.__proto < rhs.__proto;
    }
};
} // namespace std

class flow_container_test : public ::testing::Test {
protected:
    using container_t = flow_container_v3<flow_key_t, flow_traits, 64>;

    struct test_info {
        flow_key_t key;
        flow_data *data;
        void       get_key(const flow_key_t *&k) const { k = &key; }
        void       set_entry(flow_data *d) { data = d; }
    };

    void SetUp() override {
        if (!eal_initialized) {
            char *argv[] = {strdup("flow_container_test"), strdup("--no-pci"), strdup("--log-level=0"), nullptr};

            int argc = 1;
            int rc   = ::rte_eal_init(argc, argv);
            ASSERT_GE(rc, 0) << "Failed to initialize EAL";
            eal_initialized = true;
        }

        __container.init("test_table", 12, 1024, 1000000, 0);
    }

    flow_key_t __create_key(uint32_t src_ip = 0xC0A80101, uint32_t dst_ip = 0xC0A80102, uint16_t src_port = 12345,
                            uint16_t dst_port = 54321, uint8_t proto = IPPROTO_TCP, uint32_t hash = 0) {
        flow_key_t key;

        key.__src_ip   = src_ip;
        key.__dst_ip   = dst_ip;
        key.__src_port = src_port;
        key.__dst_port = dst_port;
        key.__proto    = proto;
        key.__hash     = hash ? hash : key.get_flow_hash();

        return key;
    }

    flow_key_t __create_key_predict(uint32_t id, uint32_t hash_mod = 0) {
        flow_key_t key;

        key.__src_ip   = 0xC0A80000 + (id & 0xFF);
        key.__dst_ip   = 0xC0A90000 + ((id >> 8) & 0xFF);
        key.__src_port = 1024 + (id % 0xFFFF);
        key.__dst_port = 2048 + ((id >> 16) & 0xFFFF);
        key.__proto    = (id % 2) ? IPPROTO_TCP : IPPROTO_UDP;
        key.__hash     = hash_mod ? (key.get_flow_hash() & ~0xFF) | (id % hash_mod) : key.get_flow_hash();

        return key;
    }

    void __validate_flow(const flow_data *actual, const flow_data &expected) {
        ASSERT_NE(actual, nullptr);
        EXPECT_EQ(actual->packets, expected.packets);
        EXPECT_EQ(actual->bytes, expected.bytes);
        EXPECT_EQ(actual->first_seen, expected.first_seen);
        EXPECT_EQ(actual->last_seen, expected.last_seen);
    }

    container_t __container;
};

TEST_F(flow_container_test, empty_container) {
    EXPECT_EQ(__container.get_elements_cnt(), 0);
    EXPECT_FALSE(__container.get_next().is_valid());
}

TEST_F(flow_container_test, single_insert) {

    auto key = __create_key(0xC0A80101 /* 192.168.1.1 */
                            ,
                            0xC0A80102 /* 192.168.1.2 */
                            ,
                            12345, 54321, IPPROTO_TCP);

    auto current_time = ::rte_rdtsc();

    auto [it, res] = __container.add(key, current_time);

    EXPECT_EQ(res, insert_result::INSERTED);
    EXPECT_TRUE(it.is_valid());
    EXPECT_NE(it.data(), nullptr);
    EXPECT_EQ(*it.key(), key);
    EXPECT_EQ(__container.get_elements_cnt(), 1);

    it.data()->packets = 42;
    EXPECT_EQ(it.data()->packets, 42);
}

TEST_F(flow_container_test, duplicate_insert) {
    auto key = __create_key(0xC0A80101 /* 192.168.1.1 */
                            ,
                            0xC0A80102 /* 192.168.1.2 */
                            ,
                            12345, 54321, IPPROTO_TCP);

    auto current_time = ::rte_rdtsc();

    auto [it1, res1] = __container.add(key, current_time);
    EXPECT_EQ(res1, insert_result::INSERTED);

    auto [it2, res2] = __container.add(key, current_time);
    EXPECT_EQ(res2, insert_result::ALREADY_EXISTS);
    EXPECT_TRUE(it2.is_valid());
    EXPECT_EQ(*it2.key(), key);
    EXPECT_EQ(__container.get_elements_cnt(), 1);
}

TEST_F(flow_container_test, multiply_inserts) {
    std::vector<flow_key_t> keys;
    for (uint32_t i = 0; i < 1000; i++) {
        auto key = __create_key(0xC0A80000 + i, 0xC0A90000, 8080 + i, 80, IPPROTO_TCP);
        keys.push_back(key);

        auto [it, res] = __container.add(key, ::rte_rdtsc());
        ASSERT_EQ(res, insert_result::INSERTED);
        ASSERT_NE(it.data(), nullptr);
    }

    ASSERT_EQ(__container.get_elements_cnt(), 1000);

    std::set<flow_key_t> found_keys;
    auto                 it    = __container.get_next();
    auto                 first = it;

    ASSERT_TRUE(first.is_valid());

    do {
        found_keys.insert(*it.key());
        it = __container.get_next();
    } while (it != first);

    EXPECT_EQ(found_keys.size(), 1000);
}

TEST_F(flow_container_test, erase) {
    auto key = __create_key(0xC0A80101 /* 192.168.1.1 */
                            ,
                            0xC0A80102 /* 192.168.1.2 */
                            ,
                            12345, 54321, IPPROTO_TCP);

    auto [it, res] = __container.add(key, ::rte_rdtsc());
    ASSERT_EQ(res, insert_result::INSERTED);
    uint32_t size_before = __container.get_elements_cnt();

    bool erased = __container.erase(key);
    EXPECT_TRUE(erased);
    EXPECT_EQ(__container.get_elements_cnt(), size_before - 1);
}

TEST_F(flow_container_test, collisions) {
    uint32_t collision_hash = 0x12345678;

    std::vector<flow_key_t> keys;
    uint32_t                inserted = 0;

    for (uint32_t i = 0; i < 2000; i++) {
        auto key       = __create_key(0xC0A80000 + i, 0xC0A90000, 8080 + i, 80, IPPROTO_TCP, collision_hash);
        auto [it, res] = __container.add(key, ::rte_rdtsc());
        if (res == insert_result::INSERTED) {
            inserted++;
            keys.push_back(key);
        }
    }

    std::set<flow_key_t> found_keys;
    auto                 it    = __container.get_next();
    auto                 first = it;

    do {
        found_keys.insert(*it.key());
        it = __container.get_next();
    } while (it != first);

    EXPECT_EQ(inserted, found_keys.size());

    EXPECT_EQ(inserted, __container.get_4_buckets_cnt() * 4);
    EXPECT_EQ(0, __container.get_3_buckets_cnt());
    EXPECT_EQ(0, __container.get_2_buckets_cnt());
    EXPECT_EQ(0, __container.get_1_buckets_cnt());
}

TEST_F(flow_container_test, bucket_overflow) {
    auto first_key = __create_key();

    uint32_t bkt_idx = first_key.__hash & ((1 << 8) - 1);

    std::vector<flow_key_t> keys;
    for (uint32_t i = 0; i < 500; i++) {
        flow_key_t key = __create_key(0xC0A80000 + i, 0xC0A90000 + i, 80 + i, 80 + i, IPPROTO_TCP);
        key.__hash     = (key.__hash & ~((1 << 8) - 1)) | bkt_idx;

        auto [it, res] = __container.add(key, ::rte_rdtsc());
        if (res == insert_result::INSERTED) {
            keys.push_back(key);
        }
    }

    std::set<flow_key_t> found_keys;
    auto                 it    = __container.get_next();
    auto                 first = it;

    do {
        found_keys.insert(*it.key());
        it = __container.get_next();
    } while (it != first);

    EXPECT_EQ(found_keys.size(), keys.size());
}

TEST_F(flow_container_test, no_memory) {
    container_t small_container;
    small_container.init("small container", 4, 16, 100, 0);

    uint32_t inserted = 0;

    for (uint32_t i = 0; i < 200; i++) {
        auto key = __create_key(0xC0A80000 + i, 0xC0A90000, i, 80, IPPROTO_TCP);

        auto [it, res] = small_container.add(key, ::rte_rdtsc());
        if (res == insert_result::INSERTED) {
            inserted++;
        } else if (res == insert_result::NO_MEMORY) {
            break;
        }
    }

    EXPECT_LE(inserted, 100);
    EXPECT_EQ(small_container.get_elements_cnt(), inserted);
    EXPECT_GT(small_container.get_no_space_cnt(), 0);
}

TEST_F(flow_container_test, basic_lookup) {
    constexpr uint32_t      BURST_SIZE{24};
    std::vector<flow_key_t> keys;

    for (uint32_t i = 0; i < BURST_SIZE; i++) {
        auto key = __create_key(0xC0A80000 + i, 0xC0A90000, i, 80, IPPROTO_TCP);
        keys.push_back(key);
        __container.add(key, ::rte_rdtsc());
    }

    test_info infos[BURST_SIZE];

    for (uint32_t i = 0; i < BURST_SIZE; i++) {
        infos[i].key  = keys[i];
        infos[i].data = nullptr;
    }

    uint64_t hit_mask = 0;
    __container.lookup(infos, (1ULL << BURST_SIZE) - 1, &hit_mask);

    EXPECT_EQ(hit_mask, (1ULL << BURST_SIZE) - 1);
    for (uint32_t i = 0; i < BURST_SIZE; i++) {
        EXPECT_NE(infos[i].data, nullptr);
    }
}

TEST_F(flow_container_test, lookup_miss) {
    constexpr uint32_t      BURST_SIZE{32};
    std::vector<flow_key_t> existing_keys;

    for (uint32_t i = 0; i < BURST_SIZE / 2; i++) {
        auto key = __create_key(0xC0A80000 + i, 0xC0A90000, i, 80, IPPROTO_TCP);
        existing_keys.push_back(key);
        __container.add(key, ::rte_rdtsc());
    }

    test_info infos[BURST_SIZE];

    for (uint32_t i = 0; i < BURST_SIZE / 2; i++) {
        infos[i].key  = existing_keys[i];
        infos[i].data = nullptr;
    }

    for (uint32_t i = BURST_SIZE / 2; i < BURST_SIZE; i++) {
        infos[i].key  = __create_key(0xDEADDEAD + i, 0xDEADDEAD, i, 80, IPPROTO_TCP);
        infos[i].data = nullptr;
    }

    uint64_t hit_mask = 0;
    __container.lookup(infos, (1ULL << BURST_SIZE) - 1, &hit_mask);

    for (uint32_t i = 0; i < BURST_SIZE / 2; i++) {
        EXPECT_NE(infos[i].data, nullptr);
    }

    for (uint32_t i = BURST_SIZE / 2; i < BURST_SIZE; i++) {
        EXPECT_EQ(infos[i].data, nullptr);
    }
}

TEST_F(flow_container_test, empty_iterator) { EXPECT_FALSE(__container.get_next().is_valid()); }

TEST_F(flow_container_test, complex_operations) {
    std::map<flow_key_t, int> ref;

    for (uint32_t round = 0; round < 5; round++) {
        for (uint32_t i = 0; i < 20; i++) {
            auto key       = __create_key(0xBAADF00D + round * 100 + i, 0xDEADBEEF, 1000 + i, 2000 + i, IPPROTO_UDP);
            auto [it, res] = __container.add(key, ::rte_rdtsc());

            if (res == insert_result::INSERTED) {
                ref[key]           = 1;
                it.data()->packets = round * 100 + i;
            }
        }

        uint32_t deleted = 0;
        for (auto it = ref.begin(); it != ref.end();) {
            if (deleted++ < 10) {
                __container.erase(it->first);
                it = ref.erase(it);
            } else {
                ++it;
            }
        }

        EXPECT_EQ(__container.get_elements_cnt(), ref.size());
    }
}

TEST_F(flow_container_test, insert_with_validation) {
    auto     key          = __create_key_predict(42);
    uint64_t current_time = ::rte_rdtsc();

    {
        auto [it, res] = __container.add(key, current_time);
        ASSERT_EQ(res, insert_result::INSERTED);
        ASSERT_TRUE(it.is_valid());
        ASSERT_NE(it.data(), nullptr);

        it.data()->packets    = 142;
        it.data()->bytes      = 54321;
        it.data()->first_seen = current_time;
        it.data()->last_seen  = current_time;
        it.data()->tcp_state  = 5;
    }

    bool found = false;

    auto it    = __container.get_next();
    auto first = it;
    do {
        if (*it.key() == key) {
            found = true;
            __validate_flow(it.data(), {142, 54321, current_time, current_time, 5});
            break;
        }
        it = __container.get_next();
    } while (it != first);

    EXPECT_TRUE(found);
}

TEST_F(flow_container_test, multiple_inserts_with_data_validation) {

    struct test_flow {
        flow_key_t key;
        uint64_t   time;
        uint64_t   packets;
        uint64_t   bytes;
    };

    std::vector<test_flow> flows;
    for (uint32_t i = 0; i < 1000; i++) {
        test_flow tf;

        tf.key     = __create_key_predict(i);
        tf.time    = ::rte_rdtsc();
        tf.packets = i * 20;
        tf.bytes   = i * 100;

        auto [it, res] = __container.add(tf.key, tf.time);
        ASSERT_EQ(res, insert_result::INSERTED);

        it.data()->packets    = tf.packets;
        it.data()->bytes      = tf.bytes;
        it.data()->first_seen = tf.time;
        it.data()->last_seen  = tf.time;

        flows.push_back(tf);
    }

    for (const auto &tf : flows) {
        auto [it, res] = __container.add(tf.key, 0);
        EXPECT_EQ(res, insert_result::ALREADY_EXISTS);
        __validate_flow(it.data(), {tf.packets, tf.bytes, tf.time, tf.time, 0});
    }

    std::set<flow_key_t> found_keys;
    auto                 it    = __container.get_next();
    auto                 first = it;

    do {
        found_keys.insert(*it.key());
        auto test_it =
            std::find_if(flows.begin(), flows.end(), [&](const test_flow &tf) { return tf.key == *it.key(); });
        if (test_it != flows.end()) {
            __validate_flow(it.data(), {test_it->packets, test_it->bytes, test_it->time, test_it->time, 0});
        }

        it = __container.get_next();
    } while (it != first);

    EXPECT_EQ(found_keys.size(), flows.size());
}

TEST_F(flow_container_test, update_existing_data) {
    auto key   = __create_key_predict(42);
    auto time1 = ::rte_rdtsc();

    {
        auto [it, res] = __container.add(key, time1);
        ASSERT_EQ(res, insert_result::INSERTED);

        it.data()->packets    = 142;
        it.data()->bytes      = 1560;
        it.data()->first_seen = time1;
        it.data()->last_seen  = time1;
    }

    auto time2 = ::rte_rdtsc();
    {
        auto [it, res] = __container.add(key, time2);
        ASSERT_EQ(res, insert_result::ALREADY_EXISTS);
        it.data()->packets += 10;
        it.data()->bytes += 100;
        it.data()->last_seen = time2;

        __validate_flow(it.data(), {142 + 10, 1560 + 100, time1, time2, 0});
    }
}

TEST_F(flow_container_test, collision_with_validation) {
    uint32_t collision_hash = 0xDEADDEAD;

    std::map<flow_key_t, uint64_t> packets_count;

    for (uint32_t i = 0; i < 500; i++) {
        auto key   = __create_key_predict(i, ::rte_rdtsc());
        key.__hash = collision_hash;

        auto [it, res] = __container.add(key, ::rte_rdtsc());
        if (res == insert_result::INSERTED) {
            packets_count[key] = i * 5;
            it.data()->packets = i * 5;
            it.data()->bytes   = i * 1024;
        }
    }

    uint32_t found = 0;
    auto     it    = __container.get_next();
    auto     first = it;

    do {
        auto packet_it = packets_count.find(*it.key());
        if (packet_it != packets_count.end()) {
            EXPECT_EQ(it.data()->packets, packet_it->second);
            found++;
        }
        it = __container.get_next();
    } while (it != first);

    EXPECT_EQ(found, packets_count.size());

    EXPECT_EQ(__container.get_elements_cnt(), 500);
    EXPECT_EQ(__container.get_4_buckets_cnt(), 125);
}

TEST_F(flow_container_test, delete_with_validation) {
    std::vector<flow_key_t> keys;

    for (uint32_t i = 0; i < 100; i++) {
        auto key = __create_key_predict(i);
        keys.push_back(key);

        auto [it, res]     = __container.add(key, ::rte_rdtsc());
        it.data()->packets = i;
    }

    std::set<flow_key_t> deleted_keys;
    for (uint32_t i = 0; i < 100; i += 2) {
        bool erased = __container.erase(keys[i]);
        EXPECT_TRUE(erased);
        deleted_keys.insert(keys[i]);
    }

    for (uint32_t i = 0; i < 100; i++) {
        auto [it, res] = __container.add(keys[i], ::rte_rdtsc());
        if (deleted_keys.count(keys[i])) {
            EXPECT_EQ(res, insert_result::INSERTED);
            it.data()->packets = i * 2;
        } else {
            EXPECT_EQ(res, insert_result::ALREADY_EXISTS);
            EXPECT_EQ(it.data()->packets, i);
        }
    }

    EXPECT_EQ(__container.get_elements_cnt(), 100);
}

TEST_F(flow_container_test, lookup_hit_validation) {
    std::vector<flow_key_t> keys;

    for (uint32_t i = 0; i < 20; i++) {
        auto key = __create_key_predict(i);
        keys.push_back(key);

        auto [it, res]     = __container.add(key, ::rte_rdtsc());
        it.data()->packets = i * 10;
        it.data()->bytes   = i * 1024;
    }

    test_info pkts[16];
    for (uint32_t i = 0; i < 8; i++) {
        pkts[i].key  = keys[i * 2];
        pkts[i].data = nullptr;
    }

    uint64_t hit_mask;
    __container.lookup(pkts, (1 << 8) - 1, &hit_mask);

    EXPECT_EQ(hit_mask, (1 << 8) - 1);
    for (uint32_t i = 0; i < 8; i++) {
        EXPECT_NE(pkts[i].data, nullptr);
        EXPECT_EQ(pkts[i].data->packets, i * 2 * 10);
        EXPECT_EQ(pkts[i].data->bytes, i * 2 * 1024);
    }

    for (uint32_t i = 0; i < 8; i++) {
        if (i % 2 == 0) {
            pkts[i].key = keys[i];
        } else {
            pkts[i].key = __create_key_predict(10000 + i);
        }
        pkts[i].data = nullptr;
    }

    __container.lookup(pkts, (1 << 8) - 1, &hit_mask);
    EXPECT_EQ(hit_mask, 0b01010101);
    for (uint32_t i = 0; i < 8; i++) {
        if (i % 2 == 0) {
            EXPECT_NE(pkts[i].data, nullptr);
        } else {
            EXPECT_EQ(pkts[i].data, nullptr);
        }
    }
}

#if 1
TEST_F(flow_container_test, rand_op_stress) {
    std::map<flow_key_t, uint64_t> ref;
    constexpr uint32_t             OPERATIONS = 100000;
    srand(42);

    for (uint32_t op = 0; op < OPERATIONS; op++) {
        uint32_t r   = rand() % 100;
        auto     key = __create_key_predict(rand() % 1000);
        if (r < 60) {
            auto [it, res] = __container.add(key, ::rte_rdtsc());
            if (ref.count(key)) {
                EXPECT_EQ(res, insert_result::ALREADY_EXISTS);
                EXPECT_EQ(it.data()->packets, ref[key]);
            } else {
                EXPECT_EQ(res, insert_result::INSERTED);
                ref[key]           = op;
                it.data()->packets = op;
            }
        } else if (r < 80) {
            auto erased       = __container.erase(key);
            auto should_exist = ref.count(key);

            EXPECT_EQ(erased, should_exist);
            if (erased) {
                ref.erase(key);
            }
        }
        if (op % 1000 == 0) {
            EXPECT_EQ(__container.get_elements_cnt(), ref.size());
        }
    }

    EXPECT_EQ(__container.get_elements_cnt(), ref.size());
}
#endif

class flow_container_burst_test : public ::testing::TestWithParam<int> {
protected:
    using container_t = flow_container_v3<flow_key_t, flow_traits, 64>; // PKT_LIMIT=64

    struct test_info {
        flow_key_t key;
        flow_data *data;
        void       get_key(const flow_key_t *&k) const { k = &key; }
        void       set_entry(flow_data *d) { data = d; }
    };

    void SetUp() override {
        if (!eal_initialized) {
            char *argv[] = {strdup("flow_container_test"), strdup("--no-pci"), nullptr};
            int   argc   = 2;
            int   rc     = ::rte_eal_init(argc, argv);
            ASSERT_GE(rc, 0) << "Failed to initialize EAL";
            eal_initialized = true;
        }

        __container.init("burst_test", 8, 1024, 1000000, 0);

        // Заполняем тестовыми ключами
        for (int i = 0; i < 1000; i++) {
            flow_key_t key;
            key.__src_ip   = 0xC0A80000 + (i & 0xFF);
            key.__dst_ip   = 0xC0A90000 + ((i >> 8) & 0xFF);
            key.__src_port = 1024 + i;
            key.__dst_port = 2048 + i;
            key.__proto    = i % 2 ? IPPROTO_TCP : IPPROTO_UDP;
            key.__hash     = key.get_flow_hash();
            __keys.push_back(key);
        }

        // Вставляем первые 500 ключей
        for (int i = 0; i < 500; i++) {
            __container.add(__keys[i], ::rte_rdtsc());
        }
    }

    int get_burst_size() const { return GetParam(); }

    container_t             __container;
    std::vector<flow_key_t> __keys;
};

TEST_P(flow_container_burst_test, all_hits) {
    const int BURST_SIZE = get_burst_size();
    test_info pkts[64]; // максимум 64

    uint64_t expected_hits = 0;
    for (int i = 0; i < BURST_SIZE; i++) {
        pkts[i].key  = __keys[i % 500];
        pkts[i].data = nullptr;
        expected_hits |= (1ULL << i);
    }

    uint64_t hit_mask;
    uint64_t lookup_mask = (BURST_SIZE == 64) ? ~0ULL : (1ULL << BURST_SIZE) - 1;
    __container.lookup(pkts, lookup_mask, &hit_mask);

    EXPECT_EQ(hit_mask, expected_hits);

    for (int i = 0; i < BURST_SIZE; i++) {
        EXPECT_NE(pkts[i].data, nullptr);
    }
}

TEST_P(flow_container_burst_test, mixed_hits) {
    const int BURST_SIZE = get_burst_size();
    test_info pkts[64];

    uint64_t expected_hits = 0;
    for (int i = 0; i < BURST_SIZE; i++) {
        if (i % 2 == 0) {
            pkts[i].key = __keys[i % 500];
            expected_hits |= (1ULL << i);
        } else {
            pkts[i].key = __keys[500 + (i % 500)];
        }
        pkts[i].data = nullptr;
    }

    uint64_t hit_mask;
    uint64_t lookup_mask = (BURST_SIZE == 64) ? ~0ULL : (1ULL << BURST_SIZE) - 1;
    __container.lookup(pkts, lookup_mask, &hit_mask);

    EXPECT_EQ(hit_mask, expected_hits);

    for (int i = 0; i < BURST_SIZE; i++) {
        if (i % 2 == 0) {
            EXPECT_NE(pkts[i].data, nullptr);
        } else {
            EXPECT_EQ(pkts[i].data, nullptr);
        }
    }
}

TEST_P(flow_container_burst_test, partial_burst) {
    const int BURST_SIZE  = get_burst_size();
    const int ACTUAL_SIZE = std::min(4, BURST_SIZE);
    test_info pkts[64];

    for (int i = 0; i < ACTUAL_SIZE; i++) {
        pkts[i].key  = __keys[i % 500];
        pkts[i].data = nullptr;
    }

    uint64_t hit_mask;
    uint64_t lookup_mask = (1ULL << ACTUAL_SIZE) - 1;
    __container.lookup(pkts, lookup_mask, &hit_mask);

    EXPECT_EQ(hit_mask, lookup_mask);

    for (int i = 0; i < ACTUAL_SIZE; i++) {
        EXPECT_NE(pkts[i].data, nullptr);
    }
}

INSTANTIATE_TEST_SUITE_P(burst_sizes, flow_container_burst_test,
                         ::testing::Values(1, 2, 3, 4, 5, 6, 7, 8, 12, 16, 24, 32, 48, 64));

class FlowContainerConfigTest : public ::testing::TestWithParam<std::tuple<int, int, int>> {
protected:
    using container_t = flow_container_v3<flow_key_t, flow_traits, 64>;

    struct test_info {
        flow_key_t key;
        flow_data *data;
        void       get_key(const flow_key_t *&k) const { k = &key; }
        void       set_entry(flow_data *d) { data = d; }
    };

    void SetUp() override {
        if (!eal_initialized) {
            char *argv[] = {strdup("flow_container_test"), strdup("--no-pci"), nullptr};
            int   argc   = 2;
            int   rc     = ::rte_eal_init(argc, argv);
            ASSERT_GE(rc, 0) << "Failed to initialize EAL";
            eal_initialized = true;
        }

        __hash_order  = std::get<0>(GetParam());
        __ext_buckets = std::get<1>(GetParam());
        __max_keys    = std::get<2>(GetParam());

        char name[64];
        snprintf(name, sizeof(name), "config_test_%d_%d_%d", __hash_order, __ext_buckets, __max_keys);
        std::cout << name << std::endl;
        __container.init(name, __hash_order, __ext_buckets, __max_keys, 0);

        // Генерируем ключи
        for (int i = 0; i < 10000; i++) {
            flow_key_t key;
            key.__src_ip   = rand();
            key.__dst_ip   = rand();
            key.__src_port = rand() & 0xFFFF;
            key.__dst_port = rand() & 0xFFFF;
            key.__proto    = rand() % 2 ? IPPROTO_TCP : IPPROTO_UDP;
            key.__hash     = key.get_flow_hash();
            __keys.push_back(key);
        }

        size_t to_insert = __max_keys * 0.8;
        for (size_t i = 0; i < to_insert && i < __keys.size(); i++) {
            __container.add(__keys[i], ::rte_rdtsc());
        }
    }

    container_t             __container;
    std::vector<flow_key_t> __keys;
    int                     __hash_order;
    int                     __ext_buckets;
    int                     __max_keys;
};

TEST_P(FlowContainerConfigTest, LookupPerformance) {
    const int BURST_SIZE = 16;
    test_info pkts[64];

    uint64_t total_hits = 0;
    int      iterations = 1000;

    for (int iter = 0; iter < iterations; iter++) {
        for (int i = 0; i < BURST_SIZE; i++) {
            int key_idx  = rand() % __keys.size();
            pkts[i].key  = __keys[key_idx];
            pkts[i].data = nullptr;
        }

        uint64_t hit_mask;
        __container.lookup(pkts, (1ULL << BURST_SIZE) - 1, &hit_mask);
        total_hits += __builtin_popcountll(hit_mask);
    }

    EXPECT_GT(total_hits, 0);

    //    std::cout << "\nConfig: order=" << __hash_order << " ext=" << __ext_buckets << " max=" << __max_keys <<
    //    std::endl;
    //    __container.print_stat();
}

INSTANTIATE_TEST_SUITE_P(TableConfigs, FlowContainerConfigTest,
                         ::testing::Combine(::testing::Values(8, 10, 12, 14, 16),      // hash_order
                                            ::testing::Values(1024, 4096, 16384),      // ext_buckets
                                            ::testing::Values(100000, 500000, 1000000) // max_keys
                                            ));

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
