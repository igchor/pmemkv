// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2020, Intel Corporation */

#pragma once

#include "../iterator.h"
#include "../pmemobj_engine.h"

#include <libpmemobj++/persistent_ptr.hpp>

#include <libpmemobj++/experimental/inline_string.hpp>
#include <libpmemobj++/experimental/radix_tree.hpp>

#include <libpmemobj++/detail/pair.hpp>

#include <libpmemobj++/container/string.hpp>

#include <libpmemobj++/container/concurrent_hash_map.hpp>

#include <tbb/concurrent_hash_map.h>

#include <endian.h>

#include <shared_mutex>

#include <condition_variable>

namespace pmem
{
namespace kv
{
namespace internal
{
namespace new_map
{

class key_equal {
public:
	template <typename M, typename U>
	bool operator()(const M &lhs, const U &rhs) const
	{
		return lhs == rhs;
	}
};

class string_hasher {
	/* hash multiplier used by fibonacci hashing */
	static const size_t hash_multiplier = 11400714819323198485ULL;

public:
	using transparent_key_equal = key_equal;

	size_t operator()(const obj::string &str) const
	{
		return hash(str.c_str(), str.size());
	}

	size_t operator()(string_view str) const
	{
		return hash(str.data(), str.size());
	}

private:
	size_t hash(const char *str, size_t size) const
	{
		size_t h = 0;
		for (size_t i = 0; i < size; ++i) {
			h = static_cast<size_t>(str[i]) ^ (h * hash_multiplier);
		}
		return h;
	}
};

using pmem_map_type = obj::concurrent_hash_map<obj::string, obj::string, string_hasher>;

// using pmem_map_type =
// pmem::obj::experimental::radix_tree<pmem::obj::experimental::inline_string,
// 					    pmem::obj::experimental::inline_string>;

using pmem_insert_log_type = obj::vector<detail::pair<obj::string, obj::string>>;
using pmem_remove_log_type = obj::vector<obj::string>;

struct dram_map_type {
	// using container_type = tbb::concurrent_hash_map<string_view, string_view,
	// string_hasher>;
	using container_type = tbb::concurrent_hash_map<std::string, std::string>;
	using accessor_type = container_type::accessor;
	using const_accessor_type = container_type::const_accessor;
	// static constexpr uintptr_t tombstone = std::numeric_limits<uintptr_t>::max();
	static constexpr const char *tombstone = "tombstone"; // XXX

	dram_map_type()
	{
		is_mutable = true;
	}

	~dram_map_type()
	{
		// for (const auto &e : map) {
		// 	 delete[] e.first.data();

		// 	 if ((uint64_t) e.second.data() != dram_map_type::tombstone)
		// 	 	delete[] e.second.data();
		//  }
	}

	void put(string_view key, string_view value)
	{
		container_type::accessor acc;

		// auto k = new char[key.size()];
		// std::copy(key.begin(), key.data() + key.size(), k);

		// auto v = (char*) tombstone;

		// if ((uint64_t)value.data() != tombstone) {
		// 	v = new char[value.size()];
		// 	std::copy(value.begin(), value.data() + value.size(), v);
		// }

		// container_type::value_type kv{string_view(k, key.size()),
		// string_view(v, value.size())};
		container_type::value_type kv{std::string(key.data(), key.size()),
					      std::string(value.data(), value.size())};

		// XXX - make it exception safe (use C++...)
		auto inserted = map.insert(acc, kv);
		if (!inserted) {
			// if ((uintptr_t) acc->second.data() != tombstone)
			// 	delete[] acc->second.data();
			// delete[] kv.first.data();

			acc->second = kv.second;
		}
	}

	enum class element_status { alive, removed, not_found };

	element_status get(string_view key, container_type::const_accessor &acc)
	{
		auto found = map.find(acc, std::string(key.data(), key.size()));

		if (found) {
			if (acc->second == tombstone)
				return element_status::removed;
			else
				return element_status::alive;
		}

		return element_status::not_found;
	}

	/* Element exists in the dram map (alive or tombstone) */
	bool exists(string_view key)
	{
		return map.count(std::string(key.data(), key.size())) == 0 ? false : true;
	}

	// void remove(string_view key) {
	// 	container_type::accessor acc;

	// 	// auto k = new char[key.size()];
	// 	// std::copy(key.begin(), key.data() + key.size(), k);

	// 	// container_type::value_type kv{string_view(k, key.size()),
	// string_view((const char*)tombstone, 0)}; 	container_type::value_type
	// kv{std::string(key.data(), key.size()), std::string((const char*) tombstone,
	// 0)};

	// 	// XXX - make it exception safe (use C++...)
	// 	auto inserted = map.insert(acc, kv);
	// 	if (!inserted) {
	// 		// if ((uintptr_t) acc->second.data() != tombstone)
	// 		// 	delete[] acc->second.data();
	// 		// delete[] kv.first.data();

	// 		acc->second = kv.second;
	// 	}
	// }

	container_type::iterator begin()
	{
		return map.begin();
	}

	container_type::iterator end()
	{
		return map.end();
	}

	size_t size() const
	{
		return map.size();
	}

	std::atomic<bool> is_mutable;

	container_type map;
};

struct pmem_type {
	pmem_type() : map()
	{
		std::memset(reserved, 0, sizeof(reserved));
	}

	// obj::vector<pmem_map_type> map;
	pmem_map_type map;
	pmem_insert_log_type insert_log[64];
	pmem_remove_log_type remove_log[64];
	uint64_t reserved[8];
};

// static_assert(sizeof(pmem_type) == sizeof(map_type) + 64, "");

} /* namespace new_map */
} /* namespace internal */

class new_map : public pmemobj_engine_base<internal::new_map::pmem_type> {
	template <bool IsConst>
	class iterator;

public:
	new_map(std::unique_ptr<internal::config> cfg);
	~new_map();

	new_map(const new_map &) = delete;
	new_map &operator=(const new_map &) = delete;

	std::string name() final;

	status count_all(std::size_t &cnt) final;

	status get_all(get_kv_callback *callback, void *arg) final;

	status exists(string_view key) final;

	status get(string_view key, get_v_callback *callback, void *arg) final;

	status put(string_view key, string_view value) final;

	status remove(string_view key) final;

	void flush() final
	{
		do {
			std::unique_lock<std::shared_timed_mutex> lock(compaction_mtx);

			if (immutable_map) {
				// XXX: wait on cond var
				continue;
			} else {
				/* If we end up here, neither mutable nor immutable map
				 * can be changed concurrently. The only allowed
				 * concurrent change is setting immutable_map to nullptr
				 * (by the compaction thread). */
				immutable_map = std::move(mutable_map);
				mutable_map = std::make_unique<dram_map_type>();

				auto t = start_bg_compaction();
				lock.unlock();
				t.join();

				return;
			}
		} while (true);
	}

private:
	// static constexpr int SHARDS_NUM = 1024;

	// static inline uint64_t
	// mix(uint64_t h)
	// {
	// 	h ^= h >> 23;
	// 	h *= 0x2127599bf4325c37ULL;
	// 	return h ^ h >> 47;
	// }

	// uint64_t shard(string_view key) {
	// 	const uint64_t    m = 0x880355f21e6d1965ULL;
	// 	const uint64_t *pos = (const uint64_t *)key.data();
	// 	const uint64_t *end = pos + (key.size() / 8);
	// 	uint64_t h = key.size() * m;

	// 	while (pos != end)
	// 		h = (h ^ mix(*pos++)) * m;

	// 	if (key.size() & 7) {
	// 		uint64_t shift = (key.size() & 7) * 8;
	// 		uint64_t mask = (1ULL << shift) - 1;
	// 		uint64_t v = htole64(*pos) & mask;
	// 		h = (h ^ mix(v)) * m;
	// 	}

	// 	return mix(h) & (SHARDS_NUM - 1);
	// }

	/// using container_type = obj::vector<internal::new_map::pmem_map_type>;
	using container_type = internal::new_map::pmem_map_type;
	using dram_map_type = internal::new_map::dram_map_type;
	using pmem_insert_log_type = internal::new_map::pmem_insert_log_type;
	using pmem_remove_log_type = internal::new_map::pmem_remove_log_type;

	std::thread start_bg_compaction();
	bool dram_has_space(std::unique_ptr<dram_map_type> &map);

	void Recover();

	container_type *container;
	pmem_insert_log_type *insert_logs;
	pmem_remove_log_type *remove_logs;
	std::unique_ptr<internal::config> config;

	using mutex_type = std::shared_timed_mutex;
	using unique_lock_type = std::unique_lock<mutex_type>;
	using shared_lock_type = std::shared_lock<mutex_type>;

	// std::vector<mutex_type> mtxs;

	// static constexpr uint64_t dram_capacity = 1024; /// XXX: parameterize this

	uint64_t dram_capacity = 1024;

	std::unique_ptr<dram_map_type> mutable_map;
	std::unique_ptr<dram_map_type> immutable_map;

	std::shared_timed_mutex compaction_mtx;
	std::shared_timed_mutex iteration_mtx;

	std::condition_variable_any compaction_cv;

	uint64_t bg_threads = 6;
};

} /* namespace kv */
} /* namespace pmem */
