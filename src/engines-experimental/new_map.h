// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2020, Intel Corporation */

#pragma once

#include "../iterator.h"
#include "../pmemobj_engine.h"

#include <libpmemobj++/persistent_ptr.hpp>

#include <libpmemobj++/experimental/inline_string.hpp>
#include <libpmemobj++/experimental/radix_tree.hpp>

#include <tbb/concurrent_hash_map.h>

#include <endian.h>

namespace pmem
{
namespace kv
{
namespace internal
{
namespace new_map
{

	template <typename Iterator, typename Pred>
	status iterate(Iterator first, Iterator last, get_kv_callback *callback, void *arg, Pred &&pred)
	{
		for (auto it = first; it != last; ++it) {
			string_view key = it->first;
			string_view value = it->second;

			if (!pred(key, value))
				continue;

			auto ret =
				callback(key.data(), key.size(), value.data(), value.size(), arg);

			if (ret != 0)
				return status::STOPPED_BY_CB;
		}

		return status::OK;
	}

using pmem_map_type = pmem::obj::experimental::radix_tree<pmem::obj::experimental::inline_string,
					    pmem::obj::experimental::inline_string>;

struct dram_index {
	using dram_map_type = tbb::concurrent_hash_map<string_view, string_view>;

	dram_index() {
		mutable_map = std::make_shared<dram_map_type>();
		immutable_map = nullptr;
	}

	void put(string_view key, string_view value) {
		auto mutable_map_thr = std::atomic_load_explicit(&mutable_map, std::memory_order_acquire);

		dram_map_type::accessor acc;

		auto k = new char[key.size()];
		std::copy(key.begin(), key.data() + key.size(), k);

		auto v = new char[value.size()];
		std::copy(value.begin(), value.data() + value.size(), v);

		dram_map_type::value_type kv{string_view(k, key.size()), string_view(v, value.size())};

		// XXX - make it exception safe (use C++...)
		auto inserted = mutable_map_thr->insert(acc, kv);
		if (!inserted) {
			if ((uintptr_t) acc->second.data() != tombstone)
				delete[] acc->second.data();
			delete[] kv.first.data();

			acc->second = kv.second;
		}
	}

	bool get(string_view key, get_v_callback *callback, void *arg) {
		auto mutable_map_thr = std::atomic_load_explicit(&mutable_map, std::memory_order_acquire);
		auto immutable_map_thr = std::atomic_load_explicit(&immutable_map, std::memory_order_acquire);

		dram_map_type::const_accessor acc;
		auto found = mutable_map_thr->find(acc, key);

		if (found && (uintptr_t)acc->second.data() != tombstone) {
			callback(acc->second.data(), acc->second.size(), arg);
			return true;
		}
		
		if (immutable_map_thr && immutable_map_thr->find(acc, key) && (uintptr_t)acc->second.data() != tombstone) {
			callback(acc->second.data(), acc->second.size(), arg);
			return true;
		}

		return false;
	}

	bool exists(string_view key)
	{
		return get(key, [](const char*, size_t, void*){}, nullptr);
	}

	void remove(string_view key) {
		auto mutable_map_thr = std::atomic_load_explicit(&mutable_map, std::memory_order_acquire);
		dram_map_type::accessor acc;

		auto k = new char[key.size()];
		std::copy(key.begin(), key.data() + key.size(), k);

		dram_map_type::value_type kv{string_view(k, key.size()), string_view((const char*)tombstone, 0)};

		// XXX - make it exception safe (use C++...)
		auto inserted = mutable_map_thr->insert(acc, kv);
		if (!inserted) {
			if ((uintptr_t) acc->second.data() != tombstone)
				delete[] acc->second.data();
			delete[] kv.first.data();

			acc->second = kv.second;
		}
	}

	status iterate(get_kv_callback *callback, void *arg)
	{
		// no atomic instructions since iterate is not thread safe

		auto mut_pred = [](string_view k, string_view v){return (uintptr_t)v.data() != tombstone;};
		auto imm_pred = [&](string_view key, string_view v){return mutable_map->count(key) == 0 && (uintptr_t)v.data() != tombstone;};

		auto s = ::pmem::kv::internal::new_map::iterate(mutable_map->begin(), mutable_map->end(), callback, arg, mut_pred);
		if (s != status::OK)
			return s;

		if (!immutable_map)
			return status::OK;
	
		return ::pmem::kv::internal::new_map::iterate(immutable_map->begin(), immutable_map->end(), callback, arg, imm_pred);
	}

	bool has_space()
	{
		auto mutable_map_thr = std::atomic_load_explicit(&mutable_map, std::memory_order_acquire);

		/// XXX: count also object sizes?
		return mutable_map_thr->size() <= dram_capacity;
	}

	std::mutex compaction_mtx;
	using lock_type = std::unique_lock<std::mutex>;

	// if compaction needed, returns pointer to the immutable_map
	std::shared_ptr<dram_map_type> maybe_compact()
	{
		if (has_space()) return nullptr;

		while(true) {
			lock_type lock(compaction_mtx);

			if (has_space()) return nullptr;

			if (std::atomic_load_explicit(&immutable_map, std::memory_order_acquire) != nullptr) {
				// XXX: wait on cond variable
				while (std::atomic_load_explicit(&immutable_map, std::memory_order_acquire) != nullptr) {}
			} else {
				/* Atomic operations required to synchronize with readers. */
				std::atomic_store_explicit(&immutable_map, mutable_map, std::memory_order_release);
				std::atomic_store_explicit(&mutable_map, std::make_shared<dram_map_type>(), std::memory_order_release);
			}
		}
	}

	void free_immutable()
	{
		std::shared_ptr<dram_map_type> tmp = nullptr;
		std::atomic_store_explicit(&immutable_map, tmp, std::memory_order_release);
	}

	static constexpr uintptr_t tombstone = std::numeric_limits<uintptr_t>::max();
	static constexpr uint64_t dram_capacity = 1024; /// XXX: parameterize this

	std::shared_ptr<dram_map_type> mutable_map;
	std::shared_ptr<dram_map_type> immutable_map;
};

struct pmem_type {
	pmem_type() : map()
	{
		std::memset(reserved, 0, sizeof(reserved));
	}

	obj::vector<pmem_map_type> map;
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

private:

	static constexpr int SHARDS_NUM = 1024;

	static inline uint64_t
	mix(uint64_t h)
	{
		h ^= h >> 23;
		h *= 0x2127599bf4325c37ULL;
		return h ^ h >> 47;
	}


	uint64_t shard(string_view key) {
		const uint64_t    m = 0x880355f21e6d1965ULL;
		const uint64_t *pos = (const uint64_t *)key.data();
		const uint64_t *end = pos + (key.size() / 8);
		uint64_t h = key.size() * m;

		while (pos != end)
			h = (h ^ mix(*pos++)) * m;

		if (key.size() & 7) {
			uint64_t shift = (key.size() & 7) * 8;
			uint64_t mask = (1ULL << shift) - 1;
			uint64_t v = htole64(*pos) & mask;
			h = (h ^ mix(v)) * m;
		}

		return mix(h) & (SHARDS_NUM - 1);
	}

	using container_type = obj::vector<internal::new_map::pmem_map_type>;
	using index_type = internal::new_map::dram_index;

	void Recover();

	container_type *container;
	std::unique_ptr<internal::config> config;

	index_type index;

	using mutex_type = std::shared_timed_mutex;
	using unique_lock_type = std::unique_lock<mutex_type>;
	using shared_lock_type = std::shared_lock<mutex_type>;

	std::vector<mutex_type> mtxs;
};

} /* namespace kv */
} /* namespace pmem */
