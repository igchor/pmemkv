// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2020, Intel Corporation */

#pragma once

#include "../iterator.h"
#include "../pmemobj_engine.h"

#include <libpmemobj++/container/concurrent_hash_map.hpp>
#include <libpmemobj++/container/string.hpp>
#include <libpmemobj++/detail/pair.hpp>
#include <libpmemobj++/experimental/inline_string.hpp>
#include <libpmemobj++/experimental/radix_tree.hpp>
#include <libpmemobj++/persistent_ptr.hpp>

#include <tbb/concurrent_hash_map.h>

#include <list>

#include <condition_variable>
#include <endian.h>
#include <shared_mutex>

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

class dram_string_hasher {
public:
	using is_transparent = void;

	size_t hash(const std::string &str) const
	{
		return std::hash<std::string>{}(str);
	}

	size_t hash(string_view str) const
	{
		return std::hash<string_view>{}(str);
	}

	template <typename M, typename U>
	bool equal(const M &lhs, const U &rhs) const
	{
		return lhs == rhs;
	}
};

using pmem_map_type = obj::concurrent_hash_map<obj::string, obj::string, string_hasher>;

// using pmem_map_type =
// pmem::obj::experimental::radix_tree<pmem::obj::experimental::inline_string,
// 					    pmem::obj::experimental::inline_string>;

using pmem_insert_log_type = obj::vector<detail::pair<obj::string, obj::string>>;
using pmem_remove_log_type = obj::vector<obj::string>;

struct dram_map_type {
	using container_type =
		tbb::concurrent_hash_map<std::string, std::string, dram_string_hasher>;
	using accessor_type = container_type::accessor;
	using const_accessor_type = container_type::const_accessor;

	static constexpr const char *tombstone = "tombstone"; // XXX

	dram_map_type()
	{
	}

	dram_map_type(size_t n) : map(n)
	{
	}

	void put(string_view key, string_view value)
	{
		container_type::accessor acc;

		map.insert(acc, key);
		acc->second = value;
	}

	enum class element_status { alive, removed, not_found };

	element_status get(string_view key, container_type::const_accessor &acc)
	{
		auto found = map.find(acc, key);

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

	container_type map;

	std::atomic<std::size_t> mutable_count = 0;
};

struct pmem_type {
	pmem_type() : map()
	{
		std::memset(reserved, 0, sizeof(reserved));
	}

	// obj::vector<pmem_map_type> map;
	pmem_map_type map;
	pmem_insert_log_type insert_log;
	pmem_remove_log_type remove_log;
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
		std::unique_lock<std::mutex> lock(compaction_mtx);

		// compaction_cv.wait(lock, [&] { return immutable_map == nullptr; });

		// /* If we end up here, neither mutable nor immutable map
		//  * can be changed concurrently. The only allowed
		//  * concurrent change is setting immutable_map to nullptr
		//  * (by the compaction thread). */
		// immutable_map = std::move(mutable_map);
		// mutable_map = std::make_unique<dram_map_type>();

		// auto t = start_bg_compaction();
		// lock.unlock();
		// t.join();
	}

private:
	using dram_map_type = internal::new_map::dram_map_type;
	using container_type = tbb::concurrent_hash_map<std::string, std::string>;
	using pmem_insert_log_type = internal::new_map::pmem_insert_log_type;
	using pmem_remove_log_type = internal::new_map::pmem_remove_log_type;

	std::thread start_bg_compaction();
	bool dram_has_space(dram_map_type &map);

	void Recover();

	container_type *container;
	pmem_insert_log_type *insert_log;
	pmem_remove_log_type *remove_log;
	std::unique_ptr<internal::config> config;

	using mutex_type = std::shared_timed_mutex;
	using unique_lock_type = std::unique_lock<mutex_type>;
	using shared_lock_type = std::shared_lock<mutex_type>;

	// std::vector<mutex_type> mtxs;

	// static constexpr uint64_t dram_capacity = 1024; /// XXX: parameterize this

	uint64_t dram_capacity = 1024;

	std::atomic<dram_map_type *> mutable_map;
	std::atomic<dram_map_type *> immutable_map;

	std::mutex compaction_mtx;
	std::shared_timed_mutex iteration_mtx;
	std::condition_variable compaction_cv;
	std::mutex bg_mtx;
	std::condition_variable bg_cv;
	std::atomic<bool> is_shutting_down = false;

	struct hazard_pointers {
		std::atomic<dram_map_type *> mut = nullptr, imm = nullptr;
	};

	std::mutex hazard_pointers_mtx;
	std::list<std::unique_ptr<hazard_pointers>> hazard_pointers_list;

	struct hazard_pointers_handler {
		hazard_pointers_handler(new_map *map)
		{
			std::unique_ptr<hazard_pointers> hp =
				std::unique_ptr<hazard_pointers>(new hazard_pointers());
			this->hp = hp.get();

			std::unique_lock<std::mutex> lock(map->hazard_pointers_mtx);
			map->hazard_pointers_list.push_back(std::move(hp));
		}

		// XXX - cleanup (shared_ptr for new_map?)

		hazard_pointers *hp;
	};
};

} /* namespace kv */
} /* namespace pmem */
