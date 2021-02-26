// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2020, Intel Corporation */

#pragma once

#include "../iterator.h"
#include "../pmemobj_engine.h"

#define TBB_PREVIEW_MEMORY_POOL 1

#include <libpmemobj++/container/concurrent_hash_map.hpp>
#include <libpmemobj++/container/string.hpp>
#include <libpmemobj++/detail/pair.hpp>
#include <libpmemobj++/experimental/inline_string.hpp>
#include <libpmemobj++/experimental/radix_tree.hpp>
#include <libpmemobj++/persistent_ptr.hpp>

#include <tbb/memory_pool.h>
#include <tbb/concurrent_hash_map.h>

#include <list>

#include <condition_variable>
#include <endian.h>
#include <shared_mutex>

#include <tbb/concurrent_queue.h>

#include <libpmemobj++/detail/enumerable_thread_specific.hpp>

#include <libpmemobj++/experimental/self_relative_ptr.hpp>

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

using string_type = std::basic_string<char, std::char_traits<char>>;

class dram_string_hasher {
public:
	using is_transparent = void;

	size_t hash(const string_type &str) const
	{
		return std::hash<std::string_view>{}(std::string_view(str.data(), str.size()));
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
		tbb::concurrent_hash_map<string_type, string_view, dram_string_hasher>;
	using accessor_type = container_type::accessor;
	using const_accessor_type = container_type::const_accessor;

	static constexpr const char *tombstone = "tombstone"; // XXX

	dram_map_type(size_t n) : map(n)
	{
	}

	void put(string_view key, string_view value)
	{
		container_type::accessor acc;

		map.emplace(acc, std::piecewise_construct, std::forward_as_tuple(key.data(), key.size()), std::forward_as_tuple(value));
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
		return map.count(string_type(key.data(), key.size())) == 0 ? false : true;
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
};

struct tls_data_t {
	obj::experimental::self_relative_ptr<char[]> ptr;
	obj::p<size_t> size; // XXX -> p?
};

struct pmem_type {
	pmem_type() : map()
	{
		std::memset(reserved, 0, sizeof(reserved));
	}

	// obj::vector<pmem_map_type> map;
	pmem_map_type map;
	uint64_t reserved[8];

	detail::enumerable_thread_specific<tls_data_t> ptls;
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
	}

private:

	using dram_map_type = internal::new_map::dram_map_type;
	using container_type = internal::new_map::pmem_map_type;
	using pmem_insert_log_type = internal::new_map::pmem_insert_log_type;
	using pmem_remove_log_type = internal::new_map::pmem_remove_log_type;
	using pmem_type = internal::new_map::pmem_type;

	std::thread start_bg_compaction();
	bool dram_has_space();

	void Recover();

	container_type* container;
	std::unique_ptr<internal::config> config;

	pmem_type* pmem;

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

	bool out_of_space = false;

	std::atomic<bool> is_shutting_down = false;

	struct hazard_pointers {
		std::atomic<dram_map_type *> mut = nullptr, imm = nullptr;
	};

	std::mutex hazard_pointers_mtx;
	std::list<hazard_pointers*> hazard_pointers_list;

	struct hazard_pointers_handler {
		hazard_pointers_handler(new_map *map)
		{
			std::unique_lock<std::mutex> lock(map->hazard_pointers_mtx);
			auto hp = new hazard_pointers();
			this->hp = hp;
			map->hazard_pointers_list.push_back(hp);
		}

		// XXX - cleanup (shared_ptr for new_map?)

		hazard_pointers *hp;
	};

	//using val_type = std::pair<std::string, std::string>;
	using val_type = std::pair<string_view, string_view>;

	std::atomic<int> bg_cnt = 0;

	tbb::concurrent_queue<val_type> queue[24];

	uint64_t log_size = 0;
	uint64_t bg_threads = 8;

	std::atomic<size_t> dram_size = 0;

	struct tls_holder {
		tls_holder(obj::pool_base& pmpool, pmem_type* pmem, size_t size): log(pmem->ptls.local()) {
			obj::transaction::run(pmpool, [&]{
				log.ptr = obj::make_persistent<char[]>(size);
				assert(log.ptr);
				log.size = 0;
			});
		}
		internal::new_map::tls_data_t& log;
	};
};

} /* namespace kv */
} /* namespace pmem */
