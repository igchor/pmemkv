// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2020, Intel Corporation */

#pragma once

#include "../iterator.h"
#include "../pmemobj_engine.h"

#include <string>
#include <list>

#include <tbb/concurrent_hash_map.h>
#include <tbb/concurrent_queue.h>

#include <libpmemobj/action_base.h>

#include <libpmemobj++/container/string.hpp>
#include <libpmemobj++/container/concurrent_hash_map.hpp>

#define ALIGN_UP(size, align) (((size) + (align) - 1) & ~((align) - 1))

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

using map_type = obj::concurrent_hash_map<obj::string, obj::string, string_hasher>;

struct pmem_type {
	pmem_type() : map()
	{
		std::memset(reserved, 0, sizeof(reserved));
	}

	map_type map;
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
	using container_type = internal::new_map::map_type;
	using pmem_type = internal::new_map::pmem_type;
	using dram_map_type = internal::new_map::dram_map_type;

	pmem_type *pmem_ptr;
	uint64_t dram_capacity = 1024;
	uint64_t log_size = 1024 * 1024;
	uint64_t worker_threads = 4;

	void Recover();

	struct pmem_log {
		pmem_log(obj::pool_base &pop, size_t size) {
			pobj_action act;
			auto p = (char*) pmemobj_direct(pmemobj_reserve(pop.handle(), &act, size, 0));
			ptr = (char*) ALIGN_UP((uint64_t)p , 64ULL);
			assert(ptr);
			size = 0;
		}

		char* ptr;
		size_t size = 0;
	};

	template <typename T>
	struct hazard_list {
		hazard_list(){
		}

		T* emplace() {
			std::unique_lock<std::mutex> lock(mtx);
			list.emplace_back();
			return &list.back();
		}

		template <typename F>
		void foreach(F&& f) {
			std::unique_lock<std::mutex> lock(mtx);
			for (auto &e : list)
				f(e);
		}

		std::mutex mtx;
		std::list<T> list;
	};

	template <typename T>
	struct hazard_pointer {
		hazard_pointer(hazard_list<std::atomic<T*>>& list): hazard(*list.emplace()) {
		}

		T* acquire(std::atomic<T*> &target) {
			while (true) {
				auto ptr = target.load(std::memory_order_acquire);
				hazard.store(ptr, std::memory_order_relaxed);
				if (ptr == target.load(std::memory_order_acquire))
					return ptr;
			}
		}

		void release() {
			hazard.store(nullptr, std::memory_order_release);
		}

		~hazard_pointer() {
			// XXX - remove itself from the list?
		}

		//hazard_list<std::atomic<T*>> &list;
		std::atomic<T*> &hazard;
	};

	std::atomic<size_t> bg_cnt = 0;

	std::atomic<dram_map_type*> index;
	hazard_list<std::atomic<dram_map_type*>> hazards;

	container_type *container;
	std::unique_ptr<internal::config> config;

	tbb::concurrent_queue<char*> worker_queue[128];

	std::atomic<bool> is_shutting_down = false;

	std::mutex bg_mtx;
	std::condition_variable cv;

	std::mutex client_mtx;
	std::condition_variable client_cv;
};

} /* namespace kv */
} /* namespace pmem */
