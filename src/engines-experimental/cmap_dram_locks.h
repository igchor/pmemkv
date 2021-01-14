// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2017-2020, Intel Corporation */

#pragma once

#include "../pmemobj_engine.h"
#include "../polymorphic_string.h"

#include <libpmemobj++/container/concurrent_hash_map.hpp>
#include <libpmemobj++/persistent_ptr.hpp>

#include <tbb/spin_rw_mutex.h>

namespace pmem
{
namespace kv
{
namespace internal
{
namespace cmap_dram_locks
{

class null_rw_mutex {
public:
	//! Represents acquisition of a mutex.
	class scoped_lock {
	public:
		scoped_lock()
		{
		}
		scoped_lock(null_rw_mutex &, bool w = true)
		{
			is_writer = w;
		}
		~scoped_lock()
		{
		}
		void acquire(null_rw_mutex &, bool w = true)
		{
			is_writer = w;
		}
		bool upgrade_to_writer()
		{
			is_writer = true;
			return true;
		}
		bool downgrade_to_reader()
		{
			is_writer = false;
			return true;
		}
		bool try_acquire(null_rw_mutex &, bool w = true)
		{
			is_writer = w;
			return true;
		}
		void release()
		{
		}

	protected:
		bool is_writer = false;
		null_rw_mutex *mutex = nullptr;
	};

	null_rw_mutex()
	{
	}

	// Mutex traits
	static const bool is_rw_mutex = true;
	static const bool is_recursive_mutex = true;
	static const bool is_fair_mutex = true;
};

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

	size_t operator()(const pmem::kv::polymorphic_string &str) const
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

using string_t = pmem::kv::polymorphic_string;
using map_t = pmem::obj::concurrent_hash_map<string_t, string_t, string_hasher,
					     std::equal_to<polymorphic_string>,
					     null_rw_mutex, null_rw_mutex::scoped_lock>;

} /* namespace cmap_dram_locks */
} /* namespace internal */

class cmap_dram_locks : public pmemobj_engine_base<internal::cmap_dram_locks::map_t> {
public:
	cmap_dram_locks(std::unique_ptr<internal::config> cfg);
	~cmap_dram_locks();

	cmap_dram_locks(const cmap_dram_locks &) = delete;
	cmap_dram_locks &operator=(const cmap_dram_locks &) = delete;

	std::string name() final;

	status count_all(std::size_t &cnt) final;

	status get_all(get_kv_callback *callback, void *arg) final;

	status exists(string_view key) final;

	status get(string_view key, get_v_callback *callback, void *arg) final;

	status put(string_view key, string_view value) final;

	status remove(string_view key) final;

	status defrag(double start_percent, double amount_percent) final;

private:
	static constexpr size_t N_MTXS = 1024;

	tbb::spin_rw_mutex *mtxs;

	tbb::spin_rw_mutex::scoped_lock lock_write(string_view key)
	{
		return tbb::spin_rw_mutex::scoped_lock(
			mtxs[internal::cmap_dram_locks::string_hasher{}(key) &
			     (N_MTXS - 1)],
			true);
	}

	void Recover();
	internal::cmap_dram_locks::map_t *container;
};

} /* namespace kv */
} /* namespace pmem */
