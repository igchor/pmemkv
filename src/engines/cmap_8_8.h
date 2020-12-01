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
namespace cmap_8_8
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
using map_t = pmem::obj::concurrent_hash_map<uint64_t, obj::p<uint64_t>, std::hash<int>,
					     std::equal_to<int>, null_rw_mutex,
					     null_rw_mutex::scoped_lock>;

} /* namespace cmap */
} /* namespace internal */

class cmap_8_8 : public pmemobj_engine_base<internal::cmap_8_8::map_t> {
public:
	cmap_8_8(std::unique_ptr<internal::config> cfg);
	~cmap_8_8();

	cmap_8_8(const cmap_8_8 &) = delete;
	cmap_8_8 &operator=(const cmap_8_8 &) = delete;

	std::string name() final;

	status count_all(std::size_t &cnt) final;

	status get_all(get_kv_callback *callback, void *arg) final;

	status exists(string_view key) final;

	status get(string_view key, get_v_callback *callback, void *arg) final;

	status put(string_view key, string_view value) final;

	status remove(string_view key) final;

	status defrag(double start_percent, double amount_percent) final;

private:
	void Recover();
	internal::cmap_8_8::map_t *container;

	static constexpr size_t N_MTXS = 1024;

	tbb::spin_rw_mutex *mtxs;

	tbb::spin_rw_mutex::scoped_lock lock_write(uint64_t k)
	{
		return tbb::spin_rw_mutex::scoped_lock(
			mtxs[std::hash<uint64_t>{}(k) & (N_MTXS - 1)], true);
	}
};

} /* namespace kv */
} /* namespace pmem */
