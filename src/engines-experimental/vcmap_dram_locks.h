// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2017-2020, Intel Corporation */

#pragma once

#include "../engine.h"

// XXX - for std::hash specialization
#include "../engines/vcmap.h"

#include "pmem_allocator.h"
#include <scoped_allocator>
#include <string>
#include <tbb/concurrent_hash_map.h>

#ifdef USE_LIBMEMKIND_NAMESPACE
namespace memkind_ns = libmemkind::pmem;
#else
namespace memkind_ns = pmem;
#endif

namespace pmem
{
namespace kv
{

struct dram_mutex {
	using scoped_lock = tbb::spin_rw_mutex::scoped_lock;

	dram_mutex()
	{
		mtx = std::unique_ptr<tbb::spin_rw_mutex>(new tbb::spin_rw_mutex);
	}

	operator tbb::spin_rw_mutex &()
	{
		return *mtx;
	}

	// Mutex traits
	static const bool is_rw_mutex = true;
	static const bool is_recursive_mutex = true;
	static const bool is_fair_mutex = true;

	std::unique_ptr<tbb::spin_rw_mutex> mtx;
};

class vcmap_dram_locks : public engine_base {
	template <bool IsConst>
	class vcmap_dram_locks_iterator;

public:
	vcmap_dram_locks(std::unique_ptr<internal::config> cfg);
	~vcmap_dram_locks();

	std::string name() final;

	status count_all(std::size_t &cnt) final;

	status get_all(get_kv_callback *callback, void *arg) final;

	status exists(string_view key) final;

	status get(string_view key, get_v_callback *callback, void *arg) final;

	status put(string_view key, string_view value) final;

	status remove(string_view key) final;

	internal::iterator_base *new_iterator() final;
	internal::iterator_base *new_const_iterator() final;

private:
	typedef memkind_ns::allocator<char> ch_allocator_t;
	typedef std::basic_string<char, std::char_traits<char>, ch_allocator_t>
		pmem_string;
	typedef memkind_ns::allocator<std::pair<const pmem_string, pmem_string>>
		kv_allocator_t;
	typedef tbb::concurrent_hash_map<
		pmem_string, pmem_string, tbb::tbb_hash_compare<pmem_string>,
		std::scoped_allocator_adaptor<kv_allocator_t>, dram_mutex>
		map_t;
	kv_allocator_t kv_allocator;
	ch_allocator_t ch_allocator;
	map_t pmem_kv_container;
};

template <>
class vcmap_dram_locks::vcmap_dram_locks_iterator<true>
    : virtual public internal::iterator_base {
	using container_type = vcmap_dram_locks::map_t;
	using ch_allocator_t = memkind_ns::allocator<char>;

public:
	vcmap_dram_locks_iterator(container_type *container, ch_allocator_t *ca);

	status seek(string_view key) final;

	result<string_view> key() final;

	result<pmem::obj::slice<const char *>> read_range(size_t pos, size_t n) final;

protected:
	container_type *container;
	container_type::accessor acc_;
	ch_allocator_t *ch_allocator;
};

template <>
class vcmap_dram_locks::vcmap_dram_locks_iterator<false>
    : public vcmap_dram_locks::vcmap_dram_locks_iterator<true> {
	using container_type = vcmap_dram_locks::map_t;
	using ch_allocator_t = memkind_ns::allocator<char>;

public:
	vcmap_dram_locks_iterator(container_type *container, ch_allocator_t *ca);

	result<pmem::obj::slice<char *>> write_range(size_t pos, size_t n) final;
	status commit() final;
	void abort() final;

private:
	std::vector<std::pair<std::string, size_t>> log;
};

} /* namespace kv */
} /* namespace pmem */
