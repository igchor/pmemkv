// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2017-2019, Intel Corporation */

#pragma once

#include "../engine.h"
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

class vcmap : public engine_base {
public:
	vcmap(std::unique_ptr<internal::config> cfg);
	~vcmap();

	std::string name() final;

	status count_all(std::size_t &cnt) final;

	status get_all(get_kv_callback *callback, void *arg) final;

	status exists(string_view key) final;

	status get(string_view key, get_v_callback *callback, void *arg) final;

	status put(string_view key, string_view value) final;

	status remove(string_view key) final;

private:
	typedef memkind_ns::allocator<std::pair<const uint64_t, uint64_t>> kv_allocator_t;
	typedef tbb::concurrent_hash_map<uint64_t, uint64_t,
					 tbb::tbb_hash_compare<uint64_t>,
					 std::scoped_allocator_adaptor<kv_allocator_t>>
		map_t;
	kv_allocator_t kv_allocator;
	map_t pmem_kv_container;
};

} /* namespace kv */
} /* namespace pmem */
