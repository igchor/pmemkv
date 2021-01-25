// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2020, Intel Corporation */

#pragma once

#include "../iterator.h"
#include "../pmemobj_engine.h"

#include "../pgm_index_dynamic.hpp"

namespace pmem
{
namespace kv
{
namespace internal
{
namespace new_map
{

using map_type = pmem::obj::vector<pgm::DynamicPGMIndex<uint64_t, uint64_t>::Level>;

struct pmem_type {
	pmem_type() : map()
	{
		std::memset(reserved, 0, sizeof(reserved));
	}

	map_type map;
	obj::p<uint64_t> size;
	uint64_t reserved[8];
};

// static_assert(sizeof(pmem_type) == sizeof(map_type) + 64, "");

} /* namespace new_map */
} /* namespace internal */

class new_map : public pmemobj_engine_base<internal::new_map::pmem_type> {
public:
	new_map(std::unique_ptr<internal::config> cfg);
	~new_map();

	new_map(const new_map &) = delete;
	new_map &operator=(const new_map &) = delete;

	std::string name() final;

	status count_all(std::size_t &cnt) final;
	// status count_above(string_view key, std::size_t &cnt) final;
	// status count_equal_above(string_view key, std::size_t &cnt) final;
	// status count_equal_below(string_view key, std::size_t &cnt) final;
	// status count_below(string_view key, std::size_t &cnt) final;
	// status count_between(string_view key1, string_view key2, std::size_t &cnt) final;

	status get_all(get_kv_callback *callback, void *arg) final;
	// status get_above(string_view key, get_kv_callback *callback, void *arg) final;
	// status get_equal_above(string_view key, get_kv_callback *callback,
	// 		       void *arg) final;
	// status get_equal_below(string_view key, get_kv_callback *callback,
	// 		       void *arg) final;
	// status get_below(string_view key, get_kv_callback *callback, void *arg) final;
	// status get_between(string_view key1, string_view key2, get_kv_callback *callback,
	// 		   void *arg) final;

	status exists(string_view key) final;

	status get(string_view key, get_v_callback *callback, void *arg) final;

	status put(string_view key, string_view value) final;

	status remove(string_view key) final;

private:
	using container_type = internal::new_map::map_type;

	void Recover();
	status iterate(pgm::DynamicPGMIndex<uint64_t, uint64_t>::iterator first,
		       pgm::DynamicPGMIndex<uint64_t, uint64_t>::iterator last,
		       get_kv_callback *callback, void *arg);

	container_type *container;
	std::unique_ptr<internal::config> config;

	std::unique_ptr<pgm::DynamicPGMIndex<uint64_t, uint64_t>> index;

	obj::p<uint64_t> *size;
};

} /* namespace kv */
} /* namespace pmem */
