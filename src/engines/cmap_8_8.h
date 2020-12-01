// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2017-2020, Intel Corporation */

#pragma once

#include "../pmemobj_engine.h"
#include "../polymorphic_string.h"

#include <libpmemobj++/container/concurrent_hash_map.hpp>
#include <libpmemobj++/persistent_ptr.hpp>

namespace pmem
{
namespace kv
{
namespace internal
{
namespace cmap_8_8
{

using map_t = pmem::obj::concurrent_hash_map<uint64_t, obj::p<uint64_t>>;

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
};

} /* namespace kv */
} /* namespace pmem */
