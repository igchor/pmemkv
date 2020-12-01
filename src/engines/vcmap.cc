// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2017-2019, Intel Corporation */

#include "vcmap.h"
#include "../out.h"
#include <libpmemobj++/transaction.hpp>

#include <iostream>

namespace pmem
{
namespace kv
{

static std::string get_path(internal::config &cfg)
{
	const char *path;
	if (!cfg.get_string("path", &path))
		throw internal::invalid_argument(
			"Config does not contain item with key: \"path\"");

	return std::string(path);
}

static uint64_t get_size(internal::config &cfg)
{
	std::size_t size;
	if (!cfg.get_uint64("size", &size))
		throw internal::invalid_argument(
			"Config does not contain item with key: \"size\"");

	return size;
}

vcmap::vcmap(std::unique_ptr<internal::config> cfg)
    : kv_allocator(get_path(*cfg), get_size(*cfg)),
      pmem_kv_container(std::scoped_allocator_adaptor<kv_allocator_t>(kv_allocator))
{
	LOG("Started ok");
}

vcmap::~vcmap()
{
	LOG("Stopped ok");
}

std::string vcmap::name()
{
	return "vcmap";
}

status vcmap::count_all(std::size_t &cnt)
{
	LOG("count_all");
	cnt = pmem_kv_container.size();

	return status::OK;
}

status vcmap::get_all(get_kv_callback *callback, void *arg)
{
	LOG("get_all");

	return status::OK;
}

status vcmap::exists(string_view key)
{
	LOG("exists for key=" << std::string(key.data(), key.size()));
	map_t::const_accessor result;
	
	auto k = *((uint64_t*)key.data());

	const bool result_found = pmem_kv_container.find(result, k);
	return (result_found ? status::OK : status::NOT_FOUND);
}

status vcmap::get(string_view key, get_v_callback *callback, void *arg)
{
	LOG("get key=" << std::string(key.data(), key.size()));
	auto k = *((uint64_t*)key.data());
	map_t::const_accessor result;

	const bool result_found = pmem_kv_container.find(
		result,k);
	if (!result_found) {
		LOG("  key not found");
		return status::NOT_FOUND;
	}

	callback((const char*) &result->second, 8, arg);
	return status::OK;
}

status vcmap::put(string_view key, string_view value)
{
	LOG("put key=" << std::string(key.data(), key.size())
		       << ", value.size=" << std::to_string(value.size()));

		auto k = *((uint64_t*)key.data());
			auto v = *((uint64_t*)value.data());
	bool result = pmem_kv_container.insert({k, v});
	if (!result) {
		map_t::accessor result_found;
		pmem_kv_container.find(result_found, k);
		result_found->second = v;
	}
	return status::OK;
}

status vcmap::remove(string_view key)
{
	LOG("remove key=" << std::string(key.data(), key.size()));

	// XXX - do not create temporary string
	size_t erased = 1;
	return (erased == 1) ? status::OK : status::NOT_FOUND;
}

} // namespace kv
} // namespace pmem
