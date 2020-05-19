// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2017-2020, Intel Corporation */

#include "cmap.h"
#include "../out.h"

#include <unistd.h>

namespace pmem
{
namespace kv
{

cmap::cmap(std::unique_ptr<internal::config> cfg) : handle(cfg)
{
	static_assert(
		sizeof(internal::cmap::string_t) == 40,
		"Wrong size of cmap value and key. This probably means that std::string has size > 32");

	recover();

	LOG("Started ok");
}

cmap::~cmap()
{
	LOG("Stopped ok");
}

std::string cmap::name()
{
	return "cmap";
}

status cmap::count_all(std::size_t &cnt)
{
	LOG("count_all");
	check_outside_tx();
	cnt = container->size();

	return status::OK;
}

status cmap::get_all(get_kv_callback *callback, void *arg)
{
	LOG("get_all");
	check_outside_tx();
	for (auto it = container->begin(); it != container->end(); ++it) {
		auto ret = callback(it->first.c_str(), it->first.size(),
				    it->second.c_str(), it->second.size(), arg);

		if (ret != 0)
			return status::STOPPED_BY_CB;
	}

	return status::OK;
}

status cmap::exists(string_view key)
{
	LOG("exists for key=" << std::string(key.data(), key.size()));
	check_outside_tx();
	return container->count(key) == 1 ? status::OK : status::NOT_FOUND;
}

status cmap::get(string_view key, get_v_callback *callback, void *arg)
{
	LOG("get key=" << std::string(key.data(), key.size()));
	check_outside_tx();
	internal::cmap::map_t::const_accessor result;
	bool found = container->find(result, key);
	if (!found) {
		LOG("  key not found");
		return status::NOT_FOUND;
	}

	callback(result->second.c_str(), result->second.size(), arg);
	return status::OK;
}

status cmap::put(string_view key, string_view value)
{
	LOG("put key=" << std::string(key.data(), key.size())
		       << ", value.size=" << std::to_string(value.size()));
	check_outside_tx();

	container->insert_or_assign(key, value);

	return status::OK;
}

status cmap::remove(string_view key)
{
	LOG("remove key=" << std::string(key.data(), key.size()));
	check_outside_tx();

	bool erased = container->erase(key);
	return erased ? status::OK : status::NOT_FOUND;
}

status cmap::defrag(double start_percent, double amount_percent)
{
	LOG("defrag: start_percent = " << start_percent
				       << " amount_percent = " << amount_percent);
	check_outside_tx();

	try {
		container->defragment(start_percent, amount_percent);
	} catch (std::range_error &e) {
		out_err_stream("defrag") << e.what();
		return status::INVALID_ARGUMENT;
	} catch (pmem::defrag_error &e) {
		out_err_stream("defrag") << e.what();
		return status::DEFRAG_ERROR;
	}

	return status::OK;
}

void cmap::recover()
{
	if (!handle.get()) {
		container = handle.get();
		container->runtime_initialize();

		return;
	}

	pmem::obj::transaction::run(handle.pool(), [&] {
		handle.initialize(pmem::obj::make_persistent<internal::cmap::map_t>());

		container = handle.get();
		container->runtime_initialize();
	});
}

} // namespace kv
} // namespace pmem
