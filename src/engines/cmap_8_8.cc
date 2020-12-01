// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2017-2020, Intel Corporation */

#include "cmap_8_8.h"
#include "../out.h"

#include <unistd.h>

namespace pmem
{
namespace kv
{

cmap_8_8::cmap_8_8(std::unique_ptr<internal::config> cfg) : pmemobj_engine_base(cfg, "pmemkv_cmap_8_8")
{
	LOG("Started ok");
	Recover();
}

cmap_8_8::~cmap_8_8()
{
	LOG("Stopped ok");
}

std::string cmap_8_8::name()
{
	return "cmap";
}

status cmap_8_8::count_all(std::size_t &cnt)
{
	LOG("count_all");
	check_outside_tx();
	cnt = container->size();

	return status::OK;
}

status cmap_8_8::get_all(get_kv_callback *callback, void *arg)
{
	LOG("get_all");
	check_outside_tx();
	for (auto it = container->begin(); it != container->end(); ++it) {
		auto ret = callback((const char*) &it->first, 8,
				    (const char*) &it->second, 8, arg);

		if (ret != 0)
			return status::STOPPED_BY_CB;
	}

	return status::OK;
}

status cmap_8_8::exists(string_view key)
{
	LOG("exists for key=" << std::string(key.data(), key.size()));
	check_outside_tx();
	auto k = *((uint64_t*)key.data());
	return container->count(k) == 1 ? status::OK : status::NOT_FOUND;
}

status cmap_8_8::get(string_view key, get_v_callback *callback, void *arg)
{
	LOG("get key=" << std::string(key.data(), key.size()));
	check_outside_tx();
	internal::cmap_8_8::map_t::const_accessor result;
	auto k = *((uint64_t*)key.data());
	bool found = container->find(result, k);
	if (!found) {
		LOG("  key not found");
		return status::NOT_FOUND;
	}

	callback((const char*) &result->second, 8, arg);
	return status::OK;
}

status cmap_8_8::put(string_view key, string_view value)
{
	LOG("put key=" << std::string(key.data(), key.size())
		       << ", value.size=" << std::to_string(value.size()));
	check_outside_tx();
	auto k = *((uint64_t*)key.data());
	auto v = *((uint64_t*)value.data());

	container->insert_or_assign(k, v);

	return status::OK;
}

status cmap_8_8::remove(string_view key)
{
	LOG("remove key=" << std::string(key.data(), key.size()));
	check_outside_tx();

	auto k = *((uint64_t*)key.data());

	bool erased = container->erase(k);
	return erased ? status::OK : status::NOT_FOUND;
}

status cmap_8_8::defrag(double start_percent, double amount_percent)
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

void cmap_8_8::Recover()
{
	if (!OID_IS_NULL(*root_oid)) {
		container = (pmem::kv::internal::cmap_8_8::map_t *)pmemobj_direct(*root_oid);
		container->runtime_initialize();
	} else {
		pmem::obj::transaction::run(pmpool, [&] {
			pmem::obj::transaction::snapshot(root_oid);
			*root_oid =
				pmem::obj::make_persistent<internal::cmap_8_8::map_t>().raw();
			container = (pmem::kv::internal::cmap_8_8::map_t *)pmemobj_direct(
				*root_oid);
			container->runtime_initialize();
		});
	}
}

} // namespace kv
} // namespace pmem
