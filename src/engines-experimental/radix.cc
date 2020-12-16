// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2020, Intel Corporation */

#include "radix.h"
#include "../out.h"

namespace pmem
{
namespace kv
{

radix::radix(std::unique_ptr<internal::config> cfg)
    : pmemobj_engine_base(cfg, "pmemkv_radix"),
      config(std::move(cfg)),
      mtxs(internal::radix::SHARDS)
{
	Recover();
	LOG("Started ok");
}

radix::~radix()
{
	LOG("Stopped ok");
}

std::string radix::name()
{
	return "radix";
}

static size_t hash(string_view key)
{
	return *((size_t *)key.data()) & (internal::radix::SHARDS - 1);
	// return key.size() % internal::radix::SHARDS;
}

status radix::count_all(std::size_t &cnt)
{
	size_t size = 0;
	for (size_t i = 0; i < internal::radix::SHARDS; i++) {
		shared_lock_type lock(mtxs[i]);
		size += container[i].size();
	}

	cnt = size;

	return status::OK;
}

status radix::get_all(get_kv_callback *callback, void *arg)
{
	for (size_t i = 0; i < internal::radix::SHARDS; i++) {
		shared_lock_type lock(mtxs[i]);
		for (auto it = container[i].begin(); it != container[i].end(); it++)
			callback(it->key().data(), it->key().size(), it->value().data(),
				 it->value().size(), arg);
	}

	return status::OK;
}

status radix::exists(string_view key)
{
	LOG("exists for key=" << std::string(key.data(), key.size()));
	check_outside_tx();

	auto shard = hash(key);
	shared_lock_type lock(mtxs[shard]);

	return container[shard].find(key) != container[shard].end() ? status::OK
								    : status::NOT_FOUND;
}

status radix::get(string_view key, get_v_callback *callback, void *arg)
{
	LOG("get key=" << std::string(key.data(), key.size()));
	check_outside_tx();

	auto shard = hash(key);
	shared_lock_type lock(mtxs[shard]);

	auto it = container[shard].find(key);
	if (it != container[shard].end()) {
		auto value = string_view(it->value());
		callback(value.data(), value.size(), arg);
		return status::OK;
	}

	LOG("  key not found");
	return status::NOT_FOUND;
}

status radix::put(string_view key, string_view value)
{
	LOG("put key=" << std::string(key.data(), key.size())
		       << ", value.size=" << std::to_string(value.size()));
	check_outside_tx();

	auto shard = hash(key);
	unique_lock_type lock(mtxs[shard]);

	auto result = container[shard].try_emplace(key, value);

	if (result.second == false) {
		pmem::obj::transaction::run(pmpool,
					    [&] { result.first.assign_val(value); });
	}

	return status::OK;
}

status radix::remove(string_view key)
{
	LOG("remove key=" << std::string(key.data(), key.size()));
	check_outside_tx();

	auto shard = hash(key);
	unique_lock_type lock(mtxs[shard]);

	auto it = container[shard].find(key);

	if (it == container[shard].end())
		return status::NOT_FOUND;

	container[shard].erase(it);

	return status::OK;
}

void radix::Recover()
{
	if (!OID_IS_NULL(*root_oid)) {
		auto pmem_ptr = static_cast<internal::radix::pmem_type *>(
			pmemobj_direct(*root_oid));

		container = pmem_ptr->map.get();
	} else {
		pmem::obj::transaction::run(pmpool, [&] {
			pmem::obj::transaction::snapshot(root_oid);
			*root_oid =
				pmem::obj::make_persistent<internal::radix::pmem_type>()
					.raw();
			auto pmem_ptr = static_cast<internal::radix::pmem_type *>(
				pmemobj_direct(*root_oid));

			pmem_ptr->map = obj::make_persistent<
				internal::radix::map_type[internal::radix::SHARDS]>();

			container = pmem_ptr->map.get();
		});
	}
}

} // namespace kv
} // namespace pmem
