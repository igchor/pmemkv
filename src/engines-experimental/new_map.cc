// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2020, Intel Corporation */

#include "new_map.h"
#include "../out.h"

#include <tbb/tbb.h>

#include <libpmemobj/action_base.h>

#include <libpmemobj++/make_persistent_array.hpp>

#include <set>
namespace pmem
{
namespace kv
{

new_map::new_map(std::unique_ptr<internal::config> cfg)
    : pmemobj_engine_base(cfg, "pmemkv_new_map"), config(std::move(cfg))
{
	Recover();
	LOG("Started ok");
}

new_map::~new_map()
{
	LOG("Stopped ok");
}

std::string new_map::name()
{
	return "new_map";
}

status new_map::count_all(std::size_t &cnt)
{
	LOG("count_all");
	check_outside_tx();

	// XXX - this is very slow and not thread safe
	cnt = index->size();
	return status::OK;
}

// template <typename Iterator, decltype(std::declval<Iterator>()->first) = {}>
// static int key(const Iterator& it) {
// 	return it->first;
// }

// template <typename Iterator, decltype(std::declval<Iterator>()->key()) = {}>
// static int key(const Iterator& it) {
// 	return it->key();
// }

// template <typename Iterator, decltype(std::declval<Iterator>()->second) = {}>
// static int value(const Iterator& it) {
// 	return it->second;
// }

// template <typename Iterator, decltype(std::declval<Iterator>()->value()) = {}>
// static int value(const Iterator& it) {
// 	return it->value();
// }

template <typename Iterator, typename Pred>
static status iterate(Iterator first, Iterator last, Pred &&pred,
		      get_kv_callback *callback, void *arg)
{
	for (auto it = first; it != last; ++it) {
		// string_view key = key(it);
		// string_view value = value(it);
		string_view key = it->first;
		string_view value = it->second;

		if (!pred(key, value))
			continue;

		auto ret =
			callback(key.data(), key.size(), value.data(), value.size(), arg);

		if (ret != 0)
			return status::STOPPED_BY_CB;
	}

	return status::OK;
}

status new_map::get_all(get_kv_callback *callback, void *arg)
{
	LOG("get_all");
	std::atomic<bool> shutting_down;
	
	return iterate(index->begin(), index->end(), [](string_view, string_view) { return true;}, callback, arg);
}

status new_map::exists(string_view key)
{
	LOG("exists for key=" << std::string(key.data(), key.size()));

	return get(
		key, [](const char *, size_t, void *) {}, nullptr);
}

status new_map::get(string_view key, get_v_callback *callback, void *arg)
{
	LOG("get key=" << std::string(key.data(), key.size()));

	dram_index::const_accessor acc;
	auto ret = index->find(acc, key);
	if (ret) {
		callback(acc->second.data(), acc->second.size(), arg);
		return status::OK;	
	}

	return status::NOT_FOUND;
}

status new_map::put(string_view key, string_view value)
{
	LOG("put key=" << std::string(key.data(), key.size())
		       << ", value.size=" << std::to_string(value.size()));

	static thread_local size_t tid = cnt.fetch_add(1, std::memory_order_relaxed);

	assert(tid < 16);

	auto size = sizes[tid]++;

	actions acts(pmpool, 8);

	pmem->str[tid][size].first.assign(acts, key);
	pmem->str[tid][size].second.assign(acts, value);

	pmemobj_drain(pmpool.handle());
	acts.publish();

	dram_index::accessor acc;
	auto ret = index->insert(acc, key);

	*const_cast<string_view*>(&acc->first) = string_view(pmem->str[tid][size].first);

	acc->second = string_view(pmem->str[tid][size].second);

	return status::OK;
}

status new_map::remove(string_view key)
{
	LOG("remove key=" << std::string(key.data(), key.size()));

	return index->erase(key) ? status::OK : status::NOT_FOUND;
}

void new_map::Recover()
{
	auto dram_capacity = std::getenv("DRAM_CAPACITY");
	if (dram_capacity)
		this->dram_capacity = std::stoi(dram_capacity);

	if (!OID_IS_NULL(*root_oid)) {
		pmem = static_cast<pmem_type *>(
			pmemobj_direct(*root_oid));

	} else {
		pmem::obj::transaction::run(pmpool, [&] {
			pmem::obj::transaction::snapshot(root_oid);
			*root_oid =
				pmem::obj::make_persistent<pmem_type>()
					.raw();
			pmem = static_cast<pmem_type *>(
				pmemobj_direct(*root_oid));

			for (int i = 0; i < 16; i++) {
				pmem->str[i] = obj::make_persistent<std::pair<act_string, act_string>[]>(this->dram_capacity / 16);
				sizes[i] = 0;
			}
		});
	}

	index = std::make_unique<dram_index>();
}

} // namespace kv
} // namespace pmem
