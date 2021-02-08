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
	std::unique_lock<std::shared_timed_mutex> sh_lock(compaction_mtx);
	compaction_cv.wait(sh_lock, [&] { return immutable_map == nullptr; });

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
	cnt = 0;
	return get_all(
		[](const char *k, size_t kb, const char *v, size_t vb, void *arg) {
			auto size = static_cast<size_t *>(arg);
			(*size)++;

			return PMEMKV_STATUS_OK;
		},
		&cnt);
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
	check_outside_tx();

	return status::OK;
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

	return status::OK;
}

// XXX: make sure imm is alive for this funcion duration
std::thread new_map::start_bg_compaction()
{
}

bool new_map::dram_has_space(std::unique_ptr<dram_map_type> &map)
{
	return map->size() < dram_capacity;
}

status new_map::put(string_view key, string_view value)
{
	LOG("put key=" << std::string(key.data(), key.size())
		       << ", value.size=" << std::to_string(value.size()));

	std::vector<pobj_action> actions(6);

	actions.emplace_back();
	auto k = pmemobj_reserve(pmpool.handle(), &actions.back(), key.size(), 0);
	actions.emplace_back();
	auto v = pmemobj_reserve(pmpool.handle(), &actions.back(), value.size(), 0);

	pmemobj_memcpy(pmpool.handle(), pmemobj_direct(k), key.data(), key.size(), 0);
	pmemobj_memcpy(pmpool.handle(), pmemobj_direct(v), value.data(), value.size(), 0);

	auto id = cnt.fetch_add(1, std::memory_order_relaxed);

	actions.emplace_back();
	pmemobj_set_value(pmpool.handle(), &actions.back(), &pmem->str[id].first.data.off, k.off);
	actions.emplace_back();
	pmemobj_set_value(pmpool.handle(), &actions.back(), &pmem->str[id].first.size, key.size());

	actions.emplace_back();
	pmemobj_set_value(pmpool.handle(), &actions.back(), &pmem->str[id].second.data.off, v.off);
	actions.emplace_back();
	pmemobj_set_value(pmpool.handle(), &actions.back(), &pmem->str[id].first.size, value.size());

	pmemobj_drain(pmpool.handle());
	pmemobj_publish(pmpool.handle(), actions.data(), actions.size());

	map11.insert({k.off, v.off});

	return status::OK;
}

status new_map::remove(string_view key)
{
	LOG("remove key=" << std::string(key.data(), key.size()));

	// XXX: there is no Way to know if element was actually deleted in thread safe
	// manner
	return put(key, dram_map_type::tombstone);
}

void new_map::Recover()
{
	auto dram_capacity = std::getenv("DRAM_CAPACITY");
	if (dram_capacity)
		this->dram_capacity = std::stoi(dram_capacity);

	auto grainsize = std::getenv("GRAINSIZE");
	if (grainsize)
		this->grainsize = std::stoi(grainsize);

	if (!OID_IS_NULL(*root_oid)) {
		pmem = static_cast<internal::new_map::pmem_type *>(
			pmemobj_direct(*root_oid));

		container = &pmem->map;
	} else {
		pmem::obj::transaction::run(pmpool, [&] {
			pmem::obj::transaction::snapshot(root_oid);
			*root_oid =
				pmem::obj::make_persistent<internal::new_map::pmem_type>()
					.raw();
			pmem = static_cast<internal::new_map::pmem_type *>(
				pmemobj_direct(*root_oid));

			container = &pmem->map;

			pmem->str = obj::make_persistent<std::pair<internal::new_map::act_string, internal::new_map::act_string>[]>(dram_capacity);

			// container->resize(SHARDS_NUM);
		});
	}

	// mtxs = std::vector<mutex_type>(SHARDS_NUM);

	mutable_map = std::make_unique<dram_map_type>();
	immutable_map = nullptr;

	cnt = 0;
}

} // namespace kv
} // namespace pmem
