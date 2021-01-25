// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2020, Intel Corporation */

#include "new_map.h"
#include "../out.h"

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
	cnt = index->size();

	return status::OK;
}

// template <typename It>
// static std::size_t size(It first, It last)
// {
// 	auto dist = std::distance(first, last);
// 	assert(dist >= 0);

// 	return static_cast<std::size_t>(dist);
// }

// status new_map::count_above(string_view key, std::size_t &cnt)
// {
// 	LOG("count_above for key=" << std::string(key.data(), key.size()));
// 	check_outside_tx();

// 	auto first = container->upper_bound(key);
// 	auto last = container->end();

// 	cnt = size(first, last);

// 	return status::OK;
// }

// status new_map::count_equal_above(string_view key, std::size_t &cnt)
// {
// 	LOG("count_equal_above for key=" << std::string(key.data(), key.size()));
// 	check_outside_tx();

// 	auto first = container->lower_bound(key);
// 	auto last = container->end();

// 	cnt = size(first, last);

// 	return status::OK;
// }

// status new_map::count_equal_below(string_view key, std::size_t &cnt)
// {
// 	LOG("count_equal_below for key=" << std::string(key.data(), key.size()));
// 	check_outside_tx();

// 	auto first = container->begin();
// 	auto last = container->upper_bound(key);

// 	cnt = size(first, last);

// 	return status::OK;
// }

// status new_map::count_below(string_view key, std::size_t &cnt)
// {
// 	LOG("count_below for key=" << std::string(key.data(), key.size()));
// 	check_outside_tx();

// 	auto first = container->begin();
// 	auto last = container->lower_bound(key);

// 	cnt = size(first, last);

// 	return status::OK;
// }

// status new_map::count_between(string_view key1, string_view key2, std::size_t &cnt)
// {
// 	LOG("count_between for key1=" << key1.data() << ", key2=" << key2.data());
// 	check_outside_tx();

// 	if (key1.compare(key2) < 0) {
// 		auto first = container->upper_bound(key1);
// 		auto last = container->lower_bound(key2);

// 		cnt = size(first, last);
// 	} else {
// 		cnt = 0;
// 	}

// 	return status::OK;
// }

status new_map::iterate(pgm::DynamicPGMIndex<uint64_t, uint64_t>::iterator first,
		      pgm::DynamicPGMIndex<uint64_t, uint64_t>::iterator last,
		      get_kv_callback *callback, void *arg)
{
	for (auto it = first; it != last; ++it) {
		auto ret =
			callback(reinterpret_cast<const char *>(&it->first), 8, reinterpret_cast<const char *>(&it->second), 8, arg);

		if (ret != 0)
			return status::STOPPED_BY_CB;
	}

	return status::OK;
}

status new_map::get_all(get_kv_callback *callback, void *arg)
{
	LOG("get_all");
	check_outside_tx();

	auto first = index->begin();
	auto last = index->end();

	return iterate(first, last, callback, arg);
}

// status new_map::get_above(string_view key, get_kv_callback *callback, void *arg)
// {
// 	LOG("get_above for key=" << std::string(key.data(), key.size()));
// 	check_outside_tx();

// 	auto first = container->upper_bound(key);
// 	auto last = container->end();

// 	return iterate(first, last, callback, arg);
// }

// status new_map::get_equal_above(string_view key, get_kv_callback *callback, void *arg)
// {
// 	LOG("get_equal_above for key=" << std::string(key.data(), key.size()));
// 	check_outside_tx();

// 	auto first = container->lower_bound(key);
// 	auto last = container->end();

// 	return iterate(first, last, callback, arg);
// }

// status new_map::get_equal_below(string_view key, get_kv_callback *callback, void *arg)
// {
// 	LOG("get_equal_below for key=" << std::string(key.data(), key.size()));
// 	check_outside_tx();

// 	auto first = container->begin();
// 	auto last = container->upper_bound(key);

// 	return iterate(first, last, callback, arg);
// }

// status new_map::get_below(string_view key, get_kv_callback *callback, void *arg)
// {
// 	LOG("get_below for key=" << std::string(key.data(), key.size()));
// 	check_outside_tx();

// 	auto first = container->begin();
// 	auto last = container->lower_bound(key);

// 	return iterate(first, last, callback, arg);
// }

// status new_map::get_between(string_view key1, string_view key2, get_kv_callback *callback,
// 			  void *arg)
// {
// 	LOG("get_between for key1=" << key1.data() << ", key2=" << key2.data());
// 	check_outside_tx();

// 	if (key1.compare(key2) < 0) {
// 		auto first = container->upper_bound(key1);
// 		auto last = container->lower_bound(key2);
// 		return iterate(first, last, callback, arg);
// 	}

// 	return status::OK;
// }

status new_map::exists(string_view key)
{
	LOG("exists for key=" << std::string(key.data(), key.size()));
	check_outside_tx();

	auto k = *reinterpret_cast<const uint64_t *>(key.data());

	return index->find(k) != index->end() ? status::OK : status::NOT_FOUND;
}

status new_map::get(string_view key, get_v_callback *callback, void *arg)
{
	LOG("get key=" << std::string(key.data(), key.size()));
	check_outside_tx();

	auto k = *reinterpret_cast<const uint64_t *>(key.data());

	auto it = index->find(k);
	if (it != index->end()) {
		auto value = string_view(reinterpret_cast<const char *>(&it->second), 8);
		callback(value.data(), value.size(), arg);
		return status::OK;
	}

	LOG("  key not found");
	return status::NOT_FOUND;
}

status new_map::put(string_view key, string_view value)
{
	LOG("put key=" << std::string(key.data(), key.size())
		       << ", value.size=" << std::to_string(value.size()));
	check_outside_tx();

	auto k = *reinterpret_cast<const uint64_t *>(key.data());
	auto v = *reinterpret_cast<const uint64_t *>(value.data());

	obj::transaction::run(pmpool, [&]{
		index->insert_or_assign(k, v);
		*size = index->size();
	});

	return status::OK;
}

status new_map::remove(string_view key)
{
	LOG("remove key=" << std::string(key.data(), key.size()));
	check_outside_tx();

	auto k = *reinterpret_cast<const uint64_t *>(key.data());

	auto it = index->find(k);

	if (it == index->end())
		return status::NOT_FOUND;

	obj::transaction::run(pmpool, [&]{
		index->erase(k);
		*size--;
	});

	return status::OK;
}

void new_map::Recover()
{
	if (!OID_IS_NULL(*root_oid)) {
		auto pmem_ptr = static_cast<internal::new_map::pmem_type *>(
			pmemobj_direct(*root_oid));

		container = &pmem_ptr->map;
		size = &pmem_ptr->size;
	} else {
		pmem::obj::transaction::run(pmpool, [&] {
			pmem::obj::transaction::snapshot(root_oid);
			*root_oid =
				pmem::obj::make_persistent<internal::new_map::pmem_type>()
					.raw();
			auto pmem_ptr = static_cast<internal::new_map::pmem_type *>(
				pmemobj_direct(*root_oid));
			container = &pmem_ptr->map;

			size = &pmem_ptr->size;
			*size = 0;
		});
	}

	index = std::make_unique<pgm::DynamicPGMIndex<uint64_t, uint64_t>>(*container, *size);
}

} // namespace kv
} // namespace pmem
