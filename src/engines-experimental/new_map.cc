// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2020, Intel Corporation */

#include "new_map.h"
#include "../out.h"

namespace pmem
{
namespace kv
{

static inline uint64_t mix(uint64_t h)
{
	h ^= h >> 23;
	h *= 0x2127599bf4325c37ULL;
	return h ^ h >> 47;
}

/*
 * hash --  calculate the hash of a piece of memory
 */
static uint64_t fast_hash(size_t key_size, const char *key)
{
	/* fast-hash, by Zilong Tan */
	const uint64_t m = 0x880355f21e6d1965ULL;
	const uint64_t *pos = (const uint64_t *)key;
	const uint64_t *end = pos + (key_size / 8);
	uint64_t h = key_size * m;

	while (pos != end)
		h = (h ^ mix(*pos++)) * m;

	if (key_size & 7) {
		uint64_t shift = (key_size & 7) * 8;
		uint64_t mask = (1ULL << shift) - 1;
		uint64_t v = htole64(*pos) & mask;
		h = (h ^ mix(v)) * m;
	}

	return mix(h);
}

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
	cnt = container->size();

	return status::OK;
}

template <typename It>
static std::size_t size(It first, It last)
{
	auto dist = std::distance(first, last);
	assert(dist >= 0);

	return static_cast<std::size_t>(dist);
}

status new_map::count_above(string_view key, std::size_t &cnt)
{
	LOG("count_above for key=" << std::string(key.data(), key.size()));
	check_outside_tx();

	auto first = container->upper_bound(key);
	auto last = container->end();

	cnt = size(first, last);

	return status::OK;
}

status new_map::count_equal_above(string_view key, std::size_t &cnt)
{
	LOG("count_equal_above for key=" << std::string(key.data(), key.size()));
	check_outside_tx();

	auto first = container->lower_bound(key);
	auto last = container->end();

	cnt = size(first, last);

	return status::OK;
}

status new_map::count_equal_below(string_view key, std::size_t &cnt)
{
	LOG("count_equal_below for key=" << std::string(key.data(), key.size()));
	check_outside_tx();

	auto first = container->begin();
	auto last = container->upper_bound(key);

	cnt = size(first, last);

	return status::OK;
}

status new_map::count_below(string_view key, std::size_t &cnt)
{
	LOG("count_below for key=" << std::string(key.data(), key.size()));
	check_outside_tx();

	auto first = container->begin();
	auto last = container->lower_bound(key);

	cnt = size(first, last);

	return status::OK;
}

status new_map::count_between(string_view key1, string_view key2, std::size_t &cnt)
{
	LOG("count_between for key1=" << key1.data() << ", key2=" << key2.data());
	check_outside_tx();

	if (key1.compare(key2) < 0) {
		auto first = container->upper_bound(key1);
		auto last = container->lower_bound(key2);

		cnt = size(first, last);
	} else {
		cnt = 0;
	}

	return status::OK;
}

status new_map::iterate(typename container_type::const_iterator first,
		      typename container_type::const_iterator last,
		      get_kv_callback *callback, void *arg)
{
	for (auto it = first; it != last; ++it) {
		string_view key = it->key();
		string_view value = it->value();

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
	check_outside_tx();

	auto first = container->begin();
	auto last = container->end();

	return iterate(first, last, callback, arg);
}

status new_map::get_above(string_view key, get_kv_callback *callback, void *arg)
{
	LOG("get_above for key=" << std::string(key.data(), key.size()));
	check_outside_tx();

	auto first = container->upper_bound(key);
	auto last = container->end();

	return iterate(first, last, callback, arg);
}

status new_map::get_equal_above(string_view key, get_kv_callback *callback, void *arg)
{
	LOG("get_equal_above for key=" << std::string(key.data(), key.size()));
	check_outside_tx();

	auto first = container->lower_bound(key);
	auto last = container->end();

	return iterate(first, last, callback, arg);
}

status new_map::get_equal_below(string_view key, get_kv_callback *callback, void *arg)
{
	LOG("get_equal_below for key=" << std::string(key.data(), key.size()));
	check_outside_tx();

	auto first = container->begin();
	auto last = container->upper_bound(key);

	return iterate(first, last, callback, arg);
}

status new_map::get_below(string_view key, get_kv_callback *callback, void *arg)
{
	LOG("get_below for key=" << std::string(key.data(), key.size()));
	check_outside_tx();

	auto first = container->begin();
	auto last = container->lower_bound(key);

	return iterate(first, last, callback, arg);
}

status new_map::get_between(string_view key1, string_view key2, get_kv_callback *callback,
			  void *arg)
{
	LOG("get_between for key1=" << key1.data() << ", key2=" << key2.data());
	check_outside_tx();

	if (key1.compare(key2) < 0) {
		auto first = container->upper_bound(key1);
		auto last = container->lower_bound(key2);
		return iterate(first, last, callback, arg);
	}

	return status::OK;
}

status new_map::exists(string_view key)
{
	LOG("exists for key=" << std::string(key.data(), key.size()));
	check_outside_tx();

	return container->find(key) != container->end() ? status::OK : status::NOT_FOUND;
}

status new_map::get(string_view key, get_v_callback *callback, void *arg)
{
	LOG("get key=" << std::string(key.data(), key.size()));
	check_outside_tx();

	auto it = container->find(key);
	if (it != container->end()) {
		auto value = string_view(it->value());
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

	auto result = container->try_emplace(key, value);

	if (result.second == false) {
		pmem::obj::transaction::run(pmpool,
					    [&] { result.first.assign_val(value); });
	}

	return status::OK;
}

status new_map::remove(string_view key)
{
	LOG("remove key=" << std::string(key.data(), key.size()));
	check_outside_tx();

	auto it = container->find(key);

	if (it == container->end())
		return status::NOT_FOUND;

	container->erase(it);

	return status::OK;
}

internal::transaction *new_map::begin_tx()
{
	return new internal::new_map::transaction(pmpool, container);
}

void new_map::Recover()
{
	if (!OID_IS_NULL(*root_oid)) {
		pmem_ptr = static_cast<internal::new_map::pmem_type *>(
			pmemobj_direct(*root_oid));
	} else {
		pmem::obj::transaction::run(pmpool, [&] {
			pmem::obj::transaction::snapshot(root_oid);
			*root_oid =
				pmem::obj::make_persistent<internal::new_map::pmem_type>()
					.raw();
			pmem_ptr = static_cast<internal::new_map::pmem_type *>(
				pmemobj_direct(*root_oid));
		});
	}

	auto dram_capacity = std::getenv("DRAM_CAPACITY");
	if (dram_capacity)
		this->dram_capacity = std::stoi(dram_capacity);

	auto log_size = std::getenv("LOG_SIZE");
	if (log_size)
		this->log_size = std::stoi(log_size);

	auto worker_threads = std::getenv("BG_THREADS");
	if (worker_threads)
		this->bg_threads = std::stoi(worker_threads);
}

} // namespace kv
} // namespace pmem
