// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2020, Intel Corporation */

#include "csmap.h"
#include "../out.h"

namespace pmem
{
namespace kv
{

csmap::csmap(std::unique_ptr<internal::config> cfg)
    : handle(cfg), config(std::move(cfg))
{
	LOG("Started ok");
	Recover();
}

csmap::~csmap()
{
	LOG("Stopped ok");
}

std::string csmap::name()
{
	return "csmap";
}

status csmap::count_all(std::size_t &cnt)
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

status csmap::count_above(string_view key, std::size_t &cnt)
{
	LOG("count_above for key=" << std::string(key.data(), key.size()));

	std::shared_lock<global_mutex_type> lock(mtx);

	auto it = container->upper_bound(key);
	auto end = container->end();

	cnt = size(it, end);

	return status::OK;
}

status csmap::count_equal_above(string_view key, std::size_t &cnt)
{
	LOG("count_equal_above for key=" << std::string(key.data(), key.size()));

	std::shared_lock<global_mutex_type> lock(mtx);

	auto it = container->lower_bound(key);
	auto end = container->end();

	cnt = size(it, end);

	return status::OK;
}

status csmap::count_equal_below(string_view key, std::size_t &cnt)
{
	LOG("count_equal_below for key=" << std::string(key.data(), key.size()));

	std::shared_lock<global_mutex_type> lock(mtx);

	auto it = container->begin();
	auto end = container->upper_bound(key);

	cnt = size(it, end);

	return status::OK;
}

status csmap::count_below(string_view key, std::size_t &cnt)
{
	LOG("count_below for key=" << std::string(key.data(), key.size()));

	std::shared_lock<global_mutex_type> lock(mtx);

	auto it = container->begin();
	auto end = container->lower_bound(key);

	cnt = size(it, end);

	return status::OK;
}

status csmap::count_between(string_view key1, string_view key2, std::size_t &cnt)
{
	LOG("count_between for key1=" << key1.data() << ", key2=" << key2.data());

	std::shared_lock<global_mutex_type> lock(mtx);

	if (key1.compare(key2) < 0) {
		auto it = container->upper_bound(key1);
		auto end = container->lower_bound(key2);

		cnt = size(it, end);
	} else {
		cnt = 0;
	}

	return status::OK;
}

status csmap::get_all(get_kv_callback *callback, void *arg)
{
	LOG("get_all");
	check_outside_tx();

	std::shared_lock<global_mutex_type> lock(mtx);

	for (auto it = container->begin(); it != container->end(); ++it) {
		auto ret = callback(it->first.c_str(), it->first.size(),
				    it->second.val.c_str(), it->second.val.size(), arg);

		if (ret != 0)
			return status::STOPPED_BY_CB;
	}

	return status::OK;
}

status csmap::get_above(string_view key, get_kv_callback *callback, void *arg)
{
	LOG("get_above for key=" << std::string(key.data(), key.size()));

	std::shared_lock<global_mutex_type> lock(mtx);

	for (auto it = container->upper_bound(key); it != container->end(); ++it) {
		auto ret = callback(it->first.c_str(), it->first.size(),
				    it->second.val.c_str(), it->second.val.size(), arg);

		if (ret != 0)
			return status::STOPPED_BY_CB;
	}

	return status::OK;
}

status csmap::get_equal_above(string_view key, get_kv_callback *callback, void *arg)
{
	LOG("get_equal_above for key=" << std::string(key.data(), key.size()));

	std::shared_lock<global_mutex_type> lock(mtx);

	for (auto it = container->lower_bound(key); it != container->end(); ++it) {
		auto ret = callback(it->first.c_str(), it->first.size(),
				    it->second.val.c_str(), it->second.val.size(), arg);

		if (ret != 0)
			return status::STOPPED_BY_CB;
	}

	return status::OK;
}

status csmap::get_equal_below(string_view key, get_kv_callback *callback, void *arg)
{
	LOG("get_equal_below for key=" << std::string(key.data(), key.size()));

	std::shared_lock<global_mutex_type> lock(mtx);

	for (auto it = container->begin(); it != container->upper_bound(key); ++it) {
		auto ret = callback(it->first.c_str(), it->first.size(),
				    it->second.val.c_str(), it->second.val.size(), arg);

		if (ret != 0)
			return status::STOPPED_BY_CB;
	}

	return status::OK;
}

status csmap::get_below(string_view key, get_kv_callback *callback, void *arg)
{
	LOG("get_below for key=" << std::string(key.data(), key.size()));

	std::shared_lock<global_mutex_type> lock(mtx);

	for (auto it = container->begin(); it != container->lower_bound(key); ++it) {
		auto ret = callback(it->first.c_str(), it->first.size(),
				    it->second.val.c_str(), it->second.val.size(), arg);

		if (ret != 0)
			return status::STOPPED_BY_CB;
	}

	return status::OK;
}

status csmap::get_between(string_view key1, string_view key2, get_kv_callback *callback,
			  void *arg)
{
	LOG("get_between for key1=" << key1.data() << ", key2=" << key2.data());

	std::shared_lock<global_mutex_type> lock(mtx);

	if (key1.compare(key2) < 0) {
		for (auto it = container->upper_bound(key1);
		     it != container->lower_bound(key2); ++it) {
			auto ret = callback(it->first.c_str(), it->first.size(),
					    it->second.val.c_str(), it->second.val.size(),
					    arg);

			if (ret != 0)
				return status::STOPPED_BY_CB;
		}
	}

	return status::OK;
}

status csmap::exists(string_view key)
{
	LOG("exists for key=" << std::string(key.data(), key.size()));
	check_outside_tx();

	std::shared_lock<global_mutex_type> lock(mtx);
	return container->contains(key) ? status::OK : status::NOT_FOUND;
}

status csmap::get(string_view key, get_v_callback *callback, void *arg)
{
	LOG("get key=" << std::string(key.data(), key.size()));
	check_outside_tx();

	std::shared_lock<global_mutex_type> lock(mtx);
	auto it = container->find(key);
	if (it != container->end()) {
		std::shared_lock<decltype(it->second.mtx)> lock(it->second.mtx);
		callback(it->second.val.c_str(), it->second.val.size(), arg);
		return status::OK;
	}

	LOG("  key not found");
	return status::NOT_FOUND;
}

status csmap::put(string_view key, string_view value)
{
	LOG("put key=" << std::string(key.data(), key.size())
		       << ", value.size=" << std::to_string(value.size()));
	check_outside_tx();

	std::shared_lock<global_mutex_type> lock(mtx);

	auto result = container->try_emplace(key, value);

	if (result.second == false) {
		auto &it = result.first;
		std::unique_lock<decltype(it->second.mtx)> lock(it->second.mtx);
		pmem::obj::transaction::run(handle.pop(), [&] {
			it->second.val.assign(value.data(), value.size());
		});
	}

	return status::OK;
}

status csmap::remove(string_view key)
{
	LOG("remove key=" << std::string(key.data(), key.size()));
	check_outside_tx();
	std::unique_lock<global_mutex_type> lock(mtx);
	return container->unsafe_erase(key) > 0 ? status::OK : status::NOT_FOUND;
}

void csmap::Recover()
{
	if (!handle.get()) {
		container = &handle.get()->map;
		container->runtime_initialize();
		container->key_comp().runtime_initialize(
			internal::extract_comparator(*config));

		return;
	}

	pmem::obj::transaction::run(handle.pool(), [&] {
		handle.initialize(pmem::obj::make_persistent<internal::cmap::map_t>());

		container = &handle.get()->map;
		container->key_comp().initialize(
				internal::extract_comparator(*config));
		container->runtime_initialize();
	});
}

} // namespace kv
} // namespace pmem
