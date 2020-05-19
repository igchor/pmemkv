// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2017-2020, Intel Corporation */

#include <iostream>
#include <unistd.h>

#include <libpmemobj++/make_persistent_atomic.hpp>
#include <libpmemobj++/transaction.hpp>

#include "../out.h"
#include "stree.h"

using pmem::detail::conditional_add_to_tx;
using pmem::obj::make_persistent_atomic;
using pmem::obj::transaction;

namespace pmem
{
namespace kv
{

stree::stree(std::unique_ptr<internal::config> cfg) : handle(cfg)
{
	Recover();
	LOG("Started ok");
}

stree::~stree()
{
	LOG("Stopped ok");
}

std::string stree::name()
{
	return "stree";
}

status stree::count_all(std::size_t &cnt)
{
	LOG("count_all");
	check_outside_tx();

	auto result = std::distance(my_btree->begin(), my_btree->end());
	assert(result >= 0);

	cnt = static_cast<std::size_t>(result);

	return status::OK;
}

// above key, key exclusive
status stree::count_above(string_view key, std::size_t &cnt)
{
	LOG("count_above key>=" << std::string(key.data(), key.size()));
	check_outside_tx();

	internal::stree::btree_type::iterator it = my_btree->upper_bound(
		pstring<internal::stree::MAX_KEY_SIZE>(key.data(), key.size()));
	auto result = std::distance(it, my_btree->end());
	assert(result >= 0);

	cnt = static_cast<std::size_t>(result);

	return status::OK;
}

// above or equal to key, key inclusive
status stree::count_equal_above(string_view key, std::size_t &cnt)
{
	LOG("count_above key>=" << std::string(key.data(), key.size()));
	check_outside_tx();

	internal::stree::btree_type::iterator it = my_btree->lower_bound(
		pstring<internal::stree::MAX_KEY_SIZE>(key.data(), key.size()));
	auto result = std::distance(it, my_btree->end());
	assert(result >= 0);

	cnt = static_cast<std::size_t>(result);

	return status::OK;
}

// below key, key exclusive
status stree::count_below(string_view key, std::size_t &cnt)
{
	LOG("count_below key<" << std::string(key.data(), key.size()));
	check_outside_tx();

	internal::stree::btree_type::iterator it = my_btree->lower_bound(
		pstring<internal::stree::MAX_KEY_SIZE>(key.data(), key.size()));
	auto result = std::distance(my_btree->begin(), it);
	assert(result >= 0);

	cnt = static_cast<std::size_t>(result);

	return status::OK;
}

// below or equal to key, key inclusive
status stree::count_equal_below(string_view key, std::size_t &cnt)
{
	LOG("count_above key>=" << std::string(key.data(), key.size()));
	check_outside_tx();

	internal::stree::btree_type::iterator it = my_btree->upper_bound(
		pstring<internal::stree::MAX_KEY_SIZE>(key.data(), key.size()));
	auto result = std::distance(my_btree->begin(), it);
	assert(result >= 0);

	cnt = static_cast<std::size_t>(result);

	return status::OK;
}

status stree::count_between(string_view key1, string_view key2, std::size_t &cnt)
{
	LOG("count_between key range=[" << std::string(key1.data(), key1.size()) << ","
					<< std::string(key2.data(), key2.size()) << ")");
	check_outside_tx();

	internal::stree::btree_type::iterator it1 = my_btree->upper_bound(
		pstring<internal::stree::MAX_KEY_SIZE>(key1.data(), key1.size()));
	internal::stree::btree_type::iterator it2 = my_btree->lower_bound(
		pstring<internal::stree::MAX_KEY_SIZE>(key2.data(), key2.size()));

	if (key1.compare(key2) < 0) {
		auto result = std::distance(it1, it2);
		assert(result >= 0);

		cnt = static_cast<std::size_t>(result);
	} else {
		cnt = 0;
	}

	return status::OK;
}

status stree::get_all(get_kv_callback *callback, void *arg)
{
	LOG("get_all");
	check_outside_tx();
	for (auto &iterator : *my_btree) {
		auto ret = callback(iterator.first.c_str(), iterator.first.size(),
				    iterator.second.c_str(), iterator.second.size(), arg);
		if (ret != 0)
			return status::STOPPED_BY_CB;
	}

	return status::OK;
}

// (key, end), above key
status stree::get_above(string_view key, get_kv_callback *callback, void *arg)
{
	LOG("get_above start key>=" << std::string(key.data(), key.size()));
	check_outside_tx();
	internal::stree::btree_type::iterator it = my_btree->upper_bound(
		pstring<internal::stree::MAX_KEY_SIZE>(key.data(), key.size()));
	while (it != my_btree->end()) {
		auto ret = callback((*it).first.c_str(), (*it).first.size(),
				    (*it).second.c_str(), (*it).second.size(), arg);
		if (ret != 0)
			return status::STOPPED_BY_CB;
		it++;
	}

	return status::OK;
}

// [key, end), above or equal to key
status stree::get_equal_above(string_view key, get_kv_callback *callback, void *arg)
{
	LOG("get_equal_above start key>=" << std::string(key.data(), key.size()));
	check_outside_tx();
	internal::stree::btree_type::iterator it = my_btree->lower_bound(
		pstring<internal::stree::MAX_KEY_SIZE>(key.data(), key.size()));
	while (it != my_btree->end()) {
		auto ret = callback((*it).first.c_str(), (*it).first.size(),
				    (*it).second.c_str(), (*it).second.size(), arg);
		if (ret != 0)
			return status::STOPPED_BY_CB;
		it++;
	}

	return status::OK;
}

// [start, key], below or equal to key
status stree::get_equal_below(string_view key, get_kv_callback *callback, void *arg)
{
	LOG("get_equal_above start key>=" << std::string(key.data(), key.size()));
	check_outside_tx();
	internal::stree::btree_type::iterator it = my_btree->begin();
	auto pskey = pstring<internal::stree::MAX_KEY_SIZE>(key.data(), key.size());
	while (it != my_btree->end() && !((*it).first > pskey)) {
		auto ret = callback((*it).first.c_str(), (*it).first.size(),
				    (*it).second.c_str(), (*it).second.size(), arg);
		if (ret != 0)
			return status::STOPPED_BY_CB;
		it++;
	}

	return status::OK;
}

// [start, key), less than key, key exclusive
status stree::get_below(string_view key, get_kv_callback *callback, void *arg)
{
	LOG("get_below key<" << std::string(key.data(), key.size()));
	check_outside_tx();
	auto pskey = pstring<internal::stree::MAX_KEY_SIZE>(key.data(), key.size());
	internal::stree::btree_type::iterator it = my_btree->begin();
	while (it != my_btree->end() && (*it).first < pskey) {
		auto ret = callback((*it).first.c_str(), (*it).first.size(),
				    (*it).second.c_str(), (*it).second.size(), arg);
		if (ret != 0)
			return status::STOPPED_BY_CB;
		it++;
	}

	return status::OK;
}

// get between (key1, key2), key1 exclusive, key2 exclusive
status stree::get_between(string_view key1, string_view key2, get_kv_callback *callback,
			  void *arg)
{
	LOG("get_between key range=[" << std::string(key1.data(), key1.size()) << ","
				      << std::string(key2.data(), key2.size()) << ")");
	check_outside_tx();
	auto pskey1 = pstring<internal::stree::MAX_KEY_SIZE>(key1.data(), key1.size());
	auto pskey2 = pstring<internal::stree::MAX_KEY_SIZE>(key2.data(), key2.size());
	internal::stree::btree_type::iterator it = my_btree->upper_bound(pskey1);
	while (it != my_btree->end() && (*it).first < pskey2) {
		auto ret = callback((*it).first.c_str(), (*it).first.size(),
				    (*it).second.c_str(), (*it).second.size(), arg);
		if (ret != 0)
			return status::STOPPED_BY_CB;
		it++;
	}

	return status::OK;
}

status stree::exists(string_view key)
{
	LOG("exists for key=" << std::string(key.data(), key.size()));
	check_outside_tx();
	internal::stree::btree_type::iterator it = my_btree->find(
		pstring<internal::stree::MAX_KEY_SIZE>(key.data(), key.size()));
	if (it == my_btree->end()) {
		LOG("  key not found");
		return status::NOT_FOUND;
	}
	return status::OK;
}

status stree::get(string_view key, get_v_callback *callback, void *arg)
{
	LOG("get using callback for key=" << std::string(key.data(), key.size()));
	check_outside_tx();
	internal::stree::btree_type::iterator it = my_btree->find(
		pstring<internal::stree::MAX_KEY_SIZE>(key.data(), key.size()));
	if (it == my_btree->end()) {
		LOG("  key not found");
		return status::NOT_FOUND;
	}

	callback(it->second.c_str(), it->second.size(), arg);
	return status::OK;
}

status stree::put(string_view key, string_view value)
{
	LOG("put key=" << std::string(key.data(), key.size())
		       << ", value.size=" << std::to_string(value.size()));
	check_outside_tx();

	auto result = my_btree->insert(std::make_pair(
		pstring<internal::stree::MAX_KEY_SIZE>(key.data(), key.size()),
		pstring<internal::stree::MAX_VALUE_SIZE>(value.data(), value.size())));
	if (!result.second) { // key already exists, so update
		typename internal::stree::btree_type::value_type &entry = *result.first;
		transaction::manual tx(pmpool);
		conditional_add_to_tx(&(entry.second));
		entry.second = std::string(value.data(), value.size());
		transaction::commit();
	}
	return status::OK;
}

status stree::remove(string_view key)
{
	LOG("remove key=" << std::string(key.data(), key.size()));
	check_outside_tx();

	auto result = my_btree->erase(std::string(key.data(), key.size()));
	return (result == 1) ? status::OK : status::NOT_FOUND;
}

void stree::Recover()
{
	if (!handle.get()) {
		my_btree = handle.get();
		my_btree->garbage_collection();

		return;
	}

	pmem::obj::transaction::run(handle.pool(), [&] {
		handle.initialize(pmem::obj::make_persistent<internal::stree::btree_type>());
		my_btree = handle.get();
	});
}

} // namespace kv
} // namespace pmem
