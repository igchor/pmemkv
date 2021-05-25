// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2020-2021, Intel Corporation */

#ifndef LIBPMEMKV_RADIX_H
#define LIBPMEMKV_RADIX_H

#include "../comparator/pmemobj_comparator.h"
#include "../iterator.h"
#include "../pmemobj_engine.h"

#include <libpmemobj++/persistent_ptr.hpp>

#include <libpmemobj++/experimental/inline_string.hpp>
#include <libpmemobj++/experimental/radix_tree.hpp>
#include <tbb/concurrent_queue.h>

#include <atomic>

#include <libpmemobj++/container/mpsc_queue.hpp>

#include <list>

#include <condition_variable>
#include <mutex>
#include <shared_mutex>

namespace pmem
{
namespace kv
{
namespace internal
{
namespace radix
{

using map_type =
	pmem::obj::experimental::radix_tree<pmem::obj::experimental::inline_string,
					    pmem::obj::experimental::inline_string>;

struct pmem_type {
	pmem_type() : map()
	{
		std::memset(reserved, 0, sizeof(reserved));
	}

	map_type map;
	pmem::obj::persistent_ptr<char[]> log;
	uint64_t reserved[6];
};

static_assert(sizeof(pmem_type) == sizeof(map_type) + 64, "");

class transaction : public ::pmem::kv::internal::transaction {
public:
	transaction(pmem::obj::pool_base &pop, map_type *container);
	status put(string_view key, string_view value) final;
	status remove(string_view key) final;
	status commit() final;
	void abort() final;

private:
	pmem::obj::pool_base &pop;
	dram_log log;
	map_type *container;
};

template <typename T>
struct timestamped_entry {
	template <typename... Args>
	timestamped_entry(Args &&... args) : data_(std::forward<Args>(args)...)
	{
		timestamp_.store(current_timestamp(), std::memory_order_release);
	}

	timestamped_entry(timestamped_entry&& rhs) {
		this->data_ = std::move(rhs.data_);
		this->timestamp_.store(rhs.timestamp_.load(std::memory_order_acquire), std::memory_order_release);
	}

	template <typename K>
	timestamped_entry &operator=(K &&rhs)
	{
		this->data_ = std::forward<K>(rhs);
		this->timestamp_.store(current_timestamp(), std::memory_order_release);
		return *this;
	}

	T &data()
	{
		return data_;
	}

	size_t timestamp()
	{
		return timestamp_.load(std::memory_order_acquire);
	}

	bool clear_timestamp(size_t excpected_timestamp)
	{
		return timestamp_.compare_exchange_strong(excpected_timestamp, 0,
							  std::memory_order_release);
	}

	bool clear_timestamp(size_t excpected_timestamp)
	{
		return timestamp_.compare_exchange_strong(excpected_timestamp, 0,
							  std::memory_order_release);
	}

private:
	T data_;
	std::atomic<size_t> timestamp_;

	uint64_t current_timestamp()
	{
		auto count = std::chrono::duration_cast<std::chrono::microseconds>(
				     std::chrono::steady_clock::now().time_since_epoch())
				     .count();

		return static_cast<uint64_t>(count);
	}
};

template <typename Value>
class ordered_cache {
public:
	ordered_cache(size_t max_size): max_size(max_size) {}

	static string_view tombstone()
	{
		return "tombstone"; // XXX
	}

	template <typename F, typename... Args>
	std::pair<Value*, bool> put(string_view key, Value&& v, F&& evict) {
		auto ret = map.try_emplace(key, lru_list_type::iterator{});
		if (ret.second) {
			if (lru_list.size() < max_size) {
				lru_list.emplace_front(ret.first, std::move(v));
				*ret.first = lru_list.begin();
			} else {
				auto it = evict(lru_list);

				if (it == lru_list.end())
					return {nullptr, false};

				auto cnt = map.erase(it->first);

				assert(cnt == 1 && it->first != key);

				lru_list.splice(lru_list.begin(), lru_list, it);
				lru_list.begin()->first = ret.first;
				lru_list.begin()->second = std::move(v);

				*ret.first = lru_list.begin();
			}
		} else {
			lru_list.splice(lru_list.begin(), lru_list, ret.first->second);
			lru_list.begin()->second = value;
		}

		return {&ret.first->second, true};
	}

	std::pair<Value*, bool> get(string_view key) {
		auto it = map.find(key);
		if (it == map.end())
			return std::pair<string_view, bool>{nullptr, false};
		else
			return std::pair<string_view, bool>{&it->second.data(), true};
	}

private:
	using dram_value_type =
		std::pair<std::map<std::string, lru_list_type::iterator>::iterator, Value>;
	using lru_list_type = std::list<dram_value_type>;
	using dram_map_type = std::map<std::string, lru_list_type::iterator>;

	lru_list_type lru_list;
	dram_map_type map;
	const size_t max_size;
};

} /* namespace radix */
} /* namespace internal */

/**
 * Radix tree engine backed by:
 * https://github.com/pmem/libpmemobj-cpp/blob/master/include/libpmemobj%2B%2B/experimental/radix_tree.hpp
 *
 * It is a sorted, singlethreaded engine. Unlike other sorted engines it does not support
 * custom comparator (the order is defined by the keys' representation).
 *
 * The implementation is a variation of a PATRICIA trie - the internal
 * nodes do not store the path explicitly, but only a position at which
 * the keys differ. Keys are stored entirely in leafs.
 *
 * More info about radix tree: https://en.wikipedia.org/wiki/Radix_tree
 */
class radix : public pmemobj_engine_base<internal::radix::pmem_type> {
	template <bool IsConst>
	class radix_iterator;

public:
	radix(std::unique_ptr<internal::config> cfg);
	~radix();

	radix(const radix &) = delete;
	radix &operator=(const radix &) = delete;

	std::string name() final;

	status count_all(std::size_t &cnt) final;
	status count_above(string_view key, std::size_t &cnt) final;
	status count_equal_above(string_view key, std::size_t &cnt) final;
	status count_equal_below(string_view key, std::size_t &cnt) final;
	status count_below(string_view key, std::size_t &cnt) final;
	status count_between(string_view key1, string_view key2, std::size_t &cnt) final;

	status get_all(get_kv_callback *callback, void *arg) final;
	status get_above(string_view key, get_kv_callback *callback, void *arg) final;
	status get_equal_above(string_view key, get_kv_callback *callback,
			       void *arg) final;
	status get_equal_below(string_view key, get_kv_callback *callback,
			       void *arg) final;
	status get_below(string_view key, get_kv_callback *callback, void *arg) final;
	status get_between(string_view key1, string_view key2, get_kv_callback *callback,
			   void *arg) final;

	status exists(string_view key) final;

	status get(string_view key, get_v_callback *callback, void *arg) final;

	status put(string_view key, string_view value) final;

	status remove(string_view key) final;

	internal::transaction *begin_tx() final;

	internal::iterator_base *new_iterator() final;
	internal::iterator_base *new_const_iterator() final;

private:
	using container_type = internal::radix::map_type;

	void Recover();
	status iterate(typename container_type::const_iterator first,
		       typename container_type::const_iterator last,
		       get_kv_callback *callback, void *arg);

	container_type *container;
	std::unique_ptr<internal::config> config;
};

class heterogenous_radix : public pmemobj_engine_base<internal::radix::pmem_type> {
public:
	heterogenous_radix(std::unique_ptr<internal::config> cfg);
	~heterogenous_radix();

	heterogenous_radix(const heterogenous_radix &) = delete;
	heterogenous_radix &operator=(const heterogenous_radix &) = delete;

	std::string name() final;

	status count_all(std::size_t &cnt) final;

	status get_all(get_kv_callback *callback, void *arg) final;

	status exists(string_view key) final;

	status put(string_view key, string_view value) final;

	status remove(string_view k) final;

	status get(string_view key, get_v_callback *callback, void *arg) final;

private:
	using container_type = internal::radix::map_type;

	ordered_cache cache;

	struct queue_entry {
		queue_entry(size_t timestamp, dram_value_type *dram_entry,
			    string_view key_, string_view value_);

		string_view key() const;
		string_view value() const;

		size_t timestamp;
		dram_value_type *dram_entry;

		// XXX - inline_string
		std::string key_;
		std::string value_;
	};

	size_t dram_size;

	std::atomic<bool> stopped;
	std::thread bg_thread;

	pmem::obj::pool_base pop;

	container_type *container;
	std::unique_ptr<internal::config> config;

	std::mutex eviction_lock;
	std::condition_variable eviction_cv;
	std::atomic<size_t> consumed_cnt = 0;

	tbb::concurrent_bounded_queue<queue_entry*> queue;

	void bg_work();
	void cache_put(string_view key, string_view value, bool block);
	void cache_hit();
};

template <>
class radix::radix_iterator<true> : public internal::iterator_base {
	using container_type = radix::container_type;

public:
	radix_iterator(container_type *container);

	status seek(string_view key) final;
	status seek_lower(string_view key) final;
	status seek_lower_eq(string_view key) final;
	status seek_higher(string_view key) final;
	status seek_higher_eq(string_view key) final;

	status seek_to_first() final;
	status seek_to_last() final;

	status is_next() final;
	status next() final;
	status prev() final;

	result<string_view> key() final;

	result<pmem::obj::slice<const char *>> read_range(size_t pos, size_t n) final;

protected:
	container_type *container;
	container_type::iterator it_;
	pmem::obj::pool_base pop;
};

template <>
class radix::radix_iterator<false> : public radix::radix_iterator<true> {
	using container_type = radix::container_type;

public:
	radix_iterator(container_type *container);

	result<pmem::obj::slice<char *>> write_range(size_t pos, size_t n) final;

	status commit() final;
	void abort() final;

private:
	std::vector<std::pair<std::string, size_t>> log;
};

class radix_factory : public engine_base::factory_base {
public:
	std::unique_ptr<engine_base>
	create(std::unique_ptr<internal::config> cfg) override
	{
		check_config_null(get_name(), cfg);
		return std::unique_ptr<engine_base>(
			new heterogenous_radix(std::move(cfg)));
		// uint64_t dram_caching;
		// if (cfg->get_uint64("dram_caching", &dram_caching) && dram_caching) {
		// 	return std::unique_ptr<engine_base>(new
		// heterogenous_radix(std::move(cfg))); } else { 	return
		// std::unique_ptr<engine_base>(new radix(std::move(cfg)));
		// }
	};

	std::string get_name() override
	{
		return "radix";
	};
};

} /* namespace kv */
} /* namespace pmem */

#endif /* LIBPMEMKV_RADIX_H */
