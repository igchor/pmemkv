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
	timestamped_entry(Args &&... args) : data(std::forward<Args>(args)...)
	{
		timestamp.store(current_timestamp());
	}

	template <typename K>
	timestamped_entry &operator=(K &&rhs)
	{
		this->data = std::forward<K>(rhs);
		this->timestamp.store(current_timestamp());
		return *this;
	}

// private:
	uint64_t current_timestamp()
	{
		auto count = std::chrono::duration_cast<std::chrono::microseconds>(
			       std::chrono::steady_clock::now().time_since_epoch())
			.count();

		return static_cast<uint64_t>(count);
	}

	T data;
	std::atomic<size_t> timestamp;
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

	heterogenous_radix(const heterogenous_radix&) = delete;
	heterogenous_radix& operator=(const heterogenous_radix&) = delete;

	std::string name() final;

	status count_all(std::size_t &cnt) final;

	status get_all(get_kv_callback *callback, void *arg) final;

	status exists(string_view key) final;

	status put(string_view key, string_view value) final;

	status remove(string_view k) final;

	status get(string_view key, get_v_callback *callback, void *arg) final;

private:
	using container_type = internal::radix::map_type;
	using dram_map_type = std::map<std::string, internal::radix::timestamped_entry<std::string>>;
	using pmem_queue_type = pmem::obj::experimental::mpsc_queue;
	using pmem_queue_worker_type = pmem_queue_type::worker;
	dram_map_type map;

	struct queue_entry {
		queue_entry(size_t timestamp, dram_map_type::mapped_type *dram_entry, string_view key, string_view value): timestamp(timestamp), dram_entry(dram_entry), key_size(key.size()), value_size(value.size()) {
			memcpy(reinterpret_cast<char*>(this + 1), key.data(), key.size());
			memcpy(reinterpret_cast<char*>(this + 1) + key.size(), value.data(), value.size());
		}

		static size_t size(string_view key, string_view value) {
			return sizeof(queue_entry) + key.size() + value.size();
		}

		string_view key() const {
			return string_view(reinterpret_cast<const char*>(this + 1), key_size);
		}

		string_view value() const {
			return string_view(reinterpret_cast<const char*>(this + 1) + key_size, value_size);
		}

		size_t timestamp;
		dram_map_type::mapped_type *dram_entry;
		size_t key_size;
		size_t value_size;
	};

	size_t dram_size;

	std::unique_ptr<pmem_queue_type> queue;
	std::unique_ptr<pmem_queue_worker_type> worker;

	std::atomic<bool> stopped;
	std::thread bg_thread;

	pmem::obj::pool_base pop;

	tbb::concurrent_bounded_queue<std::pair<string_view, size_t>>
		consumed_dram_entries; // XXX - string_view - must be protected by EBR

	container_type *container;
	std::unique_ptr<internal::config> config;

	void bg_work()
	{
		while (!stopped.load()) {
			auto acc = queue->consume();
			for (auto str_v : acc) {
				auto *e = reinterpret_cast<const queue_entry*>(str_v.data());

				if (e->timestamp !=
				    e->dram_entry->timestamp.load())
					continue;

				// XXX - make sure tx does not abort and use
				// defer_free
				if (e->value() == tombstone()) {
					// container->erase(e->key());
				} else {
					container->insert_or_assign(e->key(), e->value());
				}

				consumed_dram_entries.emplace(e->key(), e->timestamp);
			}
		}
	}

	static string_view tombstone()
	{
		return "tombstone"; // XXX
	}
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
		return std::unique_ptr<engine_base>(new heterogenous_radix(std::move(cfg)));
		// uint64_t dram_caching;
		// if (cfg->get_uint64("dram_caching", &dram_caching) && dram_caching) {
		// 	return std::unique_ptr<engine_base>(new heterogenous_radix(std::move(cfg)));
		// } else {
		// 	return std::unique_ptr<engine_base>(new radix(std::move(cfg)));
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
