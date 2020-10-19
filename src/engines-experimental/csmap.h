// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2020, Intel Corporation */

#pragma once

#include "../comparator/pmemobj_comparator.h"
#include "../pmemobj_engine.h"

#include <libpmemobj++/container/string.hpp>
#include <libpmemobj++/detail/enumerable_thread_specific.hpp>
#include <libpmemobj++/experimental/concurrent_map.hpp>
#include <libpmemobj++/experimental/radix_tree.hpp>
#include <libpmemobj++/experimental/self_relative_ptr.hpp>
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/shared_mutex.hpp>

#include <mutex>
#include <shared_mutex>

namespace pmem
{
namespace kv
{
namespace internal
{
namespace csmap
{

using key_type = pmem::obj::string;
using value_type = pmem::obj::string;
using node_mutex_type = pmem::obj::shared_mutex;
using global_mutex_type = std::shared_timed_mutex;
using shared_global_lock_type = std::shared_lock<global_mutex_type>;
using unique_global_lock_type = std::unique_lock<global_mutex_type>;
using shared_node_lock_type = std::shared_lock<node_mutex_type>;
using unique_node_lock_type = std::unique_lock<node_mutex_type>;

static_assert(sizeof(key_type) == 32, "");

struct mapped_type {
	mapped_type() = default;

	mapped_type(const mapped_type &other, bool commited = false) : val(other.val), commited(commited)
	{
	}

	mapped_type(mapped_type &&other, bool commited = false) : val(std::move(other.val)), commited(commited)
	{
	}

	mapped_type(const std::string &str, bool commited = false) : val(str) , commited(commited)
	{
	}

	mapped_type(string_view str, bool commited = false) : val(str.data(), str.size()), commited(commited)
	{
	}

	node_mutex_type mtx;
	value_type val;
	pmem::obj::p<bool> commited = false;
};

static_assert(sizeof(mapped_type) == 104, "");

using map_type = pmem::obj::experimental::concurrent_map<key_type, mapped_type,
							 internal::pmemobj_compare>;

using redo_log_entry_type = pmem::detail::pair<key_type, value_type>;
using redo_log_type = pmem::obj::vector<redo_log_entry_type>;

struct ptls_entry {
	ptls_entry()
	{
		std::memset(reserved, 0, sizeof(reserved));
	}

	pmem::obj::experimental::self_relative_ptr<redo_log_type> redo_log;
	uint64_t reserved[7];
};

static_assert(sizeof(ptls_entry) == 64, "");

using ptls_type = pmem::detail::enumerable_thread_specific<ptls_entry>;

struct pmem_type {
	pmem_type() : map()
	{
		std::memset(reserved, 0, sizeof(reserved));
	}

	map_type map;
	pmem::obj::experimental::self_relative_ptr<ptls_type> ptls = nullptr;
	uint64_t reserved[7];
};

static_assert(sizeof(pmem_type) == sizeof(map_type) + 64, "");

class transaction : public ::pmem::kv::internal::transaction {
public:
	transaction(pmem::obj::pool_base &pop, global_mutex_type *mtx,
		    map_type *container);
	status put(string_view key, string_view value) final;
	status commit() final;
	void abort() final;

private:
	pmem::obj::pool_base &pop;
	std::unique_ptr<pmem::obj::transaction::manual> tx;
	shared_global_lock_type lock;
	std::vector<std::pair<std::string, std::string>> v;
	map_type *container;
};

} /* namespace csmap */
} /* namespace internal */

class csmap : public pmemobj_engine_base<internal::csmap::pmem_type> {
public:
	csmap(std::unique_ptr<internal::config> cfg);
	~csmap();

	csmap(const csmap &) = delete;
	csmap &operator=(const csmap &) = delete;

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

private:
	using node_mutex_type = internal::csmap::node_mutex_type;
	using global_mutex_type = internal::csmap::global_mutex_type;
	using shared_global_lock_type = internal::csmap::shared_global_lock_type;
	using unique_global_lock_type = internal::csmap::unique_global_lock_type;
	using shared_node_lock_type = internal::csmap::shared_node_lock_type;
	using unique_node_lock_type = internal::csmap::unique_node_lock_type;
	using container_type = internal::csmap::map_type;
	using ptls_type = internal::csmap::ptls_type;

	void Recover();
	status iterate(typename container_type::iterator first,
		       typename container_type::iterator last, get_kv_callback *callback,
		       void *arg);

	/*
	 * We take read lock for thread-safe methods (like get/insert/get_all) to
	 * synchronize with unsafe_erase() which is not thread-safe.
	 */
	global_mutex_type mtx;
	container_type *container;
	ptls_type *ptls;
	std::unique_ptr<internal::config> config;
};

} /* namespace kv */
} /* namespace pmem */
