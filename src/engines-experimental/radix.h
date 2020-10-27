// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2020, Intel Corporation */

#pragma once

#include "../comparator/pmemobj_comparator.h"
#include "../pmemobj_engine.h"

#include <libpmemobj++/persistent_ptr.hpp>

#include <libpmemobj++/container/vector.hpp>

#include <libpmemobj++/experimental/inline_string.hpp>
#include <libpmemobj++/experimental/radix_tree.hpp>

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

using redo_log_type = pmem::obj::vector<map_type::node_handle>;

using set_type = pmem::obj::experimental::radix_tree<size_t, redo_log_type>;

struct redo_log_set {
	struct accessor {
		accessor(set_type *set) : set(set)
		{
			thread_local size_t counter = 0;

			auto id = counter++;
			auto ret = set->try_emplace(id);

			/* There should be no entry with specified id. */
			assert(ret.second);
			entry = ret.first;
		}

		accessor(accessor&& acc) {
			entry = acc.entry;
			set = acc.set;

			acc.set = nullptr;
		}

		void commit()
		{
			if (set) {
				auto r = set->erase(entry->key());
				assert(r == 1);
			}
		}

		redo_log_type &get()
		{
			return entry->value();
		}

	private:
		typename set_type::iterator entry;
		set_type *set;
	};

	accessor get()
	{
		return accessor(&redo_logs);
	}

private:
	set_type redo_logs;
};

struct pmem_type {
	pmem_type() : map()
	{
		std::memset(reserved, 0, sizeof(reserved));
	}

	map_type map;
	pmem::obj::experimental::self_relative_ptr<redo_log_set> redo_log = nullptr;
	uint64_t reserved[7];
};

static_assert(sizeof(pmem_type) == sizeof(map_type) + 64, "");

class transaction : public ::pmem::kv::internal::transaction {
public:
	transaction(pmem::obj::pool_base &pop, map_type *container, typename redo_log_set::accessor &&acc): pop(pop), container(container), redo_log(redo_log), tx(new pmem::obj::transaction::manual(pop)), acc(std::move(acc)) {
		acc.get().reserve(10);
	}
	status put(string_view key, string_view value) final {
		acc.get().push_back(map_type::node_type::make(nullptr, key, value));
		return status::OK;
	}

	status commit() final {
		for (auto &e : acc.get()) {
			auto result = container->insert(std::move(e));

			if (result.second == false) {
				result.first.assign_val(e->value());
			}
		}

		acc.commit();

		pmem::obj::transaction::commit();
		return status::OK;
	}
	void abort() final {
		try {
			pmem::obj::transaction::abort(0);
		} catch (pmem::manual_tx_abort &) {
			/* do nothing */
		}
	}

private:
	pmem::obj::pool_base &pop;
	map_type *container;
	redo_log_type *redo_log;
	std::unique_ptr<pmem::obj::transaction::manual> tx;
	typename redo_log_set::accessor acc;
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

	internal::transaction *begin_tx() final {
		return new internal::radix::transaction(pmpool, container, redo_log->get());
	}

private:
	using container_type = internal::radix::map_type;

	void Recover();
	status iterate(typename container_type::const_iterator first,
		       typename container_type::const_iterator last,
		       get_kv_callback *callback, void *arg);

	container_type *container;
	std::unique_ptr<internal::config> config;
	internal::radix::redo_log_set* redo_log;
};

} /* namespace kv */
} /* namespace pmem */
