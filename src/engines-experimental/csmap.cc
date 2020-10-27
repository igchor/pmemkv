// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2020, Intel Corporation */

#include "csmap.h"
#include "../out.h"

namespace pmem
{
namespace kv
{
namespace internal
{
namespace csmap
{

transaction::transaction(global_mutex_type &mtx, pmem::obj::pool_base &pop, map_type *container)
    : mtx(mtx),
      pop(pop),
      tx(new pmem::obj::transaction::manual(pop)),
      container(container)
{
}

status transaction::put(string_view key, string_view value)
{
	assert(pmemobj_tx_stage() == TX_STAGE_WORK);

	// acc.get().emplace_back(key, value);

	shared_global_lock_type lock(mtx);

	// auto ret = container->try_emplace(key, value);
	// 	auto &mapped = ret.first->second;
	// 	if (!ret.second) {
	// 		mapped.val = value; // XXX - do swap and deallocate
	// 						  // it in a separate tx

	// 		// We can just build action log for swaps (or assignments?)
	// 		// XXX - if we'd hold global lock from the beginning of tx,
	// 		// we can build it in put? what about new nodes?
	// 	}

	auto result = container->try_emplace(key, value);

	if (result.second == false) {
		auto &it = result.first;
		unique_node_lock_type lock(it->second.mtx);
		//pmem::obj::transaction::run(pop, [&] {
			it->second.val.assign(value.data(), value.size());
		//});
	}

	return status::OK;



	// XXX - optimization for blind update : check if element exists, and if so
	// just build action log for std::move(value) in csmap

	// XXX - global lock would have to be held from start of tx until commit ends
	// but we could use timed lock
}

// XXX - we could allow calling container->try_emplace in a transaction
status transaction::commit()
{
	//pmem::obj::transaction::commit();

	/* At this point, redo log is stored durably on pmem. */

	// tx.reset(nullptr);
	// tx.reset(new pmem::obj::transaction::manual(pop));

	// shared_global_lock_type lock(mtx);

	// // XXX - we could preallocate skip list nodes instead of kv pairs.
	// // For existing nodes we could just move the value (without tx, just atomic
	// // instructions) For non-existing, we also can just use atomic instructions.

	// std::vector<shared_node_lock_type> locks;
	// locks.reserve(acc.get().size());

	// for (auto &e : acc.get()) {
	// 	// XXX - do we want to keep all locks till the end of loop? PROBABLY YES
	// 	auto ret =
	// 		container->try_emplace(std::move(e.first), std::move(e.second));
	// 	auto &mapped = ret.first->second;
	// 	locks.emplace_back(mapped.mtx);
	// 	if (!ret.second) {
	// 		mapped.val = std::move(e.second); // XXX - do swap and deallocate
	// 						  // it in a separate tx

	// 		// We can just build action log for swaps (or assignments?)
	// 		// XXX - if we'd hold global lock from the beginning of tx,
	// 		// we can build it in put? what about new nodes?
	// 	}
	// }

	// // XXX - we could hold locks so that we can insert in a single tx
	// // and could use preallocated buffer
	// pmem::obj::transaction::commit();
	// tx.reset(nullptr);

	return status::OK;
}

void transaction::abort()
{
	try {
		pmem::obj::transaction::abort(0);
	} catch (pmem::manual_tx_abort &) {
		/* do nothing */
	}
}

}
}

csmap::csmap(std::unique_ptr<internal::config> cfg)
    : pmemobj_engine_base(cfg, "pmemkv_csmap"), config(std::move(cfg))
{
	Recover();
	LOG("Started ok");
}

csmap::~csmap()
{
	LOG("Stopped ok");
}

internal::transaction *csmap::begin_tx()
{
	// auto redo_log_accessor = ptls->local().get();
	return new internal::csmap::transaction(mtx, pmpool, container);
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
	check_outside_tx();

	shared_global_lock_type lock(mtx);

	auto first = container->upper_bound(key);
	auto last = container->end();

	cnt = size(first, last);

	return status::OK;
}

status csmap::count_equal_above(string_view key, std::size_t &cnt)
{
	LOG("count_equal_above for key=" << std::string(key.data(), key.size()));
	check_outside_tx();

	shared_global_lock_type lock(mtx);

	auto first = container->lower_bound(key);
	auto last = container->end();

	cnt = size(first, last);

	return status::OK;
}

status csmap::count_equal_below(string_view key, std::size_t &cnt)
{
	LOG("count_equal_below for key=" << std::string(key.data(), key.size()));
	check_outside_tx();

	shared_global_lock_type lock(mtx);

	auto first = container->begin();
	auto last = container->upper_bound(key);

	cnt = size(first, last);

	return status::OK;
}

status csmap::count_below(string_view key, std::size_t &cnt)
{
	LOG("count_below for key=" << std::string(key.data(), key.size()));
	check_outside_tx();

	shared_global_lock_type lock(mtx);

	auto first = container->begin();
	auto last = container->lower_bound(key);

	cnt = size(first, last);

	return status::OK;
}

status csmap::count_between(string_view key1, string_view key2, std::size_t &cnt)
{
	LOG("count_between for key1=" << key1.data() << ", key2=" << key2.data());
	check_outside_tx();

	if (container->key_comp()(key1, key2)) {
		shared_global_lock_type lock(mtx);

		auto first = container->upper_bound(key1);
		auto last = container->lower_bound(key2);

		cnt = size(first, last);
	} else {
		cnt = 0;
	}

	return status::OK;
}

status csmap::iterate(typename container_type::iterator first,
		      typename container_type::iterator last, get_kv_callback *callback,
		      void *arg)
{
	for (auto it = first; it != last; ++it) {
		shared_node_lock_type lock(it->second.mtx);

		auto ret = callback(it->first.c_str(), it->first.size(),
				    it->second.val.c_str(), it->second.val.size(), arg);

		if (ret != 0)
			return status::STOPPED_BY_CB;
	}

	return status::OK;
}

status csmap::get_all(get_kv_callback *callback, void *arg)
{
	LOG("get_all");
	check_outside_tx();

	shared_global_lock_type lock(mtx);

	auto first = container->begin();
	auto last = container->end();

	return iterate(first, last, callback, arg);
}

status csmap::get_above(string_view key, get_kv_callback *callback, void *arg)
{
	LOG("get_above for key=" << std::string(key.data(), key.size()));
	check_outside_tx();

	shared_global_lock_type lock(mtx);

	auto first = container->upper_bound(key);
	auto last = container->end();

	return iterate(first, last, callback, arg);
}

status csmap::get_equal_above(string_view key, get_kv_callback *callback, void *arg)
{
	LOG("get_equal_above for key=" << std::string(key.data(), key.size()));
	check_outside_tx();

	shared_global_lock_type lock(mtx);

	auto first = container->lower_bound(key);
	auto last = container->end();

	return iterate(first, last, callback, arg);
}

status csmap::get_equal_below(string_view key, get_kv_callback *callback, void *arg)
{
	LOG("get_equal_below for key=" << std::string(key.data(), key.size()));
	check_outside_tx();

	shared_global_lock_type lock(mtx);

	auto first = container->begin();
	auto last = container->upper_bound(key);

	return iterate(first, last, callback, arg);
}

status csmap::get_below(string_view key, get_kv_callback *callback, void *arg)
{
	LOG("get_below for key=" << std::string(key.data(), key.size()));
	check_outside_tx();

	shared_global_lock_type lock(mtx);

	auto first = container->begin();
	auto last = container->lower_bound(key);

	return iterate(first, last, callback, arg);
}

status csmap::get_between(string_view key1, string_view key2, get_kv_callback *callback,
			  void *arg)
{
	LOG("get_between for key1=" << key1.data() << ", key2=" << key2.data());
	check_outside_tx();

	if (container->key_comp()(key1, key2)) {
		shared_global_lock_type lock(mtx);

		auto first = container->upper_bound(key1);
		auto last = container->lower_bound(key2);
		return iterate(first, last, callback, arg);
	}

	return status::OK;
}

status csmap::exists(string_view key)
{
	LOG("exists for key=" << std::string(key.data(), key.size()));
	check_outside_tx();

	shared_global_lock_type lock(mtx);
	return container->contains(key) ? status::OK : status::NOT_FOUND;
}

status csmap::get(string_view key, get_v_callback *callback, void *arg)
{
	LOG("get key=" << std::string(key.data(), key.size()));
	check_outside_tx();

	shared_global_lock_type lock(mtx);
	auto it = container->find(key);
	if (it != container->end()) {
		shared_node_lock_type lock(it->second.mtx);
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

	shared_global_lock_type lock(mtx);

	auto result = container->try_emplace(key, value);

	if (result.second == false) {
		auto &it = result.first;
		unique_node_lock_type lock(it->second.mtx);
		pmem::obj::transaction::run(pmpool, [&] {
			it->second.val.assign(value.data(), value.size());
		});
	}

	return status::OK;
}

status csmap::remove(string_view key)
{
	LOG("remove key=" << std::string(key.data(), key.size()));
	check_outside_tx();
	unique_global_lock_type lock(mtx);
	return container->unsafe_erase(key) > 0 ? status::OK : status::NOT_FOUND;
}

void csmap::Recover()
{
	internal::csmap::pmem_type *pmem_ptr;

	if (!OID_IS_NULL(*root_oid)) {
		pmem_ptr = static_cast<internal::csmap::pmem_type *>(
			pmemobj_direct(*root_oid));

		container = &pmem_ptr->map;
		container->runtime_initialize();
		container->key_comp().runtime_initialize(
			internal::extract_comparator(*config));
	} else {
		pmem::obj::transaction::run(pmpool, [&] {
			pmem::obj::transaction::snapshot(root_oid);
			*root_oid =
				pmem::obj::make_persistent<internal::csmap::pmem_type>()
					.raw();
			pmem_ptr = static_cast<internal::csmap::pmem_type *>(
				pmemobj_direct(*root_oid));
			container = &pmem_ptr->map;
			container->runtime_initialize();
			container->key_comp().initialize(
				internal::extract_comparator(*config));
		});
	}

	// if (!pmem_ptr->ptls) {
	// 	pmem::obj::transaction::run(pmpool, [&] { // XXX - merge with prev tx
	// 		pmem_ptr->ptls = pmem::obj::make_persistent<ptls_type>();
	// 	});
	// }

	// ptls = pmem_ptr->ptls.get();
}

} // namespace kv
} // namespace pmem
