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
	while (std::atomic_load_explicit(&immutable_map, std::memory_order_acquire) != nullptr) {
		// XXX: wait on cond var
	}

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
	return get_all([](const char* k, size_t kb, const char* v, size_t vb, void* arg){
		auto size = static_cast<size_t*>(arg);
		(*size)++;

		return PMEMKV_STATUS_OK;
	}, &cnt);
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
static status iterate(Iterator first, Iterator last, Pred &&pred, get_kv_callback *callback, void *arg)
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
	LOG("get_all");	std::atomic<bool> shutting_down;
	check_outside_tx();

	auto mut = std::atomic_load_explicit(&mutable_map, std::memory_order_acquire);
	auto imm = std::atomic_load_explicit(&immutable_map, std::memory_order_acquire);

	// XXX: can this happen? probably yes
	if (mut == imm)
		imm = nullptr;

	auto mut_pred = [](string_view, string_view v) {
		return (uintptr_t)v.data() != dram_map_type::tombstone;
	};

	auto imm_pred = [&](string_view k, string_view v) {
		return !mut->exists(k) && mut_pred(k, v);
	};

	auto pmem_pred = [&](string_view k, string_view v) {
		return (!imm || !imm->exists(k)) && !mut->exists(k);
	};

	auto s = iterate(mut->begin(), mut->end(), mut_pred, callback, arg);
	if (s != status::OK)
		return s;

	if (imm) {
		s = iterate(imm->begin(), imm->end(), imm_pred, callback, arg);
		if (s != status::OK)
			return s;
	}

	// for (auto &c : *container) {
	// 	s = iterate(c.cbegin(), c.cend(), pmem_pred, callback, arg);
	// 	if (s != status::OK)
	// 		return s;
	// }

	// return s;

		// XXX: should it be above?
	std::unique_lock<std::shared_timed_mutex> lock(iteration_mtx);

	return iterate(container->begin(), container->end(), pmem_pred, callback, arg);
}

status new_map::exists(string_view key)
{
	LOG("exists for key=" << std::string(key.data(), key.size()));

	return get(key, [](const char*, size_t, void*){}, nullptr);
}

status new_map::get(string_view key, get_v_callback *callback, void *arg)
{
	LOG("get key=" << std::string(key.data(), key.size()));
	
	auto mut = std::atomic_load_explicit(&mutable_map, std::memory_order_acquire);
	auto imm = std::atomic_load_explicit(&immutable_map, std::memory_order_acquire);

	// XXX: think about consistency guarantees with regard to lookups in several maps
	// Maybe we could use sharding for dram also and then each key would have specific lock
	// it would also decrease number of locks take (3 -> 1)
	// if each dram shard corresponds with pmem shard - much simpler compaction
	// THINK ABOUT SORTED DATASTRUCTURES!!!

	{
		dram_map_type::const_accessor_type acc;
		auto s = mut->get(key, acc);
		if (s == dram_map_type::element_status::alive) {
			callback(acc->second.data(), acc->second.size(), arg);
			return status::OK;
		} else if (s ==  dram_map_type::element_status::removed) {
			return status::NOT_FOUND;
		}
	}
	
	{
		if (imm) {
			dram_map_type::const_accessor_type acc;
			auto s = imm->get(key, acc);
			if (s == dram_map_type::element_status::alive) {
				callback(acc->second.data(), acc->second.size(), arg);
				return status::OK;
			} else if (s ==  dram_map_type::element_status::removed) {
				return status::NOT_FOUND;
			}
		}
	}

	// unique_lock_type lock(mtxs[shard(key)]);

	// auto it = container->find(key);
	// if (it != container->end()) {
	// 	auto value = string_view(it->value());
	// 	callback(value.data(), value.size(), arg);
	// 	return status::OK;
	// }

	container_type::const_accessor result;
	auto found = container->find(result, key);
	if (!found)
		return status::NOT_FOUND;

	callback(result->second.c_str(), result->second.size(), arg);

	return status::OK;
}

// void radix_compaction()
// {
		// 	std::vector<std::vector<index_type::dram_map_type::value_type*>> elements(SHARDS_NUM);

		// for (auto &e : *imm) {
		// 	elements[shard(e->first)].push_back(&e);
//}

// 			// // XXX - allocate radix nodes before taking locks (action API?)
// 		// for (int i = 0; i < SHARDS_NUM; i++) {
// 		// 	/// XXX: all shards must be saved as a single atomic action (since we don't know the order)
// 		// 	// use some intermidiate layer? (list of radix nodes?) - we can treat this layer as L0 in LSM-tree probably

// 		// 	unique_lock_type lock(mtxs[i]);
// 		// 	obj::transaction::run(pmpool, [&]{
// 		// 		for (const auto &e : elements[i]) {
// 		// 			if (uintptr_t(e->second.data()) == dram_map_type::tombstone)
// 		// 				(*container)[i].erase(e->first);
// 		// 			else	
// 		// 				(*container)[i].insert_or_assign(e->first, e->second);
// 		// 		}
// 		// 	});
// 		// }
// }

void new_map::start_bg_compaction(std::shared_ptr<dram_map_type> imm)
{
	// XXX: create thread pool
	auto compaction_thread = std::thread([&, imm=imm]{
		try {
		pmem::obj::transaction::run(pmpool, [&]{
			// XXX: should we ever shrink?
			// if (log->size() < imm->size())
			// log->resize(imm->size());
			assert(insert_log->size() == 0);
			assert(remove_log->size() == 0);

			insert_log->reserve(imm->size());
			remove_log->reserve(imm->size());

			for (const auto &e : *imm) {
				if ((uint64_t) e.second.data() == dram_map_type::tombstone)
					remove_log->emplace_back(e.first);
				else
					insert_log->emplace_back(e.first, e.second);
			}
		});

		{
			std::shared_lock<std::shared_timed_mutex> lock(iteration_mtx);
			for (const auto &e : *insert_log) {
				container->insert_or_assign(std::move(e.first), std::move(e.second));
			}

			for (const auto &e : *remove_log) {
				container->erase(e);
			}
		}

		pmem::obj::transaction::run(pmpool, [&]{
			insert_log->clear();
			remove_log->clear();
		});

		// XXX - move this before cleaning logs - how to solve problem with closing db?
		std::atomic_store_explicit(&immutable_map, std::shared_ptr<dram_map_type>(nullptr), std::memory_order_release);

		} catch(std::exception &e) {
			std::cout << e.what() << std::endl;
		}
	});

	compaction_thread.detach();
}

bool new_map::dram_has_space(std::shared_ptr<dram_map_type> map)
{
	return map->size() < 1024; // XXX - parameterize
}

status new_map::put(string_view key, string_view value)
{
	LOG("put key=" << std::string(key.data(), key.size())
		       << ", value.size=" << std::to_string(value.size()));

	auto mut = std::atomic_load_explicit(&mutable_map, std::memory_order_acquire);

	if (!dram_has_space(mut)) {
		while (true) {
			// XXX: to avoid CAS for immutable/mutable map assignments
			std::unique_lock<std::mutex> lock(compaction_mtx);
			mut = std::atomic_load_explicit(&mutable_map, std::memory_order_acquire);

			// XXX - what if there is always close right after put?
			if (dram_has_space(mut)) break;

			auto imm = std::atomic_load_explicit(&immutable_map, std::memory_order_acquire);
			if (imm) {
				// XXX: wait on cond var
				//abort();
				continue;
			} else {
				std::atomic_store_explicit(&immutable_map, mutable_map, std::memory_order_release);
				std::atomic_store_explicit(&mutable_map, std::make_shared<dram_map_type>(), std::memory_order_release);

				start_bg_compaction(mut);
			}
		}	
	}

	// once we reach this point, there might be no space in the dram again...
	// but if buffer is of reasonable size this should not be a problem (we will exceed the capacity by a small value)
	// we could have custom allocation wrapper - throw exception on eom and restart
	mut->put(key, value);

	return status::OK;
}

status new_map::remove(string_view key)
{
	LOG("remove key=" << std::string(key.data(), key.size()));

	auto mut = std::atomic_load_explicit(&mutable_map, std::memory_order_acquire);

	auto found = exists(key);

	mut->remove(key);

	/* XXX: Should we just always succeed? checking if the element was actually removed could be expensive */
	// Currently, this is not thread safe
	return found;
}

void new_map::Recover()
{
	if (!OID_IS_NULL(*root_oid)) {
		auto pmem_ptr = static_cast<internal::new_map::pmem_type *>(
			pmemobj_direct(*root_oid));

		container = &pmem_ptr->map;
		insert_log = &pmem_ptr->insert_log;
		remove_log = &pmem_ptr->remove_log;
	} else {
		pmem::obj::transaction::run(pmpool, [&] {
			pmem::obj::transaction::snapshot(root_oid);
			*root_oid =
				pmem::obj::make_persistent<internal::new_map::pmem_type>()
					.raw();
			auto pmem_ptr = static_cast<internal::new_map::pmem_type *>(
				pmemobj_direct(*root_oid));

			container = &pmem_ptr->map;
			insert_log = &pmem_ptr->insert_log;
			remove_log = &pmem_ptr->remove_log;

			// container->resize(SHARDS_NUM);
		});
	}

	// mtxs = std::vector<mutex_type>(SHARDS_NUM);

	mutable_map = std::make_shared<dram_map_type>();
	immutable_map = nullptr;
}

} // namespace kv
} // namespace pmem
