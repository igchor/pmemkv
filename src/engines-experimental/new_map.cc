// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2020, Intel Corporation */

#include "new_map.h"
#include "../out.h"

#include <set>
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
	std::unique_lock<std::shared_timed_mutex> sh_lock(compaction_mtx);
	if (immutable_map == nullptr)
		compaction_cv.wait(
					sh_lock, [&] { return immutable_map == nullptr; });

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
	return get_all(
		[](const char *k, size_t kb, const char *v, size_t vb, void *arg) {
			auto size = static_cast<size_t *>(arg);
			(*size)++;

			return PMEMKV_STATUS_OK;
		},
		&cnt);
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
static status iterate(Iterator first, Iterator last, Pred &&pred,
		      get_kv_callback *callback, void *arg)
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
	LOG("get_all");
	std::atomic<bool> shutting_down;
	check_outside_tx();

	std::unique_lock<std::shared_timed_mutex> lock(iteration_mtx);

	auto &imm = immutable_map;
	auto &mut = mutable_map;

	auto mut_pred = [](string_view, string_view v) {
		return v != dram_map_type::tombstone;
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

	return iterate(container->begin(), container->end(), pmem_pred, callback, arg);
}

status new_map::exists(string_view key)
{
	LOG("exists for key=" << std::string(key.data(), key.size()));

	return get(
		key, [](const char *, size_t, void *) {}, nullptr);
}

status new_map::get(string_view key, get_v_callback *callback, void *arg)
{
	LOG("get key=" << std::string(key.data(), key.size()));

	std::shared_lock<std::shared_timed_mutex> sh_lock(compaction_mtx);

	auto &imm = immutable_map;
	auto &mut = mutable_map;

	// XXX: think about consistency guarantees with regard to lookups in several maps
	// Maybe we could use sharding for dram also and then each key would have specific
	// lock it would also decrease number of locks take (3 -> 1) if each dram shard
	// corresponds with pmem shard - much simpler compaction THINK ABOUT SORTED
	// DATASTRUCTURES!!!

	{
		dram_map_type::const_accessor_type acc;
		auto s = mut->get(key, acc);
		if (s == dram_map_type::element_status::alive) {
			callback(acc->second.data(), acc->second.size(), arg);
			return status::OK;
		} else if (s == dram_map_type::element_status::removed) {
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
			} else if (s == dram_map_type::element_status::removed) {
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
	if (!found) {
		std::cout << mut->size() << " " << (imm ? imm->size() : 0) << std::endl;
		return status::NOT_FOUND;
	}

	callback(result->second.c_str(), result->second.size(), arg);

	return status::OK;
}

// void radix_compaction()
// {
// 	std::vector<std::vector<index_type::dram_map_type::value_type*>>
// elements(SHARDS_NUM);

// for (auto &e : *imm) {
// 	elements[shard(e->first)].push_back(&e);
//}

// 			// // XXX - allocate radix nodes before taking locks (action API?)
// 		// for (int i = 0; i < SHARDS_NUM; i++) {
// 		// 	/// XXX: all shards must be saved as a single atomic action (since we
// don't know the order)
// 		// 	// use some intermidiate layer? (list of radix nodes?) - we can treat
// this layer as L0 in LSM-tree probably

// 		// 	unique_lock_type lock(mtxs[i]);
// 		// 	obj::transaction::run(pmpool, [&]{
// 		// 		for (const auto &e : elements[i]) {
// 		// 			if (uintptr_t(e->second.data()) ==
// dram_map_type::tombstone)
// 		// 				(*container)[i].erase(e->first);
// 		// 			else
// 		// 				(*container)[i].insert_or_assign(e->first,
// e->second);
// 		// 		}
// 		// 	});
// 		// }
// }


// XXX: make sure imm is alive for this funcion duration
std::thread new_map::start_bg_compaction()
{
	// XXX: create thread pool
	return std::thread([&] {
		std::unique_lock<std::shared_timed_mutex> lock(iteration_mtx);

		assert(immutable_map != nullptr);

		auto imm_size = immutable_map->size(); // XXX: ifndef NDEBUG

		try {
			pmem::obj::transaction::run(pmpool, [&] {
				// XXX: should we ever shrink?
				// if (log->size() < imm->size())
				// log->resize(imm->size());
				assert(insert_log->size() == 0);
				assert(remove_log->size() == 0);

				/* It's safe to use immutable_map ptr without locks since
				 * no other thread can modify it until it's null. */

				// XXX: use some heuristic to decrease the space for logs
				// (e.g. size() / 2)
				insert_log->reserve(immutable_map->size());
				remove_log->reserve(immutable_map->size());

#ifndef NDEBUG
				size_t elements = 0;
#endif

				for (const auto &e : *immutable_map) {
					if (e.second == dram_map_type::tombstone) {
						remove_log->emplace_back(e.first);
					} else
						insert_log->emplace_back(e.first,
									 e.second);

#ifndef NDEBUG
					elements++;
#endif
				}

				assert(elements == immutable_map->size());
			});

			{

				for (const auto &e : *insert_log) {
					container->insert_or_assign(std::move(e.first),
								    std::move(e.second));
				}

				for (const auto &e : *remove_log) {
					container->erase(e);
				}
			}

			pmem::obj::transaction::run(pmpool, [&] {
				insert_log->clear();
				remove_log->clear();
			});

			// XXX - move this before cleaning logs - how to solve problem
			// with closing db?
			std::unique_lock<std::shared_timed_mutex> lock(compaction_mtx);

			// XXX debug
#ifndef NDEBUG
			for (const auto &e : *immutable_map) {
				if (e.second == dram_map_type::tombstone) {
					assert(container->count(string_view(
						       e.first.data(), e.first.size())) ==
					       0);
				} else
					assert(container->count(string_view(
						       e.first.data(), e.first.size())) ==
					       1);
			}
#endif

			immutable_map = nullptr;

			compaction_cv.notify_all(); // -> one?

		} catch (std::exception &e) {
			std::cout << e.what() << std::endl;
		}
	});
}

bool new_map::dram_has_space(std::unique_ptr<dram_map_type> &map)
{
	return map->size() < dram_capacity;
}

status new_map::put(string_view key, string_view value)
{
	LOG("put key=" << std::string(key.data(), key.size())
		       << ", value.size=" << std::to_string(value.size()));

	do {
		std::shared_lock<std::shared_timed_mutex> sh_lock(compaction_mtx);

		if (dram_has_space(mutable_map)) {
			mutable_map->put(key, value);
			return status::OK;
		} else {
			sh_lock.unlock();
			std::unique_lock<std::shared_timed_mutex> lock(compaction_mtx);

			// if (dram_has_space(mutable_map)) continue; // XXX: move after
			// if()?

			if (immutable_map) {
				compaction_cv.wait(
					lock, [&] { return immutable_map == nullptr; });
			}

			if (dram_has_space(mutable_map))
				continue; // XXX: move after if()?

			assert(immutable_map == nullptr);

			/* If we end up here, neither mutable nor immutable map can be
			 * changed concurrently. The only allowed concurrent change is
			 * setting immutable_map to nullptr (by the compaction thread). */
			immutable_map = std::move(mutable_map);
			mutable_map = std::make_unique<dram_map_type>();

			assert(immutable_map != nullptr);

			start_bg_compaction().detach();
		}
	} while (true);

	return status::OK;
}

status new_map::remove(string_view key)
{
	LOG("remove key=" << std::string(key.data(), key.size()));

	// XXX: there is no Way to know if element was actually deleted in thread safe
	// manner
	return put(key, dram_map_type::tombstone);
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

	mutable_map = std::make_unique<dram_map_type>();
	immutable_map = nullptr;

	auto dram_capacity = std::getenv("DRAM_CAPACITY");
	if (dram_capacity)
		this->dram_capacity = std::stoi(dram_capacity);
}

} // namespace kv
} // namespace pmem
