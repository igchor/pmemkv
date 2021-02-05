// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2020, Intel Corporation */

#include "new_map.h"
#include "../out.h"

#include <tbb/tbb.h>

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
	compaction_cv.wait(sh_lock, [&] { return immutable_map == nullptr; });

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

	std::shared_lock<std::shared_timed_mutex> it_lock(iteration_mtx);
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
		return status::NOT_FOUND;
	}

	callback(result->second.c_str(), result->second.size(), arg);

	return status::OK;
}

// XXX: make sure imm is alive for this funcion duration
std::thread new_map::start_bg_compaction()
{
	static std::mutex test_mtx;

	// XXX: create thread pool
	return std::thread([&] {
		std::unique_lock<std::shared_timed_mutex> lock(iteration_mtx);

		assert(immutable_map != nullptr);

		try {
			std::atomic<size_t> tid;
			tid = 0;

			auto range = immutable_map->map.range(grainsize);

			tbb::parallel_for(
				range,
				[&](const dram_map_type::container_type::range_type
					    &range) {
					auto id = tid.fetch_add(1, std::memory_order_relaxed); // XXX: order?

					pmem::obj::transaction::run(pmpool, [&] {
						assert(pmem->insert_log[id].size() == 0);
						assert(pmem->remove_log[id].size() == 0);

						pmem->insert_log[id].reserve(
							immutable_map->size());
						pmem->remove_log[id].reserve(
							immutable_map->size());

						for (const auto &e : range) {
							if (e.second ==
							    dram_map_type::tombstone) {
								pmem->remove_log[id]
									.emplace_back(
										e.first);
							} else {
								pmem->insert_log[id]
									.emplace_back(
										e.first,
										e.second);
							}
						}
					});
				});

			pmem->logs = tid.load();
			pmpool.persist(pmem->logs);

			assert(pmem->logs > 0);

			using range_type_in =
				tbb::blocked_range<decltype(&pmem->insert_log[0])>;
			tbb::parallel_for(
				range_type_in(&pmem->insert_log[0],
					      &pmem->insert_log[0] + pmem->logs),
				[&](const range_type_in &range) {
					for (auto &r : range) {
						for (auto &e : r) {
							container->insert_or_assign(
								std::move(e.first),
								std::move(e.second));
						}
					}
				});

			using range_type_re =
				tbb::blocked_range<decltype(&pmem->remove_log[0])>;
			tbb::parallel_for(
				range_type_re(&pmem->remove_log[0],
					      &pmem->remove_log[0] + pmem->logs),
				[&](const range_type_re &range) {
					for (auto &r : range) {
						for (auto &e : r) {
							container->erase(e);
						}
					}
				});

			pmem::obj::transaction::run(pmpool, [&] {
				for (int i = 0; i < pmem->logs; i++) {
					pmem->insert_log[i].clear();
					pmem->remove_log[i].clear();
				}
			});

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

			pmem->logs = 0;
			pmpool.persist(pmem->logs);

			{
				std::unique_lock<std::shared_timed_mutex> lock(
					compaction_mtx);
				immutable_map = nullptr;
			}

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

			compaction_cv.wait(lock,
					   [&] { return immutable_map == nullptr; });

			if (dram_has_space(mutable_map))
				continue;

			assert(immutable_map == nullptr);

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
		pmem = static_cast<internal::new_map::pmem_type *>(
			pmemobj_direct(*root_oid));

		container = &pmem->map;
	} else {
		pmem::obj::transaction::run(pmpool, [&] {
			pmem::obj::transaction::snapshot(root_oid);
			*root_oid =
				pmem::obj::make_persistent<internal::new_map::pmem_type>()
					.raw();
			pmem = static_cast<internal::new_map::pmem_type *>(
				pmemobj_direct(*root_oid));

			container = &pmem->map;

			// container->resize(SHARDS_NUM);
		});
	}

	// mtxs = std::vector<mutex_type>(SHARDS_NUM);

	mutable_map = std::make_unique<dram_map_type>();
	immutable_map = nullptr;

	auto dram_capacity = std::getenv("DRAM_CAPACITY");
	if (dram_capacity)
		this->dram_capacity = std::stoi(dram_capacity);

	auto grainsize = std::getenv("GRAINSIZE");
	if (grainsize)
		this->grainsize = std::stoi(grainsize);

	oneapi::tbb::global_control c(
		oneapi::tbb::global_control::max_allowed_parallelism, 24);
}

} // namespace kv
} // namespace pmem
