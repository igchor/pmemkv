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
	is_shutting_down.store(true);
	bg_cv.notify_one();

	std::unique_lock<std::mutex> sh_lock(compaction_mtx);
	compaction_cv.wait(sh_lock, [&] { return *immutable_map.load() == nullptr; });

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

template <typename Iterator, typename Pred>
static status iterate(Iterator first, Iterator last, Pred &&pred,
		      get_kv_callback *callback, void *arg)
{
	for (auto it = first; it != last; ++it) {
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
	check_outside_tx();

	std::unique_lock<std::shared_timed_mutex> lock(iteration_mtx);

	auto mut = *mutable_map.load(std::memory_order_acquire);
	auto imm = *immutable_map.load(std::memory_order_acquire);

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
	// std::shared_lock<std::shared_timed_mutex> sh_lock(compaction_mtx);

	auto mut = *mutable_map.load(std::memory_order_acquire);
	auto imm = *immutable_map.load(std::memory_order_acquire);

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

	container_type::const_accessor result;
	auto found = container->find(result, std::string(key.data(), key.size()));
	if (!found) {
		return status::NOT_FOUND;
	}

	callback(result->second.c_str(), result->second.size(), arg);

	return status::OK;
}

std::thread new_map::start_bg_compaction()
{
	// XXX: create thread pool
	return std::thread([&] {
		/* This lock synchronizes with get_all and get/put methods which can
		 * rehash on lookup */
		std::unique_lock<std::shared_timed_mutex> lock(iteration_mtx);

		shared_ptr_type imm, mut, *imm_ptr;

			do {
		imm_ptr = immutable_map.load(std::memory_order_acquire);
		imm = *imm_ptr;
		mut = *mutable_map.load(std::memory_order_acquire);
		} while (imm->mutable_count.load(std::memory_order_relaxed) > 0);

		assert(imm != nullptr);

		size_t elements = 0;
		for (const auto &e : *imm) {
			if (e.second == dram_map_type::tombstone) {
				container->erase(e.first);
			} else {
				container->emplace(e.first, e.second);
			}
			elements++;
		}

		if (elements != imm->size()) {
			std::cout << elements << " " << imm->size() << std::endl;
		}

		assert(elements == imm->size());

		// XXX debug
#ifndef NDEBUG
		for (const auto &e : *imm) {
			if (e.second == dram_map_type::tombstone) {
				assert(container->count(e.first) == 0);
			} else
				assert(container->count(e.first) == 1);
		}
#endif

		{
			// std::unique_lock<std::shared_timed_mutex> lll(compaction_mtx);
			immutable_map.store(new shared_ptr_type(nullptr));
			delete imm_ptr;
		}

		compaction_cv.notify_all(); // -> one?
	});
}

bool new_map::dram_has_space(shared_ptr_type &map)
{
	return map->size() < dram_capacity;
}

status new_map::put(string_view key, string_view value)
{
	LOG("put key=" << std::string(key.data(), key.size())
		       << ", value.size=" << std::to_string(value.size()));

	do {
		auto mut = *mutable_map.load(std::memory_order_acquire);
		auto imm = *immutable_map.load(std::memory_order_acquire);

		mut->mutable_count.fetch_add(std::memory_order_relaxed); // XXX: order?

		if (dram_has_space(mut)) {
			mut->put(key, value);

			mut->mutable_count.fetch_sub(std::memory_order_release);
			return status::OK;
		} else {
			mut->mutable_count.fetch_sub(std::memory_order_release);
			bg_cv.notify_one();

			// Do it in a single background thread (maybe notify this bg
			// thread using notify_one()?) to avoid blocking several threads
			// here
			std::unique_lock<std::mutex> compaction_lock(compaction_mtx);

			compaction_cv.wait(compaction_lock, [&] {
				return dram_has_space(
					*mutable_map.load(std::memory_order_acquire));
			});
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

			insert_log = &pmem_ptr->insert_log;
			remove_log = &pmem_ptr->remove_log;

			// container->resize(SHARDS_NUM);
		});
	}

	// mtxs = std::vector<mutex_type>(SHARDS_NUM);

	auto dram_capacity = std::getenv("DRAM_CAPACITY");
	if (dram_capacity)
		this->dram_capacity = std::stoi(dram_capacity);

	mutable_map =
		new shared_ptr_type(std::make_shared<dram_map_type>(this->dram_capacity * 2));
	immutable_map = new shared_ptr_type(nullptr);

	container = new container_type();

	std::thread([&] {
		while (true) {
			std::unique_lock<std::mutex> lock(bg_mtx);
			bg_cv.wait(lock, [&] {
				return !dram_has_space(*mutable_map.load(
					       std::memory_order_acquire)) ||
					is_shutting_down.load();
			});

			auto mut_ptr = mutable_map.load(std::memory_order_acquire);
			auto mut = *mut_ptr;
			auto imm = *immutable_map.load(std::memory_order_acquire);

			assert(imm == nullptr);

			immutable_map.store(mut_ptr);
			mutable_map.store(new shared_ptr_type(
				std::make_shared<dram_map_type>(this->dram_capacity *
								2)));

			assert(*immutable_map.load(std::memory_order_acquire) != nullptr);

			auto imm_ptr = immutable_map.load(std::memory_order_acquire);

			//do {
				imm_ptr = immutable_map.load(std::memory_order_acquire);
			//} while ((*imm_ptr)->mutable_count.load(std::memory_order_acquire) > 0);

			std::unique_lock<std::shared_timed_mutex> it_lock(iteration_mtx);

			/// XXX: not necessary
			imm = *imm_ptr;
			mut = *mutable_map.load(std::memory_order_acquire);

			assert(imm != nullptr);

			size_t elements = 0;
			for (const auto &e : *imm) {
				if (e.second == dram_map_type::tombstone) {
					container->erase(e.first);
				} else {
					container->emplace(e.first, e.second);
				}
				elements++;
			}

			if (elements != imm->size()) {
				std::cout << elements << " " << imm->size() << std::endl;
			}

			assert(elements == imm->size());

			// XXX debug
#ifndef NDEBUG
			for (const auto &e : *imm) {
				if (e.second == dram_map_type::tombstone) {
					assert(container->count(e.first) == 0);
				} else
					assert(container->count(e.first) == 1);
			}
#endif

			{
				// std::unique_lock<std::shared_timed_mutex>
				// lll(compaction_mtx);
				immutable_map.store(
					new shared_ptr_type(nullptr));
				delete imm_ptr;
			}

			compaction_cv.notify_all(); // -> one?

			if (is_shutting_down.load())
				return;
		}
	}).detach();

	// std::thread([&] {
	// 	while (true) {
	// 		std::this_thread::yield();

	// 		do {
	// 			auto tf = to_free_size.load(std::memory_order_acquire);

	// 			for (int i = 0; i < tf; i++) {
	// 				delete to_free[i];
	// 				to_free[i] = nullptr;
	// 			}

	// 			std::atomic<int> expected = tf;
	// 		} while (!to_free_size.compare_exchage_weak(tf, 0, std::memory_order_release, std::memory_order_relaxed));
	// 	}
	// });
}

} // namespace kv
} // namespace pmem
