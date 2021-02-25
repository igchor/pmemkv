// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2020, Intel Corporation */

#include "new_map.h"
#include "../out.h"

#include <libpmemobj/action_base.h>

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

	while(true) {
		if (bg_cnt.load() == 0)
			break;
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

	static thread_local hazard_pointers_handler hp_handler(this);

restart:
	std::unique_lock<std::shared_timed_mutex> lock(iteration_mtx);

	auto mut = mutable_map.load(std::memory_order_acquire);
	hp_handler.hp->mut.store(mut, std::memory_order_release);
	if (mutable_map.load(std::memory_order_acquire) != mut)
		goto restart;

	auto mut_pred = [](string_view, string_view v) {
		return v != dram_map_type::tombstone;
	};

	auto pmem_pred = [&](string_view k, string_view v) {
		return (!mut->exists(k));
	};

	auto s = iterate(mut->begin(), mut->end(), mut_pred, callback, arg);
	if (s != status::OK) {
					hp_handler.hp->mut.store(nullptr, std::memory_order_release);
			hp_handler.hp->imm.store(nullptr, std::memory_order_release);
		return s;
	}

	auto ret = iterate(container->begin(), container->end(), pmem_pred, callback, arg);
				hp_handler.hp->mut.store(nullptr, std::memory_order_release);
			hp_handler.hp->imm.store(nullptr, std::memory_order_release);
	return ret;
}

status new_map::exists(string_view key)
{
	LOG("exists for key=" << std::string(key.data(), key.size()));

	return get(
		key, [](const char *, size_t, void *) {}, nullptr);
}

status new_map::get(string_view key, get_v_callback *callback, void *arg)
{
	static thread_local hazard_pointers_handler hp_handler(this);

	LOG("get key=" << std::string(key.data(), key.size()));

restart:
	std::shared_lock<std::shared_timed_mutex> it_lock(iteration_mtx);
	// std::shared_lock<std::shared_timed_mutex> sh_lock(compaction_mtx);

	auto mut = mutable_map.load(std::memory_order_acquire);
	hp_handler.hp->mut.store(mut, std::memory_order_release);
	if (mutable_map.load(std::memory_order_acquire) != mut)
		goto restart;

	{
		dram_map_type::const_accessor_type acc;
		auto s = mut->get(key, acc);
		if (s == dram_map_type::element_status::alive) {
			callback(acc->second.data(), acc->second.size(), arg);

			hp_handler.hp->mut.store(nullptr, std::memory_order_release);

			return status::OK;
		} else if (s == dram_map_type::element_status::removed) {
			hp_handler.hp->mut.store(nullptr, std::memory_order_release);

			return status::NOT_FOUND;
		}
	}

	container_type::const_accessor result;
	auto found = container->find(result, key);
	if (!found) {
		hp_handler.hp->mut.store(nullptr, std::memory_order_release);
		return status::NOT_FOUND;
	}

	callback(result->second.c_str(), result->second.size(), arg);
	hp_handler.hp->mut.store(nullptr, std::memory_order_release);

	return status::OK;
}

std::thread new_map::start_bg_compaction()
{
	// XXX: create thread pool
	// 	return std::thread([&] {
	// 		/* This lock synchronizes with get_all and get/put methods which
	// can
	// 		 * rehash on lookup */
	// 		std::unique_lock<std::shared_timed_mutex> lock(iteration_mtx);

	// 		shared_ptr_type imm, mut, *imm_ptr;

	// 			do {
	// 		imm_ptr = immutable_map.load(std::memory_order_acquire);
	// 		imm = *imm_ptr;
	// 		mut = *mutable_map.load(std::memory_order_acquire);
	// 		} while (imm->mutable_count.load(std::memory_order_relaxed) > 0);

	// 		assert(imm != nullptr);

	// 		size_t elements = 0;
	// 		for (const auto &e : *imm) {
	// 			if (e.second == dram_map_type::tombstone) {
	// 				container->erase(e.first);
	// 			} else {
	// 				container->emplace(e.first, e.second);
	// 			}
	// 			elements++;
	// 		}

	// 		if (elements != imm->size()) {
	// 			std::cout << elements << " " << imm->size() << std::endl;
	// 		}

	// 		assert(elements == imm->size());

	// 		// XXX debug
	// #ifndef NDEBUG
	// 		for (const auto &e : *imm) {
	// 			if (e.second == dram_map_type::tombstone) {
	// 				assert(container->count(e.first) == 0);
	// 			} else
	// 				assert(container->count(e.first) == 1);
	// 		}
	// #endif

	// 		{
	// 			// std::unique_lock<std::shared_timed_mutex>
	// lll(compaction_mtx); 			immutable_map.store(new
	// shared_ptr_type(nullptr)); 			delete imm_ptr;
	// 		}

	// 		compaction_cv.notify_all(); // -> one?
	// 	});
}

bool new_map::dram_has_space(dram_map_type &map)
{
	return map.size() < dram_capacity;
}

struct pmem_log {
	pmem_log(obj::pool_base &pop, size_t size) {
		pobj_action act;
		buffer = (char*) pmemobj_direct(pmemobj_reserve(pop.handle(), &act, size, 0));
		assert(buffer);
		size = 0;
	}

	char* buffer;
	size_t size;
};

status new_map::put(string_view key, string_view value)
{
	LOG("put key=" << std::string(key.data(), key.size())
		       << ", value.size=" << std::to_string(value.size()));

	static auto max_size = this->log_size;

	static thread_local hazard_pointers_handler hp_handler(this);
	static thread_local pmem_log plog(pmpool, max_size);

	do {
		auto mut = mutable_map.load(std::memory_order_acquire);

		hp_handler.hp->mut.store(mut, std::memory_order_release);

		if (mutable_map.load(std::memory_order_acquire) != mut)
			continue;

		if (!dram_has_space(*mut)) {
			hp_handler.hp->mut.store(nullptr, std::memory_order_release);

			bg_cv.notify_one();

			plog.size = 0; // XXX

			if (value != dram_map_type::tombstone)
				pmem->map.insert_or_assign(key, value);
			else
				pmem->map.erase(key);

			return status::OK;
		} else {
			pmemobj_memcpy_persist(pmpool.handle(), plog.buffer + plog.size, key.data(), key.size());
			pmemobj_memcpy_persist(pmpool.handle(), plog.buffer + plog.size + key.size(), value.data(), value.size());

			auto k = string_view(plog.buffer + plog.size, key.size());
			auto v = string_view(plog.buffer + plog.size + key.size(), value.size());

			queue1.emplace(k, v);
			mut->put(key, v);

			plog.size += key.size() + value.size();
			assert(plog.size <= max_size);

			hp_handler.hp->mut.store(nullptr, std::memory_order_release);

			return status::OK;
		}
	} while(true);
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

		pmem = pmem_ptr;
	} else {
		pmem::obj::transaction::run(pmpool, [&] {
			pmem::obj::transaction::snapshot(root_oid);
			*root_oid =
				pmem::obj::make_persistent<internal::new_map::pmem_type>()
					.raw();
			auto pmem_ptr = static_cast<internal::new_map::pmem_type *>(
				pmemobj_direct(*root_oid));

			pmem = pmem_ptr;

			// container->resize(SHARDS_NUM);
		});
	}

	

	// mtxs = std::vector<mutex_type>(SHARDS_NUM);

	auto dram_capacity = std::getenv("DRAM_CAPACITY");
	if (dram_capacity)
		this->dram_capacity = std::stoi(dram_capacity);

	auto log_size = std::getenv("LOG_SIZE");
	if (log_size)
		this->log_size = std::stoi(log_size);

	auto pool_size = std::getenv("POOL_SIZE");
	if (pool_size)
		this->pool_size = std::stoi(pool_size);
	
	auto pool_alloc = new internal::new_map::pool_allocator(this->pool_size);
	pool_alloc_buff = new internal::new_map::pool_allocator(this->pool_size);

	mutable_map = new dram_map_type(this->dram_capacity * 2, pool_alloc);
	immutable_map = nullptr;

	container = &pmem->map;

	std::thread([&] {
		bg_cnt++;

		while (true) {
			if (is_shutting_down.load()) {
				bg_cnt--;
				return;
			}

			val_type val;
			if (!queue1.try_pop(val))
				continue;

			if (val.second == dram_map_type::tombstone)
				container->erase(val.first);
			else
				container->insert_or_assign(val.first, val.second);
		}
	}).detach();

	std::thread([&] {
		bg_cnt++;

		while (true) {
			std::unique_lock<std::mutex> lock(bg_mtx);
			bg_cv.wait(lock, [&]{
				return is_shutting_down.load() || out_of_space || !dram_has_space(*mutable_map.load());
			});

			if (is_shutting_down.load()) {
				bg_cnt--;
				return;
			}


			// before changing mutable map because get() would not find some elements
			while (!queue1.empty()) {
				val_type val;
				if (queue1.try_pop(val)) {
					if (val.second == dram_map_type::tombstone)
						container->erase(val.first);
					else
						container->insert_or_assign(val.first, val.second);
				}
			}

			auto mut = mutable_map.load(std::memory_order_acquire);
			mutable_map.store(new dram_map_type(this->dram_capacity * 2, pool_alloc_buff));

		{
			bool spin;
			do {
					std::unique_lock<std::mutex> lock(
							hazard_pointers_mtx);
					spin = false;
					for (auto &e : hazard_pointers_list) {
							if (e->mut.load(
										std::memory_order_acquire) ==
								mut)
									spin = true;
					}
			} while (spin);
		}

			pool_alloc_buff = mut->alloc;
			pool_alloc_buff->pool.recycle();

			out_of_space = false;
		}
	}).detach();
}

} // namespace kv
} // namespace pmem
