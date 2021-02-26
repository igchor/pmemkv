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

bool new_map::dram_has_space()
{
	return dram_size.load(std::memory_order_acquire) < dram_capacity;
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

static inline uint64_t mix(uint64_t h)
{
	h ^= h >> 23;
	h *= 0x2127599bf4325c37ULL;
	return h ^ h >> 47;
}

/*
 * hash --  calculate the hash of a piece of memory
 */
static uint64_t fast_hash(size_t key_size, const char *key)
{
	/* fast-hash, by Zilong Tan */
	const uint64_t m = 0x880355f21e6d1965ULL;
	const uint64_t *pos = (const uint64_t *)key;
	const uint64_t *end = pos + (key_size / 8);
	uint64_t h = key_size * m;

	while (pos != end)
		h = (h ^ mix(*pos++)) * m;

	if (key_size & 7) {
		uint64_t shift = (key_size & 7) * 8;
		uint64_t mask = (1ULL << shift) - 1;
		uint64_t v = htole64(*pos) & mask;
		h = (h ^ mix(v)) * m;
	}

	return mix(h);
}

status new_map::put(string_view key, string_view value)
{
	LOG("put key=" << std::string(key.data(), key.size())
		       << ", value.size=" << std::to_string(value.size()));

	static thread_local hazard_pointers_handler hp_handler(this);

	do {
		if (!dram_has_space()) {
			bg_cv.notify_one();

			// log.size = 0; //

			if (value != dram_map_type::tombstone)
				pmem->map.insert_or_assign(key, value);
			else
				pmem->map.erase(key);

			return status::OK;
		} else {
			auto mut = mutable_map.load(std::memory_order_acquire);
			hp_handler.hp->mut.store(mut, std::memory_order_release);

			if (mutable_map.load(std::memory_order_acquire) != mut)
				continue;

			static thread_local tls_holder holder(pmpool, pmem, this->log_size);
			auto &log = holder.log;

			auto k_size = key.size();
			auto v_size = value.size();

			auto entry_pos = log.ptr.get() + log.size;

			pmemobj_memcpy(pmpool.handle(), entry_pos, (void*) &k_size, 8, PMEMOBJ_F_MEM_NODRAIN);
			pmemobj_memcpy(pmpool.handle(), entry_pos + 8, (void*) &v_size, 8, PMEMOBJ_F_MEM_NODRAIN);
			pmemobj_memcpy(pmpool.handle(), entry_pos + 16, key.data(), key.size(), PMEMOBJ_F_MEM_NODRAIN);
			pmemobj_memcpy(pmpool.handle(), entry_pos + 16 + key.size(), value.data(), value.size(), PMEMOBJ_F_MEM_NODRAIN);
			pmemobj_drain(pmpool.handle());

			log.size += 16 + key.size() + value.size();
			pmemobj_persist(pmpool.handle(), &log.size, 8);

			auto k = string_view(entry_pos + 16, key.size());
			auto v = string_view(entry_pos + 16 + key.size(), value.size());

			mut->put(key, v);
			queue[fast_hash(k_size, key.data()) % (this->bg_threads - 1)].emplace(k, v);

			dram_size.fetch_add(1, std::memory_order_relaxed);

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

	auto bg_threads = std::getenv("BG_THREADS");
	if (bg_threads)
		this->bg_threads = std::stoi(bg_threads);

	mutable_map = new dram_map_type(this->dram_capacity * 2);
	immutable_map = nullptr;

	container = &pmem->map;

	for (int i = 0; i < this->bg_threads; i++) {
		std::thread([&, i=i] {
			bg_cnt++;

			while (true) {
				if (is_shutting_down.load()) {
					bg_cnt--;
					return;
				}

				val_type val;
				if (!queue[i].try_pop(val))
					continue;

				if (val.second == dram_map_type::tombstone)
					container->erase(val.first);
				else
					container->insert_or_assign(val.first, val.second);
			}
		}).detach();
	}

	std::thread([&] {
		bg_cnt++;

		while (true) {
			std::unique_lock<std::mutex> lock(bg_mtx);
			bg_cv.wait(lock, [&]{
				return is_shutting_down.load() || out_of_space || !dram_has_space();
			});

			if (is_shutting_down.load()) {
				bg_cnt--;
				return;
			}

		auto mut = mutable_map.load(std::memory_order_acquire);

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

			mutable_map.store(new dram_map_type(this->dram_capacity * 2));

					// before changing mutable map because get() would not find some elements
		for (int i = 0; i < this->bg_threads; i++) {
			while (!queue[i].empty()) {
				val_type val;
				if (queue[i].try_pop(val)) {
					if (val.second == dram_map_type::tombstone)
						container->erase(val.first);
					else
						container->insert_or_assign(val.first, val.second);
				}
			}
		}

			delete mut; // do something with client logs!!!

			out_of_space = false;
			dram_size = 0;
		}
	}).detach();
}

} // namespace kv
} // namespace pmem
