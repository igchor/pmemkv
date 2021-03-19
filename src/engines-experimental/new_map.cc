// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2020, Intel Corporation */

#include "new_map.h"
#include "../out.h"

namespace pmem
{
namespace kv
{

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

new_map::new_map(std::unique_ptr<internal::config> cfg)
    : pmemobj_engine_base(cfg, "pmemkv_new_map"), config(std::move(cfg))
{
	Recover();
	LOG("Started ok");
}

new_map::~new_map()
{
	is_shutting_down.store(true);
	cv.notify_all();

	for (int i = 0; i < 128; i++)
		bg_cv[i].notify_one();

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

	cnt = 0;
	return get_all(
		[](const char *k, size_t kb, const char *v, size_t vb, void *arg) {
			auto size = static_cast<size_t *>(arg);
			(*size)++;

			return PMEMKV_STATUS_OK;
		},
		&cnt);

	return status::OK;
}

template <typename Iterator, typename Pred>
static status iterate_dram(Iterator first, Iterator last, Pred &&pred,
		      get_kv_callback *callback, void *arg)
{
	for (auto it = first; it != last; ++it) {
		string_view key = it->first;

		auto log_pos = it->second.load(std::memory_order_acquire);

		auto k_size = *((uint64_t*)log_pos);
		auto v_size = *((uint64_t*) (log_pos + 8));

		string_view value = string_view(log_pos + 24 + k_size, v_size);

		if (!pred(key, value))
			continue;

		auto ret =
			callback(key.data(), key.size(), value.data(), value.size(), arg);

		if (ret != 0)
			return status::STOPPED_BY_CB;
	}

	return status::OK;
}

template <typename Iterator, typename Pred>
static status iterate_pmem(Iterator first, Iterator last, Pred &&pred,
		      get_kv_callback *callback, void *arg)
{
	for (auto it = first; it != last; ++it) {
		string_view key = it->key();
		string_view value = it->value();

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

	while(true) {
		bool spin = false;
		for (int i = 0; i < this->worker_threads; i++) {
			if (!worker_queue[i].empty())
				spin = true;
	}

	if (!spin)
		break;
}

	static thread_local hazard_pointer hp(this->hazards, this->index);
	auto index = hp.acquire();

	auto dram_pred = [](string_view, string_view v) {
		return v != dram_map_type::tombstone;
	};

	auto pmem_pred = [&](string_view k, string_view v) {
		return (!index->exists(k));
	};

	auto s = iterate_dram(index->begin(), index->end(), dram_pred, callback, arg);
	if (s != status::OK) {
		return s;
	}

	for (int i = 0; i < this->worker_threads; i++){
		auto ret = iterate_pmem(pmem_ptr->map[i]->begin(), pmem_ptr->map[i]->end(), pmem_pred, callback, arg);
		if (s != status::OK) {
			return s;
		}
	}
	
	return status::OK;
}

status new_map::exists(string_view key)
{
	LOG("exists for key=" << std::string(key.data(), key.size()));
	check_outside_tx();

	return get(
		key, [](const char *, size_t, void *) {}, nullptr);
}

status new_map::get(string_view key, get_v_callback *callback, void *arg)
{
	// XXX - enumerable_thread_specific?
	static thread_local hazard_pointer hp(this->hazards, this->index);
	auto index = hp.acquire();

	{
		dram_map_type::const_accessor_type acc;
		auto s = index->get(key, acc);
		if (s == dram_map_type::element_status::alive) {
			auto log_pos = acc->second.load(std::memory_order_acquire);

			auto k_size = *((uint64_t*)log_pos);
			auto v_size = *((uint64_t*) (log_pos + 8));
			//auto timestamp = *((std::chrono::time_point<std::chrono::steady_clock>*) (log_pos + 16));
			//string_view k(log_pos + 24, k_size);
			//string_view v(log_pos + 24 + k_size, v_size);

			callback(log_pos + 24 + k_size, v_size, arg);
			return status::OK;
		} else if (s == dram_map_type::element_status::removed) {
			return status::NOT_FOUND;
		}
	}

	auto &c = pmem_ptr->map[fast_hash(key.size(), key.data()) & (this->worker_threads - 1)];

	auto found = c->find(key);
	if (found == c->end()) {
		return status::NOT_FOUND;
	}

	callback(found->value().data(), found->value().size(), arg);

	return status::OK;
}

status new_map::put(string_view key, string_view value)
{
	LOG("put key=" << std::string(key.data(), key.size())
		       << ", value.size=" << std::to_string(value.size()));
	check_outside_tx();

	// XXX - we probably can't use TLS since it would be shared for all instances of this engine.
	static thread_local hazard_pointer index_hp(this->hazards, this->index);
	static thread_local pmem_log log(pmpool, this->log_size);

	while (true) {
		auto index = index_hp.acquire();
		auto log_pos = log.ptr + log.size;

		auto len = key.size() + value.size() + 24;
		len = ALIGN_UP(len, 64ULL);

		if (index->size() >= this->dram_capacity) {
			cv.notify_one();

			log.size = 0;
			index.reset(nullptr);

			std::unique_lock<std::mutex> lock(client_mtx);
			client_cv.wait(lock, [&]{
				return index_hp.acquire()->size() < this->dram_capacity;
			});

			continue;
		} else if (log.size + len >= this->log_size) {
			while (log.n_reads.load(std::memory_order_acquire) != log.n_writes) {
				// XXX - shed_yield?
			}

			log.size = 0;
			log.n_reads.store(0);
			log.n_writes = 0;

			continue;
		} else {
			// XXX - do not allocate?, just copy to pmem
			char data[1024];

			auto timestamp = std::chrono::steady_clock::now();
			
			// XXX - refactor this, create some structure?
			*((uint64_t*)data) = key.size();
			*((uint64_t*)(data + 8)) = value.size();
			*((std::chrono::time_point<std::chrono::steady_clock>*)(data + 16)) = timestamp;
			memcpy(data + 24, key.data(), key.size());
			memcpy(data + 24 + key.size(), value.data(), value.size());

			pmemobj_memcpy(pmpool.handle(), log_pos, data, len, PMEMOBJ_F_MEM_NONTEMPORAL);
			log.size += len;

			log.n_writes++;

			auto ret = index->put(key, log_pos);
			worker_queue[fast_hash(key.size(), key.data()) & (this->worker_threads - 1)].emplace(message{log_pos, &log.n_reads, ret, timestamp});
			bg_cv[fast_hash(key.size(), key.data()) & (this->worker_threads - 1)].notify_one();

			return status::OK;
		}
	}
}

status new_map::remove(string_view key)
{
	LOG("remove key=" << std::string(key.data(), key.size()));
	check_outside_tx();

	/// XXX - no way to know if element existed or not
	return put(key, dram_map_type::tombstone);
}

void new_map::Recover()
{
	if (!OID_IS_NULL(*root_oid)) {
		pmem_ptr = static_cast<internal::new_map::pmem_type *>(
			pmemobj_direct(*root_oid));
	} else {
		pmem::obj::transaction::run(pmpool, [&] {
			pmem::obj::transaction::snapshot(root_oid);
			*root_oid =
				pmem::obj::make_persistent<internal::new_map::pmem_type>()
					.raw();
			pmem_ptr = static_cast<internal::new_map::pmem_type *>(
				pmemobj_direct(*root_oid));

			for (int i = 0; i < 128; i++) {
				pmem_ptr->map[i] = obj::make_persistent<internal::new_map::map_type>();
			}
		});
	}

	auto dram_capacity = std::getenv("DRAM_CAPACITY");
	if (dram_capacity)
		this->dram_capacity = std::stoi(dram_capacity);

	auto log_size = std::getenv("LOG_SIZE");
	if (log_size)
		this->log_size = std::stoi(log_size);

	auto worker_threads = std::getenv("BG_THREADS");
	if (worker_threads)
		this->worker_threads = std::stoi(worker_threads);

	index = new dram_map_type(this->dram_capacity * 2);

	for (int i = 0; i < this->worker_threads; i++) {
		std::thread([&, i=i]{
			bg_cnt++;
			while (true) {
				std::unique_lock<std::mutex> lock(bg_cv_mtx[i]);
				bg_cv[i].wait(lock, [&]{
					return !worker_queue[i].empty() || is_shutting_down.load();
				});

				if (is_shutting_down.load()) {
					bg_cnt--;
					return;
				}


				while(true) {
					// XXX - once here, we can insert all items from queue in one tx? but then there is a problem with read uncommitted
					message msg;
					if (!worker_queue[i].try_pop(msg))
						break;

					// XXX - wrap this logic in some function or structure
					auto k_size = *((uint64_t*)msg.log_pos);
					auto v_size = *((uint64_t*) (msg.log_pos + 8));
					auto timestamp = *((std::chrono::time_point<std::chrono::steady_clock>*) (msg.log_pos + 16));
					string_view k(msg.log_pos + 24, k_size);
					string_view v(msg.log_pos + 24 + k_size, v_size);

					// XXXX
					// if (timestamp_from_msg.value_ptr > msg.timestamp)
					// do nothing -> we will process this element later

					// XXX - only modify pme structure if timestamp from dram is later than the one on pmem. The order of inserting
					// to DRAM index might be different than order of inserting to concurrent queue

					// XXX - preallocate buffers so that erase/insert_or_asssing cannot fail
					if (v == dram_map_type::tombstone)
						pmem_ptr->map[i]->erase(k);
					else {
						auto ret = pmem_ptr->map[i]->insert_or_assign(k, v);
						auto ptr = msg.value_ptr->load(std::memory_order_relaxed);

						auto k_size = *((uint64_t*)ptr);
						auto v_size = *((uint64_t*) (ptr + 8));
						auto timestamp = *((std::chrono::time_point<std::chrono::steady_clock>*) (ptr + 16));
						string_view v(ptr + 24 + k_size, v_size);

						// XXX - this modification should be protected by hazard pointers probably (so that bg thread
						// won't deallocate dram index)
					// 	while(true) {
					// 		// XXX - it's not safe to read size
					// 		if (timestamp == msg.timestamp && v != dram_map_type::tombstone) {
					// 			msg.value_ptr->compare_exchange_strong(ptr, ret.first->value().data(), std::memory_order_release, std::memory_order_relaxed);
					// 		}

					// 		break;
					// 	}
					}

					msg.n_reads->fetch_add(1, std::memory_order_release);
				}
			}
		}).detach();
	}

	std::thread([&]{
		bg_cnt++;
		while (true) {
			std::unique_lock<std::mutex> lock(bg_mtx);
			cv.wait(lock, [&]{
				return is_shutting_down.load() || index.load(std::memory_order_relaxed)->size() >= this->dram_capacity;
			});

			if (is_shutting_down.load()) {
				bg_cnt--;
				return;
			}

			// wait for all bg threads to finish
			while(true) {
				bool spin = false;
				for (int i = 0; i < this->worker_threads; i++) {
					if (!worker_queue[i].empty())
						spin = true;
				}

				if (!spin)
					break;
			}

			// now, it's safe to replace dram index (all elements are already on pmem)
			auto old_index = index.load(std::memory_order_relaxed);
			index.store(new dram_map_type(this->dram_capacity));

			client_cv.notify_all();

			// some threads might still read from old index, wait for them to notice there is a new one
			while(true) {
				size_t in_use = 0;
				hazards.foreach([&](std::atomic<dram_map_type*> &ptr){
					if (ptr.load(std::memory_order_relaxed) == old_index)
						in_use++;
				});

				if (in_use == 0)
					break;
			};

			delete old_index;
		}
	}).detach();
}

} // namespace kv
} // namespace pmem
