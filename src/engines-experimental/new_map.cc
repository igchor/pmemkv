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
	cv.notify_one();

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
			if (worker_queue[i].unsafe_size() != 0)
				spin = true;
	}

	if (!spin)
		break;
}

	static thread_local hazard_pointer<dram_map_type> hp(this->hazards);
	auto index = hp.acquire(this->index);

	auto dram_pred = [](string_view, string_view v) {
		return v != dram_map_type::tombstone;
	};

	auto pmem_pred = [&](string_view k, string_view v) {
		return (!index->exists(k));
	};

	auto s = iterate_dram(index->begin(), index->end(), dram_pred, callback, arg);
	if (s != status::OK) {
		hp.release();
		return s;
	}

	for (int i = 0; i < this->worker_threads; i++){
		auto ret = iterate_pmem(pmem_ptr->map[i]->begin(), pmem_ptr->map[i]->end(), pmem_pred, callback, arg);
		if (s != status::OK) {
			hp.release();
			return s;
		}
	}
	
	hp.release();
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
	static thread_local hazard_pointer hp(this->hazards);
	auto index = hp.acquire(this->index);

	{
		dram_map_type::const_accessor_type acc;
		auto s = index->get(key, acc);
		if (s == dram_map_type::element_status::alive) {
			callback(acc->second.data(), acc->second.size(), arg);

			hp.release();
			return status::OK;
		} else if (s == dram_map_type::element_status::removed) {
			hp.release();
			return status::NOT_FOUND;
		}
	}

	//container_type::const_accessor result;
	auto &c = pmem_ptr->map[fast_hash(key.size(), key.data()) & (this->worker_threads - 1)];

	auto found = c->find(key);
	if (found == c->end()) {
		hp.release();
		return status::NOT_FOUND;
	}

	callback(found->value().data(), found->value().size(), arg);
	hp.release();

	return status::OK;
}

status new_map::put(string_view key, string_view value)
{
	LOG("put key=" << std::string(key.data(), key.size())
		       << ", value.size=" << std::to_string(value.size()));
	check_outside_tx();

	static thread_local hazard_pointer hp(this->hazards);
	static thread_local pmem_log log(pmpool, this->log_size);

	while (true) {
		auto index = hp.acquire(this->index);
		auto log_pos = log.ptr + log.size;

		if (index->size() >= this->dram_capacity) {
			cv.notify_one();

			log.size = 0;
			hp.release();

			std::unique_lock<std::mutex> lock(client_mtx);
			client_cv.wait(lock, [&]{
				// XXX - fix this, use hazard pointer?
				return this->index.load(std::memory_order_relaxed)->size() < this->dram_capacity;
			});

			continue;
			//container->insert_or_assign(key, value);
		} else {
			auto len = key.size() + value.size() + 16;
			char* data = new char[len];
			
			*((uint64_t*)data) = key.size();
			*((uint64_t*)(data + 8)) = value.size();
			memcpy(data + 16, key.data(), key.size());
			memcpy(data + 16 + key.size(), value.data(), value.size());

			pmemobj_memcpy(pmpool.handle(), log_pos, data, len, PMEMOBJ_F_MEM_NONTEMPORAL);
			log.size += len;
			log.size = ALIGN_UP(log.size, 64ULL);

			index->put(key, string_view(log_pos + 16 + key.size(), value.size()));

			worker_queue[fast_hash(key.size(), key.data()) & (this->worker_threads - 1)].emplace(data);
			hp.release();
			break;
		}
	}
	return status::OK;
}

status new_map::remove(string_view key)
{
	LOG("remove key=" << std::string(key.data(), key.size()));
	check_outside_tx();

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
				if (is_shutting_down.load()) {
					bg_cnt--;
					return;
				}

				char* val;
				if (!worker_queue[i].try_pop(val))
					continue;

				auto k_size = *((uint64_t*)val);
				auto v_size = *((uint64_t*) (val + 8));
				string_view k(val + 16, k_size);
				string_view v(val + 16 + k_size, v_size);

				if (v == dram_map_type::tombstone)
					pmem_ptr->map[i]->erase(k);
				else
					pmem_ptr->map[i]->insert_or_assign(k, v);

				delete val;
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

			while(true) {
				bool spin = false;
				for (int i = 0; i < this->worker_threads; i++) {
					if (worker_queue[i].unsafe_size() != 0)
						spin = true;
				}

				if (!spin)
					break;
			}

			auto old_index = index.load(std::memory_order_relaxed);
			index.store(new dram_map_type(this->dram_capacity));

			client_cv.notify_all();

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
