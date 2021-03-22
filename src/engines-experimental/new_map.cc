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

// 	while(true) {
// 		bool spin = false;
// 		for (int i = 0; i < this->worker_threads; i++) {
// 			if (!worker_queue[i].empty())
// 				spin = true;
// 	}

// 	if (!spin)
// 		break;
// }

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
			callback(acc->second.data(), acc->second.size(), arg);
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

static std::atomic<size_t>& get_cnt() {
	static std::atomic<size_t> t_counter = 0;
	return t_counter;
}

struct t_cnt {
	t_cnt(std::atomic<size_t> &cnt): cnt(cnt) {
		id = cnt.fetch_add(1);
	}	

	~t_cnt() {
		cnt.fetch_sub(1);
	}

	size_t get() { return id; }

	std::atomic<size_t> &cnt;
	size_t id;
};

status new_map::put(string_view key, string_view value)
{
	LOG("put key=" << std::string(key.data(), key.size())
		       << ", value.size=" << std::to_string(value.size()));
	check_outside_tx();

	static thread_local hazard_pointer index_hp(this->hazards, this->index);
	static thread_local t_cnt cnt(get_cnt());

	while (true) {
		auto index = index_hp.acquire();

		if (index->size() >= this->dram_capacity) {
			cv.notify_one();

			// XXX - wait for consumers to finish, keep read/written counters?
			index.reset(nullptr);

			std::unique_lock<std::mutex> lock(client_mtx);
			client_cv.wait(lock, [&]{
				return index_hp.acquire()->size() < this->dram_capacity;
			});

			continue;
		} else {
			// XXX - handle no space in the log

			auto len = key.size() + value.size() + 16;
			len = ALIGN_UP(len, 64ULL);

			auto hash = fast_hash(key.size(), key.data()) & (this->worker_threads - 1);

			bool exists = false;
			auto &workers = ets.local(exists);

			if (!exists) {
				workers = std::vector<ringbuf_worker_t*>(this->worker_threads);
				for (int i = 0; i < this->worker_threads; i++) {
					workers[i] = ringbuf_register(r[i], cnt.get());
				}
			}

retry:
			auto off = ringbuf_acquire(r[hash], workers[hash], len);
			if (off == -1)
				goto retry;
			
			auto log_pos = log[hash] + off;
			char data[256];
			
			// XXX - refactor this, create some structure?
			*((uint64_t*)data) = key.size();
			*((uint64_t*)(data + 8)) = value.size();
			memcpy(data + 16, key.data(), key.size());
			memcpy(data + 16 + key.size(), value.data(), value.size());

			pmemobj_memcpy(pmpool.handle(), log_pos, data, len, PMEMOBJ_F_MEM_NONTEMPORAL);

			// Since data from log can be overwritten, we must keep a copy in DRAM
			index->put(key, value);

			ringbuf_produce(r[hash], workers[hash]);

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

	size_t ringbuf_obj_size;
	ringbuf_get_sizes(128, &ringbuf_obj_size, NULL);

	pobj_action act[128];

	for (int i = 0; i < this->worker_threads; i++) {
		r[i] = (ringbuf_t*) malloc(ringbuf_obj_size);
		auto ret = ringbuf_setup(r[i], 128, this->log_size);
		if (ret != 0)
			throw std::runtime_error("ringbuf setup");

		log[i] = (char*) pmemobj_direct(pmemobj_reserve(pmpool.handle(), &act[i], this->log_size * 256, 0));
		log[i] = (char*) ALIGN_UP((uint64_t)log[i] , 64ULL);
	}

	if (pmemobj_publish(pmpool.handle(), &act[0], this->worker_threads))
		throw std::runtime_error("pmemobj_publish");

	for (int i = 0; i < this->worker_threads; i++) {
		std::thread([&, i=i]{
			bg_cnt++;
			while (true) {
				// std::unique_lock<std::mutex> lock(bg_cv_mtx[i]);

				// if (is_shutting_down.load()) {
				// 	bg_cnt--;
				// 	return;
				// }

				// bg_cv[i].wait(lock);

				if (is_shutting_down.load()) {
					bg_cnt--;
					return;
				}


				while(true) {
					size_t off;
					auto len = ringbuf_consume(r[i], &off);
					if (len == 0)
						break;

					auto val = log[i] + off;

					// XXX - wrap this logic in some function or structure
					auto k_size = *((uint64_t*)val);
					auto v_size = *((uint64_t*) (val + 8));
					string_view k(val + 16, k_size);
					string_view v(val + 16 + k_size, v_size);

					if (v == dram_map_type::tombstone)
						pmem_ptr->map[i]->erase(k);
					else
						pmem_ptr->map[i]->insert_or_assign(k, v);

					ringbuf_release(r[i], len);
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

			// // wait for all bg threads to finish
			// while(true) {
			// 	bool spin = false;
			// 	for (int i = 0; i < this->worker_threads; i++) {
			// 		if (!worker_queue[i].empty())
			// 			spin = true;
			// 	}

			// 	if (!spin)
			// 		break;
			// }

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
