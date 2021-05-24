// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2020-2021, Intel Corporation */

#include "radix.h"
#include "../out.h"

namespace pmem
{
namespace kv
{
namespace internal
{
namespace radix
{
transaction::transaction(pmem::obj::pool_base &pop, map_type *container)
    : pop(pop), container(container)
{
}

status transaction::put(string_view key, string_view value)
{
	log.insert(key, value);
	return status::OK;
}

status transaction::remove(string_view key)
{
	log.remove(key);
	return status::OK;
}

status transaction::commit()
{
	auto insert_cb = [&](const dram_log::element_type &e) {
		auto result = container->try_emplace(e.first, e.second);

		if (result.second == false)
			result.first.assign_val(e.second);
	};

	auto remove_cb = [&](const dram_log::element_type &e) {
		container->erase(e.first);
	};

	pmem::obj::transaction::run(pop, [&] { log.foreach (insert_cb, remove_cb); });

	log.clear();

	return status::OK;
}

void transaction::abort()
{
	log.clear();
}
} /* namespace radix */
} /* namespace internal */

radix::radix(std::unique_ptr<internal::config> cfg)
    : pmemobj_engine_base(cfg, "pmemkv_radix"), config(std::move(cfg))
{
	Recover();
	LOG("Started ok");
}

radix::~radix()
{
	LOG("Stopped ok");
}

std::string radix::name()
{
	return "radix";
}

status radix::count_all(std::size_t &cnt)
{
	LOG("count_all");
	check_outside_tx();
	cnt = container->size();

	return status::OK;
}

status radix::count_above(string_view key, std::size_t &cnt)
{
	LOG("count_above for key=" << std::string(key.data(), key.size()));
	check_outside_tx();

	auto first = container->upper_bound(key);
	auto last = container->end();

	cnt = internal::distance(first, last);

	return status::OK;
}

status radix::count_equal_above(string_view key, std::size_t &cnt)
{
	LOG("count_equal_above for key=" << std::string(key.data(), key.size()));
	check_outside_tx();

	auto first = container->lower_bound(key);
	auto last = container->end();

	cnt = internal::distance(first, last);

	return status::OK;
}

status radix::count_equal_below(string_view key, std::size_t &cnt)
{
	LOG("count_equal_below for key=" << std::string(key.data(), key.size()));
	check_outside_tx();

	auto first = container->begin();
	auto last = container->upper_bound(key);

	cnt = internal::distance(first, last);

	return status::OK;
}

status radix::count_below(string_view key, std::size_t &cnt)
{
	LOG("count_below for key=" << std::string(key.data(), key.size()));
	check_outside_tx();

	auto first = container->begin();
	auto last = container->lower_bound(key);

	cnt = internal::distance(first, last);

	return status::OK;
}

status radix::count_between(string_view key1, string_view key2, std::size_t &cnt)
{
	LOG("count_between for key1=" << key1.data() << ", key2=" << key2.data());
	check_outside_tx();

	if (key1.compare(key2) < 0) {
		auto first = container->upper_bound(key1);
		auto last = container->lower_bound(key2);

		cnt = internal::distance(first, last);
	} else {
		cnt = 0;
	}

	return status::OK;
}

status radix::iterate(typename container_type::const_iterator first,
		      typename container_type::const_iterator last,
		      get_kv_callback *callback, void *arg)
{
	for (auto it = first; it != last; ++it) {
		string_view key = it->key();
		string_view value = it->value();

		auto ret =
			callback(key.data(), key.size(), value.data(), value.size(), arg);

		if (ret != 0)
			return status::STOPPED_BY_CB;
	}

	return status::OK;
}

status radix::get_all(get_kv_callback *callback, void *arg)
{
	LOG("get_all");
	check_outside_tx();

	auto first = container->begin();
	auto last = container->end();

	return iterate(first, last, callback, arg);
}

status radix::get_above(string_view key, get_kv_callback *callback, void *arg)
{
	LOG("get_above for key=" << std::string(key.data(), key.size()));
	check_outside_tx();

	auto first = container->upper_bound(key);
	auto last = container->end();

	return iterate(first, last, callback, arg);
}

status radix::get_equal_above(string_view key, get_kv_callback *callback, void *arg)
{
	LOG("get_equal_above for key=" << std::string(key.data(), key.size()));
	check_outside_tx();

	auto first = container->lower_bound(key);
	auto last = container->end();

	return iterate(first, last, callback, arg);
}

status radix::get_equal_below(string_view key, get_kv_callback *callback, void *arg)
{
	LOG("get_equal_below for key=" << std::string(key.data(), key.size()));
	check_outside_tx();

	auto first = container->begin();
	auto last = container->upper_bound(key);

	return iterate(first, last, callback, arg);
}

status radix::get_below(string_view key, get_kv_callback *callback, void *arg)
{
	LOG("get_below for key=" << std::string(key.data(), key.size()));
	check_outside_tx();

	auto first = container->begin();
	auto last = container->lower_bound(key);

	return iterate(first, last, callback, arg);
}

status radix::get_between(string_view key1, string_view key2, get_kv_callback *callback,
			  void *arg)
{
	LOG("get_between for key1=" << key1.data() << ", key2=" << key2.data());
	check_outside_tx();

	if (key1.compare(key2) < 0) {
		auto first = container->upper_bound(key1);
		auto last = container->lower_bound(key2);
		return iterate(first, last, callback, arg);
	}

	return status::OK;
}

status radix::exists(string_view key)
{
	LOG("exists for key=" << std::string(key.data(), key.size()));
	check_outside_tx();

	return container->find(key) != container->end() ? status::OK : status::NOT_FOUND;
}

status radix::get(string_view key, get_v_callback *callback, void *arg)
{
	LOG("get key=" << std::string(key.data(), key.size()));
	check_outside_tx();

	auto it = container->find(key);
	if (it != container->end()) {
		auto value = string_view(it->value());
		callback(value.data(), value.size(), arg);
		return status::OK;
	}

	LOG("  key not found");
	return status::NOT_FOUND;
}

status radix::put(string_view key, string_view value)
{
	LOG("put key=" << std::string(key.data(), key.size())
		       << ", value.size=" << std::to_string(value.size()));
	check_outside_tx();

	auto result = container->try_emplace(key, value);

	if (result.second == false) {
		pmem::obj::transaction::run(pmpool,
					    [&] { result.first.assign_val(value); });
	}

	return status::OK;
}

status radix::remove(string_view key)
{
	LOG("remove key=" << std::string(key.data(), key.size()));
	check_outside_tx();

	auto it = container->find(key);

	if (it == container->end())
		return status::NOT_FOUND;

	container->erase(it);

	return status::OK;
}

internal::transaction *radix::begin_tx()
{
	return new internal::radix::transaction(pmpool, container);
}

void radix::Recover()
{
	if (!OID_IS_NULL(*root_oid)) {
		auto pmem_ptr = static_cast<internal::radix::pmem_type *>(
			pmemobj_direct(*root_oid));

		container = &pmem_ptr->map;
	} else {
		pmem::obj::transaction::run(pmpool, [&] {
			pmem::obj::transaction::snapshot(root_oid);
			*root_oid =
				pmem::obj::make_persistent<internal::radix::pmem_type>()
					.raw();
			auto pmem_ptr = static_cast<internal::radix::pmem_type *>(
				pmemobj_direct(*root_oid));
			container = &pmem_ptr->map;
		});
	}
}

// HETEROGENOUS_RADIX

heterogenous_radix::heterogenous_radix(std::unique_ptr<internal::config> cfg)
    : pmemobj_engine_base(cfg, "pmemkv_radix"), config(std::move(cfg))
{
	// size_t log_size;
	// if (!cfg->get_uint64("log_size", &log_size))
	// 	throw internal::invalid_argument("XXX");
	// if (!cfg->get_uint64("dram_size", &dram_size))
	// 	throw internal::invalid_argument("XXX");

	size_t log_size = std::stoull(std::getenv("PMEMKV_LOG_SIZE"));
	dram_size = std::stoull(std::getenv("PMEMKV_DRAM_SIZE"));

	internal::radix::pmem_type *pmem_ptr;

	if (!OID_IS_NULL(*root_oid)) {
		pmem_ptr = static_cast<internal::radix::pmem_type *>(
			pmemobj_direct(*root_oid));
	} else {
		pmem::obj::transaction::run(pmpool, [&] {
			pmem::obj::transaction::snapshot(root_oid);
			*root_oid =
				pmem::obj::make_persistent<internal::radix::pmem_type>()
					.raw();
			pmem_ptr = static_cast<internal::radix::pmem_type *>(
				pmemobj_direct(*root_oid));
			pmem_ptr->log = pmem::obj::make_persistent<char[]>(log_size);
		});
	}

	// XXX - do recovery first
	// pmem_ptr->log->resize(log_size);

	container = &pmem_ptr->map;

	container->runtime_initialize_mt();

	stopped.store(false);
	bg_thread = std::thread([&] { bg_work(); });

	pop = pmem::obj::pool_by_vptr(&pmem_ptr->log);
}

heterogenous_radix::~heterogenous_radix()
{
	stopped.store(true);
	bg_thread.join();
}

void heterogenous_radix::cache_put(string_view key, string_view value, bool write)
{
	auto it = map.find(key);

	if (it != map.end()) {
		assert(write);

		lru_list.splice(lru_list.begin(), lru_list, it->second);

		nodes_to_evict.fetch_sub(1, std::memory_order_relaxed);

		lru_list.begin()->second = value;
	} else if (lru_list.size() < dram_size) {
		lru_list.emplace_front(key, value);

		if (!write) {
			lru_list.begin()->second.clear_timestamp(lru_list.begin()->second.timestamp());
			nodes_to_evict.fetch_add(1, std::memory_order_relaxed);
		}

		auto ret = map.try_emplace(lru_list.begin()->first, lru_list.begin());
		assert(ret.second);
	} else {
		if (!write && nodes_to_evict.load() == 0)
			return;

		std::unique_lock<std::mutex> lock(eviction_lock);
		eviction_cv.wait(lock, [&] { return this->nodes_to_evict.load() > 0; });
		for (auto rit = lru_list.rbegin(); rit != lru_list.rend(); rit++) {
			auto t = rit->second.timestamp();
			if (t == 0) {
				auto cnt = map.erase(rit->first);
				assert(cnt == 1 && rit->first != key);

				lru_list.splice(lru_list.begin(), lru_list,
						std::next(rit).base());
				lru_list.begin()->first = key;
				lru_list.begin()->second = value;

				if (!write) {
					lru_list.begin()->second.clear_timestamp(lru_list.begin()->second.timestamp());
				}

				auto ret = map.try_emplace(lru_list.begin()->first,
							   lru_list.begin());
				assert(ret.second);

				if (write)
					nodes_to_evict.fetch_sub(1, std::memory_order_relaxed);

				return;
			}
		}

		assert(false);
	}
}

status heterogenous_radix::put(string_view key, string_view value)
{
	cache_put(key, value, true);

	// XXX - can use optimistic concurrency control to read data from pmem log???

	// XXX - if try_produce == false, we can just allocate new radix node to
	// TLS and the publish pointer to this node
	// NEED TX support for produce():
	// tx {
	// queue.produce([&] (data) { data = make_persistent(); }) }

	// XXX: how to add tx support for consume (multiple elements) if entires
	// must be invalidated - release must be called oncommit only????

	queue.emplace(new queue_entry(lru_list.begin()->second.timestamp(),
				  &*lru_list.begin(), key, value));

	return status::OK;
}

status heterogenous_radix::remove(string_view k)
{
	bool found = false;
	auto it = map.find(k);
	if (it == map.end()) {
		found = container->find(k) != container->end();
	} else {
		found = it->second->second.data() != tombstone();
	}

	auto s = put(k, tombstone());
	if (s != status::OK)
		return s;

	return found ? status::OK : status::NOT_FOUND;
}

status heterogenous_radix::get(string_view key, get_v_callback *callback, void *arg)
{
	auto it = map.find(key);
	if (it != map.end()) {
		if (it->second->second.data() == tombstone())
			return status::NOT_FOUND;

		callback(it->second->second.data().data(),
			 it->second->second.data().size(), arg);

		lru_list.splice(lru_list.begin(), lru_list, it->second);

		return status::OK;
	} else {
		auto it = container->find(key);
		if (it != container->end()) {
			auto value = string_view(it->value());
			callback(value.data(), value.size(), arg);

			cache_put(key, value, false);

			return status::OK;
		} else
			return status::NOT_FOUND;
	}
}

std::string heterogenous_radix::name()
{
	return "radix";
}

status heterogenous_radix::count_all(std::size_t &cnt)
{
	size_t size = 0;
	auto s = get_all(
		[](const char *, size_t, const char *, size_t, void *arg) {
			++*(size_t *)(arg);
			return 0;
		},
		(void *)&size);
	if (s != status::OK)
		return s;

	cnt = size;
	return status::OK;
}

status heterogenous_radix::get_all(get_kv_callback *callback, void *arg)
{
	auto pmem_it = container->begin();
	auto dram_it = map.begin();

	while (pmem_it != container->end() || dram_it != map.end()) {
		string_view key;
		string_view value;

		/* If keys are the same, skip the one in pmem (the dram one is more
		 * recent) */
		if (pmem_it != container->end() && dram_it != map.end() &&
		    dram_it->first == string_view(pmem_it->key())) {
			++pmem_it;
			continue;
		}

		auto process_pmem = dram_it == map.end() ||
			(pmem_it != container->end() &&
			 dram_it->first.compare(pmem_it->key()) > 0);

		if (process_pmem) {
			key = pmem_it->key();
			value = pmem_it->value();
		} else {
			key = dram_it->first;
			value = dram_it->second->second.data();

			if (value == tombstone()) {
				++dram_it;
				continue;
			}
		}

		auto s =
			callback(key.data(), key.size(), value.data(), value.size(), arg);
		if (s != 0)
			return status::STOPPED_BY_CB;

		if (process_pmem)
			++pmem_it;
		else
			++dram_it;
	}

	return status::OK;
}

status heterogenous_radix::exists(string_view key)
{
	return get(
		key, [](const char *, size_t, void *) {}, nullptr);
}

void heterogenous_radix::bg_work()
{
	while (!stopped.load()) {
		queue_entry *e;
		if (queue.try_pop(e)) {
			//if (e->timestamp != e->dram_entry->second.timestamp())
			// nodes_to_evict.fetch_add(1);
			//	continue;

			// XXX - make sure tx does not abort and use
			// defer_free
			if (e->value() == tombstone()) {
				container->erase(e->key());
			} else {
				container->insert_or_assign(e->key(), e->value());
			}

			if (e->dram_entry->second.clear_timestamp(e->timestamp)) {
				nodes_to_evict.fetch_add(1, std::memory_order_release);
				eviction_cv.notify_one();
			}

			delete e;
		}
	}
}
///

internal::iterator_base *radix::new_iterator()
{
	return new radix_iterator<false>{container};
}

internal::iterator_base *radix::new_const_iterator()
{
	return new radix_iterator<true>{container};
}

radix::radix_iterator<true>::radix_iterator(container_type *c)
    : container(c), pop(pmem::obj::pool_by_vptr(c))
{
}

radix::radix_iterator<false>::radix_iterator(container_type *c)
    : radix::radix_iterator<true>(c)
{
}

status radix::radix_iterator<true>::seek(string_view key)
{
	init_seek();

	it_ = container->find(key);
	if (it_ != container->end())
		return status::OK;

	return status::NOT_FOUND;
}

status radix::radix_iterator<true>::seek_lower(string_view key)
{
	init_seek();

	it_ = container->lower_bound(key);
	if (it_ == container->begin()) {
		it_ = container->end();
		return status::NOT_FOUND;
	}

	--it_;

	return status::OK;
}

status radix::radix_iterator<true>::seek_lower_eq(string_view key)
{
	init_seek();

	it_ = container->upper_bound(key);
	if (it_ == container->begin()) {
		it_ = container->end();
		return status::NOT_FOUND;
	}

	--it_;

	return status::OK;
}

status radix::radix_iterator<true>::seek_higher(string_view key)
{
	init_seek();

	it_ = container->upper_bound(key);
	if (it_ == container->end())
		return status::NOT_FOUND;

	return status::OK;
}

status radix::radix_iterator<true>::seek_higher_eq(string_view key)
{
	init_seek();

	it_ = container->lower_bound(key);
	if (it_ == container->end())
		return status::NOT_FOUND;

	return status::OK;
}

status radix::radix_iterator<true>::seek_to_first()
{
	init_seek();

	if (container->empty())
		return status::NOT_FOUND;

	it_ = container->begin();

	return status::OK;
}

status radix::radix_iterator<true>::seek_to_last()
{
	init_seek();

	if (container->empty())
		return status::NOT_FOUND;

	it_ = container->end();
	--it_;

	return status::OK;
}

status radix::radix_iterator<true>::is_next()
{
	auto tmp = it_;
	if (tmp == container->end() || ++tmp == container->end())
		return status::NOT_FOUND;

	return status::OK;
}

status radix::radix_iterator<true>::next()
{
	init_seek();

	if (it_ == container->end() || ++it_ == container->end())
		return status::NOT_FOUND;

	return status::OK;
}

status radix::radix_iterator<true>::prev()
{
	init_seek();

	if (it_ == container->begin())
		return status::NOT_FOUND;

	--it_;

	return status::OK;
}

result<string_view> radix::radix_iterator<true>::key()
{
	assert(it_ != container->end());

	return {it_->key().cdata()};
}

result<pmem::obj::slice<const char *>> radix::radix_iterator<true>::read_range(size_t pos,
									       size_t n)
{
	assert(it_ != container->end());

	if (pos + n > it_->value().size() || pos + n < pos)
		n = it_->value().size() - pos;

	return {{it_->value().cdata() + pos, it_->value().cdata() + pos + n}};
}

result<pmem::obj::slice<char *>> radix::radix_iterator<false>::write_range(size_t pos,
									   size_t n)
{
	assert(it_ != container->end());

	if (pos + n > it_->value().size() || pos + n < pos)
		n = it_->value().size() - pos;

	log.push_back({std::string(it_->value().cdata() + pos, n), pos});
	auto &val = log.back().first;

	return {{&val[0], &val[n]}};
}

status radix::radix_iterator<false>::commit()
{
	pmem::obj::transaction::run(pop, [&] {
		for (auto &p : log) {
			auto dest = it_->value().range(p.second, p.first.size());
			std::copy(p.first.begin(), p.first.end(), dest.begin());
		}
	});
	log.clear();

	return status::OK;
}

void radix::radix_iterator<false>::abort()
{
	log.clear();
}

static factory_registerer
	register_radix(std::unique_ptr<engine_base::factory_base>(new radix_factory));

} // namespace kv
} // namespace pmem
