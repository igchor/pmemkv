// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2020, Intel Corporation */

#include "new_map.h"
#include "../out.h"

namespace pmem
{
namespace kv
{
namespace internal
{
namespace new_map
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
} /* namespace new_map */
} /* namespace internal */

new_map::new_map(std::unique_ptr<internal::config> cfg)
    : pmemobj_engine_base(cfg, "pmemkv_new_map"), config(std::move(cfg))
{
	Recover();
	LOG("Started ok");
}

new_map::~new_map()
{
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
	cnt = container->size();

	return status::OK;
}

template <typename It>
static std::size_t size(It first, It last)
{
	auto dist = std::distance(first, last);
	assert(dist >= 0);

	return static_cast<std::size_t>(dist);
}

status new_map::count_above(string_view key, std::size_t &cnt)
{
	LOG("count_above for key=" << std::string(key.data(), key.size()));
	check_outside_tx();

	auto first = container->upper_bound(key);
	auto last = container->end();

	cnt = size(first, last);

	return status::OK;
}

status new_map::count_equal_above(string_view key, std::size_t &cnt)
{
	LOG("count_equal_above for key=" << std::string(key.data(), key.size()));
	check_outside_tx();

	auto first = container->lower_bound(key);
	auto last = container->end();

	cnt = size(first, last);

	return status::OK;
}

status new_map::count_equal_below(string_view key, std::size_t &cnt)
{
	LOG("count_equal_below for key=" << std::string(key.data(), key.size()));
	check_outside_tx();

	auto first = container->begin();
	auto last = container->upper_bound(key);

	cnt = size(first, last);

	return status::OK;
}

status new_map::count_below(string_view key, std::size_t &cnt)
{
	LOG("count_below for key=" << std::string(key.data(), key.size()));
	check_outside_tx();

	auto first = container->begin();
	auto last = container->lower_bound(key);

	cnt = size(first, last);

	return status::OK;
}

status new_map::count_between(string_view key1, string_view key2, std::size_t &cnt)
{
	LOG("count_between for key1=" << key1.data() << ", key2=" << key2.data());
	check_outside_tx();

	if (key1.compare(key2) < 0) {
		auto first = container->upper_bound(key1);
		auto last = container->lower_bound(key2);

		cnt = size(first, last);
	} else {
		cnt = 0;
	}

	return status::OK;
}

status new_map::iterate(typename container_type::const_iterator first,
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

status new_map::get_all(get_kv_callback *callback, void *arg)
{
	LOG("get_all");
	check_outside_tx();

	auto first = container->begin();
	auto last = container->end();

	return iterate(first, last, callback, arg);
}

status new_map::get_above(string_view key, get_kv_callback *callback, void *arg)
{
	LOG("get_above for key=" << std::string(key.data(), key.size()));
	check_outside_tx();

	auto first = container->upper_bound(key);
	auto last = container->end();

	return iterate(first, last, callback, arg);
}

status new_map::get_equal_above(string_view key, get_kv_callback *callback, void *arg)
{
	LOG("get_equal_above for key=" << std::string(key.data(), key.size()));
	check_outside_tx();

	auto first = container->lower_bound(key);
	auto last = container->end();

	return iterate(first, last, callback, arg);
}

status new_map::get_equal_below(string_view key, get_kv_callback *callback, void *arg)
{
	LOG("get_equal_below for key=" << std::string(key.data(), key.size()));
	check_outside_tx();

	auto first = container->begin();
	auto last = container->upper_bound(key);

	return iterate(first, last, callback, arg);
}

status new_map::get_below(string_view key, get_kv_callback *callback, void *arg)
{
	LOG("get_below for key=" << std::string(key.data(), key.size()));
	check_outside_tx();

	auto first = container->begin();
	auto last = container->lower_bound(key);

	return iterate(first, last, callback, arg);
}

status new_map::get_between(string_view key1, string_view key2, get_kv_callback *callback,
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

status new_map::exists(string_view key)
{
	LOG("exists for key=" << std::string(key.data(), key.size()));
	check_outside_tx();

	return container->find(key) != container->end() ? status::OK : status::NOT_FOUND;
}

status new_map::get(string_view key, get_v_callback *callback, void *arg)
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

status new_map::put(string_view key, string_view value)
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

status new_map::remove(string_view key)
{
	LOG("remove key=" << std::string(key.data(), key.size()));
	check_outside_tx();

	auto it = container->find(key);

	if (it == container->end())
		return status::NOT_FOUND;

	container->erase(it);

	return status::OK;
}

internal::transaction *new_map::begin_tx()
{
	return new internal::new_map::transaction(pmpool, container);
}

void new_map::Recover()
{
	if (!OID_IS_NULL(*root_oid)) {
		auto pmem_ptr = static_cast<internal::new_map::pmem_type *>(
			pmemobj_direct(*root_oid));

		container = &pmem_ptr->map;
	} else {
		pmem::obj::transaction::run(pmpool, [&] {
			pmem::obj::transaction::snapshot(root_oid);
			*root_oid =
				pmem::obj::make_persistent<internal::new_map::pmem_type>()
					.raw();
			auto pmem_ptr = static_cast<internal::new_map::pmem_type *>(
				pmemobj_direct(*root_oid));
			container = &pmem_ptr->map;
		});
	}
}

internal::iterator_base *new_map::new_iterator()
{
	return new new_map_iterator<false>{container};
}

internal::iterator_base *new_map::new_const_iterator()
{
	return new new_map_iterator<true>{container};
}

new_map::new_map_iterator<true>::new_map_iterator(container_type *c)
    : container(c), pop(pmem::obj::pool_by_vptr(c))
{
}

new_map::new_map_iterator<false>::new_map_iterator(container_type *c)
    : new_map::new_map_iterator<true>(c)
{
}

status new_map::new_map_iterator<true>::seek(string_view key)
{
	init_seek();

	it_ = container->find(key);
	if (it_ != container->end())
		return status::OK;

	return status::NOT_FOUND;
}

status new_map::new_map_iterator<true>::seek_lower(string_view key)
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

status new_map::new_map_iterator<true>::seek_lower_eq(string_view key)
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

status new_map::new_map_iterator<true>::seek_higher(string_view key)
{
	init_seek();

	it_ = container->upper_bound(key);
	if (it_ == container->end())
		return status::NOT_FOUND;

	return status::OK;
}

status new_map::new_map_iterator<true>::seek_higher_eq(string_view key)
{
	init_seek();

	it_ = container->lower_bound(key);
	if (it_ == container->end())
		return status::NOT_FOUND;

	return status::OK;
}

status new_map::new_map_iterator<true>::seek_to_first()
{
	init_seek();

	if (container->empty())
		return status::NOT_FOUND;

	it_ = container->begin();

	return status::OK;
}

status new_map::new_map_iterator<true>::seek_to_last()
{
	init_seek();

	if (container->empty())
		return status::NOT_FOUND;

	it_ = container->end();
	--it_;

	return status::OK;
}

status new_map::new_map_iterator<true>::is_next()
{
	auto tmp = it_;
	if (tmp == container->end() || ++tmp == container->end())
		return status::NOT_FOUND;

	return status::OK;
}

status new_map::new_map_iterator<true>::next()
{
	init_seek();

	if (it_ == container->end() || ++it_ == container->end())
		return status::NOT_FOUND;

	return status::OK;
}

status new_map::new_map_iterator<true>::prev()
{
	init_seek();

	if (it_ == container->begin())
		return status::NOT_FOUND;

	--it_;

	return status::OK;
}

result<string_view> new_map::new_map_iterator<true>::key()
{
	assert(it_ != container->end());

	return {it_->key().cdata()};
}

result<pmem::obj::slice<const char *>> new_map::new_map_iterator<true>::read_range(size_t pos,
									       size_t n)
{
	assert(it_ != container->end());

	if (pos + n > it_->value().size() || pos + n < pos)
		n = it_->value().size() - pos;

	return {{it_->value().cdata() + pos, it_->value().cdata() + pos + n}};
}

result<pmem::obj::slice<char *>> new_map::new_map_iterator<false>::write_range(size_t pos,
									   size_t n)
{
	assert(it_ != container->end());

	if (pos + n > it_->value().size() || pos + n < pos)
		n = it_->value().size() - pos;

	log.push_back({std::string(it_->value().cdata() + pos, n), pos});
	auto &val = log.back().first;

	return {{&val[0], &val[n]}};
}

status new_map::new_map_iterator<false>::commit()
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

void new_map::new_map_iterator<false>::abort()
{
	log.clear();
}

} // namespace kv
} // namespace pmem
