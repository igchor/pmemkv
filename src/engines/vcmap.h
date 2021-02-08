// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2017-2020, Intel Corporation */

#pragma once

#include "../engine.h"
#include <scoped_allocator>
#include <string>
#include <tbb/concurrent_hash_map.h>

namespace pmem
{
namespace kv
{

class string_hasher {
public:
	using is_transparent = void;

	size_t hash(const std::string &str) const
	{
		return std::hash<std::string>{}(str);
	}

	size_t hash(string_view str) const
	{
		return std::hash<string_view>{}(str);
	}

	template <typename M, typename U>
	bool equal(const M &lhs, const U &rhs) const
	{
		return lhs == rhs;
	}
};

class vcmap : public engine_base {
	template <bool IsConst>
	class vcmap_iterator;

public:
	vcmap(std::unique_ptr<internal::config> cfg);
	~vcmap();

	std::string name() final;

	status count_all(std::size_t &cnt) final;

	status get_all(get_kv_callback *callback, void *arg) final;

	status exists(string_view key) final;

	status get(string_view key, get_v_callback *callback, void *arg) final;

	status put(string_view key, string_view value) final;

	status remove(string_view key) final;

	internal::iterator_base *new_iterator() final;
	internal::iterator_base *new_const_iterator() final;

private:
	typedef std::basic_string<char> pmem_string;
	typedef tbb::concurrent_hash_map<pmem_string, pmem_string, string_hasher> map_t;
	map_t pmem_kv_container;
};

template <>
class vcmap::vcmap_iterator<true> : virtual public internal::iterator_base {
	using container_type = vcmap::map_t;

public:
	vcmap_iterator(container_type *container);

	status seek(string_view key) final;

	result<string_view> key() final;

	result<pmem::obj::slice<const char *>> read_range(size_t pos, size_t n) final;

protected:
	container_type *container;
	container_type::accessor acc_;
};

template <>
class vcmap::vcmap_iterator<false> : public vcmap::vcmap_iterator<true> {
	using container_type = vcmap::map_t;

public:
	vcmap_iterator(container_type *container);

	result<pmem::obj::slice<char *>> write_range(size_t pos, size_t n) final;
	status commit() final;
	void abort() final;

private:
	std::vector<std::pair<std::string, size_t>> log;
};

} /* namespace kv */
} /* namespace pmem */
