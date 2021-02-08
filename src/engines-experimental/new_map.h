// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2020, Intel Corporation */

#pragma once

#include "../iterator.h"
#include "../pmemobj_engine.h"

#include <libpmemobj++/persistent_ptr.hpp>

#include <libpmemobj++/experimental/inline_string.hpp>
#include <libpmemobj++/experimental/radix_tree.hpp>

#include <libpmemobj++/detail/pair.hpp>

#include <libpmemobj++/container/string.hpp>

#include <libpmemobj++/container/concurrent_hash_map.hpp>

#include <tbb/concurrent_hash_map.h>

#include <endian.h>

#include <shared_mutex>

#include <condition_variable>

#include <libpmemobj/action_base.h>

namespace pmem
{
namespace kv
{

	class actions {
public:
    actions(obj::pool_base pop, size_t cnt = 1) : pop(pop) {
        acts.reserve(cnt);
    }

    ~actions() {

		// XXX - for test
		//if (acts.size() > 0)
        //	pmemobj_cancel(pop.handle(), acts.data(), acts.size());
    }

    template <typename T>
    typename detail::pp_if_array<T>::type allocate(size_t cnt = 1) {
        typedef typename detail::pp_array_type<T>::type I;

        acts.emplace_back();
        return pmemobj_reserve(pop.handle(), &acts.back(), sizeof(I) * cnt, 0);
    }

    // typename detail::pp_if_not_array<T>::type allocate() {
    // }

    void set_value(uint64_t* ptr, uint64_t value) {
        acts.emplace_back();
        pmemobj_set_value(pop.handle(), &acts.back(), ptr, value);
    }

	template <typename T>
	void
	free(obj::persistent_ptr<T> data) {
		acts.emplace_back();
		pmemobj_defer_free(pop.handle(), data.raw(), &acts.back());
	}

    void publish() {
        if (pmemobj_publish(pop.handle(), acts.data(), acts.size()) != 0)
            throw std::runtime_error(std::string("publish failed: ") + pmemobj_errormsg());

		acts.clear();
    }

	obj::pool_base get_pool() { return pop; }

private:
    obj::pool_base pop;
    std::vector<pobj_action> acts;
};


using dram_index = tbb::concurrent_hash_map<string_view, string_view>;

struct pmem_type {
	pmem_type()
	{
		std::memset(reserved, 0, sizeof(reserved));
	}

	obj::persistent_ptr<std::pair<obj::string, obj::string>[]> str[16];

	uint64_t reserved[8];
};

// static_assert(sizeof(pmem_type) == sizeof(map_type) + 64, "");


class new_map : public pmemobj_engine_base<pmem_type> {
	template <bool IsConst>
	class iterator;

public:
	new_map(std::unique_ptr<internal::config> cfg);
	~new_map();

	new_map(const new_map &) = delete;
	new_map &operator=(const new_map &) = delete;

	std::string name() final;

	status count_all(std::size_t &cnt) final;

	status get_all(get_kv_callback *callback, void *arg) final;

	status exists(string_view key) final;

	status get(string_view key, get_v_callback *callback, void *arg) final;

	status put(string_view key, string_view value) final;

	status remove(string_view key) final;

private:
	/// using container_type = obj::vector<pmem_map_type>;
	//using pmem_type = pmem_type;

	void Recover();

	size_t dram_capacity = 1024;

	std::atomic<size_t> cnt = 0;

	size_t sizes[16];

	pmem_type *pmem;
	std::unique_ptr<internal::config> config;

	std::unique_ptr<dram_index> index;
};

} /* namespace kv */
} /* namespace pmem */
