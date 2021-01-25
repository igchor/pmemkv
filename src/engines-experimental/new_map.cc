// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2020, Intel Corporation */

#include "new_map.h"
#include "../out.h"

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
	return get_all([](const char* k, size_t kb, const char* v, size_t vb, void* arg){
		auto size = static_cast<size_t*>(arg);
		(*size)++;

		return PMEMKV_STATUS_OK;
	}, &cnt);
}

status new_map::get_all(get_kv_callback *callback, void *arg)
{
	LOG("get_all");
	check_outside_tx();

	auto s = index.iterate(callback, arg);
	if (s != status::OK)
		return s;

	for (auto &c : *container) {
		for (auto it = c.cbegin(); it != c.cend(); ++it) {
			string_view key = it->key();
			string_view value = it->value();

			if (index.exists(key))
				continue;

			auto ret =
				callback(key.data(), key.size(), value.data(), value.size(), arg);

			if (ret != 0)
				return status::STOPPED_BY_CB;
		}
	}

	return s;
}

status new_map::exists(string_view key)
{
	LOG("exists for key=" << std::string(key.data(), key.size()));

	return index.exists(key) ? status::OK : status::NOT_FOUND;
}

status new_map::get(string_view key, get_v_callback *callback, void *arg)
{
	LOG("get key=" << std::string(key.data(), key.size()));
	
	return index.get(key, callback, arg) ? status::OK : status::NOT_FOUND;
}

status new_map::put(string_view key, string_view value)
{
	LOG("put key=" << std::string(key.data(), key.size())
		       << ", value.size=" << std::to_string(value.size()));

	// auto imm = index.maybe_compact();
	// if (imm) {
	// 	// XXX: create thread pool,
	// 	// split the work between several threads (configurable?)
	// 	auto compaction_thread = std::thread([&]{
	// 		std::vector<std::vector<index_type::dram_map_type::value_type*>> elements(SHARDS_NUM);

	// 		for (auto &e : *imm) {
	// 			elements[shard(e->first)].push_back(&e);
	// 		}

	// 		// XXX - allocate radix nodes before taking locks

	// 		for (int i = 0; i < SHARDS_NUM; i++) {
	// 			unique_lock_type lock(mtxs[i]);
	// 			obj::transaction::run(pmpool, [&]{
	// 				for (const auto &e : elements[i])
	// 					(*container)[i].insert_or_assign(*e);
	// 			});
	// 		}
			
	// 		index.free_immutable();
	// 	});
	// }

	// once we reach this point, there might be no space in the dram again...
	// but if buffer is of reasonable size this should not be a problem (we will exceed the capacity by a small value)
	index.put(key, value);

	return status::OK;
}

status new_map::remove(string_view key)
{
	LOG("remove key=" << std::string(key.data(), key.size()));

	auto found = exists(key);

	index.remove(key);

	/* XXX: Should we just always succeed? checking if the element was actually removed could be expensive */
	// Currently, this is not thread safe
	return found;
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

			container->resize(SHARDS_NUM);
		});
	}

	mtxs = std::vector<mutex_type>(SHARDS_NUM);
}

} // namespace kv
} // namespace pmem
