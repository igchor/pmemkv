/*
 * Copyright 2019, Intel Corporation
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *
 *     * Neither the name of the copyright holder nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef LIBPMEMKV_PMEMOBJ_ENGINE_H
#define LIBPMEMKV_PMEMOBJ_ENGINE_H

#include "engine.h"
#include <libpmemobj/base.h>

#include <lbpmemobj++/pool.hpp>

namespace pmem
{
namespace kv
{

template <typename Root, typename Layout>
class pmemobj_engine_base {
public:
	pmemobj_engine_base(std::unique_ptr<internal::config> cfg)
	{
		const char *path;
		std::size_t size;
		PMEMoid oid;

		auto get_path = cfg->get_string("path", &path);

		std::size_t oid_size;
		auto get_oid = cfg->get_data("oid", &oid, &oid_size);

		if (get_path) {
			uint64_t force_create;
			if (!cfg->get_uint64("force_create", &force_create)) {
				force_create = 0;
			}

			if (force_create) {
				if (!cfg->get_uint64("size", &size))
					throw internal::invalid_argument(
						"Config does not contain item with key: \"size\"");

				pmpool = pmem::obj::pool_base::create(path, Layout::name, size, S_IRWXU);
			} else {
				pmpool = pmem::obj::pool_base::open(path, Layout::name);
			}

			data = pool<pool_root>.root(); //XXX
		} else if (get_oid) {
			pmpool = pmemobj_pool_by_oid(oid);

			if (OID_IS_NULL())
		} else {
			throw internal::invalid_argument("Config does not contain item with key: \"path\" or \"oid\"");
		}
	}

	struct pool_root {
		pmem::obj::persistent_ptr<Root> ptr;
	};

protected:
	pmem::obj::pool_base pmpool;
	pmem::obj::persistent_ptr<Root> data; 
};

} /* namespace kv */
} /* namespace pmem */

#endif /* LIBPMEMKV_PMEMOBJ_ENGINE_H */
