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

#ifndef LIBPMEMKV_ENGINE_FACTORY_H
#define LIBPMEMKV_ENGINE_FACTORY_H

#include "config.h"
#include "engine.h"

#include <functional>
#include <iostream>
#include <memory>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <vector>

template <int N>
struct Counter : public Counter<N - 1> {
	static constexpr int value = N;
};

template <>
struct Counter<0> {
	static constexpr int value = 0;
};

template <std::size_t N>
struct ConstexprList {
	template <typename T>
	ConstexprList(T)
	{
		std::cerr << "0\n";
	}
};

template <>
struct ConstexprList<0> {
	template <typename T>
	ConstexprList(T)
	{
		std::cerr << "0\n";
	}
};

using map_type =
	std::unordered_map<std::string,
			   std::function<pmem::kv::engine_base *(
				   std::unique_ptr<pmem::kv::internal::config>)>>;

static constexpr Counter<1> counter_inc(Counter<1> cnt)
{
	return cnt;
}

#define COUNTER_GET() decltype(counter_inc(Counter<255>{}))::value
#define COUNTER_INC()                                                                    \
	static constexpr Counter<COUNTER_GET() + 1> counter_inc(                         \
		Counter<COUNTER_GET() + 1> cnt)                                          \
	{                                                                                \
		return cnt;                                                              \
	}

#define REGISTER_ENGINE(Name, Engine)                                                    \
	template <>                                                                      \
	struct ConstexprList<COUNTER_GET()> : ConstexprList<COUNTER_GET() - 1> {         \
		ConstexprList(map_type &map) : ConstexprList<COUNTER_GET() - 1>(map)     \
		{                                                                        \
			std::cerr << "1";                                                \
			map.insert(                                                      \
				{Name,                                                   \
				 [](std::unique_ptr<pmem::kv::internal::config> cfg) {   \
					 return new Engine(std::move(cfg));              \
				 }});                                                    \
		}                                                                        \
	};                                                                               \
	COUNTER_INC();

struct engine_factory {
	engine_factory(const engine_factory &rhs) = delete;
	engine_factory &operator=(const engine_factory &rhs) = delete;

	template <int N>
	static engine_factory &get_instance()
	{
		static engine_factory instance;
		engine_factory::call<N>();

		return instance;
	}

	static pmem::kv::engine_base *
	create_engine(const std::string &name,
		      std::unique_ptr<pmem::kv::internal::config> cfg)
	{
		auto find = map.find(name);
		if (find == map.end())
			throw std::runtime_error("Not found");

		else
			return find->second(std::move(cfg));
	}

	static map_type map;

private:
	template <int N>
	static void call()
	{
		/* instantiate list - call constructors of ConstexprList<X> for every X
		 * from 0 to COUNTER_GET() */
		ConstexprList<N> xxx(engine_factory::map);
	}

	engine_factory()
	{
	}
};

#endif /* LIBPMEMKV_ENGINE_FACTORY_H */
