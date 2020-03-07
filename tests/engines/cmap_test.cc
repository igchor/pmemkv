/*
 * Copyright 2017-2020, Intel Corporation
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

#include "../../src/libpmemkv.hpp"
#include "gtest/gtest.h"
#include <cstdio>
#include <memory>
#include <string>
#include <thread>
#include <vector>

using namespace pmem::kv;

extern std::string test_path;
const size_t SIZE = 1024ull * 1024ull * 512ull;
const size_t LARGE_SIZE = 1024ull * 1024ull * 1024ull * 2ull;

template <typename Function>
void parallel_exec(size_t threads_number, Function f)
{
	std::vector<std::thread> threads;
	threads.reserve(threads_number);

	for (size_t i = 0; i < threads_number; ++i) {
		threads.emplace_back(f, i);
	}

	for (auto &t : threads) {
		t.join();
	}
}

template <size_t POOL_SIZE>
class CMapBaseTest : public testing::Test {
private:
	std::string PATH = test_path + "/cmap_test";

public:
	std::unique_ptr<db> kv = nullptr;

	CMapBaseTest()
	{
		std::remove(PATH.c_str());
		Start(true);
	}

	~CMapBaseTest()
	{
		kv->close();
		std::remove(PATH.c_str());
	}
	void Restart()
	{
		kv->close();
		kv.reset(nullptr);
		Start(false);
	}

protected:
	void Start(bool create)
	{
		config cfg;
		auto cfg_s = cfg.put_string("path", PATH);
		if (cfg_s != status::OK)
			throw std::runtime_error("putting 'path' to config failed");

		if (create) {
			cfg_s = cfg.put_uint64("force_create", 1);
			if (cfg_s != status::OK)
				throw std::runtime_error(
					"putting 'force_create' to config failed");

			cfg_s = cfg.put_uint64("size", POOL_SIZE);

			if (cfg_s != status::OK)
				throw std::runtime_error(
					"putting 'size' to config failed");
		}

		kv.reset(new db);
		auto s = kv->open("radix_tree", std::move(cfg));
		if (s != status::OK)
			throw std::runtime_error(errormsg());
	}
};

using CMapTest = CMapBaseTest<SIZE>;

// =============================================================================================
// TEST SMALL COLLECTIONS
// =============================================================================================

TEST_F(CMapTest, SimpleTest_TRACERS_MPHD)
{
	kv->put("0123456", "11111");
	kv->put("012345", "22222");
	kv->put("0123", "33333");
	kv->put("012XXX", "44444");
	kv->put("012YYYY", "5555");

	std::string val;
	kv->get("0123456", &val);
	ASSERT_TRUE(val == "11111");

	kv->get("012345", &val);
	ASSERT_TRUE(val == "22222");

	kv->get("0123", &val);
	ASSERT_TRUE(val == "33333");

	kv->get("012XXX", &val);
	ASSERT_TRUE(val == "44444");

	kv->get("012YYYY", &val);
	ASSERT_TRUE(val == "5555");

	auto s = kv->remove("012345");
	ASSERT_TRUE(s == status::OK);

	s = kv->remove("XXXXX");
	ASSERT_TRUE(s == status::NOT_FOUND);

	s = kv->get("012345", &val);
	ASSERT_TRUE(s == status::NOT_FOUND);
}
