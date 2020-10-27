// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2020, Intel Corporation */

#include "unittest.hpp"

using namespace pmem::kv;

static void test_commit(pmemkv_db* kv)
{
	auto tx = pmemkv_tx_begin(kv);
	pmemkv_tx_put(tx, "a", 1, "a", 1);
	pmemkv_tx_put(tx, "b", 1, "b", 1);
	pmemkv_tx_put(tx, "c", 1, "c", 1);

	// UT_ASSERT(pmemkv_exists(kv, "a", 1) == PMEMKV_STATUS_NOT_FOUND);
	// UT_ASSERT(pmemkv_exists(kv, "b", 1) == PMEMKV_STATUS_NOT_FOUND);
	// UT_ASSERT(pmemkv_exists(kv, "c", 1) == PMEMKV_STATUS_NOT_FOUND);

	pmemkv_tx_abort(tx);

	// pmemkv_tx_commit(tx);

	UT_ASSERT(pmemkv_exists(kv, "a", 1) == PMEMKV_STATUS_NOT_FOUND);
	UT_ASSERT(pmemkv_exists(kv, "b", 1) == PMEMKV_STATUS_NOT_FOUND);
	UT_ASSERT(pmemkv_exists(kv, "c", 1) == PMEMKV_STATUS_NOT_FOUND);

	// UT_ASSERT(pmemkv_exists(kv, "a", 1) == PMEMKV_STATUS_OK);
	// UT_ASSERT(pmemkv_exists(kv, "b", 1) == PMEMKV_STATUS_OK);
	// UT_ASSERT(pmemkv_exists(kv, "c", 1) == PMEMKV_STATUS_OK);
}

static void test_batched_updates(pmemkv_db* kv)
{
	const int NUM_BATCH = 10000;
	const int BATCH_SIZE = 10;

	auto gen_key = [](int b, int i) {
		return std::to_string(b) + ";" + std::to_string(i);
	};

	for (int i = 0; i < NUM_BATCH; i++) {
		auto tx = pmemkv_tx_begin(kv);

		for (int j = 0; j < BATCH_SIZE; j++) {
			std::string key = gen_key(i, j);
			std::string value = key;
			pmemkv_tx_put(tx, key.data(), key.size(), value.data(), value.size());

			// UT_ASSERT(pmemkv_exists(kv, key.data(), key.size()) == PMEMKV_STATUS_NOT_FOUND);
		}

		pmemkv_tx_commit(tx);
	}

	for (int i = 0; i < NUM_BATCH; i++) {
		for (int j = 0; j < BATCH_SIZE; j++) {
			std::string key = gen_key(i, j);
			std::string value = key;
			UT_ASSERT(pmemkv_exists(kv, key.data(), key.size()) == PMEMKV_STATUS_OK);
		}
	}
}

static void test(int argc, char *argv[])
{
	if (argc < 3)
		UT_FATAL("usage: %s engine json_config", argv[0]);

	pmemkv_db *kv;
	int s = pmemkv_open(argv[1], C_CONFIG_FROM_JSON(argv[2]), &kv);
	UT_ASSERTeq(s, PMEMKV_STATUS_OK);

	test_commit(kv);
	// test_batched_updates(kv);

	pmemkv_close(kv);
}

int main(int argc, char *argv[])
{
	return run_test([&] { test(argc, argv); });
}
