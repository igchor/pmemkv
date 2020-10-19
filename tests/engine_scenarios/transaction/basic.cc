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
	pmemkv_tx_commit(tx);

	UT_ASSERT(pmemkv_exists(kv, "a", 1) == PMEMKV_STATUS_OK);
	UT_ASSERT(pmemkv_exists(kv, "b", 1) == PMEMKV_STATUS_OK);
	UT_ASSERT(pmemkv_exists(kv, "c", 1) == PMEMKV_STATUS_OK);

}

static void test(int argc, char *argv[])
{
	if (argc < 3)
		UT_FATAL("usage: %s engine json_config", argv[0]);

	pmemkv_db *kv;
	int s = pmemkv_open(argv[1], C_CONFIG_FROM_JSON(argv[2]), &kv);
	UT_ASSERTeq(s, PMEMKV_STATUS_OK);

	test_commit(kv);

	pmemkv_close(kv);
}

int main(int argc, char *argv[])
{
	return run_test([&] { test(argc, argv); });
}
