// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2020, Intel Corporation */

/*
 * pmemkv_transaction.cpp -- example usage of pmemkv transactions.
 */

#include <cassert>
#include <cstdlib>
#include <iostream>
#include <libpmemkv.hpp>

#define ASSERT(expr)                                                                     \
	do {                                                                             \
		if (!(expr))                                                             \
			std::cout << pmemkv_errormsg() << std::endl;                     \
		assert(expr);                                                            \
	} while (0)
#define LOG(msg) std::cout << msg << std::endl

using namespace pmem::kv;

/**
 * This example expects a path to already created pool.
 *
 * To create a pool use one of the following commands.
 *
 * For regular pools use:
 * pmempool create -l -s 1G "pmemkv_radix" obj path_to_a_pool
 *
 * For poolsets use:
 * pmempool create -l "pmemkv" obj ../examples/example.poolset
 */
int main(int argc, char *argv[])
{
	if (argc < 2) {
		std::cerr << "Usage: " << argv[0] << " pool\n";
		exit(1);
	}

	/* See libpmemkv_config(3) for more detailed example of creating a config */
	LOG("Creating config");
	config cfg;

	status s = cfg.put_path(argv[1]);
	ASSERT(s == status::OK);

	LOG("Opening pmemkv database with 'cmap' engine");
	db kv;
	s = kv.open("radix", std::move(cfg));
	ASSERT(s == status::OK);

	LOG("Putting new key");
	s = kv.put("key1", "value1");
	ASSERT(s == status::OK);

	//////////////////////////////////////////////////
	// Tx with read_for_update
	auto it1 = kv.new_write_iterator();
	auto tx = kv.tx_begin();

	it1.seek("k1");
	auto r1 = it1.read_range();
	tx.read_for_update(r1);

	it1.seek("k2");
	auto r2 = it1.read_range();
	tx.read_for_update(r2);

	it1.seek("k1");	
	it.write_range()[0] = r1[0];
	tx.add(it1);

	it1.seek("k2");
	it.write_range()[0] = r2[0]; // XXX: inconsistent write_range + add vs tx.read_for_update()
	tx.add(it1);

	tx.commit(); // only succeeds if r1 and r2 didn't change
	//////////////////////////////////////////////////

	//////////////////////////////////////////////////
	// Lock multiple elements at once
	auto it1 = kv.new_write_iterator();
	auto it2 = kv.new_write_iterator();

	do {
		auto s = multi_seek(it1, "k1", it, "k2"); // aka SELECT FOR UPDATE, can fail due to conflict (deadlock prevention)
	while (s != status::CONFLICT);

	auto tx = kv.tx_begin();

	auto r1 = it1.read_range();
	auto r2 = it2.read_range();

	it2.write_range()[0] = r1[0];
	it1.write_range()[0] = r2[0];

	tx.add(it1);
	tx.add(it2);
	tx.commit();
	//////////////////////////////////////////////////

	//////////////////////////////////////////////////
	// seek and commit can return CONFLICT/ABORTED/TIMED_OUT
	auto it1 = kv.new_write_iterator();
	auto it2 = kv.new_write_iterator();

	do {
		auto tx = kv.tx_begin();

		s = it1.seek("k1");
		s = it2.seek("k2"); // can return LOCK_TIMED_OUT for lock-based engines
		if (s == status::LOCK_TIMED_OUT)
			continue;

		auto r1 = it1.read_range();
		auto r2 = it2.read_range();

		it2.write_range()[0] = r1[0];
		it1.write_range()[0] = r2[0];

		// it1.commit();
		// it2.commit();
		tx.add(it1);
		tx.add(it2);
		tx.commit();
	} while (s == status::LOCK_TIMED_OUT);

	// lock-free engines
	do {
		auto tx = kv.tx_begin();

		s = it1.seek("k1");
		s = it2.seek("k2");

		auto r1 = it1.read_for_update();
		auto r2 = it2.read_for_update();

		it2.write_range()[0] = r1[0];
		it1.write_range()[0] = r2[0];

		// it1.commit();
		// it2.commit();
		tx.add(it1);
		tx.add(it2);

		s = tx.commit(); // can return CONFLICT (needs to restart the transaction)
	} while (s != status::OK);
	//////////////////////////////////////////////////

	

	/* Until transaction is committed, changes are not visible */
	ASSERT(kv.exists("key1") == status::OK);
	ASSERT(kv.exists("key2") == status::NOT_FOUND);
	ASSERT(kv.exists("key3") == status::NOT_FOUND);

	s = tx.commit();
	ASSERT(s == status::OK);

	ASSERT(kv.exists("key1") == status::NOT_FOUND);
	ASSERT(kv.exists("key2") == status::OK);
	ASSERT(kv.exists("key3") == status::OK);

	return 0;
}
