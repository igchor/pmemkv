// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2020, Intel Corporation */

#include <cassert>
#include <cstdlib>
#include <iostream>
#include <libpmemkv.hpp>

#define LOG(msg) std::cout << msg << std::endl

using namespace pmem::kv;

const uint64_t SIZE = 1024UL * 1024UL * 1024UL;

int main(int argc, char *argv[])
{
	if (argc < 2) {
		std::cerr << "Usage: " << argv[0] << " file\n";
		exit(1);
	}

	/* See libpmemkv_config(3) for more detailed example of config creation */
	LOG("Creating config");
	config cfg;

	status s = cfg.put_string("path", argv[1]);
	s = cfg.put_uint64("size", SIZE);
	s = cfg.put_uint64("force_create", 1);

	db *kv = new db();
	s = kv->open("cmap", std::move(cfg));

	kv->put("key2", "value2");
	kv->put("key3", "value3");
	kv->get_all([](string_view k, string_view v) {
		LOG("  visited: " << k.data());
		return 0;
	});

	batch b;
    b.append("key4", "value4");
    b.append("key5", "value5");
    b.append("key6", "value6");

    kv->put(b);

    kv->get_above("key3", [](string_view k, string_view v) {
		LOG("  visited: " << k.data());
		return 1;
	});

	delete kv;

	return 0;
}
