// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2020, Intel Corporation */

#ifndef PMEMKV_UTILS
#define PMEMKV_UTILS

#include <cstdint>

namespace pmem
{
namespace kv
{
namespace internal
{
namespace utils
{

static inline uint8_t mssb_index64(uint64_t value)
{
	return ((uint8_t)(63 - __builtin_clzll(value)));
}

static inline uint8_t mssb_index(uint32_t value)
{
	return ((uint8_t)(31 - __builtin_clz(value)));
}

}
}
}
}

#endif /* PMEMKV_UTILS */
