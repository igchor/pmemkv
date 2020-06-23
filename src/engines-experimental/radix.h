// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2020, Intel Corporation */

#pragma once

#include "../pmemobj_engine.h"

#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pool.hpp>
#include <libpmemobj++/shared_mutex.hpp>

#include <libpmemobj++/container/string.hpp>

#include <atomic>
#include <shared_mutex>

namespace pmem
{
namespace kv
{
namespace internal
{
namespace radix
{

static constexpr std::size_t SLICE = 4;
static constexpr std::size_t NIB = ((1ULL << SLICE) - 1);
static constexpr std::size_t SLNODES = (1 << SLICE);

using byten_t = uint32_t;
using bitn_t = uint8_t;

/**
 * Based on: https://github.com/pmem/pmdk/blob/master/src/libpmemobj/critnib.h
 */
class tree {
public:
	/* Default ctor - constructs empty tree */
	tree();

	/* Dtor - removes entire tree */
	~tree();

	/*
	 * insert -- write a key:value pair to the radix tree
	 */
	void insert(obj::pool_base &pop, string_view key, string_view value);

	/*
	 * get -- query for a key ("==" match)
	 */
	bool get(string_view key, pmemkv_get_v_callback *cb, void *arg);

	/*
	 * remove -- delete a key from the radit tree return true if element was present
	 */
	bool remove(obj::pool_base &pop, string_view key);

	/*
	 * iterate -- iterate over all leafs
	 */
	void iterate(pmemkv_get_kv_callback *callback, void *arg);

	/*
	 * size -- return number of elements
	 */
	uint64_t size();

private:
	struct leaf;
	struct node;

	struct tagged_node_ptr {
		tagged_node_ptr();
		tagged_node_ptr(const tagged_node_ptr &rhs);
		tagged_node_ptr(std::nullptr_t);

		tagged_node_ptr(const obj::persistent_ptr<leaf> &ptr);
		tagged_node_ptr(const obj::persistent_ptr<node> &ptr);

		tagged_node_ptr &operator=(const tagged_node_ptr &rhs);
		tagged_node_ptr &operator=(std::nullptr_t);
		tagged_node_ptr &operator=(const obj::persistent_ptr<leaf> &rhs);
		tagged_node_ptr &operator=(const obj::persistent_ptr<node> &rhs);

		bool operator==(const tagged_node_ptr &rhs) const;
		bool operator!=(const tagged_node_ptr &rhs) const;

		bool is_leaf() const;

		tree::leaf *get_leaf(uint64_t) const;
		tree::node *get_node(uint64_t) const;

		tree::node *operator->() const noexcept;

		explicit operator bool() const noexcept;

	private:
		template <typename T>
		T get() const noexcept
		{
			return reinterpret_cast<T>(reinterpret_cast<uint64_t>(this) +
						   (off & ~uint64_t(1)));
		}

		obj::p<uint64_t> off;
	};

	/*
	 * Internal nodes store SLNODES children ptrs + one leaf ptr.
	 * The leaf ptr is used only for nodes for which length of the path from
	 * root is a multiple of byte (n->bit == 8 - SLICE).
	 *
	 * This ptr stores pointer to a leaf
	 * which is a prefix of some other leaf.
	 */
	struct node {
		tagged_node_ptr child[SLNODES];
		tagged_node_ptr leaf = nullptr; // -> ptr<leaf>
		byten_t byte;
		bitn_t bit;

		// uint8_t padding[256 - sizeof(mtx) - sizeof(child) - sizeof(byte) -
		// 		sizeof(bit)];
	};

	// static_assert(sizeof(node) == 256, "Wrong node size");

	struct leaf {
		leaf(string_view key, string_view value);

		const char *data() const noexcept;
		string_view key() const noexcept;

		std::size_t capacity();

		// obj::p<uint64_t> vsize;
		obj::p<uint64_t> ksize;

		pmem::obj::string value;

	private:
		char *data_rw();
	};

	int n_child(tagged_node_ptr n)
	{
		int num = 0;
		for (int i = 0; i < (int)SLNODES; i++) {
			auto &child = n->child[i];
			if (child) {
				num++;
			}
		}

		return num;
	}

	tagged_node_ptr root;
	obj::p<uint64_t> size_;
	uint64_t pool_id = 0;

	template <typename... Args>
	tagged_node_ptr make_leaf(string_view key, Args &&... args)
	{
		assert(pmemobj_tx_stage() == TX_STAGE_WORK);

		auto ptr = pmem::obj::persistent_ptr<tree::leaf>(
			pmemobj_tx_alloc(sizeof(tree::leaf) + key.size(), 0));
		new (ptr.get()) tree::leaf(key, std::forward<Args>(args)...);

		size_++;

		return ptr;
	}

	leaf *any_leaf(tagged_node_ptr n);
	leaf *descend(string_view key);

	bool keys_equal(string_view k1, string_view k2)
	{
		return k1.size() == k2.size() && k1.compare(k2) == 0;
	}

	/*
	 * internal: slice_index -- return index of child at the given nib
	 */
	unsigned slice_index(char k, uint8_t shift);

	/*
	 * internal: delete_node -- recursively free (to malloc) a subtree
	 */
	void delete_node(tagged_node_ptr n);

	/*
	 * Helper method for iteration.
	 */
	void iterate_rec(tagged_node_ptr n, pmemkv_get_kv_callback *callback, void *arg);
};

}
}

class radix : public pmemobj_engine_base<internal::radix::tree> {
public:
	radix(std::unique_ptr<internal::config> cfg);

	~radix();

	radix(const radix &) = delete;
	radix &operator=(const radix &) = delete;

	std::string name() final;

	status get_all(get_kv_callback *callback, void *arg) final;

	status count_all(std::size_t &cnt) final;

	status exists(string_view key) final;

	status get(string_view key, get_v_callback *callback, void *arg) final;

	status put(string_view key, string_view value) final;

	status remove(string_view key) final;

	// status defrag(double start_percent, double amount_percent) final;

private:
	using lock_type = std::shared_timed_mutex;

	uint64_t key_to_uint64(string_view v);

	internal::radix::tree *tree;

	std::shared_timed_mutex mtx;
};

} /* namespace kv */
} /* namespace pmem */
