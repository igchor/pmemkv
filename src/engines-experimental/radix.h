// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2020, Intel Corporation */

#pragma once

#include "../pmemobj_engine.h"
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/pool.hpp>

namespace pmem
{
namespace kv
{
namespace internal
{
namespace radix
{

/*
 * BASED ON: https://github.com/pmem/pmdk/blob/master/src/libpmemobj/critnib.h
 * STRUCTURE DESCRIPTION
 *
 * Following struct is a hybrid between a radix tree and DJ Bernstein's critbit
 * it skips nodes for uninteresting radix nodes (ie, ones that would have
 * exactly one child), this requires adding to every node a field that
 * describes the slice (4-bit in our case) that this radix level is for.
 *
 * This implementation also stores each node's path (ie, bits that are
 * common to every key in that subtree) -- this doesn't help with lookups
 * at all (unused in == match, could be reconstructed at no cost in <=
 * after first dive) but simplifies inserts and removes.  If we ever want
 * that piece of memory it's easy to trim it down.
 */

static constexpr std::size_t SLICE = 4;
static constexpr std::size_t NIB = ((1ULL << SLICE) - 1);
static constexpr std::size_t SLNODES = (1 << SLICE);

typedef unsigned char sh_t;

class tree {
public:
	tree();

	~tree();

	/*
	 * insert -- write a key:value pair to the critnib structure
	 */
	void insert(obj::pool_base &pop, uint64_t key, string_view value);

	/*
	 * get -- query for a key ("==" match), returns value or NULL
	 */
	string_view get(uint64_t key);

	/*
	 * remove -- delete a key from the critnib structure, return its value
	 */
	bool remove(obj::pool_base &pop, uint64_t key);

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
		tagged_node_ptr(const obj::persistent_ptr<leaf> &ptr);
		tagged_node_ptr(const obj::persistent_ptr<node> &ptr);

		tagged_node_ptr(tagged_node_ptr &&rhs) = delete;
		tagged_node_ptr &operator=(tagged_node_ptr &&rhs) = delete;

		tagged_node_ptr &operator=(const tagged_node_ptr &rhs);

		tagged_node_ptr &operator=(std::nullptr_t);

		tagged_node_ptr &operator=(const obj::persistent_ptr<leaf> &rhs);
		tagged_node_ptr &operator=(const obj::persistent_ptr<node> &rhs);

		bool is_leaf() const;

		tree::leaf *get_leaf(uint64_t) const;
		tree::node *get_node(uint64_t) const;

		explicit operator bool() const noexcept;

	private:
		obj::p<uint64_t> off;
	};

	struct node {
		/*
		 * path is the part of a tree that's already traversed (be it through
		 * explicit nodes or collapsed links) -- ie, any subtree below has all
		 * those bits set to this value.
		 *
		 * nib is a 4-bit slice that's an index into the node's children.
		 *
		 * shift is the length (in bits) of the part of the key below this node.
		 *
		 *            nib
		 * |XXXXXXXXXX|?|*****|
		 *    path      ^
		 *              +-----+
		 *               shift
		 */
		tagged_node_ptr child[SLNODES];
		obj::p<uint64_t> path;
		obj::p<sh_t> shift;
	};

	struct leaf {
		leaf(uint64_t key, string_view value);

		void assign(string_view value);

		const char *data() const noexcept;
		const char *cdata() const noexcept;

		std::size_t capacity();

		obj::p<uint64_t> key;
		obj::p<uint64_t> value_size ;

	private:
		char *data();
	};

	tagged_node_ptr root;
	obj::p<uint64_t> size_;
	uint64_t pool_id = 0;

	/*
	 * internal: path_mask -- return bit mask of a path above a subtree [shift]
	 * bits tall
	 */
	uint64_t path_mask(sh_t shift);

	/*
	 * internal: slice_index -- return index of child at the given nib
	 */
	unsigned slice_index(uint64_t key, sh_t shift);

	/*
	 * internal: delete_node -- recursively free (to malloc) a subtree
	 */
	void delete_node(tagged_node_ptr n);

	/*
	 * internal: make_leaf -- allocates leaf structure
	 */
	obj::persistent_ptr<leaf> make_leaf(uint64_t key, string_view value);

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
	uint64_t key_to_uint64(string_view v);

	internal::radix::tree *tree;
};

} /* namespace kv */
} /* namespace pmem */
