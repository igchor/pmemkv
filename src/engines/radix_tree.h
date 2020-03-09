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

#pragma once

#include "../pmemobj_engine.h"
#include <libpmemobj++/persistent_ptr.hpp>
#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/transaction.hpp>

#define util_mssb_index64(value) ((unsigned char)(63 - __builtin_clzll(value)))

namespace pmem
{
namespace kv
{
namespace internal
{
namespace radix_tree
{

/*
 * STRUCTURE DESCRIPTION
 *
 * Critnib is a hybrid between a radix tree and DJ Bernstein's critbit:
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

/*
 * CONCURRENCY ISSUES
 *
 * Reads are completely lock-free sync-free, but only almost wait-free:
 * if for some reason a read thread gets pathologically stalled, it will
 * notice the data being stale and restart the work.  In usual cases,
 * the structure having been modified does _not_ cause a restart.
 *
 * Writes could be easily made lock-free as well (with only a cmpxchg
 * sync), but this leads to problems with removes.  A possible solution
 * would be doing removes by overwriting by NULL w/o freeing -- yet this
 * would lead to the structure growing without bounds.  Complex per-node
 * locks would increase concurrency but they slow down individual writes
 * enough that in practice a simple global write lock works faster.
 *
 * Removes are the only operation that can break reads.  The structure
 * can do local RCU well -- the problem being knowing when it's safe to
 * free.  Any synchronization with reads would kill their speed, thus
 * instead we have a remove count.  The grace period is DELETED_LIFE,
 * after which any read will notice staleness and restart its work.
 */

/*
 * A node that has been deleted is left untouched for this many delete
 * cycles.  Reads have guaranteed correctness if they took no longer than
 * DELETED_LIFE concurrent deletes, otherwise they notice something is
 * wrong and restart.  The memory of deleted nodes is never freed to
 * malloc nor their pointers lead anywhere wrong, thus a stale read will
 * (temporarily) get a wrong answer but won't crash.
 *
 * There's no need to count writes as they never interfere with reads.
 *
 * Allowing stale reads (of arbitrarily old writes or of deletes less than
 * DELETED_LIFE old) might sound counterintuitive, but it doesn't affect
 * semantics in any way: the thread could have been stalled just after
 * returning from our code.  Thus, the guarantee is: the result of get() or
 * find_le() is a value that was current at any point between the call
 * start and end.
 */
#define DELETED_LIFE 16

#define SLICE 4
#define NIB ((1ULL << SLICE) - 1)
#define SLNODES (1 << SLICE)

typedef unsigned char sh_t;

// template <typename T>
// struct self_relative_pointer
// {

// private:

// };


struct critnib_node;
struct critnib_leaf;

struct tagged_node_ptr : public obj::persistent_ptr_base
{
	tagged_node_ptr() = default;

	tagged_node_ptr(const obj::persistent_ptr<critnib_leaf> &ptr): obj::persistent_ptr_base({ptr.raw().pool_uuid_lo, ptr.raw().off | 1})
	{
	}

	tagged_node_ptr(const obj::persistent_ptr<critnib_node> &ptr): obj::persistent_ptr_base(ptr)
	{
	}

	tagged_node_ptr& operator=(const tagged_node_ptr& rhs)
	{
		obj::persistent_ptr_base::operator=(static_cast<const obj::persistent_ptr_base&>(rhs));
		return *this;
	}

	tagged_node_ptr& operator=(std::nullptr_t)
	{
		obj::persistent_ptr_base::operator=(nullptr);
		return *this;
	}

	tagged_node_ptr& operator=(const obj::persistent_ptr<critnib_leaf>& rhs)
	{
		return this->operator=(tagged_node_ptr(rhs));
	}

	tagged_node_ptr& operator=(const obj::persistent_ptr<critnib_node> &rhs)
	{
		return this->operator=(tagged_node_ptr(rhs));
	}

	bool is_leaf()
	{
		return raw().off & 1;
	}

	obj::persistent_ptr<critnib_leaf> get_leaf()
	{
		return obj::persistent_ptr<critnib_leaf>({raw().pool_uuid_lo, raw().off & ~1ULL});
	}

	obj::persistent_ptr<critnib_node> get_node()
	{
		return obj::persistent_ptr<critnib_node>(raw());
	}

	explicit operator bool() const noexcept
	{
		return oid.off != 0;
	}
};

struct critnib_node {
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

struct critnib_leaf {
	template <typename... Args>
	critnib_leaf(uint64_t key, Args &&... args): 
		key(key), value(std::forward<Args>(args)...)
	{
	}

	obj::p<uint64_t> key;
	void *value;
	//obj::string value;
};

class pmem_radix {
public:
	pmem_radix()
	{
	}

	~pmem_radix()
	{
		if (root)
			delete_node(root);
	}

	/*
	* crinib_insert -- write a key:value pair to the critnib structure
	*
	* Returns:
	*  • 0 on success
	*  • EEXIST if such a key already exists
	*  • ENOMEM if we're out of memory
	*
	* Takes a global write lock but doesn't stall any readers.
	*/
	int
	insert(uint64_t key, void* value)
	{
		auto pop = obj::pool_base(pmemobj_pool_by_ptr(this));

			auto n = root;
			if (!n) {
				obj::transaction::run(pop, [&]{
					root = obj::make_persistent<critnib_leaf>(key, value);;
				});
				return 0;
			}

			auto *parent = &root;
			auto prev = root;

			while (n && !n.is_leaf() && (key & path_mask(n.get_node()->shift)) == n.get_node()->path) {
				prev = n;
				parent = &n.get_node()->child[slice_index(key, n.get_node()->shift)];
				n = *parent;
			}

			if (!n) {
				n = prev;
				obj::transaction::run(pop, [&]{
					n.get_node()->child[slice_index(key, n.get_node()->shift)] = obj::make_persistent<critnib_leaf>(key, value);;
				});
				return 0;
			}

			uint64_t path = n.is_leaf() ? n.get_leaf()->key : n.get_node()->path;
			/* Find where the path differs from our key. */
			uint64_t at = path ^ key;
			if (!at) {
				assert(n.is_leaf());
				n.get_leaf()->value = value;

				return 0;
				// XXX - what should be the strategy?
				// obj::delete_persistent<critnib_leaf>(leaf_ptr);
				// return EEXIST;
			}

			/* and convert that to an index. */
			sh_t sh = util_mssb_index64(at) & (sh_t)~(SLICE - 1);

		obj::transaction::run(pop, [&]{
			auto m = obj::make_persistent<critnib_node>();
			m->child[slice_index(key, sh)] = obj::make_persistent<critnib_leaf>(key, value);;
			m->child[slice_index(path, sh)] = n;
			m->shift = sh;
			m->path = key & path_mask(sh);
			*parent = m;
		});

		return 0;
	}

	/*
	* critnib_get -- query for a key ("==" match), returns value or NULL
	*
	* Doesn't need a lock but if many deletes happened while our thread was
	* somehow stalled the query is restarted (as freed nodes remain unused only
	* for a grace period).
	*
	* Counterintuitively, it's pointless to return the most current answer,
	* we need only one that was valid at any point after the call started.
	*/
	void*
	get(uint64_t key)
	{
			auto n = root;

			/*
			* critbit algorithm: dive into the tree, looking at nothing but
			* each node's critical bit^H^H^Hnibble.  This means we risk
			* going wrong way if our path is missing, but that's ok...
			*/
			while (n && !n.is_leaf())
				n = n.get_node()->child[slice_index(key, n.get_node()->shift)];

			return (n && n.get_leaf()->key == key) ? &n.get_leaf()->value : nullptr;
	}

	/*
	* critnib_remove -- delete a key from the critnib structure, return its value
	*/
	bool
	remove(uint64_t key)
	{
		auto pop = obj::pool_base(pmemobj_pool_by_ptr(this));

		auto n = root;
		if (!n)
			return false;

		if (n.is_leaf()) {
			if (n.get_leaf()->key == key) {
				obj::transaction::run(pop, [&]{
					root = nullptr;
					obj::delete_persistent<critnib_leaf>(n.get_leaf());
				});

				return true;
			}

			return false;
		}

		/*
		* n and k are a parent:child pair (after the first iteration); k is the
		* leaf that holds the key we're deleting.
		*/
		auto *k_parent = &root;
		auto *n_parent = &root;
		auto kn = n;

		while (!kn.is_leaf()) {
			n_parent = k_parent;
			n = kn;
			k_parent = &kn.get_node()->child[slice_index(key, kn.get_node()->shift)];
			kn = *k_parent;

			if (!kn)
				return false;
		}

		if (kn.get_leaf()->key != key)
			return false;

		obj::transaction::run(pop, [&]{
			obj::delete_persistent<critnib_leaf>(kn.get_leaf());

			n.get_node()->child[slice_index(key, n.get_node()->shift)] = nullptr;

			/* Remove the node if there's only one remaining child. */
			int ochild = -1;
			for (int i = 0; i < SLNODES; i++) {
				if (n.get_node()->child[i]) {
					if (ochild != -1)
						return;

					ochild = i;
				}
			}

			assert(ochild != -1);
			*n_parent = n.get_node()->child[ochild];
		});

		return true;
	}


private:
	tagged_node_ptr root;

	/*
	* internal: path_mask -- return bit mask of a path above a subtree [shift]
	* bits tall
	*/
	static inline uint64_t
	path_mask(sh_t shift)
	{
		return ~NIB << shift;
	}

	/*
	* internal: slice_index -- return index of child at the given nib
	*/
	static inline unsigned
	slice_index(uint64_t key, sh_t shift)
	{
		return (unsigned)((key >> shift) & NIB);
	}

	/*
	* internal: delete_node -- recursively free (to malloc) a subtree
	*/
	static void
	delete_node(tagged_node_ptr n)
	{
		if (!n.is_leaf()) {
			for (int i = 0; i < SLNODES; i++) {
				if (n.get_node()->child[i])
					delete_node(n.get_node()->child[i]);
			}
			obj::delete_persistent<critnib_node>(n.get_node());
		} else {
			obj::delete_persistent<critnib_leaf>(n.get_leaf());
		}
	}
};

// /*
//  * internal: find_successor -- return the rightmost non-null node in a subtree
//  */
// static void *
// find_successor(struct critnib_node *__restrict n)
// {
// 	while (1) {
// 		int nib;
// 		for (nib = NIB; nib >= 0; nib--)
// 			if (n->child[nib])
// 				break;

// 		if (nib < 0)
// 			return NULL;

// 		n = n->child[nib];
// 		if (is_leaf(n))
// 			return to_leaf(n)->value;
// 	}
// }

// /*
//  * internal: find_le -- recursively search <= in a subtree
//  */
// static void *
// find_le(struct critnib_node *__restrict n, uint64_t key)
// {
// 	if (!n)
// 		return NULL;

// 	if (is_leaf(n)) {
// 		struct critnib_leaf *k = to_leaf(n);

// 		return (k->key <= key) ? k->value : NULL;
// 	}

// 	/*
// 	 * is our key outside the subtree we're in?
// 	 *
// 	 * If we're inside, all bits above the nib will be identical; note
// 	 * that shift points at the nib's lower rather than upper edge, so it
// 	 * needs to be masked away as well.
// 	 */
// 	if ((key ^ n->path) >> (n->shift) & ~NIB) {
// 		/*
// 		 * subtree is too far to the left?
// 		 * -> its rightmost value is good
// 		 */
// 		if (n->path < key)
// 			return find_successor(n);

// 		/*
// 		 * subtree is too far to the right?
// 		 * -> it has nothing of interest to us
// 		 */
// 		return NULL;
// 	}

// 	unsigned nib = slice_index(key, n->shift);
// 	/* recursive call: follow the path */
// 	{
// 		struct critnib_node *m;
// 		load(&n->child[nib], &m);
// 		void *value = find_le(m, key);
// 		if (value)
// 			return value;
// 	}

// 	/*
// 	 * nothing in that subtree?  We strayed from the path at this point,
// 	 * thus need to search every subtree to our left in this node.  No
// 	 * need to dive into any but the first non-null, though.
// 	 */
// 	for (; nib > 0; nib--) {
// 		struct critnib_node *m;
// 		load(&n->child[nib - 1], &m);
// 		if (m) {
// 			n = m;
// 			if (is_leaf(n))
// 				return to_leaf(n)->value;

// 			return find_successor(n);
// 		}
// 	}

// 	return NULL;
// }

// /*
//  * critnib_find_le -- query for a key ("<=" match), returns value or NULL
//  *
//  * Same guarantees as critnib_get().
//  */
// void *
// critnib_find_le(struct critnib *c, uint64_t key)
// {
// 	uint64_t wrs1, wrs2;
// 	void *res;

// 	do {
// 		load(&c->remove_count, &wrs1);
// 		struct critnib_node *n; /* avoid a subtle TOCTOU */
// 		load(&c->root, &n);
// 		res = n ? find_le(n, key) : NULL;
// 		load(&c->remove_count, &wrs2);
// 	} while (wrs1 + DELETED_LIFE <= wrs2);

// 	return res;
// }

}
}

class radix_tree : public pmemobj_engine_base<internal::radix_tree::pmem_radix> {
public:
	radix_tree(std::unique_ptr<internal::config> cfg)
	: pmemobj_engine_base(cfg)
	{
		Recover();
	}

	~radix_tree()
	{

	}

	radix_tree(const radix_tree &) = delete;
	radix_tree &operator=(const radix_tree &) = delete;

	std::string name() final
	{
		return "radix_tree";
	}

	// status count_all(std::size_t &cnt) final;

	// status get_all(get_kv_callback *callback, void *arg) final;

	status exists(string_view key) final
	{
		return tree->get(convert_to_uint64(key)) == nullptr ? status::NOT_FOUND : status::OK;
	}

	status get(string_view key, get_v_callback *callback, void *arg) final
	{
		auto *value = tree->get(convert_to_uint64(key));
		if (!value)
			return status::NOT_FOUND;
			
		callback((const char*)value, 8, arg);

		return status::OK;
	}

	status put(string_view key, string_view value) final
	{
		tree->insert(convert_to_uint64(key), (void*)value.data());

		return status::OK;
	}

	status remove(string_view key) final
	{
		return tree->remove(convert_to_uint64(key)) ? status::OK : status::NOT_FOUND;
	}

	// status defrag(double start_percent, double amount_percent) final;

private:
	uint64_t convert_to_uint64(string_view v)
	{
		if (v.size() > sizeof(uint64_t))
			throw internal::invalid_argument("XXX");

		return *((uint64_t*)v.data());
	}

	void Recover()
	{
		if (!OID_IS_NULL(*root_oid)) {
			tree = (pmem::kv::internal::radix_tree::pmem_radix *)pmemobj_direct(*root_oid);
		} else {
			pmem::obj::transaction::run(pmpool, [&] {
				pmem::obj::transaction::snapshot(root_oid);
				*root_oid =
					pmem::obj::make_persistent<internal::radix_tree::pmem_radix>().raw();
				tree = (pmem::kv::internal::radix_tree::pmem_radix *)pmemobj_direct(
					*root_oid);
			});
		}
	}

	internal::radix_tree::pmem_radix *tree;
};

} /* namespace kv */
} /* namespace pmem */
