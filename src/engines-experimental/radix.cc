// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2020, Intel Corporation */

#include "radix.h"
#include "../out.h"
#include "../utils.h"

#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/pext.hpp>
#include <libpmemobj++/transaction.hpp>

#include <libpmemobj/action_base.h>

#include <mutex>
#include <shared_mutex>
#include <vector>

namespace pmem
{
namespace kv
{

namespace internal
{
namespace radix
{

static uint64_t g_pool_id;

tree::leaf::leaf(string_view key, string_view value)
    : ksize(key.size()), value(value.data(), value.size())
{
	auto *ptr = reinterpret_cast<char *>(this + 1);

	std::memcpy(ptr, key.data(), key.size());
}
//: ksize(key.size()), vsize(value.size())
// {
// 	std::memcpy(this->data_rw(), key.data(), key.size());
// 	// XXX - alignment
// 	std::memcpy(this->data_rw() + key.size(), value.data(), value.size());
// }

const char *tree::leaf::data() const noexcept
{
	return value.c_str();
}

string_view tree::leaf::key() const noexcept
{
	auto *ptr = reinterpret_cast<const char *>(this + 1);

	return string_view(ptr, ksize);
}

char *tree::leaf::data_rw()
{
	return value.data();

	// auto *ptr = reinterpret_cast<char *>(this + 1);

	// return ptr;
}

tree::tree() : root(nullptr), size_(0)
{
	pool_id = pmemobj_oid(this).pool_uuid_lo;

	g_pool_id = pool_id; // XXx:
}

tree::~tree()
{
	if (root)
		delete_node(root);
}

uint64_t tree::size()
{
	return this->size_;
}

/*
 * any_leaf -- (internal) find any leaf in a subtree
 *
 * We know they're all identical up to the divergence point between a prefix
 * shared by all of them vs the new key we're inserting.
 */
tree::leaf *tree::any_leaf(tagged_node_ptr n)
{
	for (int i = 0; i < SLNODES; i++) {
		tagged_node_ptr m;
		if ((m = n.get_node(pool_id)->child[i]))
			return m.is_leaf() ? m.get_leaf(pool_id) : any_leaf(m);
	}
	assert(false);
}

tree::leaf *tree::descend(string_view key)
{
	auto n = root;

	while (!n.is_leaf() && n->byte < key.size()) {
		auto nn = n->child[slice_index(key.data()[n->byte], n->bit)];

		if (nn)
			n = nn;
		else {
			n = any_leaf(n);
			break;
		}
	}

	if (!n.is_leaf())
		n = any_leaf(n);

	return n.get_leaf(pool_id);
}

static byten_t prefix_diff(string_view lhs, string_view rhs)
{
	byten_t diff;
	for (diff = 0; diff < std::min(lhs.size(), rhs.size()); diff++) {
		if (lhs.data()[diff] != rhs.data()[diff])
			return diff;
	}

	return diff;
}

void tree::insert(obj::pool_base &pop, string_view key, string_view value)
{
	if (!root) {
		obj::transaction::run(pop, [&] { root = make_leaf(key, value); });
		return;
	}

	/*
	 * Need to descend the tree twice: first to find a leaf that
	 * represents a subtree whose all keys share a prefix at least as
	 * long as the one common to the new key and that subtree.
	 */
	auto leaf = descend(key);
	auto diff = prefix_diff(key, leaf->key());

	/* Descend into the tree again. */
	auto n = root;
	auto parent = &root;
	auto prev = n;

	auto min_key_len = std::min(leaf->key().size(), key.size());

	bitn_t sh = 8 - SLICE; // XXX 4 to some constant
	if (diff < leaf->key().size() && diff < key.size()) {
		unsigned char at = leaf->key().data()[diff] ^ key.data()[diff];
		sh = utils::mssb_index((uint32_t)at) & (bitn_t) ~(SLICE - 1);
	}

	while (n && !n.is_leaf() &&
	       (n->byte < diff ||
		(n->byte == diff &&
		 (n->bit > sh || n->bit == sh && diff < min_key_len)))) {

		prev = n;
		parent = &n->child[slice_index(key.data()[n->byte], n->bit)];
		n = *parent;
	}

	/*
	 * If the divergence point is at same nib as an existing node, and
	 * the subtree there is empty, just place our leaf there and we're
	 * done.  Obviously this can't happen if SLICE == 1.
	 */
	if (!n) {
		assert(diff < leaf->key().size() && diff < key.size());

		obj::transaction::run(pop, [&] { *parent = make_leaf(key, value); });

		return;
	}

	/* New key is a prefix of the leaf key or they are equal. We need to add leaf ptr
	 * to internal node. */
	if (diff == key.size()) {
		if (n.is_leaf() && n.get_leaf(pool_id)->key().size() == key.size()) {
			/* Update the existing leaf. */
			obj::transaction::run(pop, [&] {
				//*parent = make_leaf(key, value.c_str(), value.size());
				// free(n);
				n.get_leaf(pool_id)->value.assign(value.data(),
								  value.size());
			});

			return;
		}

		if (!n.is_leaf() && n->byte == key.size() && n->bit == 4) {
			obj::transaction::run(pop, [&] {
				/* Update or insert in internal node */
				if (n->leaf)
					n->leaf.get_leaf(pool_id)->value.assign(
						value.data(), value.size());
				else
					n->leaf = make_leaf(key, value);
			});

			return;
		}

		obj::transaction::run(pop, [&] {
			/* We have to add new node at the edge from parent to n */
			tagged_node_ptr node = obj::make_persistent<tree::node>();
			node->child[slice_index(leaf->key().data()[diff], sh)] = n;
			node->leaf = make_leaf(key, value);
			node->byte = diff;
			node->bit = sh;

			*parent = node;
		});

		return;
	}

	if (diff == leaf->key().size()) {
		/* Leaf key is a prefix of the new key. We need to convert leaf to a node.
		 */
		obj::transaction::run(pop, [&] {
			/* We have to add new node at the edge from parent to n */
			tagged_node_ptr node = obj::make_persistent<tree::node>();
			node->child[slice_index(key.data()[diff], sh)] =
				make_leaf(key, value);
			node->leaf = n;
			node->byte = diff;
			node->bit = sh;

			*parent = node;
		});

		return;
	}

	obj::transaction::run(pop, [&] {
		/* We have to add new node at the edge from parent to n */
		tagged_node_ptr node = obj::make_persistent<tree::node>();
		node->child[slice_index(leaf->key().data()[diff], sh)] = n;
		node->child[slice_index(key.data()[diff], sh)] = make_leaf(key, value);
		node->byte = diff;
		node->bit = sh;

		*parent = node;
	});
}

bool tree::get(string_view key, pmemkv_get_v_callback *cb, void *arg)
{
	auto n = root;
	auto prev = n;
	while (n && !n.is_leaf()) {
		prev = n;
		if (n->byte == key.size() && n->bit == 4)
			n = n->leaf;
		else if (n->byte > key.size())
			return false;
		else
			n = n->child[slice_index(key.data()[n->byte], n->bit)];
	}

	if (!n)
		return false;

	auto leaf = n.get_leaf(pool_id);
	if (key.compare(leaf->key()) != 0)
		return false;

	cb(leaf->data(), leaf->value.size(), arg);

	return true;
}

bool tree::remove(obj::pool_base &pop, string_view key)
{
	auto n = root;
	auto *parent = &root;
	decltype(parent) pp = nullptr;

	while (n && !n.is_leaf()) {
		pp = parent;

		if (n->byte == key.size() && n->bit == 4)
			parent = &n->leaf;
		else if (n->byte > key.size())
			return false;
		else
			parent = &n->child[slice_index(key.data()[n->byte], n->bit)];

		n = *parent;
	}

	if (!n)
		return false;

	auto leaf = n.get_leaf(pool_id);
	if (!keys_equal(key, leaf->key()))
		return false;

	obj::transaction::run(pop, [&] {
		obj::delete_persistent<tree::leaf>(leaf);

		*parent = 0;
		size_--;

		/* was root */
		if (!pp)
			return;

		n = *pp;
		tagged_node_ptr only_child = nullptr;
		for (int i = 0; i < (int)SLNODES; i++) {
			if (n->child[i]) {
				if (only_child) {
					/* more than one child */
					return;
				}
				only_child = n->child[i];
			}
		}

		if (only_child && n->leaf && &n->leaf != parent) {
			/* there are actually 2 "childred" */
			return;
		} else if (n->leaf && &n->leaf != parent)
			only_child = n->leaf;

		assert(only_child);

		obj::delete_persistent<tree::node>(n.get_node(pool_id));
		*pp = only_child;
	});

	return true;
}

void tree::iterate_rec(tree::tagged_node_ptr n, pmemkv_get_kv_callback *callback,
		       void *arg)
{
	if (!n.is_leaf()) {
		assert(n_child(n) + bool((n)->leaf) > 1);

		if (n->leaf)
			iterate_rec(n->leaf, callback, arg);

		for (int i = 0; i < (int)SLNODES; i++) {
			if (n->child[i])
				iterate_rec(n->child[i], callback, arg);
		}
	} else {
		auto leaf = n.get_leaf(pool_id);
		callback(leaf->key().data(), leaf->key().size(), leaf->data(),
			 leaf->value.size(), arg);
	}
}

void tree::iterate(pmemkv_get_kv_callback *callback, void *arg)
{
	if (root)
		iterate_rec(root, callback, arg);
}

unsigned tree::slice_index(char b, uint8_t bit)
{
	return (b >> bit) & NIB;
}

void tree::delete_node(tree::tagged_node_ptr n)
{
	assert(pmemobj_tx_stage() == TX_STAGE_WORK);

	if (!n.is_leaf()) {
		if (n->leaf)
			delete_node(n->leaf);

		for (int i = 0; i < (int)SLNODES; i++) {
			if (n->child[i])
				delete_node(n.get_node(pool_id)->child[i]);
		}
		obj::delete_persistent<tree::node>(n.get_node(pool_id));
	} else {
		size_--;
		obj::delete_persistent<tree::leaf>(n.get_leaf(pool_id));
	}
}

tree::tagged_node_ptr::tagged_node_ptr(std::nullptr_t)
{
	this->off = 0;
}

tree::tagged_node_ptr::tagged_node_ptr()
{
	this->off = 0;
}

tree::tagged_node_ptr::tagged_node_ptr(const tagged_node_ptr &rhs)
{
	if (!rhs) {
		this->off = 0;
	} else {
		this->off = rhs.get<uint64_t>() - reinterpret_cast<uint64_t>(this);
		off |= unsigned(rhs.is_leaf());
	}
}

tree::tagged_node_ptr &tree::tagged_node_ptr::operator=(const tagged_node_ptr &rhs)
{
	if (!rhs) {
		this->off = 0;
	} else {
		this->off = rhs.get<uint64_t>() - reinterpret_cast<uint64_t>(this);
		off |= unsigned(rhs.is_leaf());
	}

	return *this;
}

tree::tagged_node_ptr::tagged_node_ptr(const obj::persistent_ptr<leaf> &ptr)
{
	if (!ptr) {
		this->off = 0;
	} else {
		off = reinterpret_cast<uint64_t>(ptr.get()) -
			reinterpret_cast<uint64_t>(this);
		off |= 1;
	}
}

tree::tagged_node_ptr::tagged_node_ptr(const obj::persistent_ptr<node> &ptr)
{
	if (!ptr) {
		this->off = 0;
	} else {
		off = reinterpret_cast<uint64_t>(ptr.get()) -
			reinterpret_cast<uint64_t>(this);
	}
}

tree::tagged_node_ptr &tree::tagged_node_ptr::operator=(std::nullptr_t)
{
	this->off = 0;

	return *this;
}

tree::tagged_node_ptr &
tree::tagged_node_ptr::operator=(const obj::persistent_ptr<leaf> &rhs)
{
	if (!rhs) {
		this->off = 0;
	} else {
		off = reinterpret_cast<uint64_t>(rhs.get()) -
			reinterpret_cast<uint64_t>(this);
		off |= 1;
	}

	return *this;
}

tree::tagged_node_ptr &
tree::tagged_node_ptr::operator=(const obj::persistent_ptr<node> &rhs)
{
	if (!rhs) {
		this->off = 0;
	} else {
		off = reinterpret_cast<uint64_t>(rhs.get()) -
			reinterpret_cast<uint64_t>(this);
	}

	return *this;
}

bool tree::tagged_node_ptr::operator==(const tree::tagged_node_ptr &rhs) const
{
	return get<uint64_t>() == rhs.get<uint64_t>() || (!*this && !rhs);
}

bool tree::tagged_node_ptr::operator!=(const tree::tagged_node_ptr &rhs) const
{
	return !(*this == rhs);
}

bool tree::tagged_node_ptr::is_leaf() const
{
	return off & 1;
}

tree::leaf *tree::tagged_node_ptr::get_leaf(uint64_t pool_id) const
{
	assert(is_leaf());
	return get<tree::leaf *>();
}

tree::node *tree::tagged_node_ptr::get_node(uint64_t pool_id = 0) const
{
	assert(!is_leaf());
	return get<tree::node *>();
}

tree::tagged_node_ptr::operator bool() const noexcept
{
	return (off & ~uint64_t(1)) != 0;
}

tree::node *tree::tagged_node_ptr::operator->() const noexcept
{
	return get_node();
}

} // namespace radix
} // namespace internal

radix::radix(std::unique_ptr<internal::config> cfg) : pmemobj_engine_base(cfg)
{
	if (!OID_IS_NULL(*root_oid)) {
		tree = (pmem::kv::internal::radix::tree *)pmemobj_direct(*root_oid);
	} else {
		pmem::obj::transaction::run(pmpool, [&] {
			pmem::obj::transaction::snapshot(root_oid);
			*root_oid =
				pmem::obj::make_persistent<internal::radix::tree>().raw();
			tree = (pmem::kv::internal::radix::tree *)pmemobj_direct(
				*root_oid);
		});
	}

	pmem::kv::internal::radix::g_pool_id = root_oid->pool_uuid_lo;
}

radix::~radix()
{
}

std::string radix::name()
{
	return "radix";
}

status radix::get_all(get_kv_callback *callback, void *arg)
{
	std::shared_lock<lock_type> lock(mtx);

	tree->iterate(callback, arg);

	return status::OK;
}

status radix::count_all(std::size_t &cnt)
{
	cnt = tree->size();

	return status::OK;
}

status radix::exists(string_view key)
{
	std::shared_lock<lock_type> lock(mtx);

	auto found = tree->get(
		key, [](const char *v, size_t size, void *arg) {}, nullptr);

	return found ? status::OK : status::NOT_FOUND;
}

status radix::get(string_view key, get_v_callback *callback, void *arg)
{
	std::shared_lock<lock_type> lock(mtx);

	auto found = tree->get(key, callback, arg);

	return found ? status::OK : status::NOT_FOUND;
}

status radix::put(string_view key, string_view value)
{
	std::shared_lock<lock_type> lock(mtx);

	tree->insert(pmpool, key, value);

	return status::OK;
}

status radix::remove(string_view key)
{
	std::unique_lock<lock_type> lock(mtx);

	return tree->remove(pmpool, key) ? status::OK : status::NOT_FOUND;
}

} // namespace kv
} // namespace pmem
