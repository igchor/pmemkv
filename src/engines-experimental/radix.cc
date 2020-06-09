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

tree::leaf::leaf(string_view key, string_view value): key_(key.data(), key.size()), value(value.data(), value.size()) {}
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
	return string_view(key_.c_str(), key_.size());
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

struct actions {
	actions(obj::pool_base pop, uint64_t pool_id, std::size_t cap = 4)
	    : pop(pop), pool_id(pool_id), mtx(pop)
	{
		// acts.reserve(cap);
	}

	void set(uint64_t *what, uint64_t value)
	{
		// acts.emplace_back();
		// pmemobj_set_value(pop.handle(), &acts.back(), what, value);
		*what = value;
	}

	void free(uint64_t off)
	{
		off = off & ~1ULL;

		if (!off)
			return;

		pmemobj_tx_free({pool_id, off});

		// acts.emplace_back();
		// pmemobj_defer_free(pop.handle(), PMEMoid{pool_id, off}, &acts.back());
	}

	template <typename T, typename... Args>
	obj::persistent_ptr<T> make(uint64_t size, Args &&... args)
	{
		// acts.emplace_back();
		// obj::persistent_ptr<T> ptr =
		// 	pmemobj_reserve(pop.handle(), &acts.back(), size, 0);

		// new (ptr.get()) T(std::forward<Args>(args)...);

		auto ptr = pmem::obj::persistent_ptr<T>(pmemobj_tx_alloc(size, 0));

		new(ptr.get()) T(std::forward<Args>(args)...);

		return ptr;
	}

	void publish()
	{
		// /* XXX - this probably won't work if there is no operation on std::atomic
		//  */
		// std::atomic_thread_fence(std::memory_order_release);

		// if (pmemobj_publish(pop.handle(), acts.data(), acts.size()))
		// 	throw std::runtime_error("XXX");

		pmem::obj::transaction::commit();
	}

private:
	std::vector<pobj_action> acts;
	obj::pool_base pop;
	uint64_t pool_id;

	pmem::obj::transaction::manual mtx;
};

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
		auto nn = n->child[slice_index(key.data()[n->byte],  n->bit)];

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
	actions acts(pop, pool_id);

	iterate([](const char*, size_t, const char*, size_t, void*){return 0;}, nullptr);

	tagged_node_ptr new_leaf = acts.make<tree::leaf>(
			sizeof(tree::leaf) + value.size() + key.size(), key, value);

	if (!root) {
		acts.set((uint64_t *)&root, new_leaf.offset());
		acts.set(&size_, size_ + 1);
		acts.publish();

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

	bitn_t sh = std::numeric_limits<bitn_t>::max(); // XXX - the loop below will stop if we find node with exactly matching nbyte and nbit
	if (diff < leaf->key().size() && diff < key.size()) {
		unsigned char at = leaf->key().data()[diff] ^ key.data()[diff];
		sh = utils::mssb_index((uint32_t)at) & (bitn_t)~(SLICE - 1);
	}

	auto byte = 0;
	while (n && !n.is_leaf() &&
		(n->byte < diff || (n->byte == diff && n->bit >= sh))) {
		assert(n->byte * 8 + (8 - n->bit) > byte);
		assert(n->bit == 0 || n->bit == 4);
		byte = n->byte * 8 + (8 - n->bit) > byte;

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

		acts.set((uint64_t *)parent, new_leaf.offset());
		acts.set(&size_, size_ + 1);
		acts.publish();
		return;
	}

	/* New key is a prefix of the leaf key or they are equal. We need to add leaf ptr to internal node. */
	if (diff == key.size()) {
		if ((!n.is_leaf() && n->byte != diff) || (!n.is_leaf() && n->bit != 0) || (n.is_leaf() && n.get_leaf(pool_id)->key().compare(key) != 0)) {
			/* We have to add new node at the edge from parent to n */
			tagged_node_ptr node = acts.make<tree::node>(sizeof(tree::node));
			node->child[slice_index(leaf->key().data()[diff], 0)] = n;
			node->leaf =  new_leaf;
			node->byte = diff;
			node->bit = 0;

			acts.set((uint64_t *)parent, node.offset());
			acts.set(&size_, size_ + 1);
			acts.publish();

			return;
		}
		
		if (n.is_leaf() ){
			/* Update of existing leaf. */
			acts.free(n.offset());
			acts.set((uint64_t*) parent, new_leaf.offset());
			acts.publish();

			return;
		}
			assert(!n->leaf || n->leaf.get_leaf(pool_id)->key().compare(key) == 0);

			/* Update or insert in internal node */
			if (n->leaf)
				acts.free(n->leaf.offset());
			else
				acts.set(&size_, size_ + 1);

			acts.set((uint64_t*) &n->leaf, new_leaf.offset());
			acts.publish();

			return;
	
	}

	if (diff == leaf->key().size()) {
		/* Leaf key is a prefix of the new key. We need to convert leaf to a node. */

		tagged_node_ptr node = acts.make<tree::node>(sizeof(tree::node));
		node->child[slice_index(key.data()[diff], 0)] = new_leaf;
		node->leaf =  n;
		node->byte = diff;
		node->bit = 0;

		acts.set((uint64_t *)parent, node.offset());
		acts.set(&size_, size_ + 1);
		acts.publish();

		return;
	}

	/* If not, we need to insert a new node in the middle of an edge. */
	tagged_node_ptr node = acts.make<tree::node>(sizeof(tree::node));

	node->child[slice_index(leaf->key().data()[diff], sh)] = n;
	node->child[slice_index(key.data()[diff], sh)] =  new_leaf;
	node->byte = diff;
	node->bit = sh;

	acts.set((uint64_t *)parent, node.offset());
	acts.set(&size_, size_ + 1);
	acts.publish();
}

bool tree::get(string_view key, pmemkv_get_v_callback *cb, void *arg)
{
	iterate([](const char*, size_t, const char*, size_t, void*){return 0;}, nullptr);

	auto n = root;
	auto byte = 0;
	while (n && !n.is_leaf()) {
		assert(n->byte * 8 + (8 - n->bit) > byte);
		assert(n->bit == 0 || n->bit == 4);
		byte = n->byte * 8 + (8 - n->bit) > byte;
		if (n->byte == key.size() && n->bit == 0)
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
	actions acts(pop, pool_id);

	auto n = root;
	auto *parent = &root;
	decltype(parent) pp = nullptr;

	iterate([](const char*, size_t, const char*, size_t, void*){return 0;}, nullptr);

	auto byte = 0;
	while (n && !n.is_leaf()) {
		assert(n->byte * 8 + (8 - n->bit) > byte);
		assert(n->bit == 0 || n->bit == 4);
		byte = n->byte * 8 + (8 - n->bit) > byte;
		pp = parent;

		if (n->byte == key.size() && n->bit == 0)
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
	// XXX turn this comparison into helper function
	if (key.size() != leaf->key().size() || key.compare(leaf->key()) != 0)
		return false;

	acts.free(n.offset());
	acts.set((uint64_t *)parent, 0);
	acts.set(&size_, size_ - 1);

	/* was root */
	if (!pp) {
		acts.publish();
		iterate([](const char*, size_t, const char*, size_t, void*){return 0;}, nullptr);
		return true;
	}

		n = *pp;
		tagged_node_ptr only_child = nullptr;
		for (int i = 0; i < (int)SLNODES; i++) {
			auto &child = n->child[i];
			if (child && &child != parent) {
				if (only_child) {
					/* more than one child */
					acts.publish();
					iterate([](const char*, size_t, const char*, size_t, void*){return 0;}, nullptr);
					return true;
				}
				only_child = n->child[i];
			}
		}

		if (only_child && n->leaf && &n->leaf != parent) {
			/* there are actually 2 "childred" */
			acts.publish();
			iterate([](const char*, size_t, const char*, size_t, void*){return 0;}, nullptr);
			return true;
		} else if (n->leaf && &n->leaf != parent)
			only_child = n->leaf;

	assert(only_child);

	acts.set((uint64_t *)pp, only_child.offset());
	acts.free(n.offset());

	acts.publish();

	iterate([](const char*, size_t, const char*, size_t, void*){return 0;}, nullptr);

	return true;
}

void tree::iterate_rec(tree::tagged_node_ptr n, pmemkv_get_kv_callback *callback,
		       void *arg)
{
	if (!n.is_leaf()) {
		assert(n_child(n) + bool((n)->leaf) > 1);
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

tree::tagged_node_ptr::tagged_node_ptr(uint64_t off)
{
	this->off = off;
}

tree::tagged_node_ptr::tagged_node_ptr(const obj::persistent_ptr<leaf> &ptr)
{
	off = (ptr.raw().off | 1);
}

tree::tagged_node_ptr::tagged_node_ptr(const obj::persistent_ptr<node> &ptr)
{
	off = ptr.raw().off;
}

tree::tagged_node_ptr &tree::tagged_node_ptr::operator=(std::nullptr_t)
{
	off = 0;
	return *this;
}

tree::tagged_node_ptr &
tree::tagged_node_ptr::operator=(const obj::persistent_ptr<leaf> &rhs)
{
	off = (rhs.raw().off | 1);
	return *this;
}

tree::tagged_node_ptr &
tree::tagged_node_ptr::operator=(const obj::persistent_ptr<node> &rhs)
{
	off = rhs.raw().off;
	return *this;
}

bool tree::tagged_node_ptr::operator==(const tree::tagged_node_ptr &rhs) const
{
	return off == rhs.off;
}

bool tree::tagged_node_ptr::operator!=(const tree::tagged_node_ptr &rhs) const
{
	return !(*this == rhs);
}

bool tree::tagged_node_ptr::is_leaf() const
{
	return off & 1;
}

uint64_t tree::tagged_node_ptr::offset() const
{
	return off;
}

tree::leaf *tree::tagged_node_ptr::get_leaf(uint64_t pool_id) const
{
	assert(is_leaf());
	return (tree::leaf *)pmemobj_direct({pool_id, off & ~1ULL});
}

tree::node *tree::tagged_node_ptr::get_node(uint64_t pool_id) const
{
	assert(!is_leaf());
	return (tree::node *)pmemobj_direct({pool_id, off});
}

tree::tagged_node_ptr::operator bool() const noexcept
{
	return off != 0;
}

tree::node *tree::tagged_node_ptr::operator->() const noexcept
{
	return get_node(g_pool_id);
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
		key, [](const char *v, size_t size, void *arg) {},
		nullptr);

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
