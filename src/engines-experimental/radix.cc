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

tree::leaf::leaf(string_view key, string_view value) : ksize(key.size()), vsize(value.size())
{
	std::memcpy(this->data_rw(), key.data(), key.size());
	// XXX - alignment
	std::memcpy(this->data_rw() + key.size(), value.data(), value.size());
}

const char *tree::leaf::data() const noexcept
{
	return key() + ksize;
}

const char *tree::leaf::key() const noexcept
{
	return reinterpret_cast<const char *>(this + 1);
}

std::size_t tree::leaf::capacity()
{
	return pmemobj_alloc_usable_size(pmemobj_oid(this)) - sizeof(this) - ksize;
}

char *tree::leaf::data_rw()
{
	auto *ptr = reinterpret_cast<char *>(this + 1);

	return ptr;
}

tree::tree() : root(nullptr), size_(0)
{
	pool_id = pmemobj_oid(this).pool_uuid_lo;
}

tree::~tree()
{
	if (root.load(std::memory_order_acquire))
		delete_node(root);
}

uint64_t tree::size()
{
	return this->size_;
}

struct actions {
	actions(obj::pool_base pop, uint64_t pool_id, std::size_t cap = 4)
	    : pop(pop), pool_id(pool_id)
	{
		acts.reserve(cap);
	}

	void set(uint64_t *what, uint64_t value)
	{
		acts.emplace_back();
		pmemobj_set_value(pop.handle(), &acts.back(), what, value);
	}

	void free(uint64_t off)
	{
		acts.emplace_back();
		pmemobj_defer_free(pop.handle(), PMEMoid{pool_id, off & ~1ULL}, &acts.back());
	}

	template <typename T, typename... Args>
	obj::persistent_ptr<T> make(uint64_t size, Args &&... args)
	{
		acts.emplace_back();
		obj::persistent_ptr<T> ptr =
			pmemobj_reserve(pop.handle(), &acts.back(), size, 0);

		new (ptr.get()) T(std::forward<Args>(args)...);

		return ptr;
	}

	void publish()
	{
		/* XXX - this probably won't work if there is no operation on std::atomic
		 */
		std::atomic_thread_fence(std::memory_order_release);

		if (pmemobj_publish(pop.handle(), acts.data(), acts.size()))
			throw std::runtime_error("XXX");
	}

	void cancel()
	{
		pmemobj_cancel(pop.handle(), acts.data(), acts.size());
	}

private:
	std::vector<pobj_action> acts;
	obj::pool_base pop;
	uint64_t pool_id;
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
	return NULL;
}

/**
 * We first find a position for a new leaf without any locks, then lock the
 * potential parent and insert/replace the leaf. Reading key from leafs
 * is always done under the parent lock.
 */
void tree::insert(obj::pool_base &pop, string_view view_key, string_view value)
{
restart : {
	actions acts(pop, pool_id);

	tagged_node_ptr leaf = acts.make<tree::leaf>(
			sizeof(tree::leaf) + value.size() + view_key.size(), view_key, value);

	auto key = (const char*) &leaf.get_leaf(pool_id)->ksize;
	auto key_len = view_key.size() + sizeof(uint64_t);

	auto n = root.load(std::memory_order_acquire);
	auto *parent = &root;

	if (!n) {
		// std::unique_lock<obj::shared_mutex> lock(root_mtx);

		// if (n != root.load(std::memory_order_relaxed))
		// 	goto restart;

		// XXX - move size to TLS
		size_++;

		acts.set((uint64_t *)&root, leaf.offset());
		acts.publish();

		return;
	}

	while (!n.is_leaf() && n.get_node(pool_id)->byte < key_len) {
		parent = &n.get_node(pool_id)->child[slice_index(key[n.get_node(pool_id)->byte],  n.get_node(pool_id)->bit)];
		auto nn = parent->load();

		if (nn)
			n = nn;
		else {
			n = any_leaf(n);
			break;
		}
	}

	assert(n);
	if (!n.is_leaf())
		n = any_leaf(n);

	assert(n);
	assert(n.is_leaf());

	auto nk_len = n.get_leaf(pool_id)->ksize + sizeof(uint64_t);

	byten_t common_len = nk_len < key_len ? nk_len : key_len;

	auto nkey = (const char*) &n.get_leaf(pool_id)->ksize;

	byten_t diff;
	for (diff = 0; diff < common_len; diff++) {
		if (nkey[diff] != key[diff])
			break;
	}

	if (diff >= common_len) {
		// auto &mtx = parent == &root ? root_mtx : prev.get_node(pool_id)->mtx;
		// std::unique_lock<obj::shared_mutex> lock(mtx);

		// /* this means that there was some concurrent insert */
		// if (n != parent->load(std::memory_order_relaxed))
		// 	goto restart;

		// tagged_node_ptr leaf = acts.make<tree::leaf>(
		// 	sizeof(tree::leaf) + key.size() + value.size(), key, value);
		// acts.set((uint64_t *)parent, leaf.offset());
		// acts.free(n.offset());

		// acts.publish();

		acts.set((uint64_t *)parent, leaf.offset());
		acts.publish();

		return;
	}

	/* Calculate the divergence point within the single byte. */
	char at = nkey[diff] ^ key[diff];
	bitn_t sh = utils::mssb_index((uint32_t)at) & (bitn_t)~(SLICE - 1);

	/* Descend into the tree again. */
	n = root.load(std::memory_order_acquire);
	parent = &root;

	while (n && !n.is_leaf() &&
			(n.get_node(pool_id)->byte < diff || (n.get_node(pool_id)->byte == diff && n.get_node(pool_id)->bit >= sh))) {
		parent = &n.get_node(pool_id)->child[slice_index(key[n.get_node(pool_id)->byte], n.get_node(pool_id)->bit)];
		n = parent->load(std::memory_order_acquire);
	}

	/*
	 * If the divergence point is at same nib as an existing node, and
	 * the subtree there is empty, just place our leaf there and we're
	 * done.  Obviously this can't happen if SLICE == 1.
	 */
	if (!n) {
		acts.set((uint64_t *)parent, leaf.offset());
		acts.publish();
		size_++;
		return;
	}

	/* If not, we need to insert a new node in the middle of an edge. */
	tagged_node_ptr node = acts.make<tree::node>(sizeof(tree::node));

	auto node_ptr = node.get_node(pool_id);
	node_ptr->child[slice_index(nkey[diff], sh)].store(n, std::memory_order_relaxed);
	node_ptr->child[slice_index(key[diff], sh)].store(leaf, std::memory_order_relaxed);
	node_ptr->byte = diff;
	node_ptr->bit = sh;

	acts.set((uint64_t *)parent, node.offset());

	// XXX - move size to TLS
	size_++;

	acts.publish();
}
}

bool tree::get(string_view view_key, pmemkv_get_v_callback *cb, void *arg)
{
	// XXX
	auto kkk = view_key.size();
	auto key = (char *)malloc(sizeof(uint64_t) + view_key.size());
	std::memcpy(key, &kkk, sizeof(uint64_t));
	std::memcpy(key + sizeof(uint64_t), view_key.data(), view_key.size());

	auto key_len = view_key.size() + sizeof(uint64_t);

restart : {
	auto n = root.load(std::memory_order_acquire);
	//auto prev = n;
	auto parent = &root;

	/*
	 * critbit algorithm: dive into the tree, looking at nothing but
	 * each node's critical bit^H^H^Hnibble.  This means we risk
	 * going wrong way if our path is missing, but that's ok...
	 */
	while (n && !n.is_leaf()) {
		if (n.get_node(pool_id)->byte >= key_len)
			return false;

		//prev = n;
		parent = &n.get_node(pool_id)
				  ->child[slice_index(key[n.get_node(pool_id)->byte], n.get_node(pool_id)->bit)];
		n = parent->load(std::memory_order_acquire);
	}

	// auto &mtx = parent == &root ? root_mtx : prev.get_node(pool_id)->mtx;
	// std::shared_lock<obj::shared_mutex> lock(mtx);

	// /* Concurrent erase could have inserted extra node at leaf position */
	// if (n != parent->load(std::memory_order_relaxed))
	// 	goto restart;

	if (!n || view_key.compare(string_view(n.get_leaf(pool_id)->key(), n.get_leaf(pool_id)->ksize)) != 0)
		return false;

	cb(n.get_leaf(pool_id)->data(), n.get_leaf(pool_id)->vsize, arg);

	return true;
}
}

bool tree::remove(obj::pool_base &pop, string_view view_key)
{
	// XXX
	auto kkk = view_key.size();
	auto key = (char *)malloc(sizeof(uint64_t) + view_key.size());
	std::memcpy(key, &kkk, sizeof(uint64_t));
	std::memcpy(key + sizeof(uint64_t), view_key.data(), view_key.size());

	auto key_len = view_key.size() + sizeof(uint64_t);

	actions acts(pop, pool_id);

	auto n = root.load(std::memory_order_acquire);
	auto *parent = &root;
	decltype(parent) pp = nullptr;

	while (n && !n.is_leaf()) {
		if (n.get_node(pool_id)->byte >= key_len)
			return false;

		pp = parent;
		parent = &n.get_node(pool_id)
				  ->child[slice_index(key[n.get_node(pool_id)->byte], n.get_node(pool_id)->bit)];
		n = parent->load(std::memory_order_acquire);
	}

	if (!n || key_len != n.get_leaf(pool_id)->ksize + sizeof(uint64_t) || view_key.compare(string_view(n.get_leaf(pool_id)->key(), n.get_leaf(pool_id)->ksize)) != 0)
		return false;

	{
			acts.free(n.offset());
	 		acts.set((uint64_t *)parent, 0);

			// XXX
			size_--;
	}

	/* was root */
	if (!pp) {
		acts.publish();
		return true;
	}

		n = pp->load();
		tagged_node_ptr only_child = nullptr;
		for (int i = 0; i < (int)SLNODES; i++) {
			auto &child = n.get_node(pool_id)->child[i];
			if (child.load() && &child != parent) {
				if (only_child) {
					acts.publish();
					return true;
				}
				only_child = n.get_node(pool_id)->child[i].load();
			}
		}

	assert(only_child);

	acts.set((uint64_t *)pp, only_child.offset());
	acts.free(n.offset()); // XXX = & ~1 ?

	acts.publish();

	return true;
}

void tree::iterate_rec(tree::tagged_node_ptr n, pmemkv_get_kv_callback *callback,
		       void *arg)
{
	if (!n.is_leaf()) {
		// auto &mtx = n == root.load(std::memory_order_acquire)
		// 	? root_mtx
		// 	: n.get_node(pool_id)->mtx;

		// /*
		//  * Keep locks on every level from root to leaf's parent. This simplifies
		//  * synchronization with concurrent inserts.
		//  */
		// std::shared_lock<obj::shared_mutex> lock(mtx);

		for (int i = 0; i < (int)SLNODES; i++) {
			auto child = n.get_node(pool_id)->child[i].load(
				std::memory_order_relaxed);
			if (child)
				iterate_rec(child, callback, arg);
		}
	} else {
		auto leaf = n.get_leaf(pool_id);
		callback(leaf->key(), leaf->ksize, leaf->data(),
			 leaf->vsize, arg);
	}
}

void tree::iterate(pmemkv_get_kv_callback *callback, void *arg)
{
	if (root.load(std::memory_order_acquire))
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
			if (n.get_node(pool_id)->child[i].load(std::memory_order_relaxed))
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
