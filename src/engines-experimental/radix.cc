// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2020, Intel Corporation */

#include "radix.h"
#include "../out.h"

#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/pext.hpp>
#include <libpmemobj++/transaction.hpp>

#include <mutex>

namespace pmem
{
namespace kv
{

namespace internal
{
namespace radix
{

// XXX - move to utils?
static inline uint64_t util_mssb_index64(std::size_t value)
{
	return ((unsigned char)(63 - __builtin_clzll(value)));
}

tree::leaf::leaf(uint64_t key, string_view value) : key(key), value_size(value.size())
{
	std::memcpy(this->data(), value.data(), value.size());
}

void tree::leaf::assign(string_view value)
{
	assert(value.size() <= capacity());
	assert(pmemobj_tx_stage() == TX_STAGE_WORK);

	std::memcpy(data(), value.data(), value.size());
	value_size = value.size();
}

const char *tree::leaf::data() const noexcept
{
	return reinterpret_cast<const char *>(this + 1);
}

const char *tree::leaf::cdata() const noexcept
{
	return reinterpret_cast<const char *>(this + 1);
}

std::size_t tree::leaf::capacity()
{
	return pmemobj_alloc_usable_size(pmemobj_oid(this)) - sizeof(this);
}

char *tree::leaf::data()
{
	assert(pmemobj_tx_stage() == TX_STAGE_WORK);

	auto *ptr = reinterpret_cast<char *>(this + 1);

	obj::transaction::snapshot(ptr, value_size);

	return ptr;
}

tree::tree()
{
	size_ = 0;
	pool_id = pmemobj_oid(this).pool_uuid_lo;
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

void tree::insert(obj::pool_base &pop, uint64_t key, string_view value)
{
restart:
{
	auto n = root.load();
	if (!n) {
		std::unique_lock<obj::mutex> lock(root_mtx);

		if (n.get() != root.load().get())
			goto restart;

		obj::transaction::run(pop, [&] { root.store(make_leaf(key, value)); });
		return;
	}

	auto prev = root.load();
	auto *parent = &root;

	while (n && !n.is_leaf() &&
	       (key & path_mask(n.get_node(pool_id)->shift)) == n.get_node(pool_id)->path) {
		prev = n;
		parent = &n.get_node(pool_id)->child[slice_index(key, n.get_node(pool_id)->shift)];
		n = *parent;
	}

	auto *mtx = parent == &root ? &root_mtx : &prev.get_node(pool_id)->mtx;

	if (!n) {
		std::unique_lock<obj::mutex> lock(*mtx);

		if (parent->load().get() != n.get())
				goto restart;

		obj::transaction::run(pop, [&] {
			parent->store(make_leaf(key, value));
		});
		return;
	}

	uint64_t path = n.is_leaf() ? n.get_leaf(pool_id)->key : n.get_node(pool_id)->path;
	/* Find where the path differs from our key. */
	uint64_t at = path ^ key;
	if (!at) {
		assert(n.is_leaf());

		std::unique_lock<obj::mutex> lock(*mtx);

		// XXX here and in other places - chcek for obsolete flag
		// because parent itself could have been unpinned
		if (parent->load().get() != n.get())
			goto restart;

		obj::transaction::run(pop, [&] {
			parent->store(make_leaf(key, value));
			delete_node(n);
		});	

		return;
	}

	/* and convert that to an index. */
	sh_t sh = util_mssb_index64(at) & (sh_t) ~(SLICE - 1);

	std::unique_lock<obj::mutex> lock(*mtx);

	if (parent->load().get() != n.get())
		goto restart;

	obj::transaction::run(pop, [&] {
		auto m = obj::make_persistent<node>();
		m->child[slice_index(key, sh)] = make_leaf(key, value);
		m->child[slice_index(path, sh)] = n;
		m->shift = sh;
		m->path = key & path_mask(sh);
		parent->store(m);
	});
}
}

string_view tree::get(uint64_t key)
{
	auto n = root;

	/*
	 * critbit algorithm: dive into the tree, looking at nothing but
	 * each node's critical bit^H^H^Hnibble.  This means we risk
	 * going wrong way if our path is missing, but that's ok...
	 */
	while (n && !n.is_leaf())
		n = n.get_node(pool_id)->child[slice_index(key, n.get_node(pool_id)->shift)].load();

	if (n && n.get_leaf(pool_id)->key == key)
		return string_view(n.get_leaf(pool_id)->cdata(), n.get_leaf(pool_id)->value_size);
	else
		throw std::out_of_range("No such key"); // XXX return this info?
}

bool tree::remove(obj::pool_base &pop, uint64_t key)
{
	auto n = root;
	if (!n)
		return false;

	if (n.is_leaf()) {
		if (n.get_leaf(pool_id)->key == key) {
			obj::transaction::run(pop, [&] {
				root = nullptr;
				delete_node(n);
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
		k_parent = &kn.get_node(pool_id)->child[slice_index(key, kn.get_node(pool_id)->shift)];
		kn = *k_parent;

		if (!kn)
			return false;
	}

	if (kn.get_leaf(pool_id)->key != key)
		return false;

	obj::transaction::run(pop, [&] {
		delete_node(kn);

		n.get_node(pool_id)->child[slice_index(key, n.get_node(pool_id)->shift)] = nullptr;

		/* Remove the node if there's only one remaining child. */
		int ochild = -1;
		for (int i = 0; i < (int)SLNODES; i++) {
			if (n.get_node(pool_id)->child[i]) {
				if (ochild != -1)
					return;

				ochild = i;
			}
		}

		assert(ochild != -1);

		*n_parent = n.get_node(pool_id)->child[ochild];
		obj::delete_persistent<tree::node>(n.get_node(pool_id));
	});

	return true;
}

void tree::iterate_rec(tree::tagged_node_ptr n, pmemkv_get_kv_callback *callback,
		       void *arg)
{
	if (!n.is_leaf()) {
		for (int i = 0; i < (int)SLNODES; i++) {
			if (n.get_node(pool_id)->child[i])
				iterate_rec(n.get_node(pool_id)->child[i], callback,
					    arg); // XXX - get rid of recursion
		}
	} else {
		auto leaf = n.get_leaf(pool_id);
		callback((const char *)&leaf->key, sizeof(leaf->key), leaf->cdata(),
			 leaf->value_size, arg);
	}
}

void tree::iterate(pmemkv_get_kv_callback *callback, void *arg)
{
	if (root)
		iterate_rec(root, callback, arg);
}

uint64_t tree::path_mask(sh_t shift)
{
	return ~NIB << shift;
}

unsigned tree::slice_index(uint64_t key, sh_t shift)
{
	return (unsigned)((key >> shift) & NIB);
}

void tree::delete_node(tree::tagged_node_ptr n)
{
	assert(pmemobj_tx_stage() == TX_STAGE_WORK);

	if (!n.is_leaf()) {
		for (int i = 0; i < (int)SLNODES; i++) {
			if (n.get_node(pool_id)->child[i])
				delete_node(
					n.get_node(pool_id)
						->child[i]); // XXX - get rid of recursion
		}
		obj::delete_persistent<tree::node>(n.get_node(pool_id));
	} else {
		size_--;
		obj::delete_persistent<tree::leaf>(n.get_leaf(pool_id));
	}
}

obj::persistent_ptr<tree::leaf> tree::make_leaf(uint64_t key, string_view value)
{
	assert(pmemobj_tx_stage() == TX_STAGE_WORK);

	obj::persistent_ptr<tree::leaf> leaf_ptr =
		pmemobj_tx_alloc(sizeof(tree::leaf) + value.size() * sizeof(char), 0);

	new (leaf_ptr.get()) leaf(key, value);

	size_++;

	return leaf_ptr;
}

tree::tagged_node_ptr::tagged_node_ptr()
{
	off = 0;
}

tree::tagged_node_ptr::tagged_node_ptr(const obj::persistent_ptr<leaf> &ptr)
{
	off = ptr.raw().off | 1;
}

tree::tagged_node_ptr::tagged_node_ptr(const obj::persistent_ptr<node> &ptr)
{
	off = ptr.raw().off;
}

tree::tagged_node_ptr::tagged_node_ptr(const tree::tagged_node_ptr &rhs)
{
	off = rhs.off.load(std::memory_order_relaxed);
}

tree::tagged_node_ptr &tree::tagged_node_ptr::operator=(const tree::tagged_node_ptr &rhs)
{
	off = rhs.off.load(std::memory_order_relaxed);
	return *this;
}

tree::tagged_node_ptr &tree::tagged_node_ptr::operator=(std::nullptr_t)
{
	off = 0;
	return *this;
}

tree::tagged_node_ptr &
tree::tagged_node_ptr::operator=(const obj::persistent_ptr<leaf> &rhs)
{
	off = rhs.raw().off | 1;
	return *this;
}

tree::tagged_node_ptr &
tree::tagged_node_ptr::operator=(const obj::persistent_ptr<node> &rhs)
{
	off = rhs.raw().off;
	return *this;
}

bool tree::tagged_node_ptr::is_leaf() const
{
	return off.load(std::memory_order_acquire) & 1;
}

tree::leaf *tree::tagged_node_ptr::get_leaf(uint64_t pool_id) const
{
	assert(is_leaf());
	return (tree::leaf *) pmemobj_direct({pool_id,  off.load(std::memory_order_acquire) & ~1ULL});
}

tree::node *tree::tagged_node_ptr::get_node(uint64_t pool_id) const
{
	assert(!is_leaf());
	return (tree::node *) pmemobj_direct({pool_id,  off.load(std::memory_order_acquire)});
}

tree::tagged_node_ptr::operator bool() const noexcept
{
	return off.load(std::memory_order_acquire) != 0;
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
	try {
		(void)tree->get(key_to_uint64(key));
		return status::OK;
	} catch (std::out_of_range &) {
		return status::NOT_FOUND;
	}
}

status radix::get(string_view key, get_v_callback *callback, void *arg)
{
	try {
		auto view = tree->get(key_to_uint64(key));
		callback(view.data(), view.size(), arg);

		return status::OK;
	} catch (std::out_of_range &) {
		return status::NOT_FOUND;
	}
}

status radix::put(string_view key, string_view value)
{
	tree->insert(pmpool, key_to_uint64(key), value);

	return status::OK;
}

status radix::remove(string_view key)
{
	return tree->remove(pmpool, key_to_uint64(key)) ? status::OK : status::NOT_FOUND;
}

// status defrag(double start_percent, double amount_percent) value_size;

uint64_t radix::key_to_uint64(string_view v)
{
	if (v.size() > sizeof(uint64_t))
		throw internal::invalid_argument("Key length must be <= 8");

	uint64_t val = 0;
	memcpy(&val, v.data(), v.size());

	return val;
}

} // namespace kv
} // namespace pmem
