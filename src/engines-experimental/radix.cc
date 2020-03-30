// SPDX-License-Identifier: BSD-3-Clause
/* Copyright 2020, Intel Corporation */

#include "radix.h"
#include "../out.h"

#include <libpmemobj++/make_persistent.hpp>
#include <libpmemobj++/p.hpp>
#include <libpmemobj++/pext.hpp>
#include <libpmemobj++/transaction.hpp>

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
	auto n = root;
	if (!n) {
		obj::transaction::run(pop, [&] { root = make_leaf(key, value); });
		return;
	}

	auto *parent = &root;
	auto prev = root;

	while (n && !n.is_leaf() &&
	       (key & path_mask(n.get_node()->shift)) == n.get_node()->path) {
		prev = n;
		parent = &n.get_node()->child[slice_index(key, n.get_node()->shift)];
		n = *parent;
	}

	if (!n) {
		obj::transaction::run(pop, [&] {
			prev.get_node()->child[slice_index(key, prev.get_node()->shift)] =
				make_leaf(key, value);
		});
		return;
	}

	uint64_t path = n.is_leaf() ? n.get_leaf()->key : n.get_node()->path;
	/* Find where the path differs from our key. */
	uint64_t at = path ^ key;
	if (!at) {
		assert(n.is_leaf());

		obj::transaction::run(pop, [&] {
			if (value.size() <= n.get_leaf()->capacity()) {
				/* Just reuse existing memory */
				n.get_leaf()->assign(value);
			} else {
				/* Allocate new, bigger node and free the old one */
				prev.get_node()->child[slice_index(
					key, prev.get_node()->shift)] =
					make_leaf(key, value);

				delete_node(n);
			}
		});

		return;
	}

	/* and convert that to an index. */
	sh_t sh = util_mssb_index64(at) & (sh_t) ~(SLICE - 1);

	obj::transaction::run(pop, [&] {
		auto m = obj::make_persistent<node>();
		m->child[slice_index(key, sh)] = make_leaf(key, value);
		m->child[slice_index(path, sh)] = n;
		m->shift = sh;
		m->path = key & path_mask(sh);
		*parent = m;
	});
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
		n = n.get_node()->child[slice_index(key, n.get_node()->shift)];

	if (n && n.get_leaf()->key == key)
		return string_view(n.get_leaf()->cdata(), n.get_leaf()->value_size);
	else
		throw std::out_of_range("No such key"); // XXX return this info?
}

bool tree::remove(obj::pool_base &pop, uint64_t key)
{
	auto n = root;
	if (!n)
		return false;

	if (n.is_leaf()) {
		if (n.get_leaf()->key == key) {
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
		k_parent = &kn.get_node()->child[slice_index(key, kn.get_node()->shift)];
		kn = *k_parent;

		if (!kn)
			return false;
	}

	if (kn.get_leaf()->key != key)
		return false;

	obj::transaction::run(pop, [&] {
		delete_node(kn);

		n.get_node()->child[slice_index(key, n.get_node()->shift)] = nullptr;

		/* Remove the node if there's only one remaining child. */
		int ochild = -1;
		for (int i = 0; i < (int)SLNODES; i++) {
			if (n.get_node()->child[i]) {
				if (ochild != -1)
					return;

				ochild = i;
			}
		}

		assert(ochild != -1);

		*n_parent = n.get_node()->child[ochild];
		obj::delete_persistent<tree::node>(n.get_node());
	});

	return true;
}

void tree::iterate_rec(tree::tagged_node_ptr n, pmemkv_get_kv_callback *callback,
		       void *arg)
{
	if (!n.is_leaf()) {
		for (int i = 0; i < (int)SLNODES; i++) {
			if (n.get_node()->child[i])
				iterate_rec(n.get_node()->child[i], callback,
					    arg); // XXX - get rid of recursion
		}
	} else {
		auto leaf = n.get_leaf();
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
			if (n.get_node()->child[i])
				delete_node(
					n.get_node()
						->child[i]); // XXX - get rid of recursion
		}
		obj::delete_persistent<tree::node>(n.get_node());
	} else {
		size_--;
		obj::delete_persistent<tree::leaf>(n.get_leaf());
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

tree::tagged_node_ptr::tagged_node_ptr(const obj::persistent_ptr<leaf> &ptr)
    : obj::persistent_ptr_base({ptr.raw().pool_uuid_lo, ptr.raw().off | 1})
{
}

tree::tagged_node_ptr::tagged_node_ptr(const obj::persistent_ptr<node> &ptr)
    : obj::persistent_ptr_base(ptr)
{
}

tree::tagged_node_ptr &tree::tagged_node_ptr::operator=(const tree::tagged_node_ptr &rhs)
{
	obj::persistent_ptr_base::operator=(
		static_cast<const obj::persistent_ptr_base &>(rhs));
	return *this;
}

tree::tagged_node_ptr &tree::tagged_node_ptr::operator=(std::nullptr_t)
{
	obj::persistent_ptr_base::operator=(nullptr);
	return *this;
}

tree::tagged_node_ptr &
tree::tagged_node_ptr::operator=(const obj::persistent_ptr<leaf> &rhs)
{
	return this->operator=(tagged_node_ptr(rhs));
}

tree::tagged_node_ptr &
tree::tagged_node_ptr::operator=(const obj::persistent_ptr<node> &rhs)
{
	return this->operator=(tagged_node_ptr(rhs));
}

bool tree::tagged_node_ptr::is_leaf()
{
	return raw().off & 1;
}

obj::persistent_ptr<tree::leaf> tree::tagged_node_ptr::get_leaf()
{
	assert(is_leaf());
	return obj::persistent_ptr<leaf>({raw().pool_uuid_lo, raw().off & ~1ULL});
}

obj::persistent_ptr<tree::node> tree::tagged_node_ptr::get_node()
{
	assert(!is_leaf());
	return obj::persistent_ptr<node>(raw());
}

tree::tagged_node_ptr::operator bool() const noexcept
{
	return oid.off != 0;
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
