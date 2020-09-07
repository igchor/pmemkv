#ifndef PMEMKV_ITERATOR_H
#define PMEMKV_ITERATOR_H

#include <libpmemkv.hpp>

namespace pmem
{
namespace kv
{

class engine_base;

namespace internal
{

class iterator_base {
public:
	iterator_base(engine_base *engine) : engine(engine)
	{
	}
	virtual ~iterator_base();

	virtual status seek_to_begin()
	{
		return status::NOT_SUPPORTED;
	}
	virtual status seek_to_end()
	{
		return status::NOT_SUPPORTED;
	}
	virtual status seek_to_lower_bound()
	{
		return status::NOT_SUPPORTED;
	}
	virtual status seek_to_upper_bound()
	{
		return status::NOT_SUPPORTED;
	}
	virtual status next()
	{
		return status::NOT_SUPPORTED;
	}
	virtual status prev()
	{
		return status::NOT_SUPPORTED;
	}
	virtual string_view key()
	{
		return "";
	}
	virtual string_view value()
	{
		return "";
	}

protected:
	engine_base *engine;
};

}
}
}

#endif