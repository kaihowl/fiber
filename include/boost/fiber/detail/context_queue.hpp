
//          Copyright Oliver Kowalke 2015.
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)
//

#ifndef BOOST_FIBERS_DETAIL_QUEUE_H
#define BOOST_FIBERS_DETAIL_QUEUE_H

#include <cstddef>
#include <cstring>
#include <mutex>

#include <boost/config.hpp>

#include <boost/fiber/context.hpp>
#include <boost/fiber/detail/config.hpp>
#include <boost/fiber/detail/spinlock.hpp>

#ifdef BOOST_HAS_ABI_HEADERS
#  include BOOST_ABI_PREFIX
#endif

namespace boost {
namespace fibers {
namespace detail {

class context_queue {
private:
	typedef context *   slot_type;

	std::size_t                                 pidx_{ 0 };
	std::size_t                                 cidx_{ 0 };
	std::size_t                                 capacity_;
	slot_type                               *   slots_;

	void resize_() {
		slot_type * old_slots = slots_;
		slots_ = new slot_type[2*capacity_];
		std::size_t offset = capacity_ - cidx_;
		std::memcpy( slots_, old_slots + cidx_, offset * sizeof( slot_type) );
		if ( 0 < cidx_) {
			std::memcpy( slots_ + offset, old_slots, pidx_ * sizeof( slot_type) );
		}
		cidx_ = 0;
		pidx_ = capacity_ - 1;
		capacity_ *= 2;
		delete [] old_slots;
	}

	bool is_full_() const noexcept {
		return cidx_ == ((pidx_ + 1) % capacity_);
	}

	bool is_empty_() const noexcept {
		return cidx_ == pidx_;
	}

public:
	context_queue( std::size_t capacity = 4096) :
			capacity_{ capacity } {
		slots_ = new slot_type[capacity_];
	}

	~context_queue() {
		delete [] slots_;
	}

    context_queue( context_queue const&) = delete;
    context_queue & operator=( context_queue const&) = delete;

	bool empty() const noexcept {
		return is_empty_();
	}

	void push( context * c) {
		if ( is_full_() ) {
			resize_();
		}
		slots_[pidx_] = c;
		pidx_ = (pidx_ + 1) % capacity_;
	}

	context * pop() {
		context * c = nullptr;
		if ( ! is_empty_() ) {
			c = slots_[cidx_];
			cidx_ = (cidx_ + 1) % capacity_;
		}
		return c;
	}
};

}}}

#ifdef BOOST_HAS_ABI_HEADERS
#  include BOOST_ABI_SUFFIX
#endif

#endif // BOOST_FIBERS_DETAIL_QUEUE_H
