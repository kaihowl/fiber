
//          Copyright Oliver Kowalke 2015.
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)
//

#ifndef BOOST_FIBERS_ALGO_WORK_STEALING_H
#define BOOST_FIBERS_ALGO_WORK_STEALING_H

#include <condition_variable>
#include <chrono>
#include <cstddef>
#include <cstring>
#include <mutex>
#include <vector>

#include <boost/config.hpp>
#include <boost/context/stack_traits.hpp>

#include <boost/fiber/algo/algorithm.hpp>
#include <boost/fiber/detail/context_spmc_queue.hpp>
#include <boost/fiber/context.hpp>
#include <boost/fiber/detail/config.hpp>
#include <boost/fiber/scheduler.hpp>

#ifdef BOOST_HAS_ABI_HEADERS
#  include BOOST_ABI_PREFIX
#endif

namespace boost {
namespace fibers {
namespace algo {

class work_stealing : public algorithm {
private:
	class circular_buffer {
	private:
		typedef context *   slot_type;

		std::size_t     pidx_{ 0 };
		std::size_t     cidx_{ 0 };
		std::size_t     capacity_;
		slot_type   *   slots_;

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

	public:
		circular_buffer( std::size_t capacity = 4*1024) :
				capacity_{ capacity } {
			slots_ = new slot_type[capacity_];
		}

		~circular_buffer() {
			delete [] slots_;
		}

		bool empty() const noexcept {
			return cidx_ == pidx_;
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
			if ( ! empty() ) {
				c = slots_[cidx_];
				cidx_ = (cidx_ + 1) % capacity_;
			}
			return c;
		}

		context * steal() {
			context * c = nullptr;
			if ( ! empty() ) {
				c = slots_[cidx_];
				if ( c->is_context( type::pinned_context) ) {
					return nullptr;
				}
				cidx_ = (cidx_ + 1) % capacity_;
			}
			return c;
		}
	};

    static std::vector< work_stealing * >           schedulers_;

    std::size_t                                     idx_;
    std::size_t                                     max_idx_;
	mutable detail::spinlock						splk_;
    circular_buffer                      			rqueue_{ 16 * boost::context::stack_traits::page_size() };
    std::mutex                                      mtx_{};
    std::condition_variable                         cnd_{};
    bool                                            flag_{ false };
    bool                                            suspend_;

    static void init_( std::size_t max_idx);

public:
    work_stealing( std::size_t max_idx, std::size_t idx, bool suspend = false);

    work_stealing( work_stealing const&) = delete;
    work_stealing( work_stealing &&) = delete;

    work_stealing & operator=( work_stealing const&) = delete;
    work_stealing & operator=( work_stealing &&) = delete;

    void awakened( context * ctx) noexcept;

    context * pick_next() noexcept;

    context * steal() noexcept {
		detail::spinlock_lock lk{ splk_ };	
        return rqueue_.steal();
    }

    bool has_ready_fibers() const noexcept {
		detail::spinlock_lock lk{ splk_ };	
        return ! rqueue_.empty();
    }

    void suspend_until( std::chrono::steady_clock::time_point const& time_point) noexcept;

    void notify() noexcept;
};

}}}

#ifdef BOOST_HAS_ABI_HEADERS
#  include BOOST_ABI_SUFFIX
#endif

#endif // BOOST_FIBERS_ALGO_WORK_STEALING_H
