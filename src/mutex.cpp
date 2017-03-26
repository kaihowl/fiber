
//          Copyright Oliver Kowalke 2013.
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)

#include "boost/fiber/mutex.hpp"

#include <algorithm>
#include <functional>
#include <system_error>

#include "boost/fiber/exceptions.hpp"
#include "boost/fiber/scheduler.hpp"

#ifdef BOOST_HAS_ABI_HEADERS
#  include BOOST_ABI_PREFIX
#endif

namespace boost {
namespace fibers {

void
mutex::lock() {
    context * active_ctx = context::active();
    // store this fiber in order to be notified later
    detail::spinlock_lock lk{ wait_queue_splk_ };
    if ( active_ctx == owner_) {
        throw lock_error{
                std::make_error_code( std::errc::resource_deadlock_would_occur),
                "boost fiber: a deadlock is detected" };
    } else if ( nullptr == owner_) {
        owner_ = active_ctx;
        return;
    }
    wait_queue_.push( active_ctx);
    // suspend this fiber
    active_ctx->suspend( lk);
}

bool
mutex::try_lock() {
    context * active_ctx = context::active();
    detail::spinlock_lock lk{ wait_queue_splk_ };
    if ( active_ctx == owner_) {
        throw lock_error{
                std::make_error_code( std::errc::resource_deadlock_would_occur),
                "boost fiber: a deadlock is detected" };
    } else if ( nullptr == owner_) {
        owner_ = active_ctx;
    }
    lk.unlock();
    // let other fiber release the lock
    active_ctx->yield();
    return active_ctx == owner_;
}

void
mutex::unlock() {
    context * active_ctx = context::active();
    detail::spinlock_lock lk{ wait_queue_splk_ };
    if ( active_ctx != owner_) {
        throw lock_error{
                std::make_error_code( std::errc::operation_not_permitted),
                "boost fiber: no  privilege to perform the operation" };
    }
    context * ctx = wait_queue_.pop();
    if ( nullptr != ctx) {
        owner_ = ctx;
        active_ctx->schedule( ctx);
    } else {
        owner_ = nullptr;
        return;
    }
}

}}

#ifdef BOOST_HAS_ABI_HEADERS
#  include BOOST_ABI_SUFFIX
#endif
