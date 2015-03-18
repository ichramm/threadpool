/* kate: space-indent off; remove-trailing-spaces all; indent-width 4; */
/*!
 * \file atomic.hpp
 * \author ichramm
 *
 * Created on December 28, 2012, 09:50 PM
 */
#ifndef threadpool_utils_atomic_hpp__
#define threadpool_utils_atomic_hpp__

#include <boost/detail/atomic_count.hpp>

namespace threadpool
{
	namespace utils
	{
		typedef boost::detail::atomic_count  atomic_counter;
	}
}

#endif // threadpool_utils_atomic_hpp__
