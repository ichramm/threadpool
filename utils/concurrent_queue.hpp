/*!
 * \file concurrent_queue.hpp
 * \author ichramm
 *
 * Created on August 3, 2010, 2:53 PM
 */
#ifndef threadpool_utils_concurrent_queue_hpp__
#define threadpool_utils_concurrent_queue_hpp__

#include <list>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition.hpp>

#include "atomic.hpp"

#if _MSC_VER > 1000
# pragma warning(push)
# pragma warning(disable: 4290) // C++ exception specification ignored except to indicate a function is not __declspec(nothrow)
#endif

namespace threadpool
{
	namespace utils
	{
		/*!
		 * This class implements FIFO queue, but with concurrency.
		 *
		 * This means not only that this object is thread safe, but also will block
		 * any \c pop() call when the queue is empty until a new element is pushed
		 * into the queue or, in some overloads, the given timeout has passed.
		 *
		 * This class is implemented as a container adaptor, this means an encapsulated
		 * object of a specific container class is used as its underlying container, providing
		 * a specific set of member functions to access its elements.
		 *
		 * Elements are pushed into the "back" of the specific container and popped from its "front".
		 *
		 * The underlying container may be one of the standard container class template or
		 * some other specifically designed container class. The only requirement is that
		 * it supports the following operations:
		 *
		 * \li empty()
		 * \li size()
		 * \li front()
		 * \li pop_front()
		 * \li push_back()
		 *
		 * Therefore, the standard container class templates deque and list can be used. By
		 * default, if no container class is specified for a particular concurrent_queue
		 * class, the standard container class template std::list is used.
		 *
		 * The implementation has two template parameters:
		 * \code
		 *  template <
		 *   class _Tp,
		 *   class _Sequence = std::list<-Tp>
		 *  >
		 * class concurrent_queue;
		 * \endcode
		 *
		 * Where the template parameters have the following meanings:
		 * \li \c _Tp : Type of the elements.
		 * \li \c _Sequence : Type of the underlying container object used to store and access the elements.
		 */
		template <
			class _Tp,
			class _Sequence = std::list<_Tp>
		> class concurrent_queue
		{
		public:

			typedef typename _Sequence::value_type   value_type;
			typedef typename _Sequence::size_type    size_type;
			typedef          _Sequence               container_type;

			/*!
			 * Thrown a wait in \c pop() has timed out
			 */
			class timeout_exception
				: public std::exception
			{
			public:

				/*!
				 * Overrides \c std::exception::what
				 */
				const char* what() const
					throw()
				{
					return "Timed-out";
				}
			};

			/*!
			 * Predicate used as argument to \c condition::wait
			 */
			class predicate_have_elements
			{
				container_type &_container;
			public:
				predicate_have_elements(container_type &c)
				 : _container(c)
				{ }

				bool operator()() const
				{
					return !_container.empty();
				}
			};

			/*!
			 * Initializes an empty queue
			 */
			concurrent_queue()
			 : _size(0)
			 , _have_elements(_container)
			{
			}

			/*!
			 * Initializes a new instance by using a copy of the
			 * container object specified by \p src
			 */
			explicit concurrent_queue(const container_type& src)
			 : _size(src.size())
			 , _container(src)
			 , _have_elements(_container)
			{
			}

			/*!
			 * \return \c true if the queue does not contain any elements, otherwise \c false
			 */
			bool empty() const
			{
				return (_size == 0);
			}

			/*!
			 * \return The current number of elements in the queue
			 */
			size_type size() const
			{
				return _size;
			}

			/*!
			 * \return A copy of the underlying container
			 */
			operator container_type() const
			{
				boost::lock_guard<boost::mutex> lock(_mutex);
				return _container;
			}

			/*!
			 * Inserts an element at the end of the queue
			 *
			 * \note If there is a thread blocked in \c pop(), this function will wake it up
			 */
			void push(const value_type& element)
			{
				boost::lock_guard<boost::mutex> lock(_mutex);
				push_one(element);
			}

			/*!
			 * Gets and removes an element from the front of the queue. If the
			 * queue is empty this function blocks until a new element is pushed
			 * into the stack.
			 *
			 * This is the recommended popping method when used wisely.
			 *
			 * \return The first element in the queue, aka the one being popped
			 */
			value_type pop()
			{
				value_type _result;
				return pop(_result);
			}

			/*!
			 * Gets and removes an element from the front of the queue. If the
			 * queue is empty this function blocks until a new element is pushed
			 * into the stack, or until \p timeout_ms milliseconds has passed.
			 *
			 * \param timeout_ms Max milliseconds to wait in case the queue is empty
			 *
			 * \return The first element in the queue, aka the one being popped
			 */
			value_type pop(size_t timeout_ms)
				throw(timeout_exception)
			{
				value_type _result;

				if ( !pop(_result, timeout_ms) )
				{
					throw timeout_exception();
				}

				return _result;
			}

			/*!
			 * Gets and removes an element from the front of the queue. If the
			 * queue is empty this function blocks until a new element is pushed
			 * into the stack.
			 *
			 * \param result Set with the element being popped.
			 *
			 * \return \p result
			 */
			value_type& pop(value_type &result)
			{
				boost::unique_lock<boost::mutex> lock(_mutex);

				_condition.wait(lock, _have_elements);

				result = pop_one();
				return result;
			}

			/*!
			 * Gets and removes an element from the front of the queue. If the
			 * queue is empty this function blocks until a new element is pushed
			 * into the stack, or until \p timeout_ms milliseconds has passed.
			 *
			 * \param result Set with the element being popped.
			 *
			 * \return \c true if an element has been popped, \c false if the queue
			 * is still empty after the given timeout
			 */
			bool pop(value_type &result, size_t timeout_ms)
			{
				boost::unique_lock<boost::mutex> lock(_mutex);

				boost::system_time deadline = boost::get_system_time() +
							boost::posix_time::milliseconds(timeout_ms);

				if ( !_condition.timed_wait(lock, deadline, _have_elements) )
				{
					return false;
				}

				result = pop_one();
				return true;
			}

			/*!
			 * Clears the queue, i.e. removes all elements
			 */
			void clear()
			{
				boost::lock_guard<boost::mutex> lock(_mutex);
				while ( _have_elements() )
				{
					pop_one();
				}
			}

		private:

			void push_one(const value_type &element)
			{
				++_size;
				_container.push_back(element);
				_condition.notify_one();
			}

			value_type pop_one()
			{
				value_type _result = _container.front();
				_container.pop_front();
				--_size;
				return _result;
			}

			atomic_counter           _size;
			container_type           _container;
			predicate_have_elements  _have_elements;
			mutable boost::mutex     _mutex;
			boost::condition         _condition;
		};
	}
}

#if _MSC_VER > 1000
# pragma warning(pop)
#endif

#endif // threadpool_utils_concurrent_queue_hpp__
