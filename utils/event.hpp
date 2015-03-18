/* kate: space-indent off; remove-trailing-spaces all; indent-width 4; */
/*!
 * \file   event.hpp
 * \author ichramm
 *
 * Created on January 14, 2013, 9:15 PM
 */
#ifndef threadpool_utils_event_hpp__
#define threadpool_utils_event_hpp__

#include <boost/noncopyable.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition.hpp>

namespace threadpool
{
	namespace utils
	{
		/*!
		 * A manual-reset event.
		 *
		 * It helps to synchronize two or more threads when a thread
		 * has to met certain condition and the other(s) have to
		 * wait until the condition is met. (i.e. the event actually happens)
		 */
		class event
			: private boost::noncopyable
		{
		public:

			/*!
			 * Lockable concept
			 */
			typedef boost::unique_lock<event> scoped_lock;

			/*!
			 * Thrown when the \c scoped_lock object passed as parameter is not valid lock.
			 *
			 * A valid lock must met the following two conditions:
			 * \li The lockable being held by the lock must be the
			 * same \c event object on which the function is called
			 * \li The lock must own the lock
			 */
			class invalid_lock
				: public std::exception
			{
			public:

				invalid_lock(const std::string& what)
				 : _what(what)
				{ }

				~invalid_lock() throw()
				{ }

				/*!
				 * Overrides \c std::exception::what
				 */
				const char* what() const throw()
				{
					return _what.c_str();
				}

			private:
				const std::string _what;
			};

			/*!
			 * Creates a new \c event object.
			 *
			 * The object's initial state is unsignaled (i.e. event is not set)
			 */
			event()
			 : _event_set(false)
			 , _is_event_set(_event_set)
			{ }

			/*!
			 * \return \c true if the event is set
			 *
			 * \remarks This function acquires the lock
			 */
			bool is_event_set() const
			{
				boost::lock_guard<boost::mutex> lock(_sync_mutex);
				return _event_set;
			}

			/*!
			 * \return \c true if the condition is met (non-const version)
			 *
			 * It really does not make much sense to call this function but
			 * if you want it then go on...
			 */
			bool is_event_set(scoped_lock& lock)
				throw(invalid_lock)
			{
				lock_is_valid_or_throw(lock);
				return _event_set;
			}

			/*!
			 * Sets the condition to true and notifies all waiters of the change
			 *
			 * This also means the event is being signaled
			 *
			 * \note This function acquires the lock
			 */
			void set()
			{
				scoped_lock lock(*this);
				internal_set();
			}

			/*!
			 * Sets the condition to true and notifies all waiters of the change
			 */
			void set(scoped_lock& lock)
				throw(invalid_lock)
			{
				lock_is_valid_or_throw(lock);
				internal_set();
			}

			/*!
			 * Resets the condition to \c false
			 *
			 * \note This function acquires the lock
			 */
			void reset()
			{
				scoped_lock lock(*this);
				internal_reset();
			}

			/*!
			 * Resets the condition to \c false
			 */
			void reset(scoped_lock& lock)
				throw(invalid_lock)
			{
				lock_is_valid_or_throw(lock);
				internal_reset();
			}

			/*!
			 * Acquires the lock (BasicLockable concept)
			 *
			 * \see http://www.boost.org/doc/html/thread/synchronization.html#thread.synchronization.mutex_concepts.lockable
			 */
			void lock()
			{
				_sync_mutex.lock();
			}

			/*!
			 * Releases the lock (BasicLockable concept)
			 *
			 * \see http://www.boost.org/doc/html/thread/synchronization.html#thread.synchronization.mutex_concepts.lockable
			 */
			void unlock()
			{
				_sync_mutex.unlock();
			}

			/*!
			 * Tries to acquire the lock (Lockable concept)
			 *
			 * return \c true on success
			 *
			 * \see http://www.boost.org/doc/html/thread/synchronization.html#thread.synchronization.mutex_concepts.lockable
			 */
			bool try_lock()
			{
				return _sync_mutex.try_lock();
			}

			/*!
			 * Waits until the event is signaled
			 *
			 * \note This function acquires the lock
			 */
			void wait()
			{
				scoped_lock lock(*this);
				internal_wait(lock);
			}

			/*!
			 * Waits until the event is signaled using the specified lock
			 */
			void wait(scoped_lock& lock)
				throw(invalid_lock)
			{
				lock_is_valid_or_throw(lock);
				internal_wait(lock);
			}

			/*!
			 * Waits until the event is signaled or current time as reported
			 * by \c boost::get_system_time() is greater than or equal
			 * to \code boost::get_system_time() + timeout \endcode
			 *
			 * \note This function acquires the lock
			 */
			bool wait(const boost::posix_time::time_duration& timeout)
			{
				scoped_lock lock(*this);
				return internal_wait(lock, boost::get_system_time() + timeout);
			}

			/*!
			 * Waits until the event is signaled or current time as reported
			 * by \c boost::get_system_time() is greater than or equal
			 * to \code boost::get_system_time() + timeout \endcode
			 */
			bool wait(scoped_lock& lock, const boost::posix_time::time_duration& timeout)
				throw(invalid_lock)
			{
				lock_is_valid_or_throw(lock);
				return internal_wait(lock, boost::get_system_time() + timeout);
			}

			/*!
			 * Waits until the event is signaled or current time as specified
			 * by \c boost::get_system_time() is greater than or equal to \p deadline
			 *
			 * \note This function acquires the lock
			 */
			bool wait(const boost::system_time& deadline)
			{
				scoped_lock lock(*this);
				return internal_wait(lock, deadline);
			}

			/*!
			 * Waits until the event is signaled or current time as specified
			 * by \c boost::get_system_time() is greater than or equal to \p deadline
			 */
			bool wait(scoped_lock& lock, const boost::system_time& deadline)
				throw(invalid_lock)
			{
				lock_is_valid_or_throw(lock);
				return internal_wait(lock, deadline);
			}

		private:

			/*!
			 * Predicate used as argument to \c condition::wait
			 */
			class predicate_event_set
			{
				bool &_event_set;
			public:
				predicate_event_set(bool &b)
				 : _event_set(b)
				{ }

				bool operator()() const
				{
					return _event_set;
				}
			};

			void lock_is_valid_or_throw(const scoped_lock& lock) const
			{
				if (lock.mutex() != this)
				{
					throw invalid_lock("Lockable instance is invalid");
				}

				if (lock.owns_lock() == false)
				{
					throw invalid_lock("Lockable object doesn't own the lock");
				}
			}

			void internal_set()
			{
				_event_set = true;
				_condition.notify_all();
			}

			void internal_reset()
			{
				_event_set = false;
			}

			void internal_wait(scoped_lock& lock)
			{
				return _condition.wait(lock, _is_event_set);
			}

			bool internal_wait(scoped_lock& lock, const boost::system_time& deadline)
			{
				return _condition.timed_wait(lock, deadline, _is_event_set);
			}

		private:
			bool                 _event_set;
			predicate_event_set  _is_event_set;
			boost::condition     _condition;
			mutable boost::mutex _sync_mutex;
		};
	}
}

#endif // threadpool_utils_event_hpp__
