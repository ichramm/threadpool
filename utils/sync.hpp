/*!
 * \file   sync.hpp
 * \author ichramm
 *
 * Created on January 14, 2013, 9:15 PM
 */
#ifndef threadpool_utils_sync_hpp__
#define threadpool_utils_sync_hpp__

#include <boost/noncopyable.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition.hpp>

namespace threadpool
{
	namespace utils
	{
		/*!
		 * This class works like a manual-reset event.
		 *
		 * It helps to synchronize two or more threads when a thread
		 * has to met certain condition and the other(s) have to
		 * wait until the condition is met.
		 *
		 * \note Wait functions are designed to cope with spurious
		 * wakeups so the user does not have to worry about checking
		 * whether the condition was actually set or not.
		 */
		class sync
			: private boost::noncopyable
		{
		public:

			/*!
			 * Lockable concept
			 */
			typedef boost::unique_lock<sync> scoped_lock;

			/*!
			 * Thrown when the \c scoped_lock object passed as parameter is not valid lock.
			 *
			 * A valid lock must met the following two conditions:
			 * \li The lockable being held by the lock must be the
			 * same \c synchronizer object on which the function is called
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
			 * Predicate used as argument to \c condition::wait
			 */
			class predicate_condition_met
			{
				bool &_condition_met;
			public:
				predicate_condition_met(bool &b)
				 : _condition_met(b)
				{ }

				bool operator()() const
				{
					return _condition_met;
				}
			};

			/*!
			 * Creates a new \c synchronizer object with condition set to \c false
			 */
			sync()
			 : _condition_met(false)
			 , _is_condition_met(_condition_met)
			{ }

			/*!
			 * \return \c true if the condition is met
			 *
			 * \remarks This function acquires the lock
			 */
			bool is_condition_met() const
			{
				boost::lock_guard<boost::mutex> lock(_sync_mutex);
				return _condition_met;
			}

			/*!
			 * \return \c true if the condition is met (non-const version)
			 *
			 * It really does not make much sense to call this function but
			 * if you want it then go on...
			 */
			bool is_condition_met(scoped_lock& lock)
				throw(invalid_lock)
			{
				lock_is_valid_or_throw(lock);
				return _condition_met;
			}

			/*!
			 * Sets the condition to true and notifies all waiters of the change
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
			 * Waits until the condition is set
			 *
			 * \note This function acquires the lock
			 */
			void wait()
			{
				scoped_lock lock(*this);
				internal_wait(lock);
			}

			/*!
			 * Waits until the condition is set using the specified lock
			 */
			void wait(scoped_lock& lock)
				throw(invalid_lock)
			{
				lock_is_valid_or_throw(lock);
				internal_wait(lock);
			}

			/*!
			 * Waits until the condition is set or current time as reported
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
			 * Waits until the condition is set or current time as reported
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
			 * Waits until the condition is set or current time as specified
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
			 * Waits until the condition is set or current time as specified
			 * by \c boost::get_system_time() is greater than or equal to \p deadline
			 */
			bool wait(scoped_lock& lock, const boost::system_time& deadline)
				throw(invalid_lock)
			{
				lock_is_valid_or_throw(lock);
				return internal_wait(lock, deadline);
			}

		private:

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
				_condition_met = true;
				_condition.notify_all();
			}

			void internal_reset()
			{
				_condition_met = false;
			}

			void internal_wait(scoped_lock& lock)
			{
				return _condition.wait(lock, _is_condition_met);
			}

			bool internal_wait(scoped_lock& lock, const boost::system_time& deadline)
			{
				return _condition.timed_wait(lock, deadline, _is_condition_met);
			}

		private:
			bool                    _condition_met;
			predicate_condition_met _is_condition_met;
			boost::condition        _condition;
			mutable boost::mutex    _sync_mutex;
		};
	}
}

#endif // threadpool_utils_sync_hpp__
