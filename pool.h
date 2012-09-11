/*!
 * \file pool.hpp
 * \author ichramm
 * \date June 30, 2012, 2:43 AM
 *
 * Thread Pool class declaration
 */
#ifndef threadpool_h__
#define threadpool_h__

#include <boost/function.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/thread/thread_time.hpp>
#include <boost/enable_shared_from_this.hpp>

#if _MSC_VER > 1000
# pragma warning(push)
# pragma warning(disable: 4251) // class 'T' needs to have dll-interface...
#endif

#ifdef _WIN32
# ifdef THREADPOOL_EXPORTS
#  define THREADPOOL_API __declspec(dllexport)
# else
#  define THREADPOOL_API __declspec(dllimport)
# endif
#else
# define THREADPOOL_API
#endif

namespace threadpool
{
	/*! Default value for the minimum amount of threads in the pool ( = 8 ) */
	extern THREADPOOL_API const unsigned int MIN_POOL_THREADS;

	/*! Default value for the maximum amount of threads in the pool ( = 1000 )*/
	extern THREADPOOL_API const unsigned int MAX_POOL_THREADS;

	/*! Defines how many milliseconds we wait until resizing the pool if all
	 * threads are busy ( = 100 ms ) */
	extern THREADPOOL_API const unsigned int TIMEOUT_ADD_MORE_THREADS;

	/*! Defines how many milliseconds we wait until removing threads from the
	 * pool if there are too many threads idle ( = 300K ms -> 5 minutes) */
	extern THREADPOOL_API const unsigned int TIMEOUT_REMOVE_THREADS;

	/*!
	 * Base class for task objects
	 * Use \c boost::bind to create objects of this type
	 */
	typedef boost::function0<void> task_type;

	/*!
	 * These options control how the pool should behave on destruction
	 */
	enum shutdown_option
	{
		/*! The pool stops immediately, all tasks are canceled */
		shutdown_option_cancel_tasks,

		/*! The pool will wait until all tasks are complete before stopping */
		shutdown_option_wait_for_tasks
	};


	/*!
	 * Thread pool class
	 *
	 *  This class implements a smart thread pool, smart in the sense it
	 * can increase or decrease the number of threads in the pool depending
	 * on how the load is.
	 *
	 *  Pending tasks are stored in a FIFO queue, each thread in the pool pops tasks
	 * from the queue and executes them in its own context.
	 *
	 *  Tasks can also be scheduled for execution in a later time, without actually
	 * blocking a thread.
	 *  These tasks are queued the same way as standard tasks, but they store the time
	 * information needed to know when to execute them. Anytime a thread dequeues a task of
	 * this type, the time information is checked and if the current time is still less than
	 * the requested execution time then the task is sent back to the queue.
	 *
	 *  This class uses an additional thread to monitor pool status, so don't be scared if
	 * you your favorite monitoring tool shows an extra thread around there.
	 *  The pool monitor uses a soft-timeout to ensure the pool is resized when it's really
	 * needed. By default the monitor waits 100 ms (\see TIMEOUT_ADD_MORE_THREADS) before
	 * adding more threads to the pool.
	 *  Something similar happens when it has to remove threads from the pool, but
	 * this time the timeout is longer because we do not want to delete threads if
	 * we will probably need them later.
	 */
	class THREADPOOL_API pool
		: public boost::enable_shared_from_this<pool>
	{
	public:

		/*!
		 * Creates the threadpool, upper and lower bounds can be specified
		 *
		 * \param min_threads Minimum threads to have in the pool
		 * \param max_threads Maximum threads the pool can create
		 * \param timeout_add_threads_ms Specifies how much time we wait until resizing the
		 * pool when there are pending tasks but all the threads are busy.
		 * \param timeout_del_threads_ms Milliseconds to wait until remove threads from the
		 * pool when the load is too low
		 *
		 * \pre \code max_threads >= min_threads \endcode
		 *
		 * The constructor creates exactly \code min_threads + 1 \endcode threads, the
		 * extra thread is for monitoring the pool status.
		 *
		 * \note If \p min_thread is equal to \p max_threads the additional thread is not
		 * created because it's obviously not needed.
		 *
		 * \note Even when this class uses an extra thread to monitor pool status, it never
		 * creates more than \p max_threads threads.
		 */
		pool (
				unsigned int min_threads            = MIN_POOL_THREADS,
				unsigned int max_threads            = MAX_POOL_THREADS,
				unsigned int timeout_add_threads_ms = TIMEOUT_ADD_MORE_THREADS,
				unsigned int timeout_del_threads_ms = TIMEOUT_REMOVE_THREADS,
				shutdown_option on_shutdown     = shutdown_option_cancel_tasks
			);

		/*!
		 * Cancels all pending tasks in the thread pool, but waits until running
		 * tasks are complete. After that, stops and destroys all the threads in the pool
		 */
		~pool();

		/*!
		 * Queue an task for immediate execution.
		 *
		 * The task is going to be executed as soon as a thread
		 * is available, if there are no available threads the monitor will create them
		 */
		void schedule(const task_type& task);

		/*!
		 * Queue a task for execution when the time as reported
		 * by \c boost::get_system_time() would be equal to or later
		 * than the specified \p abs_time
		 *
		 * \note When in very heavy load, the task could not be executed exactly when
		 * it was requested to.
		 */
		void schedule(const task_type& task, const boost::system_time& abs_time);

		/*!
		 * Queue a task for execution after the period of time indicated
		 * by the \p rel_time argument has elapsed
		 *
		 * \note When in very heavy load, the task could not be executed exactly when
		 * it was requested to.
		 */
		void schedule(const task_type& task, const boost::posix_time::time_duration& rel_time);

		/*!
		 * \return The number of active tasks in the pool, aka the number of busy threads
		 *
		 * \remarks This also counts the threads used to monitor the pool state, have it
		 * in mind if you check exactly the number of tasks your application is performing.
		 */
		unsigned int active_tasks();

		/*!
		 * \return The number of tasks waiting for an available thread
		 *
		 * If this number gets to high for a long time you should be worried (it shouldn't, BTW)
		 */
		unsigned int pending_tasks();

		/*!
		 * \return The number of threads in the pool, it should be a number
		 * between \c min_threads and \c max_threads (see constructor)
		 */
		unsigned int pool_size();

	private:
		struct impl;
		boost::scoped_ptr<impl> pimpl; // pimpl idiom

		pool( const pool& );
		const pool& operator=( const pool& );
	};

} // namespace threadpool

#if _MSC_VER > 1000
# pragma warning(pop)
#endif

#endif // threadpool_h__
