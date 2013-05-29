/*!
 * \file pool.hpp
 * \author ichramm
 * \date June 30, 2012, 2:43 AM
 *
 * Thread Pool class declaration
 */
#ifndef threadpool_pool_hpp__
#define threadpool_pool_hpp__

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
	/*!
	 * Base class for task objects
	 * Use \c boost::bind to create objects of this type
	 */
	typedef boost::function0<void> task_type;

	/*!
	 * Scheduling function will return one of these values
	 */
	enum schedule_result
	{
		/*!
		 * The task was correctly scheduled
		 */
		schedule_result_ok,

		/*!
		 * The pool is stopping and cannot accept more tasks
		 */
		schedule_result_err_down,

		/*!
		 * The pool is to busy to accept another task
		 * This may happens only if all the following conditions are met:
		 * \li All threads are busy
		 * \li Maximum number of threads has been reached (i.e. Cannot crate more threads)
		 * \li Task queue is full
		 *
		 * \note This value is not in currently in use because the task queue has no capacity limit.
		 */
		schedule_result_err_busy
	};

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
	 * from the queue and executes them.
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
	 * needed. By default the monitor waits 100 ms before adding more threads to the pool.
	 *  Something similar happens when it has to remove threads from the pool, but
	 * this time the timeout is longer because we do not want to delete threads if
	 * we will probably need them later.
	 *
	 * \note Even when this class uses an extra thread to monitor pool status, it never
	 * creates more than \p max_threads threads.
	 *
	 * \note The pool increases it's size by a factor of 1.5 and decreases it by a factor of 2.0
	 *
	 * \note The pool is considered idle when over 75% of threads are idle
	 *
	 * \note The pool is considered overloaded when all threads are busy and there is at
	 * least one pending task in the queue for not less than 2 milliseconds.
	 */
	class THREADPOOL_API pool
		: public boost::enable_shared_from_this<pool>
	{
	public:

		/*!
		 * Creates the threadpool, upper and lower bounds can be specified
		 *
		 * \param min_threads Minimum threads to have in the pool. If this parameter is equal
		 * to -1 the pool will create twice the number of processors (or HT cores) on the
		 * host computer as reported by \c boost::thread::hardware_concurrency().
		 *
		 * \param max_threads Maximum threads the pool can create.
		 *
		 * \param timeout_add_threads_ms Specifies how much time we wait until resizing the
		 * pool when there are pending tasks but all the threads are busy.
		 *
		 * \param timeout_del_threads_ms Milliseconds to wait until remove threads from the
		 * pool when the load is too low.
		 *
		 * \param on_shutdown Specifies what to do with pending tasks when the pool is being
		 * destroyed. \see shutdown_option.
		 *
		 * \pre \code max_threads >= min_threads \endcode
		 *
		 * The constructor creates exactly \code min_threads + 1 \endcode threads, the
		 * extra thread is for monitoring the pool status.
		 *
		 * \note When \p on_shutdown is set to \c shutdown_option_wait_for_tasks, the pool
		 * only waits for the tasks that were queued for immediate execution, all future
		 * tasks are canceled in order to avoid waiting too long.
		 */
		pool (
				unsigned int min_threads            = -1,
				unsigned int max_threads            = 1000,
				unsigned int timeout_add_threads_ms = 100,
				unsigned int timeout_del_threads_ms = 300000,
				shutdown_option on_shutdown         = shutdown_option_wait_for_tasks
			);

		/*!
		 * Cancels all pending tasks in the thread pool, but waits until running
		 * tasks are complete. After that, stops and destroys all the threads in the pool
		 */
		~pool();

		/*!
		 * Stops the pool and subsequently destroys all threads
		 *
		 * \remarks This function should be called only once
		 */
		void stop();

		/*!
		 * Queue an task for immediate execution.
		 *
		 * The task is going to be executed as soon as a thread
		 * is available, if there are no available threads the monitor will create them
		 */
		schedule_result schedule(const task_type& task);

		/*!
		 * Queue a task for execution when the time as reported
		 * by \c boost::get_system_time() would be equal to or later
		 * than the specified \p abs_time
		 *
		 * \note When in very heavy load, the task could not be executed exactly when
		 * it was requested to.
		 */
		schedule_result schedule(const task_type& task, const boost::system_time& abs_time);

		/*!
		 * Queue a task for execution after the period of time indicated
		 * by the \p rel_time argument has elapsed
		 *
		 * \note When in very heavy load, the task could not be executed exactly when
		 * it was requested to.
		 */
		schedule_result schedule(const task_type& task, const boost::posix_time::time_duration& rel_time);

		/*!
		 * \return The number of active tasks in the pool, aka the number of threads
		 * executing user requested tasks
		 */
		unsigned int active_tasks() const;

		/*!
		 * \return The number of tasks waiting for an available thread
		 *
		 * If this number gets too high for a long time you should be worried
		 */
		unsigned int pending_tasks() const;

		/*!
		 * \return The number of tasks that are queued but not ready
		 * to be executed
		 */
		unsigned int future_tasks() const;

		/*!
		 * \return The number of threads in the pool, it's a number
		 * between \c min_threads and \c max_threads (see constructor)
		 */
		unsigned int pool_size() const;

	protected:

		/*!
		 * Called when the pool is resized
		 *
		 * \param delta Specifies how many threads where added or removed from the pool
		 */
		virtual void pool_size_changed( int /*delta*/ )
		{ }

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

#endif // threadpool_pool_hpp__
