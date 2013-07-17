/*!
 * \file pool.cpp
 * \author ichramm
 * \date June 30, 2012, 2:43 AM
 *
 * Thread Pool class implementation
 */
#include "../pool.hpp"
#include "../utils/concurrent_queue.hpp"

#if _MSC_VER > 1000
# pragma warning(push)
// this warning shows when using this_thread::sleep()
# pragma warning(disable: 4244) //warning C4244: 'argument' : conversion from '__int64' to 'long', possible loss of data
#endif
#include <boost/bind.hpp>
#include <boost/thread.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/make_shared.hpp>
#include <boost/thread/condition.hpp>
#if _MSC_VER > 1000
# pragma warning(pop)
#endif

#include <queue>
#include <vector>

using namespace std;
using namespace boost;

namespace threadpool
{

using utils::atomic_counter;
using utils::concurrent_queue;

/*! A special initial value for \c system_time objects */
static const system_time not_a_date_time;

/*! Time to sleep to avoid 100% CPU usage */
static const posix_time::milliseconds worker_idle_time(2);

/*!
 * Helper function to remove all elements from a queue
 */
template <class queue_t> static inline void clear_queue(queue_t &q)
{
	while ( !q.empty() )
	{
		q.pop();
	}
}

/*!
 * Pool implementation, pimpl idiom
 */
struct pool::impl
{
private:

	/*!
	 * Internal flags used by the pool monitor
	 */
	enum resize_flags
	{
		flag_no_resize,
		flag_resize_up,
		flag_resize_down
	};

	/*!
	 * Internal flags used to represent the pool state
	 */
	enum pool_state
	{
		pool_state_active,
		pool_state_stopping,
		pool_state_down
	};

	/*!
	 * Task object with schedule information
	 */
	class future_task
	{
	private:
		task_type   m_task;
		system_time m_schedule;

	public:

		/*!
		 * Initializes a future task
		 *
		 * \param task The task to execute
		 * \param schedule When to execute the task
		 */
		future_task(const task_type& task, const system_time& schedule)
		 : m_task(task)
		 , m_schedule(schedule)
		{
		}

		/*!
		 * \return The schedule configured for the task
		 */
		const system_time& get_schedule() const
		{
			return m_schedule;
		}

		/*!
		 * \return The task object
		 */
		const task_type& get_task() const
		{
			return m_task;
		}
	};

	/*!
	 * Defines a comparison function for future tasks
	 */
	struct task_comparator
	{
		/*!
		 * \return \c true if the first argument goes before the second argument in the priority queue.
		 *
		 * The comparison function is used to sort elements from lower to higher priority:
		 *   [ p1, p2, ..., pn ]
		 * Given two tasks, the task scheduled to run at a greater date will have a lower
		 * priority, so we need to sort in this way:
		 *   [ far_away_task, no_so_near_task, nearest_task ]
		 *
		 * This functor is called by \c std::push_heap, which is called by \c priority_queue::push.
		 *
		 * \see http://www.cplusplus.com/reference/stl/priority_queue/push/
		 * \see http://www.cplusplus.com/reference/algorithm/push_heap/
		 */
		bool operator()(const future_task& first, const future_task& second)
		{
			return first.get_schedule() >= second.get_schedule();
		}
	};

	/*!
	 * A priority queue for future tasks
	 */
	typedef priority_queue <
			future_task,           // the object being stored
			vector<future_task>,   // the underlying container
			task_comparator        // the comparator
		> future_task_queue;

	/*!
	 * This function computes the starting (and minimum) size of the pool, given the constructor parameters
	 */
	static inline unsigned int compute_min_threads(unsigned int desired_min_threads, unsigned int desired_max_threads)
	{
		if ( desired_min_threads == unsigned(-1) )
		{
			unsigned int candidate_thread_count = thread::hardware_concurrency() * 2;
			if ( candidate_thread_count == 0 )
			{ // information not available, create at least two thread
				candidate_thread_count = 2; // hardware_concurrency * 2
			}
			desired_min_threads = std::min(candidate_thread_count, desired_max_threads);
		}

		return desired_min_threads;
	}

// members
private:
	/*! Pool object holding this pimpl */
	pool                 *m_owner;
	/*! Current pool state */
	volatile pool_state   m_state;
	/*! Minimum thread count */
	const unsigned int    m_MIN_POOL_THREADS;
	/*! Maximum thread count */
	const unsigned int    m_MAX_POOL_THREADS;
	/*! Milliseconds to wait before creating more threads */
	const unsigned int    m_TOLERANCE_RESIZE_UP;
	/*! Milliseconds to wait before deleting threads */
	const unsigned int    m_TOLERANCE_RESIZE_DOWN;
	/*! How to behave on destruction */
	const shutdown_option m_SHUTDOWN_STRATEGY;

	/* Counters */

	/*! The number of tasks being executed */
	mutable atomic_counter m_activeTaskCount;
	/*! The number of tasks scheduled for future execution */
	mutable atomic_counter m_futureTaskCount;
	/*! The number of threads in the pool */
	mutable atomic_counter m_threadCount;

	/* Sync */

	/*! A reference to the thread executing the monitor */
	thread     m_threadMonitor;
	/*!< Protects some non-atomic variables, synchronizes this */
	mutex      m_lockMonitor;
	/*! Wake-ups the monitor when stopping or when there is a new future task */
	condition  m_monitorCondition;

	/* Task Containers */

	/*! Immediate-tasks queue, tasks are processed in FIFO manner */
	concurrent_queue<task_type> m_taskQueue;
	/*! Priority queue with future tasks, the monitor pops from this queue and pushes into \c m_taskQueue */
	future_task_queue           m_futureTasks;

public:

	/*!
	 *
	 */
	impl(pool *owner, unsigned int min_threads, unsigned int max_threads, unsigned int timeout_add_threads,
	     unsigned int timeout_del_threads, shutdown_option on_shutdown)
	 : m_owner(owner)
	 , m_state(pool_state_active)
	 , m_MIN_POOL_THREADS(compute_min_threads(min_threads, max_threads))
	 , m_MAX_POOL_THREADS(max_threads) // cannot use more than max_threads threads
	 , m_TOLERANCE_RESIZE_UP(timeout_add_threads)
	 , m_TOLERANCE_RESIZE_DOWN(timeout_del_threads)
	 , m_SHUTDOWN_STRATEGY(on_shutdown)
	 , m_activeTaskCount(0)
	 , m_futureTaskCount(0)
	 , m_threadCount(0)
	{
		assert(m_MAX_POOL_THREADS >= m_MIN_POOL_THREADS);

		start_threads(m_MIN_POOL_THREADS);
		m_threadMonitor = thread(&impl::pool_monitor, this);
	}

	/*!
	 * Destroys the thread pool waiting for all threads to finish, canceling all
	 * pending tasks if required
	 */
	~impl()
	{
		if (m_state == pool_state_active)
		{
			stop();
		}
	}

	/*!
	 * Called to explicitly stop the pool
	 */
	void stop()
	{
		{ // future task must be canceled anyway
			lock_guard<mutex> lock(m_lockMonitor);

			if  (m_state != pool_state_active)
			{
				return;
			}

			m_state = pool_state_stopping;
			clear_queue(m_futureTasks);
			m_monitorCondition.notify_one();
		}

		// wait for the monitor
		m_threadMonitor.join();

		if ( m_SHUTDOWN_STRATEGY == shutdown_option_cancel_tasks )
		{ // in case there are some pending tasks
			m_taskQueue.clear();
		}

		// now nobody's messing with the threads... the pools it's (almost) down
		m_state = pool_state_down;

		// I can safely queue empty tasks, one for each thread
		// I now the pool size will no be increased...
		request_stop(pool_size());

		while ( pool_size() > 0 )
		{ // now wait until all threads are finished
		// m_threadCount is decreased after the associated thread exits
			this_thread::sleep(worker_idle_time);
		}
	}

	/*!
	 * Schedules a task for immediate execution
	 */
	schedule_result schedule(const task_type& task)
	{
		assert(task != 0);

		if ( m_state != pool_state_active )
		{ // should not happen, but we'd better be ready
			return schedule_result_err_down;
		}

		m_taskQueue.push(task); // will wake-up a thread
		m_monitorCondition.notify_one();

		return schedule_result_ok;
	}

	/*!
	 * Schedules a task for future execution
	 */
	schedule_result schedule(const task_type& task, const system_time& abs_time)
	{
		assert(task != 0);

		lock_guard<mutex> lock(m_lockMonitor);

		if ( m_state != pool_state_active )
		{ // should not happen, but we'd better be ready
			return schedule_result_err_down;
		}

		++m_futureTaskCount;
		m_futureTasks.push( future_task(task, abs_time) );
		m_monitorCondition.notify_one();

		return schedule_result_ok;
	}

	/*!
	 * This function is used to check if the pool can accept tasks
	 */
	bool is_active() const
	{
		return (m_state == pool_state_active);
	}

	/*!
	 * Returns the number of active tasks. \see worker_thread
	 */
	unsigned int active_tasks() const
	{
		return m_activeTaskCount;
	}

	/*!
	 * Returns the number of pending tasks, aka the number
	 * of tasks in the queue
	 */
	unsigned int pending_tasks() const
	{
		return m_taskQueue.size();
	}

	/*!
	 * Returns the number of future tasks
	 *
	 * \note future tasks are not considered as 'pending'
	 */
	unsigned int future_tasks() const
	{
		return m_futureTaskCount;
	}

	/*!
	 * Returns the number of threads in the pool
	 */
	unsigned int pool_size() const
	{
		return m_threadCount;
	}

private:

	/*!
	 * Starts \p count threads
	 */
	void start_threads(unsigned int count)
	{
		for ( ; count > 0; --count )
		{ // create and detach thread
			boost::thread t(&impl::worker_thread, this);
		}
	}

	/*!
	 * Posts as many empty tasks as in \p count
	 */
	void request_stop(unsigned int count)
	{
		const task_type empty_task = 0;

		for ( ; count > 0; --count )
		{ // queue es many empty tasks as threads have to remove
			m_taskQueue.push( empty_task );
		}

		// there is no need to wait since
		//all those threads were doing nothing
	}

	/*!
	 * Called by \c worker_thread when it's starting
	 */
	void on_thread_start()
	{
		++m_threadCount;
	}

	/*!
	 * Called by \c worker_thread when it's about to exit
	 */
	void on_thread_exit()
	{
		--m_threadCount;
	}

	/*!
	 * This function loops forever polling the task queue for a task to execute.
	 *
	 * The function exits when popping an empty task
	 */
	void worker_thread()
	{
		on_thread_start();

		for ( ; ; )
		{
			task_type task = m_taskQueue.pop();

			if ( task.empty() )
			{ // done with this thread
				break;
			}

			++m_activeTaskCount;
			task();
			--m_activeTaskCount;
		}

		on_thread_exit();
	}

	/*!
	 * Checks the list of scheduled tasks
	 *
	 * \return The \c system_time programmed for the nearest future task, or \c not_a_date_time
	 * if the are no future tasks.
	 *
	 * \param pendingTasks Returns how many tasks are waiting for an available thread
	 */
	system_time poll_future_tasks(unsigned int &pendingTasks)
	{ // this function runs with the lock in m_lockMonitor
		system_time result = not_a_date_time;

		while ( !m_futureTasks.empty() )
		{
			const future_task& task = m_futureTasks.top();
			const system_time& task_schedule = task.get_schedule();

			if ( task_schedule > get_system_time() )
			{ // if this task is not ready then no one is
				result = task_schedule;
				break;
			}

			m_taskQueue.push(task.get_task());
			m_futureTasks.pop();
			--m_futureTaskCount;
		}

		pendingTasks = m_taskQueue.size();
		return result;
	}

	/*!
	 *  This function monitors the pool status in order to add or
	 * remove threads depending on the load.
	 *
	 *  If the pool is full and there are queued tasks then more
	 * threads are added to the pool. No threads are created until
	 * a configurable period of heavy load has passed.
	 *
	 *  If the pool is idle then threads are removed from the pool,
	 * but this time the waiting period is longer, it helps to
	 * avoid adding and removing threads all time.
	 *  The period to wait until the pool size is decreased is far
	 * longer than the period to wait until increasing the
	 * pool size.
	 */
	void pool_monitor()
	{
		static const float RESIZE_UP_FACTOR    = 1.5; // size increase factor
		static const float RESIZE_DOWN_FACTOR  = 2.0; // size decrease factor

		// milliseconds to sleep between checks when there's nothing to do
		static const posix_time::milliseconds long_sleep_ms(10000);

		const posix_time::milliseconds resize_up_ms(max(m_TOLERANCE_RESIZE_UP, 5u));
		const posix_time::milliseconds resize_down_ms(max(m_TOLERANCE_RESIZE_DOWN, 1000u));

		unsigned int pendingTasks;
		system_time next_resize = not_a_date_time;
		resize_flags resize_flag = flag_no_resize;

		mutex::scoped_lock lock(m_lockMonitor);

		while( is_active() )
		{
			// check future tasks
			system_time next_task_time = poll_future_tasks(pendingTasks);

			// we don't need the lock anymore (allows to schedule future tasks)
			lock.unlock();

			if (m_MIN_POOL_THREADS < m_MAX_POOL_THREADS)
			{ // monitor only when the pool can actually be resized
				resize_flags step_flag;

				if ( active_tasks() == pool_size() && pendingTasks  > 0 )
				{ // pool is full and there are pending tasks
					step_flag = flag_resize_up;
				}
				else if ( active_tasks() < pool_size() / 4 )
				{ // at least the 75% of the threads in the pool are idle
					step_flag = flag_resize_down;
				}
				else
				{ // load is greater than 25% but less than 100%, it's OK
					step_flag = flag_no_resize;
				}

				if ( step_flag != resize_flag )
				{ // flag changed, reset next resize time
					resize_flag = step_flag;
					if (resize_flag == flag_resize_up && pool_size() < m_MAX_POOL_THREADS)
					{ // only if we can actually add threads
						next_resize = get_system_time() + resize_up_ms;
					}
					else if (resize_flag == flag_resize_down && pool_size() > m_MIN_POOL_THREADS)
					{ // only if we can actually remove threads
						next_resize = get_system_time() + resize_down_ms;
					}
					else if (step_flag != flag_no_resize) // just for show
					{ // nothing to be done
						resize_flag = flag_no_resize;
					}
				}

				// not we check if a resize must be performed
				if (resize_flag != flag_no_resize && get_system_time() >= next_resize)
				{ // pool needs to be resized
					unsigned int next_pool_size, prev_pool_size = pool_size();
					switch(resize_flag)
					{
					case flag_resize_up:
						next_pool_size = min(m_MAX_POOL_THREADS, static_cast<unsigned int>(pool_size() * RESIZE_UP_FACTOR));
						start_threads(next_pool_size - prev_pool_size);
						break;

					case flag_resize_down:
						next_pool_size = max(m_MIN_POOL_THREADS, static_cast<unsigned int>(pool_size() / RESIZE_DOWN_FACTOR));
						request_stop(pool_size() - next_pool_size );
						break;

					default:
						next_pool_size = 0; // just make the compiler happy
						break;
					}

					resize_flag = flag_no_resize;
					next_resize = not_a_date_time; // just for show 'cause we always test against resize_flag
					m_owner->pool_size_changed( next_pool_size - prev_pool_size );
				}

				lock.lock();

				// choose how to sleep according to pool status
				if (resize_flag == flag_no_resize && next_task_time.is_not_a_date_time())
				{ // everything "seems" normal, and there are no future tasks
					if (step_flag == flag_resize_down)
					{ // the step told us to remove threads, but the pool is at minimum
						m_monitorCondition.wait(lock); // until someone wakes us up
					}
					else
					{ //  everything is normal, or the pool is full and we cannot add more threads
						m_monitorCondition.timed_wait(lock, long_sleep_ms);
					}
				}
				else if (resize_flag == flag_no_resize)
				{ // everything "seems" normal and there is at least one future task, go on
					m_monitorCondition.timed_wait(lock, next_task_time);
				}
				else
				{ // a flag is set, or there's a future task in the queue, sleep as minimum as possible
					m_monitorCondition.timed_wait(lock,
							next_task_time.is_not_a_date_time() ? next_resize : min(next_task_time, next_resize)
						);
				}
			}
			else
			{ // pool cannot be resized, just wait for next future task
				lock.lock();
				if (next_task_time.is_not_a_date_time())
				{ // just wait until the next call to schedule()
					m_monitorCondition.wait(lock);
				}
				else
				{ // wait until task is in schedule
					m_monitorCondition.timed_wait(lock, next_task_time);
				}
			}
		}
	}
};

pool::pool(unsigned int min_threads, unsigned int max_threads, unsigned int timeout_add_threads_ms,
	unsigned int timeout_del_threads_ms, shutdown_option on_shutdown)
 : pimpl(new impl(this, min_threads, max_threads, timeout_add_threads_ms, timeout_del_threads_ms, on_shutdown))
{
}

pool::pool( const pool& )
 : enable_shared_from_this<pool>()
{ // private
}

const pool& pool::operator=( const pool& )
{ // private
	return *this;
}

pool::~pool()
{
}

void pool::stop()
{
	pimpl->stop();
}

schedule_result pool::schedule(const task_type& task)
{
	return pimpl->schedule(task);
}

schedule_result pool::schedule(const task_type& task, const boost::system_time &abs_time)
{
	return pimpl->schedule(task, abs_time);
}

schedule_result pool::schedule(const task_type& task, const boost::posix_time::time_duration &rel_time)
{
	return pimpl->schedule(task, get_system_time() + rel_time);
}

unsigned int pool::active_tasks() const
{
	return pimpl->active_tasks();
}

unsigned int pool::pending_tasks() const
{
	return pimpl->pending_tasks();
}

unsigned int pool::future_tasks() const
{
	return pimpl->future_tasks();
}

unsigned int pool::pool_size() const
{
	return pimpl->pool_size();
}

} // namespace threadpool
