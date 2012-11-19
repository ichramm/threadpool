/*!
 * \file pool.cpp
 * \author ichramm
 * \date June 30, 2012, 2:43 AM
 *
 * Thread Pool class implementation
 */
#include "../pool.h"

#if _MSC_VER > 1000
# pragma warning(push)
// this warning shows when using this_thread::sleep()
# pragma warning(disable: 4244) //warning C4244: 'argument' : conversion from '__int64' to 'long', possible loss of data
#endif
#include <boost/bind.hpp>
#include <boost/weak_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/make_shared.hpp>
#include <boost/thread.hpp>
#include <boost/thread/condition.hpp>
#include <boost/detail/atomic_count.hpp>
#if _MSC_VER > 1000
# pragma warning(pop)
#endif

#include <string>
#include <queue>

using namespace std;
using namespace boost;

typedef boost::detail::atomic_count atomic_counter;

namespace threadpool
{

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
	 * This struct holds the proper thread object and a Boolean value
	 * indicating whether the thread is busy executing a task or not.
	 */
	struct pool_thread : public enable_shared_from_this<pool_thread>
	{
		typedef shared_ptr<pool_thread> ptr;
		typedef function<void(pool_thread*)>     worker;

		bool    m_busy;   /*!< Indicates whether the worker is executing a task */
		thread  m_thread; /*!< The thread object itself */

		/*!
		 * Initializes this, creates the thread and subsequently starts the worker
		 *
		 * \param worker Function to execute in the thread
		 */
		pool_thread(const worker& work)
		 : m_busy(true)
		{ // avoid using this in member initializer list ('this' is used by the worker)
			m_thread = thread(work, this);
		}

		/*!
		 * Waits until the thread ends
		 */
		~pool_thread()
		{
			m_thread.join();
		}

		/*!
		 * The worker calls this function with \code busy = false \endcode when starts
		 * waiting for a new task, and calls it with \code busy = true \endcode when it's done waiting
		 */
		void set_busy(bool busy)
		{
			m_busy = busy;
		}

		/*!
		 * Use to ask whether a thread is executing a task or waiting for a new one
		 */
		bool is_busy() const
		{
			return m_busy;
		}

		/*!
		 * Send interrupt signal to the thread
		 * Threads in idle state (those for \c is_busy will return \c false) should
		 * end (almost) immediately
		 */
		void interrupt()
		{
			m_thread.interrupt();
		}

		/*!
		 * Waits until the thread ends
		 */
		void join()
		{
			m_thread.join();
		}
	};

	/*!
	 * This function computes the starting (and minimum) size of the pool, given the constructor parameters
	 */
	static inline unsigned int compute_min_threads(unsigned int desired_min_threads, unsigned int desired_max_threads)
	{
		if ( desired_min_threads == unsigned(-1) )
		{
			unsigned int candidate_thread_count = thread::hardware_concurrency() * 2;
			if ( candidate_thread_count == 0 )
			{ // information not available, create at least one thread
				candidate_thread_count = 1;
			}
			desired_min_threads = std::min(candidate_thread_count, desired_max_threads);
		}

		return desired_min_threads;
	}

// members
private:
	pool              *m_owner;               /*!< Pool object holding this pimpl */

	volatile bool      m_stopPool;            /*!< Set when the pool is being destroyed */
	const unsigned int m_minThreads;          /*!< Minimum thread count */
	const unsigned int m_maxThreads;          /*!< Maximum thread count */
	const unsigned int m_resizeUpTolerance;   /*!< Milliseconds to wait before creating more threads */
	const unsigned int m_resizeDownTolerance; /*!< Milliseconds to wait before deleting threads */
	shutdown_option    m_onShutdown;          /*!< How to behave on destruction */

	atomic_counter     m_activeTasks;      /*!< Number of active tasks (aka tasks being executed by a worker) */
	atomic_counter     m_pendingTasks;     /*!< Number of tasks in the queue: \code = m_taskQueue.size() + m_futureTasks.size() \endcode */
	atomic_counter     m_threadCount;      /*!< Number of threads in the pool \see http://stackoverflow.com/questions/228908/is-listsize-really-on */

	thread             m_threadMonitor;    /*! A reference to the thread executing the monitor */
	mutex              m_lockTasks;        /*!< Synchronizes access to the immediate-tasks queue */
	mutex              m_lockMonitor;      /*!< Synchronizes access to the pool monitor and future tasks */
	condition          m_tasksCondition;   /*!< Condition to notify when there is a new task for immediate execution */
	condition          m_monitorCondition; /*!< Condition to notify the monitor when it has to stop or there is a new future task */

	list<pool_thread::ptr>  m_threads;     /*!< Holds the threads being managed by this pool instance */
	queue<task_type>        m_taskQueue;   /*!< Immediate-tasks queue, tasks are processed in FIFO manner */
	future_task_queue       m_futureTasks; /*!< Priority queue with future tasks, the monitor pops from this queue and pushes into \c m_taskQueue */

public:

	/*!
	 *
	 */
	impl(pool *owner, unsigned int min_threads, unsigned int max_threads, unsigned int timeout_add_threads,
	     unsigned int timeout_del_threads, shutdown_option on_shutdown)
	 : m_owner(owner)
	 , m_stopPool(false)
	 , m_minThreads(compute_min_threads(min_threads, max_threads))
	 , m_maxThreads(max_threads) // cannot use more than max_threads threads
	 , m_resizeUpTolerance(timeout_add_threads)
	 , m_resizeDownTolerance(timeout_del_threads)
	 , m_onShutdown(on_shutdown)
	 , m_activeTasks(0)
	 , m_pendingTasks(0)
	 , m_threadCount(0)
	{
		assert(m_maxThreads >= m_minThreads);

		while( pool_size() < m_minThreads )
		{
			add_thread();
		}

		m_threadMonitor = thread(&impl::pool_monitor, this);
	}

	/*!
	 * Destroys the thread pool waiting for all threads to finish, canceling all
	 * pending tasks if required
	 */
	~impl()
	{
		if ( m_onShutdown == shutdown_option_cancel_tasks )
		{ // this is the default option
			m_stopPool = true;

			lock_guard<mutex> lock(m_lockTasks);

			// delete all pending tasks
			clear_queue(m_taskQueue);
			clear_queue(m_futureTasks);

			// wake up all threads
			m_tasksCondition.notify_all();
		}
		else
		{ // m_onShutdown == shutdown_option_wait_for_tasks
			while ( active_tasks() > 0 || pending_tasks() > 0 )
			{ // make sure there are no tasks running and/or pending
				this_thread::sleep(worker_idle_time);
			}

			// set stop flag only when all tasks are complete
			m_stopPool = true;
		}

		// wake up the monitor, the monitor must stop only when all tasks are
		// done (or canceled), otherwise future tasks will be ignored
		m_monitorCondition.notify_one();
		// make sure the monitor ends properly
		m_threadMonitor.join();

		while ( pool_size() > 0 )
		{
			remove_thread();
		}
	}

	/*!
	 * Schedules a task for immediate execution
	 */
	void schedule(const task_type& task)
	{
		assert(task != 0);

		lock_guard<mutex> lock(m_lockTasks);

		if ( m_stopPool )
		{
			return;
		}

		++m_pendingTasks;
		internal_schedule(task);
	}

	/*!
	 * Schedules a task for future execution
	 */
	void schedule(const task_type& task, const system_time& abs_time)
	{
		assert(task != 0);

		lock_guard<mutex> lock(m_lockMonitor);

		if ( m_stopPool )
		{
			return;
		}

		++m_pendingTasks;
		m_futureTasks.push(future_task(task, abs_time));

		m_monitorCondition.notify_one();
	}

	/*!
	 * Returns the number of active tasks. \see worker_thread
	 */
	unsigned int active_tasks()
	{
		return m_activeTasks;
	}

	/*!
	 * Return the number of pending tasks, aka the number
	 * of tasks in the queue
	 */
	unsigned int pending_tasks()
	{
		return m_pendingTasks;
	}

	/*!
	 * Returns the number of threads in the pool
	 *
	 * We use a separate counter because list::size is slow
	 */
	unsigned int pool_size()
	{
		return m_threadCount;
	}

private:

	/*!
	 * Adds a new thread to the pool
	 */
	void add_thread()
	{
		pool_thread::ptr t = make_shared<pool_thread>(bind(&impl::worker_thread, this, _1));

		m_threads.push_back(t);
		++m_threadCount;
	}

	/*!
	 * Removes a thread from the pool, possibly waiting until it ends
	 */
	void remove_thread()
	{
		pool_thread::ptr t = m_threads.back();
		m_threads.pop_back();

		--m_threadCount;
		t->interrupt();
		t->join();
	}

	/*!
	 * Removes \p count idle threads from the pool
	 */
	void remove_idle_threads(unsigned int count)
	{ // this function is called locked

		list<pool_thread::ptr>::iterator it = m_threads.begin();
		while ( !m_stopPool && it != m_threads.end() && count > 0 )
		{
			pool_thread::ptr &th = *it;

			{
				// don't let it take another task
				lock_guard<mutex> lock(m_lockTasks);

				if ( th->is_busy() )
				{ // it is executing a task, or at least is not waiting for a new one
					it++;
					continue;
				}

				// it's waiting, will throw thread_interrupted
				th->interrupt();
			}

			--count;
			--m_threadCount;
			th->join();
			it = m_threads.erase(it);
		}
	}

	/*!
	 * Pushes a tasks to the pending tasks queue and wakes up any worker thread
	 */
	void internal_schedule(const task_type& task)
	{
		m_taskQueue.push(task);
		m_tasksCondition.notify_one();
	}

	/*!
	 * This function loops forever polling the task queue
	 * for a task to execute.
	 *
	 * The function exists when the thread has been canceled or
	* when the pool is stopping. It holds a reference
	* to \c pool_thread::ptr so destruction is done safely
	 *
	 * \li Threads are canceled by the pool monitor
	 * \li The pool stops when it's being destroyed
	 */
	void worker_thread(pool_thread *t)
	{
		task_type task;

		for( ; ; )
		{
			{
				mutex::scoped_lock lock(m_lockTasks);

				if ( m_stopPool )
				{ // check before doing anything
					return;
				}

				// wait inside loop to cope with spurious wakes, see http://goo.gl/Oxv6T
				while( m_taskQueue.empty() )
				{
					try
					{
						t->set_busy(false);
						m_tasksCondition.wait(lock);
						t->set_busy(true);
					}
					catch ( const thread_interrupted& )
					{ // thread has been canceled
						return;
					}

					if ( m_stopPool )
					{ // stop flag was set while waiting
						return;
					}
				}

				task = m_taskQueue.front();
				m_taskQueue.pop();
				--m_pendingTasks;

				assert(task != 0);
			}

			// disable interruption for this thread
			this_thread::disable_interruption di;

			++m_activeTasks;
			task();
			--m_activeTasks;

			// check if interruption has been requested before checking for new tasks
			// this should happen only when the pool is stopping
			if ( this_thread::interruption_requested() )
			{
				return;
			}
		}
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
	{
		system_time result = not_a_date_time;
		lock_guard<mutex> lock(m_lockTasks);

		while ( !m_futureTasks.empty() )
		{
			const future_task& task = m_futureTasks.top();
			const system_time& task_schedule = task.get_schedule();

			if ( task_schedule > get_system_time() )
			{ // if this task is not ready then no one is
				result = task_schedule;
				break;
			}

			internal_schedule(task.get_task());
			m_futureTasks.pop();
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

		// milliseconds to sleep between checks
		static const posix_time::milliseconds THREAD_SLEEP_MS(2);

		const posix_time::milliseconds resize_up_ms(max(m_resizeUpTolerance, 2u));
		const posix_time::milliseconds resize_down_ms(max(m_resizeDownTolerance, 2u));

		system_time next_resize;
		unsigned int pendingTasks;
		resize_flags resize_flag = flag_no_resize;

		mutex::scoped_lock lock(m_lockMonitor);

		while( m_stopPool == false )
		{
			// check future tasks
			system_time next_task_time = poll_future_tasks(pendingTasks);

			// we don't need the lock anymore (allows to schedule future tasks)
			lock.unlock();

			if (m_minThreads < m_maxThreads )
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
					if (step_flag != flag_no_resize)
					{
						next_resize = get_system_time() + (resize_flag == flag_resize_up ? resize_up_ms : resize_down_ms);
					}
				}
				else if (step_flag != flag_no_resize && get_system_time() >= next_resize)
				{ // pool needs to be resized
					unsigned int next_pool_size, prev_pool_size = pool_size();
					switch(resize_flag)
					{
					case flag_resize_up:
						next_pool_size = min(m_maxThreads, unsigned(pool_size()*RESIZE_UP_FACTOR));
						while ( !m_stopPool && pool_size() < next_pool_size )
						{
							add_thread();
						}
						break;

					case flag_resize_down:
						next_pool_size = max(m_minThreads, unsigned(pool_size()/RESIZE_DOWN_FACTOR));
						remove_idle_threads( pool_size() - next_pool_size );
						break;

					default:
						break;
					}

					resize_flag = flag_no_resize;
					m_owner->pool_size_changed( pool_size() - prev_pool_size );
				}
			}

			// Now we wait, the wait time must be a smart value in order to guarantee not only the
			// creation of new threads, but also the proper schedule of new tasks
			system_time next_wakeup = get_system_time() + THREAD_SLEEP_MS;
			lock.lock();
			m_monitorCondition.timed_wait( lock,
					next_task_time.is_not_a_date_time() ? next_wakeup : min(next_task_time, next_wakeup)
				);
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

void pool::schedule(const task_type& task)
{
	pimpl->schedule(task);
}

void pool::schedule(const task_type& task, const boost::system_time &abs_time)
{
	pimpl->schedule(task, abs_time);
}

void pool::schedule(const task_type& task, const boost::posix_time::time_duration &rel_time)
{
	pimpl->schedule(task, get_system_time() + rel_time);
}

unsigned int pool::active_tasks()
{
	return pimpl->active_tasks();
}

unsigned int pool::pending_tasks()
{
	return pimpl->pending_tasks();
}

unsigned int pool::pool_size()
{
	return pimpl->pool_size();
}

} // namespace threadpool
