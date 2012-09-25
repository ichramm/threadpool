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

static const system_time invalid_system_time;
static const system_time::time_duration_type null_duration;

/*! Time to sleep to avoid 100% CPU usage */
static const posix_time::milliseconds worker_idle_time(2);


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
	class task_impl
	{
	private:
		task_type   m_task;
		system_time m_schedule;

	public:
		task_impl(task_type task = 0, system_time schedule = invalid_system_time)
		 : m_task(task)
		 , m_schedule(schedule)
		{
		}

		bool is_on_schedule() const
		{
			return (m_schedule.is_not_a_date_time() || m_schedule <= get_system_time());
		}

		const system_time& get_schedule() const
		{
			return m_schedule;
		}

		bool operator>(const task_impl& other) const
		{
			return (!m_schedule.is_not_a_date_time() && m_schedule >= other.m_schedule);
		}

		void operator()()
		{
			m_task();
		}
	};

	/*!
	 * Priority Queue-Based Task scheduler (or task queue)
	 *
	 * The priority queue works better with scheduled tasks because prevents from having too
	 * many awake pool_thread's (polling the queue, checking schedule, waiting).
	 *
	 * With a priority queue a thread will always get the first task in chronological order.
	 *
	 */
	class task_queue
	{
	private:

		/*!
		 * The priority queue
		 *
		 *  By default \c std::priority_queue uses the functor std::less<T> as compactor, and inserts
		 * the elements with greater priority at the beginning of the queue.
		 *
		 *  Given the previous fact we conclude that an element is moved to the beginning of the queue
		 * until \code T::operator<(const T&) \endcode returns \c true, or while the operator
		 * returns \c false.
		 *
		 *  Our algorithm uses the schedule time information to order the elements, and the elements with
		 * smaller schedule should be inserted at the beginning of the queue (because this ones need to
		 * be executed first)
		 *  We use \c std::greater as comparator, so the container will move the element to the beginning
		 * of the queue while \code T::operator>(const T&) \endcode return \c false, aka, until an element
		 * which needs to be executed first is found.
		 *  In other words, \c std::priority_queue inserts at the beginning the elements with greater
		 * priority, we insert the elements with smaller priority at the beginning of the queue.
		 */
		priority_queue <
				task_impl,
				vector<task_impl>,
				greater<task_impl>
			> m_taskQueue;

	public:

		/*!
		 * \return \c true if there are no pending tasks
		 */
		bool empty() const
		{
			return m_taskQueue.empty();
		}

		/*!
		 * Pushes a task into the priority queue
		 */
		void push(const task_impl& task)
		{
			m_taskQueue.push(task);
		}

		/*!
		 * This function pops a task from the queue if there is one
		 *
		 * \param task The popped task, if anything
		 * \param wait_duration The time to wait until task execution if the task is a future
		 * task. If this parameter is equal to \c null_duration then no wait should be performed.
		 *
		 * \return \c true If a task has been popped
		 * \return \c false If the queue is empty (Should no be the case, worker threads
		 * loop while this condition holds)
		 *
		 * \note Worker threads loop while the queue is empty so this function should
		 * not return false in real life.
		 *
		 * \note When wait_duration is set, the worker should wait in the tasks condition because
		 * the thread should not block, allowing new tasks to be executed in the same thread.
		 *
		 * \remarks If the worker decides the task is not going to be executed after the sleep, the
		 * task should be pushed again into the queue. This happens when the threads wakes up because the
		 * tasks condition has been set.
		 *
		 * \remarks If the threads wakes up because of timeout in the condition::timed_wait call, then
		 * the task MUST BE executed.
		 */
		bool pop(task_impl &task, system_time::time_duration_type &wait_duration)
		{
			if ( empty() )
			{ // this is kind of defensive programming
				return false;
			}

			// get the task on the top of the queue and pop it
			task = m_taskQueue.top();
			m_taskQueue.pop();

			// set the wait duration if required
			wait_duration = task.is_on_schedule() ? null_duration : task.get_schedule() - get_system_time();

			return true;
		};

		/*!
		 * Removes all elements from the queue
		 */
		void clear()
		{
			while (!m_taskQueue.empty())
			{
				m_taskQueue.pop();
			}
		}
	};

	/*!
	 * This struct holds the proper thread object and a Boolean value indicating whether
	 * the thread is busy executing a task or not.
	 * The Boolean value should be checked each time a task is done in order
	 * for the thread to stop.
	 *
	 * Objects of this type shall always be used with the lock acquired
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
		 , m_thread(work, this)
		{ }

		/*!
		 * Waits until the thread ends
		 */
		~pool_thread()
		{
			m_thread.join();
		}

		/*!
		 * Set when a thread is done waiting for call call
		 */
		void set_busy(bool b)
		{
			m_busy = b;
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
		return desired_min_threads == desired_max_threads ? desired_min_threads : desired_min_threads + 1;
	}

	volatile bool      m_stopPool;             /*!< Set when the pool is being destroyed */
	const unsigned int m_minThreads;           /*!< Minimum thread count */
	const unsigned int m_maxThreads;           /*!< Maximum thread count */
	const unsigned int m_resizeUpTolerance;    /*!< Milliseconds to wait before creating more threads */
	const unsigned int m_resizeDownTolerance;  /*!< Milliseconds to wait before deleting threads */
	shutdown_option    m_onShutdown;           /*!< How to behave on destruction */
	atomic_counter     m_activeTasks;          /*!< Number of active tasks */
	atomic_counter     m_pendingTasks;         /*!< Number of tasks in the queue. \note We do not use queue::size() */
	atomic_counter     m_threadCount;          /*!< Number of threads in the pool \see http://stackoverflow.com/questions/228908/is-listsize-really-on */

	mutex              m_tasksMutex;           /*!< Synchronizes access to the task queue */
	mutex              m_threadsMutex;         /*!< Synchronizes access to the pool */
	condition          m_tasksCondition;       /*!< Condition to notify when a new task arrives  */
	condition          m_monitorCondition;     /*!< Condition to notify the monitor when it has to stop */

	task_queue             m_taskQueue;        /*!< Task queue */
	list<pool_thread::ptr> m_threads;          /*!< List of threads */

public:

	/*!
	 *
	 */
	impl(unsigned int min_threads, unsigned int max_threads, unsigned int timeout_add_threads,
	     unsigned int timeout_del_threads, shutdown_option on_shutdown)
	 : m_stopPool(false)
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

		if ( m_minThreads < m_maxThreads )
		{ // monitor only when the pool can actually be resized
			schedule(bind(&impl::pool_monitor, this), invalid_system_time);
		}
	}

	/*!
	 * Cancels all pending tasks
	 * Destroys the thread pool waiting for all threads to finish
	 */
	~impl()
	{
		m_stopPool = true;

		if ( m_minThreads < m_maxThreads )
		{ // wake up the monitor
			m_monitorCondition.notify_one();

			// wait until the monitor releases the lock
			lock_guard<mutex> lock(m_threadsMutex);
		}

		if ( m_onShutdown == shutdown_option_cancel_tasks )
		{
			lock_guard<mutex> lock(m_tasksMutex);
			m_taskQueue.clear();
			// wake up all threads
			m_tasksCondition.notify_all();
		}
		else
		{ // m_onShutdown == shutdown_option_wait_for_tasks
			while ( active_tasks() > 0 || pending_tasks() > 0 )
			{ // make sure there are no tasks running and/or pending
				this_thread::sleep(worker_idle_time);
			}
		}

		while ( pool_size() > 0 )
		{ // there is no need to lock here
			remove_thread();
		}
	}

	/*!
	 * Schedules a task for execution
	 */
	void schedule(const task_type& task, const system_time& abs_time = invalid_system_time)
	{
		assert(task != 0);

		lock_guard<mutex> lock(m_tasksMutex);

		if ( m_stopPool )
		{
			return;
		}

		++m_pendingTasks;
		m_taskQueue.push(task_impl(task, abs_time));

		//wake up only one thread
		m_tasksCondition.notify_one();
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
	 * This function must be called with \c m_threadsMutex locked
	 */
	void remove_idle_threads(unsigned int count)
	{ // this function is called locked

		list<pool_thread::ptr>::iterator it = m_threads.begin();
		while ( it != m_threads.end() && count > 0 )
		{
			pool_thread::ptr &th = *it;

			{
				// don't let it take another task
				lock_guard<mutex> lock(m_tasksMutex);

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
		task_impl task;
		system_time::time_duration_type wait_duration;

		for( ; ; )
		{
			{
				mutex::scoped_lock lock(m_tasksMutex);

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

				assert(m_taskQueue.pop(task, wait_duration));
				--m_pendingTasks;

				if ( wait_duration != null_duration )
				{ // got a task with schedule information
					++m_pendingTasks;

					if (m_tasksCondition.timed_wait(lock, wait_duration))
					{ // a new task has been queued and this thread has been signaled
						m_taskQueue.push(task);
						// We must signal another thread or this task could never get executed
						m_tasksCondition.notify_one();
						continue; // for( ; ; )
					}

					// timeout reached, this thread is going to execute the task
					--m_pendingTasks;
				}
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
	 * This function monitors the pool status in order to add or
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
		static const posix_time::milliseconds THREAD_SLEEP_MS(1);

		const posix_time::milliseconds resize_up_ms(max(m_resizeUpTolerance, 2u));
		const posix_time::milliseconds resize_down_ms(max(m_resizeDownTolerance, 2u));

		system_time next_resize;
		resize_flags resize_flag = flag_no_resize;

		mutex::scoped_lock lock(m_threadsMutex);

		while( m_stopPool == false )
		{
			resize_flags step_flag;

			if ( active_tasks() == pool_size() && !m_taskQueue.empty() )
			{ // pool is full and there are pending tasks
				step_flag = flag_resize_up;
			}
			else if ( active_tasks() < pool_size() / 4 )
			{ // at least the 75% of the threads in the pool are idle
				step_flag = flag_resize_down;
			}
			else
			{ // load is greater than 25% but less than 100%, it's ok
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
				unsigned int next_pool_size;
				switch(resize_flag)
				{
				case flag_resize_up:
					next_pool_size = min(m_maxThreads, unsigned(pool_size()*RESIZE_UP_FACTOR));
					while ( pool_size() < next_pool_size )
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
			}

			// if condition is set m_stopPool was set to true
			m_monitorCondition.timed_wait(lock, THREAD_SLEEP_MS);
		}
	}
};

pool::pool(unsigned int min_threads, unsigned int max_threads, unsigned int timeout_add_threads_ms,
           unsigned int timeout_del_threads_ms, shutdown_option on_shutdown)
 : pimpl(new impl(min_threads, max_threads, timeout_add_threads_ms, timeout_del_threads_ms, on_shutdown))
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
