/*!
 * \file threadpool.cpp
 * \author ichramm
 * \date June 30, 2012, 2:43 AM
 *
 * Thread Pool class implementation
 */
#include "../threadpool.h"

#include <boost/bind.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/weak_ptr.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition.hpp>
#include <boost/detail/atomic_count.hpp>
#include <boost/thread.hpp>
#include <boost/make_shared.hpp>

#include <string>
#include <queue>

using namespace std;
using namespace boost;

typedef boost::detail::atomic_count atomic_counter;

namespace threadpool
{

const unsigned int MIN_POOL_THREADS         = 8;
const unsigned int MAX_POOL_THREADS         = 1000;
const unsigned int TIMEOUT_ADD_MORE_THREADS = 100;
const unsigned int TIMEOUT_REMOVE_THREADS   = 300*1000;


static const system_time invalid_system_time;


struct pool::impl
{
private:

	/*!
	 * Task object with schedule information
	 */
	class task_impl
	{
	private:
		task_type   _task;
		system_time _schedule;

	public:
		task_impl(task_type task = 0, system_time schedule = invalid_system_time)
		 : _task(task),
		   _schedule(schedule)
		{
		}

		bool is_on_schedule() const
		{
			return (
				_schedule.is_not_a_date_time() ||
				_schedule <= get_system_time()
			);
		}

		void operator()()
		{
			_task();
		}
	};

	/*!
	 * This struct holds the proper thread object and
	 * a Boolean value indicating if the thread is valid or not.
	 * The Boolean value should be checked each time a task is done in order
	 * for the thread to stop.
	 *
	 * Objects of this type shall always be used with the lock acquired
	 */
	struct pool_thread : public enable_shared_from_this<pool_thread>
	{
		typedef shared_ptr<pool_thread> ptr;
		typedef function<void(ptr)>     worker;

		thread*  _thread; /*!< The thread object it self */
		worker   _worker; /*!< The thread function */
		bool     _busy;   /*! Indicates whether the worker is executing a task */

		/*!
		 * Initializes this, creates the thread
		 *
		 * \param worker Function to execute in the thread
		 */
		pool_thread(worker work)
		 : _worker(work)
		{
		}

		/*!
		 * Destroys the thread, waits until the thread ends
		 */
		~pool_thread()
		{
			_thread->join();
			delete _thread;
		}

		/*!
		 * Create the thread and and subsequently starts the worker
		 */
		void run()
		{
			_thread = new thread(_worker, shared_from_this());
			_worker = 0; // _worker is no longer needed
		}

		/*!
		 * Set when a thread is done waiting for call call
		 */
		void set_busy(bool b)
		{
			_busy = b;
		}

		/*!
		 * Use to ask wetter a thread is executing a tasks
		 * or waiting for a new one
		 */
		bool is_busy()
		{
			return _busy;
		}

		/*!
		 * Send interrupt signal to the thread
		 * Threads in idle state (those for \c is_busy will return \c false) should
		 * end (almost) immediately
		 */
		void interrupt()
		{
			_thread->interrupt();
		}

		/*!
		 * Waits until the thread ends
		 */
		void join()
		{
			_thread->join();
		}
	};

	volatile bool      _pool_stop;             /*!< Set when the pool is being destroyed */
	const unsigned int _min_threads;           /*!< Minimum thread count */
	const unsigned int _max_threads;           /*!< Maximum thread count */
	const unsigned int _resize_up_tolerance;   /*!< Milliseconds to wait before creating more threads */
	const unsigned int _resize_down_tolerance; /*!< Milliseconds to wait before deleting threads */
	atomic_counter     _active_tasks;          /*!< Number of active tasks */
	atomic_counter     _thread_count;          /*!< Number of threads in the pool */

	mutex              _tasks_mutex;           /*!< Synchronizes access to the task queue */
	mutex              _threads_mutex;         /*!< Synchronizes access to the pool */
	condition          _tasks_condition;       /*!< Condition to notify when a new task arrives  */
	condition          _monitor_condition;     /*!< Condition to notify the monitor when it has to stop */

	queue<task_impl>       _pending_tasks;  /*!< Task queue */
	list<pool_thread::ptr> _threads;        /*!< List of threads */

public:

	/*!
	 *
	 */
	impl(unsigned int min_threads, unsigned int max_threads,
	 unsigned int timeout_add_threads, unsigned int timeout_del_threads)
	: _pool_stop(false),
	  _min_threads(min_threads == max_threads ? min_threads : min_threads+1),
	  _max_threads(max_threads), // cannot use more than max_threads threads
	  _resize_up_tolerance(timeout_add_threads),
	  _resize_down_tolerance(timeout_del_threads),
	  _active_tasks(0),
	  _thread_count(0)
	{
		assert(_max_threads >= _min_threads);

		while( pool_size() < _min_threads )
		{
			add_thread();
		}

		if ( _min_threads < _max_threads )
		{ // monitor only when the pull can actually be resized
			schedule(bind(&impl::pool_monitor, this), invalid_system_time);
		}
	}

	/*!
	 * Cancels all pending tasks
	 * Destroys the thread pool waiting for all threads to finish
	 */
	~impl()
	{
		_pool_stop = true;

		{
			lock_guard<mutex> lock(_tasks_mutex);

#ifdef __GXX_EXPERIMENTAL_CXX0X__
			queue<task_impl>  tmp;
			_pending_tasks.swap(tmp);
#else
			for ( ; !_pending_tasks.empty(); _pending_tasks.pop());
#endif
			// wake up all threads
			_tasks_condition.notify_all();
		}

		if ( _min_threads < _max_threads )
		{ // wake up the monitor
			_monitor_condition.notify_one();

			// wait until the monitor releases the lock
			lock_guard<mutex> lock(_threads_mutex);
		}

		while ( pool_size() > 0 )
		{
			remove_thread();
		}
	}

	/*!
	 * Schedules a task for execution
	 */
	void schedule(const task_type& task, const system_time& abs_time = invalid_system_time)
	{
		assert(task != 0);

		lock_guard<mutex> lock(_tasks_mutex);

		if ( _pool_stop )
		{
			return;
		}

		_pending_tasks.push(task_impl(task, abs_time));

		//wake up only one thread
		_tasks_condition.notify_one();
	}

	/*!
	 * Returns the number of active tasks. \see worker_thread
	 */
	unsigned int active_tasks()
	{
		return _active_tasks;
	}

	/*!
	 * Return the number of pending tasks, aka the number
	 * of tasks in the queue
	 */
	unsigned int pending_tasks()
	{
		lock_guard<mutex> lock(_tasks_mutex);
		return _pending_tasks.size();
	}

	/*!
	 * Returns the number of threads in the pool
	 *
	 * We use a separate counter because list::size is slow
	 */
	unsigned int pool_size()
	{
		return _thread_count;
	}

private:

	/*!
	 * Adds a new thread to the pool
	 */
	void add_thread()
	{
		pool_thread::ptr t = make_shared<pool_thread>(bind(&impl::worker_thread, this, _1));

		_threads.push_back(t);

		t->run();
		++_thread_count;
	}

	/*!
	 * Removes a thread from the pool but waiting until it ends
	 */
	void remove_thread()
	{
		pool_thread::ptr t = _threads.back();
		_threads.pop_back();

		--_thread_count;
		t->interrupt();
		t->join();
	}

	/*!
	 * Removes \p count idle threads from the pool
	 * This function shall be called with \c _locked_this locked
	 */
	void remove_idle_threads(unsigned int count)
	{ // this function is called locked

		list<pool_thread::ptr>::iterator it = _threads.begin();
		while ( it != _threads.end() && count > 0 )
		{
			pool_thread::ptr &th = *it;

			{
				// don't let it take another task
				lock_guard<mutex> lock(_tasks_mutex);

				if ( th->is_busy() )
				{ // it is executing a task, or at least is not waiting for a new one
					it++;
					continue;
				}

				// it's waiting, will throw thread_interrupted
				th->interrupt();
			}

			--count;
			--_thread_count;
			th->join();
			it = _threads.erase(it);
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
	void worker_thread(pool_thread::ptr t)
	{
		task_impl task;

		for( ; ; )
		{
			{
				mutex::scoped_lock lock(_tasks_mutex);

				if ( _pool_stop )
				{ // check before doing anything
					return;
				}

				for ( ; _pending_tasks.empty(); )
				{
					try
					{
						t->set_busy(false);
						_tasks_condition.wait(lock);
						t->set_busy(true);
					}
					catch ( const thread_interrupted& )
					{ // thread has been canceled
						return;
					}

					if ( _pool_stop )
					{ // stop flag was set while waiting
						return;
					}
				}

				task = _pending_tasks.front();
				_pending_tasks.pop();

				if (task.is_on_schedule() == false)
				{  // the task is not yet ready to execute, it must be re-queued
					_pending_tasks.push(task);

					// check if this one was the only pending task
					if (_pending_tasks.size() == 1)
					{ // sleep the thread for a small amount of time in order
					  //to avoid stressing the CPU
						_tasks_condition.timed_wait(lock, posix_time::microseconds(100));
					}

					continue; // while(true)
				}
			}

			// disable interruption for this thread
			this_thread::disable_interruption di;

			++_active_tasks;
			task();
			--_active_tasks;

			// check if interruption has been requested
			//before checking for new tasks
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
	 *  If the pool is full and there are queued tasks the more
	 * threads are added o the pool. No threads are created until
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
		static const unsigned char RESIZE_UP   = 1; // set when pool size has to be increased
		static const unsigned char RESIZE_DOWN = 2; // set when pool size has to be decrease
		static const float RESIZE_UP_FACTOR    = 1.5; // size increase factor
		static const float RESIZE_DOWN_FACTOR  = 2.0; // size decrease factor

		// milliseconds to sleep between checks
		static const posix_time::milliseconds THREAD_SLEEP_MS(1);

		const unsigned int MAX_STEPS_UP  = max(_resize_up_tolerance, 2u); // make at least 2 steps
		const unsigned int MAX_STEPS_DOWN = max(_resize_down_tolerance, 2u); // can wait, make at least 2 steps

		unsigned char resize_flag = 0;
		unsigned char step_flag;
		unsigned int  step_count = 0; // each step takes 1 ms

		unsigned int next_pool_size;

		mutex::scoped_lock lock(_threads_mutex);

		while( _pool_stop == false )
		{
			if ( active_tasks() == pool_size() && !_pending_tasks.empty() )
			{ // pool is full and there are pending tasks
				step_flag = RESIZE_UP;
			}
			else if ( active_tasks() < pool_size() / 4 )
			{ // at least the 75% of the threads in the pool are idle
				step_flag = RESIZE_DOWN;
			}
			else
			{ // load is greater than 25% but less than 100%, it's ok
				step_flag = 0;
			}

			if ( step_flag != resize_flag )
			{ // changes the resize flag and resets the counter
				step_count = 0;
				resize_flag = step_flag;
			}
			else
			{ // increments the counter
				step_count += 1;

				if ( resize_flag == RESIZE_UP && step_count == MAX_STEPS_UP )
				{ // max steps reached, pool size has to be increased

					next_pool_size = min(_max_threads, unsigned(pool_size()*RESIZE_UP_FACTOR));
					while ( pool_size() < next_pool_size )
					{
						add_thread();
					}

					resize_flag = 0;
					step_count = 0;
				}
				else if ( resize_flag == RESIZE_DOWN && step_count == MAX_STEPS_DOWN )
				{ // max steps reached, stop wasting resources

					next_pool_size = max(_min_threads, unsigned(pool_size()/RESIZE_DOWN_FACTOR));

					remove_idle_threads( pool_size() - next_pool_size );

					resize_flag = 0;
					step_count = 0;
				}
			}

			// if condition is set _pool_stop was set to true
			_monitor_condition.timed_wait(lock, THREAD_SLEEP_MS);
		}
	}
};

pool::pool(unsigned int min_threads, unsigned int max_threads,
 unsigned int timeout_add_threads_ms, unsigned int timeout_del_threads_ms)
 : pimpl(new impl(min_threads, max_threads, timeout_add_threads_ms, timeout_del_threads_ms))
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
