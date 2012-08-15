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
const unsigned int TIMEOUT_REMOVE_THREADS   = 120*1000;

struct pool::impl
{
private:

	/*!
	 * This struct holds the propper thread object and
	 * a boolean value indicating if the thread is valid or not.
	 * The boolean value should be checked each time a task is done in order
	 * for the thread to stop.
	 *
	 * Objects of this type shall always be used with the lock acquired
	 */
	struct pool_thread : public enable_shared_from_this<pool_thread>
	{
		typedef shared_ptr<pool_thread> ptr;
		typedef function<void(ptr)>     worker_t;

		thread*       _thread; /*!< The thread object it self */
		worker_t      _worker; /*!< The thread function */
		bool          _busy;   /*! Indicates whether the worker is executing a task */

		/*!
		 * Initializes this, creates the thread
		 *
		 * \param worker Function wo execute in the thread
		 */
		pool_thread(worker_t worker)
		 : _worker(worker)
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
		void setBusy(bool b)
		{
			_busy = b;
		}

		/*!
		 * Use to ask whetter a thread is executing a tasks
		 * or waiting for a new one
		 */
		bool isBusy()
		{
			return _busy;
		}

		/*!
		 * Send interrupt signal to the thread
		 * Threads in idle state (those for \c isBusy will return \c false) should
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

	volatile bool      _pool_stop;             /*!< Set when the pool is being destoyed */
	unsigned int       _min_threads;           /*!< Minimum thread count */
	unsigned int       _max_threads;           /*!< Maximum thread count */
	unsigned int       _resize_up_tolerance;   /*!< Milliseconds to wait before creating more threads  */
	unsigned int       _resize_down_tolerance; /*!< Milliseconds to wait before deleting threads */
	atomic_counter     _active_tasks;          /*!< Number of active tasks */
	atomic_counter     _thread_count;          /*!< Number of threads in the pool */

	list<pool_thread::ptr> _threads;           /*!< List of threads */
	queue<task_type>       _pending_tasks;     /*!< Task queue */
	mutex                  _tasks_mutex;       /*!< Synchronizes access to the task queue */
	mutex                  _threads_mutex;     /*!< Synchronizes access to the pool */
	condition              _tasks_condition;   /*!< Condition to notify when a new task arrives  */
	condition              _monitor_condition; /*!< Condition to notify the monitor when it has to stop */

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

		if ( _min_threads < _max_threads)
		{ // monitor only when the pull can actually be resized
			exec(bind(&impl::pool_monitor, this));
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
			queue<task_type>  tmp;
			_pending_tasks.swap(tmp);
#else
			for ( ; !_pending_tasks.empty(); _pending_tasks.pop());
#endif
			// wake up all threads
			_tasks_condition.notify_all();
		}

		// wake up the monitor
		{
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
	void exec(const task_type& task)
	{
		lock_guard<mutex> lock(_tasks_mutex);

		if ( _pool_stop )
		{
			return;
		}

		assert(task != 0);

		_pending_tasks.push(task);

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
	 * Return the number os pending tasks, aka the number
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
	 * Removes a thread from the pool
	 * \param detach Indicates if the thread must be detached before deleted. If the thread is
	 * not detached then this function waits until the thread ends
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

			if ( th->isBusy() )
			{ // t is executing a task, or at least is not waiting for a new one
				it++;
				continue;
			}

			--count;
			--_thread_count;
			th->interrupt();
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
		// loop until get_next_task returns false or the thread has been interrupted
		// get_next_task blocks if there are no pending tasks
		while ( true )
		{
			task_type task;

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
						t->setBusy(false);
						_tasks_condition.wait(lock);
						t->setBusy(true);
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
			}

			// disable interruption for this thread
			this_thread::disable_interruption di;

			++_active_tasks;
			task();
			--_active_tasks;

			// check if interrupion has been requested
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
	 * If the pool is full and there are queued tasks the more
	 * threads are added o the pool. No threads are created until
	 * a configurable period of loading has passed.
	 *
	 * If the pool is idle then threads are removed from the pool,
	 * ut this time the waiting period is longer, it helps to
	 * avoid adding and removing threads all time. The period to wait
	 * until the pool size is decreased is far longer than the period to
	 * wait until increasing the pool size. The reason is obvious.
	 */
	void pool_monitor()
	{
		static const unsigned char RESIZE_UP   = 1; // set when pool size has to be increased
		static const unsigned char RESIZE_DOWN = 2; // set when pool size has to be decrease
		static const float RESIZE_UP_FACTOR    = 1.5; // size increase factor
		static const float RESIZE_DOWN_FACTOR  = 2.0; // size decrease factor

		// millis to sleep between checks
		static const posix_time::milliseconds THREAD_SLEEP_MS(1);

		const unsigned int MAX_STEPS_UP  = max(_resize_up_tolerance, 2u); // make at leats 2 steps
		const unsigned int MAX_STEPS_DOWN = max(_resize_down_tolerance, 2u); // can wait, make at leats 2 steps

		unsigned char resize_flag = 0;
		unsigned int  step_count = 0; // each step takes 1 ms

		unsigned int step_flag;
		unsigned int next_pool_size;


		mutex::scoped_lock lock(_threads_mutex);

		while( _pool_stop == false )
		{
			if ( active_tasks() == pool_size() && !_pending_tasks.empty() )
			{
				step_flag = RESIZE_UP;
			}
			else if ( active_tasks() < pool_size() / 4 )
			{
				step_flag = RESIZE_DOWN;
			}
			else
			{
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

					resize_flag = step_count = 0;
				}
				else if ( resize_flag == RESIZE_DOWN && step_count == MAX_STEPS_DOWN )
				{ // max steps reached, stop wasting resources

					next_pool_size = max(_min_threads, unsigned(pool_size()/RESIZE_DOWN_FACTOR));

					remove_idle_threads( pool_size() - next_pool_size );

					resize_flag = step_count = 0;
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

pool::~pool()
{
}

void pool::exec(const task_type& task)
{
	pimpl->exec(task);
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
