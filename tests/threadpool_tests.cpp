
#include <threadpool/pool.h>

#include <stdio.h>
#include <math.h>
#include <string>
#include <iostream>
#include <boost/bind.hpp>
#include <boost/thread.hpp>

#include <boost/detail/atomic_count.hpp>
typedef boost::detail::atomic_count atomic_counter;

#ifdef _WIN32
#include <Windows.h>
#define usleep(us)   ::Sleep(us/1000)
#define pthread_self ::GetCurrentThreadId
#endif

using namespace std;
using namespace boost;

void test_function(bool print, string message, unsigned int index, unsigned int wait_time_ms = 0)
{
	if (print) {
		printf("test_function %s: index %d, thread %#lX\n", message.c_str(), index, pthread_self());
	}

	if (wait_time_ms) {
		usleep(1000*wait_time_ms);
	}
}

void test_wait_until_idle()
{
	printf("test_wait_until_idle: Start\n");
	threadpool::pool p(
				-1,
				1000
		);
	for (unsigned i = 1; i <= 200; i++) {
		p.schedule(bind(&test_function, true, "test_wait_until_idle",  i, 100));
	}
	while(p.pending_tasks() > 0 || (!p.pending_tasks() && p.active_tasks()>0)) {
		usleep(1000);
	}
	printf("test_wait_until_idle: End\n");
}

void test_wait_until_resize()
{
	printf("test_wait_until_resize: Start\n");
	unsigned int pool_size = 8;
	threadpool::pool p(pool_size, 100, 100, 3000);
	for (unsigned i = 1; i <= 200; i++) {
		p.schedule(bind(&test_function, true, "test_wait_until_resize",  i, 100));
	}
	while(p.pending_tasks() > 0 || (!p.pending_tasks() && p.active_tasks()>0)) {
		usleep(1000);
	}
	pool_size += 1;
	int counter = 0;
	while (p.pool_size() > pool_size) {
		usleep(1000);
		counter += 1;
		if (counter == 1000) {
			printf("Pool size is still %u\n", p.pool_size());
			counter = 0;
		}
	}
	printf("test_wait_until_resize: End\n");
}

void test_queue_until_pool_size()
{
	printf("test_queue_until_pool_size: Start\n");
	unsigned int pool_size = 100;
	threadpool::pool p(100);
	for (unsigned i = 1; i <= pool_size; i++) {
		p.schedule(bind(&test_function, true, "test_queue_until_pool_size",  i, 100));
	}

	pool_size += 1; // monitor
	while(p.pending_tasks() > 0 || (!p.pending_tasks() && p.active_tasks()>0)) {
		assert(p.pool_size() == pool_size);
		usleep(100000);
	}
	printf("test_queue_until_pool_size: End\n");
}

void test_destroy_with_pending_tasks()
{
	printf("test_destroy_with_pending_tasks: Start\n");
	threadpool::pool p;
	for (unsigned i = 1; i <= 200; i++) {
		p.schedule(bind(&test_function, true, "test_destroy_with_pending_tasks",  i, 100));
	}
	printf("test_destroy_with_pending_tasks: End\n");
}

void test_max_equal_min()
{
	printf("test_max_equal_min: Start\n");
	unsigned int pool_size = 8;
	threadpool::pool p(pool_size, pool_size);

	for (unsigned i = 1; i <= 200; i++) {
		p.schedule(bind(&test_function, true, "test_max_equal_min",  i, 100));
	}

	while(p.pending_tasks() > 0 || (!p.pending_tasks() && p.active_tasks()>0)) {
		assert(p.pool_size() == pool_size);
		usleep(1000);
	}
	printf("test_max_equal_min: End\n");
}

void test_pool_stress()
{
	printf("test_pool_stress: Start\n");
	threadpool::pool p;
	for (unsigned i = 1; i <= 10000; i++) {
		p.schedule(bind(&test_function, false, "test_pool_stress",  i, 1000));
	}
	while(p.pending_tasks() > 0 || (!p.pending_tasks() && p.active_tasks()>0)) {
		printf("pool_size:%u, active_tasks:%u, pending_tasks:%u\n", p.pool_size(), p.active_tasks(), p.pending_tasks());
		usleep(250*1000);
	}
	printf("test_pool_stress: End\n");
}

void test_rel_schedule()
{
	printf("test_rel_schedule: Start\n");

	threadpool::pool p;
	unsigned total_tasks = 10;
	for (unsigned i = 1; i <= total_tasks; i++) {
		p.schedule(bind(&test_function, false, "test_rel_schedule",  i, 1000), posix_time::seconds(5));
	}
	usleep(1000);
	printf("pool_size:%u, active_tasks:%u, pending_tasks:%u\n", p.pool_size(), p.active_tasks(), p.pending_tasks());
	assert(p.pending_tasks() == total_tasks);
	assert(p.active_tasks() == 0); // only the monitor is active

	while(p.pending_tasks() > 0 || (!p.pending_tasks() && p.active_tasks()>0)) {
		printf("pool_size:%u, active_tasks:%u, pending_tasks:%u\n", p.pool_size(), p.active_tasks(), p.pending_tasks());
		usleep(1000*1000);
	}
	printf("test_rel_schedule: End\n");
}

void test_abs_schedule()
{
	printf("test_abs_schedule: Start\n");

	threadpool::pool p;
	unsigned scheduled_tasks = 10;
	for (unsigned i = 1; i <= scheduled_tasks; i++) {
		p.schedule(bind(&test_function, false, "test_abs_schedule",  i, 1000), (get_system_time() + posix_time::seconds(5)));
	}
	usleep(1000);
	printf("pool_size:%u, active_tasks:%u, pending_tasks:%u\n", p.pool_size(), p.active_tasks(), p.pending_tasks());
	//assert(p.pending_tasks() == scheduled_tasks);
	assert(p.active_tasks() == 0); // only the monitor is active

	unsigned tasks = 5;
	for (unsigned i = 1; i <= tasks; i++) {
		p.schedule(bind(&test_function, false, "test_abs_schedule",  i, 3000));
	}

	usleep(2000);
	assert(p.active_tasks() == tasks);
	assert(p.pending_tasks() == scheduled_tasks);


	while(p.pending_tasks() > 0 || (!p.pending_tasks() && p.active_tasks()>0)) {
		printf("pool_size:%u, active_tasks:%u, pending_tasks:%u\n", p.pool_size(), p.active_tasks(), p.pending_tasks());
		usleep(1000*1000);
	}
	printf("test_abs_schedule: End\n");
}

class Average
{
public:
	Average()
	: m_counter(0)
	, m_numSamples(0)
	, m_max(0)
	, m_min(0)
	{}

	void add_diff(int64_t d)
	{
		lock_guard<mutex> lock(m_lock);
		m_counter += abs(d);
		m_numSamples += 1;
		if (d > m_max) {
			m_max = d;
		} else if (d < m_min) {
			m_min = d;
		}
	}

	int64_t get_average()
	{
		lock_guard<mutex> lock(m_lock);
		printf("get_average - Counter: %ld, samples: %ld max: %ld min:%ld\n", m_counter, m_numSamples, m_max, m_min);
		return ( m_counter / max(m_numSamples, int64_t(1)) );
	}

private:
	int64_t m_counter, m_numSamples, m_max, m_min;
	mutex m_lock;
};

static void test_schedule_function(Average *a, unsigned int index, bool isFuture, system_time when)
{
	int64_t diff = (get_system_time() - when).total_microseconds();
	printf("test_schedule_function(index=%d, future=%s) - diff: %ld\n", index, isFuture ? "true" : "false", diff);
	a->add_diff(diff);
	//usleep(50*1000);
}

void test_schedule_order(const char *desc, threadpool::pool & p, unsigned int num_tasks, int max_microsecs)
{
	printf("test_schedule_order(%s): Start\n", desc);

	Average a;

	for (unsigned int i = 0; i < num_tasks; i++)
	{
		if ( rand() % 2 ) {
			system_time date = get_system_time() + posix_time::milliseconds( rand()%5000 + 1 );
			p.schedule(bind(&test_schedule_function, &a, i, true, date), date);
		} else {
			p.schedule(bind(&test_schedule_function, &a, i, false, get_system_time()));
		}
	}

	while(p.pending_tasks() > 0 || (!p.pending_tasks() && p.active_tasks()>0)) {
		usleep(1000*500);
		printf("pool_size:%u, active_tasks:%u, pending_tasks:%u\n", p.pool_size(), p.active_tasks(), p.pending_tasks());
	}

	int64_t average = a.get_average();

	printf("test_schedule_order(%s) - Average: %ld, Max expected: %d\n", desc, average, max_microsecs);
	assert(average < max_microsecs);

	printf("test_schedule_order(%s): End\n", desc);
}

void test_trivial_schedule()
{
	// average seen in some runs: from 40 to 60 microsecs
	int max_microsecs = 100;
	threadpool::pool p(50);
	test_schedule_order("easy", p, 20, max_microsecs);
}

void test_easy_schedule()
{
	// average seen in some runs: 60~150 microsecs
	int max_microsecs = 150;
	threadpool::pool p;
	test_schedule_order("easy", p, 500, max_microsecs);
}

void test_easy_schedule_no_resize()
{
	// average seen in some runs: 40 to 70 microsecs
	int max_microsecs = 100;
	threadpool::pool p(1000, 1000);
	test_schedule_order("easy_no_resize", p, 1000, max_microsecs);
}

void test_heavy_schedule()
{
	// average seen in some runs: 60 ms
	int max_microsecs = 100*1000; // too many tasks, maximum thread count is 1000
	threadpool::pool p(-1, 1000);
	test_schedule_order("heavy", p, 5*1000, max_microsecs);
}

void test_heavy_schedule_no_resize()
{
	// average seen in some runs: 17 ms
	int max_microsecs = 100*1000;  // too many tasks, maximum thread count is 1000
	threadpool::pool p(1000, 1000);
	test_schedule_order("heavy_no_resize", p, 5*1000, max_microsecs);
}

void test_schedule_queue_function(atomic_counter *pcounter, unsigned int index)
{
	atomic_counter &counter = *pcounter;
	assert(++counter == index);
	printf("Tasks %d ok\n", index);
}

void test_schedule_queue()
{
	atomic_counter counter(0);
	unsigned int task_count = 1000;
	printf("test_schedule_queue: Start\n");

	threadpool::pool p;

	for (unsigned int i = 1; i <= task_count; i++)
	{
		system_time date = get_system_time() + posix_time::microseconds( i * 500);
		p.schedule(bind(&test_schedule_queue_function, &counter, i), date);
	}

	while(p.pending_tasks() > 0 || (!p.pending_tasks() && p.active_tasks()>0)) {
		usleep(1000*500);
		printf("pool_size:%u, active_tasks:%u, pending_tasks:%u\n", p.pool_size(), p.active_tasks(), p.pending_tasks());
	}

	assert(counter == task_count);
	printf("test_schedule_queue: End\n");
}

int main(int argc, char *argv[])
{
	struct {
		const char *name;
		void (*func)();
	} tests_by_name[] = {
		{ "wait_until_idle",            &test_wait_until_idle },
		{ "wait_until_resize",          &test_wait_until_resize },
		{ "queue_until_pool_size",      &test_queue_until_pool_size },
		{ "destroy_with_pending_tasks", &test_destroy_with_pending_tasks },
		{ "max_equal_min",              &test_max_equal_min },
		{ "pool_stress",                &test_pool_stress },
		{ "rel_schedule",               &test_rel_schedule },
		{ "abs_schedule",               &test_abs_schedule },
		{ "trivial_schedule",           &test_trivial_schedule },
		{ "easy_schedule",              &test_easy_schedule },
		{ "easy_schedule_no_resize",    &test_easy_schedule_no_resize },
		{ "heavy_schedule",             &test_heavy_schedule },
		{ "heavy_schedule_no_resize",   &test_heavy_schedule_no_resize },
		{ "schedule_queue",             &test_schedule_queue },

		{ NULL, NULL }
	};

	if (argc > 1)
	{
		for (int arg_index = 1; arg_index < argc; ++arg_index)
		{
			string test_name(argv[arg_index]);
			for ( int i = 0; tests_by_name[i].name; ++i )
			{
				if (test_name == tests_by_name[i].name)
				{
					tests_by_name[i].func();
				}
			}

		}
	}
	else for ( int i = 0; tests_by_name[i].name; ++i )
	{
		tests_by_name[i].func();
	}

	printf("Bye bye!\n");

	return 0;
}
