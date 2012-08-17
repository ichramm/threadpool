
#include <threadpool/threadpool.h>

#include <stdio.h>
#include <string>
#include <iostream>
#include <boost/bind.hpp>

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
				threadpool::MIN_POOL_THREADS,
				threadpool::MAX_POOL_THREADS,
				threadpool::TIMEOUT_ADD_MORE_THREADS,
				threadpool::TIMEOUT_REMOVE_THREADS
		);
	for (unsigned i = 1; i <= 200; i++) {
		p.schedule(bind(&test_function, true, "test_wait_until_idle",  i, 100));
	}
	while(p.pending_tasks() > 0 || (!p.pending_tasks() && p.active_tasks()>1)) {
		usleep(1000);
	}
	printf("test_wait_until_idle: End\n");
}

void test_wait_until_resize()
{
	printf("test_wait_until_resize: Start\n");
	unsigned int pool_size = 8;
	threadpool::pool p(pool_size, 100, threadpool::TIMEOUT_ADD_MORE_THREADS, 3000);
	for (unsigned i = 1; i <= 200; i++) {
		p.schedule(bind(&test_function, true, "test_wait_until_resize",  i, 100));
	}
	while(p.pending_tasks() > 0 || (!p.pending_tasks() && p.active_tasks()>1)) {
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
	while(p.pending_tasks() > 0 || (!p.pending_tasks() && p.active_tasks()>1)) {
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

	while(p.pending_tasks() > 0 || (!p.pending_tasks() && p.active_tasks()>1)) {
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
	while(p.pending_tasks() > 0 || (!p.pending_tasks() && p.active_tasks()>1)) {
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
	//assert(p.pending_tasks() == total_tasks);
	assert(p.active_tasks() == 1); // only the monitor is active

	while(p.pending_tasks() > 0 || (!p.pending_tasks() && p.active_tasks()>1)) {
		printf("pool_size:%u, active_tasks:%u, pending_tasks:%u\n", p.pool_size(), p.active_tasks(), p.pending_tasks());
		usleep(1000*1000);
	}
	printf("test_rel_schedule: End\n");
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
