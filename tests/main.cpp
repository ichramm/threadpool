
#include <threadpool/threadpool.h>

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
	for (unsigned i = 1; i < 200; i++) {
		p.exec(bind(&test_function, true, "test_wait_until_idle",  i, 100));
	}

	while(p.active_tasks()>1) {
		usleep(1000);
	}
	printf("test_wait_until_idle: End\n");
}

void test_queue_until_pool_size()
{
	printf("test_queue_until_pool_size: Start\n");
	unsigned int pool_size = 100;
	threadpool::pool p(100);
	for (unsigned i = 1; i < pool_size; i++) {
		p.exec(bind(&test_function, true, "test_queue_until_pool_size",  i, 100));
	}

	pool_size += 1; // monitor
	while(p.active_tasks()>1) {
		assert(p.pool_size() == pool_size);
		usleep(100000);
	}
	printf("test_queue_until_pool_size: End\n");
}

void test_destroy_with_pending_tasks()
{
	printf("test_destroy_with_pending_tasks: Start\n");
	threadpool::pool p;
	for (unsigned i = 1; i < 200; i++) {
		p.exec(bind(&test_function, true, "test_destroy_with_pending_tasks",  i, 100));
	}
	printf("test_destroy_with_pending_tasks: End\n");
}

void test_max_equal_min()
{
	printf("test_max_equal_min: Start\n");
	unsigned int pool_size = 8;
	threadpool::pool p(pool_size, pool_size);

	for (unsigned i = 1; i < 200; i++) {
		p.exec(bind(&test_function, true, "test_max_equal_min",  i, 100));
	}

	while( p.active_tasks()>1 ) {
		assert(p.pool_size() == pool_size);
		usleep(1000);
	}
	printf("test_max_equal_min: End\n");
}

void test_pool_stress()
{
	printf("test_destroy_with_pending_tasks: Start\n");
	threadpool::pool p;
	for (unsigned i = 1; i < 10000; i++) {
		p.exec(bind(&test_function, false, "test_destroy_with_pending_tasks",  i, 1000));
	}
	while( p.active_tasks()>1 ) {
		printf("pool_size:%u, active_tasks:%u, pending_tasks:%u\n", p.pool_size(), p.active_tasks(), p.pending_tasks());
		usleep(250*1000);
	}
	printf("test_destroy_with_pending_tasks: End\n");
}




int main()
{
	test_wait_until_idle();

	test_queue_until_pool_size();

	test_destroy_with_pending_tasks();

	test_max_equal_min();

	test_pool_stress();

	printf("Bye bye!\n");

	return 0;
}
