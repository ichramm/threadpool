#
# Makefile for threadpool
#

PROJECT_NAME=threadpool

INCONCERT_DEVEL   ?=../../../..
INCONCERT_SOURCES ?=$(INCONCERT_DEVEL)/sources
INCONCERT_BIN     ?=$(INCONCERT_DEVEL)/bin
INCONCERT_LIB     ?=$(INCONCERT_DEVEL)/lib
PACKAGES          ?=$(INCONCERT_DEVEL)/packages
INCLUDE_DIR       ?=/usr/local/include
LIB_DIR           ?=/usr/local/lib

BOOST_VERSION ?=1_44
BOOST_DIR     ?=$(INCLUDE_DIR)/boost-$(BOOST_VERSION)
BOOST_LIBS    ?=$(LIB_DIR)

INCONCERT_COMPILER   ?= g++
INCONCERT_COMPILER_C ?= gcc
GCC_VERSION          ?= $(shell expr substr "`$(INCONCERT_COMPILER) -dumpversion | tr -dc '[0-9]'`" 1 2)

LIB_NAME=lib$(PROJECT_NAME)

build: Debug/libthreadpool.so
Debug/libthreadpool.so: objs
	g++ -O2 -g -shared -Wl,-soname,$(LIB_NAME) Debug/threadpool.o -o Debug/$(LIB_NAME).so -lboost_thread

objs: Debug/threadpool.o
Debug/threadpool.o: threadpool.h src/threadpool.cpp
	g++ -fPIC -Wall -Wextra -pedantic -O2 -g -c src/threadpool.cpp -o Debug/threadpool.o

test_objs: Debug/main.o
Debug/main.o: threadpool.h tests/main.cpp
	g++ -Wall -Wextra -pedantic -O2 -g -c tests/main.cpp -o Debug/main.o

tests: build test_objs Debug/threadpool.bin
Debug/threadpool.bin: test_objs
	g++ -O2 -g Debug/main.o -o Debug/threadpool.bin -L Debug/ -lboost_thread -lthreadpool

run: tests
	LD_PRELOAD=Debug/libthreadpool.so ./Debug/threadpool.bin

clean:
	rm -f Debug/*
